// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! In-memory implementations of both stores.
//!
//! The reference behaviour of the contract and the test double for the
//! server (what DEV-44 calls the memory store). Not durable.

use crate::blob::{BlobInfo, BlobKey, BlobLimits, BlobRead, BlobStore, ByteRange, PutOutcome};
use crate::error::{AdvanceHeadError, ErrorKind, StoreError, StoreResult};
use crate::ids::{
    Digest, ObjectKind, ObjectRef, OperationId, ProjectId, RequestId, WorkspaceId, WorkspaceName,
};
use crate::metadata::{
    AdvanceHead, AdvanceMode, HeadEvent, HeadState, MetadataLimits, MetadataStore, ObjectRecord,
    ObjectStore, Scope, WorkspaceRecord,
};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Mutex, MutexGuard};

// ---- metadata ---------------------------------------------------------------

struct WorkspaceRow {
    record: WorkspaceRecord,
    events: Vec<HeadEvent>,
    last_request: Option<RequestId>,
}

type ObjKey = (Scope, ObjectKind, Digest);

#[derive(Default)]
struct MetaInner {
    projects: HashSet<ProjectId>,
    objects: HashMap<ObjKey, ObjectRecord>,
    /// Objects whose whole closure has been proven present.
    verified: HashSet<ObjKey>,
    /// Objects visited by the last closure walk (for tests).
    last_walk: usize,
    workspaces: HashMap<WorkspaceId, WorkspaceRow>,
    names: HashMap<WorkspaceName, WorkspaceId>,
    next_workspace: u64,
}

/// In-memory [`MetadataStore`].
pub struct MemoryMetadataStore {
    limits: MetadataLimits,
    virtual_roots: HashSet<ObjectRef>,
    inner: Mutex<MetaInner>,
}

impl MemoryMetadataStore {
    /// A store with no virtual roots.
    pub fn new(limits: MetadataLimits) -> Self {
        Self::with_virtual_roots(limits, Vec::new())
    }

    /// A store that treats `roots` as present (jj's root operation, root
    /// commit and empty tree, in a real server).
    pub fn with_virtual_roots(limits: MetadataLimits, roots: Vec<ObjectRef>) -> Self {
        Self {
            limits,
            virtual_roots: roots.into_iter().collect(),
            inner: Mutex::new(MetaInner::default()),
        }
    }

    /// The limits `default()` uses.
    pub fn default_limits() -> MetadataLimits {
        MetadataLimits {
            max_batch: 1_000,
            max_object_bytes: 4 * 1024 * 1024,
            max_batch_bytes: 32 * 1024 * 1024,
            max_missing_reported: 100,
        }
    }
}

impl Default for MemoryMetadataStore {
    fn default() -> Self {
        Self::new(Self::default_limits())
    }
}

fn err(kind: ErrorKind, msg: impl Into<String>) -> StoreError {
    StoreError::new(kind, msg)
}

/// Locks `m`. A panic while it was held may have left the data half
/// updated, so a poisoned lock is `Internal`, not a panic.
fn lock<T>(m: &Mutex<T>) -> StoreResult<MutexGuard<'_, T>> {
    m.lock()
        .map_err(|_| err(ErrorKind::Internal, "store lock poisoned"))
}

fn check_scope(scope: &Scope, kind: ObjectKind) -> StoreResult<()> {
    let ok = match scope {
        Scope::Project(_) => kind.is_project_scoped(),
        Scope::Workspace(_) => !kind.is_project_scoped(),
    };
    if ok {
        Ok(())
    } else {
        Err(err(
            ErrorKind::InvalidArgument,
            format!("{kind:?} does not belong in {scope:?}"),
        ))
    }
}

impl MetaInner {
    /// Objects reachable from `head` that are not stored, up to `limit`.
    /// Stops at virtual roots and at objects already verified. If nothing is
    /// missing, marks every object it walked as verified, so the next advance
    /// only walks new objects.
    fn check_closure(
        &mut self,
        virtual_roots: &HashSet<ObjectRef>,
        workspace: &WorkspaceId,
        project: &ProjectId,
        head: &OperationId,
        limit: usize,
    ) -> Vec<ObjectRef> {
        let ws_scope = Scope::Workspace(workspace.clone());
        let pj_scope = Scope::Project(project.clone());
        let mut seen = HashSet::new();
        let mut walked = Vec::new();
        let mut queue = VecDeque::from([ObjectRef {
            kind: ObjectKind::Operation,
            id: head.0.clone(),
        }]);
        let mut missing = Vec::new();
        while let Some(r) = queue.pop_front() {
            if virtual_roots.contains(&r) || !seen.insert(r.clone()) {
                continue;
            }
            let scope = if r.kind.is_project_scoped() {
                &pj_scope
            } else {
                &ws_scope
            };
            let key = (scope.clone(), r.kind, r.id.clone());
            if self.verified.contains(&key) {
                continue;
            }
            match self.objects.get(&key) {
                Some(rec) => {
                    queue.extend(rec.refs.iter().cloned());
                    walked.push(key);
                }
                None => {
                    missing.push(r);
                    if missing.len() >= limit {
                        break;
                    }
                }
            }
        }
        self.last_walk = walked.len();
        if missing.is_empty() {
            self.verified.extend(walked);
        }
        missing
    }

    fn scope_exists(&self, scope: &Scope) -> bool {
        match scope {
            Scope::Project(p) => self.projects.contains(p),
            Scope::Workspace(w) => self.workspaces.contains_key(w),
        }
    }
}

#[async_trait]
impl ObjectStore for MemoryMetadataStore {
    fn limits(&self) -> MetadataLimits {
        self.limits
    }

    fn virtual_roots(&self) -> Vec<ObjectRef> {
        self.virtual_roots.iter().cloned().collect()
    }

    async fn get_objects(
        &self,
        scope: &Scope,
        refs: &[ObjectRef],
    ) -> StoreResult<Vec<Option<ObjectRecord>>> {
        if refs.len() > self.limits.max_batch {
            return Err(err(ErrorKind::LimitExceeded, "batch too large"));
        }
        for r in refs {
            check_scope(scope, r.kind)?;
        }
        let inner = lock(&self.inner)?;
        Ok(refs
            .iter()
            .map(|r| {
                inner
                    .objects
                    .get(&(scope.clone(), r.kind, r.id.clone()))
                    .cloned()
            })
            .collect())
    }

    async fn put_objects(&self, scope: &Scope, objects: Vec<ObjectRecord>) -> StoreResult<()> {
        if objects.len() > self.limits.max_batch {
            return Err(err(ErrorKind::LimitExceeded, "batch too large"));
        }
        let mut total = 0usize;
        for o in &objects {
            check_scope(scope, o.kind)?;
            o.check_digests()?;
            if o.data.len() > self.limits.max_object_bytes {
                return Err(err(
                    ErrorKind::LimitExceeded,
                    format!("object {:?} too large", o.id),
                ));
            }
            total += o.data.len();
        }
        if total > self.limits.max_batch_bytes {
            return Err(err(
                ErrorKind::LimitExceeded,
                "batch bytes above max_batch_bytes",
            ));
        }
        let mut inner = lock(&self.inner)?;
        if !inner.scope_exists(scope) {
            return Err(err(ErrorKind::NotFound, format!("{scope:?}")));
        }
        // Check every object before writing any, so this implementation is in
        // fact atomic per batch (stronger than the contract requires).
        let mut in_batch: HashMap<(ObjectKind, &Digest), &ObjectRecord> = HashMap::new();
        for o in &objects {
            let clash = |e: &ObjectRecord| {
                e.data != o.data || e.refs != o.refs || e.blob_refs != o.blob_refs
            };
            let stored = inner.objects.get(&(scope.clone(), o.kind, o.id.clone()));
            let earlier = in_batch.insert((o.kind, &o.id), o);
            if stored.is_some_and(clash) || earlier.is_some_and(clash) {
                return Err(err(
                    ErrorKind::ObjectMismatch,
                    format!("{:?} {} has two different versions", o.kind, o.id.to_hex()),
                ));
            }
        }
        for o in objects {
            inner
                .objects
                .entry((scope.clone(), o.kind, o.id.clone()))
                .or_insert(o);
        }
        Ok(())
    }

    async fn find_missing(&self, scope: &Scope, refs: &[ObjectRef]) -> StoreResult<Vec<ObjectRef>> {
        if refs.len() > self.limits.max_batch {
            return Err(err(ErrorKind::LimitExceeded, "batch too large"));
        }
        for r in refs {
            check_scope(scope, r.kind)?;
        }
        let inner = lock(&self.inner)?;
        Ok(refs
            .iter()
            .filter(|r| !self.virtual_roots.contains(r))
            .filter(|r| {
                !inner
                    .objects
                    .contains_key(&(scope.clone(), r.kind, r.id.clone()))
            })
            .cloned()
            .collect())
    }
}

#[async_trait]
impl MetadataStore for MemoryMetadataStore {
    async fn create_project(&self, project: &ProjectId) -> StoreResult<()> {
        if !lock(&self.inner)?.projects.insert(project.clone()) {
            return Err(err(
                ErrorKind::AlreadyExists,
                format!("project {}", project.0),
            ));
        }
        Ok(())
    }

    async fn create_workspace(
        &self,
        project: &ProjectId,
        name: &WorkspaceName,
        initial_head: OperationId,
    ) -> StoreResult<WorkspaceRecord> {
        initial_head.0.check_len()?;
        let mut inner = lock(&self.inner)?;
        if !inner.projects.contains(project) {
            return Err(err(ErrorKind::NotFound, format!("project {project:?}")));
        }
        if inner.names.contains_key(name) {
            return Err(err(ErrorKind::AlreadyExists, format!("workspace {name:?}")));
        }
        inner.next_workspace += 1;
        let id = WorkspaceId(inner.next_workspace.to_be_bytes().to_vec());
        let record = WorkspaceRecord {
            id: id.clone(),
            name: name.clone(),
            project: project.clone(),
            head: HeadState {
                head: initial_head,
                sequence: 0,
                merged: false,
            },
        };
        inner.names.insert(name.clone(), id.clone());
        inner.workspaces.insert(
            id,
            WorkspaceRow {
                record: record.clone(),
                events: Vec::new(),
                last_request: None,
            },
        );
        Ok(record)
    }

    async fn get_workspace_by_name(
        &self,
        name: &WorkspaceName,
    ) -> StoreResult<Option<WorkspaceRecord>> {
        let inner = lock(&self.inner)?;
        Ok(inner
            .names
            .get(name)
            .and_then(|id| inner.workspaces.get(id))
            .map(|w| w.record.clone()))
    }

    async fn get_workspace(&self, id: &WorkspaceId) -> StoreResult<Option<WorkspaceRecord>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .workspaces
            .get(id)
            .map(|w| w.record.clone()))
    }

    async fn advance_head(&self, req: AdvanceHead) -> Result<HeadState, AdvanceHeadError> {
        if req.mode != AdvanceMode::Reject {
            return Err(err(ErrorKind::Unsupported, "only Reject mode is implemented").into());
        }
        req.expected.0.check_len()?;
        req.new_head.0.check_len()?;
        let mut inner = lock(&self.inner)?;
        let (current, project, last_request) = match inner.workspaces.get(&req.workspace) {
            Some(w) => (
                w.record.head.clone(),
                w.record.project.clone(),
                w.last_request.clone(),
            ),
            None => return Err(err(ErrorKind::NotFound, "unknown workspace").into()),
        };
        // A retry of the request that produced the current head succeeds.
        if last_request.as_ref() == Some(&req.request_id) {
            if current.head == req.new_head {
                return Ok(current);
            }
            return Err(err(
                ErrorKind::InvalidArgument,
                "request id reused for another head",
            )
            .into());
        }
        if current.head != req.expected {
            return Err(AdvanceHeadError::HeadMoved { current });
        }
        let missing = inner.check_closure(
            &self.virtual_roots,
            &req.workspace,
            &project,
            &req.new_head,
            self.limits.max_missing_reported,
        );
        if !missing.is_empty() {
            return Err(AdvanceHeadError::MissingObjects { missing });
        }
        let next = HeadState {
            head: req.new_head,
            sequence: current.sequence + 1,
            merged: false,
        };
        let row = inner
            .workspaces
            .get_mut(&req.workspace)
            .ok_or_else(|| err(ErrorKind::Internal, "workspace vanished under the lock"))?;
        row.record.head = next.clone();
        row.last_request = Some(req.request_id.clone());
        row.events.push(HeadEvent {
            workspace: req.workspace,
            state: next.clone(),
            request_id: req.request_id,
        });
        Ok(next)
    }

    async fn read_events(
        &self,
        workspace: &WorkspaceId,
        after_sequence: u64,
        limit: usize,
    ) -> StoreResult<Vec<HeadEvent>> {
        if limit == 0 {
            return Err(err(ErrorKind::InvalidArgument, "limit must be at least 1"));
        }
        let inner = lock(&self.inner)?;
        let row = inner
            .workspaces
            .get(workspace)
            .ok_or_else(|| err(ErrorKind::NotFound, "unknown workspace"))?;
        Ok(row
            .events
            .iter()
            .filter(|e| e.state.sequence > after_sequence)
            .take(limit)
            .cloned()
            .collect())
    }
}

// ---- blobs ------------------------------------------------------------------

/// In-memory [`BlobStore`]. Supports `delete`.
pub struct MemoryBlobStore {
    limits: BlobLimits,
    blobs: Mutex<HashMap<BlobKey, Bytes>>,
}

impl MemoryBlobStore {
    /// An empty store that enforces `limits`.
    pub fn new(limits: BlobLimits) -> Self {
        Self {
            limits,
            blobs: Mutex::new(HashMap::new()),
        }
    }
}

impl Default for MemoryBlobStore {
    fn default() -> Self {
        Self::new(BlobLimits {
            max_blob_bytes: 64 * 1024 * 1024,
            max_read_bytes: 4 * 1024 * 1024,
            max_batch: 1_000,
        })
    }
}

/// Slice `data` by `range` per the [`BlobStore::read_range`] contract.
pub(crate) fn slice_range(data: &Bytes, range: ByteRange, max_read: u64) -> StoreResult<BlobRead> {
    let total = data.len() as u64;
    let start = range.offset.min(total);
    let wanted = range.len.unwrap_or(total - start);
    let end = start.saturating_add(wanted).min(total);
    if end - start > max_read {
        return Err(err(
            ErrorKind::LimitExceeded,
            "range larger than max_read_bytes",
        ));
    }
    Ok(BlobRead {
        data: data.slice(start as usize..end as usize),
        total_len: total,
    })
}

#[async_trait]
impl BlobStore for MemoryBlobStore {
    fn limits(&self) -> BlobLimits {
        self.limits
    }

    async fn put(&self, key: &BlobKey, data: Bytes) -> StoreResult<PutOutcome> {
        key.digest.check_len()?;
        if data.len() as u64 > self.limits.max_blob_bytes {
            return Err(err(ErrorKind::LimitExceeded, "blob too large"));
        }
        if !crate::digest::verify(&key.digest, &data) {
            return Err(err(ErrorKind::DigestMismatch, "bytes do not match digest"));
        }
        let mut blobs = lock(&self.blobs)?;
        if blobs.contains_key(key) {
            return Ok(PutOutcome::AlreadyPresent);
        }
        blobs.insert(key.clone(), data);
        Ok(PutOutcome::Created)
    }

    async fn read_range(&self, key: &BlobKey, range: ByteRange) -> StoreResult<BlobRead> {
        key.digest.check_len()?;
        let data = self
            .blobs
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| err(ErrorKind::NotFound, "blob not stored"))?;
        slice_range(&data, range, self.limits.max_read_bytes)
    }

    async fn stat(&self, keys: &[BlobKey]) -> StoreResult<Vec<Option<BlobInfo>>> {
        if keys.len() > self.limits.max_batch {
            return Err(err(ErrorKind::LimitExceeded, "batch too large"));
        }
        for k in keys {
            k.digest.check_len()?;
        }
        let blobs = lock(&self.blobs)?;
        Ok(keys
            .iter()
            .map(|k| {
                blobs.get(k).map(|b| BlobInfo {
                    len: b.len() as u64,
                })
            })
            .collect())
    }

    async fn delete(&self, key: &BlobKey) -> StoreResult<()> {
        lock(&self.blobs)?.remove(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HashAlgorithm;
    use crate::digest::compute;

    fn rec(kind: ObjectKind, data: &str, refs: Vec<ObjectRef>) -> ObjectRecord {
        let data = Bytes::from(data.to_owned());
        ObjectRecord {
            kind,
            id: compute(HashAlgorithm::Sha256, &data),
            data,
            refs,
            blob_refs: vec![],
        }
    }

    /// The closure walk only visits objects added since the last successful
    /// advance; everything older is already verified.
    #[tokio::test]
    async fn closure_walk_stops_at_verified_objects() {
        let root = ObjectRef {
            kind: ObjectKind::Operation,
            id: compute(HashAlgorithm::Sha256, b"root"),
        };
        let store = MemoryMetadataStore::with_virtual_roots(
            MemoryMetadataStore::default_limits(),
            vec![root.clone()],
        );
        let project = ProjectId("p".into());
        store.create_project(&project).await.unwrap();
        let ws = store
            .create_workspace(
                &project,
                &WorkspaceName("u/w".into()),
                OperationId(root.id.clone()),
            )
            .await
            .unwrap();
        let ws_scope = Scope::Workspace(ws.id.clone());
        let mut head = OperationId(root.id.clone());
        for i in 0..50 {
            let parent = ObjectRef {
                kind: ObjectKind::Operation,
                id: head.0.clone(),
            };
            let op = rec(ObjectKind::Operation, &format!("op {i}"), vec![parent]);
            store
                .put_objects(&ws_scope, vec![op.clone()])
                .await
                .unwrap();
            let next = OperationId(op.id.clone());
            store
                .advance_head(AdvanceHead {
                    workspace: ws.id.clone(),
                    expected: head.clone(),
                    new_head: next.clone(),
                    mode: AdvanceMode::Reject,
                    request_id: RequestId(format!("r{i}").into_bytes()),
                })
                .await
                .unwrap();
            head = next;
            let inner = store.inner.lock().unwrap();
            assert_eq!(
                inner.last_walk, 1,
                "advance {i} walked only the new operation"
            );
            assert_eq!(inner.verified.len(), i + 1);
        }
    }
}
