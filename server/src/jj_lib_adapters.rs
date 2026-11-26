// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Server-side `jj_lib` storage adapters (`ServerBackend`, `ServerOpStore`,
//! `ServerOpHeadsStore`, and `ServerIndexStore`) over [`Store`].
//!
//! # Concurrency and Merge Invariants
//! [`ServerOpHeadsStore::with_expected_heads`] pins both `get_op_heads` and
//! `update_op_heads` CAS to a validated head snapshot so concurrent arrivals trigger a CAS
//! retry through safety checks and ancestor-only head pruning succeeds atomically.
//! [`ServerIndex`] resolves `is_ancestor` and `heads` via bounded commit-parent BFS cached
//! per reconciliation call and signals deferred fallback on missing commits or walk-budget
//! exhaustion so `ServerOpStore` and `ServerOpHeadsStore` abort before writing or publishing.
//! `ServerBackend::read_tree` sorts and deduplicates entries before `Tree::from_sorted_entries`.

#![allow(elided_lifetimes_in_paths)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::{self, Debug};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use async_trait::async_trait;
use futures::StreamExt as _;
use futures::stream::{self, BoxStream, LocalBoxStream};
use jj_lib::backend::*;
use jj_lib::commit::Commit as JjCommit;
use jj_lib::graph::GraphNode;
use jj_lib::index::{
    ChangeIdIndex, Index, IndexError, IndexResult, MutableIndex, ReadonlyIndex,
    ResolvedChangeTargets,
};
use jj_lib::index_store::{IndexStore, IndexStoreResult};
use jj_lib::object_id::{HexPrefix, ObjectId, PrefixResolution};
use jj_lib::op_heads_store::{OpHeadsStore, OpHeadsStoreError, OpHeadsStoreLock};
use jj_lib::op_store::{
    OpStore, OpStoreError, OpStoreResult, Operation, OperationId, View, ViewId,
};
use jj_lib::operation::Operation as JjOperation;
use jj_lib::ref_name::WorkspaceName;
use jj_lib::repo_path::{RepoPath, RepoPathBuf};
use jj_lib::revset::{ResolvedExpression, Revset, RevsetContainingFn, RevsetEvaluationError};
use jj_lib::store::Store as JjStore;
use jj_lib::submodule_store::SubmoduleStore;
use jj_lib::workspace_store::{WorkspaceStore, WorkspaceStoreError};

use crate::hash_utils::{
    compute_git_commit_hash, compute_git_tree_hash, hash_operation, hash_view,
    try_compute_git_blob_hash, validate_and_canonicalize_commit_fields,
};
use crate::store::Store;
use cc_common::conversions::backend::{
    commit_from_proto, commit_to_proto, tree_entry_from_proto, tree_entry_to_proto,
};
use cc_common::conversions::op_store::{
    operation_from_proto, operation_to_proto, view_from_proto, view_to_proto,
};

pub struct ServerBackend {
    store: Arc<dyn Store>,
    project_id: String,
    root_commit_id: CommitId,
    root_change_id: ChangeId,
    empty_tree_id: TreeId,
}

impl Debug for ServerBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerBackend")
            .field("project_id", &self.project_id)
            .finish()
    }
}

impl ServerBackend {
    pub fn new(store: Arc<dyn Store>, project_id: String) -> Self {
        let root_commit_id = CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES);
        let root_change_id = ChangeId::from_bytes(&cc_common::ROOT_CHANGE_ID_BYTES);
        let empty_tree_id = TreeId::from_hex(cc_common::EMPTY_TREE_ID_HEX);
        Self {
            store,
            project_id,
            root_commit_id,
            root_change_id,
            empty_tree_id,
        }
    }
}

#[async_trait]
impl Backend for ServerBackend {
    fn name(&self) -> &str {
        "server_backend"
    }

    fn commit_id_length(&self) -> usize {
        cc_common::COMMIT_ID_LENGTH
    }

    fn change_id_length(&self) -> usize {
        cc_common::CHANGE_ID_LENGTH
    }

    fn root_commit_id(&self) -> &CommitId {
        &self.root_commit_id
    }

    fn root_change_id(&self) -> &ChangeId {
        &self.root_change_id
    }

    fn empty_tree_id(&self) -> &TreeId {
        &self.empty_tree_id
    }

    // Use 100 as an arbitrary high value for concurrency. Google uses 100, this works since functions are async and can fetch objects in parallel.
    fn concurrency(&self) -> usize {
        100
    }

    async fn read_file(
        &self,
        _path: &RepoPath,
        id: &FileId,
    ) -> BackendResult<Pin<Box<dyn futures::AsyncRead + Send>>> {
        let file_id_bytes = id.to_bytes().to_vec();
        let content = self
            .store
            .get_file(&self.project_id, &file_id_bytes)
            .await
            .map_err(|e| BackendError::Other(e.into()))?
            .ok_or_else(|| BackendError::ObjectNotFound {
                object_type: "file".into(),
                hash: id.hex(),
                source: Some("file not found in server store".into()),
            })?;
        Ok(Box::pin(futures::io::Cursor::new(content)))
    }

    async fn write_file(
        &self,
        _path: &RepoPath,
        contents: &mut (dyn futures::AsyncRead + Send + Unpin),
    ) -> BackendResult<FileId> {
        let mut buffer = Vec::new();
        futures::AsyncReadExt::read_to_end(contents, &mut buffer)
            .await
            .map_err(|e| BackendError::Other(e.into()))?;
        let file_id_bytes =
            try_compute_git_blob_hash(&buffer).map_err(|e| BackendError::Other(e.into()))?;
        self.store
            .put_file(self.project_id.clone(), file_id_bytes.clone(), buffer)
            .await
            .map_err(|e| BackendError::Other(e.into()))?;
        Ok(FileId::from_bytes(&file_id_bytes))
    }

    async fn read_symlink(&self, _path: &RepoPath, id: &SymlinkId) -> BackendResult<String> {
        let raw = self
            .store
            .get_file(&self.project_id, id.as_bytes())
            .await
            .map_err(|e| BackendError::Other(e.into()))?
            .ok_or_else(|| BackendError::ObjectNotFound {
                object_type: "symlink".into(),
                hash: id.hex(),
                source: Some("symlink not found in server store".into()),
            })?;
        let target = String::from_utf8(raw).map_err(|e| {
            BackendError::Other(format!("invalid UTF-8 symlink target: {e}").into())
        })?;
        if target.contains('\0') {
            return Err(BackendError::Other(
                "stored symlink target contains NUL byte".into(),
            ));
        }
        Ok(target)
    }

    async fn write_symlink(&self, _path: &RepoPath, target: &str) -> BackendResult<SymlinkId> {
        if target.contains('\0') {
            return Err(BackendError::Other(
                "symlink target must not contain NUL bytes".into(),
            ));
        }
        let raw = target.as_bytes().to_vec();
        let symlink_id_bytes = crate::hash_utils::try_compute_git_blob_hash(&raw)
            .map_err(|e| BackendError::Other(e.into()))?;
        self.store
            .put_file(self.project_id.clone(), symlink_id_bytes.clone(), raw)
            .await
            .map_err(|e| BackendError::Other(e.into()))?;
        Ok(SymlinkId::from_bytes(&symlink_id_bytes))
    }

    async fn read_copy(&self, _id: &CopyId) -> BackendResult<CopyHistory> {
        Err(BackendError::Unsupported(
            "copies not supported".to_string(),
        ))
    }

    async fn write_copy(&self, _contents: &CopyHistory) -> BackendResult<CopyId> {
        Err(BackendError::Unsupported(
            "copies not supported".to_string(),
        ))
    }

    async fn get_related_copies(&self, _copy_id: &CopyId) -> BackendResult<Vec<RelatedCopy>> {
        Err(BackendError::Unsupported(
            "copies not supported".to_string(),
        ))
    }

    async fn read_tree(&self, _path: &RepoPath, id: &TreeId) -> BackendResult<Tree> {
        if *id == self.empty_tree_id {
            return Ok(Tree::from_sorted_entries(vec![]));
        }

        let proto_entries = self
            .store
            .get_tree(&self.project_id, id.as_bytes())
            .await
            .map_err(|e| BackendError::Other(e.into()))?
            .ok_or_else(|| BackendError::ObjectNotFound {
                object_type: "tree".into(),
                hash: id.hex(),
                source: Some("tree not found in server store".into()),
            })?;

        let mut jj_entries = Vec::with_capacity(proto_entries.len());
        for entry in proto_entries {
            let (comp, val) = tree_entry_from_proto(entry).map_err(BackendError::Other)?;
            jj_entries.push((comp, val));
        }
        jj_entries.sort_by(|(a, _), (b, _)| a.cmp(b));
        for pair in jj_entries.windows(2) {
            if pair[0].0 == pair[1].0 {
                return Err(BackendError::Other(
                    format!(
                        "stored tree {} contains duplicate entry {:?}",
                        id.hex(),
                        pair[0].0.as_internal_str()
                    )
                    .into(),
                ));
            }
        }

        Ok(Tree::from_sorted_entries(jj_entries))
    }

    async fn write_tree(&self, _path: &RepoPath, tree: &Tree) -> BackendResult<TreeId> {
        if tree.entries().next().is_none() {
            return Ok(self.empty_tree_id.clone());
        }

        let proto_entries: Result<Vec<_>, _> =
            tree.entries().map(|e| tree_entry_to_proto(&e)).collect();
        let mut proto_entries = proto_entries?;
        for entry in &mut proto_entries {
            if let Some(cc_common::backend::TreeValue {
                value: Some(cc_common::backend::tree_value::Value::File(file)),
            }) = entry.value.as_mut()
                // TODO: Switch to a metadata-only blob size lookup once `Store` splits
                // metadata and blob storage (avoiding a full blob read just for size).
                && let Some(content) = self
                    .store
                    .get_file(&self.project_id, &file.id)
                    .await
                    .map_err(|e| BackendError::Other(e.into()))?
            {
                file.size_bytes = Some(content.len() as u64);
            }
        }
        proto_entries.sort_by(|a, b| a.name.cmp(&b.name));
        let tree_id_bytes =
            compute_git_tree_hash(&proto_entries).map_err(|e| BackendError::Other(e.into()))?;

        self.store
            .put_tree(
                self.project_id.clone(),
                tree_id_bytes.clone(),
                proto_entries,
            )
            .await
            .map_err(|e| BackendError::Other(e.into()))?;

        Ok(TreeId::from_bytes(&tree_id_bytes))
    }

    async fn read_commit(&self, id: &CommitId) -> BackendResult<Commit> {
        if *id == self.root_commit_id {
            return Ok(make_root_commit(
                self.root_change_id().clone(),
                self.empty_tree_id.clone(),
            ));
        }

        let mut proto_commit = self
            .store
            .get_commit(&self.project_id, id.as_bytes())
            .await
            .map_err(|e| BackendError::Other(e.into()))?
            .ok_or_else(|| BackendError::ObjectNotFound {
                object_type: "commit".into(),
                hash: id.hex(),
                source: Some("commit not found in server store".into()),
            })?;

        validate_and_canonicalize_commit_fields(&mut proto_commit)
            .map_err(|e| BackendError::Other(format!("stored commit {}: {e}", id.hex()).into()))?;

        Ok(commit_from_proto(proto_commit))
    }

    async fn write_commit(
        &self,
        commit: Commit,
        _sign_with: Option<&mut SigningFn>,
    ) -> BackendResult<(CommitId, Commit)> {
        let mut proto_commit = commit_to_proto(&commit);
        let commit_id_bytes =
            compute_git_commit_hash(&proto_commit).map_err(|e| BackendError::Other(e.into()))?;
        proto_commit.commit_id = commit_id_bytes.clone();
        self.store
            .put_commit(
                self.project_id.clone(),
                commit_id_bytes.clone(),
                proto_commit,
            )
            .await
            .map_err(|e| BackendError::Other(e.into()))?;
        Ok((CommitId::from_bytes(&commit_id_bytes), commit))
    }

    fn get_copy_records(
        &self,
        _paths: Option<&[RepoPathBuf]>,
        _root: &CommitId,
        _head: &CommitId,
    ) -> BackendResult<BoxStream<'_, BackendResult<CopyRecord>>> {
        Ok(stream::empty().boxed())
    }

    async fn gc(&self, _heads: &[CommitId], _keep_newer: SystemTime) -> BackendResult<()> {
        Ok(())
    }
}

pub struct ServerOpStore {
    store: Arc<dyn Store>,
    repo_id: String,
    root_operation_id: OperationId,
    abort_on_index_failure: Option<DeferredIndexFailure>,
}

impl Debug for ServerOpStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerOpStore")
            .field("repo_id", &self.repo_id)
            .finish()
    }
}

impl ServerOpStore {
    pub fn new(store: Arc<dyn Store>, repo_id: String) -> Self {
        Self {
            store,
            repo_id,
            root_operation_id: OperationId::from_bytes(&cc_common::ROOT_OPERATION_ID_BYTES),
            abort_on_index_failure: None,
        }
    }

    /// Aborts `write_view` and `write_operation` before mutating the store if `ServerIndex`
    /// recorded a deferred ancestry failure (such as walk-budget exhaustion or a missing commit).
    pub fn with_abort_on_index_failure(mut self, failure: DeferredIndexFailure) -> Self {
        self.abort_on_index_failure = Some(failure);
        self
    }

    fn pending_index_failure(&self) -> Option<ServerIndexFailure> {
        self.abort_on_index_failure
            .as_ref()
            .and_then(|slot| slot.lock().unwrap().clone())
    }
}

#[async_trait]
impl OpStore for ServerOpStore {
    fn name(&self) -> &str {
        "server_op_store"
    }

    fn root_operation_id(&self) -> &OperationId {
        &self.root_operation_id
    }

    async fn read_operation(&self, id: &OperationId) -> OpStoreResult<Operation> {
        if id.as_bytes() == cc_common::ROOT_OPERATION_ID_BYTES {
            let root_op = cc_common::op_store::Operation {
                view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
                parents: vec![],
                metadata: Some(cc_common::op_store::OperationMetadata {
                    start_time_millis: 0,
                    end_time_millis: 0,
                    description: "root()".to_string(),
                    hostname: "".to_string(),
                    username: "".to_string(),
                    is_snapshot: false,
                    workspace_name: None,
                    attributes: std::collections::HashMap::new(),
                }),
                commit_predecessors: vec![],
                commit_predecessors_set: true,
            };
            return Ok(operation_from_proto(root_op));
        }

        let op = self
            .store
            .get_operation(&self.repo_id, id.as_bytes())
            .await
            .map_err(|e| OpStoreError::Other(e.into()))?
            .ok_or_else(|| OpStoreError::ObjectNotFound {
                object_type: "operation".into(),
                hash: id.hex(),
                source: Some("operation not found in server store".into()),
            })?;

        Ok(operation_from_proto(op))
    }

    async fn write_operation(&self, operation: &Operation) -> OpStoreResult<OperationId> {
        if let Some(failure) = self.pending_index_failure() {
            return Err(OpStoreError::Other(Box::new(failure)));
        }
        let op_proto = operation_to_proto(operation);
        let op_id_bytes = hash_operation(&op_proto).map_err(|e| OpStoreError::Other(e.into()))?;

        self.store
            .put_operation(self.repo_id.clone(), op_id_bytes.clone(), op_proto)
            .await
            .map_err(|e| OpStoreError::Other(e.into()))?;

        Ok(OperationId::from_bytes(&op_id_bytes))
    }

    async fn read_view(&self, id: &ViewId) -> OpStoreResult<View> {
        if id.as_bytes() == cc_common::ROOT_VIEW_ID_BYTES {
            let root_view = cc_common::op_store::View {
                head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                wc_commit_ids: std::collections::HashMap::new(),
                local_bookmarks: std::collections::HashMap::new(),
                remote_bookmarks: std::collections::HashMap::new(),
            };
            return Ok(view_from_proto(root_view));
        }

        let view = self
            .store
            .get_view(&self.repo_id, id.as_bytes())
            .await
            .map_err(|e| OpStoreError::Other(e.into()))?
            .ok_or_else(|| OpStoreError::ObjectNotFound {
                object_type: "view".into(),
                hash: id.hex(),
                source: Some("view not found in server store".into()),
            })?;

        Ok(view_from_proto(view))
    }

    async fn write_view(&self, view: &View) -> OpStoreResult<ViewId> {
        if let Some(failure) = self.pending_index_failure() {
            return Err(OpStoreError::Other(Box::new(failure)));
        }
        let view_proto = view_to_proto(view);
        let view_id_bytes = hash_view(&view_proto).map_err(|e| OpStoreError::Other(e.into()))?;

        self.store
            .put_view(self.repo_id.clone(), view_id_bytes.clone(), view_proto)
            .await
            .map_err(|e| OpStoreError::Other(e.into()))?;

        Ok(ViewId::from_bytes(&view_id_bytes))
    }

    // TODO: Extend function to support partial prefix matching. Currently an open todo on the client side OpStore as well, see lib/src/cc_op_store.rs
    async fn resolve_operation_id_prefix(
        &self,
        prefix: &HexPrefix,
    ) -> OpStoreResult<PrefixResolution<OperationId>> {
        if prefix.matches(&self.root_operation_id) {
            Ok(PrefixResolution::SingleMatch(
                self.root_operation_id.clone(),
            ))
        } else {
            Ok(PrefixResolution::NoMatch)
        }
    }

    async fn gc(
        &self,
        _target_op_ids: &[OperationId],
        _keep_newer: SystemTime,
    ) -> OpStoreResult<()> {
        Ok(())
    }
}

pub struct ServerOpHeadsStore {
    store: Arc<dyn Store>,
    repo_id: String,
    expected_heads: Option<Vec<Vec<u8>>>,
    abort_on_index_failure: Option<DeferredIndexFailure>,
}

impl Debug for ServerOpHeadsStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerOpHeadsStore")
            .field("repo_id", &self.repo_id)
            .finish()
    }
}

pub struct ServerOpHeadsStoreNoLock;
impl OpHeadsStoreLock for ServerOpHeadsStoreNoLock {}

impl ServerOpHeadsStore {
    pub fn new(store: Arc<dyn Store>, repo_id: String) -> Self {
        Self {
            store,
            repo_id,
            expected_heads: None,
            abort_on_index_failure: None,
        }
    }

    /// Pins `get_op_heads` and `update_op_heads` CAS to a previously validated head snapshot so
    /// concurrent arrivals force a retry instead of bypassing server-side safety checks, and so
    /// ancestor-only head pruning passes exact-set CAS.
    pub fn with_expected_heads(
        store: Arc<dyn Store>,
        repo_id: String,
        expected_heads: Vec<Vec<u8>>,
    ) -> Self {
        Self {
            store,
            repo_id,
            expected_heads: Some(expected_heads),
            abort_on_index_failure: None,
        }
    }

    /// Aborts `update_op_heads` before publishing if `ServerIndex` recorded a deferred ancestry
    /// failure.
    pub fn with_abort_on_index_failure(mut self, failure: DeferredIndexFailure) -> Self {
        self.abort_on_index_failure = Some(failure);
        self
    }

    fn pending_index_failure(&self) -> Option<ServerIndexFailure> {
        self.abort_on_index_failure
            .as_ref()
            .and_then(|slot| slot.lock().unwrap().clone())
    }
}

#[async_trait]
impl OpHeadsStore for ServerOpHeadsStore {
    fn name(&self) -> &str {
        "server_op_heads_store"
    }

    async fn update_op_heads(
        &self,
        old_ids: &[OperationId],
        new_id: &OperationId,
    ) -> Result<(), OpHeadsStoreError> {
        if let Some(failure) = self.pending_index_failure() {
            return Err(OpHeadsStoreError::Write {
                new_op_id: new_id.clone(),
                source: Box::new(failure),
            });
        }
        let cas_expected: Vec<Vec<u8>> = match &self.expected_heads {
            Some(pinned) => pinned.clone(),
            None => old_ids.iter().map(|id| id.as_bytes().to_vec()).collect(),
        };
        let new_vec = new_id.as_bytes().to_vec();
        let target_id = new_id.clone();

        self.store
            .update_op_heads_compare_and_swap(self.repo_id.clone(), &cas_expected, new_vec)
            .await
            .map_err(|e| OpHeadsStoreError::Write {
                new_op_id: target_id,
                source: e.into(),
            })?;

        Ok(())
    }

    async fn get_op_heads(&self) -> Result<Vec<OperationId>, OpHeadsStoreError> {
        if let Some(pinned) = &self.expected_heads {
            return Ok(pinned.iter().map(|b| OperationId::from_bytes(b)).collect());
        }
        let heads = self
            .store
            .get_op_heads(&self.repo_id)
            .await
            .map_err(|e| OpHeadsStoreError::Read(e.into()))?;

        let head_ids: Vec<OperationId> = heads
            .unwrap_or_else(|| vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()])
            .into_iter()
            .map(|b| OperationId::from_bytes(&b))
            .collect();

        Ok(head_ids)
    }

    async fn lock(&self) -> Result<Box<dyn OpHeadsStoreLock + '_>, OpHeadsStoreError> {
        Ok(Box::new(ServerOpHeadsStoreNoLock))
    }
}

#[derive(Debug)]
pub struct EmptyRevset;

#[async_trait(?Send)]
impl Revset for EmptyRevset {
    fn stream<'a>(&self) -> LocalBoxStream<'a, Result<CommitId, RevsetEvaluationError>>
    where
        Self: 'a,
    {
        Box::pin(futures::stream::empty())
    }

    fn commit_change_ids<'a>(
        &self,
    ) -> LocalBoxStream<'a, Result<(CommitId, ChangeId), RevsetEvaluationError>>
    where
        Self: 'a,
    {
        Box::pin(futures::stream::empty())
    }

    fn stream_graph<'a>(
        &self,
    ) -> LocalBoxStream<'a, Result<GraphNode<CommitId>, RevsetEvaluationError>>
    where
        Self: 'a,
    {
        Box::pin(futures::stream::empty())
    }

    async fn is_empty(&self) -> Result<bool, RevsetEvaluationError> {
        Ok(true)
    }

    async fn count_estimate(&self) -> Result<(usize, Option<usize>), RevsetEvaluationError> {
        Ok((0, Some(0)))
    }

    fn containing_fn<'a>(&self) -> Box<RevsetContainingFn<'a>>
    where
        Self: 'a,
    {
        Box::new(|_id| Box::pin(async { Ok(false) }))
    }
}

/// Maximum number of commits visited during a single server-side commit-graph ancestry query
/// before delegating reconciliation back to the client.
pub const MAX_ANCESTRY_WALK_COMMITS: usize = 4_096;

/// Aggregate upper bound on distinct commits loaded into the per-reconciliation commit cache
/// across both precheck and `ServerIndex` walks.
pub const MAX_RECONCILE_COMMIT_CACHE_ENTRIES: usize = 16_384;

/// Aggregate upper bound on total commit BFS visits across all ancestry queries within a single
/// reconciliation attempt (prevents quadratic CPU blowup over already-cached commits).
pub const MAX_RECONCILE_TOTAL_COMMIT_VISITS: usize = 65_536;

/// Per-walk and aggregate commit-graph walk limits enforced during server-side reconciliation.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReconciliationWalkLimits {
    pub max_walk_commits: usize,
    pub max_cache_entries: usize,
    pub max_total_walk_visits: usize,
}

impl Default for ReconciliationWalkLimits {
    fn default() -> Self {
        Self {
            max_walk_commits: MAX_ANCESTRY_WALK_COMMITS,
            max_cache_entries: MAX_RECONCILE_COMMIT_CACHE_ENTRIES,
            max_total_walk_visits: MAX_RECONCILE_TOTAL_COMMIT_VISITS,
        }
    }
}

/// Per-reconciliation cache of `commit_id -> Option<parent_commit_ids>` (`None` caches a missing
/// commit lookup).
pub type CommitParentsCache = Arc<Mutex<HashMap<Vec<u8>, Option<Vec<Vec<u8>>>>>>;

/// Shared slot recording the first ancestry-resolution failure observed by [`ServerIndex`].
pub type DeferredIndexFailure = Arc<Mutex<Option<ServerIndexFailure>>>;

/// Failure returned or deferred by [`ServerIndex`] during bounded commit-graph walks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerIndexFailure {
    WalkBudgetExceeded { limit: usize },
    MissingCommit { commit_id: Vec<u8> },
    Store(String),
}

impl ServerIndexFailure {
    /// Returns true when the server should delegate reconciliation back to the client instead of
    /// failing with an internal error.
    pub fn requires_client_resolution(&self) -> bool {
        matches!(
            self,
            Self::WalkBudgetExceeded { .. } | Self::MissingCommit { .. }
        )
    }
}

impl fmt::Display for ServerIndexFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WalkBudgetExceeded { limit } => {
                write!(f, "commit ancestry walk exceeded budget of {limit} commits")
            }
            Self::MissingCommit { commit_id } => {
                write!(
                    f,
                    "commit {} not found in store during ancestry walk",
                    hex::encode(commit_id)
                )
            }
            Self::Store(msg) => write!(f, "store error during ancestry walk: {msg}"),
        }
    }
}

impl std::error::Error for ServerIndexFailure {}

struct ServerIndexState {
    store: Arc<dyn Store>,
    project_id: String,
    commit_cache: CommitParentsCache,
    unchanged_heads: Arc<HashSet<Vec<u8>>>,
    maximal_view_heads_union: Arc<HashSet<Vec<u8>>>,
    max_walk_commits: usize,
    max_cache_entries: usize,
    max_total_walk_visits: usize,
    total_walk_visits: Arc<AtomicUsize>,
    soft_fail_heads: bool,
    deferred_failure: DeferredIndexFailure,
}

/// Bounded commit-parent graph [`Index`], [`ReadonlyIndex`], and [`MutableIndex`] backed by
/// [`Store::get_commit`] and cached per reconciliation call.
#[derive(Clone)]
pub struct ServerIndex {
    state: Arc<ServerIndexState>,
}

impl Debug for ServerIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerIndex")
            .field("project_id", &self.state.project_id)
            .field("max_walk_commits", &self.state.max_walk_commits)
            .field("max_cache_entries", &self.state.max_cache_entries)
            .field("max_total_walk_visits", &self.state.max_total_walk_visits)
            .finish()
    }
}

impl ServerIndex {
    pub fn new(store: Arc<dyn Store>, project_id: String) -> Self {
        Self::with_max_walk_commits(store, project_id, MAX_ANCESTRY_WALK_COMMITS)
    }

    pub fn with_max_walk_commits(
        store: Arc<dyn Store>,
        project_id: String,
        max_walk_commits: usize,
    ) -> Self {
        Self::with_walk_limits(
            store,
            project_id,
            ReconciliationWalkLimits {
                max_walk_commits,
                max_cache_entries: MAX_RECONCILE_COMMIT_CACHE_ENTRIES.max(max_walk_commits),
                max_total_walk_visits: MAX_RECONCILE_TOTAL_COMMIT_VISITS.max(max_walk_commits),
            },
        )
    }

    pub(crate) fn with_walk_limits(
        store: Arc<dyn Store>,
        project_id: String,
        limits: ReconciliationWalkLimits,
    ) -> Self {
        Self {
            state: Arc::new(ServerIndexState {
                store,
                project_id,
                commit_cache: Arc::new(Mutex::new(HashMap::new())),
                unchanged_heads: Arc::new(HashSet::new()),
                maximal_view_heads_union: Arc::new(HashSet::new()),
                max_walk_commits: limits.max_walk_commits,
                max_cache_entries: limits.max_cache_entries,
                max_total_walk_visits: limits.max_total_walk_visits,
                total_walk_visits: Arc::new(AtomicUsize::new(0)),
                soft_fail_heads: false,
                deferred_failure: Arc::new(Mutex::new(None)),
            }),
        }
    }

    fn record_visit(&self) -> Result<(), ServerIndexFailure> {
        let prev = self.state.total_walk_visits.fetch_add(1, Ordering::Relaxed);
        if prev >= self.state.max_total_walk_visits {
            return Err(ServerIndexFailure::WalkBudgetExceeded {
                limit: self.state.max_total_walk_visits,
            });
        }
        Ok(())
    }

    fn record_failure(&self, failure: &ServerIndexFailure) {
        let mut guard = self.state.deferred_failure.lock().unwrap();
        if guard.is_none() {
            *guard = Some(failure.clone());
        }
    }

    fn existing_failure(&self) -> Option<ServerIndexFailure> {
        self.state.deferred_failure.lock().unwrap().clone()
    }

    async fn load_parent_ids(&self, commit_id: &[u8]) -> Result<Vec<Vec<u8>>, ServerIndexFailure> {
        if commit_id == cc_common::ROOT_COMMIT_ID_BYTES {
            return Ok(Vec::new());
        }
        {
            let cache = self.state.commit_cache.lock().unwrap();
            if let Some(cached) = cache.get(commit_id) {
                return match cached {
                    Some(parents) => Ok(parents.clone()),
                    None => Err(ServerIndexFailure::MissingCommit {
                        commit_id: commit_id.to_vec(),
                    }),
                };
            }
            if cache.len() >= self.state.max_cache_entries {
                return Err(ServerIndexFailure::WalkBudgetExceeded {
                    limit: self.state.max_cache_entries,
                });
            }
        }

        let fetched = self
            .state
            .store
            .get_commit(&self.state.project_id, commit_id)
            .await
            .map_err(|e| ServerIndexFailure::Store(e.to_string()))?;

        match fetched {
            Some(commit) => {
                let parents = commit.parent_commit_ids;
                self.state
                    .commit_cache
                    .lock()
                    .unwrap()
                    .insert(commit_id.to_vec(), Some(parents.clone()));
                Ok(parents)
            }
            None => {
                self.state
                    .commit_cache
                    .lock()
                    .unwrap()
                    .insert(commit_id.to_vec(), None);
                Err(ServerIndexFailure::MissingCommit {
                    commit_id: commit_id.to_vec(),
                })
            }
        }
    }

    async fn is_ancestor_inner(
        &self,
        ancestor_id: &CommitId,
        descendant_id: &CommitId,
    ) -> Result<bool, ServerIndexFailure> {
        if let Some(prev) = self.existing_failure() {
            return Err(prev);
        }
        if ancestor_id == descendant_id {
            return Ok(true);
        }
        if ancestor_id.as_bytes() == cc_common::ROOT_COMMIT_ID_BYTES {
            return Ok(true);
        }
        if descendant_id.as_bytes() == cc_common::ROOT_COMMIT_ID_BYTES {
            return Ok(false);
        }

        let target = ancestor_id.as_bytes();
        let mut visited: HashSet<Vec<u8>> = HashSet::new();
        let mut queue: VecDeque<Vec<u8>> = VecDeque::new();

        let start = descendant_id.as_bytes().to_vec();
        visited.insert(start.clone());
        if visited.len() > self.state.max_walk_commits {
            return Err(ServerIndexFailure::WalkBudgetExceeded {
                limit: self.state.max_walk_commits,
            });
        }
        self.record_visit()?;
        queue.push_back(start);

        while let Some(current) = queue.pop_front() {
            let parents = self.load_parent_ids(&current).await?;
            for parent in parents {
                if parent.as_slice() == target {
                    return Ok(true);
                }
                if parent.as_slice() == cc_common::ROOT_COMMIT_ID_BYTES {
                    continue;
                }
                if visited.insert(parent.clone()) {
                    if visited.len() > self.state.max_walk_commits {
                        return Err(ServerIndexFailure::WalkBudgetExceeded {
                            limit: self.state.max_walk_commits,
                        });
                    }
                    self.record_visit()?;
                    queue.push_back(parent);
                }
            }
        }

        Ok(false)
    }

    async fn heads_inner(
        &self,
        unique_candidates: &[CommitId],
        candidate_set: &HashSet<Vec<u8>>,
    ) -> Result<Vec<CommitId>, ServerIndexFailure> {
        if let Some(prev) = self.existing_failure() {
            return Err(prev);
        }

        let mut visited: HashSet<Vec<u8>> = HashSet::new();
        let mut strict_ancestors: HashSet<Vec<u8>> = HashSet::new();
        let mut queue: VecDeque<Vec<u8>> = VecDeque::new();

        // Commits present in every maximal divergent op head's view already form an antichain with
        // every commit from those views when all views are normalized. Skip seeding unchanged heads
        // only when every candidate belongs to `maximal_view_heads_union`; whenever any candidate
        // falls outside that union (e.g. an interior ancestor injected by a bookmark or
        // working-copy update during `MutableRepo::merge_view`), seed and walk all candidates so
        // strict ancestors of unchanged heads are still pruned.
        let skip_unchanged = !self.state.unchanged_heads.is_empty()
            && unique_candidates
                .iter()
                .all(|cid| self.state.maximal_view_heads_union.contains(cid.as_bytes()));
        for cid in unique_candidates {
            let bytes = cid.as_bytes();
            if skip_unchanged && self.state.unchanged_heads.contains(bytes) {
                continue;
            }
            let owned = bytes.to_vec();
            if visited.insert(owned.clone()) {
                if visited.len() > self.state.max_walk_commits {
                    return Err(ServerIndexFailure::WalkBudgetExceeded {
                        limit: self.state.max_walk_commits,
                    });
                }
                self.record_visit()?;
                queue.push_back(owned);
            }
        }

        while let Some(current) = queue.pop_front() {
            let parents = self.load_parent_ids(&current).await?;
            for parent in parents {
                if parent.as_slice() == cc_common::ROOT_COMMIT_ID_BYTES {
                    continue;
                }
                if parent != current && candidate_set.contains(&parent) {
                    strict_ancestors.insert(parent.clone());
                    if unique_candidates
                        .len()
                        .saturating_sub(strict_ancestors.len())
                        <= 1
                    {
                        queue.clear();
                        break;
                    }
                }
                if visited.insert(parent.clone()) {
                    if visited.len() > self.state.max_walk_commits {
                        return Err(ServerIndexFailure::WalkBudgetExceeded {
                            limit: self.state.max_walk_commits,
                        });
                    }
                    self.record_visit()?;
                    queue.push_back(parent);
                }
            }
        }

        let result: Vec<CommitId> = unique_candidates
            .iter()
            .filter(|cid| !strict_ancestors.contains(cid.as_bytes()))
            .cloned()
            .collect();
        if result.is_empty() && !unique_candidates.is_empty() {
            return Err(ServerIndexFailure::Store(
                "commit parent graph contains a cycle among candidate heads".to_string(),
            ));
        }
        Ok(result)
    }
}

#[async_trait]
impl Index for ServerIndex {
    async fn shortest_unique_commit_id_prefix_len(
        &self,
        _commit_id: &CommitId,
    ) -> IndexResult<usize> {
        Ok(12)
    }

    async fn resolve_commit_id_prefix(
        &self,
        _prefix: &HexPrefix,
    ) -> IndexResult<PrefixResolution<CommitId>> {
        Ok(PrefixResolution::NoMatch)
    }

    async fn has_id(&self, _commit_id: &CommitId) -> IndexResult<bool> {
        Ok(false)
    }

    async fn is_ancestor(
        &self,
        ancestor_id: &CommitId,
        descendant_id: &CommitId,
    ) -> IndexResult<bool> {
        match self.is_ancestor_inner(ancestor_id, descendant_id).await {
            Ok(is_anc) => Ok(is_anc),
            Err(failure) => {
                self.record_failure(&failure);
                Err(IndexError::Other(Box::new(failure)))
            }
        }
    }

    async fn common_ancestors(
        &self,
        _set1: &[CommitId],
        _set2: &[CommitId],
    ) -> IndexResult<Vec<CommitId>> {
        Ok(vec![])
    }

    fn all_heads_for_gc(&self) -> IndexResult<Box<dyn Iterator<Item = CommitId> + '_>> {
        Err(IndexError::AllHeadsForGcUnsupported)
    }

    async fn heads(
        &self,
        candidates: &mut (dyn Iterator<Item = &CommitId> + Send),
    ) -> IndexResult<Vec<CommitId>> {
        let mut unique_candidates: Vec<CommitId> = Vec::new();
        let mut candidate_set: HashSet<Vec<u8>> = HashSet::new();
        for cid in candidates {
            if candidate_set.insert(cid.as_bytes().to_vec()) {
                unique_candidates.push(cid.clone());
            }
        }
        if unique_candidates.len() <= 1 {
            return Ok(unique_candidates);
        }
        if candidate_set.remove(cc_common::ROOT_COMMIT_ID_BYTES.as_slice()) {
            unique_candidates.retain(|cid| cid.as_bytes() != cc_common::ROOT_COMMIT_ID_BYTES);
            if unique_candidates.len() <= 1 {
                return Ok(unique_candidates);
            }
        }

        match self.heads_inner(&unique_candidates, &candidate_set).await {
            Ok(pruned) => Ok(pruned),
            Err(failure) => {
                self.record_failure(&failure);
                if self.state.soft_fail_heads {
                    // Defense in depth: return unpruned candidates so no `jj_lib` view-merge
                    // caller can panic on an `IndexError` from `heads()` (`View::normalize_heads`
                    // propagates `IndexResult`), while `ServerOpStore` and `ServerOpHeadsStore`
                    // still abort via `deferred_failure` before persisting any unpruned state.
                    Ok(unique_candidates)
                } else {
                    Err(IndexError::Other(Box::new(failure)))
                }
            }
        }
    }

    async fn changed_paths_in_commit(
        &self,
        _commit_id: &CommitId,
    ) -> IndexResult<Option<Box<dyn Iterator<Item = RepoPathBuf> + '_>>> {
        Ok(None)
    }

    fn evaluate_revset(
        &self,
        _expression: &ResolvedExpression,
        _store: &Arc<JjStore>,
    ) -> Result<Box<dyn Revset>, RevsetEvaluationError> {
        Ok(Box::new(EmptyRevset))
    }
}

pub struct ServerChangeIdIndex;

#[async_trait]
impl ChangeIdIndex for ServerChangeIdIndex {
    async fn resolve_prefix(
        &self,
        _prefix: &HexPrefix,
    ) -> IndexResult<PrefixResolution<ResolvedChangeTargets>> {
        Ok(PrefixResolution::NoMatch)
    }

    async fn shortest_unique_prefix_len(&self, _change_id: &ChangeId) -> IndexResult<usize> {
        Ok(12)
    }
}

impl ReadonlyIndex for ServerIndex {
    fn as_index(&self) -> &dyn Index {
        self
    }

    fn change_id_index(
        &self,
        _heads: &mut dyn Iterator<Item = &CommitId>,
    ) -> IndexResult<Box<dyn ChangeIdIndex>> {
        Ok(Box::new(ServerChangeIdIndex))
    }

    fn start_modification(&self) -> Box<dyn MutableIndex> {
        Box::new(self.clone())
    }
}

#[async_trait]
impl MutableIndex for ServerIndex {
    fn as_index(&self) -> &dyn Index {
        self
    }

    fn change_id_index(
        &self,
        _heads: &mut dyn Iterator<Item = &CommitId>,
    ) -> IndexResult<Box<dyn ChangeIdIndex + '_>> {
        Ok(Box::new(ServerChangeIdIndex))
    }

    async fn add_commit(&mut self, commit: &JjCommit) -> IndexResult<()> {
        let parents = commit
            .parent_ids()
            .iter()
            .map(|id| id.as_bytes().to_vec())
            .collect();
        self.state
            .commit_cache
            .lock()
            .unwrap()
            .insert(commit.id().as_bytes().to_vec(), Some(parents));
        Ok(())
    }

    fn merge_in(&mut self, _other: &dyn ReadonlyIndex) -> IndexResult<()> {
        Ok(())
    }
}

/// In-memory [`IndexStore`] for server-side operations backed by [`Store::get_commit`].
pub struct ServerIndexStore {
    state: Arc<ServerIndexState>,
}

impl Debug for ServerIndexStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerIndexStore")
            .field("project_id", &self.state.project_id)
            .field("max_walk_commits", &self.state.max_walk_commits)
            .finish()
    }
}

impl ServerIndexStore {
    pub fn new(store: Arc<dyn Store>, project_id: String) -> Self {
        Self {
            state: Arc::new(ServerIndexState {
                store,
                project_id,
                commit_cache: Arc::new(Mutex::new(HashMap::new())),
                unchanged_heads: Arc::new(HashSet::new()),
                maximal_view_heads_union: Arc::new(HashSet::new()),
                max_walk_commits: MAX_ANCESTRY_WALK_COMMITS,
                max_cache_entries: MAX_RECONCILE_COMMIT_CACHE_ENTRIES,
                max_total_walk_visits: MAX_RECONCILE_TOTAL_COMMIT_VISITS,
                total_walk_visits: Arc::new(AtomicUsize::new(0)),
                soft_fail_heads: false,
                deferred_failure: Arc::new(Mutex::new(None)),
            }),
        }
    }

    /// Creates a reconciliation-scoped [`ServerIndexStore`] sharing `commit_cache` and returning
    /// a [`DeferredIndexFailure`] slot that aborts store writes before any unpruned view or
    /// operation can be persisted.
    pub fn for_reconciliation(
        store: Arc<dyn Store>,
        project_id: String,
        commit_cache: CommitParentsCache,
        unchanged_heads: Arc<HashSet<Vec<u8>>>,
        maximal_view_heads_union: Arc<HashSet<Vec<u8>>>,
        max_walk_commits: usize,
    ) -> (Self, DeferredIndexFailure) {
        Self::for_reconciliation_with_limits(
            store,
            project_id,
            commit_cache,
            unchanged_heads,
            maximal_view_heads_union,
            ReconciliationWalkLimits {
                max_walk_commits,
                max_cache_entries: MAX_RECONCILE_COMMIT_CACHE_ENTRIES.max(max_walk_commits),
                max_total_walk_visits: MAX_RECONCILE_TOTAL_COMMIT_VISITS.max(max_walk_commits),
            },
            0,
        )
    }

    /// Creates a reconciliation-scoped [`ServerIndexStore`] with explicit per-walk and aggregate
    /// cache/visit budgets seeded with `initial_walk_visits` already spent during precheck.
    pub(crate) fn for_reconciliation_with_limits(
        store: Arc<dyn Store>,
        project_id: String,
        commit_cache: CommitParentsCache,
        unchanged_heads: Arc<HashSet<Vec<u8>>>,
        maximal_view_heads_union: Arc<HashSet<Vec<u8>>>,
        limits: ReconciliationWalkLimits,
        initial_walk_visits: usize,
    ) -> (Self, DeferredIndexFailure) {
        let deferred_failure = Arc::new(Mutex::new(None));
        let index_store = Self {
            state: Arc::new(ServerIndexState {
                store,
                project_id,
                commit_cache,
                unchanged_heads,
                maximal_view_heads_union,
                max_walk_commits: limits.max_walk_commits,
                max_cache_entries: limits.max_cache_entries,
                max_total_walk_visits: limits.max_total_walk_visits,
                total_walk_visits: Arc::new(AtomicUsize::new(initial_walk_visits)),
                soft_fail_heads: true,
                deferred_failure: deferred_failure.clone(),
            }),
        };
        (index_store, deferred_failure)
    }

    fn index(&self) -> ServerIndex {
        ServerIndex {
            state: self.state.clone(),
        }
    }
}

#[async_trait(?Send)]
impl IndexStore for ServerIndexStore {
    fn name(&self) -> &str {
        "server"
    }

    async fn get_index_at_op(
        &self,
        _op: &JjOperation,
        _store: &Arc<JjStore>,
    ) -> IndexStoreResult<Box<dyn ReadonlyIndex>> {
        Ok(Box::new(self.index()))
    }

    fn write_index(
        &self,
        _index: Box<dyn MutableIndex>,
        _op: &JjOperation,
    ) -> IndexStoreResult<Box<dyn ReadonlyIndex>> {
        Ok(Box::new(self.index()))
    }
}

/// In-memory [`SubmoduleStore`] for server-side operations with no filesystem dependencies.
#[derive(Debug, Default)]
pub struct ServerSubmoduleStore;

impl SubmoduleStore for ServerSubmoduleStore {
    fn name(&self) -> &str {
        "server"
    }
}
/// No-op [`WorkspaceStore`] for server-side operations. The server never checks
/// out workspaces on disk, so there are no workspace paths to record.
#[derive(Debug, Default)]
pub struct ServerWorkspaceStore;

impl WorkspaceStore for ServerWorkspaceStore {
    fn name(&self) -> &str {
        "server"
    }

    fn add(
        &self,
        _workspace_name: &WorkspaceName,
        _path: &Path,
    ) -> Result<(), WorkspaceStoreError> {
        Ok(())
    }

    fn forget(&self, _workspace_names: &[&WorkspaceName]) -> Result<(), WorkspaceStoreError> {
        Ok(())
    }

    fn rename(
        &self,
        _old_name: &WorkspaceName,
        _new_name: &WorkspaceName,
    ) -> Result<(), WorkspaceStoreError> {
        Ok(())
    }

    fn get_workspace_path(
        &self,
        _workspace_name: &WorkspaceName,
    ) -> Result<Option<PathBuf>, WorkspaceStoreError> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryStore;

    #[tokio::test]
    async fn test_server_backend_write_commit_sets_commit_id_and_read_commit_rejects_corrupt_rows()
    {
        let store = Arc::new(MemoryStore::new());
        store
            .register_project("proj-adapter".to_string(), None)
            .await
            .unwrap();
        let backend = ServerBackend::new(store.clone(), "proj-adapter".to_string());

        let commit = Commit {
            parents: vec![CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES)],
            predecessors: Vec::new(),
            root_tree: jj_lib::merge::Merge::resolved(TreeId::from_bytes(
                &cc_common::EMPTY_TREE_ID_BYTES,
            )),
            conflict_labels: jj_lib::merge::Merge::resolved(String::new()),
            change_id: ChangeId::from_bytes(&[0x33; cc_common::CHANGE_ID_LENGTH]),
            description: "adapter roundtrip".to_string(),
            author: Signature {
                name: "Alice".to_string(),
                email: "alice@example.com".to_string(),
                timestamp: Timestamp {
                    timestamp: MillisSinceEpoch(1_700_000_000_000),
                    tz_offset: 0,
                },
            },
            committer: Signature {
                name: "Alice".to_string(),
                email: "alice@example.com".to_string(),
                timestamp: Timestamp {
                    timestamp: MillisSinceEpoch(1_700_000_000_000),
                    tz_offset: 0,
                },
            },
            secure_sig: None,
        };
        let (written_id, _) = backend.write_commit(commit.clone(), None).await.unwrap();
        let stored = store
            .get_commit("proj-adapter", written_id.as_bytes())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.commit_id, written_id.as_bytes());
        let read_back = backend.read_commit(&written_id).await.unwrap();
        assert_eq!(read_back.description, commit.description);

        // Legacy all-empty per-tree conflict_labels (`["", "", ""]`) on a 3-tree conflicted
        // commit must succeed and canonicalize to unlabeled (`Merge::resolved("")`).
        let legacy_conflict_id = vec![0xc0; cc_common::COMMIT_ID_LENGTH];
        store
            .put_commit(
                "proj-adapter".to_string(),
                legacy_conflict_id.clone(),
                cc_common::backend::Commit {
                    commit_id: legacy_conflict_id.clone(),
                    change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
                    parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                    root_tree_id: vec![
                        vec![0x11; cc_common::TREE_ID_LENGTH],
                        vec![0x22; cc_common::TREE_ID_LENGTH],
                        vec![0x33; cc_common::TREE_ID_LENGTH],
                    ],
                    conflict_labels: vec![String::new(), String::new(), String::new()],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let legacy_read = backend
            .read_commit(&CommitId::from_bytes(&legacy_conflict_id))
            .await
            .expect("legacy all-empty per-tree conflict_labels must succeed");
        assert_eq!(
            legacy_read.conflict_labels,
            jj_lib::merge::Merge::resolved(String::new())
        );

        // Corrupt rows must return BackendError::Other instead of panicking in commit_from_proto.
        let corrupt_cases = [
            // Empty root_tree_id
            cc_common::backend::Commit {
                change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
                parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: Vec::new(),
                ..Default::default()
            },
            // Even-length root_tree_id
            cc_common::backend::Commit {
                change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
                parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: vec![
                    cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                    cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                ],
                ..Default::default()
            },
            // Wrong-length tree ID
            cc_common::backend::Commit {
                change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
                parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: vec![vec![0xaa; 19]],
                ..Default::default()
            },
            // Even-length conflict_labels
            cc_common::backend::Commit {
                change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
                parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
                conflict_labels: vec!["a".to_string(), "b".to_string()],
                ..Default::default()
            },
            // Wrong-length all-empty conflict_labels on 3-tree commit
            cc_common::backend::Commit {
                change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
                parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: vec![
                    cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                    cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                    cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                ],
                conflict_labels: vec![String::new(), String::new()],
                ..Default::default()
            },
            // Wrong-length mixed conflict_labels on 3-tree commit
            cc_common::backend::Commit {
                change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
                parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: vec![
                    cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                    cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                    cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                ],
                conflict_labels: vec!["left".to_string(), String::new()],
                ..Default::default()
            },
            // Non-empty conflict label on resolved commit
            cc_common::backend::Commit {
                change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
                parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
                conflict_labels: vec!["unexpected".to_string()],
                ..Default::default()
            },
            // Newline inside conflict label
            cc_common::backend::Commit {
                change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
                parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: vec![
                    cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                    cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                    cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                ],
                conflict_labels: vec!["a\nb".to_string(), "base".to_string(), "c".to_string()],
                ..Default::default()
            },
            // Wrong-length change_id
            cc_common::backend::Commit {
                change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH - 1],
                parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
                ..Default::default()
            },
            // Empty parent_commit_ids on non-root commit
            cc_common::backend::Commit {
                change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
                parent_commit_ids: Vec::new(),
                root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
                ..Default::default()
            },
            // Wrong-length parent_commit_id
            cc_common::backend::Commit {
                change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
                parent_commit_ids: vec![vec![0xaa; cc_common::COMMIT_ID_LENGTH - 1]],
                root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
                ..Default::default()
            },
            // Mixed ROOT_COMMIT_ID with another parent
            cc_common::backend::Commit {
                change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
                parent_commit_ids: vec![
                    cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
                    vec![0xaa; cc_common::COMMIT_ID_LENGTH],
                ],
                root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
                ..Default::default()
            },
            // Wrong-length predecessor
            cc_common::backend::Commit {
                change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
                parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
                predecessors: vec![vec![0xbb; cc_common::COMMIT_ID_LENGTH - 1]],
                ..Default::default()
            },
        ];

        for (i, bad_commit) in corrupt_cases.into_iter().enumerate() {
            let bad_id_bytes = vec![0xd0 + (i as u8); cc_common::COMMIT_ID_LENGTH];
            store
                .put_commit("proj-adapter".to_string(), bad_id_bytes.clone(), bad_commit)
                .await
                .unwrap();
            let err = backend
                .read_commit(&CommitId::from_bytes(&bad_id_bytes))
                .await
                .unwrap_err();
            assert!(
                matches!(err, BackendError::Other(_)),
                "corrupt case {i} should return BackendError::Other, got {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_server_index_is_ancestor_and_heads_walk_and_bounds() {
        let store = Arc::new(MemoryStore::new());
        let project_id = "proj-index";
        store
            .register_project(project_id.to_string(), None)
            .await
            .unwrap();

        let root = CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES);
        let c0_bytes = vec![0x10; cc_common::COMMIT_ID_LENGTH];
        let c1_bytes = vec![0x11; cc_common::COMMIT_ID_LENGTH];
        let c2_bytes = vec![0x12; cc_common::COMMIT_ID_LENGTH];
        let c_side_bytes = vec![0x13; cc_common::COMMIT_ID_LENGTH];
        let c_merge_bytes = vec![0x14; cc_common::COMMIT_ID_LENGTH];
        let c_missing_bytes = vec![0x15; cc_common::COMMIT_ID_LENGTH];
        let c_dangling_bytes = vec![0x16; cc_common::COMMIT_ID_LENGTH];

        for (cid, parents) in [
            (&c0_bytes, vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()]),
            (&c1_bytes, vec![c0_bytes.clone()]),
            (&c2_bytes, vec![c1_bytes.clone()]),
            (
                &c_side_bytes,
                vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
            ),
            (&c_merge_bytes, vec![c1_bytes.clone(), c_side_bytes.clone()]),
            (&c_dangling_bytes, vec![c_missing_bytes.clone()]),
        ] {
            store
                .put_commit(
                    project_id.to_string(),
                    cid.clone(),
                    cc_common::backend::Commit {
                        commit_id: cid.clone(),
                        parent_commit_ids: parents,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
        }

        let c0 = CommitId::from_bytes(&c0_bytes);
        let c1 = CommitId::from_bytes(&c1_bytes);
        let c2 = CommitId::from_bytes(&c2_bytes);
        let c_side = CommitId::from_bytes(&c_side_bytes);
        let c_merge = CommitId::from_bytes(&c_merge_bytes);
        let c_missing = CommitId::from_bytes(&c_missing_bytes);
        let c_dangling = CommitId::from_bytes(&c_dangling_bytes);

        let index = ServerIndex::new(store.clone(), project_id.to_string());

        // ROOT_COMMIT_ID and reflexive ancestry invariants.
        assert!(index.is_ancestor(&root, &root).await.unwrap());
        assert!(index.is_ancestor(&root, &c0).await.unwrap());
        assert!(index.is_ancestor(&root, &c2).await.unwrap());
        assert!(!index.is_ancestor(&c0, &root).await.unwrap());
        assert!(index.is_ancestor(&c1, &c1).await.unwrap());

        // Multi-hop linear and merge-commit ancestry.
        assert!(index.is_ancestor(&c0, &c1).await.unwrap());
        assert!(index.is_ancestor(&c0, &c2).await.unwrap());
        assert!(index.is_ancestor(&c1, &c2).await.unwrap());
        assert!(!index.is_ancestor(&c2, &c0).await.unwrap());
        assert!(!index.is_ancestor(&c_side, &c2).await.unwrap());
        assert!(index.is_ancestor(&c0, &c_merge).await.unwrap());
        assert!(index.is_ancestor(&c_side, &c_merge).await.unwrap());
        assert!(!index.is_ancestor(&c2, &c_merge).await.unwrap());

        // `heads` prunes strict ancestors, prunes ROOT_COMMIT_ID, and deduplicates.
        assert_eq!(
            index
                .heads(&mut [&root, &c0, &c1, &c2, &c2].into_iter())
                .await
                .unwrap(),
            vec![c2.clone()]
        );
        assert_eq!(
            index
                .heads(&mut [&c0, &c_side, &c_merge, &c2].into_iter())
                .await
                .unwrap(),
            vec![c_merge.clone(), c2.clone()]
        );
        assert_eq!(
            index.heads(&mut [&root].into_iter()).await.unwrap(),
            vec![root.clone()]
        );

        // Missing commit returns an IndexError.
        let missing_index = ServerIndex::new(store.clone(), project_id.to_string());
        assert!(missing_index.is_ancestor(&c0, &c_missing).await.is_err());
        let missing_heads_index = ServerIndex::new(store.clone(), project_id.to_string());
        assert!(
            missing_heads_index
                .heads(&mut [&c_dangling, &c_side].into_iter())
                .await
                .is_err()
        );

        // Bounded walk returns WalkBudgetExceeded when exceeded.
        let tight_index =
            ServerIndex::with_max_walk_commits(store.clone(), project_id.to_string(), 2);
        assert!(tight_index.is_ancestor(&c_side, &c2).await.is_err());
        let tight_heads_index =
            ServerIndex::with_max_walk_commits(store.clone(), project_id.to_string(), 2);
        assert!(
            tight_heads_index
                .heads(&mut [&c2, &c_side].into_iter())
                .await
                .is_err()
        );

        // `for_reconciliation` skips unchanged heads when every candidate belongs to
        // `maximal_view_heads_union`, but seeds and walks unchanged heads whenever any candidate
        // falls outside that union.
        let (reconcile_store, deferred_failure) = ServerIndexStore::for_reconciliation(
            store.clone(),
            project_id.to_string(),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(HashSet::from([c_dangling_bytes.clone()])),
            Arc::new(HashSet::from([
                c_dangling_bytes.clone(),
                c_side_bytes.clone(),
            ])),
            MAX_ANCESTRY_WALK_COMMITS,
        );
        let reconcile_index = reconcile_store.index();
        assert_eq!(
            reconcile_index
                .heads(&mut [&c_dangling, &c_side].into_iter())
                .await
                .unwrap(),
            vec![c_dangling.clone(), c_side.clone()]
        );
        assert!(deferred_failure.lock().unwrap().is_none());

        // Adding an out-of-union interior ancestor `c0` alongside unchanged head `c1` forces
        // walking `c1` so `c0` is pruned.
        let (pruning_store, pruning_failure) = ServerIndexStore::for_reconciliation(
            store.clone(),
            project_id.to_string(),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(HashSet::from([c1_bytes.clone()])),
            Arc::new(HashSet::from([c1_bytes.clone(), c_side_bytes.clone()])),
            MAX_ANCESTRY_WALK_COMMITS,
        );
        assert_eq!(
            pruning_store
                .index()
                .heads(&mut [&c1, &c_side, &c0].into_iter())
                .await
                .unwrap(),
            vec![c1.clone(), c_side.clone()]
        );
        assert!(pruning_failure.lock().unwrap().is_none());

        // When forced to walk unchanged heads, `max_walk_commits` still bounds the walk and
        // records `WalkBudgetExceeded` in `deferred_failure` without panicking.
        let (budget_store, budget_failure) = ServerIndexStore::for_reconciliation(
            store.clone(),
            project_id.to_string(),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(HashSet::from([c2_bytes.clone()])),
            Arc::new(HashSet::from([c2_bytes, c_side_bytes])),
            2,
        );
        let _ = budget_store
            .index()
            .heads(&mut [&c2, &c_side, &c0].into_iter())
            .await
            .unwrap();
        assert_eq!(
            *budget_failure.lock().unwrap(),
            Some(ServerIndexFailure::WalkBudgetExceeded { limit: 2 })
        );

        // Aggregate `max_cache_entries` caps distinct commits loaded across multiple small calls.
        let cache_capped = ServerIndex::with_walk_limits(
            store.clone(),
            project_id.to_string(),
            ReconciliationWalkLimits {
                max_walk_commits: 10,
                max_cache_entries: 2,
                max_total_walk_visits: 100,
            },
        );
        // Walking `c1 -> c0` caches 2 entries (`c1` and `c0`), staying within budget.
        assert!(cache_capped.is_ancestor(&c0, &c1).await.unwrap());
        assert!(!cache_capped.is_ancestor(&c_side, &c1).await.unwrap());
        // Loading a 3rd distinct commit (`c2`) trips the aggregate cache cap.
        assert!(cache_capped.is_ancestor(&c_side, &c2).await.is_err());

        // Aggregate `max_total_walk_visits` caps repeated walks even over already-cached commits.
        let visit_capped = ServerIndex::with_walk_limits(
            store,
            project_id.to_string(),
            ReconciliationWalkLimits {
                max_walk_commits: 10,
                max_cache_entries: 100,
                max_total_walk_visits: 3,
            },
        );
        // First call visits `c1` and `c0` (2 visits); second call visits `c1` (3rd) and `c0` (4th).
        assert!(!visit_capped.is_ancestor(&c_side, &c1).await.unwrap());
        assert!(visit_capped.is_ancestor(&c_side, &c1).await.is_err());
    }
}
