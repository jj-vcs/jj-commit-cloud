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

//! `jj_lib::op_store::OpStore` client adapter over `OpStoreService` gRPC RPCs.
//!
//! # Authority and Crash-Safety Ordering
//! `write_view` persists a repository `View` before `write_operation` persists the
//! referencing `Operation`, and rejects views carrying unrepresented tags or colocated
//! Git refs rather than dropping them silently.
//!
//! # Cross-Process and RPC Boundaries
//! Validates digest lengths, non-empty `View.head_ids`, bookmark targets, and operation
//! parent invariants on gRPC responses, and maps `Code::NotFound` to
//! `OpStoreError::ObjectNotFound`.

use async_trait::async_trait;
use cc_common::op_store::op_store_service_client::OpStoreServiceClient;
use cc_common::op_store::*;
use jj_lib::backend::{CommitId, MillisSinceEpoch, Timestamp};
use jj_lib::object_id::{HexPrefix, ObjectId, PrefixResolution};
use jj_lib::op_store::{
    OpStore, OpStoreError, OpStoreResult, Operation, OperationId, OperationMetadata, RefTarget,
    RemoteRef, RemoteRefState, RemoteView, TimestampRange, View, ViewId,
};
use jj_lib::ref_name::{RefNameBuf, RemoteNameBuf, WorkspaceNameBuf};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Debug;
use std::path::Path;
use std::time::SystemTime;

use crate::util::{CommitCloudConfig, run_async};

fn ensure_op_digest(bytes: &[u8], expected: usize, field: &str) -> Result<(), String> {
    if bytes.len() != expected {
        return Err(format!(
            "invalid {field} length: expected {expected} bytes, got {} bytes",
            bytes.len()
        ));
    }
    Ok(())
}

fn ref_target_to_proto(target: &RefTarget) -> cc_common::op_store::RefTarget {
    let removes = target
        .removed_ids()
        .map(|id| cc_common::op_store::RefTargetTerm {
            commit_id: id.as_bytes().to_vec(),
        })
        .collect();
    let adds = target
        .added_ids()
        .map(|id| cc_common::op_store::RefTargetTerm {
            commit_id: id.as_bytes().to_vec(),
        })
        .collect();
    cc_common::op_store::RefTarget { removes, adds }
}

fn ref_target_from_proto(
    target: &cc_common::op_store::RefTarget,
    field: &str,
    require_present: bool,
) -> Result<RefTarget, String> {
    if require_present && target.removes.is_empty() && target.adds.is_empty() {
        return Err(format!("{field} RefTarget must not be absent"));
    }
    let mut removed_ids = Vec::with_capacity(target.removes.len());
    for t in &target.removes {
        ensure_op_digest(
            &t.commit_id,
            cc_common::COMMIT_ID_LENGTH,
            "RefTarget.removes.commit_id",
        )?;
        removed_ids.push(CommitId::from_bytes(&t.commit_id));
    }
    let mut added_ids = Vec::with_capacity(target.adds.len());
    for t in &target.adds {
        ensure_op_digest(
            &t.commit_id,
            cc_common::COMMIT_ID_LENGTH,
            "RefTarget.adds.commit_id",
        )?;
        added_ids.push(CommitId::from_bytes(&t.commit_id));
    }
    Ok(RefTarget::from_legacy_form(removed_ids, added_ids))
}

fn operation_to_proto(op: &Operation) -> cc_common::op_store::Operation {
    let metadata = cc_common::op_store::OperationMetadata {
        start_time_millis: op.metadata.time.start.timestamp.0,
        end_time_millis: op.metadata.time.end.timestamp.0,
        description: op.metadata.description.clone(),
        hostname: op.metadata.hostname.clone(),
        username: op.metadata.username.clone(),
        is_snapshot: op.metadata.is_snapshot,
        workspace_name: op
            .metadata
            .workspace_name
            .as_ref()
            .map(|w| w.as_str().to_string()),
        attributes: op
            .metadata
            .attributes
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    };
    let commit_predecessors = match &op.commit_predecessors {
        Some(map) => map
            .iter()
            .map(|(k, v)| cc_common::op_store::CommitPredecessors {
                commit_id: k.as_bytes().to_vec(),
                predecessor_ids: v.iter().map(|id| id.as_bytes().to_vec()).collect(),
            })
            .collect(),
        None => vec![],
    };

    cc_common::op_store::Operation {
        view_id: op.view_id.as_bytes().to_vec(),
        parents: op.parents.iter().map(|p| p.as_bytes().to_vec()).collect(),
        metadata: Some(metadata),
        commit_predecessors,
        commit_predecessors_set: op.commit_predecessors.is_some(),
    }
}

fn operation_from_proto(
    mut op: cc_common::op_store::Operation,
    is_root: bool,
) -> Result<Operation, String> {
    ensure_op_digest(&op.view_id, cc_common::VIEW_ID_LENGTH, "Operation.view_id")?;
    if !is_root && op.parents.is_empty() {
        return Err("non-root Operation.parents must not be empty".to_string());
    }
    let mut seen_parents = HashSet::with_capacity(op.parents.len());
    let mut has_root_parent = false;
    for p in &op.parents {
        ensure_op_digest(p, cc_common::OPERATION_ID_LENGTH, "Operation.parents")?;
        if !seen_parents.insert(p.as_slice()) {
            return Err("duplicate OperationId in Operation.parents".to_string());
        }
        if p.as_slice() == cc_common::ROOT_OPERATION_ID_BYTES {
            has_root_parent = true;
        }
    }
    if has_root_parent && op.parents.len() > 1 {
        return Err(
            "ROOT_OPERATION_ID may only appear as the sole entry in Operation.parents".to_string(),
        );
    }
    if !op.commit_predecessors_set && !op.commit_predecessors.is_empty() {
        return Err(
            "Operation.commit_predecessors must be empty when commit_predecessors_set is false"
                .to_string(),
        );
    }
    let meta = op.metadata.take().unwrap_or_default();
    let start_ts = Timestamp {
        timestamp: MillisSinceEpoch(meta.start_time_millis),
        tz_offset: 0,
    };
    let end_ts = Timestamp {
        timestamp: MillisSinceEpoch(meta.end_time_millis),
        tz_offset: 0,
    };
    let metadata = OperationMetadata {
        time: TimestampRange {
            start: start_ts,
            end: end_ts,
        },
        description: meta.description,
        hostname: meta.hostname,
        username: meta.username,
        is_snapshot: meta.is_snapshot,
        workspace_name: meta.workspace_name.map(WorkspaceNameBuf::from),
        attributes: meta.attributes.into_iter().collect(),
    };
    let commit_predecessors = if op.commit_predecessors_set {
        let mut map = BTreeMap::new();
        for p in op.commit_predecessors {
            ensure_op_digest(
                &p.commit_id,
                cc_common::COMMIT_ID_LENGTH,
                "CommitPredecessors.commit_id",
            )?;
            let mut val = Vec::with_capacity(p.predecessor_ids.len());
            for id in &p.predecessor_ids {
                ensure_op_digest(
                    id,
                    cc_common::COMMIT_ID_LENGTH,
                    "CommitPredecessors.predecessor_ids",
                )?;
                val.push(CommitId::from_bytes(id));
            }
            if map
                .insert(CommitId::from_bytes(&p.commit_id), val)
                .is_some()
            {
                return Err("duplicate commit_id in Operation.commit_predecessors".to_string());
            }
        }
        Some(map)
    } else {
        None
    };

    Ok(Operation {
        view_id: ViewId::from_bytes(&op.view_id),
        parents: op
            .parents
            .iter()
            .map(|p| OperationId::from_bytes(p))
            .collect(),
        metadata,
        commit_predecessors,
    })
}

fn view_to_proto(view: &View) -> Result<cc_common::op_store::View, String> {
    if !view.local_tags.is_empty() {
        return Err("local_tags are not yet supported by CommitCloudOpStore".to_string());
    }
    if view.remote_views.values().any(|rv| !rv.tags.is_empty()) {
        return Err("remote tags are not yet supported by CommitCloudOpStore".to_string());
    }
    if !view.git_refs.is_empty() || view.git_head.is_present() {
        return Err(
            "colocated git_refs/git_head are not supported by CommitCloudOpStore".to_string(),
        );
    }
    let head_ids = view
        .head_ids
        .iter()
        .map(|id| id.as_bytes().to_vec())
        .collect();
    let mut wc_commit_ids = HashMap::with_capacity(view.wc_commit_ids.len());
    for (k, v) in &view.wc_commit_ids {
        let ws_str = k.as_str();
        if ws_str.trim().is_empty() || ws_str.contains('\0') {
            return Err(
                "View.wc_commit_ids workspace name must be non-empty without NUL".to_string(),
            );
        }
        wc_commit_ids.insert(ws_str.to_string(), v.as_bytes().to_vec());
    }
    let mut local_bookmarks = HashMap::with_capacity(view.local_bookmarks.len());
    for (k, v) in &view.local_bookmarks {
        let bm_str = k.as_str();
        if bm_str.trim().is_empty() || bm_str.contains('\0') {
            return Err("View.local_bookmarks name must be non-empty without NUL".to_string());
        }
        local_bookmarks.insert(bm_str.to_string(), ref_target_to_proto(v));
    }
    let mut remote_bookmarks = HashMap::new();
    for (remote_name, remote_view) in &view.remote_views {
        let r_str = remote_name.as_str();
        if r_str.trim().is_empty() || r_str.contains('\0') || r_str.contains('@') {
            return Err(format!(
                "remote name {r_str:?} must be non-empty without '@' or NUL bytes"
            ));
        }
        for (bookmark_name, remote_ref) in &remote_view.bookmarks {
            let b_str = bookmark_name.as_str();
            if b_str.trim().is_empty() || b_str.contains('\0') {
                return Err("remote bookmark name must be non-empty without NUL bytes".to_string());
            }
            let key = format!("{b_str}@{r_str}");
            remote_bookmarks.insert(
                key,
                cc_common::op_store::RemoteRef {
                    target: Some(ref_target_to_proto(&remote_ref.target)),
                    is_tracked: remote_ref.state == RemoteRefState::Tracked,
                },
            );
        }
    }

    Ok(cc_common::op_store::View {
        head_ids,
        wc_commit_ids,
        local_bookmarks,
        remote_bookmarks,
    })
}

fn view_from_proto(view: cc_common::op_store::View) -> Result<View, String> {
    if view.head_ids.is_empty() {
        return Err("View.head_ids must contain at least one CommitId".to_string());
    }
    let mut head_ids = HashSet::with_capacity(view.head_ids.len());
    for id in &view.head_ids {
        ensure_op_digest(id, cc_common::COMMIT_ID_LENGTH, "View.head_ids")?;
        head_ids.insert(CommitId::from_bytes(id));
    }
    let mut wc_commit_ids = BTreeMap::new();
    for (k, v) in view.wc_commit_ids {
        if k.trim().is_empty() || k.contains('\0') {
            return Err(
                "View.wc_commit_ids workspace name must be non-empty without NUL".to_string(),
            );
        }
        ensure_op_digest(&v, cc_common::COMMIT_ID_LENGTH, "View.wc_commit_ids")?;
        wc_commit_ids.insert(WorkspaceNameBuf::from(k), CommitId::from_bytes(&v));
    }
    let mut local_bookmarks = BTreeMap::new();
    for (k, v) in &view.local_bookmarks {
        if k.trim().is_empty() || k.contains('\0') {
            return Err("View.local_bookmarks name must be non-empty without NUL".to_string());
        }
        local_bookmarks.insert(
            RefNameBuf::from(k.as_str()),
            ref_target_from_proto(v, "View.local_bookmarks", true)?,
        );
    }
    let mut remote_views: BTreeMap<RemoteNameBuf, RemoteView> = BTreeMap::new();
    for (key, remote_ref_proto) in &view.remote_bookmarks {
        let Some((b_str, r_str)) = key.rsplit_once('@') else {
            return Err(format!(
                "invalid View.remote_bookmarks key {key:?}: expected '<bookmark>@<remote>'"
            ));
        };
        if b_str.trim().is_empty() || r_str.trim().is_empty() || key.contains('\0') {
            return Err(format!(
                "invalid View.remote_bookmarks key {key:?}: expected non-empty '<bookmark>@<remote>'"
            ));
        }
        let target = match &remote_ref_proto.target {
            Some(t) => {
                ref_target_from_proto(t, "View.remote_bookmarks", !remote_ref_proto.is_tracked)?
            }
            None if !remote_ref_proto.is_tracked => {
                return Err(format!(
                    "untracked View.remote_bookmarks entry {key:?} must carry a non-absent target"
                ));
            }
            None => RefTarget::absent(),
        };
        let remote_ref = RemoteRef {
            target,
            state: if remote_ref_proto.is_tracked {
                RemoteRefState::Tracked
            } else {
                RemoteRefState::New
            },
        };
        remote_views
            .entry(RemoteNameBuf::from(r_str))
            .or_default()
            .bookmarks
            .insert(RefNameBuf::from(b_str), remote_ref);
    }

    Ok(View {
        head_ids,
        wc_commit_ids,
        local_bookmarks,
        local_tags: BTreeMap::new(),
        remote_views,
        git_refs: BTreeMap::new(),
        git_head: RefTarget::absent(),
    })
}

#[derive(Debug)]
pub struct CommitCloudOpStore {
    server_url: String,
    repo_id: String,
    root_operation_id: OperationId,
}

impl CommitCloudOpStore {
    pub fn name() -> &'static str {
        "commit_cloud"
    }

    pub fn load(store_path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config = CommitCloudConfig::load_from_store(store_path)?;
        Ok(Self {
            server_url: config.server_url,
            repo_id: config.repo_id,
            root_operation_id: OperationId::from_bytes(&cc_common::ROOT_OPERATION_ID_BYTES),
        })
    }
}

#[async_trait]
impl OpStore for CommitCloudOpStore {
    fn name(&self) -> &str {
        Self::name()
    }

    fn root_operation_id(&self) -> &OperationId {
        &self.root_operation_id
    }

    async fn read_view(&self, id: &ViewId) -> OpStoreResult<View> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let view_id_bytes = id.as_bytes().to_vec();
        let view_hex = id.hex();

        run_async(move || async move {
            let mut client = OpStoreServiceClient::connect(server_url).await?;
            let response = match client
                .read_view(ReadViewRequest {
                    repo_id,
                    view_id: view_id_bytes.clone(),
                })
                .await
            {
                Ok(r) => r,
                Err(status) if status.code() == tonic::Code::NotFound => {
                    return Err(Box::new(OpStoreError::ObjectNotFound {
                        object_type: "view".to_string(),
                        hash: view_hex,
                        source: status.into(),
                    })
                        as Box<dyn std::error::Error + Send + Sync>);
                }
                Err(status) => return Err(status.into()),
            };
            let inner = response.into_inner();
            if !inner.view_id.is_empty() && inner.view_id != view_id_bytes {
                return Err("ReadViewResponse view_id did not match requested view_id".into());
            }
            let proto_view = inner
                .view
                .ok_or_else(|| "server response should have contained a view".to_string())?;
            Ok(view_from_proto(proto_view)?)
        })
        .map_err(|e| match e.downcast::<OpStoreError>() {
            Ok(op_err) => *op_err,
            Err(other) => OpStoreError::Other(other),
        })
    }

    async fn write_view(&self, contents: &View) -> OpStoreResult<ViewId> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let proto_view = view_to_proto(contents).map_err(|e| OpStoreError::WriteObject {
            object_type: "view",
            source: e.into(),
        })?;

        run_async(move || async move {
            let mut client = OpStoreServiceClient::connect(server_url).await?;
            let response = client
                .write_view(WriteViewRequest {
                    repo_id,
                    view: Some(proto_view),
                })
                .await?;
            let view_id_bytes = response.into_inner().view_id;
            ensure_op_digest(
                &view_id_bytes,
                cc_common::VIEW_ID_LENGTH,
                "WriteViewResponse.view_id",
            )?;
            Ok(ViewId::from_bytes(&view_id_bytes))
        })
        .map_err(|e| OpStoreError::Other(e))
    }

    async fn read_operation(&self, id: &OperationId) -> OpStoreResult<Operation> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let op_id_bytes = id.as_bytes().to_vec();
        let op_hex = id.hex();
        let is_root = id == &self.root_operation_id;

        run_async(move || async move {
            let mut client = OpStoreServiceClient::connect(server_url).await?;
            let response = match client
                .read_operation(ReadOperationRequest {
                    repo_id,
                    operation_id: op_id_bytes,
                })
                .await
            {
                Ok(r) => r,
                Err(status) if status.code() == tonic::Code::NotFound => {
                    return Err(Box::new(OpStoreError::ObjectNotFound {
                        object_type: "operation".to_string(),
                        hash: op_hex,
                        source: status.into(),
                    })
                        as Box<dyn std::error::Error + Send + Sync>);
                }
                Err(status) => return Err(status.into()),
            };
            let proto_op = response
                .into_inner()
                .operation
                .ok_or_else(|| "server response should have contained an operation".to_string())?;
            Ok(operation_from_proto(proto_op, is_root)?)
        })
        .map_err(|e| match e.downcast::<OpStoreError>() {
            Ok(op_err) => *op_err,
            Err(other) => OpStoreError::Other(other),
        })
    }

    async fn write_operation(&self, contents: &Operation) -> OpStoreResult<OperationId> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let proto_op = operation_to_proto(contents);

        run_async(move || async move {
            let mut client = OpStoreServiceClient::connect(server_url).await?;
            let response = client
                .write_operation(WriteOperationRequest {
                    repo_id,
                    operation: Some(proto_op),
                })
                .await?;
            let op_id_bytes = response.into_inner().operation_id;
            ensure_op_digest(
                &op_id_bytes,
                cc_common::OPERATION_ID_LENGTH,
                "WriteOperationResponse.operation_id",
            )?;
            Ok(OperationId::from_bytes(&op_id_bytes))
        })
        .map_err(|e| OpStoreError::Other(e))
    }

    // TODO: Delegate to `OpStoreService::ResolveOperationPrefix` once wired on the server.
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

    async fn gc(&self, _head_ids: &[OperationId], _keep_newer: SystemTime) -> OpStoreResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn view_and_operation_proto_codecs_reject_unrepresented_tags_and_malformed_wire_data() {
        let mut jj_view = View::make_root(CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES));
        let encoded = view_to_proto(&jj_view).expect("clean root view serializes");
        let round_tripped = view_from_proto(encoded).expect("clean root view decodes");
        assert_eq!(round_tripped, jj_view);

        let mut bad_remote_view = jj_view.clone();
        bad_remote_view
            .remote_views
            .entry(RemoteNameBuf::from("fork@gh"))
            .or_default()
            .bookmarks
            .insert(
                RefNameBuf::from("main"),
                RemoteRef {
                    target: RefTarget::normal(CommitId::from_bytes(&[0x11; 20])),
                    state: RemoteRefState::Tracked,
                },
            );
        assert!(view_to_proto(&bad_remote_view).is_err());

        jj_view.local_tags.insert(
            RefNameBuf::from("v1.0"),
            RefTarget::normal(CommitId::from_bytes(&[0x11; 20])),
        );
        assert!(view_to_proto(&jj_view).is_err());

        assert!(view_from_proto(cc_common::op_store::View::default()).is_err());

        let mut bad_remote = HashMap::new();
        bad_remote.insert(
            "missing_at_sign".to_string(),
            cc_common::op_store::RemoteRef {
                target: None,
                is_tracked: true,
            },
        );
        assert!(
            view_from_proto(cc_common::op_store::View {
                head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                remote_bookmarks: bad_remote,
                ..Default::default()
            })
            .is_err()
        );

        // Non-root operation requires non-empty parents and forbids mixed ROOT_OPERATION_ID.
        let parentless = cc_common::op_store::Operation {
            view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
            parents: Vec::new(),
            commit_predecessors_set: true,
            ..Default::default()
        };
        assert!(operation_from_proto(parentless.clone(), false).is_err());
        assert!(operation_from_proto(parentless, true).is_ok());

        let mixed_parents = cc_common::op_store::Operation {
            view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
            parents: vec![
                cc_common::ROOT_OPERATION_ID_BYTES.to_vec(),
                vec![0x55; cc_common::OPERATION_ID_LENGTH],
            ],
            commit_predecessors_set: true,
            ..Default::default()
        };
        assert!(operation_from_proto(mixed_parents, false).is_err());
    }
}
