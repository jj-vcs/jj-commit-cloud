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

//! `jj_lib::op_store::OpStore` adapter over `OpStoreService` gRPC RPCs.
//!
//! # Authority and Crash-Safety Ordering
//! `write_view` persists a repository `View` snapshot before `write_operation` persists
//! the `Operation` referencing that `ViewId`. Neither mutation becomes active until
//! `CommitCloudOpHeadsStore::update_op_heads` advances the repository's operation heads.
//!
//! # Concurrency and Merge Invariants
//! Views and operations are immutable and content-addressed by 16-byte truncated SHA-1
//! digests computed server-side over key-sorted maps.
//!
//! # Cross-Process and RPC Boundaries
//! Resolves short operation hex prefixes via `ResolveOperationPrefix` so CLI commands
//! such as `jj op show <prefix>` execute server-side range scans without full log downloads.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Debug;
use std::path::Path;
use std::time::SystemTime;

use async_trait::async_trait;
use cc_common::op_store::*;
use jj_lib::backend::{CommitId, MillisSinceEpoch, Timestamp};
use jj_lib::object_id::{HexPrefix, ObjectId, PrefixResolution};
use jj_lib::op_store::{
    OpStore, OpStoreError, OpStoreResult, Operation, OperationId, OperationMetadata, RefTarget,
    RemoteRef, RemoteRefState, RemoteView, TimestampRange, View, ViewId,
};
use jj_lib::ref_name::{RefNameBuf, RemoteNameBuf, WorkspaceNameBuf};

use crate::util::{CommitCloudConfig, run_on_tokio};

fn parse_commit_id(bytes: &[u8], field: &str) -> OpStoreResult<CommitId> {
    if bytes.len() != cc_common::COMMIT_ID_LENGTH {
        return Err(OpStoreError::Other(
            format!(
                "{field} must be {} bytes (got {} bytes)",
                cc_common::COMMIT_ID_LENGTH,
                bytes.len()
            )
            .into(),
        ));
    }
    Ok(CommitId::from_bytes(bytes))
}

fn parse_operation_id(bytes: &[u8], field: &str) -> OpStoreResult<OperationId> {
    if bytes.len() != cc_common::OPERATION_ID_LENGTH {
        return Err(OpStoreError::Other(
            format!(
                "{field} must be {} bytes (got {} bytes)",
                cc_common::OPERATION_ID_LENGTH,
                bytes.len()
            )
            .into(),
        ));
    }
    Ok(OperationId::from_bytes(bytes))
}

fn parse_view_id(bytes: &[u8], field: &str) -> OpStoreResult<ViewId> {
    if bytes.len() != cc_common::VIEW_ID_LENGTH {
        return Err(OpStoreError::Other(
            format!(
                "{field} must be {} bytes (got {} bytes)",
                cc_common::VIEW_ID_LENGTH,
                bytes.len()
            )
            .into(),
        ));
    }
    Ok(ViewId::from_bytes(bytes))
}

fn map_op_store_err(err: Box<dyn std::error::Error + Send + Sync>) -> OpStoreError {
    match err.downcast::<OpStoreError>() {
        Ok(op_err) => *op_err,
        Err(other) => OpStoreError::Other(other),
    }
}

fn ref_target_to_proto(target: &RefTarget) -> cc_common::op_store::RefTarget {
    let removes = target
        .present_removes()
        .map(|id| cc_common::op_store::RefTargetTerm {
            commit_id: id.as_bytes().to_vec(),
        })
        .collect();
    let adds = target
        .present_adds()
        .map(|id| cc_common::op_store::RefTargetTerm {
            commit_id: id.as_bytes().to_vec(),
        })
        .collect();
    cc_common::op_store::RefTarget { removes, adds }
}

fn ref_target_from_proto(target: &cc_common::op_store::RefTarget) -> OpStoreResult<RefTarget> {
    let removed_ids = target
        .removes
        .iter()
        .map(|t| parse_commit_id(&t.commit_id, "RefTarget.removes.commit_id"))
        .collect::<OpStoreResult<Vec<_>>>()?;
    let added_ids = target
        .adds
        .iter()
        .map(|t| parse_commit_id(&t.commit_id, "RefTarget.adds.commit_id"))
        .collect::<OpStoreResult<Vec<_>>>()?;
    Ok(RefTarget::from_legacy_form(removed_ids, added_ids))
}

pub fn operation_to_proto(op: &Operation) -> cc_common::op_store::Operation {
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

pub fn operation_from_proto(mut op: cc_common::op_store::Operation) -> OpStoreResult<Operation> {
    let view_id = parse_view_id(&op.view_id, "Operation.view_id")?;
    let parents = op
        .parents
        .iter()
        .map(|p| parse_operation_id(p, "Operation.parents"))
        .collect::<OpStoreResult<Vec<_>>>()?;
    if parents.is_empty() && view_id.as_bytes() != cc_common::ROOT_VIEW_ID_BYTES {
        return Err(OpStoreError::Other(
            "non-root Operation.parents must not be empty".into(),
        ));
    }
    let mut seen_parents = HashSet::with_capacity(parents.len());
    let mut has_root_parent = false;
    for p in &parents {
        if !seen_parents.insert(p) {
            return Err(OpStoreError::Other(
                "Operation.parents must not contain duplicate parent IDs".into(),
            ));
        }
        if p.as_bytes() == cc_common::ROOT_OPERATION_ID_BYTES {
            has_root_parent = true;
        }
    }
    if has_root_parent && parents.len() > 1 {
        return Err(OpStoreError::Other(
            "ROOT_OPERATION_ID may only appear as the sole parent in Operation.parents".into(),
        ));
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
            let key = parse_commit_id(&p.commit_id, "Operation.commit_predecessors.commit_id")?;
            let val = p
                .predecessor_ids
                .iter()
                .map(|id| parse_commit_id(id, "Operation.commit_predecessors.predecessor_ids"))
                .collect::<OpStoreResult<Vec<_>>>()?;
            if map.insert(key.clone(), val).is_some() {
                return Err(OpStoreError::Other(
                    format!(
                        "Operation.commit_predecessors contains duplicate commit_id {}",
                        key.hex()
                    )
                    .into(),
                ));
            }
        }
        Some(map)
    } else if !op.commit_predecessors.is_empty() {
        return Err(OpStoreError::Other(
            "Operation.commit_predecessors must be empty when commit_predecessors_set is false"
                .into(),
        ));
    } else {
        None
    };

    Ok(Operation {
        view_id,
        parents,
        metadata,
        commit_predecessors,
    })
}

fn verified_operation_from_proto(
    id: &OperationId,
    op: cc_common::op_store::Operation,
) -> OpStoreResult<Operation> {
    if id.as_bytes() == cc_common::ROOT_OPERATION_ID_BYTES {
        if !op.parents.is_empty() {
            return Err(OpStoreError::Other(
                "ReadOperation root operation must not have parents".into(),
            ));
        }
        if op.view_id.as_slice() != cc_common::ROOT_VIEW_ID_BYTES {
            return Err(OpStoreError::Other(
                format!(
                    "ReadOperation root operation view_id {} must equal ROOT_VIEW_ID",
                    hex::encode(&op.view_id),
                )
                .into(),
            ));
        }
    } else if op.parents.is_empty() {
        return Err(OpStoreError::Other(
            format!(
                "ReadOperation non-root operation {} must have at least one parent",
                id.hex()
            )
            .into(),
        ));
    }
    operation_from_proto(op)
}

pub fn view_to_proto(view: &View) -> cc_common::op_store::View {
    let head_ids = view
        .head_ids
        .iter()
        .map(|id| id.as_bytes().to_vec())
        .collect();
    let wc_commit_ids = view
        .wc_commit_ids
        .iter()
        .map(|(k, v)| (k.as_str().to_string(), v.as_bytes().to_vec()))
        .collect();
    let local_bookmarks = view
        .local_bookmarks
        .iter()
        .map(|(k, v)| (k.as_str().to_string(), ref_target_to_proto(v)))
        .collect();
    let mut remote_bookmarks = HashMap::new();
    for (remote_name, remote_view) in &view.remote_views {
        for (bookmark_name, remote_ref) in &remote_view.bookmarks {
            let key = format!("{}@{}", bookmark_name.as_str(), remote_name.as_str());
            remote_bookmarks.insert(
                key,
                cc_common::op_store::RemoteRef {
                    target: Some(ref_target_to_proto(&remote_ref.target)),
                    is_tracked: remote_ref.state == RemoteRefState::Tracked,
                },
            );
        }
    }

    cc_common::op_store::View {
        head_ids,
        wc_commit_ids,
        local_bookmarks,
        remote_bookmarks,
    }
}

pub fn view_from_proto(view: cc_common::op_store::View) -> OpStoreResult<View> {
    if view.head_ids.is_empty() {
        return Err(OpStoreError::Other(
            "View.head_ids must not be empty".into(),
        ));
    }
    let head_ids = view
        .head_ids
        .iter()
        .map(|id| parse_commit_id(id, "View.head_ids"))
        .collect::<OpStoreResult<HashSet<_>>>()?;
    let wc_commit_ids = view
        .wc_commit_ids
        .into_iter()
        .map(|(k, v)| {
            if k.is_empty() {
                return Err(OpStoreError::Other(
                    "View.wc_commit_ids workspace name must not be empty".into(),
                ));
            }
            Ok((
                WorkspaceNameBuf::from(k),
                parse_commit_id(&v, "View.wc_commit_ids")?,
            ))
        })
        .collect::<OpStoreResult<_>>()?;
    let local_bookmarks = view
        .local_bookmarks
        .iter()
        .map(|(k, v)| {
            if k.is_empty() {
                return Err(OpStoreError::Other(
                    "View.local_bookmarks bookmark name must not be empty".into(),
                ));
            }
            let target = ref_target_from_proto(v)?;
            if target.simplify().is_absent() {
                return Err(OpStoreError::Other(
                    format!("View.local_bookmarks[{k:?}] must not have an absent RefTarget").into(),
                ));
            }
            Ok((RefNameBuf::from(k.as_str()), target))
        })
        .collect::<OpStoreResult<_>>()?;
    let mut remote_views: BTreeMap<RemoteNameBuf, RemoteView> = BTreeMap::new();
    for (key, remote_ref_proto) in &view.remote_bookmarks {
        let Some((b_str, r_str)) = key.rsplit_once('@') else {
            return Err(OpStoreError::Other(
                format!("View.remote_bookmarks key {key:?} must use '<bookmark>@<remote>' format")
                    .into(),
            ));
        };
        if b_str.is_empty() || r_str.is_empty() {
            return Err(OpStoreError::Other(
                format!(
                    "View.remote_bookmarks key {key:?} must have non-empty bookmark and remote names"
                )
                .into(),
            ));
        }
        let target_proto = remote_ref_proto.target.as_ref().ok_or_else(|| {
            OpStoreError::Other(
                format!("View.remote_bookmarks[{key:?}] must contain a RefTarget").into(),
            )
        })?;
        let target = ref_target_from_proto(target_proto)?;
        if !remote_ref_proto.is_tracked && target.simplify().is_absent() {
            return Err(OpStoreError::Other(
                format!(
                    "View.remote_bookmarks[{key:?}] must not have an absent RefTarget when untracked"
                )
                .into(),
            ));
        }
        let remote_ref = RemoteRef {
            target,
            state: if remote_ref_proto.is_tracked {
                RemoteRefState::Tracked
            } else {
                RemoteRefState::New
            },
        };
        let r_name = RemoteNameBuf::from(r_str);
        let b_name = RefNameBuf::from(b_str);
        remote_views
            .entry(r_name)
            .or_default()
            .bookmarks
            .insert(b_name, remote_ref);
    }

    Ok(View {
        head_ids,
        wc_commit_ids,
        local_bookmarks,
        local_tags: BTreeMap::new(),
        remote_views,
        git_refs: BTreeMap::new(),
        git_heads: BTreeMap::new(),
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

    pub fn new(repo_id: String, server_url: String) -> Self {
        Self {
            server_url,
            repo_id,
            root_operation_id: OperationId::from_bytes(&cc_common::ROOT_OPERATION_ID_BYTES),
        }
    }

    pub fn load(store_path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config = CommitCloudConfig::load_from_store(store_path)?;
        Ok(Self::new(config.repo_id, config.server_url))
    }

    /// Publishes a `View`, `Operation`, and `UpdateOpHeads` transition in one server RPC.
    pub async fn publish_operation(
        &self,
        view: &View,
        operation: &Operation,
        old_op_heads: &[OperationId],
    ) -> OpStoreResult<(ViewId, OperationId, Vec<OperationId>)> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let proto_view = view_to_proto(view);
        let mut proto_op = operation_to_proto(operation);
        proto_op.view_id.clear();
        let old_op_head_ids = old_op_heads
            .iter()
            .map(|id| id.as_bytes().to_vec())
            .collect();

        run_on_tokio(async move {
            let mut client = crate::util::connect_op_store_client(server_url).await?;
            let res = client
                .publish_operation(PublishOperationRequest {
                    repo_id,
                    view: Some(proto_view),
                    operation: Some(proto_op),
                    old_op_head_ids,
                })
                .await?
                .into_inner();
            let view_id = parse_view_id(&res.view_id, "PublishOperationResponse.view_id")?;
            let operation_id =
                parse_operation_id(&res.operation_id, "PublishOperationResponse.operation_id")?;
            let heads = res
                .current_op_head_ids
                .into_iter()
                .map(|b| parse_operation_id(&b, "PublishOperationResponse.current_op_head_ids"))
                .collect::<OpStoreResult<Vec<_>>>()?;
            if heads.is_empty() {
                return Err(OpStoreError::Other(
                    "PublishOperation returned empty current_op_head_ids".into(),
                )
                .into());
            }
            Ok((view_id, operation_id, heads))
        })
        .await
        .map_err(map_op_store_err)
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
        let view_id_hex = id.hex();

        run_on_tokio(async move {
            let mut client = crate::util::connect_op_store_client(server_url).await?;
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
                        object_type: "view".into(),
                        hash: view_id_hex,
                        source: Some(status.into()),
                    })
                        as Box<dyn std::error::Error + Send + Sync>);
                }
                Err(status) => {
                    return Err(Box::new(status) as Box<dyn std::error::Error + Send + Sync>);
                }
            };
            let resp = response.into_inner();
            if !resp.view_id.is_empty() && resp.view_id != view_id_bytes {
                return Err(format!(
                    "ReadView response view_id {} did not match requested {view_id_hex}",
                    hex::encode(&resp.view_id),
                )
                .into());
            }
            let proto_view = resp
                .view
                .ok_or_else(|| "server response should have contained a view".to_string())?;
            Ok(view_from_proto(proto_view)?)
        })
        .await
        .map_err(map_op_store_err)
    }

    async fn write_view(&self, contents: &View) -> OpStoreResult<ViewId> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let proto_view = view_to_proto(contents);

        run_on_tokio(async move {
            let mut client = crate::util::connect_op_store_client(server_url).await?;
            let response = client
                .write_view(WriteViewRequest {
                    repo_id,
                    view: Some(proto_view),
                })
                .await?;
            let view_id_bytes = response.into_inner().view_id;
            Ok(parse_view_id(&view_id_bytes, "WriteViewResponse.view_id")?)
        })
        .await
        .map_err(map_op_store_err)
    }

    async fn read_operation(&self, id: &OperationId) -> OpStoreResult<Operation> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let op_id_bytes = id.as_bytes().to_vec();
        let op_id_hex = id.hex();

        let requested_id = id.clone();

        run_on_tokio(async move {
            let mut client = crate::util::connect_op_store_client(server_url).await?;
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
                        object_type: "operation".into(),
                        hash: op_id_hex,
                        source: Some(status.into()),
                    })
                        as Box<dyn std::error::Error + Send + Sync>);
                }
                Err(status) => {
                    return Err(Box::new(status) as Box<dyn std::error::Error + Send + Sync>);
                }
            };
            let proto_op = response
                .into_inner()
                .operation
                .ok_or_else(|| "server response should have contained an operation".to_string())?;
            Ok(verified_operation_from_proto(&requested_id, proto_op)?)
        })
        .await
        .map_err(map_op_store_err)
    }

    async fn write_operation(&self, contents: &Operation) -> OpStoreResult<OperationId> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let proto_op = operation_to_proto(contents);

        run_on_tokio(async move {
            let mut client = crate::util::connect_op_store_client(server_url).await?;
            let response = client
                .write_operation(WriteOperationRequest {
                    repo_id,
                    operation: Some(proto_op),
                })
                .await?;
            let op_id_bytes = response.into_inner().operation_id;
            Ok(parse_operation_id(
                &op_id_bytes,
                "WriteOperationResponse.operation_id",
            )?)
        })
        .await
        .map_err(map_op_store_err)
    }

    async fn resolve_operation_id_prefix(
        &self,
        prefix: &HexPrefix,
    ) -> OpStoreResult<PrefixResolution<OperationId>> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let hex_prefix = prefix.hex().to_string();
        // A prefix longer than an operation ID, such as a 64-byte ID from jj's default op
        // store, matches no operation.
        if hex_prefix.len() > 2 * cc_common::OPERATION_ID_LENGTH {
            return Ok(PrefixResolution::NoMatch);
        }

        run_on_tokio(async move {
            let mut client = crate::util::connect_op_store_client(server_url).await?;
            let response = client
                .resolve_operation_prefix(ResolveOperationPrefixRequest {
                    repo_id,
                    hex_prefix: hex_prefix.clone(),
                })
                .await?
                .into_inner();

            let status = PrefixResolutionStatus::try_from(response.status)
                .unwrap_or(PrefixResolutionStatus::Unspecified);
            match status {
                PrefixResolutionStatus::PrefixResolutionNoMatch => Ok(PrefixResolution::NoMatch),
                PrefixResolutionStatus::PrefixResolutionSingleMatch => {
                    if response.matched_operation_id.len() != cc_common::OPERATION_ID_LENGTH {
                        return Err(format!(
                            "server returned invalid matched_operation_id length {}",
                            response.matched_operation_id.len()
                        )
                        .into());
                    }
                    if !hex::encode(&response.matched_operation_id).starts_with(&hex_prefix) {
                        return Err(format!(
                            "ResolveOperationPrefix matched_operation_id {} did not match requested prefix {hex_prefix}",
                            hex::encode(&response.matched_operation_id)
                        )
                        .into());
                    }
                    Ok(PrefixResolution::SingleMatch(OperationId::from_bytes(
                        &response.matched_operation_id,
                    )))
                }
                PrefixResolutionStatus::PrefixResolutionAmbiguousMatch => {
                    Ok(PrefixResolution::AmbiguousMatch)
                }
                PrefixResolutionStatus::Unspecified => {
                    Err("server returned unspecified PrefixResolutionStatus".into())
                }
            }
        })
        .await
        .map_err(jj_lib::op_store::OpStoreError::Other)
    }

    async fn gc(&self, _head_ids: &[OperationId], _keep_newer: SystemTime) -> OpStoreResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
