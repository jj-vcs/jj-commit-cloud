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

//! `OpStoreService` gRPC handler validating `View` and `Operation` payloads, write-before-
//! publish dependencies, and operation-head transitions over [`Store`].
//!
//! # Authority and Crash-Safety Ordering
//! `WriteView` and `WriteOperation` verify referenced commits, views, and parent operations
//! exist (`FAILED_PRECONDITION`) before persisting, and `UpdateOpHeads` verifies the new
//! operation head exists before advancing active heads.

use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

use cc_common::op_store::op_store_service_server::OpStoreService;
use cc_common::op_store::*;

use crate::backend::{validate_digest_len, validate_identifier};
use crate::error_util::ensure_repo_registered_error;
use crate::hash_utils::{hash_operation, hash_view};
use crate::store::Store;

#[derive(Clone)]
pub struct CommitCloudOpStoreService {
    store: Arc<dyn Store>,
}

impl CommitCloudOpStoreService {
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self { store }
    }
}

fn validate_ref_target(
    target: &RefTarget,
    field: &str,
    allow_absent: bool,
) -> Result<(), tonic::Status> {
    if !allow_absent && target.adds.is_empty() && target.removes.is_empty() {
        return Err(tonic::Status::invalid_argument(format!(
            "{field} RefTarget must not be absent (empty adds and removes)"
        )));
    }
    for term in &target.removes {
        validate_digest_len(&term.commit_id, cc_common::COMMIT_ID_LENGTH, field)?;
    }
    for term in &target.adds {
        validate_digest_len(&term.commit_id, cc_common::COMMIT_ID_LENGTH, field)?;
    }
    Ok(())
}

fn validate_view(view: &View) -> Result<(), tonic::Status> {
    if view.head_ids.is_empty() {
        return Err(tonic::Status::invalid_argument(
            "view.head_ids must not be empty",
        ));
    }
    for h in &view.head_ids {
        validate_digest_len(h, cc_common::COMMIT_ID_LENGTH, "view.head_ids")?;
    }
    for (ws_name, cid) in &view.wc_commit_ids {
        if ws_name.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "view.wc_commit_ids key must not be empty",
            ));
        }
        validate_digest_len(cid, cc_common::COMMIT_ID_LENGTH, "view.wc_commit_ids")?;
    }
    for (name, target) in &view.local_bookmarks {
        if name.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "view.local_bookmarks key must not be empty",
            ));
        }
        validate_ref_target(target, "view.local_bookmarks", false)?;
    }
    for (name, remote_ref) in &view.remote_bookmarks {
        let Some((bookmark, remote)) = name.rsplit_once('@') else {
            return Err(tonic::Status::invalid_argument(format!(
                "view.remote_bookmarks key '{name}' must use '<bookmark>@<remote>' format"
            )));
        };
        if bookmark.is_empty() || remote.is_empty() {
            return Err(tonic::Status::invalid_argument(format!(
                "view.remote_bookmarks key '{name}' must have non-empty bookmark and remote names"
            )));
        }
        let target = remote_ref.target.as_ref().ok_or_else(|| {
            tonic::Status::invalid_argument(format!(
                "view.remote_bookmarks['{name}'] missing required target"
            ))
        })?;
        // jj retains tracked absent remote bookmarks (for example, `jj bookmark track feat@origin`
        // before the first push, or partial restores such as `jj op restore --what=repo` or
        // `--what=remote-tracking` without a matching local bookmark).
        let allow_absent = remote_ref.is_tracked;
        validate_ref_target(target, "view.remote_bookmarks", allow_absent)?;
    }
    Ok(())
}

fn validate_operation(op: &Operation) -> Result<(), tonic::Status> {
    validate_digest_len(&op.view_id, cc_common::VIEW_ID_LENGTH, "operation.view_id")?;
    if op.parents.is_empty() {
        return Err(tonic::Status::invalid_argument(
            "non-root operation.parents must not be empty",
        ));
    }
    let mut seen_parents = std::collections::HashSet::with_capacity(op.parents.len());
    let mut has_root_parent = false;
    for p in &op.parents {
        validate_digest_len(p, cc_common::OPERATION_ID_LENGTH, "operation.parents")?;
        if !seen_parents.insert(p.as_slice()) {
            return Err(tonic::Status::invalid_argument(
                "operation.parents must not contain duplicate parent IDs",
            ));
        }
        if p.as_slice() == cc_common::ROOT_OPERATION_ID_BYTES {
            has_root_parent = true;
        }
    }
    if has_root_parent && op.parents.len() > 1 {
        return Err(tonic::Status::invalid_argument(
            "ROOT_OPERATION_ID may only appear as the sole parent in operation.parents",
        ));
    }
    if !op.commit_predecessors_set && !op.commit_predecessors.is_empty() {
        return Err(tonic::Status::invalid_argument(
            "operation.commit_predecessors must be empty when commit_predecessors_set is false",
        ));
    }
    let mut seen_pred_keys = std::collections::HashSet::with_capacity(op.commit_predecessors.len());
    for entry in &op.commit_predecessors {
        validate_digest_len(
            &entry.commit_id,
            cc_common::COMMIT_ID_LENGTH,
            "commit_predecessors.commit_id",
        )?;
        if !seen_pred_keys.insert(entry.commit_id.as_slice()) {
            return Err(tonic::Status::invalid_argument(
                "operation.commit_predecessors must not contain duplicate commit_id entries",
            ));
        }
        for pred in &entry.predecessor_ids {
            validate_digest_len(
                pred,
                cc_common::COMMIT_ID_LENGTH,
                "commit_predecessors.predecessor_ids",
            )?;
        }
    }
    Ok(())
}

#[tonic::async_trait]
impl OpStoreService for CommitCloudOpStoreService {
    async fn read_operation(
        &self,
        request: tonic::Request<ReadOperationRequest>,
    ) -> Result<tonic::Response<ReadOperationResponse>, tonic::Status> {
        let req = request.into_inner();
        validate_identifier(&req.repo_id, "repo_id")?;
        validate_digest_len(
            &req.operation_id,
            cc_common::OPERATION_ID_LENGTH,
            "operation_id",
        )?;
        info!(
            "Reading operation: {} for repo: {}",
            hex::encode(&req.operation_id),
            req.repo_id
        );

        ensure_repo_registered_error(self.store.as_ref(), &req.repo_id, "reading operations")
            .await?;

        if let Some(op) = self
            .store
            .get_operation(&req.repo_id, &req.operation_id)
            .await?
        {
            return Ok(tonic::Response::new(ReadOperationResponse {
                operation: Some(op),
            }));
        }

        if req.operation_id == cc_common::ROOT_OPERATION_ID_BYTES {
            let root_op = Operation {
                view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
                parents: vec![],
                metadata: Some(OperationMetadata {
                    start_time_millis: 0,
                    end_time_millis: 0,
                    description: "root()".to_string(),
                    hostname: String::new(),
                    username: String::new(),
                    is_snapshot: false,
                    workspace_name: None,
                    attributes: HashMap::new(),
                }),
                commit_predecessors: vec![],
                commit_predecessors_set: true,
            };
            return Ok(tonic::Response::new(ReadOperationResponse {
                operation: Some(root_op),
            }));
        }

        Err(tonic::Status::not_found(format!(
            "operation should have been present in cloud database: {} in repo: {}",
            hex::encode(&req.operation_id),
            req.repo_id
        )))
    }

    async fn write_operation(
        &self,
        request: tonic::Request<WriteOperationRequest>,
    ) -> Result<tonic::Response<WriteOperationResponse>, tonic::Status> {
        let req = request.into_inner();
        validate_identifier(&req.repo_id, "repo_id")?;
        let op = req.operation.ok_or_else(|| {
            tonic::Status::invalid_argument("request should have contained an operation object")
        })?;
        validate_operation(&op)?;

        info!("Writing operation for repo: {}", req.repo_id);

        let project_id = self
            .store
            .get_repo_project_id(&req.repo_id)
            .await?
            .ok_or_else(|| {
                tonic::Status::not_found(format!(
                    "repository should have been registered before writing operations: {}",
                    req.repo_id
                ))
            })?;

        if op.view_id.as_slice() != cc_common::ROOT_VIEW_ID_BYTES
            && self
                .store
                .get_view(&req.repo_id, &op.view_id)
                .await?
                .is_none()
        {
            return Err(tonic::Status::failed_precondition(format!(
                "view {} referenced by operation does not exist in repo '{}'; write view before operation",
                hex::encode(&op.view_id),
                req.repo_id
            )));
        }
        for parent_id in &op.parents {
            if parent_id.as_slice() != cc_common::ROOT_OPERATION_ID_BYTES
                && self
                    .store
                    .get_operation(&req.repo_id, parent_id)
                    .await?
                    .is_none()
            {
                return Err(tonic::Status::failed_precondition(format!(
                    "parent operation {} does not exist in repo '{}'; write parent operations before child operations",
                    hex::encode(parent_id),
                    req.repo_id
                )));
            }
        }
        let mut pred_commit_ids: std::collections::HashSet<&[u8]> =
            std::collections::HashSet::new();
        for entry in &op.commit_predecessors {
            pred_commit_ids.insert(entry.commit_id.as_slice());
            for pred in &entry.predecessor_ids {
                pred_commit_ids.insert(pred.as_slice());
            }
        }
        for cid in pred_commit_ids {
            if cid != cc_common::ROOT_COMMIT_ID_BYTES
                && self.store.get_commit(&project_id, cid).await?.is_none()
            {
                return Err(tonic::Status::failed_precondition(format!(
                    "commit {} referenced in operation.commit_predecessors does not exist in project '{}'; write commits before operations",
                    hex::encode(cid),
                    project_id
                )));
            }
        }

        let op_id = hash_operation(&op).map_err(tonic::Status::invalid_argument)?;

        self.store
            .put_operation(req.repo_id, op_id.clone(), op)
            .await?;

        Ok(tonic::Response::new(WriteOperationResponse {
            operation_id: op_id,
        }))
    }

    async fn resolve_operation_prefix(
        &self,
        _request: tonic::Request<ResolveOperationPrefixRequest>,
    ) -> Result<tonic::Response<ResolveOperationPrefixResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "ResolveOperationPrefix is not implemented yet",
        ))
    }

    async fn read_view(
        &self,
        request: tonic::Request<ReadViewRequest>,
    ) -> Result<tonic::Response<ReadViewResponse>, tonic::Status> {
        let req = request.into_inner();
        validate_identifier(&req.repo_id, "repo_id")?;
        validate_digest_len(&req.view_id, cc_common::VIEW_ID_LENGTH, "view_id")?;
        info!(
            "Reading view: {} for repo: {}",
            hex::encode(&req.view_id),
            req.repo_id
        );

        ensure_repo_registered_error(self.store.as_ref(), &req.repo_id, "reading views").await?;

        if let Some(v) = self.store.get_view(&req.repo_id, &req.view_id).await? {
            return Ok(tonic::Response::new(ReadViewResponse {
                view_id: req.view_id,
                view: Some(v),
            }));
        }

        if req.view_id == cc_common::ROOT_VIEW_ID_BYTES {
            let root_view = View {
                head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                wc_commit_ids: HashMap::new(),
                local_bookmarks: HashMap::new(),
                remote_bookmarks: HashMap::new(),
            };
            return Ok(tonic::Response::new(ReadViewResponse {
                view_id: req.view_id,
                view: Some(root_view),
            }));
        }

        Err(tonic::Status::not_found(format!(
            "view should have been present in cloud database: {} in repo: {}",
            hex::encode(&req.view_id),
            req.repo_id
        )))
    }

    async fn write_view(
        &self,
        request: tonic::Request<WriteViewRequest>,
    ) -> Result<tonic::Response<WriteViewResponse>, tonic::Status> {
        let req = request.into_inner();
        validate_identifier(&req.repo_id, "repo_id")?;
        let view = req.view.ok_or_else(|| {
            tonic::Status::invalid_argument("request should have contained a view object")
        })?;
        validate_view(&view)?;

        info!("Writing view for repo: {}", req.repo_id);

        let project_id = self
            .store
            .get_repo_project_id(&req.repo_id)
            .await?
            .ok_or_else(|| {
                tonic::Status::not_found(format!(
                    "repository should have been registered before writing views: {}",
                    req.repo_id
                ))
            })?;

        let mut required_commits: std::collections::HashSet<&[u8]> =
            std::collections::HashSet::new();
        for head_id in &view.head_ids {
            required_commits.insert(head_id.as_slice());
        }
        for wc_id in view.wc_commit_ids.values() {
            required_commits.insert(wc_id.as_slice());
        }
        for target in view.local_bookmarks.values() {
            for term in target.adds.iter().chain(target.removes.iter()) {
                required_commits.insert(term.commit_id.as_slice());
            }
        }
        for remote_ref in view.remote_bookmarks.values() {
            if let Some(target) = &remote_ref.target {
                for term in target.adds.iter().chain(target.removes.iter()) {
                    required_commits.insert(term.commit_id.as_slice());
                }
            }
        }
        for cid in required_commits {
            if cid != cc_common::ROOT_COMMIT_ID_BYTES
                && self.store.get_commit(&project_id, cid).await?.is_none()
            {
                return Err(tonic::Status::failed_precondition(format!(
                    "commit {} referenced by view does not exist in project '{}'; write commits before views",
                    hex::encode(cid),
                    project_id
                )));
            }
        }

        let view_id = hash_view(&view).map_err(tonic::Status::invalid_argument)?;

        self.store
            .put_view(req.repo_id, view_id.clone(), view)
            .await?;

        Ok(tonic::Response::new(WriteViewResponse { view_id }))
    }

    async fn get_op_heads(
        &self,
        request: tonic::Request<GetOpHeadsRequest>,
    ) -> Result<tonic::Response<GetOpHeadsResponse>, tonic::Status> {
        let req = request.into_inner();
        validate_identifier(&req.repo_id, "repo_id")?;
        info!("Get op heads for repo: {}", req.repo_id);

        ensure_repo_registered_error(self.store.as_ref(), &req.repo_id, "requesting op heads")
            .await?;

        let heads = self
            .store
            .get_op_heads(&req.repo_id)
            .await?
            .unwrap_or_else(|| vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()]);

        Ok(tonic::Response::new(GetOpHeadsResponse {
            op_head_ids: heads,
        }))
    }

    async fn update_op_heads(
        &self,
        request: tonic::Request<UpdateOpHeadsRequest>,
    ) -> Result<tonic::Response<UpdateOpHeadsResponse>, tonic::Status> {
        let req = request.into_inner();
        validate_identifier(&req.repo_id, "repo_id")?;
        validate_digest_len(
            &req.new_op_head_id,
            cc_common::OPERATION_ID_LENGTH,
            "new_op_head_id",
        )?;
        if req.new_op_head_id.as_slice() == cc_common::ROOT_OPERATION_ID_BYTES {
            return Err(tonic::Status::invalid_argument(
                "new_op_head_id must not be ROOT_OPERATION_ID",
            ));
        }
        for old_id in &req.old_op_head_ids {
            validate_digest_len(old_id, cc_common::OPERATION_ID_LENGTH, "old_op_head_ids")?;
        }
        info!("Update op heads for repo: {}", req.repo_id);

        ensure_repo_registered_error(self.store.as_ref(), &req.repo_id, "updating op heads")
            .await?;

        if self
            .store
            .get_operation(&req.repo_id, &req.new_op_head_id)
            .await?
            .is_none()
        {
            return Err(tonic::Status::failed_precondition(format!(
                "new_op_head_id {} does not exist in repo '{}'; write operation before updating op heads",
                hex::encode(&req.new_op_head_id),
                req.repo_id
            )));
        }

        let current_heads = self
            .store
            .update_op_heads_append_new_remove_old(
                req.repo_id,
                &req.old_op_head_ids,
                req.new_op_head_id,
            )
            .await?;

        Ok(tonic::Response::new(UpdateOpHeadsResponse {
            current_op_head_ids: current_heads,
        }))
    }

    async fn reconcile_op_heads(
        &self,
        request: tonic::Request<ReconcileOpHeadsRequest>,
    ) -> Result<tonic::Response<ReconcileOpHeadsResponse>, tonic::Status> {
        let req = request.into_inner();
        validate_identifier(&req.repo_id, "repo_id")?;
        info!("Reconcile op heads for repo: {}", req.repo_id);

        ensure_repo_registered_error(self.store.as_ref(), &req.repo_id, "reconciling op heads")
            .await?;

        let outcome =
            crate::reconcile_op_heads::reconcile_repo_op_heads(self.store.clone(), &req.repo_id)
                .await?;

        let resp = match outcome {
            crate::reconcile_op_heads::ReconcileOutcome::Reconciled(head) => {
                ReconcileOpHeadsResponse {
                    op_head: head.clone(),
                    client_resolution_required: false,
                    current_op_head_ids: vec![head],
                }
            }
            crate::reconcile_op_heads::ReconcileOutcome::ClientResolutionRequired(heads) => {
                ReconcileOpHeadsResponse {
                    op_head: Vec::new(),
                    client_resolution_required: true,
                    current_op_head_ids: heads,
                }
            }
        };
        Ok(tonic::Response::new(resp))
    }

    async fn publish_operation(
        &self,
        _request: tonic::Request<PublishOperationRequest>,
    ) -> Result<tonic::Response<PublishOperationResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "PublishOperation is not implemented yet",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryStore;
    use tonic::Code;

    async fn setup_repo() -> (Arc<MemoryStore>, CommitCloudOpStoreService) {
        let store = Arc::new(MemoryStore::default());
        store
            .register_repo("repo-1".to_string(), "proj-1".to_string(), None)
            .await
            .unwrap();
        let svc = CommitCloudOpStoreService::new(store.clone());
        (store, svc)
    }

    #[tokio::test]
    async fn test_write_view_validates_ref_targets_remote_keys_and_bookmark_commits() {
        let (store, svc) = setup_repo().await;
        let existing_commit = vec![0xaa; cc_common::COMMIT_ID_LENGTH];
        let missing_commit = vec![0xbb; cc_common::COMMIT_ID_LENGTH];
        store
            .put_commit(
                "proj-1".to_string(),
                existing_commit.clone(),
                cc_common::backend::Commit::default(),
            )
            .await
            .unwrap();

        // Absent RefTarget rejected.
        let mut bad_local = HashMap::new();
        bad_local.insert(
            "main".to_string(),
            RefTarget {
                removes: Vec::new(),
                adds: Vec::new(),
            },
        );
        let err = svc
            .write_view(tonic::Request::new(WriteViewRequest {
                repo_id: "repo-1".to_string(),
                view: Some(View {
                    head_ids: vec![existing_commit.clone()],
                    wc_commit_ids: HashMap::new(),
                    local_bookmarks: bad_local,
                    remote_bookmarks: HashMap::new(),
                }),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);

        // Empty commit_id term inside RefTarget rejected.
        let mut empty_term = HashMap::new();
        empty_term.insert(
            "main".to_string(),
            RefTarget {
                removes: Vec::new(),
                adds: vec![RefTargetTerm {
                    commit_id: Vec::new(),
                }],
            },
        );
        let err = svc
            .write_view(tonic::Request::new(WriteViewRequest {
                repo_id: "repo-1".to_string(),
                view: Some(View {
                    head_ids: vec![existing_commit.clone()],
                    wc_commit_ids: HashMap::new(),
                    local_bookmarks: empty_term,
                    remote_bookmarks: HashMap::new(),
                }),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);

        // Malformed remote bookmark keys rejected.
        for bad_key in ["no-at-sign", "@origin", "main@"] {
            let mut bad_remote = HashMap::new();
            bad_remote.insert(
                bad_key.to_string(),
                RemoteRef {
                    target: Some(RefTarget {
                        removes: Vec::new(),
                        adds: vec![RefTargetTerm {
                            commit_id: existing_commit.clone(),
                        }],
                    }),
                    is_tracked: true,
                },
            );
            let err = svc
                .write_view(tonic::Request::new(WriteViewRequest {
                    repo_id: "repo-1".to_string(),
                    view: Some(View {
                        head_ids: vec![existing_commit.clone()],
                        wc_commit_ids: HashMap::new(),
                        local_bookmarks: HashMap::new(),
                        remote_bookmarks: bad_remote,
                    }),
                }))
                .await
                .unwrap_err();
            assert_eq!(err.code(), Code::InvalidArgument, "bad key: {bad_key}");
        }

        // Unpersisted commit referenced by bookmark returns FAILED_PRECONDITION.
        let mut missing_local = HashMap::new();
        missing_local.insert(
            "main".to_string(),
            RefTarget {
                removes: Vec::new(),
                adds: vec![RefTargetTerm {
                    commit_id: missing_commit,
                }],
            },
        );
        let err = svc
            .write_view(tonic::Request::new(WriteViewRequest {
                repo_id: "repo-1".to_string(),
                view: Some(View {
                    head_ids: vec![existing_commit.clone()],
                    wc_commit_ids: HashMap::new(),
                    local_bookmarks: missing_local,
                    remote_bookmarks: HashMap::new(),
                }),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);

        // Tracked-absent remote bookmark with matching local bookmark succeeds (`jj bookmark track` before push).
        let mut local_with_feat = HashMap::new();
        local_with_feat.insert(
            "feat".to_string(),
            RefTarget {
                removes: Vec::new(),
                adds: vec![RefTargetTerm {
                    commit_id: existing_commit.clone(),
                }],
            },
        );
        let mut tracked_absent_remote = HashMap::new();
        tracked_absent_remote.insert(
            "feat@origin".to_string(),
            RemoteRef {
                target: Some(RefTarget {
                    removes: Vec::new(),
                    adds: Vec::new(),
                }),
                is_tracked: true,
            },
        );
        svc.write_view(tonic::Request::new(WriteViewRequest {
            repo_id: "repo-1".to_string(),
            view: Some(View {
                head_ids: vec![existing_commit.clone()],
                wc_commit_ids: HashMap::new(),
                local_bookmarks: local_with_feat.clone(),
                remote_bookmarks: tracked_absent_remote,
            }),
        }))
        .await
        .expect("tracked-absent remote bookmark with matching local bookmark must succeed");

        // Untracked-absent remote bookmark rejected.
        let mut untracked_absent = HashMap::new();
        untracked_absent.insert(
            "feat@origin".to_string(),
            RemoteRef {
                target: Some(RefTarget {
                    removes: Vec::new(),
                    adds: Vec::new(),
                }),
                is_tracked: false,
            },
        );
        let err = svc
            .write_view(tonic::Request::new(WriteViewRequest {
                repo_id: "repo-1".to_string(),
                view: Some(View {
                    head_ids: vec![existing_commit.clone()],
                    wc_commit_ids: HashMap::new(),
                    local_bookmarks: local_with_feat,
                    remote_bookmarks: untracked_absent,
                }),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);

        // Tracked-absent remote bookmark without a matching local bookmark succeeds
        // (for example, partial restores like `jj op restore --what=repo` or
        // `--what=remote-tracking`).
        let mut tracked_absent_without_local = HashMap::new();
        tracked_absent_without_local.insert(
            "orphan@origin".to_string(),
            RemoteRef {
                target: Some(RefTarget {
                    removes: Vec::new(),
                    adds: Vec::new(),
                }),
                is_tracked: true,
            },
        );
        svc.write_view(tonic::Request::new(WriteViewRequest {
            repo_id: "repo-1".to_string(),
            view: Some(View {
                head_ids: vec![existing_commit.clone()],
                wc_commit_ids: HashMap::new(),
                local_bookmarks: HashMap::new(),
                remote_bookmarks: tracked_absent_without_local,
            }),
        }))
        .await
        .expect("tracked-absent remote bookmark without matching local bookmark must succeed");

        // Untracked-absent remote bookmark without a matching local bookmark is still rejected.
        let mut untracked_absent_without_local = HashMap::new();
        untracked_absent_without_local.insert(
            "orphan@origin".to_string(),
            RemoteRef {
                target: Some(RefTarget {
                    removes: Vec::new(),
                    adds: Vec::new(),
                }),
                is_tracked: false,
            },
        );
        let err = svc
            .write_view(tonic::Request::new(WriteViewRequest {
                repo_id: "repo-1".to_string(),
                view: Some(View {
                    head_ids: vec![existing_commit],
                    wc_commit_ids: HashMap::new(),
                    local_bookmarks: HashMap::new(),
                    remote_bookmarks: untracked_absent_without_local,
                }),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_write_operation_validates_duplicate_and_missing_commit_predecessors() {
        let (store, svc) = setup_repo().await;
        let existing_commit = vec![0xaa; cc_common::COMMIT_ID_LENGTH];
        let missing_commit = vec![0xbb; cc_common::COMMIT_ID_LENGTH];
        store
            .put_commit(
                "proj-1".to_string(),
                existing_commit.clone(),
                cc_common::backend::Commit::default(),
            )
            .await
            .unwrap();

        // Duplicate commit_predecessors.commit_id rejected.
        let dup_err = svc
            .write_operation(tonic::Request::new(WriteOperationRequest {
                repo_id: "repo-1".to_string(),
                operation: Some(Operation {
                    view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
                    parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                    metadata: None,
                    commit_predecessors: vec![
                        CommitPredecessors {
                            commit_id: existing_commit.clone(),
                            predecessor_ids: Vec::new(),
                        },
                        CommitPredecessors {
                            commit_id: existing_commit.clone(),
                            predecessor_ids: Vec::new(),
                        },
                    ],
                    commit_predecessors_set: true,
                }),
            }))
            .await
            .unwrap_err();
        assert_eq!(dup_err.code(), Code::InvalidArgument);

        // Missing commit in commit_predecessors returns FAILED_PRECONDITION.
        let missing_err = svc
            .write_operation(tonic::Request::new(WriteOperationRequest {
                repo_id: "repo-1".to_string(),
                operation: Some(Operation {
                    view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
                    parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                    metadata: None,
                    commit_predecessors: vec![CommitPredecessors {
                        commit_id: existing_commit,
                        predecessor_ids: vec![missing_commit],
                    }],
                    commit_predecessors_set: true,
                }),
            }))
            .await
            .unwrap_err();
        assert_eq!(missing_err.code(), Code::FailedPrecondition);

        // Deferred RPCs return UNIMPLEMENTED.
        assert_eq!(
            svc.resolve_operation_prefix(tonic::Request::new(ResolveOperationPrefixRequest {
                repo_id: "repo-1".to_string(),
                hex_prefix: "abcd".to_string(),
            }))
            .await
            .unwrap_err()
            .code(),
            Code::Unimplemented
        );
        assert_eq!(
            svc.publish_operation(tonic::Request::new(PublishOperationRequest::default()))
                .await
                .unwrap_err()
                .code(),
            Code::Unimplemented
        );
    }

    #[tokio::test]
    async fn test_update_op_heads_rejects_root_operation_id() {
        let (store, svc) = setup_repo().await;
        let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();

        // Reject virtual ROOT_OPERATION_ID as new_op_head_id on both empty and root-parented updates.
        for old_op_head_ids in [Vec::new(), vec![root_op.clone()]] {
            let err = svc
                .update_op_heads(tonic::Request::new(UpdateOpHeadsRequest {
                    repo_id: "repo-1".to_string(),
                    old_op_head_ids,
                    new_op_head_id: root_op.clone(),
                }))
                .await
                .unwrap_err();
            assert_eq!(err.code(), Code::InvalidArgument);
        }
        assert!(store.get_op_heads("repo-1").await.unwrap().is_none());

        // Also reject ROOT_OPERATION_ID when transitioning away from an active non-root head.
        let op1_id = vec![0x11; cc_common::OPERATION_ID_LENGTH];
        store
            .put_operation(
                "repo-1".to_string(),
                op1_id.clone(),
                Operation {
                    view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
                    parents: vec![root_op.clone()],
                    metadata: None,
                    commit_predecessors: Vec::new(),
                    commit_predecessors_set: true,
                },
            )
            .await
            .unwrap();
        svc.update_op_heads(tonic::Request::new(UpdateOpHeadsRequest {
            repo_id: "repo-1".to_string(),
            old_op_head_ids: vec![root_op.clone()],
            new_op_head_id: op1_id.clone(),
        }))
        .await
        .expect("valid non-root op head transition must succeed");

        let err = svc
            .update_op_heads(tonic::Request::new(UpdateOpHeadsRequest {
                repo_id: "repo-1".to_string(),
                old_op_head_ids: vec![op1_id.clone()],
                new_op_head_id: root_op,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
        assert_eq!(
            store.get_op_heads("repo-1").await.unwrap(),
            Some(vec![op1_id])
        );
    }
}
