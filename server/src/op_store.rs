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

//! `OpStoreService` gRPC handler managing repository `View` snapshots, `Operation` nodes, and
//! active operation heads over [`Store`].
//!
//! # Authority and Crash-Safety Ordering
//! Synthesizes the virtual all-zero `ROOT_VIEW_ID` and `ROOT_OPERATION_ID` when absent, verifies
//! `View` and parent `Operation` dependencies before publishing (`FAILED_PRECONDITION`), hashes
//! views and operations deterministically, and preserves concurrent sibling heads during
//! `UpdateOpHeads`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::info;

use cc_common::op_store::op_store_service_server::OpStoreService;
use cc_common::op_store::*;

use crate::backend::{validate_digest_len, validate_identifier};
use crate::error_util::ensure_repo_registered_error;
use crate::hash_utils::{hash_operation, hash_view};
use crate::store::Store;

fn validate_ref_target(
    target: &RefTarget,
    field: &str,
    require_present: bool,
) -> Result<(), tonic::Status> {
    if require_present && target.removes.is_empty() && target.adds.is_empty() {
        return Err(tonic::Status::invalid_argument(format!(
            "{field} RefTarget must not be absent (both removes and adds empty)"
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
            "view.head_ids must contain at least one commit_id",
        ));
    }
    let mut seen_heads = HashSet::with_capacity(view.head_ids.len());
    for head in &view.head_ids {
        validate_digest_len(head, cc_common::COMMIT_ID_LENGTH, "view.head_ids")?;
        if !seen_heads.insert(head.as_slice()) {
            return Err(tonic::Status::invalid_argument(
                "duplicate commit_id in view.head_ids",
            ));
        }
    }
    for (ws_name, commit_id) in &view.wc_commit_ids {
        if ws_name.trim().is_empty() || ws_name.contains('\0') {
            return Err(tonic::Status::invalid_argument(
                "view.wc_commit_ids workspace name must be non-empty without NUL bytes",
            ));
        }
        validate_digest_len(commit_id, cc_common::COMMIT_ID_LENGTH, "view.wc_commit_ids")?;
    }
    for (name, target) in &view.local_bookmarks {
        if name.trim().is_empty() || name.contains('\0') {
            return Err(tonic::Status::invalid_argument(
                "view.local_bookmarks name must be non-empty without NUL bytes",
            ));
        }
        validate_ref_target(target, "view.local_bookmarks", true)?;
    }
    for (name, remote) in &view.remote_bookmarks {
        let Some((bookmark_name, remote_name)) = name.rsplit_once('@') else {
            return Err(tonic::Status::invalid_argument(
                "view.remote_bookmarks key must use '<bookmark>@<remote>' format",
            ));
        };
        if bookmark_name.trim().is_empty() || remote_name.trim().is_empty() || name.contains('\0') {
            return Err(tonic::Status::invalid_argument(
                "view.remote_bookmarks key must have non-empty bookmark and remote names without NUL bytes",
            ));
        }
        match &remote.target {
            Some(target) => {
                validate_ref_target(target, "view.remote_bookmarks", !remote.is_tracked)?
            }
            None if !remote.is_tracked => {
                return Err(tonic::Status::invalid_argument(
                    "untracked view.remote_bookmarks entry must carry a non-absent target",
                ));
            }
            None => {}
        }
    }
    Ok(())
}

async fn ensure_commit_persisted(
    store: &dyn Store,
    repo_id: &str,
    commit_id: &[u8],
    context: &str,
) -> Result<(), tonic::Status> {
    if commit_id != cc_common::ROOT_COMMIT_ID_BYTES
        && store.get_commit(repo_id, commit_id).await.is_none()
    {
        return Err(tonic::Status::failed_precondition(format!(
            "commit {} must be persisted before {context}",
            hex::encode(commit_id)
        )));
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
    let mut seen_parents = HashSet::with_capacity(op.parents.len());
    let mut has_root_parent = false;
    for p in &op.parents {
        validate_digest_len(p, cc_common::OPERATION_ID_LENGTH, "operation.parents")?;
        if !seen_parents.insert(p.as_slice()) {
            return Err(tonic::Status::invalid_argument(
                "duplicate operation_id in operation.parents",
            ));
        }
        if p.as_slice() == cc_common::ROOT_OPERATION_ID_BYTES {
            has_root_parent = true;
        }
    }
    if has_root_parent && op.parents.len() > 1 {
        return Err(tonic::Status::invalid_argument(
            "ROOT_OPERATION_ID may only appear as the sole entry in operation.parents",
        ));
    }
    if !op.commit_predecessors_set && !op.commit_predecessors.is_empty() {
        return Err(tonic::Status::invalid_argument(
            "operation.commit_predecessors must be empty when commit_predecessors_set is false",
        ));
    }
    let mut seen_pred_commits = HashSet::with_capacity(op.commit_predecessors.len());
    for pred in &op.commit_predecessors {
        validate_digest_len(
            &pred.commit_id,
            cc_common::COMMIT_ID_LENGTH,
            "commit_predecessors.commit_id",
        )?;
        if !seen_pred_commits.insert(pred.commit_id.as_slice()) {
            return Err(tonic::Status::invalid_argument(
                "duplicate commit_id in operation.commit_predecessors",
            ));
        }
        for p_id in &pred.predecessor_ids {
            validate_digest_len(
                p_id,
                cc_common::COMMIT_ID_LENGTH,
                "commit_predecessors.predecessor_ids",
            )?;
        }
    }
    Ok(())
}

#[derive(Clone)]
pub struct CommitCloudOpStoreService {
    store: Arc<dyn Store>,
}

impl CommitCloudOpStoreService {
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self { store }
    }
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
            .await
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
                    hostname: "".to_string(),
                    username: "".to_string(),
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

        ensure_repo_registered_error(self.store.as_ref(), &req.repo_id, "writing operations")
            .await?;

        if op.view_id != cc_common::ROOT_VIEW_ID_BYTES
            && self
                .store
                .get_view(&req.repo_id, &op.view_id)
                .await
                .is_none()
        {
            return Err(tonic::Status::failed_precondition(format!(
                "view {} must be persisted before WriteOperation",
                hex::encode(&op.view_id)
            )));
        }
        for parent_id in &op.parents {
            if parent_id.as_slice() != cc_common::ROOT_OPERATION_ID_BYTES
                && self
                    .store
                    .get_operation(&req.repo_id, parent_id)
                    .await
                    .is_none()
            {
                return Err(tonic::Status::failed_precondition(format!(
                    "parent operation {} must be persisted before WriteOperation",
                    hex::encode(parent_id)
                )));
            }
        }
        for pred in &op.commit_predecessors {
            ensure_commit_persisted(
                self.store.as_ref(),
                &req.repo_id,
                &pred.commit_id,
                "WriteOperation",
            )
            .await?;
            for pred_id in &pred.predecessor_ids {
                ensure_commit_persisted(
                    self.store.as_ref(),
                    &req.repo_id,
                    pred_id,
                    "WriteOperation",
                )
                .await?;
            }
        }

        let op_id = hash_operation(&op).map_err(tonic::Status::invalid_argument)?;

        self.store
            .put_operation(req.repo_id, op_id.clone(), op)
            .await;

        Ok(tonic::Response::new(WriteOperationResponse {
            operation_id: op_id,
        }))
    }

    async fn resolve_operation_prefix(
        &self,
        _request: tonic::Request<ResolveOperationPrefixRequest>,
    ) -> Result<tonic::Response<ResolveOperationPrefixResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented yet"))
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

        if let Some(v) = self.store.get_view(&req.repo_id, &req.view_id).await {
            return Ok(tonic::Response::new(ReadViewResponse {
                view_id: req.view_id.clone(),
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
                view_id: req.view_id.clone(),
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

        ensure_repo_registered_error(self.store.as_ref(), &req.repo_id, "writing views").await?;

        for head_id in &view.head_ids {
            ensure_commit_persisted(self.store.as_ref(), &req.repo_id, head_id, "WriteView")
                .await?;
        }
        for wc_id in view.wc_commit_ids.values() {
            ensure_commit_persisted(self.store.as_ref(), &req.repo_id, wc_id, "WriteView").await?;
        }
        for target in view.local_bookmarks.values() {
            for term in target.removes.iter().chain(target.adds.iter()) {
                ensure_commit_persisted(
                    self.store.as_ref(),
                    &req.repo_id,
                    &term.commit_id,
                    "WriteView",
                )
                .await?;
            }
        }
        for remote in view.remote_bookmarks.values() {
            if let Some(target) = &remote.target {
                for term in target.removes.iter().chain(target.adds.iter()) {
                    ensure_commit_persisted(
                        self.store.as_ref(),
                        &req.repo_id,
                        &term.commit_id,
                        "WriteView",
                    )
                    .await?;
                }
            }
        }

        let view_id = hash_view(&view).map_err(tonic::Status::invalid_argument)?;

        self.store
            .put_view(req.repo_id, view_id.clone(), view)
            .await;

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
            .await
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
        for old_id in &req.old_op_head_ids {
            validate_digest_len(old_id, cc_common::OPERATION_ID_LENGTH, "old_op_head_ids")?;
        }
        info!("Update op heads for repo: {}", req.repo_id);

        ensure_repo_registered_error(self.store.as_ref(), &req.repo_id, "updating op heads")
            .await?;

        if req.new_op_head_id != cc_common::ROOT_OPERATION_ID_BYTES
            && self
                .store
                .get_operation(&req.repo_id, &req.new_op_head_id)
                .await
                .is_none()
        {
            return Err(tonic::Status::failed_precondition(format!(
                "new_op_head_id {} must be persisted before UpdateOpHeads",
                hex::encode(&req.new_op_head_id)
            )));
        }

        let current_heads = self
            .store
            .update_op_heads(req.repo_id, &req.old_op_head_ids, req.new_op_head_id)
            .await;

        Ok(tonic::Response::new(UpdateOpHeadsResponse {
            current_op_head_ids: current_heads,
        }))
    }

    async fn reconcile_op_heads(
        &self,
        _request: tonic::Request<ReconcileOpHeadsRequest>,
    ) -> Result<tonic::Response<ReconcileOpHeadsResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented yet"))
    }

    async fn publish_operation(
        &self,
        _request: tonic::Request<PublishOperationRequest>,
    ) -> Result<tonic::Response<PublishOperationResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented yet"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryStore;
    use tonic::{Code, Request};

    #[tokio::test]
    async fn op_store_synthesizes_roots_enforces_preconditions_and_stubs_new_rpcs() {
        let store = Arc::new(MemoryStore::new());
        store.register_repo("repo-op".to_string()).await;
        let svc = CommitCloudOpStoreService::new(store);

        // New RPCs return UNIMPLEMENTED.
        assert_eq!(
            svc.resolve_operation_prefix(Request::new(ResolveOperationPrefixRequest {
                repo_id: "repo-op".to_string(),
                hex_prefix: "00".to_string(),
            }))
            .await
            .unwrap_err()
            .code(),
            Code::Unimplemented
        );
        assert_eq!(
            svc.reconcile_op_heads(Request::new(ReconcileOpHeadsRequest {
                repo_id: "repo-op".to_string(),
            }))
            .await
            .unwrap_err()
            .code(),
            Code::Unimplemented
        );
        assert_eq!(
            svc.publish_operation(Request::new(PublishOperationRequest {
                repo_id: "repo-op".to_string(),
                view: None,
                operation: None,
                old_op_head_ids: Vec::new(),
            }))
            .await
            .unwrap_err()
            .code(),
            Code::Unimplemented
        );

        // Root view and root operation synthesize cleanly.
        let root_view = svc
            .read_view(Request::new(ReadViewRequest {
                repo_id: "repo-op".to_string(),
                view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            root_view.view.unwrap().head_ids,
            vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()]
        );

        let root_op = svc
            .read_operation(Request::new(ReadOperationRequest {
                repo_id: "repo-op".to_string(),
                operation_id: cc_common::ROOT_OPERATION_ID_BYTES.to_vec(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            root_op.operation.unwrap().view_id,
            cc_common::ROOT_VIEW_ID_BYTES
        );

        // WriteOperation rejects unpersisted view_id with FAILED_PRECONDITION.
        assert_eq!(
            svc.write_operation(Request::new(WriteOperationRequest {
                repo_id: "repo-op".to_string(),
                operation: Some(Operation {
                    view_id: vec![0xaa; 16],
                    parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                    ..Default::default()
                }),
            }))
            .await
            .unwrap_err()
            .code(),
            Code::FailedPrecondition
        );

        // UpdateOpHeads rejects unpersisted new_op_head_id with FAILED_PRECONDITION.
        assert_eq!(
            svc.update_op_heads(Request::new(UpdateOpHeadsRequest {
                repo_id: "repo-op".to_string(),
                old_op_head_ids: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                new_op_head_id: vec![0xbb; 16],
            }))
            .await
            .unwrap_err()
            .code(),
            Code::FailedPrecondition
        );
    }

    #[tokio::test]
    async fn update_op_heads_preserves_concurrent_siblings_and_skips_superseded_retries() {
        let store = Arc::new(MemoryStore::new());
        store.register_repo("repo-conc".to_string()).await;
        let svc = CommitCloudOpStoreService::new(store);

        assert_eq!(
            svc.write_operation(Request::new(WriteOperationRequest {
                repo_id: "repo-conc".to_string(),
                operation: Some(Operation {
                    view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
                    parents: Vec::new(),
                    ..Default::default()
                }),
            }))
            .await
            .unwrap_err()
            .code(),
            Code::InvalidArgument
        );
        assert_eq!(
            svc.write_operation(Request::new(WriteOperationRequest {
                repo_id: "repo-conc".to_string(),
                operation: Some(Operation {
                    view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
                    parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                    commit_predecessors_set: true,
                    commit_predecessors: vec![
                        CommitPredecessors {
                            commit_id: vec![0x11; 20],
                            predecessor_ids: Vec::new(),
                        },
                        CommitPredecessors {
                            commit_id: vec![0x11; 20],
                            predecessor_ids: vec![vec![0x22; 20]],
                        },
                    ],
                    ..Default::default()
                }),
            }))
            .await
            .unwrap_err()
            .code(),
            Code::InvalidArgument
        );

        let op1 = svc
            .write_operation(Request::new(WriteOperationRequest {
                repo_id: "repo-conc".to_string(),
                operation: Some(Operation {
                    view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
                    parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                    metadata: Some(OperationMetadata {
                        description: "client-1".to_string(),
                        ..Default::default()
                    }),
                    commit_predecessors_set: true,
                    ..Default::default()
                }),
            }))
            .await
            .unwrap()
            .into_inner()
            .operation_id;
        let op2 = svc
            .write_operation(Request::new(WriteOperationRequest {
                repo_id: "repo-conc".to_string(),
                operation: Some(Operation {
                    view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
                    parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                    metadata: Some(OperationMetadata {
                        description: "client-2".to_string(),
                        ..Default::default()
                    }),
                    commit_predecessors_set: true,
                    ..Default::default()
                }),
            }))
            .await
            .unwrap()
            .into_inner()
            .operation_id;

        let heads1 = svc
            .update_op_heads(Request::new(UpdateOpHeadsRequest {
                repo_id: "repo-conc".to_string(),
                old_op_head_ids: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                new_op_head_id: op1.clone(),
            }))
            .await
            .unwrap()
            .into_inner()
            .current_op_head_ids;
        assert_eq!(heads1, vec![op1.clone()]);

        let heads2 = svc
            .update_op_heads(Request::new(UpdateOpHeadsRequest {
                repo_id: "repo-conc".to_string(),
                old_op_head_ids: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                new_op_head_id: op2.clone(),
            }))
            .await
            .unwrap()
            .into_inner()
            .current_op_head_ids;
        assert_eq!(heads2, vec![op1.clone(), op2.clone()]);

        let op3 = svc
            .write_operation(Request::new(WriteOperationRequest {
                repo_id: "repo-conc".to_string(),
                operation: Some(Operation {
                    view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
                    parents: vec![op1.clone(), op2.clone()],
                    metadata: Some(OperationMetadata {
                        description: "merge".to_string(),
                        ..Default::default()
                    }),
                    commit_predecessors_set: true,
                    ..Default::default()
                }),
            }))
            .await
            .unwrap()
            .into_inner()
            .operation_id;
        let heads3 = svc
            .update_op_heads(Request::new(UpdateOpHeadsRequest {
                repo_id: "repo-conc".to_string(),
                old_op_head_ids: vec![op1.clone(), op2.clone()],
                new_op_head_id: op3.clone(),
            }))
            .await
            .unwrap()
            .into_inner()
            .current_op_head_ids;
        assert_eq!(heads3, vec![op3.clone()]);

        let retry_heads = svc
            .update_op_heads(Request::new(UpdateOpHeadsRequest {
                repo_id: "repo-conc".to_string(),
                old_op_head_ids: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                new_op_head_id: op1,
            }))
            .await
            .unwrap()
            .into_inner()
            .current_op_head_ids;
        assert_eq!(retry_heads, vec![op3]);
    }

    #[tokio::test]
    async fn write_view_validates_heads_bookmarks_and_commit_preconditions() {
        let store = Arc::new(MemoryStore::new());
        store.register_repo("repo-view".to_string()).await;
        let svc = CommitCloudOpStoreService::new(store.clone());

        // Empty head_ids is rejected.
        assert_eq!(
            svc.write_view(Request::new(WriteViewRequest {
                repo_id: "repo-view".to_string(),
                view: Some(View::default()),
            }))
            .await
            .unwrap_err()
            .code(),
            Code::InvalidArgument
        );

        // Absent local_bookmarks RefTarget is rejected.
        let mut absent_bm = HashMap::new();
        absent_bm.insert("main".to_string(), RefTarget::default());
        assert_eq!(
            svc.write_view(Request::new(WriteViewRequest {
                repo_id: "repo-view".to_string(),
                view: Some(View {
                    head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                    local_bookmarks: absent_bm,
                    ..Default::default()
                }),
            }))
            .await
            .unwrap_err()
            .code(),
            Code::InvalidArgument
        );

        // Unpersisted commit in head_ids fails with FAILED_PRECONDITION.
        let missing_commit = vec![0x44; cc_common::COMMIT_ID_LENGTH];
        assert_eq!(
            svc.write_view(Request::new(WriteViewRequest {
                repo_id: "repo-view".to_string(),
                view: Some(View {
                    head_ids: vec![missing_commit.clone()],
                    ..Default::default()
                }),
            }))
            .await
            .unwrap_err()
            .code(),
            Code::FailedPrecondition
        );

        // Once the commit is persisted, WriteView succeeds.
        store
            .put_commit(
                "repo-view".to_string(),
                missing_commit.clone(),
                cc_common::backend::Commit::default(),
            )
            .await;
        let view_id = svc
            .write_view(Request::new(WriteViewRequest {
                repo_id: "repo-view".to_string(),
                view: Some(View {
                    head_ids: vec![missing_commit],
                    ..Default::default()
                }),
            }))
            .await
            .unwrap()
            .into_inner()
            .view_id;
        assert_eq!(view_id.len(), cc_common::VIEW_ID_LENGTH);
    }
}
