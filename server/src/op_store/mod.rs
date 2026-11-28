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

//! gRPC `OpStoreService` implementation backed by [`CompositeStore`].
//!
//! # Authority and Crash-Safety Ordering
//! Clients write `View` objects (`WriteView`) before referencing them in `Operation`
//! nodes (`WriteOperation`), and persist `Operation` nodes before advancing repository
//! heads via `UpdateOpHeads`. `CompositeStore` enforces these dependencies before
//! exposing new operation heads to readers.
//!
//! # Concurrency and Merge Invariants
//! `UpdateOpHeads` delegates to `MetadataStore::update_op_heads` under serializable
//! isolation, removing superseded `old_op_head_ids` while preserving concurrent sibling
//! heads for lock-free client-side reconciliation. `ReconcileOpHeads` enforces optional
//! per-repository [`ReconcileKillSwitch`] policies (`Bypass` and `Quarantine`) while
//! keeping `GetOpHeads` pure and unaffected.
//!
//! # Cross-Process and RPC Boundaries
//! Synthesizes the all-zero root `Operation` and root `View` on read without requiring
//! pre-seeded rows, and resolves hex prefixes via byte-range scans in `ResolveOperationPrefix`.

use std::collections::HashMap;
use std::sync::Arc;

use tracing::info;

use cc_common::op_store::op_store_service_server::OpStoreService;
use cc_common::op_store::*;

use crate::auth::{CallerIdentity, enforce_actor_repo_suffix};
use crate::backend::{validate_digest_len, validate_identifier};
use crate::hash_utils::{hash_operation, hash_view};
use crate::metrics::ServerMetrics;
use crate::reconcile_op_heads::{
    ReconcileKillSwitch, reconcile_repo_op_heads_proto_with_policy_and_metrics,
};
use crate::store::{CompositeStore, IdPrefixResolution};

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

#[derive(Clone)]
pub struct CommitCloudOpStoreService {
    store: Arc<CompositeStore>,
    metrics: ServerMetrics,
    enforce_actor_repo_suffix: bool,
    reconcile_kill_switch: ReconcileKillSwitch,
}

impl CommitCloudOpStoreService {
    pub fn new(store: Arc<CompositeStore>) -> Self {
        Self::with_metrics(store, ServerMetrics::new())
    }

    pub fn with_metrics(store: Arc<CompositeStore>, metrics: ServerMetrics) -> Self {
        Self {
            store,
            metrics,
            enforce_actor_repo_suffix: false,
            reconcile_kill_switch: ReconcileKillSwitch::new(),
        }
    }

    pub fn with_actor_repo_suffix_enforcement(mut self, enforce: bool) -> Self {
        self.enforce_actor_repo_suffix = enforce;
        self
    }

    pub fn with_reconcile_kill_switch(mut self, kill_switch: ReconcileKillSwitch) -> Self {
        self.reconcile_kill_switch = kill_switch;
        self
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
        let caller_identity = request.extensions().get::<CallerIdentity>().cloned();
        let req = request.into_inner();
        validate_identifier(&req.repo_id, "repo_id")?;
        enforce_actor_repo_suffix(
            caller_identity.as_ref(),
            self.enforce_actor_repo_suffix,
            &req.repo_id,
        )?;
        let op = req.operation.ok_or_else(|| {
            tonic::Status::invalid_argument("request should have contained an operation object")
        })?;
        validate_operation(&op)?;

        info!("Writing operation for repo: {}", req.repo_id);
        let op_id = hash_operation(&op).map_err(tonic::Status::invalid_argument)?;
        self.store.put_operation(&req.repo_id, &op_id, &op).await?;

        Ok(tonic::Response::new(WriteOperationResponse {
            operation_id: op_id,
        }))
    }

    async fn resolve_operation_prefix(
        &self,
        request: tonic::Request<ResolveOperationPrefixRequest>,
    ) -> Result<tonic::Response<ResolveOperationPrefixResponse>, tonic::Status> {
        let req = request.into_inner();
        validate_identifier(&req.repo_id, "repo_id")?;
        let resolution = self
            .store
            .resolve_operation_prefix(&req.repo_id, &req.hex_prefix)
            .await?;

        let (status, matched_operation_id) = match resolution {
            IdPrefixResolution::NoMatch => {
                (PrefixResolutionStatus::PrefixResolutionNoMatch, vec![])
            }
            IdPrefixResolution::SingleMatch(id) => {
                (PrefixResolutionStatus::PrefixResolutionSingleMatch, id)
            }
            IdPrefixResolution::AmbiguousMatch => (
                PrefixResolutionStatus::PrefixResolutionAmbiguousMatch,
                vec![],
            ),
        };

        Ok(tonic::Response::new(ResolveOperationPrefixResponse {
            status: status as i32,
            matched_operation_id,
        }))
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
        let caller_identity = request.extensions().get::<CallerIdentity>().cloned();
        let req = request.into_inner();
        validate_identifier(&req.repo_id, "repo_id")?;
        enforce_actor_repo_suffix(
            caller_identity.as_ref(),
            self.enforce_actor_repo_suffix,
            &req.repo_id,
        )?;
        let view = req.view.ok_or_else(|| {
            tonic::Status::invalid_argument("request should have contained a view object")
        })?;
        validate_view(&view)?;

        info!("Writing view for repo: {}", req.repo_id);
        let view_id = hash_view(&view).map_err(tonic::Status::invalid_argument)?;
        self.store.put_view(&req.repo_id, &view_id, &view).await?;

        Ok(tonic::Response::new(WriteViewResponse { view_id }))
    }

    async fn get_op_heads(
        &self,
        request: tonic::Request<GetOpHeadsRequest>,
    ) -> Result<tonic::Response<GetOpHeadsResponse>, tonic::Status> {
        let req = request.into_inner();
        validate_identifier(&req.repo_id, "repo_id")?;
        info!("Get op heads for repo: {}", req.repo_id);
        let heads = self.store.get_op_heads(&req.repo_id).await?;
        Ok(tonic::Response::new(GetOpHeadsResponse {
            op_head_ids: heads,
        }))
    }

    async fn update_op_heads(
        &self,
        request: tonic::Request<UpdateOpHeadsRequest>,
    ) -> Result<tonic::Response<UpdateOpHeadsResponse>, tonic::Status> {
        let caller_identity = request.extensions().get::<CallerIdentity>().cloned();
        let req = request.into_inner();
        validate_identifier(&req.repo_id, "repo_id")?;
        enforce_actor_repo_suffix(
            caller_identity.as_ref(),
            self.enforce_actor_repo_suffix,
            &req.repo_id,
        )?;
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
        let current_heads = self
            .store
            .update_op_heads(&req.repo_id, &req.old_op_head_ids, &req.new_op_head_id)
            .await?;

        Ok(tonic::Response::new(UpdateOpHeadsResponse {
            current_op_head_ids: current_heads,
        }))
    }

    async fn reconcile_op_heads(
        &self,
        request: tonic::Request<ReconcileOpHeadsRequest>,
    ) -> Result<tonic::Response<ReconcileOpHeadsResponse>, tonic::Status> {
        let caller_identity = request.extensions().get::<CallerIdentity>().cloned();
        let req = request.into_inner();
        validate_identifier(&req.repo_id, "repo_id")?;
        enforce_actor_repo_suffix(
            caller_identity.as_ref(),
            self.enforce_actor_repo_suffix,
            &req.repo_id,
        )?;
        info!("Reconcile op heads for repo: {}", req.repo_id);
        let response = reconcile_repo_op_heads_proto_with_policy_and_metrics(
            self.store.clone(),
            &req.repo_id,
            Some(&self.reconcile_kill_switch),
            Some(&self.metrics),
        )
        .await?;
        Ok(tonic::Response::new(response))
    }

    async fn publish_operation(
        &self,
        request: tonic::Request<PublishOperationRequest>,
    ) -> Result<tonic::Response<PublishOperationResponse>, tonic::Status> {
        let caller_identity = request.extensions().get::<CallerIdentity>().cloned();
        let req = request.into_inner();
        validate_identifier(&req.repo_id, "repo_id")?;
        enforce_actor_repo_suffix(
            caller_identity.as_ref(),
            self.enforce_actor_repo_suffix,
            &req.repo_id,
        )?;
        for old_id in &req.old_op_head_ids {
            validate_digest_len(old_id, cc_common::OPERATION_ID_LENGTH, "old_op_head_ids")?;
        }
        let view = req.view.ok_or_else(|| {
            tonic::Status::invalid_argument("PublishOperationRequest must include view")
        })?;
        validate_view(&view)?;
        let mut operation = req.operation.ok_or_else(|| {
            tonic::Status::invalid_argument("PublishOperationRequest must include operation")
        })?;

        let view_id = hash_view(&view).map_err(tonic::Status::invalid_argument)?;
        if operation.view_id.is_empty() {
            operation.view_id = view_id.clone();
        } else if operation.view_id != view_id {
            return Err(tonic::Status::invalid_argument(
                "operation.view_id does not match canonical hash of supplied view",
            ));
        }
        validate_operation(&operation)?;

        let operation_id = hash_operation(&operation).map_err(tonic::Status::invalid_argument)?;
        let current_op_head_ids = self
            .store
            .publish_operation(
                &req.repo_id,
                &view_id,
                &view,
                &operation_id,
                &operation,
                &req.old_op_head_ids,
            )
            .await?;

        Ok(tonic::Response::new(PublishOperationResponse {
            view_id,
            operation_id,
            current_op_head_ids,
        }))
    }
}

#[cfg(test)]
mod tests;
