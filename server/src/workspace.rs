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

//! gRPC `WorkspaceService` implementation persisting per-workspace checkout state in
//! [`Store`].
//!
//! # Authority and Crash-Safety Ordering
//! Clients update `WorkspaceState` (`commit_id`, `operation_id`, `tree_id` / `tree_ids`,
//! `conflict_labels`, and `sparse_patterns`) after committing the underlying `Operation`,
//! `Commit`, and `Tree` objects in `OpStoreService` and `BackendService`. `UpdateWorkspace`
//! verifies that referenced non-root operations/views, commits, and deduplicated trees exist
//! (returning `FAILED_PRECONDITION` if missing) and derives `commit_id` from
//! `View.wc_commit_ids[workspace_name]` when `commit_id` is empty.
//!
//! # Concurrency and Merge Invariants
//! Keyed by `(repo_id, user, workspace_name)`. Upserts support opt-in optimistic CAS via
//! `expected_operation_id` (`ABORTED` on mismatch, idempotent `OK` when already equal), and
//! `DeleteWorkspace` is advisory because active workspaces upsert their record on finish.
//!
//! # Cross-Process and RPC Boundaries
//! `CheckWorkingCopyChanges` resolves `repo_id -> project_id` and compares
//! `WorkspaceState.tree_ids` against `Commit.root_tree_id` (preserving full `Merge<TreeId>`
//! algebraic conflict sides) without client I/O.

// Legacy single-tree fields (`tree_id`, `current_tree_id`, `commit_tree_id`) remain
// populated for legacy single-tree clients.
#![allow(deprecated)]

use std::collections::HashSet;
use std::sync::Arc;

use cc_common::workspace::workspace_service_server::WorkspaceService;
use cc_common::workspace::*;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::error_util::ensure_repo_registered_error;
use crate::store::Store;

const DEFAULT_LIST_WORKSPACES_PAGE_SIZE: usize = 500;
const MAX_LIST_WORKSPACES_PAGE_SIZE: usize = 1000;
const MAX_LIST_WORKSPACES_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
const MAX_REPO_ID_LEN: usize = 128;
const MAX_SEGMENT_LEN: usize = 256;
const MAX_WORKSPACE_TREE_TERMS: usize = 1025;
const MAX_CONFLICT_LABELS_BYTES: usize = 64 * 1024;
const MAX_SPARSE_PATTERNS: usize = 4096;
const MAX_SPARSE_PATTERNS_BYTES: usize = 256 * 1024;
const MAX_CURSOR_HEX_LEN: usize = 16 * 1024;

fn validate_repo_id(repo_id: &str) -> Result<(), Status> {
    if repo_id.is_empty()
        || repo_id.len() > MAX_REPO_ID_LEN
        || repo_id == "."
        || repo_id == ".."
        || !repo_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    {
        return Err(Status::invalid_argument(
            "repo_id must be 1..=128 ASCII [A-Za-z0-9._-] characters (excluding '.' and '..')",
        ));
    }
    Ok(())
}

fn validate_key_segment(val: &str, field: &str, allow_empty: bool) -> Result<(), Status> {
    if val.is_empty() {
        return if allow_empty {
            Ok(())
        } else {
            Err(Status::invalid_argument(format!(
                "{field} must not be empty"
            )))
        };
    }
    if val.len() > MAX_SEGMENT_LEN || val.trim().is_empty() || val.chars().any(char::is_control) {
        return Err(Status::invalid_argument(format!(
            "{field} must be <= {MAX_SEGMENT_LEN} non-control bytes and not whitespace-only"
        )));
    }
    Ok(())
}

fn validate_sparse_pattern(pat: &str) -> Result<(), Status> {
    if pat.is_empty() {
        return Ok(());
    }
    if pat.starts_with('/') || pat.ends_with('/') || pat.contains("//") || pat.contains('\0') {
        return Err(Status::invalid_argument(format!(
            "invalid RepoPath sparse pattern {pat:?}"
        )));
    }
    for comp in pat.split('/') {
        if comp.is_empty() || comp == "." || comp == ".." {
            return Err(Status::invalid_argument(format!(
                "invalid path component {comp:?} in sparse pattern {pat:?}"
            )));
        }
    }
    Ok(())
}

fn encode_workspace_cursor(user: &str, workspace_name: &str) -> String {
    let mut buf = Vec::with_capacity(4 + user.len() + workspace_name.len());
    buf.extend_from_slice(&(user.len() as u32).to_be_bytes());
    buf.extend_from_slice(user.as_bytes());
    buf.extend_from_slice(workspace_name.as_bytes());
    hex::encode(buf)
}

fn decode_workspace_cursor(token: &str) -> Result<(String, String), Status> {
    if token.len() > MAX_CURSOR_HEX_LEN {
        return Err(Status::invalid_argument(
            "malformed ListWorkspaces page_token",
        ));
    }
    let raw = hex::decode(token)
        .map_err(|_| Status::invalid_argument("malformed ListWorkspaces page_token"))?;
    if raw.len() < 4 {
        return Err(Status::invalid_argument(
            "malformed ListWorkspaces page_token",
        ));
    }
    let user_len = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
    if raw.len() - 4 < user_len {
        return Err(Status::invalid_argument(
            "malformed ListWorkspaces page_token",
        ));
    }
    let user = String::from_utf8(raw[4..4 + user_len].to_vec())
        .map_err(|_| Status::invalid_argument("malformed ListWorkspaces page_token"))?;
    let ws = String::from_utf8(raw[4 + user_len..].to_vec())
        .map_err(|_| Status::invalid_argument("malformed ListWorkspaces page_token"))?;
    Ok((user, ws))
}

#[derive(Clone)]
pub struct CommitCloudWorkspaceService {
    store: Arc<dyn Store>,
}

impl CommitCloudWorkspaceService {
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl WorkspaceService for CommitCloudWorkspaceService {
    async fn get_workspace(
        &self,
        request: Request<GetWorkspaceRequest>,
    ) -> Result<Response<GetWorkspaceResponse>, Status> {
        let req = request.into_inner();
        validate_repo_id(&req.repo_id)?;
        validate_key_segment(&req.user, "user", true)?;
        validate_key_segment(&req.workspace_name, "workspace_name", false)?;
        ensure_repo_registered_error(self.store.as_ref(), &req.repo_id, "reading workspaces")
            .await?;
        let workspace = self
            .store
            .get_workspace(&req.repo_id, &req.user, &req.workspace_name)
            .await?;
        Ok(Response::new(GetWorkspaceResponse { workspace }))
    }

    async fn update_workspace(
        &self,
        request: Request<UpdateWorkspaceRequest>,
    ) -> Result<Response<UpdateWorkspaceResponse>, Status> {
        let req = request.into_inner();
        let expected_operation_id = req.expected_operation_id;
        if let Some(ref exp) = expected_operation_id
            && !exp.is_empty()
            && exp.len() != cc_common::OPERATION_ID_LENGTH
        {
            return Err(Status::invalid_argument(format!(
                "UpdateWorkspaceRequest expected_operation_id must be 0 or {} bytes (got {} bytes)",
                cc_common::OPERATION_ID_LENGTH,
                exp.len()
            )));
        }
        let mut ws = req
            .workspace
            .ok_or_else(|| Status::invalid_argument("WorkspaceState payload is required"))?;
        validate_repo_id(&ws.repo_id)?;
        validate_key_segment(&ws.user, "user", true)?;
        validate_key_segment(&ws.workspace_name, "workspace_name", false)?;

        if ws.tree_id.is_empty() && ws.tree_ids.is_empty() {
            return Err(Status::invalid_argument(
                "at least one of workspace tree_id or tree_ids must be populated",
            ));
        }
        if !ws.tree_ids.is_empty() {
            if ws.tree_ids.len() > MAX_WORKSPACE_TREE_TERMS {
                return Err(Status::invalid_argument(format!(
                    "workspace tree_ids count ({}) exceeds maximum ({MAX_WORKSPACE_TREE_TERMS})",
                    ws.tree_ids.len()
                )));
            }
            if ws.tree_ids.len().is_multiple_of(2) {
                return Err(Status::invalid_argument(format!(
                    "workspace tree_ids must have odd length (got {})",
                    ws.tree_ids.len()
                )));
            }
            for tid in &ws.tree_ids {
                if tid.len() != cc_common::EMPTY_TREE_ID_BYTES.len() {
                    return Err(Status::invalid_argument(format!(
                        "workspace tree_ids entry must be {} bytes (got {} bytes)",
                        cc_common::EMPTY_TREE_ID_BYTES.len(),
                        tid.len()
                    )));
                }
            }
            if !ws.tree_id.is_empty() && ws.tree_id != ws.tree_ids[0] {
                return Err(Status::invalid_argument(
                    "workspace tree_id does not match tree_ids[0]",
                ));
            }
            ws.tree_id = ws.tree_ids[0].clone();
        } else {
            if ws.tree_id.len() != cc_common::EMPTY_TREE_ID_BYTES.len() {
                return Err(Status::invalid_argument(format!(
                    "workspace tree_id must be {} bytes (got {} bytes)",
                    cc_common::EMPTY_TREE_ID_BYTES.len(),
                    ws.tree_id.len()
                )));
            }
            ws.tree_ids = vec![ws.tree_id.clone()];
        }

        let valid_label_len =
            ws.conflict_labels.is_empty() || ws.conflict_labels.len() == ws.tree_ids.len();
        if !valid_label_len {
            return Err(Status::invalid_argument(format!(
                "workspace conflict_labels count ({}) must be 0 or match tree_ids count ({})",
                ws.conflict_labels.len(),
                ws.tree_ids.len()
            )));
        }
        let mut total_label_bytes = 0usize;
        for label in &ws.conflict_labels {
            if label.contains('\n') {
                return Err(Status::invalid_argument(
                    "workspace conflict_labels must not contain newline characters",
                ));
            }
            total_label_bytes = total_label_bytes.saturating_add(label.len());
            if total_label_bytes > MAX_CONFLICT_LABELS_BYTES {
                return Err(Status::invalid_argument(format!(
                    "workspace conflict_labels total bytes exceed maximum ({MAX_CONFLICT_LABELS_BYTES})"
                )));
            }
        }
        if ws.conflict_labels.iter().all(|l| l.is_empty()) {
            ws.conflict_labels.clear();
        } else if ws.tree_ids.len() == 1 {
            return Err(Status::invalid_argument(
                "non-empty conflict_labels are not allowed on a resolved single-tree workspace",
            ));
        }

        if ws.sparse_patterns.len() > MAX_SPARSE_PATTERNS {
            return Err(Status::invalid_argument(format!(
                "workspace sparse_patterns count ({}) exceeds maximum ({MAX_SPARSE_PATTERNS})",
                ws.sparse_patterns.len()
            )));
        }
        let mut total_sparse_bytes = 0usize;
        for pat in &ws.sparse_patterns {
            total_sparse_bytes = total_sparse_bytes.saturating_add(pat.len());
            if total_sparse_bytes > MAX_SPARSE_PATTERNS_BYTES {
                return Err(Status::invalid_argument(format!(
                    "workspace sparse_patterns total bytes exceed maximum ({MAX_SPARSE_PATTERNS_BYTES})"
                )));
            }
            validate_sparse_pattern(pat)?;
        }
        if ws.sparse_patterns.is_empty() && !ws.sparse_patterns_set {
            ws.sparse_patterns.push(String::new());
        }
        ws.sparse_patterns_set = true;

        if ws.operation_id.is_empty() {
            ws.operation_id = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
        } else if ws.operation_id.len() != cc_common::OPERATION_ID_LENGTH {
            return Err(Status::invalid_argument(format!(
                "workspace operation_id must be {} bytes (got {} bytes)",
                cc_common::OPERATION_ID_LENGTH,
                ws.operation_id.len()
            )));
        }
        if !ws.commit_id.is_empty() && ws.commit_id.len() != cc_common::COMMIT_ID_LENGTH {
            return Err(Status::invalid_argument(format!(
                "workspace commit_id must be {} bytes (got {} bytes)",
                cc_common::COMMIT_ID_LENGTH,
                ws.commit_id.len()
            )));
        }

        let project_id = self
            .store
            .get_repo_project_id(&ws.repo_id)
            .await?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "repository should have been registered before updating workspace: {}",
                    ws.repo_id
                ))
            })?;

        let mut derived_from_view: Option<Vec<u8>> = None;
        if ws.operation_id.as_slice() != cc_common::ROOT_OPERATION_ID_BYTES {
            let op = self
                .store
                .get_operation(&ws.repo_id, &ws.operation_id)
                .await?
                .ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "workspace operation {} does not exist in repo '{}'; publish operation before updating workspace",
                        hex::encode(&ws.operation_id),
                        ws.repo_id
                    ))
                })?;
            if op.view_id.as_slice() != cc_common::ROOT_VIEW_ID_BYTES {
                if ws.commit_id.is_empty() {
                    let view = self
                        .store
                        .get_view(&ws.repo_id, &op.view_id)
                        .await?
                        .ok_or_else(|| {
                            Status::failed_precondition(format!(
                                "view {} referenced by workspace operation {} does not exist in repo '{}'",
                                hex::encode(&op.view_id),
                                hex::encode(&ws.operation_id),
                                ws.repo_id
                            ))
                        })?;
                    derived_from_view = view.wc_commit_ids.get(&ws.workspace_name).cloned();
                } else if !self.store.has_view(&ws.repo_id, &op.view_id).await? {
                    return Err(Status::failed_precondition(format!(
                        "view {} referenced by workspace operation {} does not exist in repo '{}'",
                        hex::encode(&op.view_id),
                        hex::encode(&ws.operation_id),
                        ws.repo_id
                    )));
                }
            }
        }

        if ws.commit_id.is_empty() {
            if let Some(wc_id) = derived_from_view {
                ws.commit_id = wc_id;
            } else if ws.operation_id.as_slice() == cc_common::ROOT_OPERATION_ID_BYTES {
                ws.commit_id = cc_common::ROOT_COMMIT_ID_BYTES.to_vec();
            } else {
                return Err(Status::invalid_argument(format!(
                    "workspace commit_id is empty and operation {} has no wc_commit_ids entry for '{}'",
                    hex::encode(&ws.operation_id),
                    ws.workspace_name
                )));
            }
        }
        if ws.commit_id.len() != cc_common::COMMIT_ID_LENGTH {
            return Err(Status::invalid_argument(format!(
                "workspace commit_id must be {} bytes (got {} bytes)",
                cc_common::COMMIT_ID_LENGTH,
                ws.commit_id.len()
            )));
        }

        if ws.commit_id.as_slice() != cc_common::ROOT_COMMIT_ID_BYTES
            && !self.store.has_commit(&project_id, &ws.commit_id).await?
        {
            return Err(Status::failed_precondition(format!(
                "workspace commit {} does not exist in project '{}'; write commit before updating workspace",
                hex::encode(&ws.commit_id),
                project_id
            )));
        }

        let mut seen_trees: HashSet<&[u8]> = HashSet::with_capacity(ws.tree_ids.len());
        let unique_trees: Vec<Vec<u8>> = ws
            .tree_ids
            .iter()
            .filter(|tid| {
                tid.as_slice() != cc_common::EMPTY_TREE_ID_BYTES
                    && seen_trees.insert(tid.as_slice())
            })
            .cloned()
            .collect();
        if !unique_trees.is_empty() {
            let existing_trees = self
                .store
                .find_existing_trees(&project_id, &unique_trees)
                .await?;
            for tid in &unique_trees {
                if !existing_trees.contains(tid) {
                    return Err(Status::failed_precondition(format!(
                        "workspace tree {} does not exist in project '{}'; write tree before updating workspace",
                        hex::encode(tid),
                        project_id
                    )));
                }
            }
        }

        info!(
            "Update workspace '{}' for user '{}' in repo '{}'",
            ws.workspace_name, ws.user, ws.repo_id
        );
        self.store
            .put_workspace_cas(ws.clone(), expected_operation_id.as_deref())
            .await?;
        Ok(Response::new(UpdateWorkspaceResponse {
            success: true,
            workspace: Some(ws),
        }))
    }

    async fn list_workspaces(
        &self,
        request: Request<ListWorkspacesRequest>,
    ) -> Result<Response<ListWorkspacesResponse>, Status> {
        let req = request.into_inner();
        validate_repo_id(&req.repo_id)?;
        let cursor = if req.page_token.is_empty() {
            None
        } else {
            Some(decode_workspace_cursor(&req.page_token)?)
        };
        let limit = if req.page_size == 0 {
            DEFAULT_LIST_WORKSPACES_PAGE_SIZE
        } else {
            (req.page_size as usize).min(MAX_LIST_WORKSPACES_PAGE_SIZE)
        };
        ensure_repo_registered_error(self.store.as_ref(), &req.repo_id, "listing workspaces")
            .await?;

        let cursor_ref = cursor
            .as_ref()
            .map(|(user, ws)| (user.as_str(), ws.as_str()));
        let (workspaces, has_more) = self
            .store
            .list_workspaces_page(
                &req.repo_id,
                cursor_ref,
                limit,
                Some(MAX_LIST_WORKSPACES_RESPONSE_BYTES),
            )
            .await?;
        let next_page_token = if has_more {
            let last = workspaces.last().expect("page has at least one record");
            encode_workspace_cursor(&last.user, &last.workspace_name)
        } else {
            String::new()
        };

        Ok(Response::new(ListWorkspacesResponse {
            workspaces,
            next_page_token,
        }))
    }

    async fn check_working_copy_changes(
        &self,
        request: Request<CheckWorkingCopyChangesRequest>,
    ) -> Result<Response<CheckWorkingCopyChangesResponse>, Status> {
        let req = request.into_inner();
        validate_repo_id(&req.repo_id)?;
        validate_key_segment(&req.user, "user", true)?;
        validate_key_segment(&req.workspace_name, "workspace_name", false)?;
        let project_id = self
            .store
            .get_repo_project_id(&req.repo_id)
            .await?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "repository should have been registered before checking working copy changes: {}",
                    req.repo_id
                ))
            })?;
        let ws = self
            .store
            .get_workspace(&req.repo_id, &req.user, &req.workspace_name)
            .await?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "workspace '{}' for user '{}' not found in repo '{}'",
                    req.workspace_name, req.user, req.repo_id
                ))
            })?;

        let current_tree_ids = if !ws.tree_ids.is_empty() {
            ws.tree_ids
        } else if !ws.tree_id.is_empty() {
            vec![ws.tree_id.clone()]
        } else {
            vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()]
        };

        let commit_tree_ids = if ws.commit_id.is_empty()
            || ws.commit_id.as_slice() == cc_common::ROOT_COMMIT_ID_BYTES
        {
            vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()]
        } else {
            let commit = self
                .store
                .get_commit(&project_id, &ws.commit_id)
                .await?
                .ok_or_else(|| Status::not_found("Commit not found"))?;
            if commit.root_tree_id.is_empty() {
                vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()]
            } else {
                commit.root_tree_id
            }
        };

        let current_tree_id = current_tree_ids
            .first()
            .cloned()
            .unwrap_or_else(|| cc_common::EMPTY_TREE_ID_BYTES.to_vec());
        let commit_tree_id = commit_tree_ids
            .first()
            .cloned()
            .unwrap_or_else(|| cc_common::EMPTY_TREE_ID_BYTES.to_vec());
        let has_changes = current_tree_ids != commit_tree_ids;

        Ok(Response::new(CheckWorkingCopyChangesResponse {
            has_changes,
            current_tree_id,
            commit_tree_id,
            current_tree_ids,
            commit_tree_ids,
        }))
    }

    async fn delete_workspace(
        &self,
        request: Request<DeleteWorkspaceRequest>,
    ) -> Result<Response<DeleteWorkspaceResponse>, Status> {
        let req = request.into_inner();
        validate_repo_id(&req.repo_id)?;
        validate_key_segment(&req.user, "user", true)?;
        validate_key_segment(&req.workspace_name, "workspace_name", false)?;
        info!(
            "Delete workspace '{}' for user '{}' in repo '{}'",
            req.workspace_name, req.user, req.repo_id
        );
        let existed = self
            .store
            .delete_workspace(&req.repo_id, &req.user, &req.workspace_name)
            .await?;
        Ok(Response::new(DeleteWorkspaceResponse { existed }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{MemoryStore, SqliteStore};
    use prost::Message as _;
    use std::collections::HashMap;
    use tonic::Code;

    async fn setup() -> (Arc<MemoryStore>, CommitCloudWorkspaceService) {
        let store = Arc::new(MemoryStore::default());
        store
            .register_repo("repo-1".to_string(), "proj-1".to_string(), None)
            .await
            .unwrap();
        let svc = CommitCloudWorkspaceService::new(store.clone());
        (store, svc)
    }

    fn base_ws(name: &str) -> WorkspaceState {
        WorkspaceState {
            repo_id: "repo-1".to_string(),
            user: "alice".to_string(),
            workspace_name: name.to_string(),
            commit_id: cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
            operation_id: cc_common::ROOT_OPERATION_ID_BYTES.to_vec(),
            tree_id: cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
            sparse_patterns: vec!["".to_string()],
            tree_ids: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
            conflict_labels: Vec::new(),
            sparse_patterns_set: true,
        }
    }

    #[tokio::test]
    async fn test_memory_store_cas_and_concurrent_race() {
        let (store, svc) = setup().await;
        let op1 = vec![0x11; cc_common::OPERATION_ID_LENGTH];
        let op2 = vec![0x22; cc_common::OPERATION_ID_LENGTH];
        let op3 = vec![0x33; cc_common::OPERATION_ID_LENGTH];
        for op_id in [&op1, &op2, &op3] {
            store
                .put_operation(
                    "repo-1".to_string(),
                    op_id.clone(),
                    cc_common::op_store::Operation {
                        view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
                        parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                        metadata: None,
                        commit_predecessors: Vec::new(),
                        commit_predecessors_set: true,
                    },
                )
                .await
                .unwrap();
        }

        // Create-only CAS succeeds when absent, then fails when already created with different op.
        let mut ws1 = base_ws("ws");
        ws1.operation_id = op1.clone();
        svc.update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(ws1.clone()),
            expected_operation_id: Some(Vec::new()),
        }))
        .await
        .unwrap();

        // Idempotent retry with identical normalized state succeeds.
        svc.update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(ws1.clone()),
            expected_operation_id: Some(Vec::new()),
        }))
        .await
        .unwrap();

        // Concurrent racing CAS writers from op1: exactly one wins, one gets ABORTED.
        let mut ws2 = ws1.clone();
        ws2.operation_id = op2.clone();
        let mut ws3 = ws1.clone();
        ws3.operation_id = op3.clone();
        let (r1, r2) = tokio::join!(
            svc.update_workspace(Request::new(UpdateWorkspaceRequest {
                workspace: Some(ws2),
                expected_operation_id: Some(op1.clone()),
            })),
            svc.update_workspace(Request::new(UpdateWorkspaceRequest {
                workspace: Some(ws3),
                expected_operation_id: Some(op1),
            })),
        );
        let ok_count = [r1.is_ok(), r2.is_ok()].into_iter().filter(|b| *b).count();
        assert_eq!(ok_count, 1, "exactly one racing CAS writer must win");
        let err = r1.err().or_else(|| r2.err()).unwrap();
        assert_eq!(err.code(), Code::Aborted);
    }

    #[tokio::test]
    async fn test_validation_matrix_conflict_labels_preconditions_and_byte_budget() {
        let (store, svc) = setup().await;
        let t1 = vec![0x11; 20];
        let t2 = vec![0x22; 20];
        let t3 = vec![0x33; 20];
        for t in [&t1, &t2, &t3] {
            store
                .put_tree("proj-1".to_string(), t.clone(), Vec::new())
                .await
                .unwrap();
        }

        // Round-trip conflicted workspace with labels via UpdateWorkspaceResponse and GetWorkspace.
        let mut conflicted = base_ws("conflicted");
        conflicted.tree_id = t1.clone();
        conflicted.tree_ids = vec![t1.clone(), t2.clone(), t3.clone()];
        conflicted.conflict_labels = vec!["side-a".into(), "base".into(), "side-b".into()];
        let updated = svc
            .update_workspace(Request::new(UpdateWorkspaceRequest {
                workspace: Some(conflicted.clone()),
                expected_operation_id: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(updated.success);
        assert_eq!(updated.workspace.as_ref(), Some(&conflicted));
        let fetched = svc
            .get_workspace(Request::new(GetWorkspaceRequest {
                repo_id: "repo-1".to_string(),
                user: "alice".to_string(),
                workspace_name: "conflicted".to_string(),
            }))
            .await
            .unwrap()
            .into_inner()
            .workspace
            .unwrap();
        assert_eq!(fetched.tree_ids, conflicted.tree_ids);
        assert_eq!(fetched.conflict_labels, conflicted.conflict_labels);

        // Length-1 `[""]` on a 3-tree conflicted workspace is rejected (must be 0 or tree_ids.len()).
        let mut bad_len_labels = conflicted.clone();
        bad_len_labels.conflict_labels = vec![String::new()];
        assert_eq!(
            svc.update_workspace(Request::new(UpdateWorkspaceRequest {
                workspace: Some(bad_len_labels),
                expected_operation_id: None,
            }))
            .await
            .unwrap_err()
            .code(),
            Code::InvalidArgument
        );

        // Oversized conflict_labels rejected.
        let mut huge_labels = conflicted.clone();
        huge_labels.conflict_labels =
            vec!["a".repeat(30_000), "b".repeat(30_000), "c".repeat(30_000)];
        assert_eq!(
            svc.update_workspace(Request::new(UpdateWorkspaceRequest {
                workspace: Some(huge_labels),
                expected_operation_id: None,
            }))
            .await
            .unwrap_err()
            .code(),
            Code::InvalidArgument
        );

        // Unicode C1 control char in workspace_name rejected.
        let mut c1_ws = base_ws("bad\u{0085}name");
        c1_ws.user = "alice".to_string();
        assert_eq!(
            svc.update_workspace(Request::new(UpdateWorkspaceRequest {
                workspace: Some(c1_ws),
                expected_operation_id: None,
            }))
            .await
            .unwrap_err()
            .code(),
            Code::InvalidArgument
        );

        // tree_id != tree_ids[0], even tree_ids, bad expected_operation_id, bad sparse pattern.
        let mut mismatch_tree = base_ws("m1");
        mismatch_tree.tree_id = t2.clone();
        mismatch_tree.tree_ids = vec![t1.clone()];
        assert_eq!(
            svc.update_workspace(Request::new(UpdateWorkspaceRequest {
                workspace: Some(mismatch_tree),
                expected_operation_id: None,
            }))
            .await
            .unwrap_err()
            .code(),
            Code::InvalidArgument
        );

        let mut even_trees = base_ws("m2");
        even_trees.tree_id = t1.clone();
        even_trees.tree_ids = vec![t1.clone(), t2.clone()];
        assert_eq!(
            svc.update_workspace(Request::new(UpdateWorkspaceRequest {
                workspace: Some(even_trees),
                expected_operation_id: None,
            }))
            .await
            .unwrap_err()
            .code(),
            Code::InvalidArgument
        );

        let mut bad_sparse = base_ws("m3");
        bad_sparse.sparse_patterns = vec!["../escape".to_string()];
        assert_eq!(
            svc.update_workspace(Request::new(UpdateWorkspaceRequest {
                workspace: Some(bad_sparse),
                expected_operation_id: None,
            }))
            .await
            .unwrap_err()
            .code(),
            Code::InvalidArgument
        );

        assert_eq!(
            svc.update_workspace(Request::new(UpdateWorkspaceRequest {
                workspace: Some(base_ws("m4")),
                expected_operation_id: Some(vec![0xaa; 3]),
            }))
            .await
            .unwrap_err()
            .code(),
            Code::InvalidArgument
        );

        // Missing non-root Commit -> FAILED_PRECONDITION.
        let mut missing_commit = base_ws("m5");
        missing_commit.commit_id = vec![0x99; cc_common::COMMIT_ID_LENGTH];
        assert_eq!(
            svc.update_workspace(Request::new(UpdateWorkspaceRequest {
                workspace: Some(missing_commit),
                expected_operation_id: None,
            }))
            .await
            .unwrap_err()
            .code(),
            Code::FailedPrecondition
        );

        // Missing non-root View referenced by Operation -> FAILED_PRECONDITION.
        let op_missing_view = vec![0x44; cc_common::OPERATION_ID_LENGTH];
        store
            .put_operation(
                "repo-1".to_string(),
                op_missing_view.clone(),
                cc_common::op_store::Operation {
                    view_id: vec![0x55; cc_common::VIEW_ID_LENGTH],
                    parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                    metadata: None,
                    commit_predecessors: Vec::new(),
                    commit_predecessors_set: true,
                },
            )
            .await
            .unwrap();
        let mut missing_view_ws = base_ws("m6");
        missing_view_ws.operation_id = op_missing_view;
        assert_eq!(
            svc.update_workspace(Request::new(UpdateWorkspaceRequest {
                workspace: Some(missing_view_ws),
                expected_operation_id: None,
            }))
            .await
            .unwrap_err()
            .code(),
            Code::FailedPrecondition
        );

        // Empty commit_id with view lacking wc_commit_ids[workspace_name] -> INVALID_ARGUMENT.
        let valid_view = vec![0x66; cc_common::VIEW_ID_LENGTH];
        store
            .put_view(
                "repo-1".to_string(),
                valid_view.clone(),
                cc_common::op_store::View {
                    head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                    wc_commit_ids: HashMap::new(),
                    local_bookmarks: HashMap::new(),
                    remote_bookmarks: HashMap::new(),
                },
            )
            .await
            .unwrap();
        let op_empty_wc = vec![0x77; cc_common::OPERATION_ID_LENGTH];
        store
            .put_operation(
                "repo-1".to_string(),
                op_empty_wc.clone(),
                cc_common::op_store::Operation {
                    view_id: valid_view,
                    parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                    metadata: None,
                    commit_predecessors: Vec::new(),
                    commit_predecessors_set: true,
                },
            )
            .await
            .unwrap();
        let mut no_wc_ws = base_ws("missing-wc");
        no_wc_ws.commit_id.clear();
        no_wc_ws.operation_id = op_empty_wc;
        assert_eq!(
            svc.update_workspace(Request::new(UpdateWorkspaceRequest {
                workspace: Some(no_wc_ws),
                expected_operation_id: None,
            }))
            .await
            .unwrap_err()
            .code(),
            Code::InvalidArgument
        );

        // ListWorkspaces enforces 2 MiB response byte budget even with page_size = 0.
        let big_patterns: Vec<String> = (0..1024)
            .map(|i| format!("dir{i:04}/{}", "x".repeat(200)))
            .collect();
        for i in 0..15 {
            let mut ws = base_ws(&format!("big-{i:02}"));
            ws.sparse_patterns = big_patterns.clone();
            svc.update_workspace(Request::new(UpdateWorkspaceRequest {
                workspace: Some(ws),
                expected_operation_id: None,
            }))
            .await
            .unwrap();
        }
        let page1 = svc
            .list_workspaces(Request::new(ListWorkspacesRequest {
                repo_id: "repo-1".to_string(),
                page_size: 0,
                page_token: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(
            !page1.next_page_token.is_empty(),
            "expected byte-budget pagination"
        );
        assert!(page1.encoded_len() <= MAX_LIST_WORKSPACES_RESPONSE_BYTES + 1024);

        // Follow pages to exhaustion and verify all 16 records (conflicted + 15 big) are returned.
        let mut total = page1.workspaces.len();
        let mut token = page1.next_page_token;
        while !token.is_empty() {
            let p = svc
                .list_workspaces(Request::new(ListWorkspacesRequest {
                    repo_id: "repo-1".to_string(),
                    page_size: 0,
                    page_token: token,
                }))
                .await
                .unwrap()
                .into_inner();
            assert!(!p.workspaces.is_empty());
            total += p.workspaces.len();
            token = p.next_page_token;
        }
        assert_eq!(total, 16);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_sqlite_store_cas_and_concurrent_race() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("cas_race.db");
        let store = Arc::new(SqliteStore::open(&db_path).unwrap());
        store
            .register_repo("repo-1".to_string(), "proj-1".to_string(), None)
            .await
            .unwrap();
        let svc = CommitCloudWorkspaceService::new(store.clone());

        let op_init = vec![0x01; cc_common::OPERATION_ID_LENGTH];
        let writer_ops: Vec<Vec<u8>> = (0..8u8)
            .map(|i| vec![0x10 + i; cc_common::OPERATION_ID_LENGTH])
            .collect();
        for op_id in std::iter::once(&op_init).chain(writer_ops.iter()) {
            store
                .put_operation(
                    "repo-1".to_string(),
                    op_id.clone(),
                    cc_common::op_store::Operation {
                        view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
                        parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                        metadata: None,
                        commit_predecessors: Vec::new(),
                        commit_predecessors_set: true,
                    },
                )
                .await
                .unwrap();
        }

        // Create-only CAS succeeds on insert and is idempotent on exact retry.
        let mut initial_ws = base_ws("ws");
        initial_ws.operation_id = op_init.clone();
        svc.update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(initial_ws.clone()),
            expected_operation_id: Some(Vec::new()),
        }))
        .await
        .unwrap();
        svc.update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(initial_ws.clone()),
            expected_operation_id: Some(Vec::new()),
        }))
        .await
        .unwrap();

        // Race 8 concurrent CAS writers from `op_init`: exactly 1 wins, 7 receive ABORTED.
        let handles: Vec<_> = writer_ops
            .iter()
            .cloned()
            .map(|op_id| {
                let svc = svc.clone();
                let mut next_ws = initial_ws.clone();
                next_ws.operation_id = op_id.clone();
                let expected = op_init.clone();
                tokio::spawn(async move {
                    let res = svc
                        .update_workspace(Request::new(UpdateWorkspaceRequest {
                            workspace: Some(next_ws),
                            expected_operation_id: Some(expected),
                        }))
                        .await;
                    (op_id, res)
                })
            })
            .collect();

        let results = futures::future::join_all(handles).await;
        let mut winners = Vec::new();
        let mut aborted_count = 0usize;
        for joined in results {
            let (op_id, res) = joined.unwrap();
            match res {
                Ok(_) => winners.push(op_id),
                Err(status) => {
                    assert_eq!(status.code(), Code::Aborted);
                    aborted_count += 1;
                }
            }
        }
        assert_eq!(winners.len(), 1, "exactly one racing CAS writer must win");
        assert_eq!(aborted_count, writer_ops.len() - 1);

        let persisted = svc
            .get_workspace(Request::new(GetWorkspaceRequest {
                repo_id: "repo-1".to_string(),
                user: "alice".to_string(),
                workspace_name: "ws".to_string(),
            }))
            .await
            .unwrap()
            .into_inner()
            .workspace
            .unwrap();
        assert_eq!(persisted.operation_id, winners[0]);
    }

    #[tokio::test]
    async fn test_check_working_copy_changes_conflicted_three_term_trees() {
        let (store, svc) = setup().await;
        let t1 = vec![0x11; cc_common::EMPTY_TREE_ID_BYTES.len()];
        let t2 = vec![0x22; cc_common::EMPTY_TREE_ID_BYTES.len()];
        let t3 = vec![0x33; cc_common::EMPTY_TREE_ID_BYTES.len()];
        let t4 = vec![0x44; cc_common::EMPTY_TREE_ID_BYTES.len()];
        for t in [&t1, &t2, &t3, &t4] {
            store
                .put_tree("proj-1".to_string(), t.clone(), Vec::new())
                .await
                .unwrap();
        }

        let commit_id = vec![0xaa; cc_common::COMMIT_ID_LENGTH];
        store
            .put_commit(
                "proj-1".to_string(),
                commit_id.clone(),
                cc_common::backend::Commit {
                    commit_id: commit_id.clone(),
                    change_id: vec![0xbb; cc_common::CHANGE_ID_LENGTH],
                    parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                    root_tree_id: vec![t1.clone(), t2.clone(), t3.clone()],
                    description: "conflicted commit".to_string(),
                    conflict_labels: vec!["side-a".into(), "base".into(), "side-b".into()],
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Matching 3-term conflicted tree reports has_changes = false.
        let mut ws = base_ws("conflicted-ws");
        ws.commit_id = commit_id.clone();
        ws.tree_id = t1.clone();
        ws.tree_ids = vec![t1.clone(), t2.clone(), t3.clone()];
        ws.conflict_labels = vec!["side-a".into(), "base".into(), "side-b".into()];
        svc.update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(ws.clone()),
            expected_operation_id: None,
        }))
        .await
        .unwrap();

        let matching = svc
            .check_working_copy_changes(Request::new(CheckWorkingCopyChangesRequest {
                repo_id: "repo-1".to_string(),
                user: "alice".to_string(),
                workspace_name: "conflicted-ws".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!matching.has_changes);
        assert_eq!(matching.current_tree_id, t1);
        assert_eq!(matching.commit_tree_id, t1);
        assert_eq!(
            matching.current_tree_ids,
            vec![t1.clone(), t2.clone(), t3.clone()]
        );
        assert_eq!(
            matching.commit_tree_ids,
            vec![t1.clone(), t2.clone(), t3.clone()]
        );

        // Differing non-primary side (with identical primary tree_id == t1) reports has_changes = true.
        ws.tree_ids = vec![t1.clone(), t2.clone(), t4.clone()];
        svc.update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(ws),
            expected_operation_id: None,
        }))
        .await
        .unwrap();

        let diverged = svc
            .check_working_copy_changes(Request::new(CheckWorkingCopyChangesRequest {
                repo_id: "repo-1".to_string(),
                user: "alice".to_string(),
                workspace_name: "conflicted-ws".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(diverged.has_changes);
        assert_eq!(diverged.current_tree_id, diverged.commit_tree_id);
        assert_eq!(
            diverged.current_tree_ids,
            vec![t1.clone(), t2.clone(), t4.clone()]
        );
        assert_eq!(diverged.commit_tree_ids, vec![t1, t2, t3]);
    }

    #[tokio::test]
    async fn test_list_workspaces_keyset_pagination_and_byte_budget() {
        async fn verify_backend(store: Arc<dyn Store>) {
            store
                .register_repo("repo-1".to_string(), "proj-1".to_string(), None)
                .await
                .unwrap();
            let svc = CommitCloudWorkspaceService::new(store);

            // Empty repo yields empty page with no next_page_token.
            let empty = svc
                .list_workspaces(Request::new(ListWorkspacesRequest {
                    repo_id: "repo-1".to_string(),
                    page_size: 2,
                    page_token: String::new(),
                }))
                .await
                .unwrap()
                .into_inner();
            assert!(empty.workspaces.is_empty());
            assert!(empty.next_page_token.is_empty());

            // Populate 6 workspaces across 3 users in unsorted order.
            let entries = [
                ("carol", "ws-1"),
                ("alice", "ws-b"),
                ("bob", "ws-2"),
                ("alice", "ws-a"),
                ("bob", "ws-1"),
                ("carol", "ws-2"),
            ];
            for (user, ws_name) in entries {
                let mut ws = base_ws(ws_name);
                ws.user = user.to_string();
                svc.update_workspace(Request::new(UpdateWorkspaceRequest {
                    workspace: Some(ws),
                    expected_operation_id: None,
                }))
                .await
                .unwrap();
            }

            // Paginate with page_size = 2 (exact multiple of 6): 3 pages, final next_page_token is empty.
            let mut collected = Vec::new();
            let mut token = String::new();
            let mut pages = 0usize;
            loop {
                let resp = svc
                    .list_workspaces(Request::new(ListWorkspacesRequest {
                        repo_id: "repo-1".to_string(),
                        page_size: 2,
                        page_token: token,
                    }))
                    .await
                    .unwrap()
                    .into_inner();
                pages += 1;
                assert_eq!(resp.workspaces.len(), 2);
                for ws in resp.workspaces {
                    collected.push((ws.user, ws.workspace_name));
                }
                if resp.next_page_token.is_empty() {
                    break;
                }
                token = resp.next_page_token;
            }
            assert_eq!(pages, 3);
            assert_eq!(
                collected,
                vec![
                    ("alice".to_string(), "ws-a".to_string()),
                    ("alice".to_string(), "ws-b".to_string()),
                    ("bob".to_string(), "ws-1".to_string()),
                    ("bob".to_string(), "ws-2".to_string()),
                    ("carol".to_string(), "ws-1".to_string()),
                    ("carol".to_string(), "ws-2".to_string()),
                ]
            );

            // Cursor past the last workspace returns an empty final page.
            let past_end = svc
                .list_workspaces(Request::new(ListWorkspacesRequest {
                    repo_id: "repo-1".to_string(),
                    page_size: 10,
                    page_token: encode_workspace_cursor("carol", "ws-2"),
                }))
                .await
                .unwrap()
                .into_inner();
            assert!(past_end.workspaces.is_empty());
            assert!(past_end.next_page_token.is_empty());

            // Add large sparse_patterns workspaces to verify 2 MiB byte budget across keyset pages.
            let big_patterns: Vec<String> = (0..1024)
                .map(|i| format!("dir{i:04}/{}", "x".repeat(200)))
                .collect();
            for i in 0..12 {
                let mut ws = base_ws(&format!("big-{i:02}"));
                ws.user = "dave".to_string();
                ws.sparse_patterns = big_patterns.clone();
                svc.update_workspace(Request::new(UpdateWorkspaceRequest {
                    workspace: Some(ws),
                    expected_operation_id: None,
                }))
                .await
                .unwrap();
            }

            let mut all_keys = Vec::new();
            let mut token = String::new();
            let mut page_count = 0usize;
            loop {
                let resp = svc
                    .list_workspaces(Request::new(ListWorkspacesRequest {
                        repo_id: "repo-1".to_string(),
                        page_size: 100,
                        page_token: token,
                    }))
                    .await
                    .unwrap()
                    .into_inner();
                page_count += 1;
                assert!(resp.encoded_len() <= MAX_LIST_WORKSPACES_RESPONSE_BYTES + 1024);
                for ws in resp.workspaces {
                    all_keys.push((ws.user, ws.workspace_name));
                }
                if resp.next_page_token.is_empty() {
                    break;
                }
                token = resp.next_page_token;
            }
            assert!(page_count >= 2, "expected byte-budget pagination");
            assert_eq!(all_keys.len(), 18);
            assert!(all_keys.windows(2).all(|w| w[0] < w[1]));
        }

        verify_backend(Arc::new(MemoryStore::default())).await;
        verify_backend(Arc::new(SqliteStore::in_memory().unwrap())).await;
    }

    #[tokio::test]
    async fn test_legacy_cursor_pagination_and_slash_workspace_names() {
        async fn verify_backend(store: Arc<dyn Store>) {
            store
                .register_repo("repo-1".to_string(), "proj-1".to_string(), None)
                .await
                .unwrap();
            let svc = CommitCloudWorkspaceService::new(store.clone());

            // Insert legacy rows directly into Store that would fail current UpdateWorkspace
            // validation (> 256 bytes, whitespace-only, control chars) plus a valid slash name.
            let oversized_ws = format!("b-legacy/{}", "x".repeat(300));
            let legacy_keys = [
                ("alice", "   ".to_string()),
                ("alice", "a-normal".to_string()),
                ("alice", oversized_ws.clone()),
                ("alice", "c-legacy\u{0007}ctrl".to_string()),
                ("bob", "feature/foo".to_string()),
            ];
            for (user, ws_name) in &legacy_keys {
                let mut ws = base_ws(ws_name);
                ws.user = (*user).to_string();
                store.put_workspace(ws).await.unwrap();
            }

            // Paginate 1 row at a time; cursors emitted for legacy rows must decode cleanly so
            // rows after them remain reachable.
            let mut seen = Vec::new();
            let mut token = String::new();
            loop {
                let resp = svc
                    .list_workspaces(Request::new(ListWorkspacesRequest {
                        repo_id: "repo-1".to_string(),
                        page_size: 1,
                        page_token: token,
                    }))
                    .await
                    .expect("pagination past legacy workspace rows must succeed")
                    .into_inner();
                for ws in resp.workspaces {
                    seen.push((ws.user, ws.workspace_name));
                }
                if resp.next_page_token.is_empty() {
                    break;
                }
                token = resp.next_page_token;
            }
            let expected: Vec<(String, String)> = legacy_keys
                .iter()
                .map(|(u, w)| ((*u).to_string(), w.clone()))
                .collect();
            assert_eq!(seen, expected);

            // Slash-containing workspace names (valid in upstream jj) work across all RPCs.
            let mut slash_ws = base_ws("team/feature/bar");
            slash_ws.user = "carol".to_string();
            let updated = svc
                .update_workspace(Request::new(UpdateWorkspaceRequest {
                    workspace: Some(slash_ws.clone()),
                    expected_operation_id: None,
                }))
                .await
                .expect("workspace_name containing '/' must be accepted")
                .into_inner();
            assert_eq!(updated.workspace.as_ref(), Some(&slash_ws));

            let fetched = svc
                .get_workspace(Request::new(GetWorkspaceRequest {
                    repo_id: "repo-1".to_string(),
                    user: "carol".to_string(),
                    workspace_name: "team/feature/bar".to_string(),
                }))
                .await
                .unwrap()
                .into_inner()
                .workspace;
            assert_eq!(fetched.as_ref(), Some(&slash_ws));

            let check = svc
                .check_working_copy_changes(Request::new(CheckWorkingCopyChangesRequest {
                    repo_id: "repo-1".to_string(),
                    user: "carol".to_string(),
                    workspace_name: "team/feature/bar".to_string(),
                }))
                .await
                .unwrap()
                .into_inner();
            assert!(!check.has_changes);

            let del = svc
                .delete_workspace(Request::new(DeleteWorkspaceRequest {
                    repo_id: "repo-1".to_string(),
                    user: "carol".to_string(),
                    workspace_name: "team/feature/bar".to_string(),
                }))
                .await
                .unwrap()
                .into_inner();
            assert!(del.existed);
        }

        verify_backend(Arc::new(MemoryStore::default())).await;
        verify_backend(Arc::new(SqliteStore::in_memory().unwrap())).await;
    }

    #[tokio::test]
    async fn test_conflict_labels_legacy_normalization_and_view_commit_derivation() {
        let (store, svc) = setup().await;
        let t1 = vec![0x11; cc_common::EMPTY_TREE_ID_BYTES.len()];
        let t2 = vec![0x22; cc_common::EMPTY_TREE_ID_BYTES.len()];
        let t3 = vec![0x33; cc_common::EMPTY_TREE_ID_BYTES.len()];
        for t in [&t1, &t2, &t3] {
            store
                .put_tree("proj-1".to_string(), t.clone(), Vec::new())
                .await
                .unwrap();
        }

        // 1. conflict_labels containing '\n' is rejected.
        let mut newline_labels = base_ws("ws-nl");
        newline_labels.tree_id = t1.clone();
        newline_labels.tree_ids = vec![t1.clone(), t2.clone(), t3.clone()];
        newline_labels.conflict_labels = vec!["bad\nlabel".into(), "base".into(), "side-b".into()];
        assert_eq!(
            svc.update_workspace(Request::new(UpdateWorkspaceRequest {
                workspace: Some(newline_labels),
                expected_operation_id: None,
            }))
            .await
            .unwrap_err()
            .code(),
            Code::InvalidArgument
        );

        // 2. Non-empty conflict_labels on a resolved 1-tree workspace is rejected.
        let mut resolved_labeled = base_ws("ws-resolved-label");
        resolved_labeled.tree_id = t1.clone();
        resolved_labeled.tree_ids = vec![t1.clone()];
        resolved_labeled.conflict_labels = vec!["unexpected".into()];
        assert_eq!(
            svc.update_workspace(Request::new(UpdateWorkspaceRequest {
                workspace: Some(resolved_labeled),
                expected_operation_id: None,
            }))
            .await
            .unwrap_err()
            .code(),
            Code::InvalidArgument
        );

        // 3. All-empty ["", "", ""] canonicalizes to [], while partially-empty
        // ["side-1", "", "side-2"] is preserved verbatim.
        let mut all_empty_labels = base_ws("ws-all-empty-labels");
        all_empty_labels.tree_id = t1.clone();
        all_empty_labels.tree_ids = vec![t1.clone(), t2.clone(), t3.clone()];
        all_empty_labels.conflict_labels = vec![String::new(), String::new(), String::new()];
        let resp = svc
            .update_workspace(Request::new(UpdateWorkspaceRequest {
                workspace: Some(all_empty_labels),
                expected_operation_id: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.workspace.unwrap().conflict_labels.is_empty());
        let fetched_empty = svc
            .get_workspace(Request::new(GetWorkspaceRequest {
                repo_id: "repo-1".to_string(),
                user: "alice".to_string(),
                workspace_name: "ws-all-empty-labels".to_string(),
            }))
            .await
            .unwrap()
            .into_inner()
            .workspace
            .unwrap();
        assert!(fetched_empty.conflict_labels.is_empty());

        let mut partial_labels = base_ws("ws-partial-labels");
        partial_labels.tree_id = t1.clone();
        partial_labels.tree_ids = vec![t1.clone(), t2.clone(), t3.clone()];
        partial_labels.conflict_labels = vec!["side-1".into(), String::new(), "side-2".into()];
        let resp = svc
            .update_workspace(Request::new(UpdateWorkspaceRequest {
                workspace: Some(partial_labels.clone()),
                expected_operation_id: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            resp.workspace.unwrap().conflict_labels,
            partial_labels.conflict_labels
        );
        let fetched_partial = svc
            .get_workspace(Request::new(GetWorkspaceRequest {
                repo_id: "repo-1".to_string(),
                user: "alice".to_string(),
                workspace_name: "ws-partial-labels".to_string(),
            }))
            .await
            .unwrap()
            .into_inner()
            .workspace
            .unwrap();
        assert_eq!(
            fetched_partial.conflict_labels,
            partial_labels.conflict_labels
        );

        // 4. Legacy tree_id-only + sparse_patterns_set = false + empty operation_id + empty
        // commit_id normalize cleanly.
        let legacy_req = WorkspaceState {
            repo_id: "repo-1".to_string(),
            user: "alice".to_string(),
            workspace_name: "ws-legacy-norm".to_string(),
            commit_id: Vec::new(),
            operation_id: Vec::new(),
            tree_id: t1.clone(),
            sparse_patterns: Vec::new(),
            tree_ids: Vec::new(),
            conflict_labels: Vec::new(),
            sparse_patterns_set: false,
        };
        let norm_resp = svc
            .update_workspace(Request::new(UpdateWorkspaceRequest {
                workspace: Some(legacy_req),
                expected_operation_id: None,
            }))
            .await
            .unwrap()
            .into_inner()
            .workspace
            .unwrap();
        assert_eq!(norm_resp.commit_id, cc_common::ROOT_COMMIT_ID_BYTES);
        assert_eq!(norm_resp.operation_id, cc_common::ROOT_OPERATION_ID_BYTES);
        assert_eq!(norm_resp.tree_id, t1);
        assert_eq!(norm_resp.tree_ids, vec![t1.clone()]);
        assert_eq!(norm_resp.sparse_patterns, vec![String::new()]);
        assert!(norm_resp.sparse_patterns_set);

        // 5. Happy-path server-side commit_id derivation from View.wc_commit_ids[workspace_name].
        let derived_commit = vec![0xcc; cc_common::COMMIT_ID_LENGTH];
        store
            .put_commit(
                "proj-1".to_string(),
                derived_commit.clone(),
                cc_common::backend::Commit {
                    commit_id: derived_commit.clone(),
                    change_id: vec![0xdd; cc_common::CHANGE_ID_LENGTH],
                    parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                    root_tree_id: vec![t1.clone()],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let view_id = vec![0xee; cc_common::VIEW_ID_LENGTH];
        let mut wc_commit_ids = HashMap::new();
        wc_commit_ids.insert("derived-ws".to_string(), derived_commit.clone());
        store
            .put_view(
                "repo-1".to_string(),
                view_id.clone(),
                cc_common::op_store::View {
                    head_ids: vec![derived_commit.clone()],
                    wc_commit_ids,
                    local_bookmarks: HashMap::new(),
                    remote_bookmarks: HashMap::new(),
                },
            )
            .await
            .unwrap();
        let op_id = vec![0xfe; cc_common::OPERATION_ID_LENGTH];
        store
            .put_operation(
                "repo-1".to_string(),
                op_id.clone(),
                cc_common::op_store::Operation {
                    view_id,
                    parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                    metadata: None,
                    commit_predecessors: Vec::new(),
                    commit_predecessors_set: true,
                },
            )
            .await
            .unwrap();

        let mut derived_ws = base_ws("derived-ws");
        derived_ws.commit_id.clear();
        derived_ws.operation_id = op_id.clone();
        derived_ws.tree_id = t1.clone();
        derived_ws.tree_ids = vec![t1.clone()];
        let derived_resp = svc
            .update_workspace(Request::new(UpdateWorkspaceRequest {
                workspace: Some(derived_ws),
                expected_operation_id: None,
            }))
            .await
            .unwrap()
            .into_inner()
            .workspace
            .unwrap();
        assert_eq!(derived_resp.commit_id, derived_commit);
        let fetched_derived = svc
            .get_workspace(Request::new(GetWorkspaceRequest {
                repo_id: "repo-1".to_string(),
                user: "alice".to_string(),
                workspace_name: "derived-ws".to_string(),
            }))
            .await
            .unwrap()
            .into_inner()
            .workspace
            .unwrap();
        assert_eq!(fetched_derived.commit_id, derived_commit);

        // Explicit commit_id with a non-root op exercises the lightweight has_view/has_commit path.
        let mut explicit_ws = base_ws("explicit-ws");
        explicit_ws.commit_id = derived_commit.clone();
        explicit_ws.operation_id = op_id;
        explicit_ws.tree_id = t1.clone();
        explicit_ws.tree_ids = vec![t1];
        let explicit_resp = svc
            .update_workspace(Request::new(UpdateWorkspaceRequest {
                workspace: Some(explicit_ws),
                expected_operation_id: None,
            }))
            .await
            .unwrap()
            .into_inner()
            .workspace
            .unwrap();
        assert_eq!(explicit_resp.commit_id, derived_commit);
    }
}
