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
//! [`CompositeStore`].
//!
//! # Authority and Crash-Safety Ordering
//! Clients update `WorkspaceState` (`commit_id`, `operation_id`, `tree_id` / `tree_ids`,
//! `conflict_labels`, and `sparse_patterns`) after committing the underlying `Operation`,
//! `Commit`, and `Tree` objects in `OpStoreService` and `BackendService`. `UpdateWorkspace`
//! verifies that referenced non-root operations/views, commits, and trees exist (returning
//! `FAILED_PRECONDITION` if missing) and derives `commit_id` from
//! `View.wc_commit_ids[workspace_name]` when `commit_id` is empty.
//!
//! # Concurrency and Merge Invariants
//! Keyed by `(repo_id, user, workspace_name)`. Upserts are idempotent across retries, and
//! `DeleteWorkspace` is advisory because active workspaces upsert their record on finish.
//!
//! # Cross-Process and RPC Boundaries
//! `CheckWorkingCopyChanges` resolves `repo_id -> project_id` through `CompositeStore`
//! and compares `WorkspaceState.tree_ids` against `Commit.root_tree_id` (preserving full
//! `Merge<TreeId>` algebraic conflict sides) without client I/O.

// Legacy single-tree fields (`tree_id`, `current_tree_id`, `commit_tree_id`) remain
// populated for legacy single-tree clients.
#![allow(deprecated)]

use std::collections::HashSet;
use std::sync::Arc;

use cc_common::workspace::workspace_service_server::WorkspaceService;
use cc_common::workspace::*;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::store::CompositeStore;

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
    store: Arc<CompositeStore>,
}

impl CommitCloudWorkspaceService {
    pub fn new(store: Arc<CompositeStore>) -> Self {
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
                if tid.len() != cc_common::TREE_ID_LENGTH {
                    return Err(Status::invalid_argument(format!(
                        "workspace tree_ids entry must be {} bytes (got {} bytes)",
                        cc_common::TREE_ID_LENGTH,
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
            if ws.tree_id.len() != cc_common::TREE_ID_LENGTH {
                return Err(Status::invalid_argument(format!(
                    "workspace tree_id must be {} bytes (got {} bytes)",
                    cc_common::TREE_ID_LENGTH,
                    ws.tree_id.len()
                )));
            }
            ws.tree_ids = vec![ws.tree_id.clone()];
        }

        let valid_label_len =
            ws.conflict_labels.is_empty() || ws.conflict_labels.len() == ws.tree_ids.len();
        if !valid_label_len {
            return Err(Status::invalid_argument(format!(
                "workspace conflict_labels count ({}) must be empty or match tree_ids count ({})",
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
                "workspace conflict_labels must be empty for a resolved single-tree workspace",
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

        let mut derived_from_view: Option<Vec<u8>> = None;
        if !ws.operation_id.is_empty()
            && ws.operation_id.as_slice() != cc_common::ROOT_OPERATION_ID_BYTES
        {
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
            } else if ws.operation_id.is_empty()
                || ws.operation_id.as_slice() == cc_common::ROOT_OPERATION_ID_BYTES
            {
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

        let project_id = self
            .store
            .resolve_repo_project(&ws.repo_id, "updating workspace")
            .await?;
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
                if !existing_trees.contains(tid.as_slice()) {
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
            .put_workspace_cas(&ws, expected_operation_id.as_deref())
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
            let project_id = self
                .store
                .resolve_repo_project(&req.repo_id, "checking working copy changes")
                .await?;
            let commit = self.store.get_commit(&project_id, &ws.commit_id).await?;
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
mod tests;
