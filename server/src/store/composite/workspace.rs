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

//! Workspace state persistence and compare-and-swap helpers on `CompositeStore`.
//!
//! # Authority and Crash-Safety Ordering
//! Workspace updates verify repository membership and optional expected operation-head
//! preconditions before recording new working-copy state in `MetadataStore`.
//!
//! # Concurrency and Merge Invariants
//! Compare-and-swap precondition checks normalize empty and root operation identifiers so
//! retried workspace updates stay idempotent across clients.

use cc_common::workspace::WorkspaceState;

use super::CompositeStore;
use crate::store::{StoreError, StoreResult};

impl CompositeStore {
    pub async fn get_workspace(
        &self,
        repo_id: &str,
        user: &str,
        workspace_name: &str,
    ) -> StoreResult<Option<WorkspaceState>> {
        self.ensure_repo_registered(repo_id, "reading workspaces")
            .await?;
        self.metadata
            .get_workspace(repo_id, user, workspace_name)
            .await
    }

    pub async fn put_workspace(&self, workspace: &WorkspaceState) -> StoreResult<()> {
        self.put_workspace_cas(workspace, None).await
    }

    pub async fn put_workspace_cas(
        &self,
        workspace: &WorkspaceState,
        expected_operation_id: Option<&[u8]>,
    ) -> StoreResult<()> {
        if workspace.repo_id.trim().is_empty() || workspace.workspace_name.trim().is_empty() {
            return Err(StoreError::InvalidArgument(
                "workspace repo_id and workspace_name must be non-empty".to_string(),
            ));
        }
        if let Some(exp) = expected_operation_id
            && !exp.is_empty()
            && exp.len() != cc_common::OPERATION_ID_LENGTH
        {
            return Err(StoreError::InvalidArgument(format!(
                "expected_operation_id must be 0 or {} bytes (got {} bytes)",
                cc_common::OPERATION_ID_LENGTH,
                exp.len()
            )));
        }
        self.ensure_repo_registered(&workspace.repo_id, "updating workspaces")
            .await?;
        let mut normalized = workspace.clone();
        if normalized.operation_id.is_empty() {
            normalized.operation_id = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
        }
        #[allow(deprecated)]
        if normalized.tree_ids.is_empty() && !normalized.tree_id.is_empty() {
            normalized.tree_ids = vec![normalized.tree_id.clone()];
        } else if normalized.tree_id.is_empty()
            && let Some(first) = normalized.tree_ids.first()
        {
            normalized.tree_id = first.clone();
        }
        self.metadata
            .put_workspace_cas(&normalized, expected_operation_id)
            .await
    }

    pub async fn list_workspaces(&self, repo_id: &str) -> StoreResult<Vec<WorkspaceState>> {
        self.ensure_repo_registered(repo_id, "listing workspaces")
            .await?;
        self.metadata.list_workspaces(repo_id).await
    }

    pub async fn list_workspaces_page(
        &self,
        repo_id: &str,
        after: Option<(&str, &str)>,
        limit: usize,
        max_bytes: Option<usize>,
    ) -> StoreResult<(Vec<WorkspaceState>, bool)> {
        self.ensure_repo_registered(repo_id, "listing workspaces")
            .await?;
        self.metadata
            .list_workspaces_page(repo_id, after, limit, max_bytes)
            .await
    }

    pub async fn delete_workspace(
        &self,
        repo_id: &str,
        user: &str,
        workspace_name: &str,
    ) -> StoreResult<bool> {
        match self
            .ensure_repo_registered(repo_id, "deleting workspaces")
            .await
        {
            Ok(()) => {}
            Err(StoreError::RepoNotFound { .. }) => return Ok(false),
            Err(err) => return Err(err),
        }
        self.metadata
            .delete_workspace(repo_id, user, workspace_name)
            .await
    }
}

pub(crate) fn check_workspace_cas_precondition(
    existing: Option<&WorkspaceState>,
    target: &WorkspaceState,
    expected_operation_id: Option<&[u8]>,
) -> StoreResult<bool> {
    let Some(exp) = expected_operation_id else {
        return Ok(false);
    };
    fn normalize_op(op: &[u8]) -> &[u8] {
        if op.is_empty() {
            &cc_common::ROOT_OPERATION_ID_BYTES[..]
        } else {
            op
        }
    }
    if let Some(cur) = existing {
        let cur_op = normalize_op(&cur.operation_id);
        let target_op = normalize_op(&target.operation_id);
        if cur.commit_id == target.commit_id
            && cur_op == target_op
            && cur.tree_ids == target.tree_ids
            && cur.sparse_patterns == target.sparse_patterns
            && cur.conflict_labels == target.conflict_labels
        {
            return Ok(true);
        }
        if exp.is_empty() {
            return Err(StoreError::CasConflict(format!(
                "workspace '{}' for user '{}' already exists in repo '{}'",
                target.workspace_name, target.user, target.repo_id
            )));
        }
        if cur_op != exp {
            return Err(StoreError::CasConflict(format!(
                "workspace '{}' operation_id changed concurrently in repo '{}' (expected {}, found {})",
                target.workspace_name,
                target.repo_id,
                hex::encode(exp),
                hex::encode(cur_op)
            )));
        }
    } else if !exp.is_empty() {
        return Err(StoreError::CasConflict(format!(
            "workspace '{}' for user '{}' does not exist in repo '{}'",
            target.workspace_name, target.user, target.repo_id
        )));
    }
    Ok(false)
}
