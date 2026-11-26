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

//! Storage trait and shared workspace normalization / optimistic CAS helpers for
//! `jj-cc-server`.
//!
//! # Concurrency and Merge Invariants
//! [`verify_workspace_cas`] enforces `UpdateWorkspaceRequest.expected_operation_id`
//! preconditions uniformly across `MemoryStore`, `SqliteStore`, and `SpannerStore`
//! (`None` = unconditional upsert, `Some(b"")` = create-only, `Some(op_id)` = require
//! existing normalized `operation_id == op_id`, with idempotent `OK` on identical retries).

use async_trait::async_trait;
use cc_common::backend::*;
use cc_common::op_store::*;
use cc_common::workspace::WorkspaceState;
use prost::Message;

pub mod memorystore;
pub mod spannerstore;
pub mod sqlitestore;

pub use memorystore::{CommitId, FileId, MemoryStore, OpId, ProjectId, RepoId, TreeId, ViewId};
pub use spannerstore::SpannerStore;
pub use sqlitestore::SqliteStore;

// Error returned by fallible store operations.
// Kept independent of any transport type so store implementations
// Note: a read that finds nothing is not an error — that is reported as
// `Ok(None)`. `Read` is reserved for the backend actually failing.
#[derive(Debug)]
pub enum StoreError {
    // A read from the storage backend failed (e.g. query/statement error or I/O failure).
    Read(String),
    // A write to the storage backend failed.
    Write(String),
    // A Compare-And-Swap (CAS) optimistic concurrency conflict occurred.
    CasConflict(String),
    // Data failed to be encoded into protobuf format.
    Encode(String),
    // Data was retrieved but could not be decoded, indicating corruption.
    Decode(String),
    // The blocking storage task failed to run to completion (e.g. panicked or was cancelled).
    Task(String),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::Read(msg) => write!(f, "storage read error: {msg}"),
            StoreError::Write(msg) => write!(f, "storage write error: {msg}"),
            StoreError::CasConflict(msg) => write!(f, "CAS conflict: {msg}"),
            StoreError::Encode(msg) => write!(f, "storage encode error: {msg}"),
            StoreError::Decode(msg) => write!(f, "stored data is corrupt: {msg}"),
            StoreError::Task(msg) => write!(f, "storage task error: {msg}"),
        }
    }
}

impl std::error::Error for StoreError {}

pub type StoreResult<T> = Result<T, StoreError>;

/// Canonicalizes a persisted `WorkspaceState` record (including rows written before
/// `proto_data` was added) so reads and CAS comparisons observe consistent defaults.
#[allow(deprecated)]
pub fn normalize_workspace_record(mut ws: WorkspaceState) -> WorkspaceState {
    if ws.operation_id.is_empty() {
        ws.operation_id = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    }
    if ws.tree_ids.is_empty() && !ws.tree_id.is_empty() {
        ws.tree_ids.push(ws.tree_id.clone());
    } else if ws.tree_id.is_empty() && !ws.tree_ids.is_empty() {
        ws.tree_id = ws.tree_ids[0].clone();
    }
    if ws.conflict_labels.iter().all(|l| l.is_empty()) {
        ws.conflict_labels.clear();
    }
    if ws.sparse_patterns.is_empty() && !ws.sparse_patterns_set {
        ws.sparse_patterns.push(String::new());
    }
    ws.sparse_patterns_set = true;
    ws
}

/// Decodes a workspace row carrying legacy scalar columns plus optional `proto_data`.
///
/// During mixed-version Spanner rollouts an older server can update only the scalar
/// `(commit_id, operation_id, tree_id)` columns while leaving an earlier `proto_data`
/// blob intact. Whenever scalar columns diverge from `proto_data`, `proto_data`
/// is stale and the authoritative scalar columns win.
#[allow(deprecated)]
pub fn decode_workspace_row(
    repo_id: String,
    user: String,
    workspace_name: String,
    commit_id: Vec<u8>,
    operation_id: Vec<u8>,
    tree_id: Vec<u8>,
    proto_data: Option<Vec<u8>>,
) -> StoreResult<WorkspaceState> {
    let scalar_fallback = || WorkspaceState {
        repo_id: repo_id.clone(),
        user: user.clone(),
        workspace_name: workspace_name.clone(),
        commit_id: commit_id.clone(),
        operation_id: operation_id.clone(),
        tree_id: tree_id.clone(),
        sparse_patterns: Vec::new(),
        tree_ids: Vec::new(),
        conflict_labels: Vec::new(),
        sparse_patterns_set: false,
    };
    let ws = match proto_data {
        Some(data) if !data.is_empty() => {
            let mut decoded = WorkspaceState::decode(data.as_slice())
                .map_err(|e| StoreError::Decode(e.to_string()))?;
            let decoded_norm = normalize_workspace_record(decoded.clone());
            let scalar_op_id = if operation_id.is_empty() {
                cc_common::ROOT_OPERATION_ID_BYTES.as_slice()
            } else {
                operation_id.as_slice()
            };
            let scalar_diverged = decoded_norm.commit_id != commit_id
                || decoded_norm.operation_id.as_slice() != scalar_op_id
                || decoded_norm.tree_id != tree_id;
            if scalar_diverged {
                scalar_fallback()
            } else {
                decoded.repo_id = repo_id;
                decoded.user = user;
                decoded.workspace_name = workspace_name;
                if decoded.commit_id.is_empty() {
                    decoded.commit_id = commit_id;
                }
                if decoded.operation_id.is_empty() {
                    decoded.operation_id = operation_id;
                }
                if decoded.tree_id.is_empty() {
                    decoded.tree_id = tree_id;
                }
                decoded
            }
        }
        _ => scalar_fallback(),
    };
    Ok(normalize_workspace_record(ws))
}

/// Evaluates `UpdateWorkspaceRequest.expected_operation_id` preconditions against the
/// currently persisted workspace record (`existing`) and incoming normalized record (`incoming`).
pub fn verify_workspace_cas(
    existing: Option<&WorkspaceState>,
    incoming: &WorkspaceState,
    expected_operation_id: Option<&[u8]>,
) -> StoreResult<()> {
    let Some(expected) = expected_operation_id else {
        return Ok(());
    };
    let existing_norm = existing.cloned().map(normalize_workspace_record);
    let incoming_norm = normalize_workspace_record(incoming.clone());
    if expected.is_empty() {
        match existing_norm {
            None => Ok(()),
            Some(ex) if ex == incoming_norm => Ok(()),
            Some(_) => Err(StoreError::CasConflict(format!(
                "workspace '{}' for user '{}' in repo '{}' already exists",
                incoming.workspace_name, incoming.user, incoming.repo_id
            ))),
        }
    } else {
        match existing_norm {
            Some(ex) if ex == incoming_norm || ex.operation_id.as_slice() == expected => Ok(()),
            Some(ex) => Err(StoreError::CasConflict(format!(
                "workspace '{}' for user '{}' in repo '{}' operation_id mismatch: expected {}, found {}",
                incoming.workspace_name,
                incoming.user,
                incoming.repo_id,
                hex::encode(expected),
                hex::encode(&ex.operation_id)
            ))),
            None => Err(StoreError::CasConflict(format!(
                "workspace '{}' for user '{}' in repo '{}' does not exist (expected operation_id {})",
                incoming.workspace_name,
                incoming.user,
                incoming.repo_id,
                hex::encode(expected)
            ))),
        }
    }
}

#[async_trait]
pub trait Store: Send + Sync {
    async fn is_project_registered(&self, project_id: &str) -> StoreResult<bool>;
    async fn register_project(&self, project_id: String, name: Option<String>) -> StoreResult<()>;

    async fn is_repo_registered(&self, repo_id: &str) -> StoreResult<bool>;
    async fn register_repo(
        &self,
        repo_id: String,
        project_id: String,
        name: Option<String>,
    ) -> StoreResult<()>;
    async fn get_repo_project_id(&self, repo_id: &str) -> StoreResult<Option<String>>;

    async fn get_commit(&self, project_id: &str, commit_id: &[u8]) -> StoreResult<Option<Commit>>;
    async fn list_project_commit_ids(&self, project_id: &str) -> StoreResult<Vec<CommitId>>;
    async fn put_commit(
        &self,
        project_id: String,
        commit_id: Vec<u8>,
        commit: Commit,
    ) -> StoreResult<()>;

    async fn get_tree(
        &self,
        project_id: &str,
        tree_id: &[u8],
    ) -> StoreResult<Option<Vec<TreeEntry>>>;
    async fn put_tree(
        &self,
        project_id: String,
        tree_id: Vec<u8>,
        entries: Vec<TreeEntry>,
    ) -> StoreResult<()>;

    async fn get_file(&self, project_id: &str, file_id: &[u8]) -> StoreResult<Option<Vec<u8>>>;
    async fn put_file(
        &self,
        project_id: String,
        file_id: Vec<u8>,
        content: Vec<u8>,
    ) -> StoreResult<()>;

    async fn get_operation(&self, repo_id: &str, op_id: &[u8]) -> StoreResult<Option<Operation>>;
    async fn put_operation(
        &self,
        repo_id: String,
        op_id: Vec<u8>,
        op: Operation,
    ) -> StoreResult<()>;

    async fn get_view(&self, repo_id: &str, view_id: &[u8]) -> StoreResult<Option<View>>;
    async fn put_view(&self, repo_id: String, view_id: Vec<u8>, view: View) -> StoreResult<()>;

    async fn get_op_heads(&self, repo_id: &str) -> StoreResult<Option<Vec<Vec<u8>>>>;
    async fn update_op_heads_append_new_remove_old(
        &self,
        repo_id: String,
        ids_to_remove: &[Vec<u8>],
        id_to_add: Vec<u8>,
    ) -> StoreResult<Vec<Vec<u8>>>;

    async fn update_op_heads_compare_and_swap(
        &self,
        repo_id: String,
        expected_exact_heads: &[Vec<u8>],
        new_id: Vec<u8>,
    ) -> StoreResult<Vec<Vec<u8>>>;

    async fn get_workspace(
        &self,
        repo_id: &str,
        user: &str,
        workspace_name: &str,
    ) -> StoreResult<Option<WorkspaceState>>;
    async fn put_workspace(&self, workspace: WorkspaceState) -> StoreResult<()> {
        self.put_workspace_cas(workspace, None).await
    }
    async fn put_workspace_cas(
        &self,
        workspace: WorkspaceState,
        expected_operation_id: Option<&[u8]>,
    ) -> StoreResult<()>;
    async fn delete_workspace(
        &self,
        repo_id: &str,
        user: &str,
        workspace_name: &str,
    ) -> StoreResult<bool>;
    async fn list_workspaces(&self, repo_id: &str) -> StoreResult<Vec<WorkspaceState>>;
    async fn list_workspaces_page(
        &self,
        repo_id: &str,
        after: Option<(&str, &str)>,
        limit: usize,
    ) -> StoreResult<Vec<WorkspaceState>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(deprecated)]
    fn decode_workspace_row_discards_stale_proto_data_when_scalar_columns_diverge() {
        let old_commit = vec![0x11; cc_common::COMMIT_ID_LENGTH];
        let old_op = vec![0x22; cc_common::OPERATION_ID_LENGTH];
        let old_tree = vec![0x33; cc_common::EMPTY_TREE_ID_BYTES.len()];
        let stale_ws = WorkspaceState {
            repo_id: "repo-1".to_string(),
            user: "alice".to_string(),
            workspace_name: "default".to_string(),
            commit_id: old_commit,
            operation_id: old_op,
            tree_id: old_tree.clone(),
            sparse_patterns: vec!["src".to_string()],
            tree_ids: vec![old_tree],
            conflict_labels: Vec::new(),
            sparse_patterns_set: true,
        };
        let stale_proto = stale_ws.encode_to_vec();

        // Simulate an older server updating only scalar columns during a mixed-version rollout.
        let new_commit = vec![0x44; cc_common::COMMIT_ID_LENGTH];
        let new_op = vec![0x55; cc_common::OPERATION_ID_LENGTH];
        let new_tree = vec![0x66; cc_common::EMPTY_TREE_ID_BYTES.len()];
        let decoded = decode_workspace_row(
            "repo-1".to_string(),
            "alice".to_string(),
            "default".to_string(),
            new_commit.clone(),
            new_op.clone(),
            new_tree.clone(),
            Some(stale_proto),
        )
        .unwrap();

        assert_eq!(decoded.commit_id, new_commit);
        assert_eq!(decoded.operation_id, new_op);
        assert_eq!(decoded.tree_id, new_tree);
        assert_eq!(decoded.tree_ids, vec![new_tree]);
        assert_eq!(decoded.sparse_patterns, vec!["".to_string()]);
    }

    #[test]
    #[allow(deprecated)]
    fn decode_workspace_row_detects_empty_scalar_divergence() {
        let old_commit = vec![0x11; cc_common::COMMIT_ID_LENGTH];
        let old_op = vec![0x22; cc_common::OPERATION_ID_LENGTH];
        let old_tree = vec![0x33; cc_common::EMPTY_TREE_ID_BYTES.len()];
        let stale_ws = WorkspaceState {
            repo_id: "repo-1".to_string(),
            user: "alice".to_string(),
            workspace_name: "default".to_string(),
            commit_id: old_commit.clone(),
            operation_id: old_op,
            tree_id: old_tree.clone(),
            sparse_patterns: vec!["src".to_string()],
            tree_ids: vec![old_tree.clone()],
            conflict_labels: Vec::new(),
            sparse_patterns_set: true,
        };
        let stale_proto = stale_ws.encode_to_vec();

        // Older server writing empty operation_id (root) trips divergence against non-root proto.
        let decoded = decode_workspace_row(
            "repo-1".to_string(),
            "alice".to_string(),
            "default".to_string(),
            old_commit.clone(),
            Vec::new(),
            old_tree.clone(),
            Some(stale_proto.clone()),
        )
        .unwrap();
        assert_eq!(
            decoded.operation_id,
            cc_common::ROOT_OPERATION_ID_BYTES.to_vec()
        );
        assert_eq!(decoded.sparse_patterns, vec!["".to_string()]);

        // Older server writing empty commit_id also trips divergence.
        let decoded_empty_commit = decode_workspace_row(
            "repo-1".to_string(),
            "alice".to_string(),
            "default".to_string(),
            Vec::new(),
            vec![0x22; cc_common::OPERATION_ID_LENGTH],
            old_tree.clone(),
            Some(stale_proto),
        )
        .unwrap();
        assert!(decoded_empty_commit.commit_id.is_empty());
        assert_eq!(decoded_empty_commit.sparse_patterns, vec!["".to_string()]);

        // Empty scalar operation_id matching ROOT_OPERATION_ID_BYTES in proto_data is not diverged.
        let root_ws = WorkspaceState {
            operation_id: cc_common::ROOT_OPERATION_ID_BYTES.to_vec(),
            ..stale_ws
        };
        let decoded_root = decode_workspace_row(
            "repo-1".to_string(),
            "alice".to_string(),
            "default".to_string(),
            old_commit,
            Vec::new(),
            old_tree,
            Some(root_ws.encode_to_vec()),
        )
        .unwrap();
        assert_eq!(decoded_root.sparse_patterns, vec!["src".to_string()]);
    }
}
