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

//! Ephemeral in-memory `MetadataStore` and `BlobStore` implementations for unit tests and
//! volatile local development stacks (`--store-type=memory`).
//!
//! # Authority and Crash-Safety Ordering
//! `MemoryMetadataStore` mirrors the two-scope `Projects` / `Repositories` relational
//! boundary of the SQLite and Cloud Spanner backends in `Mutex`-guarded maps, keeping
//! immutable content-addressed commits (`or_insert`) and UTF-8 name-sorted directory
//! entries consistent with persistent backends.
//!
//! # Concurrency and Merge Invariants
//! `update_op_heads`, `compare_and_swap_op_heads`, `publish_operation`, `*_for_project`
//! checks, and `delete_repository` acquire `repos` before repo-scoped maps in lock order
//! to preserve sibling heads, enforce project bindings atomically, and cascade repository
//! state while keeping shared project objects intact.
//!
//! # Cross-Process and RPC Boundaries
//! State lives strictly inside the `jj-cc-server` process address space and resets on
//! process exit.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;

use cc_common::backend::*;
use cc_common::op_store::*;
use cc_common::workspace::WorkspaceState;

use super::{
    BlobRecord, StoreError, StoreResult, apply_op_heads_mutation, collect_non_root_op_parents,
    op_heads_sets_equal, verify_optional_repo_project,
};

mod blobs;
mod metadata;
#[cfg(test)]
mod tests;

pub use blobs::MemoryBlobStore;

pub type ProjectId = String;

pub type RepoId = String;

pub type CommitId = Vec<u8>;

pub type TreeId = Vec<u8>;

pub type FileId = Vec<u8>;

pub type SymlinkId = Vec<u8>;

pub type OpId = Vec<u8>;

pub type ViewId = Vec<u8>;

#[derive(Clone, Debug)]
struct StoredCommit {
    commit: Commit,
    generation: u64,
    seq: u64,
}

#[derive(Clone, Debug)]
struct RepoMeta {
    project_id: ProjectId,
    name: Option<String>,
}

#[derive(Clone, Debug)]
struct StoredOperation {
    op: Operation,
    published: bool,
}

#[derive(Debug, Default)]
pub struct MemoryMetadataStore {
    next_commit_seq: AtomicU64,
    // Lock order across namespace and repo-scoped maps:
    // `projects` -> `repos` -> `project_names`
    // `projects` -> `repos` -> `views` -> `ops` -> `op_heads` -> `workspaces`
    // Every method holding multiple locks acquires them strictly in this order so concurrent
    // registrations, deletions, and repo-scoped mutations cannot deadlock or resurrect rows.
    projects: Mutex<HashMap<ProjectId, Option<String>>>,
    project_names: Mutex<HashMap<String, ProjectId>>,
    repos: Mutex<HashMap<RepoId, RepoMeta>>,
    commits: Mutex<HashMap<ProjectId, BTreeMap<CommitId, StoredCommit>>>,
    trees: Mutex<HashMap<ProjectId, HashMap<TreeId, Vec<TreeEntry>>>>,
    symlinks: Mutex<HashMap<ProjectId, HashMap<SymlinkId, String>>>,
    // Methods that hold both blob locks take `project_blobs` before `blobs`, so concurrent
    // blob writes and tree or file reads cannot deadlock.
    blobs: Mutex<HashMap<FileId, BlobRecord>>,
    project_blobs: Mutex<HashMap<ProjectId, HashSet<FileId>>>,
    views: Mutex<HashMap<RepoId, HashMap<ViewId, View>>>,
    ops: Mutex<HashMap<RepoId, BTreeMap<OpId, StoredOperation>>>,
    op_heads: Mutex<HashMap<RepoId, Vec<OpId>>>,
    workspaces: Mutex<HashMap<RepoId, BTreeMap<String, WorkspaceState>>>,
}

impl MemoryMetadataStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn put_operation_inner(
        &self,
        repo_id: &str,
        expected_project_id: Option<&str>,
        op_id: &[u8],
        op: &Operation,
        require_dependencies_exist: bool,
    ) -> StoreResult<()> {
        let repos = self.repos.lock().unwrap();
        verify_optional_repo_project(
            repo_id,
            repos.get(repo_id).map(|m| m.project_id.as_str()),
            expected_project_id,
            "writing operations",
        )?;
        // Respect lock order `repos -> views -> ops`.
        let views = if require_dependencies_exist
            && op.view_id.as_slice() != cc_common::ROOT_VIEW_ID_BYTES
        {
            Some(self.views.lock().unwrap())
        } else {
            None
        };
        let mut ops = self.ops.lock().unwrap();
        if ops.get(repo_id).is_some_and(|m| m.contains_key(op_id)) {
            return Ok(());
        }
        if require_dependencies_exist {
            if let Some(views) = views
                && views
                    .get(repo_id)
                    .and_then(|m| m.get(&op.view_id))
                    .is_none()
            {
                return Err(StoreError::MissingDependency {
                    parent_kind: "operation",
                    missing_kind: "view",
                    id_hex: hex::encode(&op.view_id),
                });
            }
            for parent_id in collect_non_root_op_parents(op) {
                if !ops.get(repo_id).is_some_and(|m| m.contains_key(&parent_id)) {
                    return Err(StoreError::MissingDependency {
                        parent_kind: "operation",
                        missing_kind: "parent operation",
                        id_hex: hex::encode(&parent_id),
                    });
                }
            }
        }
        ops.entry(repo_id.to_string()).or_default().insert(
            op_id.to_vec(),
            StoredOperation {
                op: op.clone(),
                published: false,
            },
        );
        Ok(())
    }

    fn put_view_inner(
        &self,
        repo_id: &str,
        expected_project_id: Option<&str>,
        view_id: &[u8],
        view: &View,
    ) -> StoreResult<()> {
        let repos = self.repos.lock().unwrap();
        verify_optional_repo_project(
            repo_id,
            repos.get(repo_id).map(|m| m.project_id.as_str()),
            expected_project_id,
            "writing views",
        )?;
        let mut views = self.views.lock().unwrap();
        views
            .entry(repo_id.to_string())
            .or_default()
            .entry(view_id.to_vec())
            .or_insert_with(|| view.clone());
        Ok(())
    }

    fn update_op_heads_inner(
        &self,
        repo_id: &str,
        expected_project_id: Option<&str>,
        old_ids: &[Vec<u8>],
        new_id: &[u8],
        require_op_exists: bool,
    ) -> StoreResult<Vec<OpId>> {
        let repos = self.repos.lock().unwrap();
        verify_optional_repo_project(
            repo_id,
            repos.get(repo_id).map(|m| m.project_id.as_str()),
            expected_project_id,
            "updating op heads",
        )?;
        let mut ops = self.ops.lock().unwrap();
        let stored_op = ops.get(repo_id).and_then(|m| m.get(new_id));
        if require_op_exists && stored_op.is_none() {
            return Err(StoreError::MissingDependency {
                parent_kind: "op_heads",
                missing_kind: "operation",
                id_hex: hex::encode(new_id),
            });
        }
        let already_published = stored_op.is_some_and(|stored| stored.published);
        let mut op_heads = self.op_heads.lock().unwrap();
        let current_heads = op_heads
            .entry(repo_id.to_string())
            .or_insert_with(|| vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()]);
        if already_published && !current_heads.iter().any(|h| h.as_slice() == new_id) {
            return Ok(current_heads.clone());
        }
        apply_op_heads_mutation(current_heads, old_ids, new_id);
        if let Some(stored) = ops.get_mut(repo_id).and_then(|m| m.get_mut(new_id)) {
            stored.published = true;
        }
        Ok(current_heads.clone())
    }

    #[allow(clippy::too_many_arguments)]
    fn publish_operation_inner(
        &self,
        repo_id: &str,
        expected_project_id: Option<&str>,
        view_id: &[u8],
        view: &View,
        op_id: &[u8],
        op: &Operation,
        old_ids: &[Vec<u8>],
        require_dependencies_exist: bool,
    ) -> StoreResult<Vec<OpId>> {
        let repos = self.repos.lock().unwrap();
        verify_optional_repo_project(
            repo_id,
            repos.get(repo_id).map(|m| m.project_id.as_str()),
            expected_project_id,
            "publishing operation",
        )?;
        let mut views = self.views.lock().unwrap();
        let mut ops = self.ops.lock().unwrap();
        let mut op_heads = self.op_heads.lock().unwrap();
        let current_heads = op_heads
            .entry(repo_id.to_string())
            .or_insert_with(|| vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()]);
        let repo_ops = ops.entry(repo_id.to_string()).or_default();
        if repo_ops.get(op_id).is_some_and(|stored| stored.published) {
            return Ok(current_heads.clone());
        }
        if require_dependencies_exist {
            if op.view_id.as_slice() != view_id
                && op.view_id.as_slice() != cc_common::ROOT_VIEW_ID_BYTES
                && views
                    .get(repo_id)
                    .and_then(|m| m.get(&op.view_id))
                    .is_none()
            {
                return Err(StoreError::MissingDependency {
                    parent_kind: "operation",
                    missing_kind: "view",
                    id_hex: hex::encode(&op.view_id),
                });
            }
            for parent_id in &op.parents {
                if parent_id.as_slice() != cc_common::ROOT_OPERATION_ID_BYTES
                    && !repo_ops.contains_key(parent_id)
                {
                    return Err(StoreError::MissingDependency {
                        parent_kind: "operation",
                        missing_kind: "parent operation",
                        id_hex: hex::encode(parent_id),
                    });
                }
            }
        }
        views
            .entry(repo_id.to_string())
            .or_default()
            .entry(view_id.to_vec())
            .or_insert_with(|| view.clone());
        repo_ops
            .entry(op_id.to_vec())
            .and_modify(|stored| stored.published = true)
            .or_insert_with(|| StoredOperation {
                op: op.clone(),
                published: true,
            });
        apply_op_heads_mutation(current_heads, old_ids, op_id);
        Ok(current_heads.clone())
    }

    fn compare_and_swap_op_heads_inner(
        &self,
        repo_id: &str,
        expected_project_id: Option<&str>,
        expected_old_ids: &[Vec<u8>],
        new_id: &[u8],
        require_op_exists: bool,
    ) -> StoreResult<()> {
        let repos = self.repos.lock().unwrap();
        verify_optional_repo_project(
            repo_id,
            repos.get(repo_id).map(|m| m.project_id.as_str()),
            expected_project_id,
            "comparing and swapping op heads",
        )?;
        let mut ops = self.ops.lock().unwrap();
        if require_op_exists && ops.get(repo_id).and_then(|m| m.get(new_id)).is_none() {
            return Err(StoreError::MissingDependency {
                parent_kind: "op_heads",
                missing_kind: "operation",
                id_hex: hex::encode(new_id),
            });
        }
        let mut op_heads = self.op_heads.lock().unwrap();
        let current_heads = op_heads
            .entry(repo_id.to_string())
            .or_insert_with(|| vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()]);
        if !op_heads_sets_equal(current_heads, expected_old_ids) {
            return Err(StoreError::CasConflict(format!(
                "op heads changed concurrently in repo '{repo_id}'"
            )));
        }
        *current_heads = vec![new_id.to_vec()];
        if let Some(stored) = ops.get_mut(repo_id).and_then(|m| m.get_mut(new_id)) {
            stored.published = true;
        }
        Ok(())
    }

    fn put_workspace_cas_inner(
        &self,
        expected_project_id: Option<&str>,
        workspace: &WorkspaceState,
        expected_operation_id: Option<&[u8]>,
        require_operation_exists: bool,
    ) -> StoreResult<()> {
        let repos = self.repos.lock().unwrap();
        verify_optional_repo_project(
            &workspace.repo_id,
            repos.get(&workspace.repo_id).map(|m| m.project_id.as_str()),
            expected_project_id,
            "updating workspace",
        )?;
        // Respect lock order `repos -> ops -> workspaces`.
        if require_operation_exists
            && !workspace.operation_id.is_empty()
            && workspace.operation_id.as_slice() != cc_common::ROOT_OPERATION_ID_BYTES
        {
            let ops = self.ops.lock().unwrap();
            if !ops
                .get(&workspace.repo_id)
                .is_some_and(|m| m.contains_key(&workspace.operation_id))
            {
                return Err(StoreError::MissingDependency {
                    parent_kind: "workspace",
                    missing_kind: "operation",
                    id_hex: hex::encode(&workspace.operation_id),
                });
            }
        }
        let mut workspaces = self.workspaces.lock().unwrap();
        let repo_map = workspaces.entry(workspace.repo_id.clone()).or_default();
        let key = workspace.workspace_name.clone();
        if crate::store::check_workspace_cas_precondition(
            repo_map.get(&key),
            workspace,
            expected_operation_id,
        )? {
            return Ok(());
        }
        repo_map.insert(key, workspace.clone());
        Ok(())
    }
}
