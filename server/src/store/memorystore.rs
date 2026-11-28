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

//! In-memory [`Store`] backend for local development and unit tests.
//!
//! # Concurrency and Merge Invariants
//! Workspace CAS updates normalize incoming `WorkspaceState` records and check
//! `expected_operation_id` atomically under the `workspaces` mutex, returning
//! `StoreError::CasConflict` on mismatch.

use async_trait::async_trait;
use cc_common::backend::*;
use cc_common::op_store::*;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use super::{Store, StoreError, StoreResult};

pub type ProjectId = String;
pub type RepoId = String;
pub type CommitId = Vec<u8>;
pub type TreeId = Vec<u8>;
pub type FileId = Vec<u8>;
pub type OpId = Vec<u8>;
pub type ViewId = Vec<u8>;

use cc_common::workspace::WorkspaceState;

#[derive(Debug, Default)]
pub struct MemoryStore {
    pub projects: Mutex<HashSet<ProjectId>>,
    pub repos: Mutex<HashMap<RepoId, ProjectId>>,
    pub commits: Mutex<HashMap<ProjectId, HashMap<CommitId, Commit>>>,
    pub trees: Mutex<HashMap<ProjectId, HashMap<TreeId, Vec<TreeEntry>>>>,
    pub files: Mutex<HashMap<ProjectId, HashMap<FileId, Vec<u8>>>>,
    pub ops: Mutex<HashMap<RepoId, HashMap<OpId, Operation>>>,
    pub views: Mutex<HashMap<RepoId, HashMap<ViewId, View>>>,
    pub op_heads: Mutex<HashMap<RepoId, Vec<OpId>>>,
    pub workspaces: Mutex<HashMap<RepoId, HashMap<(String, String), WorkspaceState>>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Store for MemoryStore {
    async fn is_project_registered(&self, project_id: &str) -> StoreResult<bool> {
        Ok(self.projects.lock().unwrap().contains(project_id))
    }

    async fn register_project(&self, project_id: String, _name: Option<String>) -> StoreResult<()> {
        self.projects.lock().unwrap().insert(project_id);
        Ok(())
    }

    async fn is_repo_registered(&self, repo_id: &str) -> StoreResult<bool> {
        Ok(self.repos.lock().unwrap().contains_key(repo_id))
    }

    async fn register_repo(
        &self,
        repo_id: String,
        project_id: String,
        _name: Option<String>,
    ) -> StoreResult<()> {
        self.projects.lock().unwrap().insert(project_id.clone());
        self.repos.lock().unwrap().insert(repo_id, project_id);
        Ok(())
    }

    async fn get_repo_project_id(&self, repo_id: &str) -> StoreResult<Option<String>> {
        Ok(self.repos.lock().unwrap().get(repo_id).cloned())
    }

    async fn get_commit(&self, project_id: &str, commit_id: &[u8]) -> StoreResult<Option<Commit>> {
        let commits = self.commits.lock().unwrap();
        Ok(commits
            .get(project_id)
            .and_then(|m| m.get(commit_id).cloned()))
    }

    async fn has_commit(&self, project_id: &str, commit_id: &[u8]) -> StoreResult<bool> {
        let commits = self.commits.lock().unwrap();
        Ok(commits
            .get(project_id)
            .is_some_and(|m| m.contains_key(commit_id)))
    }

    async fn list_project_commit_ids(&self, project_id: &str) -> StoreResult<Vec<CommitId>> {
        let commits = self.commits.lock().unwrap();
        Ok(commits
            .get(project_id)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default())
    }

    async fn put_commit(
        &self,
        project_id: String,
        commit_id: Vec<u8>,
        commit: Commit,
    ) -> StoreResult<()> {
        let mut commits = self.commits.lock().unwrap();
        commits
            .entry(project_id)
            .or_default()
            .insert(commit_id, commit);
        Ok(())
    }

    async fn get_tree(
        &self,
        project_id: &str,
        tree_id: &[u8],
    ) -> StoreResult<Option<Vec<TreeEntry>>> {
        let trees = self.trees.lock().unwrap();
        Ok(trees.get(project_id).and_then(|m| m.get(tree_id).cloned()))
    }

    async fn find_existing_trees(
        &self,
        project_id: &str,
        tree_ids: &[Vec<u8>],
    ) -> StoreResult<HashSet<Vec<u8>>> {
        if tree_ids.is_empty() {
            return Ok(HashSet::new());
        }
        let trees = self.trees.lock().unwrap();
        let Some(project_trees) = trees.get(project_id) else {
            return Ok(HashSet::new());
        };
        Ok(tree_ids
            .iter()
            .filter(|tid| project_trees.contains_key(tid.as_slice()))
            .cloned()
            .collect())
    }

    async fn put_tree(
        &self,
        project_id: String,
        tree_id: Vec<u8>,
        entries: Vec<TreeEntry>,
    ) -> StoreResult<()> {
        let mut trees = self.trees.lock().unwrap();
        trees
            .entry(project_id)
            .or_default()
            .insert(tree_id, entries);
        Ok(())
    }

    async fn get_file(&self, project_id: &str, file_id: &[u8]) -> StoreResult<Option<Vec<u8>>> {
        let files = self.files.lock().unwrap();
        Ok(files.get(project_id).and_then(|m| m.get(file_id).cloned()))
    }

    async fn put_file(
        &self,
        project_id: String,
        file_id: Vec<u8>,
        content: Vec<u8>,
    ) -> StoreResult<()> {
        let mut files = self.files.lock().unwrap();
        files
            .entry(project_id)
            .or_default()
            .insert(file_id, content);
        Ok(())
    }

    async fn get_operation(&self, repo_id: &str, op_id: &[u8]) -> StoreResult<Option<Operation>> {
        let ops = self.ops.lock().unwrap();
        Ok(ops.get(repo_id).and_then(|m| m.get(op_id).cloned()))
    }

    async fn put_operation(
        &self,
        repo_id: String,
        op_id: Vec<u8>,
        op: Operation,
    ) -> StoreResult<()> {
        let mut ops = self.ops.lock().unwrap();
        ops.entry(repo_id).or_default().insert(op_id, op);
        Ok(())
    }

    async fn get_view(&self, repo_id: &str, view_id: &[u8]) -> StoreResult<Option<View>> {
        let views = self.views.lock().unwrap();
        Ok(views.get(repo_id).and_then(|m| m.get(view_id).cloned()))
    }

    async fn has_view(&self, repo_id: &str, view_id: &[u8]) -> StoreResult<bool> {
        let views = self.views.lock().unwrap();
        Ok(views.get(repo_id).is_some_and(|m| m.contains_key(view_id)))
    }

    async fn put_view(&self, repo_id: String, view_id: Vec<u8>, view: View) -> StoreResult<()> {
        let mut views = self.views.lock().unwrap();
        views.entry(repo_id).or_default().insert(view_id, view);
        Ok(())
    }

    async fn get_op_heads(&self, repo_id: &str) -> StoreResult<Option<Vec<OpId>>> {
        let op_heads = self.op_heads.lock().unwrap();
        Ok(op_heads.get(repo_id).cloned())
    }

    async fn update_op_heads_append_new_remove_old(
        &self,
        repo_id: String,
        ids_to_remove: &[Vec<u8>],
        id_to_add: Vec<u8>,
    ) -> StoreResult<Vec<OpId>> {
        let mut op_heads = self.op_heads.lock().unwrap();
        let current_heads = op_heads
            .entry(repo_id.clone())
            .or_insert_with(|| vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()]);

        if ids_to_remove.is_empty() {
            if !current_heads.contains(&id_to_add) {
                current_heads.push(id_to_add);
            }
        } else {
            for old in ids_to_remove {
                // A workspace joining an existing repo starts from the root op;
                // let it fork and leave the merge to reconcile.
                if old.as_slice() == cc_common::ROOT_OPERATION_ID_BYTES.as_slice() {
                    continue;
                }
                if !current_heads.contains(old) {
                    return Err(StoreError::CasConflict(format!(
                        "write race detected on stale old_op_head_ids: head {} no longer active in repo {}",
                        hex::encode(old),
                        repo_id
                    )));
                }
            }

            current_heads.retain(|head| !ids_to_remove.contains(head));
            if !current_heads.contains(&id_to_add) {
                current_heads.push(id_to_add);
            }
        }

        Ok(current_heads.clone())
    }

    async fn update_op_heads_compare_and_swap(
        &self,
        repo_id: String,
        expected_exact_heads: &[Vec<u8>],
        new_id: Vec<u8>,
    ) -> StoreResult<Vec<OpId>> {
        let mut op_heads = self.op_heads.lock().unwrap();
        let current_heads = op_heads
            .entry(repo_id.clone())
            .or_insert_with(|| vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()]);

        let current_set: std::collections::HashSet<Vec<u8>> =
            current_heads.iter().cloned().collect();
        let expected_set: std::collections::HashSet<Vec<u8>> =
            expected_exact_heads.iter().cloned().collect();

        if current_set != expected_set {
            return Err(StoreError::CasConflict(format!(
                "reconciliation CAS conflict for repo {repo_id}: expected heads {expected_set:?}, found {current_set:?}"
            )));
        }

        *current_heads = vec![new_id.clone()];
        Ok(vec![new_id])
    }

    async fn get_workspace(
        &self,
        repo_id: &str,
        user: &str,
        workspace_name: &str,
    ) -> StoreResult<Option<WorkspaceState>> {
        let workspaces = self.workspaces.lock().unwrap();
        Ok(workspaces
            .get(repo_id)
            .and_then(|ws| {
                ws.get(&(user.to_string(), workspace_name.to_string()))
                    .cloned()
            })
            .map(super::normalize_workspace_record))
    }

    async fn put_workspace_cas(
        &self,
        workspace: WorkspaceState,
        expected_operation_id: Option<&[u8]>,
    ) -> StoreResult<()> {
        let mut workspaces = self.workspaces.lock().unwrap();
        let key = (workspace.user.clone(), workspace.workspace_name.clone());
        let existing = workspaces.get(&workspace.repo_id).and_then(|m| m.get(&key));
        super::verify_workspace_cas(existing, &workspace, expected_operation_id)?;
        let normalized = super::normalize_workspace_record(workspace);
        workspaces
            .entry(normalized.repo_id.clone())
            .or_default()
            .insert(key, normalized);
        Ok(())
    }

    async fn delete_workspace(
        &self,
        repo_id: &str,
        user: &str,
        workspace_name: &str,
    ) -> StoreResult<bool> {
        let mut workspaces = self.workspaces.lock().unwrap();
        if let Some(repo_workspaces) = workspaces.get_mut(repo_id) {
            let existed = repo_workspaces
                .remove(&(user.to_string(), workspace_name.to_string()))
                .is_some();
            Ok(existed)
        } else {
            Ok(false)
        }
    }

    async fn list_workspaces(&self, repo_id: &str) -> StoreResult<Vec<WorkspaceState>> {
        let workspaces = self.workspaces.lock().unwrap();
        Ok(workspaces
            .get(repo_id)
            .map(|ws| {
                ws.values()
                    .cloned()
                    .map(super::normalize_workspace_record)
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn list_workspaces_page(
        &self,
        repo_id: &str,
        after: Option<(&str, &str)>,
        limit: usize,
        max_bytes: Option<usize>,
    ) -> StoreResult<(Vec<WorkspaceState>, bool)> {
        if limit == 0 {
            return Ok((Vec::new(), false));
        }
        let workspaces = self.workspaces.lock().unwrap();
        let Some(repo_ws) = workspaces.get(repo_id) else {
            return Ok((Vec::new(), false));
        };
        let mut matching: Vec<&WorkspaceState> = repo_ws
            .values()
            .filter(|ws| match after {
                Some((after_user, after_ws)) => {
                    (ws.user.as_str(), ws.workspace_name.as_str()) > (after_user, after_ws)
                }
                None => true,
            })
            .collect();
        matching.sort_by(|a, b| (&a.user, &a.workspace_name).cmp(&(&b.user, &b.workspace_name)));

        let mut result = Vec::new();
        let mut response_bytes = 0usize;
        let mut has_more = false;
        for ws_ref in matching {
            if result.len() >= limit {
                has_more = true;
                break;
            }
            let ws = super::normalize_workspace_record(ws_ref.clone());
            let entry_bytes = super::workspace_list_entry_bytes(&ws);
            if let Some(budget) = max_bytes
                && !result.is_empty()
                && response_bytes.saturating_add(entry_bytes) > budget
            {
                has_more = true;
                break;
            }
            response_bytes = response_bytes.saturating_add(entry_bytes);
            result.push(ws);
        }
        Ok((result, has_more))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(deprecated)]
    fn make_ws(repo_id: &str, user: &str, workspace_name: &str) -> WorkspaceState {
        WorkspaceState {
            repo_id: repo_id.to_string(),
            user: user.to_string(),
            workspace_name: workspace_name.to_string(),
            commit_id: cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
            operation_id: cc_common::ROOT_OPERATION_ID_BYTES.to_vec(),
            tree_id: cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
            sparse_patterns: vec![String::new()],
            tree_ids: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
            conflict_labels: Vec::new(),
            sparse_patterns_set: true,
        }
    }

    #[tokio::test]
    async fn test_memory_store_list_workspaces_page_keyset_pagination() {
        let store = MemoryStore::new();
        assert_eq!(
            store
                .list_workspaces_page("repo-1", None, 10, None)
                .await
                .unwrap(),
            (Vec::new(), false)
        );

        // Insert out of order across multiple users, multiple workspaces, and another repo.
        let fixtures = [
            ("repo-1", "bob", "ws-2"),
            ("repo-1", "alice", "z-ws"),
            ("repo-1", "alice", "a-ws"),
            ("repo-1", "bob", "ws-1"),
            ("repo-1", "", "default"),
            ("repo-1", "carol", "main"),
            ("repo-2", "alice", "isolated"),
        ];
        for (repo, user, ws) in fixtures {
            store.put_workspace(make_ws(repo, user, ws)).await.unwrap();
        }

        // limit == 0 returns immediately.
        assert_eq!(
            store
                .list_workspaces_page("repo-1", None, 0, None)
                .await
                .unwrap(),
            (Vec::new(), false)
        );

        // First page ordered strictly by (user ASC, workspace_name ASC).
        let (p1, has_more1) = store
            .list_workspaces_page("repo-1", None, 2, None)
            .await
            .unwrap();
        let keys1: Vec<(&str, &str)> = p1
            .iter()
            .map(|w| (w.user.as_str(), w.workspace_name.as_str()))
            .collect();
        assert_eq!(keys1, vec![("", "default"), ("alice", "a-ws")]);
        assert!(has_more1);

        // Cursor within the same user advances to that user's next workspace, then next user.
        let (p2, has_more2) = store
            .list_workspaces_page("repo-1", Some(("alice", "a-ws")), 2, None)
            .await
            .unwrap();
        let keys2: Vec<(&str, &str)> = p2
            .iter()
            .map(|w| (w.user.as_str(), w.workspace_name.as_str()))
            .collect();
        assert_eq!(keys2, vec![("alice", "z-ws"), ("bob", "ws-1")]);
        assert!(has_more2);

        // Cursor on the last workspace of a user advances cleanly to the next user.
        let (p3, has_more3) = store
            .list_workspaces_page("repo-1", Some(("bob", "ws-1")), 5, None)
            .await
            .unwrap();
        let keys3: Vec<(&str, &str)> = p3
            .iter()
            .map(|w| (w.user.as_str(), w.workspace_name.as_str()))
            .collect();
        assert_eq!(keys3, vec![("bob", "ws-2"), ("carol", "main")]);
        assert!(!has_more3);

        // Cursor at or past the last key returns empty.
        assert_eq!(
            store
                .list_workspaces_page("repo-1", Some(("carol", "main")), 5, None)
                .await
                .unwrap(),
            (Vec::new(), false)
        );
        assert_eq!(
            store
                .list_workspaces_page("repo-1", Some(("zzz", "zzz")), 5, None)
                .await
                .unwrap(),
            (Vec::new(), false)
        );

        // Cursor pointing to a missing/deleted key resumes at the next strictly greater row.
        let (after_gap, has_more_gap) = store
            .list_workspaces_page("repo-1", Some(("alice", "m-deleted")), 2, None)
            .await
            .unwrap();
        let gap_keys: Vec<(&str, &str)> = after_gap
            .iter()
            .map(|w| (w.user.as_str(), w.workspace_name.as_str()))
            .collect();
        assert_eq!(gap_keys, vec![("alice", "z-ws"), ("bob", "ws-1")]);
        assert!(has_more_gap);

        // Tight byte budget always returns at least one record and sets has_more = true.
        let (single, has_more_budget) = store
            .list_workspaces_page("repo-1", None, 10, Some(1))
            .await
            .unwrap();
        assert_eq!(single.len(), 1);
        assert!(has_more_budget);
    }

    #[tokio::test]
    async fn test_memory_store_lightweight_existence_checks() {
        let store = MemoryStore::new();
        let c1 = vec![0x11; 20];
        let v1 = vec![0x22; 16];
        let t1 = vec![0x33; 20];
        let t2 = vec![0x44; 20];
        let t_missing = vec![0x55; 20];

        assert!(!store.has_commit("proj-1", &c1).await.unwrap());
        assert!(!store.has_view("repo-1", &v1).await.unwrap());
        assert!(
            store
                .find_existing_trees("proj-1", &[t1.clone(), t2.clone()])
                .await
                .unwrap()
                .is_empty()
        );

        store
            .put_commit("proj-1".to_string(), c1.clone(), Commit::default())
            .await
            .unwrap();
        store
            .put_view("repo-1".to_string(), v1.clone(), View::default())
            .await
            .unwrap();
        store
            .put_tree("proj-1".to_string(), t1.clone(), Vec::new())
            .await
            .unwrap();
        store
            .put_tree("proj-1".to_string(), t2.clone(), Vec::new())
            .await
            .unwrap();

        assert!(store.has_commit("proj-1", &c1).await.unwrap());
        assert!(!store.has_commit("proj-other", &c1).await.unwrap());
        assert!(store.has_view("repo-1", &v1).await.unwrap());
        assert!(!store.has_view("repo-other", &v1).await.unwrap());
        let found = store
            .find_existing_trees("proj-1", &[t1.clone(), t_missing, t2.clone()])
            .await
            .unwrap();
        assert_eq!(found, HashSet::from([t1, t2]));
    }
}
