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

//! `MetadataStore` trait implementation for `MemoryMetadataStore`.
//!
//! # Authority and Crash-Safety Ordering
//! Project-scoped objects and repository-scoped operations mirror persistent backend
//! schemas in `Mutex`-guarded maps while rejecting cross-project namespace collisions.
//!
//! # Concurrency and Merge Invariants
//! Lock acquisition follows a fixed hierarchy (`projects` -> `repos` -> `project_names`,
//! `project_blobs` -> `blobs`, and `ops` -> `op_heads`) so concurrent reads and writes
//! never deadlock.

use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use cc_common::backend::*;
use cc_common::op_store::*;
use cc_common::workspace::WorkspaceState;

use super::{CommitId, MemoryMetadataStore, OpId, RepoMeta, StoredCommit, StoredOperation};
use crate::store::{
    BlobRecord, MetadataStore, StoreError, StoreResult, apply_op_heads_mutation,
    op_heads_sets_equal,
};

#[async_trait]
impl MetadataStore for MemoryMetadataStore {
    fn backend_name(&self) -> &'static str {
        "memory"
    }

    async fn register_project(&self, project_id: &str, name: Option<&str>) -> StoreResult<()> {
        let clean_name = name.map(str::trim).filter(|s| !s.is_empty());
        let mut projects = self.projects.lock().unwrap();
        let repos = self.repos.lock().unwrap();
        let mut project_names = self.project_names.lock().unwrap();
        if let Some(existing_meta) = repos.get(project_id)
            && existing_meta.project_id != project_id
        {
            return Err(StoreError::ProjectAlreadyExists(format!(
                "project_id '{project_id}' conflicts with existing repository '{project_id}' bound to project '{}'",
                existing_meta.project_id
            )));
        }
        if let Some(alias) = clean_name {
            if let Some(existing) = project_names.get(alias)
                && existing != project_id
            {
                return Err(StoreError::ProjectAlreadyExists(alias.to_string()));
            }
            if let Some(Some(prev_alias)) = projects.get(project_id)
                && prev_alias != alias
            {
                project_names.remove(prev_alias);
            }
            project_names.insert(alias.to_string(), project_id.to_string());
        }
        let entry = projects.entry(project_id.to_string()).or_insert(None);
        if clean_name.is_some() {
            *entry = clean_name.map(ToOwned::to_owned);
        }
        Ok(())
    }

    async fn is_project_registered(&self, project_id: &str) -> StoreResult<bool> {
        Ok(self.projects.lock().unwrap().contains_key(project_id))
    }

    async fn get_repo_project_id(&self, repo_id: &str) -> StoreResult<Option<String>> {
        Ok(self
            .repos
            .lock()
            .unwrap()
            .get(repo_id)
            .map(|m| m.project_id.clone()))
    }

    async fn register_repo_in_project(
        &self,
        repo_id: &str,
        project_id: &str,
        name: Option<&str>,
    ) -> StoreResult<String> {
        let mut projects = self.projects.lock().unwrap();
        let mut repos = self.repos.lock().unwrap();

        if let Some(existing_meta) = repos.get(repo_id)
            && existing_meta.project_id != project_id
        {
            return Err(StoreError::RepoAlreadyExists(format!(
                "repository '{repo_id}' is already bound to project '{}', cannot rebind to '{project_id}'",
                existing_meta.project_id
            )));
        }
        if let Some(shadowed_repo) = repos.get(project_id)
            && shadowed_repo.project_id != project_id
        {
            return Err(StoreError::RepoAlreadyExists(format!(
                "project_id '{project_id}' conflicts with existing repository '{project_id}' bound to project '{}'",
                shadowed_repo.project_id
            )));
        }
        if repo_id != project_id && projects.contains_key(repo_id) {
            return Err(StoreError::RepoAlreadyExists(format!(
                "repository '{repo_id}' conflicts with existing project '{repo_id}'"
            )));
        }

        projects.entry(project_id.to_string()).or_insert(None);
        let clean_name = name.map(str::trim).filter(|s| !s.is_empty());
        let effective_name = clean_name
            .map(ToOwned::to_owned)
            .or_else(|| repos.get(repo_id).and_then(|m| m.name.clone()));
        repos.insert(
            repo_id.to_string(),
            RepoMeta {
                project_id: project_id.to_string(),
                name: effective_name,
            },
        );
        Ok(project_id.to_string())
    }

    async fn get_commit(&self, project_id: &str, commit_id: &[u8]) -> StoreResult<Option<Commit>> {
        let commits = self.commits.lock().unwrap();
        Ok(commits
            .get(project_id)
            .and_then(|m| m.get(commit_id))
            .map(|sc| sc.commit.clone()))
    }

    async fn has_commit(&self, project_id: &str, commit_id: &[u8]) -> StoreResult<bool> {
        let commits = self.commits.lock().unwrap();
        Ok(commits
            .get(project_id)
            .is_some_and(|m| m.contains_key(commit_id)))
    }

    async fn get_commit_generation(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> StoreResult<Option<u64>> {
        let commits = self.commits.lock().unwrap();
        Ok(commits
            .get(project_id)
            .and_then(|m| m.get(commit_id))
            .map(|sc| sc.generation))
    }

    async fn get_commit_with_generation(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> StoreResult<Option<(Commit, u64)>> {
        let commits = self.commits.lock().unwrap();
        Ok(commits
            .get(project_id)
            .and_then(|m| m.get(commit_id))
            .map(|sc| (sc.commit.clone(), sc.generation)))
    }

    async fn get_commit_generations(
        &self,
        project_id: &str,
        commit_ids: &[Vec<u8>],
    ) -> StoreResult<HashMap<Vec<u8>, u64>> {
        let commits = self.commits.lock().unwrap();
        let Some(proj_commits) = commits.get(project_id) else {
            return Ok(HashMap::new());
        };
        Ok(commit_ids
            .iter()
            .filter_map(|id| proj_commits.get(id).map(|sc| (id.clone(), sc.generation)))
            .collect())
    }

    async fn put_commit(
        &self,
        project_id: &str,
        commit_id: &[u8],
        commit: &Commit,
        generation: u64,
    ) -> StoreResult<()> {
        let mut commits = self.commits.lock().unwrap();
        commits
            .entry(project_id.to_string())
            .or_default()
            .entry(commit_id.to_vec())
            .or_insert_with(|| {
                let seq = self.next_commit_seq.fetch_add(1, Ordering::Relaxed) + 1;
                StoredCommit {
                    commit: commit.clone(),
                    generation,
                    seq,
                }
            });
        Ok(())
    }

    async fn list_commit_ids(
        &self,
        project_id: &str,
        after_commit_id: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<Vec<CommitId>> {
        let commits = self.commits.lock().unwrap();
        let Some(proj_commits) = commits.get(project_id) else {
            return Ok(Vec::new());
        };
        match after_commit_id {
            Some(cursor) if !cursor.is_empty() => Ok(proj_commits
                .range((Bound::Excluded(cursor.to_vec()), Bound::Unbounded))
                .take(limit)
                .map(|(k, _)| k.clone())
                .collect()),
            _ => Ok(proj_commits.keys().take(limit).cloned().collect()),
        }
    }

    async fn current_commit_watermark(&self, project_id: &str) -> StoreResult<Vec<u8>> {
        let commits = self.commits.lock().unwrap();
        let max_seq = commits
            .get(project_id)
            .and_then(|m| m.values().map(|sc| sc.seq).max())
            .unwrap_or(0);
        Ok(max_seq.to_be_bytes().to_vec())
    }

    async fn list_commit_ids_since(
        &self,
        project_id: &str,
        since_token: &[u8],
        limit: usize,
    ) -> StoreResult<(Vec<CommitId>, Vec<u8>, bool)> {
        let since_seq = if since_token.len() == 8 {
            u64::from_be_bytes(since_token.try_into().unwrap())
        } else {
            return Err(StoreError::InvalidArgument(
                "invalid memory commit sync token".to_string(),
            ));
        };
        let commits = self.commits.lock().unwrap();
        let Some(proj_commits) = commits.get(project_id) else {
            return Ok((Vec::new(), since_token.to_vec(), false));
        };
        let mut ordered: Vec<(u64, CommitId)> = proj_commits
            .iter()
            .filter(|(_, sc)| sc.seq > since_seq)
            .map(|(cid, sc)| (sc.seq, cid.clone()))
            .collect();
        ordered.sort_by_key(|(seq, _)| *seq);
        let has_more = ordered.len() > limit;
        if has_more {
            ordered.truncate(limit);
        }
        let Some(&(last_seq, _)) = ordered.last() else {
            return Ok((Vec::new(), since_token.to_vec(), false));
        };
        let ids = ordered.into_iter().map(|(_, cid)| cid).collect();
        Ok((ids, last_seq.to_be_bytes().to_vec(), has_more))
    }

    async fn resolve_commit_id_prefix(
        &self,
        project_id: &str,
        min_id: &[u8],
        max_id: &[u8],
    ) -> StoreResult<Vec<CommitId>> {
        let commits = self.commits.lock().unwrap();
        let Some(proj_commits) = commits.get(project_id) else {
            return Ok(Vec::new());
        };
        Ok(proj_commits
            .range(min_id.to_vec()..=max_id.to_vec())
            .take(2)
            .map(|(k, _)| k.clone())
            .collect())
    }

    async fn resolve_change_id_prefix(
        &self,
        project_id: &str,
        min_id: &[u8],
        max_id: &[u8],
        limit: usize,
    ) -> StoreResult<Vec<(Vec<u8>, Vec<CommitId>)>> {
        let commits = self.commits.lock().unwrap();
        let Some(proj_commits) = commits.get(project_id) else {
            return Ok(Vec::new());
        };
        let mut matched_change: Option<Vec<u8>> = None;
        let mut matched_commits: Vec<CommitId> = Vec::new();
        for (cid, sc) in proj_commits {
            let ch = sc.commit.change_id.as_slice();
            if ch >= min_id && ch <= max_id {
                match &matched_change {
                    None => {
                        matched_change = Some(sc.commit.change_id.clone());
                        if matched_commits.len() < limit {
                            matched_commits.push(cid.clone());
                        }
                    }
                    Some(existing) if existing.as_slice() == ch => {
                        if matched_commits.len() < limit {
                            matched_commits.push(cid.clone());
                        }
                    }
                    Some(existing) => {
                        return Ok(vec![
                            (existing.clone(), Vec::new()),
                            (sc.commit.change_id.clone(), Vec::new()),
                        ]);
                    }
                }
            }
        }
        Ok(match matched_change {
            Some(ch) => vec![(ch, matched_commits)],
            None => Vec::new(),
        })
    }

    async fn get_tree(
        &self,
        project_id: &str,
        tree_id: &[u8],
    ) -> StoreResult<Option<Vec<TreeEntry>>> {
        let trees = self.trees.lock().unwrap();
        Ok(trees.get(project_id).and_then(|m| m.get(tree_id)).cloned())
    }

    async fn get_tree_page(
        &self,
        project_id: &str,
        tree_id: &[u8],
        after_name: Option<&str>,
        limit: usize,
    ) -> StoreResult<Option<Vec<TreeEntry>>> {
        let trees = self.trees.lock().unwrap();
        let Some(entries) = trees.get(project_id).and_then(|m| m.get(tree_id)) else {
            return Ok(None);
        };
        let iter = entries.iter().filter(|e| {
            after_name
                .filter(|a| !a.is_empty())
                .is_none_or(|after| e.name.as_bytes() > after.as_bytes())
        });
        Ok(Some(iter.take(limit).cloned().collect()))
    }

    async fn put_tree(
        &self,
        project_id: &str,
        tree_id: &[u8],
        entries: &[TreeEntry],
    ) -> StoreResult<()> {
        let mut sorted = entries.to_vec();
        sorted.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));
        let mut trees = self.trees.lock().unwrap();
        trees
            .entry(project_id.to_string())
            .or_default()
            .entry(tree_id.to_vec())
            .or_insert(sorted);
        Ok(())
    }

    async fn find_existing_trees(
        &self,
        project_id: &str,
        tree_ids: &[Vec<u8>],
    ) -> StoreResult<HashSet<Vec<u8>>> {
        let trees = self.trees.lock().unwrap();
        let Some(proj_trees) = trees.get(project_id) else {
            return Ok(HashSet::new());
        };
        Ok(tree_ids
            .iter()
            .filter(|id| proj_trees.contains_key(id.as_slice()))
            .cloned()
            .collect())
    }

    async fn get_symlink(
        &self,
        project_id: &str,
        symlink_id: &[u8],
    ) -> StoreResult<Option<String>> {
        let symlinks = self.symlinks.lock().unwrap();
        Ok(symlinks
            .get(project_id)
            .and_then(|m| m.get(symlink_id))
            .cloned())
    }

    async fn put_symlink(
        &self,
        project_id: &str,
        symlink_id: &[u8],
        target: &str,
    ) -> StoreResult<()> {
        let mut symlinks = self.symlinks.lock().unwrap();
        symlinks
            .entry(project_id.to_string())
            .or_default()
            .entry(symlink_id.to_vec())
            .or_insert_with(|| target.to_string());
        Ok(())
    }

    async fn find_existing_symlinks(
        &self,
        project_id: &str,
        symlink_ids: &[Vec<u8>],
    ) -> StoreResult<HashSet<Vec<u8>>> {
        let symlinks = self.symlinks.lock().unwrap();
        let Some(proj_symlinks) = symlinks.get(project_id) else {
            return Ok(HashSet::new());
        };
        Ok(symlink_ids
            .iter()
            .filter(|id| proj_symlinks.contains_key(id.as_slice()))
            .cloned()
            .collect())
    }

    async fn get_blob_record(
        &self,
        project_id: &str,
        blob_id: &[u8],
    ) -> StoreResult<Option<BlobRecord>> {
        let project_blobs = self.project_blobs.lock().unwrap();
        let authorized = project_blobs
            .get(project_id)
            .is_some_and(|set| set.contains(blob_id));
        if !authorized {
            return Ok(None);
        }
        let blobs = self.blobs.lock().unwrap();
        Ok(blobs.get(blob_id).cloned())
    }

    async fn get_blob_sizes_batch(
        &self,
        project_id: &str,
        blob_ids: &[Vec<u8>],
    ) -> StoreResult<HashMap<Vec<u8>, u64>> {
        let project_blobs = self.project_blobs.lock().unwrap();
        let Some(proj_set) = project_blobs.get(project_id) else {
            return Ok(HashMap::new());
        };
        let blobs = self.blobs.lock().unwrap();
        let mut out = HashMap::with_capacity(blob_ids.len());
        for id in blob_ids {
            if proj_set.contains(id.as_slice())
                && let Some(rec) = blobs.get(id.as_slice())
            {
                out.insert(id.clone(), rec.content_size);
            }
        }
        Ok(out)
    }

    async fn get_global_blob_record(&self, blob_id: &[u8]) -> StoreResult<Option<BlobRecord>> {
        let blobs = self.blobs.lock().unwrap();
        Ok(blobs.get(blob_id).cloned())
    }

    async fn put_blob_record(&self, project_id: &str, record: &BlobRecord) -> StoreResult<()> {
        let mut project_blobs = self.project_blobs.lock().unwrap();
        let mut blobs = self.blobs.lock().unwrap();
        blobs
            .entry(record.blob_id.clone())
            .and_modify(|existing| {
                if existing.primary_location.is_none() {
                    existing.primary_location = record.primary_location.clone();
                }
                if existing.inline_content.is_none() {
                    existing.inline_content = record.inline_content.clone();
                }
                for loc in &record.replicated_locations {
                    if !existing.replicated_locations.iter().any(|l| l == loc) {
                        existing.replicated_locations.push(loc.clone());
                    }
                }
            })
            .or_insert_with(|| record.clone());
        project_blobs
            .entry(project_id.to_string())
            .or_default()
            .insert(record.blob_id.clone());
        Ok(())
    }

    async fn add_blob_replica_location(&self, blob_id: &[u8], location: &str) -> StoreResult<()> {
        let mut blobs = self.blobs.lock().unwrap();
        if let Some(rec) = blobs.get_mut(blob_id)
            && !rec.replicated_locations.iter().any(|l| l == location)
        {
            rec.replicated_locations.push(location.to_string());
        }
        Ok(())
    }

    async fn get_operation(&self, repo_id: &str, op_id: &[u8]) -> StoreResult<Option<Operation>> {
        let ops = self.ops.lock().unwrap();
        Ok(ops
            .get(repo_id)
            .and_then(|m| m.get(op_id))
            .map(|stored| stored.op.clone()))
    }

    async fn put_operation(&self, repo_id: &str, op_id: &[u8], op: &Operation) -> StoreResult<()> {
        let mut ops = self.ops.lock().unwrap();
        ops.entry(repo_id.to_string())
            .or_default()
            .entry(op_id.to_vec())
            .or_insert_with(|| StoredOperation {
                op: op.clone(),
                published: false,
            });
        Ok(())
    }

    async fn resolve_operation_id_prefix(
        &self,
        repo_id: &str,
        min_id: &[u8],
        max_id: &[u8],
    ) -> StoreResult<Vec<OpId>> {
        let ops = self.ops.lock().unwrap();
        let Some(repo_ops) = ops.get(repo_id) else {
            return Ok(Vec::new());
        };
        Ok(repo_ops
            .range(min_id.to_vec()..=max_id.to_vec())
            .take(2)
            .map(|(k, _)| k.clone())
            .collect())
    }

    async fn get_view(&self, repo_id: &str, view_id: &[u8]) -> StoreResult<Option<View>> {
        let views = self.views.lock().unwrap();
        Ok(views.get(repo_id).and_then(|m| m.get(view_id)).cloned())
    }

    async fn has_view(&self, repo_id: &str, view_id: &[u8]) -> StoreResult<bool> {
        let views = self.views.lock().unwrap();
        Ok(views.get(repo_id).is_some_and(|m| m.contains_key(view_id)))
    }

    async fn put_view(&self, repo_id: &str, view_id: &[u8], view: &View) -> StoreResult<()> {
        let mut views = self.views.lock().unwrap();
        views
            .entry(repo_id.to_string())
            .or_default()
            .entry(view_id.to_vec())
            .or_insert_with(|| view.clone());
        Ok(())
    }

    async fn get_op_heads(&self, repo_id: &str) -> StoreResult<Option<Vec<OpId>>> {
        let op_heads = self.op_heads.lock().unwrap();
        Ok(op_heads.get(repo_id).cloned())
    }

    async fn update_op_heads(
        &self,
        repo_id: &str,
        old_ids: &[Vec<u8>],
        new_id: &[u8],
    ) -> StoreResult<Vec<OpId>> {
        let mut ops = self.ops.lock().unwrap();
        let mut op_heads = self.op_heads.lock().unwrap();
        let current_heads = op_heads
            .entry(repo_id.to_string())
            .or_insert_with(|| vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()]);
        let already_published = ops
            .get(repo_id)
            .and_then(|m| m.get(new_id))
            .is_some_and(|stored| stored.published);
        if already_published && !current_heads.iter().any(|h| h.as_slice() == new_id) {
            return Ok(current_heads.clone());
        }
        apply_op_heads_mutation(current_heads, old_ids, new_id);
        if let Some(stored) = ops.get_mut(repo_id).and_then(|m| m.get_mut(new_id)) {
            stored.published = true;
        }
        Ok(current_heads.clone())
    }

    async fn publish_operation_atomic(
        &self,
        repo_id: &str,
        view_id: &[u8],
        view: &View,
        op_id: &[u8],
        op: &Operation,
        old_ids: &[Vec<u8>],
    ) -> StoreResult<Vec<OpId>> {
        {
            let mut views = self.views.lock().unwrap();
            views
                .entry(repo_id.to_string())
                .or_default()
                .entry(view_id.to_vec())
                .or_insert_with(|| view.clone());
        }
        let mut ops = self.ops.lock().unwrap();
        let mut op_heads = self.op_heads.lock().unwrap();
        let current_heads = op_heads
            .entry(repo_id.to_string())
            .or_insert_with(|| vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()]);
        let repo_ops = ops.entry(repo_id.to_string()).or_default();
        if repo_ops.get(op_id).is_some_and(|stored| stored.published) {
            return Ok(current_heads.clone());
        }
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

    async fn compare_and_swap_op_heads(
        &self,
        repo_id: &str,
        expected_old_ids: &[Vec<u8>],
        new_id: &[u8],
    ) -> StoreResult<()> {
        let mut ops = self.ops.lock().unwrap();
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

    async fn get_workspace(
        &self,
        repo_id: &str,
        user: &str,
        workspace_name: &str,
    ) -> StoreResult<Option<WorkspaceState>> {
        let workspaces = self.workspaces.lock().unwrap();
        Ok(workspaces
            .get(repo_id)
            .and_then(|m| m.get(&(user.to_string(), workspace_name.to_string())))
            .cloned())
    }

    async fn put_workspace_cas(
        &self,
        workspace: &WorkspaceState,
        expected_operation_id: Option<&[u8]>,
    ) -> StoreResult<()> {
        let mut workspaces = self.workspaces.lock().unwrap();
        let repo_map = workspaces.entry(workspace.repo_id.clone()).or_default();
        let key = (workspace.user.clone(), workspace.workspace_name.clone());
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

    async fn list_workspaces(&self, repo_id: &str) -> StoreResult<Vec<WorkspaceState>> {
        let workspaces = self.workspaces.lock().unwrap();
        Ok(workspaces
            .get(repo_id)
            .map(|m| m.values().cloned().collect())
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
        let Some(repo_map) = workspaces.get(repo_id) else {
            return Ok((Vec::new(), false));
        };
        let mut page = Vec::new();
        let mut response_bytes = 0usize;
        let mut has_more = false;
        let iter: Box<dyn Iterator<Item = &WorkspaceState> + '_> = match after {
            Some((after_user, after_ws)) => Box::new(
                repo_map
                    .range((
                        Bound::Excluded((after_user.to_string(), after_ws.to_string())),
                        Bound::Unbounded,
                    ))
                    .map(|(_, ws)| ws),
            ),
            None => Box::new(repo_map.values()),
        };
        for ws in iter {
            let entry_bytes = crate::store::workspace_list_entry_bytes(ws);
            if page.len() >= limit
                || max_bytes.is_some_and(|cap| {
                    !page.is_empty() && response_bytes.saturating_add(entry_bytes) > cap
                })
            {
                has_more = true;
                break;
            }
            response_bytes = response_bytes.saturating_add(entry_bytes);
            page.push(ws.clone());
        }
        Ok((page, has_more))
    }

    async fn delete_workspace(
        &self,
        repo_id: &str,
        user: &str,
        workspace_name: &str,
    ) -> StoreResult<bool> {
        let mut workspaces = self.workspaces.lock().unwrap();
        Ok(workspaces
            .get_mut(repo_id)
            .and_then(|m| m.remove(&(user.to_string(), workspace_name.to_string())))
            .is_some())
    }
}
