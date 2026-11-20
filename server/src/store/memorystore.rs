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

//! Ephemeral in-memory [`Store`] implementation for single-node development and tests.
//!
//! # Concurrency and Merge Invariants
//! `register_repo` is non-destructive, and all content-addressed `put_*` methods are
//! strictly insert-only (`or_insert`) so existing immutable objects cannot be mutated.
//! `update_op_heads` acquires `op_heads` before `ops` (no other method holds two mutexes)
//! and prunes superseded `old_ids` while retaining concurrent sibling operation heads and
//! ignoring stale retries whose `new_id` is already an ancestor of an active head.

use async_trait::async_trait;
use cc_common::backend::*;
use cc_common::op_store::*;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Mutex;

use super::Store;

pub type RepoId = String;
pub type CommitId = Vec<u8>;
pub type TreeId = Vec<u8>;
pub type FileId = Vec<u8>;
pub type OpId = Vec<u8>;
pub type ViewId = Vec<u8>;

fn is_strict_op_ancestor(
    repo_ops: Option<&HashMap<OpId, Operation>>,
    candidate: &[u8],
    heads: &[OpId],
) -> bool {
    let mut visited: HashSet<Vec<u8>> = HashSet::new();
    let mut queue: VecDeque<Vec<u8>> = heads
        .iter()
        .filter(|h| h.as_slice() != candidate)
        .cloned()
        .collect();
    while let Some(curr) = queue.pop_front() {
        if curr.as_slice() == cc_common::ROOT_OPERATION_ID_BYTES {
            continue;
        }
        let Some(op) = repo_ops.and_then(|m| m.get(&curr)) else {
            continue;
        };
        for parent in &op.parents {
            if parent.as_slice() == candidate {
                return true;
            }
            if visited.insert(parent.clone()) {
                queue.push_back(parent.clone());
            }
        }
    }
    false
}

#[derive(Debug, Default)]
pub struct MemoryStore {
    pub repos: Mutex<HashSet<RepoId>>,
    pub commits: Mutex<HashMap<RepoId, HashMap<CommitId, Commit>>>,
    pub trees: Mutex<HashMap<RepoId, HashMap<TreeId, Vec<TreeEntry>>>>,
    pub files: Mutex<HashMap<RepoId, HashMap<FileId, Vec<u8>>>>,
    pub ops: Mutex<HashMap<RepoId, HashMap<OpId, Operation>>>,
    pub views: Mutex<HashMap<RepoId, HashMap<ViewId, View>>>,
    pub op_heads: Mutex<HashMap<RepoId, Vec<OpId>>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Store for MemoryStore {
    async fn is_repo_registered(&self, repo_id: &str) -> bool {
        self.repos.lock().unwrap().contains(repo_id)
    }

    async fn register_repo(&self, repo_id: String) {
        self.repos.lock().unwrap().insert(repo_id.clone());
        self.op_heads
            .lock()
            .unwrap()
            .entry(repo_id)
            .or_insert_with(|| vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()]);
    }

    async fn get_commit(&self, repo_id: &str, commit_id: &[u8]) -> Option<Commit> {
        let commits = self.commits.lock().unwrap();
        commits.get(repo_id)?.get(commit_id).cloned()
    }

    async fn put_commit(&self, repo_id: String, commit_id: Vec<u8>, commit: Commit) {
        let mut commits = self.commits.lock().unwrap();
        commits
            .entry(repo_id)
            .or_default()
            .entry(commit_id)
            .or_insert(commit);
    }

    async fn get_tree(&self, repo_id: &str, tree_id: &[u8]) -> Option<Vec<TreeEntry>> {
        let trees = self.trees.lock().unwrap();
        trees.get(repo_id)?.get(tree_id).cloned()
    }

    async fn put_tree(&self, repo_id: String, tree_id: Vec<u8>, entries: Vec<TreeEntry>) {
        let mut trees = self.trees.lock().unwrap();
        trees
            .entry(repo_id)
            .or_default()
            .entry(tree_id)
            .or_insert(entries);
    }

    async fn get_file(&self, repo_id: &str, file_id: &[u8]) -> Option<Vec<u8>> {
        let files = self.files.lock().unwrap();
        files.get(repo_id)?.get(file_id).cloned()
    }

    async fn put_file(&self, repo_id: String, file_id: Vec<u8>, content: Vec<u8>) {
        let mut files = self.files.lock().unwrap();
        files
            .entry(repo_id)
            .or_default()
            .entry(file_id)
            .or_insert(content);
    }

    async fn get_operation(&self, repo_id: &str, op_id: &[u8]) -> Option<Operation> {
        let ops = self.ops.lock().unwrap();
        ops.get(repo_id)?.get(op_id).cloned()
    }

    async fn put_operation(&self, repo_id: String, op_id: Vec<u8>, op: Operation) {
        let mut ops = self.ops.lock().unwrap();
        ops.entry(repo_id).or_default().entry(op_id).or_insert(op);
    }

    async fn get_view(&self, repo_id: &str, view_id: &[u8]) -> Option<View> {
        let views = self.views.lock().unwrap();
        views.get(repo_id)?.get(view_id).cloned()
    }

    async fn put_view(&self, repo_id: String, view_id: Vec<u8>, view: View) {
        let mut views = self.views.lock().unwrap();
        views
            .entry(repo_id)
            .or_default()
            .entry(view_id)
            .or_insert(view);
    }

    async fn get_op_heads(&self, repo_id: &str) -> Option<Vec<OpId>> {
        let op_heads = self.op_heads.lock().unwrap();
        op_heads.get(repo_id).cloned()
    }

    async fn update_op_heads(
        &self,
        repo_id: String,
        old_ids: &[Vec<u8>],
        new_id: Vec<u8>,
    ) -> Vec<OpId> {
        let mut op_heads = self.op_heads.lock().unwrap();
        let ops = self.ops.lock().unwrap();
        let repo_ops = ops.get(&repo_id);
        let current_heads = op_heads
            .entry(repo_id)
            .or_insert_with(|| vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()]);

        current_heads.retain(|head| head == &new_id || !old_ids.contains(head));

        if !current_heads.contains(&new_id)
            && !is_strict_op_ancestor(repo_ops, &new_id, current_heads)
        {
            current_heads.push(new_id);
        }

        current_heads.clone()
    }
}
