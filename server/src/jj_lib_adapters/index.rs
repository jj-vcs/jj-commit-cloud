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

//! Server-side `jj_lib::index::Index` and `IndexStore` adapters backed by [`CompositeStore`].
//!
//! # Concurrency and Merge Invariants
//! Prunes `is_ancestor`, `heads`, and `common_ancestors` graph traversals with commit
//! generation numbers, enforces per-walk (`MAX_ANCESTRY_WALK_COMMITS`) and aggregate
//! (`MAX_RECONCILE_TOTAL_COMMIT_VISITS`) visit budgets failing closed with
//! [`IndexWalkBudgetExceeded`], caches immutable commit graph nodes in a globally bounded
//! store-scoped cache, and surfaces missing or malformed rows as [`StoreError::Corruption`].

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};
use std::fmt::{self, Debug};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock};

use async_trait::async_trait;
use futures::stream::LocalBoxStream;
use jj_lib::backend::{ChangeId, CommitId};
use jj_lib::commit::Commit as JjCommit;
use jj_lib::graph::GraphNode;
use jj_lib::index::{
    ChangeIdIndex, Index, IndexError, IndexResult, MutableIndex, ReadonlyIndex,
    ResolvedChangeTargets,
};
use jj_lib::index_store::{IndexStore, IndexStoreResult};
use jj_lib::object_id::{HexPrefix, ObjectId, PrefixResolution};
use jj_lib::operation::Operation as JjOperation;
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::revset::{ResolvedExpression, Revset, RevsetContainingFn, RevsetEvaluationError};
use jj_lib::store::Store as JjStore;

use super::{BoundedCache, StoreScopedEntry, store_cache_key, store_entry_matches};
use crate::store::{CompositeStore, StoreError};

const COMMIT_GRAPH_CACHE_CAPACITY: usize = 65_536;

/// Maximum number of commits visited during a single server-side commit-graph traversal
/// before failing closed to client-side reconciliation.
pub const MAX_ANCESTRY_WALK_COMMITS: usize = 4_096;

/// Aggregate upper bound on commit visits across all traversals within a single
/// reconciliation attempt (prevents quadratic CPU blowup over already-cached commits).
pub const MAX_RECONCILE_TOTAL_COMMIT_VISITS: usize = 65_536;

/// Typed error returned inside `IndexError::Other` when a `ServerIndex` commit-graph walk
/// exceeds its per-walk or aggregate visit budget.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexWalkBudgetExceeded {
    pub limit: usize,
}

impl fmt::Display for IndexWalkBudgetExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "commit ancestry walk exceeded budget of {} commits",
            self.limit
        )
    }
}

impl std::error::Error for IndexWalkBudgetExceeded {}

/// Per-walk and aggregate commit-graph visit limits enforced by [`ServerIndex`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexWalkLimits {
    pub max_walk_commits: usize,
    pub max_total_walk_visits: usize,
}

impl Default for IndexWalkLimits {
    fn default() -> Self {
        Self {
            max_walk_commits: MAX_ANCESTRY_WALK_COMMITS,
            max_total_walk_visits: MAX_RECONCILE_TOTAL_COMMIT_VISITS,
        }
    }
}

/// Shared per-reconciliation visit budget tracking aggregate commit-graph visits across
/// precheck walks and `RepoLoader::load_at_head` index queries.
#[derive(Debug, Clone)]
pub struct IndexWalkBudget {
    limits: IndexWalkLimits,
    total_visits: Arc<AtomicUsize>,
}

impl Default for IndexWalkBudget {
    fn default() -> Self {
        Self::new(IndexWalkLimits::default())
    }
}

impl IndexWalkBudget {
    pub fn new(limits: IndexWalkLimits) -> Self {
        Self {
            limits,
            total_visits: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn limits(&self) -> IndexWalkLimits {
        self.limits
    }

    pub fn max_walk_commits(&self) -> usize {
        self.limits.max_walk_commits
    }

    pub fn check_walk_count(&self, walk_visits: usize) -> Result<(), IndexWalkBudgetExceeded> {
        if walk_visits > self.limits.max_walk_commits {
            return Err(IndexWalkBudgetExceeded {
                limit: self.limits.max_walk_commits,
            });
        }
        Ok(())
    }

    pub fn record_aggregate_visit(&self) -> Result<(), IndexWalkBudgetExceeded> {
        let prev = self.total_visits.fetch_add(1, AtomicOrdering::Relaxed);
        if prev >= self.limits.max_total_walk_visits {
            return Err(IndexWalkBudgetExceeded {
                limit: self.limits.max_total_walk_visits,
            });
        }
        Ok(())
    }

    pub fn record_walk_visit(
        &self,
        walk_visits: &mut usize,
    ) -> Result<(), IndexWalkBudgetExceeded> {
        *walk_visits = walk_visits.saturating_add(1);
        self.check_walk_count(*walk_visits)?;
        self.record_aggregate_visit()
    }
}

pub(super) static COMMIT_GRAPH_CACHE: LazyLock<
    Mutex<BoundedCache<(usize, String, CommitId), StoreScopedEntry<(u64, Vec<CommitId>)>>>,
> = LazyLock::new(|| Mutex::new(BoundedCache::with_capacity(COMMIT_GRAPH_CACHE_CAPACITY)));

fn validate_stored_parent_ids(
    commit_id: &CommitId,
    parents: &[Vec<u8>],
) -> Result<Vec<CommitId>, StoreError> {
    let mut out = Vec::with_capacity(parents.len());
    for p in parents {
        if p.len() != cc_common::COMMIT_ID_LENGTH {
            return Err(StoreError::Corruption(format!(
                "corrupt commit graph: stored commit {} parent_commit_ids entry must be {} bytes (got {} bytes)",
                commit_id.hex(),
                cc_common::COMMIT_ID_LENGTH,
                p.len()
            )));
        }
        out.push(CommitId::from_bytes(p));
    }
    Ok(out)
}

#[derive(Debug)]
pub struct EmptyRevset;

#[async_trait(?Send)]
impl Revset for EmptyRevset {
    fn stream<'a>(&self) -> LocalBoxStream<'a, Result<CommitId, RevsetEvaluationError>>
    where
        Self: 'a,
    {
        Box::pin(futures::stream::empty())
    }

    fn commit_change_ids<'a>(
        &self,
    ) -> LocalBoxStream<'a, Result<(CommitId, ChangeId), RevsetEvaluationError>>
    where
        Self: 'a,
    {
        Box::pin(futures::stream::empty())
    }

    fn stream_graph<'a>(
        &self,
    ) -> LocalBoxStream<'a, Result<GraphNode<CommitId>, RevsetEvaluationError>>
    where
        Self: 'a,
    {
        Box::pin(futures::stream::empty())
    }

    async fn is_empty(&self) -> Result<bool, RevsetEvaluationError> {
        Ok(true)
    }

    async fn count_estimate(&self) -> Result<(usize, Option<usize>), RevsetEvaluationError> {
        Ok((0, Some(0)))
    }

    fn containing_fn<'a>(&self) -> Box<RevsetContainingFn<'a>>
    where
        Self: 'a,
    {
        Box::new(|_id| Box::pin(async { Ok(false) }))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GenCommitNode(u64, CommitId);

impl Ord for GenCommitNode {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .cmp(&other.0)
            .then_with(|| self.1.as_bytes().cmp(other.1.as_bytes()))
    }
}

impl PartialOrd for GenCommitNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Graph-aware [`Index`] backed by [`CompositeStore`] commit headers and generation
/// numbers so view merges resolve fast-forward bookmark advances (`is_ancestor`) and
/// prune superseded heads (`heads`) within bounded commit-visit caps.
#[derive(Clone)]
pub struct ServerIndex {
    store: Arc<CompositeStore>,
    repo_id: String,
    project_id: Arc<OnceLock<String>>,
    budget: IndexWalkBudget,
}

impl Debug for ServerIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerIndex")
            .field("repo_id", &self.repo_id)
            .field("limits", &self.budget.limits())
            .finish()
    }
}

impl ServerIndex {
    pub fn new(store: Arc<CompositeStore>, repo_id: String) -> Self {
        Self::with_budget(store, repo_id, IndexWalkBudget::default())
    }

    pub fn with_walk_limits(
        store: Arc<CompositeStore>,
        repo_id: String,
        limits: IndexWalkLimits,
    ) -> Self {
        Self::with_budget(store, repo_id, IndexWalkBudget::new(limits))
    }

    pub fn with_budget(
        store: Arc<CompositeStore>,
        repo_id: String,
        budget: IndexWalkBudget,
    ) -> Self {
        Self {
            store,
            repo_id,
            project_id: Arc::new(OnceLock::new()),
            budget,
        }
    }

    pub fn walk_budget(&self) -> &IndexWalkBudget {
        &self.budget
    }

    fn record_walk_visit(&self, walk_visits: &mut usize) -> IndexResult<()> {
        self.budget
            .record_walk_visit(walk_visits)
            .map_err(|e| IndexError::Other(Box::new(e)))
    }

    async fn resolve_project_id(&self) -> IndexResult<&str> {
        if let Some(pid) = self.project_id.get() {
            return Ok(pid.as_str());
        }
        let resolved = self
            .store
            .resolve_repo_project(&self.repo_id, "indexing server commits")
            .await
            .map_err(|e| IndexError::Other(e.into()))?;
        let _ = self.project_id.set(resolved);
        Ok(self.project_id.get().unwrap().as_str())
    }

    async fn commit_node(&self, commit_id: &CommitId) -> IndexResult<(u64, Vec<CommitId>)> {
        if commit_id.as_bytes() == cc_common::ROOT_COMMIT_ID_BYTES {
            return Ok((0, Vec::new()));
        }
        let project_id = self.resolve_project_id().await?;
        let cache_key = (
            store_cache_key(&self.store),
            project_id.to_string(),
            commit_id.clone(),
        );
        if let Some(entry) = COMMIT_GRAPH_CACHE
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&cache_key)
            && store_entry_matches(entry, &self.store)
        {
            return Ok(entry.value.clone());
        }

        let (proto_commit, generation) = self
            .store
            .metadata()
            .get_commit_with_generation(project_id, commit_id.as_bytes())
            .await
            .map_err(|e| IndexError::Other(e.into()))?
            .ok_or_else(|| {
                IndexError::Other(
                    StoreError::Corruption(format!(
                        "corrupt commit graph: missing commit {} in project {project_id}",
                        commit_id.hex()
                    ))
                    .into(),
                )
            })?;
        let parents = validate_stored_parent_ids(commit_id, &proto_commit.parent_commit_ids)
            .map_err(|e| IndexError::Other(e.into()))?;

        COMMIT_GRAPH_CACHE
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                cache_key,
                StoreScopedEntry {
                    metadata_ref: Arc::downgrade(self.store.metadata()),
                    value: (generation, parents.clone()),
                },
            );
        Ok((generation, parents))
    }

    async fn enqueue_parents(
        &self,
        commit_id: &CommitId,
        min_gen: u64,
        proper_ancestors: &mut HashSet<CommitId>,
        frontier: &mut BinaryHeap<GenCommitNode>,
        walk_visits: &mut usize,
    ) -> IndexResult<()> {
        let (_, parents) = self.commit_node(commit_id).await?;
        for p in parents {
            if proper_ancestors.insert(p.clone()) && p.as_bytes() != cc_common::ROOT_COMMIT_ID_BYTES
            {
                self.record_walk_visit(walk_visits)?;
                let (p_gen, _) = self.commit_node(&p).await?;
                if p_gen > min_gen || p_gen == u64::MAX {
                    frontier.push(GenCommitNode(p_gen, p));
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Index for ServerIndex {
    async fn shortest_unique_commit_id_prefix_len(
        &self,
        _commit_id: &CommitId,
    ) -> IndexResult<usize> {
        Ok(12)
    }

    async fn resolve_commit_id_prefix(
        &self,
        _prefix: &HexPrefix,
    ) -> IndexResult<PrefixResolution<CommitId>> {
        Ok(PrefixResolution::NoMatch)
    }

    async fn has_id(&self, commit_id: &CommitId) -> IndexResult<bool> {
        if commit_id.as_bytes() == cc_common::ROOT_COMMIT_ID_BYTES {
            return Ok(true);
        }
        let project_id = self.resolve_project_id().await?;
        let cache_key = (
            store_cache_key(&self.store),
            project_id.to_string(),
            commit_id.clone(),
        );
        if let Some(entry) = COMMIT_GRAPH_CACHE
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&cache_key)
            && store_entry_matches(entry, &self.store)
        {
            return Ok(true);
        }
        match self
            .store
            .metadata()
            .get_commit_with_generation(project_id, commit_id.as_bytes())
            .await
            .map_err(|e| IndexError::Other(e.into()))?
        {
            Some((proto_commit, generation)) => {
                let parents =
                    validate_stored_parent_ids(commit_id, &proto_commit.parent_commit_ids)
                        .map_err(|e| IndexError::Other(e.into()))?;
                COMMIT_GRAPH_CACHE
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(
                        cache_key,
                        StoreScopedEntry {
                            metadata_ref: Arc::downgrade(self.store.metadata()),
                            value: (generation, parents),
                        },
                    );
                Ok(true)
            }
            None => Ok(false),
        }
    }

    async fn is_ancestor(
        &self,
        ancestor_id: &CommitId,
        descendant_id: &CommitId,
    ) -> IndexResult<bool> {
        if ancestor_id == descendant_id || ancestor_id.as_bytes() == cc_common::ROOT_COMMIT_ID_BYTES
        {
            return Ok(true);
        }
        if descendant_id.as_bytes() == cc_common::ROOT_COMMIT_ID_BYTES {
            return Ok(false);
        }
        let mut walk_visits = 0usize;
        self.record_walk_visit(&mut walk_visits)?;
        let (target_gen, _) = self.commit_node(ancestor_id).await?;
        let (desc_gen, _) = self.commit_node(descendant_id).await?;
        if desc_gen <= target_gen && target_gen < u64::MAX {
            return Ok(false);
        }

        let mut visited = HashSet::from([descendant_id.clone()]);
        let mut heap = BinaryHeap::from([GenCommitNode(desc_gen, descendant_id.clone())]);
        while let Some(GenCommitNode(_, curr_id)) = heap.pop() {
            let (_, parents) = self.commit_node(&curr_id).await?;
            for p in parents {
                if &p == ancestor_id {
                    return Ok(true);
                }
                if p.as_bytes() == cc_common::ROOT_COMMIT_ID_BYTES || !visited.insert(p.clone()) {
                    continue;
                }
                self.record_walk_visit(&mut walk_visits)?;
                let (p_gen, _) = self.commit_node(&p).await?;
                if target_gen == u64::MAX || p_gen > target_gen || p_gen == u64::MAX {
                    heap.push(GenCommitNode(p_gen, p));
                }
            }
        }
        Ok(false)
    }

    async fn common_ancestors(
        &self,
        set1: &[CommitId],
        set2: &[CommitId],
    ) -> IndexResult<Vec<CommitId>> {
        if set1.is_empty() || set2.is_empty() {
            return Ok(Vec::new());
        }
        let walk_visits = Arc::new(AtomicUsize::new(0));
        let record_ca_visit =
            |budget: &IndexWalkBudget, counter: &AtomicUsize| -> IndexResult<()> {
                let prev = counter.fetch_add(1, AtomicOrdering::Relaxed);
                budget
                    .check_walk_count(prev.saturating_add(1))
                    .map_err(|e| IndexError::Other(Box::new(e)))?;
                budget
                    .record_aggregate_visit()
                    .map_err(|e| IndexError::Other(Box::new(e)))?;
                Ok(())
            };

        let mut start1 = Vec::with_capacity(set1.len());
        for c in set1 {
            if c.as_bytes() != cc_common::ROOT_COMMIT_ID_BYTES {
                record_ca_visit(&self.budget, &walk_visits)?;
            }
            start1.push(GenCommitNode(self.commit_node(c).await?.0, c.clone()));
        }
        let mut start2 = Vec::with_capacity(set2.len());
        for c in set2 {
            if c.as_bytes() != cc_common::ROOT_COMMIT_ID_BYTES {
                record_ca_visit(&self.budget, &walk_visits)?;
            }
            start2.push(GenCommitNode(self.commit_node(c).await?.0, c.clone()));
        }
        let root_id = CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES);
        let this = self.clone();
        let walk_visits_for_closure = Arc::clone(&walk_visits);
        let common_nodes = jj_lib::dag_walk_async::closest_common_nodes(
            start1,
            start2,
            |n: &GenCommitNode| n.1.clone(),
            move |n: &GenCommitNode| {
                let this = this.clone();
                let cid = n.1.clone();
                let root_id = root_id.clone();
                let walk_visits = Arc::clone(&walk_visits_for_closure);
                async move {
                    if cid == root_id {
                        return Ok::<Vec<GenCommitNode>, IndexError>(Vec::new());
                    }
                    let (_, mut parents) = this.commit_node(&cid).await?;
                    if parents.is_empty() {
                        parents.push(root_id.clone());
                    }
                    let mut out = Vec::with_capacity(parents.len());
                    for p in parents {
                        if p != root_id {
                            let prev = walk_visits.fetch_add(1, AtomicOrdering::Relaxed);
                            this.budget
                                .check_walk_count(prev.saturating_add(1))
                                .map_err(|e| IndexError::Other(Box::new(e)))?;
                            this.budget
                                .record_aggregate_visit()
                                .map_err(|e| IndexError::Other(Box::new(e)))?;
                        }
                        out.push(GenCommitNode(this.commit_node(&p).await?.0, p));
                    }
                    Ok(out)
                }
            },
        )
        .await?;
        let common_ids: Vec<CommitId> = common_nodes.into_iter().map(|n| n.1).collect();
        self.heads(&mut common_ids.iter()).await
    }

    fn all_heads_for_gc(&self) -> IndexResult<Box<dyn Iterator<Item = CommitId> + '_>> {
        Err(IndexError::AllHeadsForGcUnsupported)
    }

    async fn heads(
        &self,
        candidates: &mut (dyn Iterator<Item = &CommitId> + Send),
    ) -> IndexResult<Vec<CommitId>> {
        let mut unique = Vec::new();
        let mut seen = HashSet::new();
        for c in candidates {
            if seen.insert(c.clone()) {
                unique.push(c.clone());
            }
        }
        if unique.len() <= 1 {
            return Ok(unique);
        }
        unique.retain(|c| c.as_bytes() != cc_common::ROOT_COMMIT_ID_BYTES);
        if unique.len() <= 1 {
            return Ok(unique);
        }

        let mut cand_nodes = Vec::with_capacity(unique.len());
        for c in unique {
            self.budget
                .record_aggregate_visit()
                .map_err(|e| IndexError::Other(Box::new(e)))?;
            let (gen_num, _) = self.commit_node(&c).await?;
            cand_nodes.push(GenCommitNode(gen_num, c));
        }
        let min_gen = cand_nodes.iter().map(|n| n.0).min().unwrap_or(0);
        cand_nodes.sort_by(|a, b| b.cmp(a));

        let mut proper_ancestors = HashSet::new();
        let mut frontier: BinaryHeap<GenCommitNode> = BinaryHeap::new();
        let mut heads = Vec::new();
        let mut walk_visits = 0usize;

        for GenCommitNode(cand_gen, cand_id) in cand_nodes {
            while frontier
                .peek()
                .is_some_and(|top| top.0 > cand_gen || top.0 == u64::MAX)
            {
                let GenCommitNode(curr_gen, curr_id) = frontier.pop().unwrap();
                if curr_gen > min_gen || curr_gen == u64::MAX {
                    self.enqueue_parents(
                        &curr_id,
                        min_gen,
                        &mut proper_ancestors,
                        &mut frontier,
                        &mut walk_visits,
                    )
                    .await?;
                }
            }
            if proper_ancestors.contains(&cand_id) {
                continue;
            }
            if cand_gen > min_gen || cand_gen == u64::MAX {
                walk_visits = walk_visits.saturating_add(1);
                self.budget
                    .check_walk_count(walk_visits)
                    .map_err(|e| IndexError::Other(Box::new(e)))?;
                self.enqueue_parents(
                    &cand_id,
                    min_gen,
                    &mut proper_ancestors,
                    &mut frontier,
                    &mut walk_visits,
                )
                .await?;
            }
            heads.push(cand_id);
        }

        // Drain any remaining unknown-generation (`u64::MAX`) ancestors and drop candidates
        // discovered to be proper ancestors of another candidate processed later.
        while frontier.peek().is_some_and(|top| top.0 == u64::MAX) {
            let GenCommitNode(_, curr_id) = frontier.pop().unwrap();
            self.enqueue_parents(
                &curr_id,
                min_gen,
                &mut proper_ancestors,
                &mut frontier,
                &mut walk_visits,
            )
            .await?;
        }
        heads.retain(|h| !proper_ancestors.contains(h));
        heads.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        Ok(heads)
    }

    async fn changed_paths_in_commit(
        &self,
        _commit_id: &CommitId,
    ) -> IndexResult<Option<Box<dyn Iterator<Item = RepoPathBuf> + '_>>> {
        Ok(None)
    }

    fn evaluate_revset(
        &self,
        _expression: &ResolvedExpression,
        _store: &Arc<JjStore>,
    ) -> Result<Box<dyn Revset>, RevsetEvaluationError> {
        Ok(Box::new(EmptyRevset))
    }
}

pub struct ServerChangeIdIndex;

#[async_trait]
impl ChangeIdIndex for ServerChangeIdIndex {
    async fn resolve_prefix(
        &self,
        _prefix: &HexPrefix,
    ) -> IndexResult<PrefixResolution<ResolvedChangeTargets>> {
        Ok(PrefixResolution::NoMatch)
    }

    async fn shortest_unique_prefix_len(&self, _change_id: &ChangeId) -> IndexResult<usize> {
        Ok(12)
    }
}

impl ReadonlyIndex for ServerIndex {
    fn as_index(&self) -> &dyn Index {
        self
    }

    fn change_id_index(
        &self,
        _heads: &mut dyn Iterator<Item = &CommitId>,
    ) -> IndexResult<Box<dyn ChangeIdIndex>> {
        Ok(Box::new(ServerChangeIdIndex))
    }

    fn start_modification(&self) -> Box<dyn MutableIndex> {
        Box::new(self.clone())
    }
}

#[async_trait]
impl MutableIndex for ServerIndex {
    fn as_index(&self) -> &dyn Index {
        self
    }

    fn change_id_index(
        &self,
        _heads: &mut dyn Iterator<Item = &CommitId>,
    ) -> IndexResult<Box<dyn ChangeIdIndex + '_>> {
        Ok(Box::new(ServerChangeIdIndex))
    }

    async fn add_commit(&mut self, commit: &JjCommit) -> IndexResult<()> {
        let project_id = self.resolve_project_id().await?.to_string();
        let mut max_parent_gen = 0u64;
        for parent_id in commit.parent_ids() {
            let (p_gen, _) = self.commit_node(parent_id).await?;
            max_parent_gen = max_parent_gen.max(p_gen);
        }
        let generation = max_parent_gen.saturating_add(1);
        COMMIT_GRAPH_CACHE
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                (
                    store_cache_key(&self.store),
                    project_id,
                    commit.id().clone(),
                ),
                StoreScopedEntry {
                    metadata_ref: Arc::downgrade(self.store.metadata()),
                    value: (generation, commit.parent_ids().to_vec()),
                },
            );
        Ok(())
    }

    fn merge_in(&mut self, _other: &dyn ReadonlyIndex) -> IndexResult<()> {
        Ok(())
    }
}

pub struct ServerIndexStore {
    store: Arc<CompositeStore>,
    repo_id: String,
    budget: IndexWalkBudget,
}

impl Debug for ServerIndexStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerIndexStore")
            .field("repo_id", &self.repo_id)
            .field("limits", &self.budget.limits())
            .finish()
    }
}

impl ServerIndexStore {
    pub fn new(store: Arc<CompositeStore>, repo_id: String) -> Self {
        Self::with_budget(store, repo_id, IndexWalkBudget::default())
    }

    pub fn with_budget(
        store: Arc<CompositeStore>,
        repo_id: String,
        budget: IndexWalkBudget,
    ) -> Self {
        Self {
            store,
            repo_id,
            budget,
        }
    }
}

#[async_trait(?Send)]
impl IndexStore for ServerIndexStore {
    fn name(&self) -> &str {
        "server"
    }

    async fn get_index_at_op(
        &self,
        _op: &JjOperation,
        _store: &Arc<JjStore>,
    ) -> IndexStoreResult<Box<dyn ReadonlyIndex>> {
        Ok(Box::new(ServerIndex::with_budget(
            self.store.clone(),
            self.repo_id.clone(),
            self.budget.clone(),
        )))
    }

    fn write_index(
        &self,
        index: Box<dyn MutableIndex>,
        _op: &JjOperation,
    ) -> IndexStoreResult<Box<dyn ReadonlyIndex>> {
        let _ = index;
        Ok(Box::new(ServerIndex::with_budget(
            self.store.clone(),
            self.repo_id.clone(),
            self.budget.clone(),
        )))
    }
}
