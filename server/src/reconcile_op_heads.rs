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

//! Server-side operation-head reconciliation using `jj_lib`'s 3-way view merge algebra.
//!
//! # Concurrency and Merge Invariants
//! Before running `RepoLoader::load_at_head()`, `reconcile_repo_op_heads` inspects non-common
//! operations across divergent heads via an early-stopping LCA walk and delegates back to the
//! client (`ReconcileOutcome::ClientResolutionRequired`) whenever a divergent branch contains
//! commit rewrites, abandonments, or missing `commit_predecessors` metadata. The validated head
//! snapshot is pinned into `ServerOpHeadsStore::with_expected_heads` so concurrent arrivals
//! force a CAS retry through safety checks and ancestor-only head pruning succeeds atomically.

#![allow(elided_lifetimes_in_paths)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use jj_lib::object_id::ObjectId as _;
use jj_lib::op_heads_store::OpHeadsStore;
use jj_lib::op_store::OpStore;
use jj_lib::repo::RepoLoader;
use jj_lib::settings::UserSettings;
use jj_lib::signing::Signer;
use jj_lib::store::Store as JjStore;
use jj_lib::tree_merge::MergeOptions;

use crate::jj_lib_adapters::{
    ServerBackend, ServerIndexStore, ServerOpHeadsStore, ServerOpStore, ServerSubmoduleStore,
};
use crate::store::Store;

/// Result of attempting server-side operation-head reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileOutcome {
    /// All heads were reconciled (or only a single head existed) into this operation ID.
    Reconciled(Vec<u8>),
    /// Divergent branches include commit rewrites, abandonments, or missing predecessor
    /// metadata that require client-side `DefaultIndexStore` descendant rebasing.
    ClientResolutionRequired(Vec<Vec<u8>>),
}

const MAX_OP_WALK_VISITED: usize = 10_000;
const MAX_COMMIT_WALK_VISITED: usize = 4_096;

async fn load_view_heads(
    store: &dyn Store,
    repo_id: &str,
    view_id: &[u8],
) -> Result<Option<HashSet<Vec<u8>>>, tonic::Status> {
    if view_id == cc_common::ROOT_VIEW_ID_BYTES {
        return Ok(Some(HashSet::from([
            cc_common::ROOT_COMMIT_ID_BYTES.to_vec()
        ])));
    }
    let Some(view) = store.get_view(repo_id, view_id).await? else {
        return Ok(None);
    };
    Ok(Some(view.head_ids.into_iter().collect()))
}

/// Walks commit ancestors via bounded BFS from `seeds`, removing discovered ancestors from
/// `missing_heads` while sharing `visited_commits` and `commit_cache` across phases and ops.
/// Returns `Ok(true)` if the walk exceeds `MAX_COMMIT_WALK_VISITED` before `missing_heads`
/// becomes empty.
async fn walk_ancestors_until_found(
    store: &dyn Store,
    project_id: &str,
    seeds: Vec<Vec<u8>>,
    missing_heads: &mut HashSet<Vec<u8>>,
    visited_commits: &mut HashSet<Vec<u8>>,
    commit_cache: &mut HashMap<Vec<u8>, Option<cc_common::backend::Commit>>,
) -> Result<bool, tonic::Status> {
    let mut commit_queue: VecDeque<Vec<u8>> = VecDeque::new();
    for h in seeds {
        if h.as_slice() == cc_common::ROOT_COMMIT_ID_BYTES {
            continue;
        }
        if visited_commits.insert(h.clone()) {
            if visited_commits.len() > MAX_COMMIT_WALK_VISITED {
                return Ok(true);
            }
            commit_queue.push_back(h);
        }
    }

    while let Some(cid) = commit_queue.pop_front() {
        if visited_commits.len() > MAX_COMMIT_WALK_VISITED {
            return Ok(true);
        }
        let parent_ids = if let Some(cached) = commit_cache.get(&cid) {
            cached.as_ref().map(|c| c.parent_commit_ids.clone())
        } else {
            let fetched = store.get_commit(project_id, &cid).await?;
            let parent_ids = fetched.as_ref().map(|c| c.parent_commit_ids.clone());
            commit_cache.insert(cid, fetched);
            parent_ids
        };
        let Some(parent_ids) = parent_ids else {
            continue;
        };
        for pid in &parent_ids {
            if pid.as_slice() != cc_common::ROOT_COMMIT_ID_BYTES {
                missing_heads.remove(pid);
            }
        }
        if missing_heads.is_empty() {
            return Ok(false);
        }
        for pid in parent_ids {
            if pid.as_slice() == cc_common::ROOT_COMMIT_ID_BYTES {
                continue;
            }
            if visited_commits.insert(pid.clone()) {
                if visited_commits.len() > MAX_COMMIT_WALK_VISITED {
                    return Ok(true);
                }
                commit_queue.push_back(pid);
            }
        }
    }
    Ok(false)
}

/// Checks whether any non-common operation across divergent `heads` rewrites or abandons
/// commits (or lacks `commit_predecessors` metadata), which requires client-side
/// `DefaultIndexStore` descendant rebasing rather than server-side view merging.
async fn requires_client_resolution(
    store: &dyn Store,
    repo_id: &str,
    project_id: &str,
    heads: &[Vec<u8>],
) -> Result<bool, tonic::Status> {
    let mut unique_heads: Vec<Vec<u8>> = Vec::with_capacity(heads.len());
    let mut seen_heads: HashSet<&[u8]> = HashSet::with_capacity(heads.len());
    for h in heads {
        if seen_heads.insert(h.as_slice()) {
            unique_heads.push(h.clone());
        }
    }
    let num_heads = unique_heads.len();
    if num_heads <= 1 {
        return Ok(false);
    }

    let mut op_cache: HashMap<Vec<u8>, cc_common::op_store::Operation> = HashMap::new();
    let mut reach: HashMap<Vec<u8>, Vec<bool>> = HashMap::new();
    let mut common: HashSet<Vec<u8>> = HashSet::new();
    let mut common_expanded: HashSet<Vec<u8>> = HashSet::new();
    let mut non_common_in_queue: HashSet<Vec<u8>> = HashSet::new();
    let mut common_in_queue: HashSet<Vec<u8>> = HashSet::new();
    let mut queue: VecDeque<Vec<u8>> = VecDeque::new();

    for (idx, head) in unique_heads.iter().enumerate() {
        let mut flags = vec![false; num_heads];
        flags[idx] = true;
        reach.insert(head.clone(), flags);
        non_common_in_queue.insert(head.clone());
        queue.push_back(head.clone());
    }

    let mark_common_and_propagate =
        |start: Vec<u8>,
         common: &mut HashSet<Vec<u8>>,
         common_expanded: &HashSet<Vec<u8>>,
         reach: &mut HashMap<Vec<u8>, Vec<bool>>,
         non_common_in_queue: &mut HashSet<Vec<u8>>,
         common_in_queue: &mut HashSet<Vec<u8>>,
         queue: &mut VecDeque<Vec<u8>>,
         op_cache: &HashMap<Vec<u8>, cc_common::op_store::Operation>|
         -> Vec<Vec<u8>> {
            let mut newly_common = Vec::new();
            let mut stack = vec![start];
            while let Some(node) = stack.pop() {
                if !common.insert(node.clone()) {
                    continue;
                }
                newly_common.push(node.clone());
                non_common_in_queue.remove(&node);
                if let Some(flags) = reach.get_mut(&node) {
                    flags.fill(true);
                } else {
                    reach.insert(node.clone(), vec![true; num_heads]);
                }
                if !common_expanded.contains(&node) && common_in_queue.insert(node.clone()) {
                    queue.push_back(node.clone());
                }
                if let Some(op) = op_cache.get(&node) {
                    for p in &op.parents {
                        if !common.contains(p) && reach.contains_key(p) {
                            stack.push(p.clone());
                        }
                    }
                }
            }
            newly_common
        };

    // Expand both non-common and common frontier nodes in BFS lockstep while any non-common
    // node remains queued. Expanding common nodes only while `non_common_in_queue` is non-empty
    // keeps ordinary divergent walks bounded; if a shorter bypass branch exits before a lagging
    // common frontier reaches a suspect op, we verify reachability from `common_in_queue` below.
    while !non_common_in_queue.is_empty() {
        let Some(op_id) = queue.pop_front() else {
            break;
        };
        if reach.len() > MAX_OP_WALK_VISITED {
            return Ok(true);
        }
        if common.contains(&op_id) {
            common_in_queue.remove(&op_id);
            if !common_expanded.insert(op_id.clone()) {
                continue;
            }
            if op_id.as_slice() == cc_common::ROOT_OPERATION_ID_BYTES {
                continue;
            }
            if !op_cache.contains_key(&op_id) {
                let Some(op) = store.get_operation(repo_id, &op_id).await? else {
                    // Never fail open on a missing historical common op below the LCA.
                    continue;
                };
                op_cache.insert(op_id.clone(), op);
            }
            let parents = op_cache.get(&op_id).expect("just inserted").parents.clone();
            for parent_id in parents {
                if !common.contains(&parent_id) {
                    let _ = mark_common_and_propagate(
                        parent_id,
                        &mut common,
                        &common_expanded,
                        &mut reach,
                        &mut non_common_in_queue,
                        &mut common_in_queue,
                        &mut queue,
                        &op_cache,
                    );
                }
            }
            continue;
        }

        if !non_common_in_queue.remove(&op_id) {
            continue;
        }
        if op_id.as_slice() == cc_common::ROOT_OPERATION_ID_BYTES {
            continue;
        }
        if !op_cache.contains_key(&op_id) {
            let Some(op) = store.get_operation(repo_id, &op_id).await? else {
                // Let RepoLoader report missing/corrupt operation errors consistently.
                return Ok(false);
            };
            op_cache.insert(op_id.clone(), op);
        }
        let my_flags = reach.get(&op_id).expect("queued node has reach").clone();
        let parents = op_cache.get(&op_id).expect("just inserted").parents.clone();
        for parent_id in parents {
            if common.contains(&parent_id) {
                continue;
            }
            let flags = reach
                .entry(parent_id.clone())
                .or_insert_with(|| vec![false; num_heads]);
            let mut changed = false;
            for (dst, &src) in flags.iter_mut().zip(my_flags.iter()) {
                if src && !*dst {
                    *dst = true;
                    changed = true;
                }
            }
            if flags.iter().all(|b| *b) {
                let _ = mark_common_and_propagate(
                    parent_id,
                    &mut common,
                    &common_expanded,
                    &mut reach,
                    &mut non_common_in_queue,
                    &mut common_in_queue,
                    &mut queue,
                    &op_cache,
                );
            } else if changed && non_common_in_queue.insert(parent_id.clone()) {
                queue.push_back(parent_id);
            }
        }
    }

    // Pure ancestor pruning: at most one deduplicated head is maximal (reached only by itself),
    // even across 3+ head linear ancestor chains `H0 -> H1 -> H2`. No 3-way view merge or
    // descendant rebasing is required.
    let maximal_heads = unique_heads
        .iter()
        .filter(|h| {
            reach
                .get(*h)
                .is_none_or(|f| f.iter().filter(|&&b| b).count() == 1)
        })
        .count();
    if maximal_heads <= 1 {
        return Ok(false);
    }

    let non_common: Vec<Vec<u8>> = reach
        .keys()
        .filter(|id| !common.contains(*id) && id.as_slice() != cc_common::ROOT_OPERATION_ID_BYTES)
        .cloned()
        .collect();
    let mut commit_cache: HashMap<Vec<u8>, Option<cc_common::backend::Commit>> = HashMap::new();

    for op_id in non_common {
        if common.contains(&op_id) {
            continue;
        }
        let Some(op) = op_cache.get(&op_id).cloned() else {
            continue;
        };

        let has_rewrites = op
            .commit_predecessors
            .iter()
            .any(|entry| !entry.predecessor_ids.is_empty());
        let mut suspect_requires_client = !op.commit_predecessors_set || has_rewrites;

        if !suspect_requires_client {
            if let Some(op_heads) = load_view_heads(store, repo_id, &op.view_id).await? {
                for parent_op_id in &op.parents {
                    let parent_view_id = if parent_op_id.as_slice()
                        == cc_common::ROOT_OPERATION_ID_BYTES
                    {
                        cc_common::ROOT_VIEW_ID_BYTES.to_vec()
                    } else if let Some(parent_op) = op_cache.get(parent_op_id) {
                        parent_op.view_id.clone()
                    } else {
                        let Some(parent_op) = store.get_operation(repo_id, parent_op_id).await?
                        else {
                            return Ok(false);
                        };
                        let vid = parent_op.view_id.clone();
                        op_cache.insert(parent_op_id.clone(), parent_op);
                        vid
                    };
                    let Some(parent_heads) =
                        load_view_heads(store, repo_id, &parent_view_id).await?
                    else {
                        suspect_requires_client = true;
                        break;
                    };
                    let mut missing_heads: HashSet<Vec<u8>> = parent_heads
                        .iter()
                        .filter(|h| {
                            h.as_slice() != cc_common::ROOT_COMMIT_ID_BYTES
                                && !op_heads.contains(*h)
                        })
                        .cloned()
                        .collect();
                    if missing_heads.is_empty() {
                        continue;
                    }

                    // A parent view head missing from the child view is normal commit advancement
                    // if it is an ancestor of at least one head in the child view; otherwise it
                    // was abandoned/hidden and needs client-side descendant rebasing.
                    // Prioritize newly added heads (`op_heads \ parent_heads`) so unrelated
                    // unchanged branches do not exhaust `MAX_COMMIT_WALK_VISITED`, falling back
                    // to unchanged heads only if `missing_heads` remains non-empty (e.g. an
                    // unnormalized synthetic view).
                    let mut visited_commits: HashSet<Vec<u8>> = HashSet::new();
                    let mut newly_added_heads: Vec<Vec<u8>> = op_heads
                        .difference(&parent_heads)
                        .filter(|h| h.as_slice() != cc_common::ROOT_COMMIT_ID_BYTES)
                        .cloned()
                        .collect();
                    newly_added_heads.sort();

                    let mut walk_budget_exceeded = walk_ancestors_until_found(
                        store,
                        project_id,
                        newly_added_heads,
                        &mut missing_heads,
                        &mut visited_commits,
                        &mut commit_cache,
                    )
                    .await?;

                    if !walk_budget_exceeded && !missing_heads.is_empty() {
                        let mut unchanged_heads: Vec<Vec<u8>> = op_heads
                            .intersection(&parent_heads)
                            .filter(|h| h.as_slice() != cc_common::ROOT_COMMIT_ID_BYTES)
                            .cloned()
                            .collect();
                        unchanged_heads.sort();
                        walk_budget_exceeded = walk_ancestors_until_found(
                            store,
                            project_id,
                            unchanged_heads,
                            &mut missing_heads,
                            &mut visited_commits,
                            &mut commit_cache,
                        )
                        .await?;
                    }

                    if walk_budget_exceeded || !missing_heads.is_empty() {
                        suspect_requires_client = true;
                        break;
                    }
                }
            } else {
                // Fail closed if a non-common op's view is missing instead of skipping checks.
                suspect_requires_client = true;
            }
        }

        if !suspect_requires_client {
            continue;
        }

        // Before delegating to the client, verify that `op_id` is not actually a deeper common
        // ancestor sitting behind a lagging `common_in_queue` wave in an asymmetric merge diamond.
        // Any op reachable from `op_id` in `op_cache` cannot reach `op_id` in an acyclic DAG, so
        // ordinary non-bypass rewrites (whose common LCA is an ancestor of `op_id`) do zero reads.
        if !common_in_queue.is_empty() {
            let mut strict_ancestors: HashSet<Vec<u8>> = HashSet::new();
            let mut dfs_stack: Vec<Vec<u8>> = op.parents.clone();
            while let Some(anc) = dfs_stack.pop() {
                if anc == op_id {
                    continue;
                }
                if strict_ancestors.insert(anc.clone())
                    && let Some(anc_op) = op_cache.get(&anc)
                {
                    for p in &anc_op.parents {
                        if !strict_ancestors.contains(p) {
                            dfs_stack.push(p.clone());
                        }
                    }
                }
            }

            let mut verify_queue: VecDeque<Vec<u8>> = common_in_queue
                .iter()
                .filter(|c| {
                    c.as_slice() != cc_common::ROOT_OPERATION_ID_BYTES
                        && !common_expanded.contains(*c)
                        && !strict_ancestors.contains(*c)
                })
                .cloned()
                .collect();

            while let Some(cid) = verify_queue.pop_front() {
                if common.contains(&op_id) {
                    break;
                }
                if reach.len() > MAX_OP_WALK_VISITED {
                    return Ok(true);
                }
                common_in_queue.remove(&cid);
                if !common_expanded.insert(cid.clone()) {
                    continue;
                }
                if cid.as_slice() == cc_common::ROOT_OPERATION_ID_BYTES {
                    continue;
                }
                if !op_cache.contains_key(&cid) {
                    let Some(loaded) = store.get_operation(repo_id, &cid).await? else {
                        continue;
                    };
                    op_cache.insert(cid.clone(), loaded);
                }
                let parents = op_cache.get(&cid).expect("just inserted").parents.clone();
                for parent_id in parents {
                    if !common.contains(&parent_id) {
                        let newly_common = mark_common_and_propagate(
                            parent_id,
                            &mut common,
                            &common_expanded,
                            &mut reach,
                            &mut non_common_in_queue,
                            &mut common_in_queue,
                            &mut queue,
                            &op_cache,
                        );
                        if !common.contains(&op_id) {
                            for nc in newly_common {
                                if nc.as_slice() != cc_common::ROOT_OPERATION_ID_BYTES
                                    && !common_expanded.contains(&nc)
                                    && !strict_ancestors.contains(&nc)
                                {
                                    verify_queue.push_back(nc);
                                }
                            }
                        }
                    }
                }
                if common.contains(&op_id) {
                    break;
                }
            }

            if common.contains(&op_id) {
                continue;
            }
        }

        return Ok(true);
    }

    Ok(false)
}

static RECONCILER_ENV: std::sync::LazyLock<(UserSettings, MergeOptions)> =
    std::sync::LazyLock::new(|| {
        let user_settings =
            UserSettings::from_config(jj_lib::config::StackedConfig::with_defaults())
                .expect("failed to load default user settings");
        let merge_options = MergeOptions::from_settings(&user_settings)
            .expect("failed to create merge options from settings");
        (user_settings, merge_options)
    });

pub async fn reconcile_repo_op_heads(
    store: Arc<dyn Store>,
    repo_id: &str,
) -> Result<ReconcileOutcome, tonic::Status> {
    const MAX_RETRIES: usize = 3;
    const BASE_BACKOFF_MS: u64 = 25;
    const MAX_JITTER_MS: u64 = 15;

    let project_id = store.get_repo_project_id(repo_id).await?.ok_or_else(|| {
        tonic::Status::not_found(format!(
            "repository should have been registered before reconciling op heads: {repo_id}"
        ))
    })?;

    for attempt in 0..MAX_RETRIES {
        let heads = store
            .get_op_heads(repo_id)
            .await?
            .unwrap_or_else(|| vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()]);

        if heads.len() <= 1 {
            let head = heads
                .into_iter()
                .next()
                .unwrap_or_else(|| cc_common::ROOT_OPERATION_ID_BYTES.to_vec());
            return Ok(ReconcileOutcome::Reconciled(head));
        }

        if requires_client_resolution(store.as_ref(), repo_id, &project_id, &heads).await? {
            return Ok(ReconcileOutcome::ClientResolutionRequired(heads));
        }

        let store_clone = store.clone();
        let project_id_owned = project_id.clone();
        let repo_id_owned = repo_id.to_string();
        let pinned_heads = heads.clone();

        // Use `spawn_blocking` and `pollster::block_on` here. This is unavoidable because jj-lib's `load_at_head()`
        // is not `Send` since `dyn Repo` is not `Sync`. Since Tonic requires async RPC handlers to be `Send`,
        // we must run this on a separate thread using `spawn_blocking`.
        let handle = tokio::task::spawn_blocking(move || {
            pollster::block_on(async move {
                let (ref user_settings, ref merge_options) = *RECONCILER_ENV;
                let signer = Signer::from_settings(user_settings)
                    .map_err(|e| format!("failed to initialize signer: {e}"))?;
                let index_store = Arc::new(ServerIndexStore);
                let submodule_store = Arc::new(ServerSubmoduleStore);

                let server_backend =
                    Box::new(ServerBackend::new(store_clone.clone(), project_id_owned));
                let jj_store = JjStore::new(server_backend, signer, merge_options.clone());
                let op_store: Arc<dyn OpStore> = Arc::new(ServerOpStore::new(
                    store_clone.clone(),
                    repo_id_owned.clone(),
                ));
                let op_heads_store: Arc<dyn OpHeadsStore> =
                    Arc::new(ServerOpHeadsStore::with_expected_heads(
                        store_clone,
                        repo_id_owned,
                        pinned_heads,
                    ));

                // TODO: optimize shared jj-lib adapter trait construction to not create new ones for every retry
                let repo_loader = RepoLoader::new(
                    user_settings.clone(),
                    jj_store,
                    op_store,
                    op_heads_store,
                    index_store,
                    submodule_store,
                );

                // This is where the core reconciliation happens. Calling `load_at_head()` invokes
                // `jj_lib::op_heads_store::resolve_op_heads()`, which identifies divergent operation heads,
                // performs a 3-way view merge, creates a new merge operation, and eventually invokes
                // the `Store` trait (`crate::store::Store::update_op_heads` via `ServerOpHeadsStore`)
                // with strict exact CAS verification.
                repo_loader
                    .load_at_head()
                    .await
                    .map(|repo| repo.op_id().as_bytes().to_vec())
                    .map_err(|e| e.to_string())
            })
        });

        let reconcile_result = handle
            .await
            .map_err(|e| tonic::Status::internal(format!("Reconciliation task failed: {e}")))?;

        // If reconciliation fails (e.g. CAS conflict because another concurrent client or reconciliation
        // updated op_heads in the meantime), retry up to MAX_RETRIES times using exponential backoff
        // (BASE_BACKOFF_MS << attempt, i.e. 2^attempt * 25ms) plus randomized bounded jitter in [0, MAX_JITTER_MS)
        // (upper-bounded strictly below 15ms) to desynchronize concurrent reconciliation attempts on the same repo.
        // On each retry, the loop re-fetches the latest op heads and recalculates the resolved merge operation
        // using jj-lib.
        match reconcile_result {
            Ok(op_id) => {
                return Ok(ReconcileOutcome::Reconciled(op_id));
            }
            Err(_e) if attempt + 1 < MAX_RETRIES => {
                let jitter = rand::random::<u64>() % MAX_JITTER_MS;
                let sleep_duration =
                    std::time::Duration::from_millis((BASE_BACKOFF_MS << attempt) + jitter);
                tokio::time::sleep(sleep_duration).await;
            }
            Err(e) => {
                return Err(tonic::Status::internal(format!(
                    "Failed to reconcile op heads for repo {repo_id}: {e}"
                )));
            }
        }
    }

    unreachable!("reconciliation retry loop always returns on the final attempt")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memorystore::MemoryStore;
    use cc_common::conversions::op_store::{operation_from_proto, view_from_proto};
    use cc_common::op_store::{
        Operation as ProtoOperation, OperationMetadata as ProtoOperationMetadata, View as ProtoView,
    };
    use jj_lib::op_store::OperationId;
    use std::collections::HashSet;

    fn make_test_proto_op(view_id: Vec<u8>, parents: Vec<Vec<u8>>, desc: &str) -> ProtoOperation {
        ProtoOperation {
            view_id,
            parents,
            metadata: Some(ProtoOperationMetadata {
                start_time_millis: 1000,
                end_time_millis: 1000,
                description: desc.to_string(),
                is_snapshot: false,
                workspace_name: None,
                hostname: "test-host".to_string(),
                username: "test-user".to_string(),
                attributes: std::collections::HashMap::new(),
            }),
            commit_predecessors: vec![],
            commit_predecessors_set: true,
        }
    }

    fn new_commit_predecessors(commit_id: Vec<u8>) -> Vec<cc_common::op_store::CommitPredecessors> {
        vec![cc_common::op_store::CommitPredecessors {
            commit_id,
            predecessor_ids: vec![],
        }]
    }

    #[tokio::test]
    async fn test_reconcile_repo_op_heads_single_head_is_noop() {
        let store = Arc::new(MemoryStore::new());
        let repo_id = "test-single-head-repo";
        store
            .register_repo(repo_id.to_string(), "default".to_string(), None)
            .await
            .unwrap();

        let root_op_bytes = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
        let result = reconcile_repo_op_heads(store.clone(), repo_id)
            .await
            .unwrap();
        assert_eq!(result, ReconcileOutcome::Reconciled(root_op_bytes));
    }

    #[tokio::test]
    async fn test_reconcile_op_heads_matches_jj_lib_resolution() {
        use std::collections::HashMap;

        let store = Arc::new(MemoryStore::new());
        let repo_id_jj = "repo-jj-direct";
        let repo_id_server = "repo-server-reconcile";
        store
            .register_repo(repo_id_jj.to_string(), "default".to_string(), None)
            .await
            .unwrap();
        store
            .register_repo(repo_id_server.to_string(), "default".to_string(), None)
            .await
            .unwrap();

        let root_op_bytes = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();

        // Branch A introduces "branch-a" bookmark
        let mut bookmarks_a = HashMap::new();
        bookmarks_a.insert(
            "branch-a".to_string(),
            cc_common::op_store::RefTarget {
                removes: vec![],
                adds: vec![cc_common::op_store::RefTargetTerm {
                    commit_id: vec![0x01; 20],
                }],
            },
        );
        let view_a_proto = ProtoView {
            head_ids: vec![vec![0x01; 20]],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: bookmarks_a,
            remote_bookmarks: HashMap::new(),
        };
        let view_a_bytes = crate::hash_utils::hash_view(&view_a_proto).unwrap();
        store
            .put_view(
                repo_id_jj.to_string(),
                view_a_bytes.clone(),
                view_a_proto.clone(),
            )
            .await
            .unwrap();
        store
            .put_view(
                repo_id_server.to_string(),
                view_a_bytes.clone(),
                view_a_proto,
            )
            .await
            .unwrap();

        // Branch B introduces "branch-b" bookmark
        let mut bookmarks_b = HashMap::new();
        bookmarks_b.insert(
            "branch-b".to_string(),
            cc_common::op_store::RefTarget {
                removes: vec![],
                adds: vec![cc_common::op_store::RefTargetTerm {
                    commit_id: vec![0x02; 20],
                }],
            },
        );
        let view_b_proto = ProtoView {
            head_ids: vec![vec![0x02; 20]],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: bookmarks_b,
            remote_bookmarks: HashMap::new(),
        };
        let view_b_bytes = crate::hash_utils::hash_view(&view_b_proto).unwrap();
        store
            .put_view(
                repo_id_jj.to_string(),
                view_b_bytes.clone(),
                view_b_proto.clone(),
            )
            .await
            .unwrap();
        store
            .put_view(
                repo_id_server.to_string(),
                view_b_bytes.clone(),
                view_b_proto,
            )
            .await
            .unwrap();

        // Operation A
        let op_a_bytes = vec![0x0A; cc_common::OPERATION_ID_LENGTH];
        let mut op_a_proto = make_test_proto_op(
            view_a_bytes.clone(),
            vec![root_op_bytes.clone()],
            "branch a",
        );
        op_a_proto.commit_predecessors = new_commit_predecessors(vec![0x01; 20]);
        store
            .put_operation(
                repo_id_jj.to_string(),
                op_a_bytes.clone(),
                op_a_proto.clone(),
            )
            .await
            .unwrap();
        store
            .put_operation(repo_id_server.to_string(), op_a_bytes.clone(), op_a_proto)
            .await
            .unwrap();

        // Operation B
        let op_b_bytes = vec![0x0B; cc_common::OPERATION_ID_LENGTH];
        let mut op_b_proto = make_test_proto_op(
            view_b_bytes.clone(),
            vec![root_op_bytes.clone()],
            "branch b",
        );
        op_b_proto.commit_predecessors = new_commit_predecessors(vec![0x02; 20]);
        store
            .put_operation(
                repo_id_jj.to_string(),
                op_b_bytes.clone(),
                op_b_proto.clone(),
            )
            .await
            .unwrap();
        store
            .put_operation(repo_id_server.to_string(), op_b_bytes.clone(), op_b_proto)
            .await
            .unwrap();

        // Set divergent op heads [op_a, op_b] in both repositories
        store
            .update_op_heads_append_new_remove_old(
                repo_id_jj.to_string(),
                &[root_op_bytes.clone()],
                op_a_bytes.clone(),
            )
            .await
            .unwrap();
        store
            .update_op_heads_append_new_remove_old(repo_id_jj.to_string(), &[], op_b_bytes.clone())
            .await
            .unwrap();

        store
            .update_op_heads_append_new_remove_old(
                repo_id_server.to_string(),
                &[root_op_bytes.clone()],
                op_a_bytes.clone(),
            )
            .await
            .unwrap();
        store
            .update_op_heads_append_new_remove_old(
                repo_id_server.to_string(),
                &[],
                op_b_bytes.clone(),
            )
            .await
            .unwrap();

        // Direct jj-lib resolution on repo_id_jj
        let index_store = Arc::new(ServerIndexStore);
        let submodule_store = Arc::new(ServerSubmoduleStore);
        let user_settings =
            UserSettings::from_config(jj_lib::config::StackedConfig::with_defaults()).unwrap();
        let signer = Signer::from_settings(&user_settings).unwrap();
        let merge_options = MergeOptions::from_settings(&user_settings).unwrap();

        let jj_backend = Box::new(ServerBackend::new(store.clone(), "default".to_string()));
        let jj_store = JjStore::new(jj_backend, signer, merge_options);
        let jj_op_store: Arc<dyn OpStore> =
            Arc::new(ServerOpStore::new(store.clone(), repo_id_jj.to_string()));
        let jj_op_heads_store: Arc<dyn OpHeadsStore> = Arc::new(ServerOpHeadsStore::new(
            store.clone(),
            repo_id_jj.to_string(),
        ));

        let repo_loader = RepoLoader::new(
            user_settings,
            jj_store,
            jj_op_store,
            jj_op_heads_store,
            index_store,
            submodule_store,
        );

        let jj_lib_repo = repo_loader
            .load_at_head()
            .await
            .expect("jj-lib direct resolution failed");
        let jj_lib_op = jj_lib_repo.operation().clone();
        let jj_lib_view = jj_lib_repo.view().store_view().clone();

        // Server reconciliation logic on repo_id_server
        let server_merged_op_id = match reconcile_repo_op_heads(store.clone(), repo_id_server)
            .await
            .expect("Server reconciliation failed")
        {
            ReconcileOutcome::Reconciled(id) => id,
            ReconcileOutcome::ClientResolutionRequired(heads) => {
                panic!("expected server reconciliation, got client fallback: {heads:?}");
            }
        };

        let server_op_proto = store
            .get_operation(repo_id_server, &server_merged_op_id)
            .await
            .unwrap()
            .expect("Server merged operation not found in store");
        let server_op = operation_from_proto(server_op_proto);

        let server_view_proto = store
            .get_view(repo_id_server, server_op.view_id.as_bytes())
            .await
            .unwrap()
            .expect("Server merged view not found in store");
        let server_view = view_from_proto(server_view_proto);

        // Verify exact parity between both resolution methods
        // Parent operation sets must match exactly between jj-lib and server resolution
        let server_parent_set: HashSet<_> = server_op.parents.iter().cloned().collect();
        let jj_lib_parent_set: HashSet<_> = jj_lib_op.parent_ids().iter().cloned().collect();
        assert_eq!(server_parent_set, jj_lib_parent_set);
        assert_eq!(
            server_parent_set,
            HashSet::from([
                OperationId::from_bytes(&op_a_bytes),
                OperationId::from_bytes(&op_b_bytes),
            ])
        );

        // Merged View IDs must match exactly
        assert_eq!(server_op.view_id, *jj_lib_op.view_id());

        // Merged View contents must match exactly
        assert_eq!(server_view.head_ids, jj_lib_view.head_ids);
        assert_eq!(server_view.local_bookmarks, jj_lib_view.local_bookmarks);
        assert_eq!(server_view.wc_commit_ids, jj_lib_view.wc_commit_ids);

        // Verify that both divergent bookmarks were successfully resolved in the merged view
        assert!(
            server_view
                .local_bookmarks
                .contains_key(&jj_lib::ref_name::RefNameBuf::from("branch-a"))
        );
        assert!(
            server_view
                .local_bookmarks
                .contains_key(&jj_lib::ref_name::RefNameBuf::from("branch-b"))
        );

        // Operation metadata description matches standard jj-lib reconciliation message
        assert_eq!(
            server_op.metadata.description,
            jj_lib_op.metadata().description
        );
        assert_eq!(
            server_op.metadata.description,
            "reconcile divergent operations"
        );

        // The server repo op heads store now holds strictly the single merged operation head
        let final_heads = store.get_op_heads(repo_id_server).await.unwrap().unwrap();
        assert_eq!(final_heads, vec![server_merged_op_id]);
    }

    /// This test verifies that when divergent operation heads cannot be resolved due to
    /// missing or corrupted underlying store objects (in this case, an operation head is registered
    /// in op_heads but its corresponding Operation record is absent from the op store),
    /// `reconcile_repo_op_heads` attempts all `MAX_RETRIES` (3 retries with backoff) and ultimately
    /// fails with a `tonic::Status::internal` error describing the missing object.
    #[tokio::test]
    async fn test_reconcile_repo_op_heads_fails_after_exhausting_retries_on_missing_operation() {
        let store = Arc::new(MemoryStore::new());
        let repo_id = "test-reconcile-failure-repo";
        store
            .register_repo(repo_id.to_string(), "default".to_string(), None)
            .await
            .unwrap();

        let root_op_bytes = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
        let root_view_bytes = cc_common::ROOT_VIEW_ID_BYTES.to_vec();

        // Write operation A into store
        let op_a_bytes = vec![0x0A; cc_common::OPERATION_ID_LENGTH];
        let op_a_proto = make_test_proto_op(
            root_view_bytes.clone(),
            vec![root_op_bytes.clone()],
            "branch a",
        );
        store
            .put_operation(repo_id.to_string(), op_a_bytes.clone(), op_a_proto)
            .await
            .unwrap();

        // Do NOT put operation B into the store (missing operation object)
        let op_b_bytes = vec![0x0B; cc_common::OPERATION_ID_LENGTH];

        // Register divergent op heads: [op_a, op_b]
        store
            .update_op_heads_append_new_remove_old(
                repo_id.to_string(),
                &[root_op_bytes.clone()],
                op_a_bytes.clone(),
            )
            .await
            .unwrap();
        store
            .update_op_heads_append_new_remove_old(repo_id.to_string(), &[], op_b_bytes.clone())
            .await
            .unwrap();

        let initial_heads = store.get_op_heads(repo_id).await.unwrap().unwrap();
        assert_eq!(initial_heads.len(), 2);

        // Call reconcile_repo_op_heads - should fail all 3 retries because op_b is missing
        let result = reconcile_repo_op_heads(store.clone(), repo_id).await;

        let status =
            result.expect_err("Reconciliation should fail when operation object is missing");
        assert_eq!(status.code(), tonic::Code::Internal);

        // Verify that op heads were NOT modified since reconciliation failed
        let post_heads = store.get_op_heads(repo_id).await.unwrap().unwrap();
        assert_eq!(post_heads, initial_heads);
    }

    #[tokio::test]
    async fn test_reconcile_returns_client_resolution_required_on_rewrite_or_abandon() {
        use std::collections::HashMap;

        let store = Arc::new(MemoryStore::new());
        let repo_id = "repo-rewrite-fallback";
        store
            .register_repo(repo_id.to_string(), "proj-rf".to_string(), None)
            .await
            .unwrap();

        let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
        let c0 = vec![0x10; 20];
        let c1 = vec![0x11; 20];
        let c2 = vec![0x12; 20];
        for (cid, parent) in [
            (&c0, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
            (&c1, c0.clone()),
            (&c2, c0.clone()),
        ] {
            store
                .put_commit(
                    "proj-rf".to_string(),
                    cid.clone(),
                    cc_common::backend::Commit {
                        commit_id: cid.clone(),
                        parent_commit_ids: vec![parent],
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
        }

        // Common base op with head c0.
        let base_view = ProtoView {
            head_ids: vec![c0.clone()],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: HashMap::new(),
        };
        let base_view_id = crate::hash_utils::hash_view(&base_view).unwrap();
        store
            .put_view(repo_id.to_string(), base_view_id.clone(), base_view)
            .await
            .unwrap();
        let mut base_op = make_test_proto_op(base_view_id, vec![root_op.clone()], "base");
        base_op.commit_predecessors = new_commit_predecessors(c0.clone());
        let base_op_id = crate::hash_utils::hash_operation(&base_op).unwrap();
        store
            .put_operation(repo_id.to_string(), base_op_id.clone(), base_op)
            .await
            .unwrap();

        // Branch A adds child c1 (normal commit creation).
        let view_a = ProtoView {
            head_ids: vec![c1.clone()],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: HashMap::new(),
        };
        let view_a_id = crate::hash_utils::hash_view(&view_a).unwrap();
        store
            .put_view(repo_id.to_string(), view_a_id.clone(), view_a)
            .await
            .unwrap();
        let mut op_a = make_test_proto_op(view_a_id, vec![base_op_id.clone()], "add c1");
        op_a.commit_predecessors = new_commit_predecessors(c1);
        let op_a_id = crate::hash_utils::hash_operation(&op_a).unwrap();
        store
            .put_operation(repo_id.to_string(), op_a_id.clone(), op_a)
            .await
            .unwrap();

        // Branch B rewrites c0 -> c2 (non-empty commit_predecessors).
        let view_b = ProtoView {
            head_ids: vec![c2.clone()],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: HashMap::new(),
        };
        let view_b_id = crate::hash_utils::hash_view(&view_b).unwrap();
        store
            .put_view(repo_id.to_string(), view_b_id.clone(), view_b)
            .await
            .unwrap();
        let mut op_b = make_test_proto_op(view_b_id, vec![base_op_id.clone()], "rewrite c0");
        op_b.commit_predecessors = vec![cc_common::op_store::CommitPredecessors {
            commit_id: c2.clone(),
            predecessor_ids: vec![c0.clone()],
        }];
        let op_b_id = crate::hash_utils::hash_operation(&op_b).unwrap();
        store
            .put_operation(repo_id.to_string(), op_b_id.clone(), op_b)
            .await
            .unwrap();

        store
            .update_op_heads_append_new_remove_old(
                repo_id.to_string(),
                &[root_op.clone()],
                op_a_id.clone(),
            )
            .await
            .unwrap();
        store
            .update_op_heads_append_new_remove_old(repo_id.to_string(), &[], op_b_id.clone())
            .await
            .unwrap();

        let outcome = reconcile_repo_op_heads(store.clone(), repo_id)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            ReconcileOutcome::ClientResolutionRequired(vec![op_a_id.clone(), op_b_id.clone()])
        );
        // Op heads must remain untouched when delegating back to the client.
        assert_eq!(
            store.get_op_heads(repo_id).await.unwrap().unwrap(),
            vec![op_a_id.clone(), op_b_id.clone()]
        );

        // Now test silent abandonment (c0 disappears from head_ids with empty commit_predecessors).
        let abandon_view = ProtoView {
            head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: HashMap::new(),
        };
        let abandon_view_id = crate::hash_utils::hash_view(&abandon_view).unwrap();
        store
            .put_view(repo_id.to_string(), abandon_view_id.clone(), abandon_view)
            .await
            .unwrap();
        let op_abandon = make_test_proto_op(abandon_view_id, vec![base_op_id], "abandon c0");
        let op_abandon_id = crate::hash_utils::hash_operation(&op_abandon).unwrap();
        store
            .put_operation(repo_id.to_string(), op_abandon_id.clone(), op_abandon)
            .await
            .unwrap();
        store
            .update_op_heads_append_new_remove_old(
                repo_id.to_string(),
                &[op_b_id],
                op_abandon_id.clone(),
            )
            .await
            .unwrap();

        let outcome = reconcile_repo_op_heads(store.clone(), repo_id)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            ReconcileOutcome::ClientResolutionRequired(vec![op_a_id, op_abandon_id])
        );
    }

    #[tokio::test]
    async fn test_reconcile_ancestor_only_head_pruning_and_pinned_cas() {
        use std::collections::HashMap;

        let store = Arc::new(MemoryStore::new());
        let repo_id = "repo-ancestor-prune";
        store
            .register_repo(repo_id.to_string(), "proj-ap".to_string(), None)
            .await
            .unwrap();

        let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
        let c0 = vec![0x20; 20];
        let c1 = vec![0x21; 20];
        store
            .put_commit(
                "proj-ap".to_string(),
                c0.clone(),
                cc_common::backend::Commit {
                    commit_id: c0.clone(),
                    parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        store
            .put_commit(
                "proj-ap".to_string(),
                c1.clone(),
                cc_common::backend::Commit {
                    commit_id: c1.clone(),
                    parent_commit_ids: vec![c0.clone()],
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let view_parent = ProtoView {
            head_ids: vec![c0.clone()],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: HashMap::new(),
        };
        let view_parent_id = crate::hash_utils::hash_view(&view_parent).unwrap();
        store
            .put_view(repo_id.to_string(), view_parent_id.clone(), view_parent)
            .await
            .unwrap();
        let mut op_parent = make_test_proto_op(view_parent_id, vec![root_op.clone()], "parent op");
        op_parent.commit_predecessors = new_commit_predecessors(c0);
        let op_parent_id = crate::hash_utils::hash_operation(&op_parent).unwrap();
        store
            .put_operation(repo_id.to_string(), op_parent_id.clone(), op_parent)
            .await
            .unwrap();

        let view_child = ProtoView {
            head_ids: vec![c1.clone()],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: HashMap::new(),
        };
        let view_child_id = crate::hash_utils::hash_view(&view_child).unwrap();
        store
            .put_view(repo_id.to_string(), view_child_id.clone(), view_child)
            .await
            .unwrap();
        let mut op_child =
            make_test_proto_op(view_child_id, vec![op_parent_id.clone()], "child op");
        op_child.commit_predecessors = new_commit_predecessors(c1);
        let op_child_id = crate::hash_utils::hash_operation(&op_child).unwrap();
        store
            .put_operation(repo_id.to_string(), op_child_id.clone(), op_child)
            .await
            .unwrap();

        // Both parent and descendant are active heads (e.g. partial client update).
        store
            .update_op_heads_append_new_remove_old(
                repo_id.to_string(),
                &[root_op],
                op_parent_id.clone(),
            )
            .await
            .unwrap();
        store
            .update_op_heads_append_new_remove_old(repo_id.to_string(), &[], op_child_id.clone())
            .await
            .unwrap();

        let outcome = reconcile_repo_op_heads(store.clone(), repo_id)
            .await
            .unwrap();
        assert_eq!(outcome, ReconcileOutcome::Reconciled(op_child_id.clone()));
        assert_eq!(
            store.get_op_heads(repo_id).await.unwrap().unwrap(),
            vec![op_child_id.clone()]
        );

        // Verify pinned ServerOpHeadsStore rejects stale CAS when live heads diverged concurrently.
        let stale_store = ServerOpHeadsStore::with_expected_heads(
            store.clone(),
            repo_id.to_string(),
            vec![op_parent_id],
        );
        let cas_err = stale_store
            .update_op_heads(
                &[OperationId::from_bytes(&op_child_id)],
                &OperationId::from_bytes(&op_child_id),
            )
            .await;
        assert!(
            cas_err.is_err(),
            "pinned ServerOpHeadsStore must fail CAS when live heads differ from pinned snapshot"
        );
    }

    #[tokio::test]
    async fn test_reconcile_merge_op_diamond_does_not_leak_past_common_ancestor() {
        use std::collections::HashMap;

        let store = Arc::new(MemoryStore::new());
        let repo_id = "repo-merge-diamond";
        store
            .register_repo(repo_id.to_string(), "proj-md".to_string(), None)
            .await
            .unwrap();

        let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
        let c_old = vec![0x30; 20];
        let c_rewritten = vec![0x31; 20];
        let c_l = vec![0x32; 20];
        let c_m = vec![0x33; 20];
        let c_h1 = vec![0x34; 20];
        for (cid, parent) in [
            (&c_old, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
            (&c_rewritten, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
            (&c_l, c_rewritten.clone()),
            (&c_m, c_rewritten.clone()),
            (&c_h1, c_l.clone()),
        ] {
            store
                .put_commit(
                    "proj-md".to_string(),
                    cid.clone(),
                    cc_common::backend::Commit {
                        commit_id: cid.clone(),
                        parent_commit_ids: vec![parent],
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
        }

        // Historical rewrite op X sits beneath both L and M. Because both H0 = merge(L, M)
        // and H1 = child(L) descend from L -> X, X is a common ancestor and must NOT trigger
        // a false ClientResolutionRequired fallback.
        let view_x = ProtoView {
            head_ids: vec![c_rewritten.clone()],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: HashMap::new(),
        };
        let view_x_id = crate::hash_utils::hash_view(&view_x).unwrap();
        store
            .put_view(repo_id.to_string(), view_x_id.clone(), view_x)
            .await
            .unwrap();
        let mut op_x = make_test_proto_op(view_x_id, vec![root_op.clone()], "historical rewrite");
        op_x.commit_predecessors = vec![cc_common::op_store::CommitPredecessors {
            commit_id: c_rewritten,
            predecessor_ids: vec![c_old],
        }];
        let op_x_id = crate::hash_utils::hash_operation(&op_x).unwrap();
        store
            .put_operation(repo_id.to_string(), op_x_id.clone(), op_x)
            .await
            .unwrap();

        let view_l = ProtoView {
            head_ids: vec![c_l.clone()],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: HashMap::new(),
        };
        let view_l_id = crate::hash_utils::hash_view(&view_l).unwrap();
        store
            .put_view(repo_id.to_string(), view_l_id.clone(), view_l)
            .await
            .unwrap();
        let mut op_l = make_test_proto_op(view_l_id, vec![op_x_id.clone()], "op L");
        op_l.commit_predecessors = new_commit_predecessors(c_l.clone());
        let op_l_id = crate::hash_utils::hash_operation(&op_l).unwrap();
        store
            .put_operation(repo_id.to_string(), op_l_id.clone(), op_l)
            .await
            .unwrap();

        let view_m = ProtoView {
            head_ids: vec![c_m.clone()],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: HashMap::new(),
        };
        let view_m_id = crate::hash_utils::hash_view(&view_m).unwrap();
        store
            .put_view(repo_id.to_string(), view_m_id.clone(), view_m)
            .await
            .unwrap();
        let mut op_m = make_test_proto_op(view_m_id, vec![op_x_id], "op M");
        op_m.commit_predecessors = new_commit_predecessors(c_m.clone());
        let op_m_id = crate::hash_utils::hash_operation(&op_m).unwrap();
        store
            .put_operation(repo_id.to_string(), op_m_id.clone(), op_m)
            .await
            .unwrap();

        // H0 = merge(L, M), preserving both c_l and c_m.
        let view_h0 = ProtoView {
            head_ids: vec![c_l, c_m],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: HashMap::new(),
        };
        let view_h0_id = crate::hash_utils::hash_view(&view_h0).unwrap();
        store
            .put_view(repo_id.to_string(), view_h0_id.clone(), view_h0)
            .await
            .unwrap();
        let op_h0 = make_test_proto_op(view_h0_id, vec![op_l_id.clone(), op_m_id], "merge L and M");
        let op_h0_id = crate::hash_utils::hash_operation(&op_h0).unwrap();
        store
            .put_operation(repo_id.to_string(), op_h0_id.clone(), op_h0)
            .await
            .unwrap();

        // H1 = child(L), advancing c_l -> c_h1.
        let view_h1 = ProtoView {
            head_ids: vec![c_h1.clone()],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: HashMap::new(),
        };
        let view_h1_id = crate::hash_utils::hash_view(&view_h1).unwrap();
        store
            .put_view(repo_id.to_string(), view_h1_id.clone(), view_h1)
            .await
            .unwrap();
        let mut op_h1 = make_test_proto_op(view_h1_id, vec![op_l_id], "child of L");
        op_h1.commit_predecessors = new_commit_predecessors(c_h1);
        let op_h1_id = crate::hash_utils::hash_operation(&op_h1).unwrap();
        store
            .put_operation(repo_id.to_string(), op_h1_id.clone(), op_h1)
            .await
            .unwrap();

        store
            .update_op_heads_append_new_remove_old(
                repo_id.to_string(),
                &[root_op],
                op_h0_id.clone(),
            )
            .await
            .unwrap();
        store
            .update_op_heads_append_new_remove_old(repo_id.to_string(), &[], op_h1_id)
            .await
            .unwrap();

        let outcome = reconcile_repo_op_heads(store.clone(), repo_id)
            .await
            .unwrap();
        assert!(
            matches!(outcome, ReconcileOutcome::Reconciled(_)),
            "expected server reconciliation without leaking past common ancestor X, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn test_reconcile_three_head_linear_ancestor_chain_with_rewrite() {
        use std::collections::HashMap;

        let store = Arc::new(MemoryStore::new());
        let repo_id = "repo-three-head-chain";
        store
            .register_repo(repo_id.to_string(), "proj-3h".to_string(), None)
            .await
            .unwrap();

        let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
        let c0 = vec![0x40; 20];
        let c1 = vec![0x41; 20];
        let c2 = vec![0x42; 20];
        for (cid, parent) in [
            (&c0, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
            (&c1, c0.clone()),
            (&c2, c0.clone()),
        ] {
            store
                .put_commit(
                    "proj-3h".to_string(),
                    cid.clone(),
                    cc_common::backend::Commit {
                        commit_id: cid.clone(),
                        parent_commit_ids: vec![parent],
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
        }

        let make_and_put =
            |head_id: Vec<u8>,
             parents: Vec<Vec<u8>>,
             desc: &str,
             predecessors: Vec<cc_common::op_store::CommitPredecessors>| {
                let store = store.clone();
                let desc = desc.to_string();
                async move {
                    let view = ProtoView {
                        head_ids: vec![head_id],
                        wc_commit_ids: HashMap::new(),
                        local_bookmarks: HashMap::new(),
                        remote_bookmarks: HashMap::new(),
                    };
                    let view_id = crate::hash_utils::hash_view(&view).unwrap();
                    store
                        .put_view(repo_id.to_string(), view_id.clone(), view)
                        .await
                        .unwrap();
                    let mut op = make_test_proto_op(view_id, parents, &desc);
                    op.commit_predecessors = predecessors;
                    let op_id = crate::hash_utils::hash_operation(&op).unwrap();
                    store
                        .put_operation(repo_id.to_string(), op_id.clone(), op)
                        .await
                        .unwrap();
                    op_id
                }
            };

        // Chain: op_h0 (rewrites c1 -> c2) -> op_h1 (adds c1) -> op_h2 (adds c0) -> root.
        let op_h2 = make_and_put(
            c0.clone(),
            vec![root_op.clone()],
            "h2",
            new_commit_predecessors(c0),
        )
        .await;
        let op_h1 = make_and_put(
            c1.clone(),
            vec![op_h2.clone()],
            "h1",
            new_commit_predecessors(c1.clone()),
        )
        .await;
        let op_h0 = make_and_put(
            c2.clone(),
            vec![op_h1.clone()],
            "h0 rewrite",
            vec![cc_common::op_store::CommitPredecessors {
                commit_id: c2,
                predecessor_ids: vec![c1],
            }],
        )
        .await;

        store
            .update_op_heads_append_new_remove_old(repo_id.to_string(), &[root_op], op_h2)
            .await
            .unwrap();
        store
            .update_op_heads_append_new_remove_old(repo_id.to_string(), &[], op_h1)
            .await
            .unwrap();
        store
            .update_op_heads_append_new_remove_old(repo_id.to_string(), &[], op_h0.clone())
            .await
            .unwrap();

        let outcome = reconcile_repo_op_heads(store.clone(), repo_id)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            ReconcileOutcome::Reconciled(op_h0.clone()),
            "3-head linear ancestor chain should prune ancestors directly to H0 even when H0 is a rewrite"
        );
        assert_eq!(
            store.get_op_heads(repo_id).await.unwrap().unwrap(),
            vec![op_h0]
        );
    }

    #[tokio::test]
    async fn test_reconcile_asymmetric_merge_diamonds_do_not_leak_past_common_ancestor() {
        use std::collections::HashMap;

        // Case 1: Asymmetric diamond terminating at root:
        // root <- X (rewrite) <- D2 <- D1 <- C, B -> X, A -> C, H0 = merge(A, B), H1 -> C.
        {
            let store = Arc::new(MemoryStore::new());
            let repo_id = "repo-asym-diamond-root";
            store
                .register_repo(repo_id.to_string(), "proj-adr".to_string(), None)
                .await
                .unwrap();

            let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
            let c_old = vec![0x50; 20];
            let c_x = vec![0x51; 20];
            let c_d2 = vec![0x52; 20];
            let c_d1 = vec![0x53; 20];
            let c_c = vec![0x54; 20];
            let c_a = vec![0x55; 20];
            let c_b = vec![0x56; 20];
            let c_h1 = vec![0x57; 20];
            for (cid, parent) in [
                (&c_old, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
                (&c_x, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
                (&c_d2, c_x.clone()),
                (&c_d1, c_d2.clone()),
                (&c_c, c_d1.clone()),
                (&c_a, c_c.clone()),
                (&c_b, c_x.clone()),
                (&c_h1, c_c.clone()),
            ] {
                store
                    .put_commit(
                        "proj-adr".to_string(),
                        cid.clone(),
                        cc_common::backend::Commit {
                            commit_id: cid.clone(),
                            parent_commit_ids: vec![parent],
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
            }

            let put_op =
                |heads: Vec<Vec<u8>>,
                 parents: Vec<Vec<u8>>,
                 desc: &str,
                 preds: Vec<cc_common::op_store::CommitPredecessors>| {
                    let store = store.clone();
                    let desc = desc.to_string();
                    async move {
                        let view = ProtoView {
                            head_ids: heads,
                            wc_commit_ids: HashMap::new(),
                            local_bookmarks: HashMap::new(),
                            remote_bookmarks: HashMap::new(),
                        };
                        let vid = crate::hash_utils::hash_view(&view).unwrap();
                        store
                            .put_view(repo_id.to_string(), vid.clone(), view)
                            .await
                            .unwrap();
                        let mut op = make_test_proto_op(vid, parents, &desc);
                        op.commit_predecessors = preds;
                        let oid = crate::hash_utils::hash_operation(&op).unwrap();
                        store
                            .put_operation(repo_id.to_string(), oid.clone(), op)
                            .await
                            .unwrap();
                        oid
                    }
                };

            let op_x = put_op(
                vec![c_x.clone()],
                vec![root_op.clone()],
                "historical rewrite X",
                vec![cc_common::op_store::CommitPredecessors {
                    commit_id: c_x.clone(),
                    predecessor_ids: vec![c_old],
                }],
            )
            .await;
            let op_d2 = put_op(
                vec![c_d2.clone()],
                vec![op_x.clone()],
                "D2",
                new_commit_predecessors(c_d2),
            )
            .await;
            let op_d1 = put_op(
                vec![c_d1.clone()],
                vec![op_d2],
                "D1",
                new_commit_predecessors(c_d1),
            )
            .await;
            let op_c = put_op(
                vec![c_c.clone()],
                vec![op_d1],
                "C",
                new_commit_predecessors(c_c),
            )
            .await;
            let op_a = put_op(
                vec![c_a.clone()],
                vec![op_c.clone()],
                "A",
                new_commit_predecessors(c_a.clone()),
            )
            .await;
            let op_b = put_op(
                vec![c_b.clone()],
                vec![op_x],
                "B",
                new_commit_predecessors(c_b.clone()),
            )
            .await;
            let op_h0 = put_op(vec![c_a, c_b], vec![op_a, op_b], "H0 merge(A, B)", vec![]).await;
            let op_h1 = put_op(
                vec![c_h1.clone()],
                vec![op_c],
                "H1 -> C",
                new_commit_predecessors(c_h1),
            )
            .await;

            store
                .update_op_heads_append_new_remove_old(repo_id.to_string(), &[root_op], op_h0)
                .await
                .unwrap();
            store
                .update_op_heads_append_new_remove_old(repo_id.to_string(), &[], op_h1)
                .await
                .unwrap();

            let outcome = reconcile_repo_op_heads(store.clone(), repo_id)
                .await
                .unwrap();
            assert!(
                matches!(outcome, ReconcileOutcome::Reconciled(_)),
                "asymmetric diamond terminating at root must reconcile cleanly, got {outcome:?}"
            );
        }

        // Case 2: Mid-history asymmetric diamond terminating at a deeper common ancestor C2:
        // root <- C2 <- X (rewrite) <- D2 <- D1 <- C, B -> X, E -> C2, F -> C2,
        // A = merge(C, E), H1 = merge(C, F), H0 = merge(A, B).
        {
            let store = Arc::new(MemoryStore::new());
            let repo_id = "repo-asym-diamond-mid";
            store
                .register_repo(repo_id.to_string(), "proj-adm".to_string(), None)
                .await
                .unwrap();

            let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
            let c_c2 = vec![0x60; 20];
            let c_old = vec![0x61; 20];
            let c_x = vec![0x62; 20];
            let c_d2 = vec![0x63; 20];
            let c_d1 = vec![0x64; 20];
            let c_c = vec![0x65; 20];
            let c_e = vec![0x66; 20];
            let c_f = vec![0x67; 20];
            let c_b = vec![0x68; 20];
            for (cid, parent) in [
                (&c_c2, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
                (&c_old, c_c2.clone()),
                (&c_x, c_c2.clone()),
                (&c_d2, c_x.clone()),
                (&c_d1, c_d2.clone()),
                (&c_c, c_d1.clone()),
                (&c_e, c_c2.clone()),
                (&c_f, c_c2.clone()),
                (&c_b, c_x.clone()),
            ] {
                store
                    .put_commit(
                        "proj-adm".to_string(),
                        cid.clone(),
                        cc_common::backend::Commit {
                            commit_id: cid.clone(),
                            parent_commit_ids: vec![parent],
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
            }

            let put_op =
                |heads: Vec<Vec<u8>>,
                 parents: Vec<Vec<u8>>,
                 desc: &str,
                 preds: Vec<cc_common::op_store::CommitPredecessors>| {
                    let store = store.clone();
                    let desc = desc.to_string();
                    async move {
                        let view = ProtoView {
                            head_ids: heads,
                            wc_commit_ids: HashMap::new(),
                            local_bookmarks: HashMap::new(),
                            remote_bookmarks: HashMap::new(),
                        };
                        let vid = crate::hash_utils::hash_view(&view).unwrap();
                        store
                            .put_view(repo_id.to_string(), vid.clone(), view)
                            .await
                            .unwrap();
                        let mut op = make_test_proto_op(vid, parents, &desc);
                        op.commit_predecessors = preds;
                        let oid = crate::hash_utils::hash_operation(&op).unwrap();
                        store
                            .put_operation(repo_id.to_string(), oid.clone(), op)
                            .await
                            .unwrap();
                        oid
                    }
                };

            let op_c2 = put_op(
                vec![c_c2.clone()],
                vec![root_op.clone()],
                "C2",
                new_commit_predecessors(c_c2),
            )
            .await;
            let op_x = put_op(
                vec![c_x.clone()],
                vec![op_c2.clone()],
                "mid-history rewrite X",
                vec![cc_common::op_store::CommitPredecessors {
                    commit_id: c_x.clone(),
                    predecessor_ids: vec![c_old],
                }],
            )
            .await;
            let op_d2 = put_op(
                vec![c_d2.clone()],
                vec![op_x.clone()],
                "D2",
                new_commit_predecessors(c_d2),
            )
            .await;
            let op_d1 = put_op(
                vec![c_d1.clone()],
                vec![op_d2],
                "D1",
                new_commit_predecessors(c_d1),
            )
            .await;
            let op_c = put_op(
                vec![c_c.clone()],
                vec![op_d1],
                "C",
                new_commit_predecessors(c_c.clone()),
            )
            .await;
            let op_e = put_op(
                vec![c_e.clone()],
                vec![op_c2.clone()],
                "E",
                new_commit_predecessors(c_e.clone()),
            )
            .await;
            let op_f = put_op(
                vec![c_f.clone()],
                vec![op_c2],
                "F",
                new_commit_predecessors(c_f.clone()),
            )
            .await;
            let op_b = put_op(
                vec![c_b.clone()],
                vec![op_x],
                "B",
                new_commit_predecessors(c_b.clone()),
            )
            .await;
            let op_a = put_op(
                vec![c_c.clone(), c_e.clone()],
                vec![op_c.clone(), op_e],
                "A merge(C, E)",
                vec![],
            )
            .await;
            let op_h1 = put_op(
                vec![c_c.clone(), c_f],
                vec![op_c, op_f],
                "H1 merge(C, F)",
                vec![],
            )
            .await;
            let op_h0 = put_op(
                vec![c_c, c_e, c_b],
                vec![op_a, op_b],
                "H0 merge(A, B)",
                vec![],
            )
            .await;

            store
                .update_op_heads_append_new_remove_old(repo_id.to_string(), &[root_op], op_h0)
                .await
                .unwrap();
            store
                .update_op_heads_append_new_remove_old(repo_id.to_string(), &[], op_h1)
                .await
                .unwrap();

            let outcome = reconcile_repo_op_heads(store.clone(), repo_id)
                .await
                .unwrap();
            assert!(
                matches!(outcome, ReconcileOutcome::Reconciled(_)),
                "mid-history asymmetric diamond must reconcile cleanly, got {outcome:?}"
            );
        }

        // Case 3: Lagging common merge op with two distinct historical rewrite parents (X1, X2).
        // Expanding C = merge(X1, X2) when clearing the first suspect must still mark both X1
        // and X2 common before breaking out of verify_queue.
        for swap_order in [false, true] {
            let store = Arc::new(MemoryStore::new());
            let repo_id = if swap_order {
                "repo-asym-two-rewrites-swapped"
            } else {
                "repo-asym-two-rewrites"
            };
            store
                .register_repo(repo_id.to_string(), "proj-a2r".to_string(), None)
                .await
                .unwrap();

            let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
            let c_old1 = vec![0x70; 20];
            let c_x1 = vec![0x71; 20];
            let c_old2 = vec![0x72; 20];
            let c_x2 = vec![0x73; 20];
            let c_e2 = vec![0x74; 20];
            let c_e1 = vec![0x75; 20];
            let c_a = vec![0x76; 20];
            let c_h1 = vec![0x77; 20];
            for (cid, parent) in [
                (&c_old1, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
                (&c_x1, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
                (&c_old2, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
                (&c_x2, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
                (&c_e2, c_x1.clone()),
                (&c_e1, c_e2.clone()),
                (&c_a, c_e1.clone()),
                (&c_h1, c_e1.clone()),
            ] {
                store
                    .put_commit(
                        "proj-a2r".to_string(),
                        cid.clone(),
                        cc_common::backend::Commit {
                            commit_id: cid.clone(),
                            parent_commit_ids: vec![parent],
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
            }

            let put_op =
                |heads: Vec<Vec<u8>>,
                 parents: Vec<Vec<u8>>,
                 desc: &str,
                 preds: Vec<cc_common::op_store::CommitPredecessors>| {
                    let store = store.clone();
                    let desc = desc.to_string();
                    async move {
                        let view = ProtoView {
                            head_ids: heads,
                            wc_commit_ids: HashMap::new(),
                            local_bookmarks: HashMap::new(),
                            remote_bookmarks: HashMap::new(),
                        };
                        let vid = crate::hash_utils::hash_view(&view).unwrap();
                        store
                            .put_view(repo_id.to_string(), vid.clone(), view)
                            .await
                            .unwrap();
                        let mut op = make_test_proto_op(vid, parents, &desc);
                        op.commit_predecessors = preds;
                        let oid = crate::hash_utils::hash_operation(&op).unwrap();
                        store
                            .put_operation(repo_id.to_string(), oid.clone(), op)
                            .await
                            .unwrap();
                        oid
                    }
                };

            let op_x1 = put_op(
                vec![c_x1.clone()],
                vec![root_op.clone()],
                "rewrite X1",
                vec![cc_common::op_store::CommitPredecessors {
                    commit_id: c_x1.clone(),
                    predecessor_ids: vec![c_old1],
                }],
            )
            .await;
            let op_x2 = put_op(
                vec![c_x2.clone()],
                vec![root_op.clone()],
                "rewrite X2",
                vec![cc_common::op_store::CommitPredecessors {
                    commit_id: c_x2.clone(),
                    predecessor_ids: vec![c_old2],
                }],
            )
            .await;
            let c_parents = if swap_order {
                vec![op_x2.clone(), op_x1.clone()]
            } else {
                vec![op_x1.clone(), op_x2.clone()]
            };
            let op_c = put_op(
                vec![c_x1.clone(), c_x2.clone()],
                c_parents.clone(),
                "C merge(X1, X2)",
                vec![],
            )
            .await;
            let op_e2 = put_op(
                vec![c_e2.clone(), c_x2.clone()],
                vec![op_c],
                "E2",
                new_commit_predecessors(c_e2),
            )
            .await;
            let op_e1 = put_op(
                vec![c_e1.clone(), c_x2.clone()],
                vec![op_e2],
                "E1",
                new_commit_predecessors(c_e1),
            )
            .await;
            let op_a = put_op(
                vec![c_a.clone(), c_x2.clone()],
                vec![op_e1.clone()],
                "A",
                new_commit_predecessors(c_a.clone()),
            )
            .await;
            let op_h1 = put_op(
                vec![c_h1.clone(), c_x2.clone()],
                vec![op_e1],
                "H1",
                new_commit_predecessors(c_h1),
            )
            .await;
            let op_b = put_op(
                vec![c_x1, c_x2.clone()],
                c_parents,
                "B merge(X1, X2)",
                vec![],
            )
            .await;
            let op_h0 = put_op(vec![c_a, c_x2], vec![op_a, op_b], "H0 merge(A, B)", vec![]).await;

            store
                .update_op_heads_append_new_remove_old(repo_id.to_string(), &[root_op], op_h0)
                .await
                .unwrap();
            store
                .update_op_heads_append_new_remove_old(repo_id.to_string(), &[], op_h1)
                .await
                .unwrap();

            let outcome = reconcile_repo_op_heads(store.clone(), repo_id)
                .await
                .unwrap();
            assert!(
                matches!(outcome, ReconcileOutcome::Reconciled(_)),
                "lagging common merge with two rewrite parents must reconcile cleanly (swap={swap_order}), got {outcome:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_reconcile_concurrent_new_commits_with_empty_predecessors_reconciles_server_side()
    {
        use std::collections::HashMap;

        let store = Arc::new(MemoryStore::new());
        let repo_id = "repo-concurrent-new-commits";
        store
            .register_repo(repo_id.to_string(), "proj-cnc".to_string(), None)
            .await
            .unwrap();

        let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
        let c0 = vec![0x80; 20];
        let c1 = vec![0x81; 20];
        let c2 = vec![0x82; 20];
        for (cid, parent) in [
            (&c0, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
            (&c1, c0.clone()),
            (&c2, c0.clone()),
        ] {
            store
                .put_commit(
                    "proj-cnc".to_string(),
                    cid.clone(),
                    cc_common::backend::Commit {
                        commit_id: cid.clone(),
                        parent_commit_ids: vec![parent],
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
        }

        let base_view = ProtoView {
            head_ids: vec![c0.clone()],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: HashMap::new(),
        };
        let base_view_id = crate::hash_utils::hash_view(&base_view).unwrap();
        store
            .put_view(repo_id.to_string(), base_view_id.clone(), base_view)
            .await
            .unwrap();
        let mut base_op = make_test_proto_op(base_view_id, vec![root_op.clone()], "base c0");
        base_op.commit_predecessors = new_commit_predecessors(c0);
        let base_op_id = crate::hash_utils::hash_operation(&base_op).unwrap();
        store
            .put_operation(repo_id.to_string(), base_op_id.clone(), base_op)
            .await
            .unwrap();

        // Branch A creates new commit c1 (recording empty predecessor_ids as jj_lib 0.43 does).
        let view_a = ProtoView {
            head_ids: vec![c1.clone()],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: HashMap::new(),
        };
        let view_a_id = crate::hash_utils::hash_view(&view_a).unwrap();
        store
            .put_view(repo_id.to_string(), view_a_id.clone(), view_a)
            .await
            .unwrap();
        let mut op_a = make_test_proto_op(view_a_id, vec![base_op_id.clone()], "new commit c1");
        op_a.commit_predecessors = new_commit_predecessors(c1.clone());
        let op_a_id = crate::hash_utils::hash_operation(&op_a).unwrap();
        store
            .put_operation(repo_id.to_string(), op_a_id.clone(), op_a)
            .await
            .unwrap();

        // Branch B concurrently creates new commit c2 (also with empty predecessor_ids).
        let view_b = ProtoView {
            head_ids: vec![c2.clone()],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: HashMap::new(),
        };
        let view_b_id = crate::hash_utils::hash_view(&view_b).unwrap();
        store
            .put_view(repo_id.to_string(), view_b_id.clone(), view_b)
            .await
            .unwrap();
        let mut op_b = make_test_proto_op(view_b_id, vec![base_op_id], "new commit c2");
        op_b.commit_predecessors = new_commit_predecessors(c2.clone());
        let op_b_id = crate::hash_utils::hash_operation(&op_b).unwrap();
        store
            .put_operation(repo_id.to_string(), op_b_id.clone(), op_b)
            .await
            .unwrap();

        store
            .update_op_heads_append_new_remove_old(repo_id.to_string(), &[root_op], op_a_id)
            .await
            .unwrap();
        store
            .update_op_heads_append_new_remove_old(repo_id.to_string(), &[], op_b_id)
            .await
            .unwrap();

        let merged_op_id = match reconcile_repo_op_heads(store.clone(), repo_id)
            .await
            .unwrap()
        {
            ReconcileOutcome::Reconciled(id) => id,
            other => panic!("concurrent new commits must reconcile server-side, got {other:?}"),
        };
        let merged_op = store
            .get_operation(repo_id, &merged_op_id)
            .await
            .unwrap()
            .unwrap();
        let merged_view = store
            .get_view(repo_id, &merged_op.view_id)
            .await
            .unwrap()
            .unwrap();
        let head_set: HashSet<Vec<u8>> = merged_view.head_ids.into_iter().collect();
        assert_eq!(head_set, HashSet::from([c1, c2]));
        assert_eq!(
            store.get_op_heads(repo_id).await.unwrap().unwrap(),
            vec![merged_op_id]
        );
    }

    #[tokio::test]
    async fn test_reconcile_combined_abandon_and_new_commit_requires_client_resolution() {
        use std::collections::HashMap;

        let store = Arc::new(MemoryStore::new());
        let repo_id = "repo-abandon-plus-new";
        store
            .register_repo(repo_id.to_string(), "proj-apn".to_string(), None)
            .await
            .unwrap();

        let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
        let c0 = vec![0x90; 20];
        let c1 = vec![0x91; 20];
        let c2 = vec![0x92; 20];
        for (cid, parent) in [
            (&c0, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
            (&c1, c0.clone()),
            // c2 is rooted directly on ROOT_COMMIT_ID (does not descend from abandoned c0).
            (&c2, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
        ] {
            store
                .put_commit(
                    "proj-apn".to_string(),
                    cid.clone(),
                    cc_common::backend::Commit {
                        commit_id: cid.clone(),
                        parent_commit_ids: vec![parent],
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
        }

        let base_view = ProtoView {
            head_ids: vec![c0.clone()],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: HashMap::new(),
        };
        let base_view_id = crate::hash_utils::hash_view(&base_view).unwrap();
        store
            .put_view(repo_id.to_string(), base_view_id.clone(), base_view)
            .await
            .unwrap();
        let mut base_op = make_test_proto_op(base_view_id, vec![root_op.clone()], "base c0");
        base_op.commit_predecessors = new_commit_predecessors(c0);
        let base_op_id = crate::hash_utils::hash_operation(&base_op).unwrap();
        store
            .put_operation(repo_id.to_string(), base_op_id.clone(), base_op)
            .await
            .unwrap();

        // Branch A adds child c1 on top of c0.
        let view_a = ProtoView {
            head_ids: vec![c1.clone()],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: HashMap::new(),
        };
        let view_a_id = crate::hash_utils::hash_view(&view_a).unwrap();
        store
            .put_view(repo_id.to_string(), view_a_id.clone(), view_a)
            .await
            .unwrap();
        let mut op_a = make_test_proto_op(view_a_id, vec![base_op_id.clone()], "add c1");
        op_a.commit_predecessors = new_commit_predecessors(c1);
        let op_a_id = crate::hash_utils::hash_operation(&op_a).unwrap();
        store
            .put_operation(repo_id.to_string(), op_a_id.clone(), op_a)
            .await
            .unwrap();

        // Branch B abandons c0 and creates a fresh commit c2 in the same operation (recording
        // c2 with empty predecessor_ids, as `jj abandon` does when creating a new working copy).
        let view_b = ProtoView {
            head_ids: vec![c2.clone()],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: HashMap::new(),
        };
        let view_b_id = crate::hash_utils::hash_view(&view_b).unwrap();
        store
            .put_view(repo_id.to_string(), view_b_id.clone(), view_b)
            .await
            .unwrap();
        let mut op_b = make_test_proto_op(view_b_id, vec![base_op_id], "abandon c0 and new c2");
        op_b.commit_predecessors = new_commit_predecessors(c2);
        let op_b_id = crate::hash_utils::hash_operation(&op_b).unwrap();
        store
            .put_operation(repo_id.to_string(), op_b_id.clone(), op_b)
            .await
            .unwrap();

        store
            .update_op_heads_append_new_remove_old(repo_id.to_string(), &[root_op], op_a_id.clone())
            .await
            .unwrap();
        store
            .update_op_heads_append_new_remove_old(repo_id.to_string(), &[], op_b_id.clone())
            .await
            .unwrap();

        let outcome = reconcile_repo_op_heads(store.clone(), repo_id)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            ReconcileOutcome::ClientResolutionRequired(vec![op_a_id.clone(), op_b_id.clone()])
        );
        assert_eq!(
            store.get_op_heads(repo_id).await.unwrap().unwrap(),
            vec![op_a_id, op_b_id]
        );
    }

    struct CountingStore {
        inner: Arc<MemoryStore>,
        get_commit_calls: std::sync::atomic::AtomicUsize,
    }

    impl CountingStore {
        fn new(inner: Arc<MemoryStore>) -> Self {
            Self {
                inner,
                get_commit_calls: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn commit_lookups(&self) -> usize {
            self.get_commit_calls
                .load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl Store for CountingStore {
        async fn is_project_registered(&self, project_id: &str) -> crate::store::StoreResult<bool> {
            self.inner.is_project_registered(project_id).await
        }

        async fn register_project(
            &self,
            project_id: String,
            name: Option<String>,
        ) -> crate::store::StoreResult<()> {
            self.inner.register_project(project_id, name).await
        }

        async fn is_repo_registered(&self, repo_id: &str) -> crate::store::StoreResult<bool> {
            self.inner.is_repo_registered(repo_id).await
        }

        async fn register_repo(
            &self,
            repo_id: String,
            project_id: String,
            name: Option<String>,
        ) -> crate::store::StoreResult<()> {
            self.inner.register_repo(repo_id, project_id, name).await
        }

        async fn get_repo_project_id(
            &self,
            repo_id: &str,
        ) -> crate::store::StoreResult<Option<String>> {
            self.inner.get_repo_project_id(repo_id).await
        }

        async fn get_commit(
            &self,
            project_id: &str,
            commit_id: &[u8],
        ) -> crate::store::StoreResult<Option<cc_common::backend::Commit>> {
            self.get_commit_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.get_commit(project_id, commit_id).await
        }

        async fn list_project_commit_ids(
            &self,
            project_id: &str,
        ) -> crate::store::StoreResult<Vec<crate::store::CommitId>> {
            self.inner.list_project_commit_ids(project_id).await
        }

        async fn put_commit(
            &self,
            project_id: String,
            commit_id: Vec<u8>,
            commit: cc_common::backend::Commit,
        ) -> crate::store::StoreResult<()> {
            self.inner.put_commit(project_id, commit_id, commit).await
        }

        async fn get_tree(
            &self,
            project_id: &str,
            tree_id: &[u8],
        ) -> crate::store::StoreResult<Option<Vec<cc_common::backend::TreeEntry>>> {
            self.inner.get_tree(project_id, tree_id).await
        }

        async fn put_tree(
            &self,
            project_id: String,
            tree_id: Vec<u8>,
            entries: Vec<cc_common::backend::TreeEntry>,
        ) -> crate::store::StoreResult<()> {
            self.inner.put_tree(project_id, tree_id, entries).await
        }

        async fn get_file(
            &self,
            project_id: &str,
            file_id: &[u8],
        ) -> crate::store::StoreResult<Option<Vec<u8>>> {
            self.inner.get_file(project_id, file_id).await
        }

        async fn put_file(
            &self,
            project_id: String,
            file_id: Vec<u8>,
            content: Vec<u8>,
        ) -> crate::store::StoreResult<()> {
            self.inner.put_file(project_id, file_id, content).await
        }

        async fn get_operation(
            &self,
            repo_id: &str,
            op_id: &[u8],
        ) -> crate::store::StoreResult<Option<cc_common::op_store::Operation>> {
            self.inner.get_operation(repo_id, op_id).await
        }

        async fn put_operation(
            &self,
            repo_id: String,
            op_id: Vec<u8>,
            op: cc_common::op_store::Operation,
        ) -> crate::store::StoreResult<()> {
            self.inner.put_operation(repo_id, op_id, op).await
        }

        async fn get_view(
            &self,
            repo_id: &str,
            view_id: &[u8],
        ) -> crate::store::StoreResult<Option<cc_common::op_store::View>> {
            self.inner.get_view(repo_id, view_id).await
        }

        async fn put_view(
            &self,
            repo_id: String,
            view_id: Vec<u8>,
            view: cc_common::op_store::View,
        ) -> crate::store::StoreResult<()> {
            self.inner.put_view(repo_id, view_id, view).await
        }

        async fn get_op_heads(
            &self,
            repo_id: &str,
        ) -> crate::store::StoreResult<Option<Vec<Vec<u8>>>> {
            self.inner.get_op_heads(repo_id).await
        }

        async fn update_op_heads_append_new_remove_old(
            &self,
            repo_id: String,
            ids_to_remove: &[Vec<u8>],
            id_to_add: Vec<u8>,
        ) -> crate::store::StoreResult<Vec<Vec<u8>>> {
            self.inner
                .update_op_heads_append_new_remove_old(repo_id, ids_to_remove, id_to_add)
                .await
        }

        async fn update_op_heads_compare_and_swap(
            &self,
            repo_id: String,
            expected_exact_heads: &[Vec<u8>],
            new_id: Vec<u8>,
        ) -> crate::store::StoreResult<Vec<Vec<u8>>> {
            self.inner
                .update_op_heads_compare_and_swap(repo_id, expected_exact_heads, new_id)
                .await
        }

        async fn get_workspace(
            &self,
            repo_id: &str,
            user: &str,
            workspace_name: &str,
        ) -> crate::store::StoreResult<Option<cc_common::workspace::WorkspaceState>> {
            self.inner
                .get_workspace(repo_id, user, workspace_name)
                .await
        }

        async fn put_workspace_cas(
            &self,
            workspace: cc_common::workspace::WorkspaceState,
            expected_operation_id: Option<&[u8]>,
        ) -> crate::store::StoreResult<()> {
            self.inner
                .put_workspace_cas(workspace, expected_operation_id)
                .await
        }

        async fn delete_workspace(
            &self,
            repo_id: &str,
            user: &str,
            workspace_name: &str,
        ) -> crate::store::StoreResult<bool> {
            self.inner
                .delete_workspace(repo_id, user, workspace_name)
                .await
        }

        async fn list_workspaces(
            &self,
            repo_id: &str,
        ) -> crate::store::StoreResult<Vec<cc_common::workspace::WorkspaceState>> {
            self.inner.list_workspaces(repo_id).await
        }

        async fn list_workspaces_page(
            &self,
            repo_id: &str,
            after: Option<(&str, &str)>,
            limit: usize,
        ) -> crate::store::StoreResult<Vec<cc_common::workspace::WorkspaceState>> {
            self.inner.list_workspaces_page(repo_id, after, limit).await
        }
    }

    #[tokio::test]
    async fn test_reconcile_prioritizes_newly_added_heads_over_many_unchanged_heads() {
        use std::collections::HashMap;

        let inner = Arc::new(MemoryStore::new());
        let repo_id = "repo-many-unchanged-heads";
        let project_id = "proj-muh";
        inner
            .register_repo(repo_id.to_string(), project_id.to_string(), None)
            .await
            .unwrap();

        let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
        let c_a0 = vec![0xa0; 20];
        let c_a1 = vec![0xa1; 20];
        let c_b0 = vec![0xb0; 20];
        let c_b1 = vec![0xb1; 20];
        for (cid, parent) in [
            (&c_a0, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
            (&c_a1, c_a0.clone()),
            (&c_b0, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
            (&c_b1, c_b0.clone()),
        ] {
            inner
                .put_commit(
                    project_id.to_string(),
                    cid.clone(),
                    cc_common::backend::Commit {
                        commit_id: cid.clone(),
                        parent_commit_ids: vec![parent],
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
        }

        // Populate more unrelated unchanged heads than MAX_COMMIT_WALK_VISITED so a BFS that
        // seeds all view heads blindly would exceed the walk budget and falsely fall back to
        // ClientResolutionRequired.
        let num_unrelated = MAX_COMMIT_WALK_VISITED + 64;
        let mut unrelated_heads = Vec::with_capacity(num_unrelated);
        for i in 0..num_unrelated {
            let mut cid = vec![0u8; 20];
            cid[0] = 0x55;
            cid[16..20].copy_from_slice(&(i as u32).to_be_bytes());
            inner
                .put_commit(
                    project_id.to_string(),
                    cid.clone(),
                    cc_common::backend::Commit {
                        commit_id: cid.clone(),
                        parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            unrelated_heads.push(cid);
        }

        let mut base_heads = unrelated_heads.clone();
        base_heads.push(c_a0.clone());
        base_heads.push(c_b0.clone());
        let base_view = ProtoView {
            head_ids: base_heads,
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: HashMap::new(),
        };
        let base_view_id = crate::hash_utils::hash_view(&base_view).unwrap();
        inner
            .put_view(repo_id.to_string(), base_view_id.clone(), base_view)
            .await
            .unwrap();
        let base_op = make_test_proto_op(base_view_id, vec![root_op.clone()], "base");
        let base_op_id = crate::hash_utils::hash_operation(&base_op).unwrap();
        inner
            .put_operation(repo_id.to_string(), base_op_id.clone(), base_op)
            .await
            .unwrap();

        // Branch A advances c_a0 -> c_a1 while leaving all unrelated heads untouched.
        let mut heads_a = unrelated_heads.clone();
        heads_a.push(c_a1.clone());
        heads_a.push(c_b0);
        let view_a = ProtoView {
            head_ids: heads_a,
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: HashMap::new(),
        };
        let view_a_id = crate::hash_utils::hash_view(&view_a).unwrap();
        inner
            .put_view(repo_id.to_string(), view_a_id.clone(), view_a)
            .await
            .unwrap();
        let mut op_a = make_test_proto_op(view_a_id, vec![base_op_id.clone()], "advance a");
        op_a.commit_predecessors = new_commit_predecessors(c_a1.clone());
        let op_a_id = crate::hash_utils::hash_operation(&op_a).unwrap();
        inner
            .put_operation(repo_id.to_string(), op_a_id.clone(), op_a)
            .await
            .unwrap();

        // Branch B concurrently advances c_b0 -> c_b1 while leaving all unrelated heads untouched.
        let mut heads_b = unrelated_heads;
        heads_b.push(c_a0);
        heads_b.push(c_b1.clone());
        let view_b = ProtoView {
            head_ids: heads_b,
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: HashMap::new(),
        };
        let view_b_id = crate::hash_utils::hash_view(&view_b).unwrap();
        inner
            .put_view(repo_id.to_string(), view_b_id.clone(), view_b)
            .await
            .unwrap();
        let mut op_b = make_test_proto_op(view_b_id, vec![base_op_id], "advance b");
        op_b.commit_predecessors = new_commit_predecessors(c_b1.clone());
        let op_b_id = crate::hash_utils::hash_operation(&op_b).unwrap();
        inner
            .put_operation(repo_id.to_string(), op_b_id.clone(), op_b)
            .await
            .unwrap();

        inner
            .update_op_heads_append_new_remove_old(repo_id.to_string(), &[root_op], op_a_id)
            .await
            .unwrap();
        inner
            .update_op_heads_append_new_remove_old(repo_id.to_string(), &[], op_b_id)
            .await
            .unwrap();

        let counting_store = Arc::new(CountingStore::new(inner.clone()));
        let outcome = reconcile_repo_op_heads(counting_store.clone(), repo_id)
            .await
            .unwrap();
        assert!(
            matches!(outcome, ReconcileOutcome::Reconciled(_)),
            "normal advancement with >MAX_COMMIT_WALK_VISITED unchanged heads must reconcile cleanly, got {outcome:?}"
        );
        assert_eq!(
            counting_store.commit_lookups(),
            2,
            "abandonment check should only fetch the two newly added heads (c_a1, c_b1)"
        );
    }

    #[tokio::test]
    async fn test_reconcile_falls_back_to_unchanged_heads_for_unnormalized_view_and_caches_commits()
    {
        use std::collections::HashMap;

        let inner = Arc::new(MemoryStore::new());
        let repo_id = "repo-unnormalized-fallback";
        let project_id = "proj-uf";
        inner
            .register_repo(repo_id.to_string(), project_id.to_string(), None)
            .await
            .unwrap();

        let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
        let c_old = vec![0xc0; 20];
        let c_new = vec![0xc1; 20];
        let c_side = vec![0xc2; 20];
        for (cid, parent) in [
            (&c_old, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
            (&c_new, c_old.clone()),
            (&c_side, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
        ] {
            inner
                .put_commit(
                    project_id.to_string(),
                    cid.clone(),
                    cc_common::backend::Commit {
                        commit_id: cid.clone(),
                        parent_commit_ids: vec![parent],
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
        }

        // Synthetic unnormalized base view lists both c_old and its child c_new as heads.
        let base_view = ProtoView {
            head_ids: vec![c_old.clone(), c_new.clone()],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: HashMap::new(),
        };
        let base_view_id = crate::hash_utils::hash_view(&base_view).unwrap();
        inner
            .put_view(repo_id.to_string(), base_view_id.clone(), base_view)
            .await
            .unwrap();
        let base_op = make_test_proto_op(base_view_id, vec![root_op.clone()], "unnormalized base");
        let base_op_id = crate::hash_utils::hash_operation(&base_op).unwrap();
        inner
            .put_operation(repo_id.to_string(), base_op_id.clone(), base_op)
            .await
            .unwrap();

        // Branch A normalizes the view by dropping c_old while keeping unchanged head c_new
        // (so newly_added_heads is empty and Phase 2 must fall back to unchanged_heads).
        let view_a = ProtoView {
            head_ids: vec![c_new.clone()],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: HashMap::new(),
        };
        let view_a_id = crate::hash_utils::hash_view(&view_a).unwrap();
        inner
            .put_view(repo_id.to_string(), view_a_id.clone(), view_a)
            .await
            .unwrap();
        let op_a = make_test_proto_op(view_a_id, vec![base_op_id.clone()], "normalize view a");
        let op_a_id = crate::hash_utils::hash_operation(&op_a).unwrap();
        inner
            .put_operation(repo_id.to_string(), op_a_id.clone(), op_a)
            .await
            .unwrap();

        // Branch B also drops c_old while adding an unrelated side head c_side, forcing a
        // second walk over c_new that should reuse the per-call commit_cache entry.
        let view_b = ProtoView {
            head_ids: vec![c_new.clone(), c_side.clone()],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: HashMap::new(),
        };
        let view_b_id = crate::hash_utils::hash_view(&view_b).unwrap();
        inner
            .put_view(repo_id.to_string(), view_b_id.clone(), view_b)
            .await
            .unwrap();
        let mut op_b = make_test_proto_op(view_b_id, vec![base_op_id], "normalize + side b");
        op_b.commit_predecessors = new_commit_predecessors(c_side);
        let op_b_id = crate::hash_utils::hash_operation(&op_b).unwrap();
        inner
            .put_operation(repo_id.to_string(), op_b_id.clone(), op_b)
            .await
            .unwrap();

        inner
            .update_op_heads_append_new_remove_old(repo_id.to_string(), &[root_op], op_a_id)
            .await
            .unwrap();
        inner
            .update_op_heads_append_new_remove_old(repo_id.to_string(), &[], op_b_id)
            .await
            .unwrap();

        let counting_store = Arc::new(CountingStore::new(inner));
        let outcome = reconcile_repo_op_heads(counting_store.clone(), repo_id)
            .await
            .unwrap();
        assert!(
            matches!(outcome, ReconcileOutcome::Reconciled(_)),
            "unchanged-head fallback must recognize ancestor c_old of c_new, got {outcome:?}"
        );
        // c_new is fetched once (cached across op_a and op_b) and c_side is fetched once.
        assert_eq!(counting_store.commit_lookups(), 2);
    }
}
