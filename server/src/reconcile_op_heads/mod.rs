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

//! Server-side operation-head reconciliation engine executing `jj_lib`'s 3-way view and
//! tree merge algebra against [`CompositeStore`].
//!
//! # Authority and Crash-Safety Ordering
//! `reconcile_repo_op_heads_proto` resolves `project_id` once per attempt alongside `heads` and
//! pins all adapters (`ServerBackend`, `ServerOpStore`, `ServerOpHeadsStore`, `ServerIndex`,
//! `ServerIndexStore`) to that `project_id`, writing the merged `View` and `Operation`
//! through project-pinned `*_for_project` calls before atomically swapping the validated
//! head snapshot via [`ServerOpHeadsStore::with_expected_heads_for_project`] CAS.
//!
//! # Concurrency and Merge Invariants
//! In-process callers targeting the same `repo_id` serialize behind a per-repository async
//! mutex, while concurrent cross-process writers retry [`StoreError::CasConflict`] through
//! rewrite-safety checks. Divergent rewrites, abandons, missing `commit_predecessors`, walks
//! exceeding safety caps, or [`ReconcileRepoMode::Bypass`] on divergent heads fail closed with
//! `client_resolution_required = true`. [`ReconcileRepoMode::Quarantine`] fails closed before
//! store reads with non-retryable `FAILED_PRECONDITION` to shed load during merge storms.
//!
//! # Cross-Process and RPC Boundaries
//! Runs `RepoLoader::load_at_head()` (`!Send` due to `dyn Repo`) on Tokio's
//! `spawn_blocking` pool via `pollster::block_on`, propagating [`crate::resilience::current_rpc_deadline`]
//! into adapters, preserving typed [`StoreError`] gRPC status codes, and retrying transient
//! failures with bounded exponential backoff without sleeping past the caller's deadline.

#![allow(elided_lifetimes_in_paths)]

mod errors;
mod kill_switch;
#[cfg(test)]
mod tests;

pub use kill_switch::{ReconcileKillSwitch, ReconcileRepoMode};

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use futures::TryStreamExt as _;
use jj_lib::backend::CommitId;
use jj_lib::files::MergeOptions;
use jj_lib::index::Index as _;
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_heads_store::OpHeadsStore;
use jj_lib::op_store::{OpStore, OperationId};
use jj_lib::operation::Operation as JjOperation;
use jj_lib::repo::RepoLoader;
use jj_lib::settings::UserSettings;
use jj_lib::signing_factory::signer_from_settings;
use jj_lib::store::Store as JjStore;

use self::errors::{
    ReconcileError, is_walk_budget_exceeded_in_index, is_walk_budget_exceeded_in_repo_loader,
};
use crate::jj_lib_adapters::{
    IndexWalkBudget, IndexWalkLimits, MAX_ANCESTRY_WALK_COMMITS, ServerBackend, ServerIndex,
    ServerIndexStore, ServerOpHeadsStore, ServerOpStore, ServerSubmoduleStore,
    ServerWorkspaceStore,
};
use crate::store::{CompositeStore, StoreError};

const MAX_OP_WALK_VISITED: usize = 10_000;
const MAX_COMMIT_WALK_VISITED: usize = MAX_ANCESTRY_WALK_COMMITS;
const MAX_DIVERGENT_OP_HEADS: usize = 32;

static RECONCILER_ENV: LazyLock<(UserSettings, MergeOptions)> = LazyLock::new(|| {
    let mut config = jj_lib::config::StackedConfig::with_defaults();
    config.add_layer(
        jj_lib::config::ConfigLayer::parse(
            jj_lib::config::ConfigSource::Default,
            "operation.hostname = \"jj-cc-server\"\noperation.username = \"jj-cc-server\"\n",
        )
        .expect("reconciler config layer should parse"),
    );
    let user_settings = UserSettings::from_config_and_home_dir(config, None)
        .expect("default user settings should load");
    let merge_options = user_settings
        .merge_options()
        .expect("default merge options should initialize");
    (user_settings, merge_options)
});

static REPO_RECONCILE_LOCKS: LazyLock<
    Mutex<HashMap<String, std::sync::Weak<tokio::sync::Mutex<()>>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

fn repo_reconcile_mutex(repo_id: &str) -> Arc<tokio::sync::Mutex<()>> {
    let mut locks = REPO_RECONCILE_LOCKS
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    locks.retain(|_, weak| weak.strong_count() > 0);
    if let Some(existing) = locks.get(repo_id).and_then(std::sync::Weak::upgrade) {
        return existing;
    }
    let fresh = Arc::new(tokio::sync::Mutex::new(()));
    locks.insert(repo_id.to_string(), Arc::downgrade(&fresh));
    fresh
}

fn single_head_response(
    repo_id: &str,
    heads: Vec<Vec<u8>>,
) -> Result<cc_common::op_store::ReconcileOpHeadsResponse, tonic::Status> {
    let op_head = heads
        .into_iter()
        .next()
        .unwrap_or_else(|| cc_common::ROOT_OPERATION_ID_BYTES.to_vec());
    if op_head.len() != cc_common::OPERATION_ID_LENGTH {
        return Err(StoreError::Corruption(format!(
            "stored op_head in repo '{repo_id}' must be {} bytes (got {} bytes)",
            cc_common::OPERATION_ID_LENGTH,
            op_head.len()
        ))
        .into());
    }
    Ok(cc_common::op_store::ReconcileOpHeadsResponse {
        current_op_head_ids: vec![op_head.clone()],
        op_head,
        client_resolution_required: false,
    })
}

fn quarantine_status(repo_id: &str) -> tonic::Status {
    tonic::Status::failed_precondition(format!(
        "server op-head reconciliation is quarantined for repository '{repo_id}'"
    ))
}

fn bypass_response(
    repo_id: &str,
    heads: Vec<Vec<u8>>,
) -> Result<cc_common::op_store::ReconcileOpHeadsResponse, tonic::Status> {
    for head in &heads {
        if head.len() != cc_common::OPERATION_ID_LENGTH {
            return Err(StoreError::Corruption(format!(
                "stored op_head in repo '{repo_id}' must be {} bytes (got {} bytes)",
                cc_common::OPERATION_ID_LENGTH,
                head.len()
            ))
            .into());
        }
    }
    tracing::warn!(
        repo_id = %repo_id,
        head_count = heads.len(),
        "Server op-head reconciliation bypassed by per-repo kill switch; returning active heads for client resolution"
    );
    Ok(cc_common::op_store::ReconcileOpHeadsResponse {
        op_head: Vec::new(),
        client_resolution_required: true,
        current_op_head_ids: heads,
    })
}

/// Reconciles operation heads for `repo_id` under an optional per-repository
/// [`ReconcileKillSwitch`] policy and records outcome metrics when `metrics` is provided.
///
/// Mode semantics:
/// - [`ReconcileRepoMode::Quarantine`]: checked upfront before any store read or per-repo lock
///   (and re-checked inside the retry loop after lock acquisition and before retry backoff or
///   fallback responses) to shed DB load and halt automated client merge storms with non-retryable
///   `FAILED_PRECONDITION`, without incrementing success metrics.
/// - [`ReconcileRepoMode::Bypass`]: allows already-converged repositories (`heads.len() <= 1`)
///   to take the zero-merge single-head fast path (`reconcile_merged_total`), and on divergent
///   heads (`heads.len() > 1`) skips `jj_lib` merging and DB writes to return
///   `client_resolution_required = true` with validated `current_op_head_ids`
///   (`reconcile_client_fallback_total`). Re-checked inside the retry loop after lock
///   acquisition and after transient failures so dynamic toggles take effect immediately
///   without waiting on backoff sleep.
pub async fn reconcile_repo_op_heads_proto_with_policy_and_metrics(
    store: Arc<CompositeStore>,
    repo_id: &str,
    kill_switch: Option<&ReconcileKillSwitch>,
    metrics: Option<&crate::metrics::ServerMetrics>,
) -> Result<cc_common::op_store::ReconcileOpHeadsResponse, tonic::Status> {
    let response = reconcile_repo_op_heads_with_policy_and_limits(
        store,
        repo_id,
        kill_switch,
        IndexWalkLimits::default(),
        metrics,
    )
    .await?;
    if let Some(m) = metrics {
        m.record_reconcile_op_heads(response.client_resolution_required);
    }
    Ok(response)
}

pub async fn reconcile_repo_op_heads_proto(
    store: Arc<CompositeStore>,
    repo_id: &str,
) -> Result<cc_common::op_store::ReconcileOpHeadsResponse, tonic::Status> {
    reconcile_repo_op_heads_proto_with_policy(store, repo_id, None).await
}

/// Reconciles operation heads for `repo_id` while enforcing an optional per-repository
/// [`ReconcileKillSwitch`] policy.
pub async fn reconcile_repo_op_heads_proto_with_policy(
    store: Arc<CompositeStore>,
    repo_id: &str,
    kill_switch: Option<&ReconcileKillSwitch>,
) -> Result<cc_common::op_store::ReconcileOpHeadsResponse, tonic::Status> {
    reconcile_repo_op_heads_with_policy_and_limits(
        store,
        repo_id,
        kill_switch,
        IndexWalkLimits::default(),
        None,
    )
    .await
}

#[cfg(test)]
pub(crate) async fn reconcile_repo_op_heads_with_limits(
    store: Arc<CompositeStore>,
    repo_id: &str,
    walk_limits: IndexWalkLimits,
) -> Result<cc_common::op_store::ReconcileOpHeadsResponse, tonic::Status> {
    reconcile_repo_op_heads_with_policy_and_limits(store, repo_id, None, walk_limits, None).await
}

async fn reconcile_repo_op_heads_with_policy_and_limits(
    store: Arc<CompositeStore>,
    repo_id: &str,
    kill_switch: Option<&ReconcileKillSwitch>,
    walk_limits: IndexWalkLimits,
    metrics: Option<&crate::metrics::ServerMetrics>,
) -> Result<cc_common::op_store::ReconcileOpHeadsResponse, tonic::Status> {
    const MAX_RETRIES: usize = 6;
    const BASE_BACKOFF_MS: u64 = 20;
    const MAX_JITTER_MS: u64 = 15;

    // Shed load before touching MetadataStore when a thrashing repository is quarantined.
    if kill_switch.is_some_and(|ks| ks.mode_for(repo_id) == ReconcileRepoMode::Quarantine) {
        tracing::warn!(
            repo_id = %repo_id,
            "Server op-head reconciliation quarantined by per-repo kill switch"
        );
        return Err(quarantine_status(repo_id));
    }

    if crate::resilience::remaining_rpc_deadline().is_some_and(|rem| rem.is_zero()) {
        return Err(StoreError::DeadlineExceeded(
            "gRPC deadline expired before op-head reconciliation".to_string(),
        )
        .into());
    }
    store
        .ensure_repo_registered(repo_id, "reconciling op heads")
        .await?;

    // Fast path when heads are already converged.
    let initial_heads = store.get_op_heads(repo_id).await?;
    if initial_heads.len() <= 1 {
        return single_head_response(repo_id, initial_heads);
    }
    if let Some(ks) = kill_switch {
        match ks.mode_for(repo_id) {
            ReconcileRepoMode::Quarantine => {
                tracing::warn!(
                    repo_id = %repo_id,
                    "Server op-head reconciliation quarantined by per-repo kill switch"
                );
                return Err(quarantine_status(repo_id));
            }
            ReconcileRepoMode::Bypass => {
                return bypass_response(repo_id, initial_heads);
            }
            ReconcileRepoMode::Normal => {}
        }
    }

    // Coalesce concurrent in-process reconcile calls for the same `repo_id`.
    let repo_lock = repo_reconcile_mutex(repo_id);
    let _guard = match crate::resilience::remaining_rpc_deadline() {
        Some(rem) if rem.is_zero() => {
            return Err(StoreError::DeadlineExceeded(
                "gRPC deadline expired waiting for reconciliation lock".to_string(),
            )
            .into());
        }
        Some(rem) => match tokio::time::timeout(rem, repo_lock.lock()).await {
            Ok(guard) => guard,
            Err(_) => {
                return Err(StoreError::DeadlineExceeded(
                    "gRPC deadline exceeded waiting for reconciliation lock".to_string(),
                )
                .into());
            }
        },
        None => repo_lock.lock().await,
    };

    let mut attempt = 0usize;
    loop {
        if kill_switch.is_some_and(|ks| ks.mode_for(repo_id) == ReconcileRepoMode::Quarantine) {
            tracing::warn!(
                repo_id = %repo_id,
                attempt = attempt + 1,
                "Server op-head reconciliation quarantined by per-repo kill switch"
            );
            return Err(quarantine_status(repo_id));
        }

        if crate::resilience::remaining_rpc_deadline().is_some_and(|rem| rem.is_zero()) {
            return Err(StoreError::DeadlineExceeded(
                "gRPC deadline expired before op-head reconciliation attempt".to_string(),
            )
            .into());
        }
        let project_id = store
            .resolve_repo_project(repo_id, "reconciling op heads")
            .await?;
        let heads = store.get_op_heads(repo_id).await?;
        if heads.len() <= 1 {
            return single_head_response(repo_id, heads);
        }
        if let Some(ks) = kill_switch {
            match ks.mode_for(repo_id) {
                ReconcileRepoMode::Quarantine => {
                    tracing::warn!(
                        repo_id = %repo_id,
                        attempt = attempt + 1,
                        "Server op-head reconciliation quarantined by per-repo kill switch"
                    );
                    return Err(quarantine_status(repo_id));
                }
                ReconcileRepoMode::Bypass => {
                    return bypass_response(repo_id, heads);
                }
                ReconcileRepoMode::Normal => {}
            }
        }
        let heads_snapshot = heads.clone();

        let store_clone = store.clone();
        let repo_id_owned = repo_id.to_string();
        let project_id_owned = project_id.clone();
        // Propagate the caller's `RPC_DEADLINE` and `RPC_METRICS` across `spawn_blocking` so
        // Spanner/PostgreSQL/SQLite calls inside `ServerOpStore`/`ServerBackend`/`ServerOpHeadsStore`
        // inherit them.
        let rpc_deadline = crate::resilience::current_rpc_deadline();
        let rpc_metrics = metrics
            .cloned()
            .or_else(crate::resilience::current_rpc_metrics);

        let handle = tokio::task::spawn_blocking(move || {
            pollster::block_on(crate::resilience::with_rpc_context(
                rpc_deadline,
                rpc_metrics,
                async move {
                    let work = async move {
                        let op_store: Arc<dyn OpStore> = Arc::new(ServerOpStore::with_project_id(
                            store_clone.clone(),
                            repo_id_owned.clone(),
                            project_id_owned.clone(),
                        ));
                        let walk_budget = IndexWalkBudget::new(walk_limits);
                        let index = ServerIndex::with_project_id_and_budget(
                            store_clone.clone(),
                            repo_id_owned.clone(),
                            project_id_owned.clone(),
                            walk_budget.clone(),
                        );
                        if requires_client_rewrite_resolution(
                            &store_clone,
                            &repo_id_owned,
                            &project_id_owned,
                            &op_store,
                            &index,
                            &heads,
                        )
                        .await?
                        {
                            return Ok(None);
                        }

                        let (ref user_settings, ref merge_options) = *RECONCILER_ENV;
                        let signer = signer_from_settings(user_settings).map_err(|e| {
                            tracing::error!(
                                error = %e,
                                "Failed to initialize op-head reconciliation signer"
                            );
                            ReconcileError {
                                status: tonic::Status::internal("internal server error"),
                                transient: false,
                            }
                        })?;
                        let index_store = Arc::new(ServerIndexStore::with_project_id_and_budget(
                            store_clone.clone(),
                            repo_id_owned.clone(),
                            project_id_owned.clone(),
                            walk_budget,
                        ));
                        let submodule_store = Arc::new(ServerSubmoduleStore);

                        let server_backend = Box::new(ServerBackend::with_project_id(
                            store_clone.clone(),
                            repo_id_owned.clone(),
                            project_id_owned.clone(),
                        ));
                        let jj_store = JjStore::new(server_backend, signer, merge_options.clone());
                        // Pin both `project_id` and the validated `heads` snapshot so
                        // `RepoLoader::load_at_head()` merges strictly within `project_id` and
                        // atomically CAS-swaps the full snapshot via
                        // `compare_and_swap_op_heads_for_project`.
                        let op_heads_store: Arc<dyn OpHeadsStore> =
                            Arc::new(ServerOpHeadsStore::with_expected_heads_for_project(
                                store_clone,
                                repo_id_owned,
                                project_id_owned,
                                heads,
                            ));

                        let repo_loader = RepoLoader::new(
                            user_settings.clone(),
                            jj_store,
                            Arc::new(ServerWorkspaceStore),
                            op_store,
                            op_heads_store,
                            index_store,
                            submodule_store,
                        );

                        match repo_loader.load_at_head(None).await {
                            Ok(repo) => Ok(Some(repo.op_id().as_bytes().to_vec())),
                            Err(err) if is_walk_budget_exceeded_in_repo_loader(&err) => Ok(None),
                            Err(err) => Err(ReconcileError::from_repo_loader(err)),
                        }
                    };

                    // Bound the worker future itself by the active RPC deadline so CPU-heavy or
                    // Memory/SQLite walks also abort promptly when the caller's budget elapses.
                    match crate::resilience::remaining_rpc_deadline() {
                        Some(rem) if rem.is_zero() => {
                            Err(ReconcileError::from_store(StoreError::DeadlineExceeded(
                                "gRPC deadline expired before op-head reconciliation worker"
                                    .to_string(),
                            )))
                        }
                        Some(rem) => match tokio::time::timeout(rem, work).await {
                            Ok(res) => res,
                            Err(_) => {
                                Err(ReconcileError::from_store(StoreError::DeadlineExceeded(
                                    "gRPC deadline exceeded during op-head reconciliation"
                                        .to_string(),
                                )))
                            }
                        },
                        None => work.await,
                    }
                },
            ))
        });

        let reconcile_result = run_reconcile_join_with_metrics(handle, metrics).await;

        match reconcile_result {
            Ok(Some(op_id)) => {
                return Ok(cc_common::op_store::ReconcileOpHeadsResponse {
                    current_op_head_ids: vec![op_id.clone()],
                    op_head: op_id,
                    client_resolution_required: false,
                });
            }
            Ok(None) => {
                if kill_switch
                    .is_some_and(|ks| ks.mode_for(repo_id) == ReconcileRepoMode::Quarantine)
                {
                    tracing::warn!(
                        repo_id = %repo_id,
                        attempt = attempt + 1,
                        "Server op-head reconciliation quarantined by per-repo kill switch"
                    );
                    return Err(quarantine_status(repo_id));
                }
                return Ok(cc_common::op_store::ReconcileOpHeadsResponse {
                    op_head: Vec::new(),
                    client_resolution_required: true,
                    current_op_head_ids: heads_snapshot,
                });
            }
            Err(err) => {
                if let Some(ks) = kill_switch {
                    match ks.mode_for(repo_id) {
                        ReconcileRepoMode::Quarantine => {
                            tracing::warn!(
                                repo_id = %repo_id,
                                attempt = attempt + 1,
                                "Server op-head reconciliation quarantined by per-repo kill switch"
                            );
                            return Err(quarantine_status(repo_id));
                        }
                        ReconcileRepoMode::Bypass if err.transient => {
                            let heads = store.get_op_heads(repo_id).await?;
                            if ks.mode_for(repo_id) == ReconcileRepoMode::Quarantine {
                                tracing::warn!(
                                    repo_id = %repo_id,
                                    attempt = attempt + 1,
                                    "Server op-head reconciliation quarantined by per-repo kill switch"
                                );
                                return Err(quarantine_status(repo_id));
                            }
                            if heads.len() <= 1 {
                                return single_head_response(repo_id, heads);
                            }
                            return bypass_response(repo_id, heads);
                        }
                        ReconcileRepoMode::Bypass | ReconcileRepoMode::Normal => {}
                    }
                }
                if attempt + 1 < MAX_RETRIES && err.transient {
                    let jitter = rand::random::<u64>() % MAX_JITTER_MS;
                    let sleep_ms = (BASE_BACKOFF_MS << attempt.min(5)) + jitter;
                    let sleep_dur = Duration::from_millis(sleep_ms);
                    if let Some(rem) = crate::resilience::remaining_rpc_deadline()
                        && rem <= sleep_dur
                    {
                        return Err(StoreError::DeadlineExceeded(
                            "gRPC deadline insufficient for op-head reconciliation retry backoff"
                                .to_string(),
                        )
                        .into());
                    }
                    tracing::warn!(
                        repo_id = %repo_id,
                        attempt = attempt + 1,
                        error = %err.status,
                        "Transient op-head reconciliation failure; retrying"
                    );
                    tokio::time::sleep(sleep_dur).await;
                    attempt += 1;
                } else {
                    return Err(err.status);
                }
            }
        }
    }
}

async fn run_reconcile_join_with_metrics<T>(
    handle: tokio::task::JoinHandle<Result<T, ReconcileError>>,
    metrics: Option<&crate::metrics::ServerMetrics>,
) -> Result<T, ReconcileError> {
    match handle.await {
        Ok(res) => res,
        Err(join_err) => {
            let is_panic =
                crate::resilience::handle_join_error("reconciliation worker", join_err, metrics);
            Err(ReconcileError {
                transient: !is_panic,
                status: tonic::Status::internal("reconciliation worker failed"),
            })
        }
    }
}

/// Walks commits reachable from `wanted_heads` that are not ancestors of `unwanted_heads`,
/// returning `Ok(Some(exclusive_map))` or `Ok(None)` if `MAX_COMMIT_WALK_VISITED` is exceeded.
async fn collect_exclusive_commits(
    store: &CompositeStore,
    project_id: &str,
    index: &ServerIndex,
    wanted_heads: &HashSet<CommitId>,
    unwanted_heads: &HashSet<CommitId>,
) -> Result<Option<HashMap<CommitId, Vec<CommitId>>>, ReconcileError> {
    // Seed only heads absent from `unwanted_heads` so repositories with thousands of unchanged
    // heads neither exhaust `MAX_COMMIT_WALK_VISITED` nor incur unnecessary commit fetches.
    let mut stack: Vec<CommitId> = wanted_heads
        .difference(unwanted_heads)
        .filter(|cid| cid.as_bytes() != cc_common::ROOT_COMMIT_ID_BYTES)
        .cloned()
        .collect();
    // Probe newly added heads (`unwanted_heads \ wanted_heads`) before unchanged heads so
    // fast-forward advancements match in O(1) while still handling unnormalized views whose
    // interior ancestors sit below an unchanged head.
    let newly_added_unwanted: Vec<&CommitId> = unwanted_heads.difference(wanted_heads).collect();
    let unchanged_unwanted: Vec<&CommitId> = unwanted_heads.intersection(wanted_heads).collect();
    let mut visited = HashSet::new();
    let mut exclusive = HashMap::new();
    while let Some(cid) = stack.pop() {
        if crate::resilience::remaining_rpc_deadline().is_some_and(|r| r.is_zero()) {
            return Err(ReconcileError::from_store(StoreError::DeadlineExceeded(
                "gRPC deadline expired during op-head commit ancestry walk".to_string(),
            )));
        }
        if !visited.insert(cid.clone()) {
            continue;
        }
        if visited.len() > MAX_COMMIT_WALK_VISITED
            || index.walk_budget().check_walk_count(visited.len()).is_err()
            || index.walk_budget().record_aggregate_visit().is_err()
        {
            return Ok(None);
        }
        let mut in_unwanted = false;
        for u in newly_added_unwanted.iter().chain(unchanged_unwanted.iter()) {
            if crate::resilience::remaining_rpc_deadline().is_some_and(|r| r.is_zero()) {
                return Err(ReconcileError::from_store(StoreError::DeadlineExceeded(
                    "gRPC deadline expired during op-head commit ancestry walk".to_string(),
                )));
            }
            match index.is_ancestor(&cid, u).await {
                Ok(true) => {
                    in_unwanted = true;
                    break;
                }
                Ok(false) => {}
                Err(err) if is_walk_budget_exceeded_in_index(&err) => {
                    return Ok(None);
                }
                Err(err) => return Err(ReconcileError::from_index(err)),
            }
        }
        if in_unwanted {
            continue;
        }
        let proto_commit = store
            .get_commit(project_id, cid.as_bytes())
            .await
            .map_err(ReconcileError::from_store)?;
        let mut parents = Vec::with_capacity(proto_commit.parent_commit_ids.len());
        for p in &proto_commit.parent_commit_ids {
            if p.len() != cc_common::COMMIT_ID_LENGTH {
                return Err(ReconcileError::from_store(StoreError::Corruption(format!(
                    "stored commit {} parent_commit_ids entry must be {} bytes (got {} bytes)",
                    cid.hex(),
                    cc_common::COMMIT_ID_LENGTH,
                    p.len()
                ))));
            }
            parents.push(CommitId::from_bytes(p));
        }
        for p in &parents {
            if p.as_bytes() != cc_common::ROOT_COMMIT_ID_BYTES
                && !unwanted_heads.contains(p)
                && !visited.contains(p)
            {
                stack.push(p.clone());
            }
        }
        exclusive.insert(cid, parents);
    }
    Ok(Some(exclusive))
}

/// Returns `true` when divergent operation branches contain commit rewrites, abandonments,
/// missing `commit_predecessors` metadata, or walks exceeding safety caps that require
/// `DefaultIndexStore` descendant rebasing on the client.
async fn requires_client_rewrite_resolution(
    store: &CompositeStore,
    repo_id: &str,
    project_id: &str,
    op_store: &Arc<dyn OpStore>,
    index: &ServerIndex,
    raw_head_ids: &[Vec<u8>],
) -> Result<bool, ReconcileError> {
    let mut head_ops = Vec::with_capacity(raw_head_ids.len());
    for raw in raw_head_ids {
        if raw.len() != cc_common::OPERATION_ID_LENGTH {
            return Err(ReconcileError::from_store(StoreError::Corruption(format!(
                "stored op_head in repo '{repo_id}' must be {} bytes (got {} bytes)",
                cc_common::OPERATION_ID_LENGTH,
                raw.len()
            ))));
        }
        let id = OperationId::from_bytes(raw);
        let data = op_store
            .read_operation(&id)
            .await
            .map_err(ReconcileError::from_op_store)?;
        head_ops.push(JjOperation::new(op_store.clone(), id, data));
    }

    let filtered = jj_lib::dag_walk_async::heads(
        head_ops,
        |op: &JjOperation| op.id().clone(),
        async |op: &JjOperation| op.parents().await,
    )
    .await
    .map_err(ReconcileError::from_op_store)?;
    let filtered_vec: Vec<JjOperation> = filtered.into_iter().collect();
    if filtered_vec.len() <= 1 {
        return Ok(false);
    }
    if filtered_vec.len() > MAX_DIVERGENT_OP_HEADS {
        return Ok(true);
    }

    let mut head_views = Vec::with_capacity(filtered_vec.len());
    for h in &filtered_vec {
        head_views.push(h.view().await.map_err(ReconcileError::from_op_store)?);
    }

    for i in 0..filtered_vec.len() {
        for j in 0..filtered_vec.len() {
            if i == j {
                continue;
            }
            if crate::resilience::remaining_rpc_deadline().is_some_and(|r| r.is_zero()) {
                return Err(ReconcileError::from_store(StoreError::DeadlineExceeded(
                    "gRPC deadline expired during op-head rewrite analysis".to_string(),
                )));
            }

            // Evaluate operations exclusive to branch `i` relative to branch `j` (`::H_i ~ ::H_j`)
            // so intermediate ancestors shared by `(H_i, H_j)` in 3+ head merges are never masked.
            let mut exclusive_ops = Vec::new();
            let op_stream = jj_lib::op_walk::walk_ancestors_range(
                std::slice::from_ref(&filtered_vec[i]),
                std::slice::from_ref(&filtered_vec[j]),
            );
            let mut op_stream = std::pin::pin!(op_stream);
            while let Some(op) = op_stream
                .try_next()
                .await
                .map_err(ReconcileError::from_op_store)?
            {
                if crate::resilience::remaining_rpc_deadline().is_some_and(|r| r.is_zero()) {
                    return Err(ReconcileError::from_store(StoreError::DeadlineExceeded(
                        "gRPC deadline expired during op-head operation walk".to_string(),
                    )));
                }
                exclusive_ops.push(op);
                if exclusive_ops.len() > MAX_OP_WALK_VISITED {
                    return Ok(true);
                }
            }

            let mut created_in_branch = HashSet::new();
            let mut affected_preds = HashSet::new();
            for op in &exclusive_ops {
                // Fail closed when `commit_predecessors` metadata was not recorded.
                let Some(cp_map) = &op.store_operation().commit_predecessors else {
                    return Ok(true);
                };
                // `jj_lib 0.47` records empty `predecessor_ids` for newly created commits; only
                // non-empty predecessor lists denote commit rewrites.
                let has_rewrites = cp_map.values().any(|preds| !preds.is_empty());
                for (new_cid, preds) in cp_map {
                    created_in_branch.insert(new_cid.clone());
                    if has_rewrites {
                        for p in preds {
                            affected_preds.insert(p.clone());
                        }
                    }
                }
            }
            affected_preds.retain(|p| !created_in_branch.contains(p));

            let pair_base_ops = jj_lib::op_walk::closest_common_ancestors(
                vec![filtered_vec[i].clone()],
                std::iter::once(filtered_vec[j].clone()),
            )
            .await
            .map_err(ReconcileError::from_op_store)?;
            let mut pair_base_views = Vec::with_capacity(pair_base_ops.len());
            for b in &pair_base_ops {
                pair_base_views.push(b.view().await.map_err(ReconcileError::from_op_store)?);
            }

            // Also detect commits abandoned on branch `i` (`jj abandon` records an empty
            // `commit_predecessors` map or new working-copy commits with empty predecessors): any
            // commit reachable from `bv.heads()` that is no longer reachable from
            // `head_views[i].heads()` was rewritten or abandoned.
            for bv in &pair_base_views {
                let Some(removed) = collect_exclusive_commits(
                    store,
                    project_id,
                    index,
                    bv.heads(),
                    head_views[i].heads(),
                )
                .await?
                else {
                    return Ok(true);
                };
                affected_preds.extend(removed.into_keys());
            }

            if affected_preds.is_empty() {
                continue;
            }

            let other_view = &head_views[j];
            for bv in &pair_base_views {
                let Some(added_in_j) = collect_exclusive_commits(
                    store,
                    project_id,
                    index,
                    other_view.heads(),
                    bv.heads(),
                )
                .await?
                else {
                    return Ok(true);
                };
                if added_in_j
                    .values()
                    .any(|parents| parents.iter().any(|p| affected_preds.contains(p)))
                {
                    return Ok(true);
                }
                for pred in &affected_preds {
                    if bv.heads().contains(pred) && !other_view.heads().contains(pred) {
                        return Ok(true);
                    }
                }
            }

            for pred in &affected_preds {
                if other_view.local_bookmarks().any(|(_, target)| {
                    target.present_adds().any(|id| id == pred)
                        || target.present_removes().any(|id| id == pred)
                }) {
                    return Ok(true);
                }
                if other_view.all_remote_bookmarks().any(|(_, remote_ref)| {
                    remote_ref.target.present_adds().any(|id| id == pred)
                        || remote_ref.target.present_removes().any(|id| id == pred)
                }) {
                    return Ok(true);
                }
                for (ws_name, wc_id) in other_view.wc_commit_ids() {
                    if wc_id == pred
                        && (!pair_base_views
                            .iter()
                            .all(|bv| bv.wc_commit_ids().get(ws_name) == Some(pred))
                            || head_views[i].wc_commit_ids().get(ws_name) == Some(pred))
                    {
                        return Ok(true);
                    }
                }
            }
        }
    }

    Ok(false)
}
