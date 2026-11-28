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

//! Cloud Spanner and Spanner Omni `MetadataStore` implementation.
//!
//! # Authority and Crash-Safety Ordering
//! `SpannerMetadataStore` shares one `Spanner` client and multiplexed `DatabaseClient`.
//! Content-addressed writes (`Commits`, `Trees` + `TreeEntries`, `Symlinks`, `Views`,
//! `Operations`, `Blobs` + `ProjectBlobs`) commit atomically after external `BlobStore`
//! uploads and batched dependency checks. `Commits` rows use insert-only mutations.
//!
//! # Concurrency and Merge Invariants
//! Point reads, `UNNEST` dependency checks, and prefix scans use lock-free `single_use()`
//! reads. All mutable transitions (`register_project`, `register_repo`, `*_for_project`,
//! `put_blob_record`, `update_op_heads`, `publish_operation_atomic`, `compare_and_swap_op_heads`,
//! `put_workspace_cas`, `delete_workspace`, `delete_repository`) run in `Serializable`
//! `ReadWriteTransaction`s verifying `Repositories.ProjectId` atomically. `delete_repository`
//! removes `Workspaces`, `Operations`, `Views`, and `Repositories` atomically when within
//! Spanner's mutation budget and drains in bounded batches for very large repositories.
//!
//! # Cross-Process and RPC Boundaries
//! Supports Cloud Spanner via ADC, Spanner Omni via custom endpoints and CA/mTLS, and the
//! Cloud Spanner Emulator (`--spanner-emulator` or `SPANNER_EMULATOR_HOST`).

mod codec;
mod objects;
mod ops;
mod repo;
mod schema;
mod workspaces;

#[cfg(test)]
mod tests;

pub use repo::{MAX_DELETE_BATCH_ITERATIONS, SPANNER_DELETE_BATCH_LIMIT};

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use cc_common::backend::{Commit, TreeEntry};
use cc_common::op_store::{Operation, View};
use cc_common::workspace::WorkspaceState;
use google_cloud_spanner::builder::TransactionRunnerBuilder;
use google_cloud_spanner::channel_pool::StaticChannelPoolConfig;
use google_cloud_spanner::client::{DatabaseClient, Spanner, SpannerBuilderExt};
use google_cloud_spanner::model::transaction_options::IsolationLevel;
use google_cloud_spanner::omni::{InstanceType, TlsConfig};
use google_cloud_spanner::transaction::ReadWriteTransaction;

use super::{BlobRecord, CommitId, MetadataStore, OpId, StoreError, StoreResult};
use repo::{clamp_delete_batch_limit, clamp_max_delete_batch_iterations};
use schema::ensure_database_and_schema;

const UNNEST_BATCH_SIZE: usize = 5_000;

const RW_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, Default)]
pub struct SpannerConnectConfig {
    /// Full database resource path: `projects/<project>/instances/<instance>/databases/<db>`.
    pub database: String,
    /// Optional gRPC endpoint override (e.g. `http://127.0.0.1:9010` or Spanner Omni URL).
    pub endpoint: Option<String>,
    /// Optional HTTP/REST admin endpoint override for emulator DDL administration.
    pub admin_endpoint: Option<String>,
    /// Connect in Spanner Omni mode (`InstanceType::Omni`).
    pub omni: bool,
    /// Optional Root CA PEM path for Spanner Omni TLS verification.
    pub omni_ca_cert: Option<PathBuf>,
    /// Optional client certificate PEM path for Spanner Omni mTLS.
    pub omni_client_cert: Option<PathBuf>,
    /// Optional client private key PEM path for Spanner Omni mTLS.
    pub omni_client_key: Option<PathBuf>,
    /// Create the database if missing on the instance and apply `spanner_schema.sql`.
    pub auto_migrate: bool,
    /// Explicitly target a local Cloud Spanner Emulator (permits plaintext HTTP/1.0 REST DDL
    /// administration without requiring process-global `SPANNER_EMULATOR_HOST`).
    pub emulator: bool,
}

struct TxRetryTracker {
    attempts: AtomicUsize,
    counter: Option<Arc<AtomicU64>>,
}

impl TxRetryTracker {
    fn record_attempt(&self) {
        if self.attempts.fetch_add(1, Ordering::Relaxed) > 0
            && let Some(counter) = &self.counter
        {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[derive(Clone)]
pub struct SpannerMetadataStore {
    _spanner: Spanner,
    db_client: DatabaseClient,
    tx_retries: Option<Arc<AtomicU64>>,
    delete_batch_limit: i64,
    max_delete_batch_iterations: usize,
}

impl SpannerMetadataStore {
    pub async fn connect(config: &SpannerConnectConfig) -> StoreResult<Self> {
        let _ = split_database_resource(&config.database)?;
        let mut builder = Spanner::builder();

        let has_tls_certs = config.omni_ca_cert.is_some()
            || config.omni_client_cert.is_some()
            || config.omni_client_key.is_some();
        let tls_or_omni = config.omni || has_tls_certs;
        let emulator_host_env = std::env::var("SPANNER_EMULATOR_HOST").ok();

        if let Some(normalized) = resolve_data_endpoint(config, emulator_host_env.as_deref())? {
            if normalized.starts_with("http://") {
                builder = builder.with_channel_pool(StaticChannelPoolConfig::new(1));
            }
            builder = builder.with_endpoint(normalized);
        }

        if tls_or_omni {
            builder = builder.with_instance_type(InstanceType::Omni);
            if has_tls_certs {
                let mut tls = TlsConfig::new();
                if let Some(ca_path) = &config.omni_ca_cert {
                    tls = tls.with_root_certificate_file(ca_path).map_err(|e| {
                        StoreError::InvalidArgument(format!(
                            "failed to load Spanner Omni CA certificate {}: {e}",
                            ca_path.display()
                        ))
                    })?;
                }
                match (&config.omni_client_cert, &config.omni_client_key) {
                    (Some(cert), Some(key)) => {
                        tls = tls.with_client_certificate_file(cert, key).map_err(|e| {
                            StoreError::InvalidArgument(format!(
                                "failed to load Spanner Omni mTLS identity: {e}"
                            ))
                        })?;
                    }
                    (Some(_), None) | (None, Some(_)) => {
                        return Err(StoreError::InvalidArgument(
                            "both --spanner-omni-client-cert and --spanner-omni-client-key must be set together"
                                .to_string(),
                        ));
                    }
                    (None, None) => {}
                }
                builder = builder.with_omni_tls(tls);
            }
        }

        let spanner = builder
            .build()
            .await
            .map_err(|e| StoreError::Unavailable(format!("failed to build Spanner client: {e}")))?;

        if config.auto_migrate {
            ensure_database_and_schema(&spanner, config).await?;
        }

        let db_client = spanner
            .database_client(&config.database)
            .build()
            .await
            .map_err(|e| {
                StoreError::Unavailable(format!(
                    "failed to initialize Spanner DatabaseClient for '{}': {e}",
                    config.database
                ))
            })?;

        Ok(Self {
            _spanner: spanner,
            db_client,
            tx_retries: None,
            delete_batch_limit: SPANNER_DELETE_BATCH_LIMIT,
            max_delete_batch_iterations: MAX_DELETE_BATCH_ITERATIONS,
        })
    }

    pub fn with_tx_retries_counter(mut self, counter: Arc<AtomicU64>) -> Self {
        self.tx_retries = Some(counter);
        self
    }

    /// Overrides the per-transaction child-row delete batch cap (clamped to
    /// `1..=SPANNER_DELETE_BATCH_LIMIT`), primarily for multi-batch integration tests.
    pub fn with_delete_batch_limit(mut self, limit: i64) -> Self {
        self.delete_batch_limit = clamp_delete_batch_limit(limit);
        self
    }

    /// Overrides the maximum delete-batch transactions per `delete_repository` call (clamped to
    /// `1..=MAX_DELETE_BATCH_ITERATIONS`), primarily for integration tests.
    pub fn with_max_delete_batch_iterations(mut self, limit: usize) -> Self {
        self.max_delete_batch_iterations = clamp_max_delete_batch_iterations(limit);
        self
    }

    fn new_retry_tracker(&self) -> TxRetryTracker {
        TxRetryTracker {
            attempts: AtomicUsize::new(0),
            counter: self.tx_retries.clone(),
        }
    }

    fn rw_transaction(&self) -> TransactionRunnerBuilder {
        self.db_client
            .read_write_transaction()
            .set_isolation_level(IsolationLevel::Serializable)
            .with_transaction_timeout(RW_TRANSACTION_TIMEOUT)
    }

    /// Runs a serializable read-write transaction where conflict or no-op branches can return
    /// [`TxAction::Rollback`] to roll back without issuing an empty `Commit` RPC.
    async fn run_rw_tx<T, F>(
        &self,
        mut work: F,
    ) -> Result<StoreResult<T>, google_cloud_spanner::Error>
    where
        F: AsyncFnMut(ReadWriteTransaction) -> google_cloud_spanner::Result<TxAction<T>>,
    {
        let early_exit: Arc<std::sync::Mutex<Option<StoreResult<T>>>> =
            Arc::new(std::sync::Mutex::new(None));
        let early_exit_cb = Arc::clone(&early_exit);
        let retry_tracker = self.new_retry_tracker();
        let runner = self.rw_transaction().build();
        let res = runner
            .run(async move |tx| {
                retry_tracker.record_attempt();
                *early_exit_cb.lock().unwrap_or_else(|e| e.into_inner()) = None;
                match work(tx).await? {
                    TxAction::Commit(val) => Ok(val),
                    TxAction::Rollback(outcome) => {
                        *early_exit_cb.lock().unwrap_or_else(|e| e.into_inner()) = Some(outcome);
                        // Non-status `deser` error triggers `transaction.rollback()` without
                        // matching `is_aborted` / emulator retry checks.
                        Err(google_cloud_spanner::Error::deser(TxRollbackSentinel))
                    }
                }
            })
            .await;
        match res {
            Ok(committed) => Ok(Ok(committed.into_inner())),
            Err(err) => {
                if let Some(outcome) = early_exit.lock().unwrap_or_else(|e| e.into_inner()).take() {
                    Ok(outcome)
                } else {
                    Err(err)
                }
            }
        }
    }
}

#[derive(Debug)]
struct TxRollbackSentinel;

impl std::fmt::Display for TxRollbackSentinel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Spanner read-write transaction rolled back without commit")
    }
}

impl std::error::Error for TxRollbackSentinel {}

enum TxAction<T> {
    Commit(T),
    Rollback(StoreResult<T>),
}

pub(super) fn resolve_data_endpoint(
    config: &SpannerConnectConfig,
    emulator_host_env: Option<&str>,
) -> StoreResult<Option<String>> {
    let Some(ep) = config
        .endpoint
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    else {
        return Ok(None);
    };

    let has_tls_certs = config.omni_ca_cert.is_some()
        || config.omni_client_cert.is_some()
        || config.omni_client_key.is_some();
    let tls_or_omni = config.omni || has_tls_certs;
    let has_emulator_env = emulator_host_env
        .map(str::trim)
        .is_some_and(|s| !s.is_empty());
    let is_emulator = !tls_or_omni && (config.emulator || has_emulator_env);

    let normalized = normalize_endpoint(ep, !is_emulator)?;
    if normalized.starts_with("http://") {
        if tls_or_omni {
            return Err(StoreError::InvalidArgument(
                "plaintext http:// Spanner endpoint cannot be used with Spanner Omni or TLS certificates"
                    .to_string(),
            ));
        }
        if !is_emulator {
            return Err(StoreError::InvalidArgument(
                "plaintext http:// Spanner endpoint is only permitted when targeting the Spanner emulator (--spanner-emulator or SPANNER_EMULATOR_HOST)"
                    .to_string(),
            ));
        }
    }
    Ok(Some(normalized))
}

fn normalize_endpoint(ep: &str, default_https: bool) -> StoreResult<String> {
    let trimmed = ep.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Err(StoreError::InvalidArgument(
            "Spanner endpoint must not be empty".to_string(),
        ));
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return Ok(trimmed.to_string());
    }
    if trimmed.contains("://") {
        return Err(StoreError::InvalidArgument(format!(
            "unsupported Spanner endpoint scheme in '{trimmed}' (expected http:// or https://)"
        )));
    }
    let scheme = if default_https { "https" } else { "http" };
    Ok(format!("{scheme}://{trimmed}"))
}

fn is_valid_spanner_resource_segment(segment: &str) -> bool {
    !segment.is_empty()
        && segment.len() <= 128
        && segment
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

fn split_database_resource(database: &str) -> StoreResult<(&str, &str)> {
    let mut parts = database.split('/');
    let valid = matches!(
        (
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
        ),
        (
            Some("projects"),
            Some(project_id),
            Some("instances"),
            Some(instance_id),
            Some("databases"),
            Some(db_id),
            None,
        ) if is_valid_spanner_resource_segment(project_id)
            && is_valid_spanner_resource_segment(instance_id)
            && is_valid_spanner_resource_segment(db_id)
    );
    if !valid {
        return Err(StoreError::InvalidArgument(format!(
            "invalid Spanner database path '{database}' (expected projects/<p>/instances/<i>/databases/<d>)"
        )));
    }
    let (instance_path, db_id) = database
        .rsplit_once("/databases/")
        .expect("validated 6-segment database path must contain /databases/");
    Ok((instance_path, db_id))
}

#[async_trait]
impl MetadataStore for SpannerMetadataStore {
    fn backend_name(&self) -> &'static str {
        "spanner"
    }

    async fn register_project(&self, project_id: &str, name: Option<&str>) -> StoreResult<()> {
        self.register_project_impl(project_id, name).await
    }

    async fn is_project_registered(&self, project_id: &str) -> StoreResult<bool> {
        self.is_project_registered_impl(project_id).await
    }

    async fn get_repo_project_id(&self, repo_id: &str) -> StoreResult<Option<String>> {
        self.get_repo_project_id_impl(repo_id).await
    }

    async fn register_repo_in_project(
        &self,
        repo_id: &str,
        project_id: &str,
        name: Option<&str>,
    ) -> StoreResult<String> {
        self.register_repo_in_project_impl(repo_id, project_id, name)
            .await
    }

    async fn delete_repository(
        &self,
        repo_id: &str,
        expected_project_id: Option<&str>,
    ) -> StoreResult<bool> {
        self.delete_repository_with_after_batch_hook(repo_id, expected_project_id, |_| {
            std::future::ready(())
        })
        .await
    }

    async fn get_commit(&self, project_id: &str, commit_id: &[u8]) -> StoreResult<Option<Commit>> {
        self.get_commit_impl(project_id, commit_id).await
    }

    async fn has_commit(&self, project_id: &str, commit_id: &[u8]) -> StoreResult<bool> {
        self.has_commit_impl(project_id, commit_id).await
    }

    async fn get_commit_generation(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> StoreResult<Option<u64>> {
        self.get_commit_generation_impl(project_id, commit_id).await
    }

    async fn get_commit_with_generation(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> StoreResult<Option<(Commit, u64)>> {
        self.get_commit_with_generation_impl(project_id, commit_id)
            .await
    }

    async fn get_commit_generations(
        &self,
        project_id: &str,
        commit_ids: &[Vec<u8>],
    ) -> StoreResult<HashMap<Vec<u8>, u64>> {
        self.get_commit_generations_impl(project_id, commit_ids)
            .await
    }

    async fn put_commit(
        &self,
        project_id: &str,
        commit_id: &[u8],
        commit: &Commit,
        generation: u64,
    ) -> StoreResult<()> {
        self.put_commit_impl(project_id, commit_id, commit, generation)
            .await
    }

    async fn list_commit_ids(
        &self,
        project_id: &str,
        after_commit_id: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<Vec<CommitId>> {
        self.list_commit_ids_impl(project_id, after_commit_id, limit)
            .await
    }

    async fn current_commit_watermark(&self, project_id: &str) -> StoreResult<Vec<u8>> {
        self.current_commit_watermark_impl(project_id).await
    }

    async fn list_commit_ids_since(
        &self,
        project_id: &str,
        since_token: &[u8],
        limit: usize,
    ) -> StoreResult<(Vec<CommitId>, Vec<u8>, bool)> {
        self.list_commit_ids_since_impl(project_id, since_token, limit)
            .await
    }

    async fn resolve_commit_id_prefix(
        &self,
        project_id: &str,
        min_id: &[u8],
        max_id: &[u8],
    ) -> StoreResult<Vec<CommitId>> {
        self.resolve_commit_id_prefix_impl(project_id, min_id, max_id)
            .await
    }

    async fn resolve_change_id_prefix(
        &self,
        project_id: &str,
        min_id: &[u8],
        max_id: &[u8],
        limit: usize,
    ) -> StoreResult<Vec<(Vec<u8>, Vec<CommitId>)>> {
        self.resolve_change_id_prefix_impl(project_id, min_id, max_id, limit)
            .await
    }

    async fn get_tree(
        &self,
        project_id: &str,
        tree_id: &[u8],
    ) -> StoreResult<Option<Vec<TreeEntry>>> {
        self.get_tree_impl(project_id, tree_id).await
    }

    async fn get_tree_page(
        &self,
        project_id: &str,
        tree_id: &[u8],
        after_name: Option<&str>,
        limit: usize,
    ) -> StoreResult<Option<Vec<TreeEntry>>> {
        self.get_tree_page_impl(project_id, tree_id, after_name, limit)
            .await
    }

    async fn put_tree(
        &self,
        project_id: &str,
        tree_id: &[u8],
        entries: &[TreeEntry],
    ) -> StoreResult<()> {
        self.put_tree_impl(project_id, tree_id, entries).await
    }

    async fn find_existing_trees(
        &self,
        project_id: &str,
        tree_ids: &[Vec<u8>],
    ) -> StoreResult<HashSet<Vec<u8>>> {
        self.find_existing_trees_impl(project_id, tree_ids).await
    }

    async fn get_symlink(
        &self,
        project_id: &str,
        symlink_id: &[u8],
    ) -> StoreResult<Option<String>> {
        self.get_symlink_impl(project_id, symlink_id).await
    }

    async fn put_symlink(
        &self,
        project_id: &str,
        symlink_id: &[u8],
        target: &str,
    ) -> StoreResult<()> {
        self.put_symlink_impl(project_id, symlink_id, target).await
    }

    async fn find_existing_symlinks(
        &self,
        project_id: &str,
        symlink_ids: &[Vec<u8>],
    ) -> StoreResult<HashSet<Vec<u8>>> {
        self.find_existing_symlinks_impl(project_id, symlink_ids)
            .await
    }

    async fn get_blob_record(
        &self,
        project_id: &str,
        blob_id: &[u8],
    ) -> StoreResult<Option<BlobRecord>> {
        self.get_blob_record_impl(project_id, blob_id).await
    }

    async fn get_blob_sizes_batch(
        &self,
        project_id: &str,
        blob_ids: &[Vec<u8>],
    ) -> StoreResult<HashMap<Vec<u8>, u64>> {
        self.get_blob_sizes_batch_impl(project_id, blob_ids).await
    }

    async fn get_global_blob_record(&self, blob_id: &[u8]) -> StoreResult<Option<BlobRecord>> {
        self.get_global_blob_record_impl(blob_id).await
    }

    async fn put_blob_record(&self, project_id: &str, record: &BlobRecord) -> StoreResult<()> {
        self.put_blob_record_impl(project_id, record).await
    }

    async fn add_blob_replica_location(&self, blob_id: &[u8], location: &str) -> StoreResult<()> {
        self.add_blob_replica_location_impl(blob_id, location).await
    }

    async fn get_operation(&self, repo_id: &str, op_id: &[u8]) -> StoreResult<Option<Operation>> {
        self.get_operation_impl(repo_id, op_id).await
    }

    async fn put_operation(&self, repo_id: &str, op_id: &[u8], op: &Operation) -> StoreResult<()> {
        self.put_operation_inner(repo_id, None, op_id, op, false)
            .await
    }

    async fn put_operation_for_project(
        &self,
        repo_id: &str,
        expected_project_id: &str,
        op_id: &[u8],
        op: &Operation,
        require_dependencies_exist: bool,
    ) -> StoreResult<()> {
        self.put_operation_inner(
            repo_id,
            Some(expected_project_id),
            op_id,
            op,
            require_dependencies_exist,
        )
        .await
    }

    async fn resolve_operation_id_prefix(
        &self,
        repo_id: &str,
        min_id: &[u8],
        max_id: &[u8],
    ) -> StoreResult<Vec<OpId>> {
        self.resolve_operation_id_prefix_impl(repo_id, min_id, max_id)
            .await
    }

    async fn get_view(&self, repo_id: &str, view_id: &[u8]) -> StoreResult<Option<View>> {
        self.get_view_impl(repo_id, view_id).await
    }

    async fn has_view(&self, repo_id: &str, view_id: &[u8]) -> StoreResult<bool> {
        self.has_view_impl(repo_id, view_id).await
    }

    async fn put_view(&self, repo_id: &str, view_id: &[u8], view: &View) -> StoreResult<()> {
        self.put_view_inner(repo_id, None, view_id, view).await
    }

    async fn put_view_for_project(
        &self,
        repo_id: &str,
        expected_project_id: &str,
        view_id: &[u8],
        view: &View,
    ) -> StoreResult<()> {
        self.put_view_inner(repo_id, Some(expected_project_id), view_id, view)
            .await
    }

    async fn get_op_heads(&self, repo_id: &str) -> StoreResult<Option<Vec<OpId>>> {
        self.get_op_heads_impl(repo_id).await
    }

    async fn update_op_heads(
        &self,
        repo_id: &str,
        old_ids: &[Vec<u8>],
        new_id: &[u8],
    ) -> StoreResult<Vec<OpId>> {
        self.update_op_heads_inner(repo_id, None, old_ids, new_id, false)
            .await
    }

    async fn update_op_heads_for_project(
        &self,
        repo_id: &str,
        expected_project_id: &str,
        old_head_ids: &[Vec<u8>],
        new_head_id: &[u8],
        require_op_exists: bool,
    ) -> StoreResult<Vec<OpId>> {
        self.update_op_heads_inner(
            repo_id,
            Some(expected_project_id),
            old_head_ids,
            new_head_id,
            require_op_exists,
        )
        .await
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
        self.publish_operation_inner(repo_id, None, view_id, view, op_id, op, old_ids, false)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn publish_operation_for_project(
        &self,
        repo_id: &str,
        expected_project_id: &str,
        view_id: &[u8],
        view: &View,
        op_id: &[u8],
        op: &Operation,
        old_op_head_ids: &[Vec<u8>],
        require_dependencies_exist: bool,
    ) -> StoreResult<Vec<OpId>> {
        self.publish_operation_inner(
            repo_id,
            Some(expected_project_id),
            view_id,
            view,
            op_id,
            op,
            old_op_head_ids,
            require_dependencies_exist,
        )
        .await
    }

    async fn compare_and_swap_op_heads(
        &self,
        repo_id: &str,
        expected_old_ids: &[Vec<u8>],
        new_id: &[u8],
    ) -> StoreResult<()> {
        self.compare_and_swap_op_heads_inner(repo_id, None, expected_old_ids, new_id, false)
            .await
    }

    async fn update_op_heads_cas_for_project(
        &self,
        repo_id: &str,
        expected_project_id: &str,
        expected_exact_heads: &[Vec<u8>],
        new_head_id: &[u8],
        require_op_exists: bool,
    ) -> StoreResult<()> {
        self.compare_and_swap_op_heads_inner(
            repo_id,
            Some(expected_project_id),
            expected_exact_heads,
            new_head_id,
            require_op_exists,
        )
        .await
    }

    async fn get_workspace(
        &self,
        repo_id: &str,
        workspace_name: &str,
    ) -> StoreResult<Option<WorkspaceState>> {
        self.get_workspace_impl(repo_id, workspace_name).await
    }

    async fn put_workspace_cas(
        &self,
        workspace: &WorkspaceState,
        expected_operation_id: Option<&[u8]>,
    ) -> StoreResult<()> {
        self.put_workspace_cas_inner(None, workspace, expected_operation_id, false)
            .await
    }

    async fn put_workspace_cas_for_project(
        &self,
        expected_project_id: &str,
        workspace: &WorkspaceState,
        expected_operation_id: Option<&[u8]>,
        require_operation_exists: bool,
    ) -> StoreResult<()> {
        self.put_workspace_cas_inner(
            Some(expected_project_id),
            workspace,
            expected_operation_id,
            require_operation_exists,
        )
        .await
    }

    async fn list_workspaces(&self, repo_id: &str) -> StoreResult<Vec<WorkspaceState>> {
        self.list_workspaces_impl(repo_id).await
    }

    async fn list_workspaces_page(
        &self,
        repo_id: &str,
        after_workspace_name: Option<&str>,
        limit: usize,
        max_bytes: Option<usize>,
    ) -> StoreResult<(Vec<WorkspaceState>, bool)> {
        self.list_workspaces_page_impl(repo_id, after_workspace_name, limit, max_bytes)
            .await
    }

    async fn delete_workspace(&self, repo_id: &str, workspace_name: &str) -> StoreResult<bool> {
        self.delete_workspace_impl(repo_id, workspace_name).await
    }
}

#[allow(dead_code)]
fn _assert_arc_send_sync() {
    fn check<T: Send + Sync>() {}
    check::<Arc<SpannerMetadataStore>>();
}
