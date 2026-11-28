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

//! Native PostgreSQL 17 `MetadataStore` implementation backed by `deadpool-postgres` and
//! `tokio-postgres` with `rustls` TLS support.
//!
//! # Authority and Crash-Safety Ordering
//! `PostgresMetadataStore` persists relational commit, tree, symlink, view, operation, and
//! blob-location rows after `CompositeStore` completes out-of-line `BlobStore` uploads.
//! Wide trees stage `trees(ready = FALSE)` and batch-insert `tree_entries` via `UNNEST`
//! before flipping `ready = TRUE` inside an atomic transaction, and schema auto-migration
//! serializes concurrent replicas under `pg_advisory_xact_lock`.
//!
//! # Concurrency and Merge Invariants
//! Mutable repository transitions lock `repositories` (`FOR NO KEY UPDATE` on op-head and
//! registration transitions, `FOR UPDATE` on `delete_repository`) and retry `SQLSTATE`
//! `40001`/`40P01` with exponential backoff and jitter. Namespace registrations acquire sorted
//! advisory locks to block cross-table shadowing races, while `tree_entries.name` and
//! `workspaces` keys use `COLLATE "C"` for UTF-8 byte order.
//!
//! # Cross-Process and RPC Boundaries
//! Honors PostgreSQL `sslmode` (`disable` uses `NoTls`; `prefer`/`require`/`verify-ca`/
//! `verify-full` follow libpq TLS semantics over `rustls` with `sslrootcert`; explicit
//! `--postgres-ca-cert` fails closed to TLS `verify-full` unless `sslmode=verify-ca`/`require`
//! and rejects `disable`/`prefer` or Unix-domain sockets), redacts credentials from all logs
//! and errors, and maps `SQLSTATE` codes deterministically into [`StoreError`].

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use async_trait::async_trait;
use cc_common::backend::*;
use cc_common::op_store::*;
use cc_common::workspace::WorkspaceState;
use deadpool_postgres::{Manager, ManagerConfig, Object, Pool, RecyclingMethod, Runtime};
use tokio_postgres::NoTls;
use tokio_postgres::config::{SslMode as PgSslMode, SslNegotiation as PgSslNegotiation};
use tracing::info;

use super::{BlobRecord, CommitId, MetadataStore, OpId, StoreError, StoreResult};

mod codec;
mod commits;
mod dsn;
mod objects;
mod ops;
mod pool;
mod repo;
mod schema;
#[cfg(test)]
mod tests;
mod tls;
mod workspaces;

use codec::{map_pg_err_with_context, map_pool_err_with_context};
use dsn::{parse_postgres_tls_options, redact_postgres_url};
use pool::default_pool_timeouts;
use schema::apply_schema;
use tls::{
    ParsedSslMode, ResolvedTlsPolicy, build_encrypt_only_rustls_connector,
    build_verified_rustls_connector, resolve_tls_policy,
};

const DEFAULT_MAX_CONNS: usize = 16;

const TREE_ENTRY_BATCH_SIZE: usize = 1_000;

const UNNEST_BATCH_SIZE: usize = 5_000;

#[derive(Clone)]
pub struct PostgresConnectConfig {
    /// PostgreSQL connection URI (`postgres://...` / `postgresql://...`) or libpq key-value DSN.
    pub url: String,
    /// Maximum number of pooled PostgreSQL connections.
    pub max_conns: usize,
    /// Apply `postgres_schema.sql` idempotently under `pg_advisory_xact_lock` on connect.
    pub auto_migrate: bool,
    /// Optional PEM CA certificate bundle for PostgreSQL TLS verification (overrides `sslrootcert`,
    /// enforces TLS `verify-full` unless overridden by `sslmode=verify-ca`/`require`, and rejects
    /// `sslmode=disable`/`prefer` or Unix-domain socket hosts).
    pub ca_cert: Option<PathBuf>,
    /// Optional counter incremented on every retryable transaction failure (`40001`/`40P01`).
    pub tx_retries: Option<Arc<AtomicU64>>,
}

impl fmt::Debug for PostgresConnectConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresConnectConfig")
            .field("url", &redact_postgres_url(&self.url))
            .field("max_conns", &self.max_conns)
            .field("auto_migrate", &self.auto_migrate)
            .field("ca_cert", &self.ca_cert)
            .field("tx_retries", &self.tx_retries.is_some())
            .finish()
    }
}

impl Default for PostgresConnectConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            max_conns: DEFAULT_MAX_CONNS,
            auto_migrate: false,
            ca_cert: None,
            tx_retries: None,
        }
    }
}

#[derive(Clone)]
pub struct PostgresMetadataStore {
    pool: Pool,
    tx_retries: Option<Arc<AtomicU64>>,
}

impl PostgresMetadataStore {
    pub async fn connect(config: &PostgresConnectConfig) -> StoreResult<Self> {
        let trimmed_url = config.url.trim();
        if trimmed_url.is_empty() {
            return Err(StoreError::InvalidArgument(
                "PostgreSQL connection URL must not be empty".to_string(),
            ));
        }
        if config.max_conns == 0 {
            return Err(StoreError::InvalidArgument(
                "PostgreSQL max_conns must be at least 1".to_string(),
            ));
        }

        let redacted_url = redact_postgres_url(trimmed_url);
        let tls_opts = parse_postgres_tls_options(trimmed_url)?;
        let mut pg_config =
            tokio_postgres::Config::from_str(&tls_opts.normalized_dsn).map_err(|_| {
                StoreError::InvalidArgument(format!(
                    "invalid PostgreSQL connection config '{redacted_url}': invalid connection string"
                ))
            })?;

        // Fail closed if `tokio_postgres::Config::from_str` parsed a different `sslmode`
        // than our TLS pre-parser so an ambiguous URI can never silently downgrade TLS.
        let expected_pg_ssl_mode = match tls_opts.ssl_mode {
            ParsedSslMode::Disable => PgSslMode::Disable,
            ParsedSslMode::Unspecified | ParsedSslMode::Prefer => PgSslMode::Prefer,
            ParsedSslMode::Require | ParsedSslMode::VerifyCa | ParsedSslMode::VerifyFull => {
                PgSslMode::Require
            }
        };
        if pg_config.get_ssl_mode() != expected_pg_ssl_mode {
            return Err(StoreError::InvalidArgument(format!(
                "ambiguous PostgreSQL sslmode configuration in '{redacted_url}'"
            )));
        }

        let tls_policy = resolve_tls_policy(
            pg_config.get_hosts(),
            tls_opts.ssl_mode,
            config.ca_cert.as_deref(),
            tls_opts.ssl_root_cert.as_deref(),
            &redacted_url,
        )?;

        let manager_cfg = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };

        pg_config.ssl_mode(tls_policy.pg_ssl_mode());
        let direct_ssl = pg_config.get_ssl_negotiation() == PgSslNegotiation::Direct;
        let manager = match tls_policy {
            ResolvedTlsPolicy::NoTls => Manager::from_config(pg_config, NoTls, manager_cfg),
            ResolvedTlsPolicy::EncryptOnly { .. } => {
                let tls = build_encrypt_only_rustls_connector(direct_ssl)?;
                Manager::from_config(pg_config, tls, manager_cfg)
            }
            ResolvedTlsPolicy::VerifyCa { ca_source, .. } => {
                let tls = build_verified_rustls_connector(&ca_source, false, direct_ssl)?;
                Manager::from_config(pg_config, tls, manager_cfg)
            }
            ResolvedTlsPolicy::VerifyFull { ca_source } => {
                let tls = build_verified_rustls_connector(&ca_source, true, direct_ssl)?;
                Manager::from_config(pg_config, tls, manager_cfg)
            }
        };

        let pool = Pool::builder(manager)
            .max_size(config.max_conns)
            .timeouts(default_pool_timeouts())
            .runtime(Runtime::Tokio1)
            .build()
            .map_err(|e| {
                StoreError::Unavailable(format!(
                    "failed to build PostgreSQL pool for '{redacted_url}': {e}"
                ))
            })?;

        let mut client = pool.get().await.map_err(|e| {
            map_pool_err_with_context(e, &format!("connecting to PostgreSQL '{redacted_url}'"))
        })?;

        if config.auto_migrate {
            info!("Applying idempotent PostgreSQL schema to {redacted_url}");
            apply_schema(&mut client).await?;
        } else {
            client
                .simple_query("SELECT 1")
                .await
                .map_err(|e| map_pg_err_with_context(e, "verifying PostgreSQL connection"))?;
        }

        Ok(Self {
            pool,
            tx_retries: config.tx_retries.clone(),
        })
    }

    #[doc(hidden)]
    pub async fn checkout_client_for_test(&self) -> StoreResult<Object> {
        self.get_client().await
    }
}

#[allow(dead_code)]
fn _assert_arc_send_sync() {
    fn check<T: Send + Sync>() {}
    check::<Arc<PostgresMetadataStore>>();
}

#[async_trait]
impl MetadataStore for PostgresMetadataStore {
    fn backend_name(&self) -> &'static str {
        "postgres"
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
        self.delete_repository_impl(repo_id, expected_project_id)
            .await
    }

    async fn has_commit(&self, project_id: &str, commit_id: &[u8]) -> StoreResult<bool> {
        self.has_commit_impl(project_id, commit_id).await
    }

    async fn get_commit(&self, project_id: &str, commit_id: &[u8]) -> StoreResult<Option<Commit>> {
        self.get_commit_impl(project_id, commit_id).await
    }

    async fn get_commit_with_generation(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> StoreResult<Option<(Commit, u64)>> {
        self.get_commit_with_generation_impl(project_id, commit_id)
            .await
    }

    async fn get_commit_generation(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> StoreResult<Option<u64>> {
        self.get_commit_generation_impl(project_id, commit_id).await
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

    async fn has_view(&self, repo_id: &str, view_id: &[u8]) -> StoreResult<bool> {
        self.has_view_impl(repo_id, view_id).await
    }

    async fn get_view(&self, repo_id: &str, view_id: &[u8]) -> StoreResult<Option<View>> {
        self.get_view_impl(repo_id, view_id).await
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
        self.publish_operation_atomic_inner(repo_id, None, view_id, view, op_id, op, old_ids, false)
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
        self.publish_operation_atomic_inner(
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
        user: &str,
        workspace_name: &str,
    ) -> StoreResult<Option<WorkspaceState>> {
        self.get_workspace_impl(repo_id, user, workspace_name).await
    }

    #[allow(deprecated)]
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
        after: Option<(&str, &str)>,
        limit: usize,
        max_bytes: Option<usize>,
    ) -> StoreResult<(Vec<WorkspaceState>, bool)> {
        self.list_workspaces_page_impl(repo_id, after, limit, max_bytes)
            .await
    }

    async fn delete_workspace(
        &self,
        repo_id: &str,
        user: &str,
        workspace_name: &str,
    ) -> StoreResult<bool> {
        self.delete_workspace_impl(repo_id, user, workspace_name)
            .await
    }
}
