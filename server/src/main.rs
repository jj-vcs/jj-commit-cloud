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

//! Entrypoint for `jj-cc-server`, wiring gRPC `BackendService`, `OpStoreService`,
//! `WorkspaceService`, and `tonic_health` with optional constant-time Bearer token
//! authentication.
//!
//! # Cross-Process and RPC Boundaries
//! Wraps `BackendService`, `OpStoreService`, and `WorkspaceService` in
//! [`BearerAuthInterceptor`] while leaving `tonic_health` open for unauthenticated
//! liveness/readiness probes, and refuses to bind non-loopback `--host` interfaces without
//! authentication unless `--allow-unauthenticated` is explicitly enabled.

#![allow(clippy::result_large_err)]

use clap::{Parser, ValueEnum};
use std::sync::Arc;
use tonic::transport::Server;
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use cc_common::backend::backend_service_server::BackendServiceServer;
use cc_common::op_store::op_store_service_server::OpStoreServiceServer;
use cc_common::workspace::workspace_service_server::WorkspaceServiceServer;

pub mod auth;
mod backend;
pub mod error_util;
mod hash_utils;
pub mod jj_lib_adapters;
mod op_store;
pub mod reconcile_op_heads;
pub mod store;
mod workspace;

use auth::{BearerAuthInterceptor, load_tokens_from_file, resolve_listen_addrs, scrub_tokens};
use backend::CommitCloudBackendService;
use op_store::CommitCloudOpStoreService;
use store::{MemoryStore, Store};
use workspace::CommitCloudWorkspaceService;

// Storage backend to run the server against.
#[derive(Copy, Clone, Debug, ValueEnum)]
enum StoreType {
    Memory,
    Sqlite,
    Spanner,
}

#[derive(Parser)]
#[command(name = "jj-cc-server", about = "Jujutsu Commit Cloud Server")]
struct Args {
    /// Host interface to bind to
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Port to listen on (use 0 for ephemeral port assignment)
    #[arg(short, long, default_value_t = 8080)]
    port: u16,

    /// Storage backend type
    #[arg(long, value_enum, default_value_t = StoreType::Memory)]
    store_type: StoreType,

    /// Path to SQLite database file (defaults to ~/.jj-cc-server/commit_cloud.db)
    #[arg(long)]
    sqlite_path: Option<std::path::PathBuf>,

    /// Spanner database resource path (e.g. projects/<project>/instances/<instance>/databases/<db>)
    #[arg(long, env = "SPANNER_DATABASE")]
    spanner_db: Option<String>,

    /// Comma-separated graphic-ASCII Bearer token(s) required on gRPC requests (prefer
    /// `--auth-token-file` on shared hosts where CLI arguments appear in process listings)
    #[arg(
        long = "auth-token",
        env = "JJ_CC_SERVER_AUTH_TOKEN",
        value_delimiter = ',',
        hide_env_values = true
    )]
    auth_tokens: Vec<String>,

    /// Path to a UTF-8 file containing newline- or comma-separated graphic-ASCII Bearer
    /// token(s) (whitespace-delimited `#` comments and blank lines are ignored)
    #[arg(long = "auth-token-file", env = "JJ_CC_SERVER_AUTH_TOKEN_FILE")]
    auth_token_file: Option<std::path::PathBuf>,

    /// Allow binding to a non-loopback `--host` without Bearer authentication (for example
    /// behind Cloud Run IAM, Identity-Aware Proxy, or an isolated container network)
    #[arg(long, env = "JJ_CC_ALLOW_UNAUTHENTICATED")]
    allow_unauthenticated: bool,
}

pub struct CommitCloudServerImpl {
    pub store: Arc<dyn Store>,
}

impl Default for CommitCloudServerImpl {
    fn default() -> Self {
        Self {
            store: Arc::new(MemoryStore::default()),
        }
    }
}

pub fn get_default_sqlite_path_from_home(
    home: Option<&std::path::Path>,
) -> Result<std::path::PathBuf, std::io::Error> {
    if let Some(home_dir) = home {
        let dir = home_dir.join(".jj-cc-server");
        std::fs::create_dir_all(&dir).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!(
                    "Failed to create default SQLite directory '{}': {}",
                    dir.display(),
                    e
                ),
            )
        })?;
        Ok(dir.join("commit_cloud.db"))
    } else {
        warn!("HOME environment variable is not set; using local directory for SQLite database");
        Ok(std::path::PathBuf::from("commit_cloud.db"))
    }
}

pub fn get_default_sqlite_path() -> Result<std::path::PathBuf, std::io::Error> {
    let home = std::env::var_os("HOME").map(std::path::PathBuf::from);
    get_default_sqlite_path_from_home(home.as_deref())
}

pub fn resolve_sqlite_path(
    custom_path: Option<std::path::PathBuf>,
) -> Result<std::path::PathBuf, std::io::Error> {
    if let Some(path) = custom_path {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|e| {
                std::io::Error::new(
                    e.kind(),
                    format!(
                        "Failed to create parent directory '{}' for SQLite database: {}",
                        parent.display(),
                        e
                    ),
                )
            })?;
        }
        Ok(path)
    } else {
        get_default_sqlite_path()
    }
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("jj_cc_server=info".parse()?),
        )
        .init();

    let mut args = Args::parse();

    let mut combined_tokens = std::mem::take(&mut args.auth_tokens);
    if let Some(token_file) = &args.auth_token_file {
        match load_tokens_from_file(token_file) {
            Ok(file_tokens) => combined_tokens.extend(file_tokens),
            Err(err) => {
                scrub_tokens(&mut combined_tokens);
                return Err(err.into());
            }
        }
    }
    let has_combined_tokens = !combined_tokens.is_empty();
    let auth_res = BearerAuthInterceptor::try_from_tokens(&combined_tokens);
    scrub_tokens(&mut combined_tokens);
    drop(combined_tokens);
    let auth = auth_res?;

    let cli_auth_flag = std::env::args_os().any(|a| {
        let s = a.to_string_lossy();
        s == "--auth-token" || s.starts_with("--auth-token=")
    });
    let auth_requested = has_combined_tokens
        || cli_auth_flag
        || args.auth_token_file.is_some()
        || std::env::var_os("JJ_CC_SERVER_AUTH_TOKEN").is_some();
    if auth_requested && !auth.is_enabled() {
        return Err(
            "--auth-token (or JJ_CC_SERVER_AUTH_TOKEN) is set but contains no non-empty token; unset it to run without authentication"
                .into(),
        );
    }
    let listen_addrs = resolve_listen_addrs(
        &args.host,
        args.port,
        auth.is_enabled(),
        args.allow_unauthenticated,
    )
    .await?;
    if auth.is_enabled() {
        info!("Bearer token authentication enabled for gRPC services");
    } else {
        warn!("Bearer token authentication disabled: gRPC services accept any caller");
    }

    let store: Arc<dyn Store> = match args.store_type {
        StoreType::Memory => Arc::new(MemoryStore::default()),
        StoreType::Sqlite => {
            let path = resolve_sqlite_path(args.sqlite_path)?;
            info!("Opening SQLite Database Store at '{}'", path.display());
            Arc::new(store::SqliteStore::open(&path).map_err(|e| {
                format!(
                    "Failed to open SQLite database at '{}': {}",
                    path.display(),
                    e
                )
            })?)
        }
        StoreType::Spanner => {
            let spanner_db = args.spanner_db.ok_or_else(|| {
                "--spanner-db (or SPANNER_DATABASE environment variable) must be specified when using the spanner store type"
            })?;
            info!("Connecting to Cloud Spanner at '{spanner_db}'");
            Arc::new(store::SpannerStore::connect(&spanner_db).await?)
        }
    };

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_service_status("", tonic_health::ServingStatus::Serving)
        .await;
    health_reporter
        .set_serving::<BackendServiceServer<CommitCloudBackendService>>()
        .await;
    health_reporter
        .set_serving::<OpStoreServiceServer<CommitCloudOpStoreService>>()
        .await;
    health_reporter
        .set_serving::<WorkspaceServiceServer<CommitCloudWorkspaceService>>()
        .await;

    let listener = tokio::net::TcpListener::bind(listen_addrs.as_slice()).await?;
    let local_addr = listener.local_addr()?;
    info!("jj-cc-server listening on {}", local_addr);

    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let backend_service = CommitCloudBackendService::new(store.clone());
    let op_store_service = CommitCloudOpStoreService::new(store.clone());
    let workspace_service = CommitCloudWorkspaceService::new(store);

    Server::builder()
        .http2_max_header_list_size(64 * 1024)
        .add_service(health_service)
        .add_service(tonic::service::interceptor::InterceptedService::new(
            BackendServiceServer::new(backend_service),
            auth.clone(),
        ))
        .add_service(tonic::service::interceptor::InterceptedService::new(
            OpStoreServiceServer::new(op_store_service),
            auth.clone(),
        ))
        .add_service(tonic::service::interceptor::InterceptedService::new(
            WorkspaceServiceServer::new(workspace_service),
            auth,
        ))
        .serve_with_incoming_shutdown(incoming, async {
            match tokio::signal::ctrl_c().await {
                Ok(()) => {
                    info!("Shutdown signal received, shutting down gracefully...");
                }
                Err(e) => {
                    warn!(
                        "Failed to listen for shutdown signal: {}, shutting down...",
                        e
                    );
                }
            }
        })
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_default_sqlite_path_happy_path() {
        let temp_dir = tempdir().unwrap();
        let path = get_default_sqlite_path_from_home(Some(temp_dir.path())).unwrap();
        assert!(path.ends_with("commit_cloud.db"));
        assert!(temp_dir.path().join(".jj-cc-server").is_dir());
    }

    #[test]
    fn test_default_sqlite_path_without_home() {
        let path = get_default_sqlite_path_from_home(None).unwrap();
        assert_eq!(path, std::path::PathBuf::from("commit_cloud.db"));
    }

    #[test]
    fn test_default_sqlite_path_failure_when_dir_creation_fails() {
        let temp_dir = tempdir().unwrap();
        let blocker_file = temp_dir.path().join(".jj-cc-server");
        std::fs::write(&blocker_file, "blocking file").unwrap();

        let result = get_default_sqlite_path_from_home(Some(temp_dir.path()));
        assert!(result.is_err());
        let err = result.unwrap_err();
        let expected_io_err = std::fs::create_dir_all(&blocker_file).unwrap_err();
        let expected_msg = format!(
            "Failed to create default SQLite directory '{}': {}",
            blocker_file.display(),
            expected_io_err
        );
        assert_eq!(err.to_string(), expected_msg);
    }

    #[test]
    fn test_resolve_sqlite_path_custom_happy_path() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("nested").join("custom.db");
        let resolved = resolve_sqlite_path(Some(db_path.clone())).unwrap();
        assert_eq!(resolved, db_path);
        assert!(temp_dir.path().join("nested").is_dir());
    }

    #[test]
    fn test_resolve_sqlite_path_custom_failure() {
        let temp_dir = tempdir().unwrap();
        let blocker_file = temp_dir.path().join("nested");
        std::fs::write(&blocker_file, "blocking file").unwrap();

        let db_path = blocker_file.join("custom.db");
        let result = resolve_sqlite_path(Some(db_path));
        assert!(result.is_err());
        let err = result.unwrap_err();
        let expected_io_err = std::fs::create_dir_all(&blocker_file).unwrap_err();
        let expected_msg = format!(
            "Failed to create parent directory '{}' for SQLite database: {}",
            blocker_file.display(),
            expected_io_err
        );
        assert_eq!(err.to_string(), expected_msg);
    }
}
