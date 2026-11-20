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

//! Entrypoint for `jj-cc-server`, wiring gRPC `BackendService`, `OpStoreService`, and
//! `tonic_health` with optional constant-time Bearer token authentication.
//!
//! # Cross-Process and RPC Boundaries
//! Wraps `BackendService` and `OpStoreService` in [`BearerAuthInterceptor`] while leaving
//! `tonic_health` open for unauthenticated liveness/readiness probes, and refuses to bind
//! non-loopback `--host` interfaces without authentication unless `--allow-unauthenticated`
//! is explicitly enabled.

#![allow(clippy::result_large_err)]

use clap::{Parser, ValueEnum};
use std::sync::Arc;
use tonic::transport::Server;
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use cc_common::backend::backend_service_server::BackendServiceServer;
use cc_common::op_store::op_store_service_server::OpStoreServiceServer;

pub mod auth;
mod backend;
pub mod error_util;
mod hash_utils;
mod op_store;
pub mod store;

use auth::{BearerAuthInterceptor, load_tokens_from_file, resolve_listen_addrs};
use backend::CommitCloudBackendService;
use op_store::CommitCloudOpStoreService;
use store::{MemoryStore, Store};

#[derive(Copy, Clone, Debug, ValueEnum)]
enum StoreType {
    Memory,
    Sqlite,
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
        combined_tokens.extend(load_tokens_from_file(token_file)?);
    }
    let has_combined_tokens = !combined_tokens.is_empty();
    let auth = BearerAuthInterceptor::try_from_tokens(&combined_tokens)?;
    for tok in &mut combined_tokens {
        let mut bytes = std::mem::take(tok).into_bytes();
        bytes.fill(0);
        std::hint::black_box(bytes.as_slice());
    }
    drop(combined_tokens);

    let cli_auth_flag = std::env::args_os().any(|a| {
        let s = a.to_string_lossy();
        s == "--auth-token" || s.starts_with("--auth-token=")
    });
    let auth_requested = has_combined_tokens
        || cli_auth_flag
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
    match args.store_type {
        StoreType::Memory => {}
        StoreType::Sqlite => {
            eprintln!("SQLite storage backend is not yet implemented");
            std::process::exit(1);
        }
    }

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

    let listener = tokio::net::TcpListener::bind(listen_addrs.as_slice()).await?;
    let local_addr = listener.local_addr()?;
    info!("jj-cc-server listening on {}", local_addr);

    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let server_impl = CommitCloudServerImpl::default();
    let backend_service = CommitCloudBackendService::new(server_impl.store.clone());
    let op_store_service = CommitCloudOpStoreService::new(server_impl.store.clone());

    Server::builder()
        .http2_max_header_list_size(64 * 1024)
        .add_service(health_service)
        .add_service(tonic::service::interceptor::InterceptedService::new(
            BackendServiceServer::new(backend_service),
            auth.clone(),
        ))
        .add_service(tonic::service::interceptor::InterceptedService::new(
            OpStoreServiceServer::new(op_store_service),
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
