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

//! Entrypoint and CLI wiring for the `jj-cc-server` gRPC server.
//!
//! # Authority and Crash-Safety Ordering
//! Pairs a durable [`MetadataStore`] (`memory`, `sqlite`, `postgres`, or `spanner`) with a
//! content-addressed [`BlobStore`] (`memory`, `local`, `s3`, or `gcs`) inside [`CompositeStore`]
//! with `validate_dependencies` enabled before serving gRPC traffic. Refuses to start
//! `--store-type=spanner` or `--store-type=postgres` without an explicit blob store so metadata
//! never outlives throwaway in-memory blobs.
//!
//! # Security and Network Boundaries
//! Wires [`ServerAuthLayer`] from optional static Bearer tokens (`--auth-token` /
//! `--auth-token-file`, scrubbed after building [`BearerAuthInterceptor`]), OIDC JWT
//! verification (`--auth-oidc-*`, with optional `--auth-enforce-actor-repo-suffix`), and
//! Kubernetes `TokenReview` (`--auth-k8s-*`). Fails closed on empty or partial auth specs
//! and unauthenticated non-loopback binds (unless `--allow-unauthenticated`), while keeping
//! gRPC health probes and optional HTTP `--metrics-listen` (`/healthz`, `/statz`, `/metrics`)
//! open for readiness checks and telemetry scraping.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use tonic::transport::Server;
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use cc_common::backend::backend_service_server::BackendServiceServer;
use cc_common::op_store::op_store_service_server::OpStoreServiceServer;
use cc_common::workspace::workspace_service_server::WorkspaceServiceServer;
use jj_commit_cloud_server::auth::{
    BearerAuthInterceptor, K8sTokenReviewConfig, OidcAuthConfig, ServerAuthConfig, ServerAuthLayer,
    load_tokens_from_file, resolve_listen_addrs, scrub_tokens,
};
use jj_commit_cloud_server::backend::{
    CommitCloudBackendService, DEFAULT_WRITE_STREAM_SPILL_THRESHOLD_BYTES,
};
use jj_commit_cloud_server::metrics::{MetricsLayer, ServerMetrics, bind_and_serve_metrics};
use jj_commit_cloud_server::op_store::CommitCloudOpStoreService;
use jj_commit_cloud_server::store::gcsblob::{normalize_bucket_identifiers, validate_gcs_endpoint};
use jj_commit_cloud_server::store::s3blob::{
    DEFAULT_S3_REGION, normalize_s3_bucket, parse_s3_peer_spec, redact_s3_peer_spec,
    validate_s3_endpoint,
};
use jj_commit_cloud_server::store::{
    BlobStore, CompositeStore, CompositeStoreConfig, GcsBlobStore, GcsConnectConfig,
    LocalFsBlobStore, MemoryBlobStore, MemoryMetadataStore, MetadataStore, PostgresConnectConfig,
    PostgresMetadataStore, ReplicatedBlobStore, ReplicatedBlobStoreConfig, S3BlobStore,
    S3ConnectConfig, SpannerConnectConfig, SpannerMetadataStore, SqliteMetadataStore,
};
use jj_commit_cloud_server::workspace::CommitCloudWorkspaceService;

/// Metadata storage backend to run the server against.
#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum StoreType {
    Memory,
    Sqlite,
    Postgres,
    Spanner,
}

/// Content-addressed blob storage backend.
#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum BlobStoreType {
    Memory,
    Local,
    S3,
    Gcs,
}

/// Upper bound for `--inline-blob-threshold-bytes` (4 MiB), matching the streaming upload
/// spill threshold and staying well under Cloud Spanner's 10 MiB column limit.
const MAX_INLINE_BLOB_THRESHOLD_BYTES: usize = DEFAULT_WRITE_STREAM_SPILL_THRESHOLD_BYTES;

fn parse_inline_blob_threshold_bytes(raw: &str) -> Result<usize, String> {
    let value: usize = raw
        .parse()
        .map_err(|e| format!("invalid --inline-blob-threshold-bytes value '{raw}': {e}"))?;
    if value > MAX_INLINE_BLOB_THRESHOLD_BYTES {
        return Err(format!(
            "--inline-blob-threshold-bytes ({value}) exceeds maximum allowed value of \
             {MAX_INLINE_BLOB_THRESHOLD_BYTES} bytes (4 MiB)"
        ));
    }
    Ok(value)
}

// No Debug derive: `auth_tokens` holds secrets.
#[derive(Parser)]
#[command(name = "jj-cc-server", about = "jj Commit Cloud Server")]
struct Args {
    /// Host interface to bind to
    #[arg(long, env = "HOST", default_value = "127.0.0.1")]
    host: String,

    /// Port to listen on (use 0 for ephemeral port assignment)
    #[arg(short, long, env = "PORT", default_value_t = 8080)]
    port: u16,

    /// Metadata storage backend type
    #[arg(long, env = "STORE_TYPE", value_enum, default_value_t = StoreType::Memory)]
    store_type: StoreType,

    /// Blob storage backend type (auto-detected from --gcs-bucket, --s3-bucket, --blob-path, and
    /// --store-type if omitted; required with --store-type=spanner or --store-type=postgres unless
    /// --gcs-bucket, --s3-bucket, or --blob-path is set)
    #[arg(long, env = "BLOB_STORE", value_enum)]
    blob_store: Option<BlobStoreType>,

    /// Maximum blob size in bytes stored inline inside MetadataStore (0 disables inlining;
    /// max 4 MiB)
    #[arg(
        long,
        env = "INLINE_BLOB_THRESHOLD_BYTES",
        default_value_t = 0,
        value_parser = parse_inline_blob_threshold_bytes
    )]
    inline_blob_threshold_bytes: usize,

    /// Path to SQLite database file (defaults to ~/.jj-cc-server/commit_cloud.db)
    #[arg(long, env = "SQLITE_PATH")]
    sqlite_path: Option<PathBuf>,

    /// PostgreSQL connection URL (reads `--postgres-url`, `POSTGRES_URL`, or fallback `DATABASE_URL`)
    #[arg(long, env = "POSTGRES_URL", hide_env_values = true)]
    postgres_url: Option<String>,

    /// Maximum PostgreSQL connection pool size
    #[arg(long, env = "POSTGRES_MAX_CONNS", default_value_t = 40)]
    postgres_max_conns: usize,

    /// Automatically apply `postgres_schema.sql` idempotently on startup (default: true)
    #[arg(
        long,
        env = "POSTGRES_AUTO_MIGRATE",
        default_value_t = true,
        default_missing_value = "true",
        num_args = 0..=1,
        action = clap::ArgAction::Set
    )]
    postgres_auto_migrate: bool,

    /// Optional PEM CA certificate bundle for PostgreSQL TLS verification (overrides `sslrootcert`;
    /// enforces TLS `verify-full` by default unless overridden by `sslmode=verify-ca` or `require`,
    /// and rejects `sslmode=disable`, `sslmode=prefer`, and Unix-domain socket hosts)
    #[arg(long, env = "POSTGRES_CA_CERT")]
    postgres_ca_cert: Option<PathBuf>,

    /// Root directory for local content-addressed blob storage
    #[arg(long, env = "BLOB_PATH")]
    blob_path: Option<PathBuf>,

    /// Optional logical location URI override for local blob store (e.g. gs://region-a)
    #[arg(long, env = "BLOB_LOCATION_ID")]
    blob_location_id: Option<String>,

    /// Optional peer local blob directories formatted as `<location_uri>=<dir_path>`
    #[arg(long, env = "LOCAL_PEER_BLOB_DIRS", value_delimiter = ',')]
    local_peer_blob_dirs: Vec<String>,

    /// Cloud Spanner database resource path (projects/<p>/instances/<i>/databases/<d>)
    #[arg(long, visible_alias = "spanner-db", env = "SPANNER_DATABASE")]
    spanner_database: Option<String>,

    /// Optional Spanner gRPC endpoint override (e.g. Spanner Omni or custom emulator URL)
    #[arg(long, env = "SPANNER_ENDPOINT")]
    spanner_endpoint: Option<String>,

    /// Optional Spanner HTTP/REST admin endpoint override for DDL auto-migration
    #[arg(long, env = "SPANNER_EMULATOR_REST_HOST")]
    spanner_admin_endpoint: Option<String>,

    /// Automatically create missing Spanner database and apply spanner_schema.sql on startup
    #[arg(long, env = "SPANNER_AUTO_MIGRATE")]
    spanner_auto_migrate: bool,

    /// Target a local Cloud Spanner Emulator (allows plaintext endpoints and HTTP/1.0 REST DDL
    /// administration without setting SPANNER_EMULATOR_HOST)
    #[arg(long, env = "SPANNER_EMULATOR", default_value_t = false)]
    spanner_emulator: bool,

    /// Connect to a containerized Spanner Omni endpoint instead of managed Cloud Spanner
    #[arg(long)]
    spanner_omni: bool,

    /// Path to custom Root CA PEM file for Spanner Omni TLS verification
    #[arg(long)]
    spanner_omni_ca_cert: Option<PathBuf>,

    /// Path to client certificate PEM file for Spanner Omni mTLS authentication
    #[arg(long)]
    spanner_omni_client_cert: Option<PathBuf>,

    /// Path to client private key PEM file for Spanner Omni mTLS authentication
    #[arg(long)]
    spanner_omni_client_key: Option<PathBuf>,

    /// Primary Google Cloud Storage bucket (`my-bucket` or `gs://my-bucket`)
    #[arg(long, env = "GCS_BUCKET")]
    gcs_bucket: Option<String>,

    /// Optional comma-separated peer Cloud Storage buckets (`gs://peer` or `gs://peer=http://endpoint`)
    #[arg(long, env = "GCS_PEER_BUCKETS", value_delimiter = ',')]
    gcs_peer_buckets: Vec<String>,

    /// Optional Cloud Storage endpoint override (e.g. local emulator or VPC-SC endpoint)
    #[arg(long, env = "STORAGE_EMULATOR_HOST")]
    gcs_endpoint: Option<String>,

    /// Primary S3-compatible bucket (`my-bucket` or `s3://my-bucket`)
    #[arg(long, env = "S3_BUCKET")]
    s3_bucket: Option<String>,

    /// Optional S3-compatible endpoint override (e.g. `http://127.0.0.1:9000` for MinIO or R2/B2)
    #[arg(long, env = "S3_ENDPOINT")]
    s3_endpoint: Option<String>,

    /// AWS SigV4 signing region for S3-compatible object storage (falls back to `AWS_REGION`,
    /// `AWS_DEFAULT_REGION`, then `us-east-1`)
    #[arg(long, env = "S3_REGION")]
    s3_region: Option<String>,

    /// S3 access key ID (falls back to `AWS_ACCESS_KEY_ID`)
    #[arg(long, env = "S3_ACCESS_KEY_ID", hide_env_values = true)]
    s3_access_key: Option<String>,

    /// S3 secret access key (falls back to `AWS_SECRET_ACCESS_KEY`)
    #[arg(long, env = "S3_SECRET_ACCESS_KEY", hide_env_values = true)]
    s3_secret_key: Option<String>,

    /// Optional temporary S3 session token (falls back to `AWS_SESSION_TOKEN`)
    #[arg(long, env = "S3_SESSION_TOKEN", hide_env_values = true)]
    s3_session_token: Option<String>,

    /// Force S3 path-style addressing (`/<bucket>/<key>`); defaults to true when `--s3-endpoint` is set
    #[arg(
        long,
        env = "S3_PATH_STYLE",
        default_missing_value = "true",
        num_args = 0..=1,
        action = clap::ArgAction::Set
    )]
    s3_path_style: Option<bool>,

    /// Automatically create missing S3 bucket via `HEAD`/`PUT` on startup (defaults to true for `http://` endpoints)
    #[arg(
        long,
        env = "S3_AUTO_CREATE_BUCKET",
        default_missing_value = "true",
        num_args = 0..=1,
        action = clap::ArgAction::Set
    )]
    s3_auto_create_bucket: Option<bool>,

    /// Optional comma-separated peer S3 buckets (`s3://peer`, `s3://peer:region`, or
    /// `s3://peer[:region]=http://endpoint`)
    #[arg(long, env = "S3_PEER_BUCKETS", value_delimiter = ',')]
    s3_peer_buckets: Vec<String>,

    /// In-memory read-through LRU blob cache size in bytes (0 disables caching). Setting a
    /// non-zero size, or configuring peer blob stores, wraps the blob store in ReplicatedBlobStore
    /// (default cache size with peers: 64 MiB; default for a single store: 0)
    #[arg(long, env = "BLOB_CACHE_BYTES")]
    blob_cache_bytes: Option<usize>,

    /// Retry window in milliseconds for metadata-confirmed blobs that no blob store returns yet
    /// (0 disables). Cloud Storage provides read-after-write consistency within a bucket, and peer
    /// fallback covers lag between buckets, so only object stores without read-after-write
    /// consistency need this
    #[arg(long, env = "GCS_REPLICATION_RETRY_MS", default_value_t = 0)]
    gcs_replication_retry_ms: u64,

    /// Replicate uploaded blobs asynchronously to configured peer buckets in the background
    #[arg(long, env = "GCS_ASYNC_WRITE_FANOUT")]
    gcs_async_write_fanout: bool,

    /// Optional Bearer token(s) required on gRPC requests (comma-separated; health checks stay
    /// open)
    #[arg(
        long = "auth-token",
        env = "JJ_CC_SERVER_AUTH_TOKEN",
        value_delimiter = ',',
        hide_env_values = true
    )]
    auth_tokens: Vec<String>,

    /// Path to a file containing Bearer token(s) (newline- or comma-separated)
    #[arg(long = "auth-token-file", env = "JJ_CC_SERVER_AUTH_TOKEN_FILE")]
    auth_token_file: Option<std::path::PathBuf>,

    /// Allow binding to a non-loopback `--host` without authentication (for example
    /// behind Cloud Run IAM, Identity-Aware Proxy, or an isolated container network)
    #[arg(long, env = "JJ_CC_ALLOW_UNAUTHENTICATED")]
    allow_unauthenticated: bool,

    /// Optional OIDC issuer URL for JWT authentication (e.g. `https://accounts.google.com` or
    /// `https://idp.ate-system.svc`)
    #[arg(long = "auth-oidc-issuer", env = "JJ_CC_AUTH_OIDC_ISSUER")]
    auth_oidc_issuer: Option<String>,

    /// Required audience (`aud` claim) when `--auth-oidc-issuer` is set (e.g. `cloud-jj`)
    #[arg(long = "auth-oidc-audience", env = "JJ_CC_AUTH_OIDC_AUDIENCE")]
    auth_oidc_audience: Option<String>,

    /// Optional explicit JWKS URI override (otherwise discovered via
    /// `<issuer>/.well-known/openid-configuration`)
    #[arg(long = "auth-oidc-jwks-uri", env = "JJ_CC_AUTH_OIDC_JWKS_URI")]
    auth_oidc_jwks_uri: Option<String>,

    /// Optional PEM CA certificate bundle for TLS verification against the OIDC issuer/JWKS
    #[arg(long = "auth-oidc-ca-cert", env = "JJ_CC_AUTH_OIDC_CA_CERT")]
    auth_oidc_ca_cert: Option<PathBuf>,

    /// Restrict OIDC actor JWT writes carrying a `repo_id` to `<alnum>_<atespace>_<actorName>`
    /// repositories matching the actor's required `atespace` and `actorName` claims
    #[arg(
        long = "auth-enforce-actor-repo-suffix",
        env = "JJ_CC_AUTH_ENFORCE_ACTOR_REPO_SUFFIX"
    )]
    auth_enforce_actor_repo_suffix: bool,

    /// Optional Kubernetes API server URL (or full `tokenreviews` URL) for ServiceAccount
    /// `TokenReview` authentication
    #[arg(
        long = "auth-k8s-tokenreview-url",
        env = "JJ_CC_AUTH_K8S_TOKENREVIEW_URL"
    )]
    auth_k8s_tokenreview_url: Option<String>,

    /// Optional expected audience passed to Kubernetes `TokenReview`
    #[arg(
        long = "auth-k8s-tokenreview-aud",
        env = "JJ_CC_AUTH_K8S_TOKENREVIEW_AUD"
    )]
    auth_k8s_tokenreview_aud: Option<String>,

    /// Optional PEM CA certificate bundle for TLS verification against the Kubernetes API server
    #[arg(long = "auth-k8s-ca-cert", env = "JJ_CC_AUTH_K8S_CA_CERT")]
    auth_k8s_ca_cert: Option<PathBuf>,

    /// Optional path to projected ServiceAccount token used to authenticate `TokenReview` requests
    #[arg(long = "auth-k8s-sa-token-file", env = "JJ_CC_AUTH_K8S_SA_TOKEN_FILE")]
    auth_k8s_sa_token_file: Option<PathBuf>,

    /// Optional HTTP listen address for unauthenticated observability endpoints
    /// (`/healthz`, `/statz`, `/metrics`), e.g. `127.0.0.1:9090` (disabled when unset or blank)
    #[arg(long, env = "METRICS_LISTEN")]
    metrics_listen: Option<String>,
}

const DEFAULT_BLOB_CACHE_BYTES: usize = 64 * 1024 * 1024;

fn default_server_data_dir() -> PathBuf {
    std::env::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".jj-cc-server")
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
                .add_directive("jj_cc_server=info".parse()?)
                .add_directive("jj_commit_cloud_server=info".parse()?),
        )
        .init();

    let mut args = Args::parse();
    args.sqlite_path = args
        .sqlite_path
        .filter(|p| !p.as_os_str().is_empty() && p.to_str().is_none_or(|s| !s.trim().is_empty()));
    args.blob_path = args
        .blob_path
        .filter(|p| !p.as_os_str().is_empty() && p.to_str().is_none_or(|s| !s.trim().is_empty()));
    args.blob_location_id = args
        .blob_location_id
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    args.spanner_database = args
        .spanner_database
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let metrics_listen_addr: Option<std::net::SocketAddr> = args
        .metrics_listen
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|raw| {
            raw.parse::<std::net::SocketAddr>().map_err(|e| {
                format!(
                    "invalid --metrics-listen address '{raw}' \
                     (expected ip:port, e.g. 127.0.0.1:9090): {e}"
                )
            })
        })
        .transpose()?;
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

    // A secret that renders as "" or " , " must not leave the server open without a trace.
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
            "--auth-token (or JJ_CC_SERVER_AUTH_TOKEN) is set but contains no non-empty \
                    token; unset it to run without authentication"
                .into(),
        );
    }

    let require_non_blank_str = |opt: Option<String>,
                                 flag: &str,
                                 env_var: &str|
     -> Result<Option<String>, String> {
        match opt {
            None => Ok(None),
            Some(raw) => {
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    Err(format!(
                        "{flag} (or {env_var}) must not be empty or whitespace; unset it to disable"
                    ))
                } else {
                    Ok(Some(trimmed.to_string()))
                }
            }
        }
    };
    let require_non_blank_path = |opt: Option<PathBuf>,
                                  flag: &str,
                                  env_var: &str|
     -> Result<Option<PathBuf>, String> {
        match opt {
            None => Ok(None),
            Some(path) => {
                if path.as_os_str().is_empty() || path.to_str().is_some_and(|s| s.trim().is_empty())
                {
                    Err(format!(
                        "{flag} (or {env_var}) must not be empty or whitespace; unset it to disable"
                    ))
                } else {
                    Ok(Some(path))
                }
            }
        }
    };

    let oidc_issuer = require_non_blank_str(
        args.auth_oidc_issuer.take(),
        "--auth-oidc-issuer",
        "JJ_CC_AUTH_OIDC_ISSUER",
    )?;
    let oidc_audience = require_non_blank_str(
        args.auth_oidc_audience.take(),
        "--auth-oidc-audience",
        "JJ_CC_AUTH_OIDC_AUDIENCE",
    )?;
    let oidc_jwks_uri = require_non_blank_str(
        args.auth_oidc_jwks_uri.take(),
        "--auth-oidc-jwks-uri",
        "JJ_CC_AUTH_OIDC_JWKS_URI",
    )?;
    let oidc_ca_cert = require_non_blank_path(
        args.auth_oidc_ca_cert.take(),
        "--auth-oidc-ca-cert",
        "JJ_CC_AUTH_OIDC_CA_CERT",
    )?;
    let oidc_config = match oidc_issuer {
        Some(issuer) => {
            let audience = oidc_audience.ok_or(
                "--auth-oidc-audience (or JJ_CC_AUTH_OIDC_AUDIENCE) is required when \
                 --auth-oidc-issuer is set",
            )?;
            Some(OidcAuthConfig {
                issuer,
                audience,
                jwks_uri: oidc_jwks_uri,
                ca_cert: oidc_ca_cert,
                enforce_actor_repo_suffix: args.auth_enforce_actor_repo_suffix,
            })
        }
        None => {
            if oidc_audience.is_some()
                || oidc_jwks_uri.is_some()
                || oidc_ca_cert.is_some()
                || args.auth_enforce_actor_repo_suffix
            {
                return Err(
                    "--auth-oidc-issuer (or JJ_CC_AUTH_OIDC_ISSUER) is required when OIDC auth \
                     options are configured"
                        .into(),
                );
            }
            None
        }
    };

    let k8s_url = require_non_blank_str(
        args.auth_k8s_tokenreview_url.take(),
        "--auth-k8s-tokenreview-url",
        "JJ_CC_AUTH_K8S_TOKENREVIEW_URL",
    )?;
    let k8s_aud = require_non_blank_str(
        args.auth_k8s_tokenreview_aud.take(),
        "--auth-k8s-tokenreview-aud",
        "JJ_CC_AUTH_K8S_TOKENREVIEW_AUD",
    )?;
    let k8s_ca_cert = require_non_blank_path(
        args.auth_k8s_ca_cert.take(),
        "--auth-k8s-ca-cert",
        "JJ_CC_AUTH_K8S_CA_CERT",
    )?;
    let k8s_sa_token_file = require_non_blank_path(
        args.auth_k8s_sa_token_file.take(),
        "--auth-k8s-sa-token-file",
        "JJ_CC_AUTH_K8S_SA_TOKEN_FILE",
    )?;
    let k8s_config = match k8s_url {
        Some(url) => Some(K8sTokenReviewConfig {
            url,
            audience: k8s_aud,
            ca_cert: k8s_ca_cert,
            sa_token_file: k8s_sa_token_file,
        }),
        None => {
            if k8s_aud.is_some() || k8s_ca_cert.is_some() || k8s_sa_token_file.is_some() {
                return Err(
                    "--auth-k8s-tokenreview-url (or JJ_CC_AUTH_K8S_TOKENREVIEW_URL) is required \
                     when Kubernetes TokenReview options are configured"
                        .into(),
                );
            }
            None
        }
    };

    let static_auth_enabled = auth.is_enabled();
    let oidc_enabled = oidc_config.is_some();
    let k8s_enabled = k8s_config.is_some();
    let auth_layer = ServerAuthLayer::new(ServerAuthConfig {
        static_tokens: auth,
        oidc: oidc_config,
        k8s_tokenreview: k8s_config,
    })?;
    let enforce_actor_repo_suffix = auth_layer.enforce_actor_repo_suffix();

    let listen_addrs = resolve_listen_addrs(
        &args.host,
        args.port,
        auth_layer.is_enabled(),
        args.allow_unauthenticated,
    )
    .await?;
    if auth_layer.is_enabled() {
        let mut modes = Vec::new();
        if static_auth_enabled {
            modes.push("static-bearer");
        }
        if oidc_enabled {
            modes.push("oidc-jwt");
        }
        if k8s_enabled {
            modes.push("k8s-tokenreview");
        }
        info!(
            "gRPC authentication enabled (modes: {}, enforce_actor_repo_suffix={})",
            modes.join(", "),
            enforce_actor_repo_suffix
        );
    } else {
        warn!("gRPC authentication disabled: gRPC services accept any caller");
    }

    let clean_opt_preserve_ctrl = |opt: Option<String>| -> Option<String> {
        opt.and_then(|raw| {
            if raw.bytes().any(|b| b.is_ascii_control()) {
                Some(raw)
            } else {
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            }
        })
    };

    let resolved_sqlite_path = args
        .sqlite_path
        .clone()
        .unwrap_or_else(|| default_server_data_dir().join("commit_cloud.db"));
    let postgres_url = clean_opt_preserve_ctrl(args.postgres_url.take())
        .or_else(|| clean_opt_preserve_ctrl(std::env::var("DATABASE_URL").ok()));
    let postgres_ca_cert = match args.postgres_ca_cert.take() {
        None => None,
        Some(path) => {
            if path.as_os_str().is_empty() || path.to_str().is_some_and(|s| s.trim().is_empty()) {
                return Err(
                    "--postgres-ca-cert (or POSTGRES_CA_CERT) must not be empty or whitespace; \
                     unset it to disable"
                        .into(),
                );
            }
            Some(path)
        }
    };
    let gcs_bucket = clean_opt_preserve_ctrl(args.gcs_bucket.clone());
    let s3_bucket = clean_opt_preserve_ctrl(args.s3_bucket.take());
    let s3_endpoint = clean_opt_preserve_ctrl(args.s3_endpoint.take());
    let s3_region = clean_opt_preserve_ctrl(args.s3_region.take())
        .or_else(|| clean_opt_preserve_ctrl(std::env::var("AWS_REGION").ok()))
        .or_else(|| clean_opt_preserve_ctrl(std::env::var("AWS_DEFAULT_REGION").ok()))
        .unwrap_or_else(|| DEFAULT_S3_REGION.to_string());
    let has_gcs_bucket = gcs_bucket.is_some();
    let has_s3_bucket = s3_bucket.is_some();
    let has_blob_path = args.blob_path.is_some();

    let spanner_database = if args.store_type == StoreType::Spanner {
        let database = args.spanner_database.clone().ok_or(
            "--spanner-database (or SPANNER_DATABASE) is required when --store-type=spanner",
        )?;
        // Durable metadata over a silent in-memory blob default would lose file contents
        // on restart while commits and trees still reference them.
        if args.blob_store.is_none() && !has_gcs_bucket && !has_s3_bucket && !has_blob_path {
            return Err("--store-type=spanner requires an explicit blob store: set \
                        --gcs-bucket (GCS_BUCKET), --s3-bucket (S3_BUCKET), or --blob-path \
                        (BLOB_PATH), or pass --blob-store=memory (BLOB_STORE=memory) for \
                        throwaway tests"
                .into());
        }
        Some(database)
    } else {
        None
    };

    let postgres_config_url = if args.store_type == StoreType::Postgres {
        let url = postgres_url.ok_or(
            "--postgres-url (or POSTGRES_URL / DATABASE_URL) is required when \
             --store-type=postgres",
        )?;
        if args.postgres_max_conns == 0 {
            return Err("--postgres-max-conns must be greater than 0".into());
        }
        if args.blob_store.is_none() && !has_gcs_bucket && !has_s3_bucket && !has_blob_path {
            return Err(
                "--store-type=postgres requires an explicit blob store: set \
                        --s3-bucket (S3_BUCKET), --gcs-bucket (GCS_BUCKET), or --blob-path \
                        (BLOB_PATH), or pass --blob-store=memory (BLOB_STORE=memory) for \
                        throwaway tests"
                    .into(),
            );
        }
        Some(url)
    } else {
        None
    };

    let effective_blob_store = match args.blob_store {
        Some(explicit) => explicit,
        None => {
            if has_gcs_bucket && has_s3_bucket {
                return Err(
                    "both --gcs-bucket and --s3-bucket are set; pass --blob-store explicitly"
                        .into(),
                );
            } else if has_gcs_bucket {
                BlobStoreType::Gcs
            } else if has_s3_bucket {
                BlobStoreType::S3
            } else if has_blob_path {
                BlobStoreType::Local
            } else {
                match args.store_type {
                    StoreType::Memory => BlobStoreType::Memory,
                    // Spanner and Postgres startup reject a missing explicit blob store above,
                    // so those arms are unreachable without explicit flags.
                    StoreType::Sqlite | StoreType::Spanner | StoreType::Postgres => {
                        BlobStoreType::Local
                    }
                }
            }
        }
    };
    let primary_gcs_config = if effective_blob_store == BlobStoreType::Gcs {
        let bucket =
            gcs_bucket.ok_or("--gcs-bucket (or GCS_BUCKET) is required when --blob-store=gcs")?;
        let (_, location_uri) = normalize_bucket_identifiers(&bucket)?;
        let cfg = GcsConnectConfig {
            bucket,
            endpoint: args.gcs_endpoint.clone(),
        };
        cfg.validate()?;
        Some((cfg, location_uri))
    } else {
        if let Some(bucket) = &gcs_bucket {
            normalize_bucket_identifiers(bucket)?;
        }
        if let Some(ep) = args.gcs_endpoint.as_deref()
            && (ep.bytes().any(|b| b.is_ascii_control()) || !ep.trim().is_empty())
        {
            validate_gcs_endpoint(ep)?;
        }
        None
    };

    // Fall back to ambient `AWS_*` credentials only as a complete unit when no explicit
    // `--s3-*` / `S3_*` credential input is set, so explicit S3 keys never mix with an ambient
    // `AWS_SESSION_TOKEN` or `AWS_SECRET_ACCESS_KEY`.
    let explicit_s3_ak = clean_opt_preserve_ctrl(args.s3_access_key.take());
    let explicit_s3_sk = clean_opt_preserve_ctrl(args.s3_secret_key.take());
    let explicit_s3_st = clean_opt_preserve_ctrl(args.s3_session_token.take());
    let (s3_access_key, s3_secret_key, s3_session_token) =
        if explicit_s3_ak.is_some() || explicit_s3_sk.is_some() || explicit_s3_st.is_some() {
            (explicit_s3_ak, explicit_s3_sk, explicit_s3_st)
        } else {
            (
                clean_opt_preserve_ctrl(std::env::var("AWS_ACCESS_KEY_ID").ok()),
                clean_opt_preserve_ctrl(std::env::var("AWS_SECRET_ACCESS_KEY").ok()),
                clean_opt_preserve_ctrl(std::env::var("AWS_SESSION_TOKEN").ok()),
            )
        };
    let s3_defaults = S3ConnectConfig {
        bucket: String::new(),
        endpoint: s3_endpoint.clone(),
        region: s3_region,
        access_key: s3_access_key,
        secret_key: s3_secret_key,
        session_token: s3_session_token,
        path_style: args.s3_path_style,
        auto_create_bucket: args.s3_auto_create_bucket,
    };

    let primary_s3_config = if effective_blob_store == BlobStoreType::S3 {
        let bucket =
            s3_bucket.ok_or("--s3-bucket (or S3_BUCKET) is required when --blob-store=s3")?;
        let location_uri = format!("s3://{}", normalize_s3_bucket(&bucket)?);
        let cfg = S3ConnectConfig {
            bucket,
            ..s3_defaults.clone()
        };
        cfg.validate()?;
        Some((cfg, location_uri))
    } else {
        if let Some(bucket) = &s3_bucket {
            normalize_s3_bucket(bucket)?;
        }
        if let Some(ep) = &s3_endpoint {
            validate_s3_endpoint(ep)?;
        }
        None
    };

    let s3_peer_configs = parse_s3_peer_configs(&args.s3_peer_buckets, &s3_defaults)?;

    let mut gcs_peer_configs: Vec<(GcsConnectConfig, String)> = Vec::new();
    for raw_spec in &args.gcs_peer_buckets {
        let spec = if raw_spec.bytes().any(|b| b.is_ascii_control()) {
            raw_spec.as_str()
        } else {
            let trimmed = raw_spec.trim();
            if trimmed.is_empty() {
                continue;
            }
            trimmed
        };
        let (bucket, endpoint) = match spec.split_once('=') {
            Some((b, ep)) => {
                let bucket = if b.bytes().any(|c| c.is_ascii_control()) {
                    b
                } else {
                    b.trim()
                };
                let ep_clean = if ep.bytes().any(|c| c.is_ascii_control()) {
                    ep
                } else {
                    ep.trim()
                };
                if bucket.is_empty() || ep_clean.is_empty() {
                    return Err(format!(
                        "invalid --gcs-peer-buckets entry {spec:?} \
                         (expected <bucket> or <bucket>=<endpoint>)"
                    )
                    .into());
                }
                (bucket.to_string(), Some(ep_clean.to_string()))
            }
            None => (spec.to_string(), args.gcs_endpoint.clone()),
        };
        let (_, location_uri) = normalize_bucket_identifiers(&bucket)?;
        let cfg = GcsConnectConfig { bucket, endpoint };
        cfg.validate()?;
        gcs_peer_configs.push((cfg, location_uri));
    }

    let mut local_peer_specs: Vec<(String, String)> = Vec::new();
    for spec in args
        .local_peer_blob_dirs
        .iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        let (loc_uri, dir_str) = spec.split_once('=').ok_or_else(|| {
            format!("invalid --local-peer-blob-dirs entry '{spec}' (expected <uri>=<path>)")
        })?;
        let loc_uri = loc_uri.trim();
        let dir_str = dir_str.trim();
        if loc_uri.is_empty() || dir_str.is_empty() {
            return Err(format!(
                "invalid --local-peer-blob-dirs entry '{spec}' (expected <uri>=<path>)"
            )
            .into());
        }
        local_peer_specs.push((loc_uri.to_string(), dir_str.to_string()));
    }

    let metrics = ServerMetrics::new();

    let metadata_store: Arc<dyn MetadataStore> = match args.store_type {
        StoreType::Memory => Arc::new(MemoryMetadataStore::new()),
        StoreType::Sqlite => {
            info!(
                "Opening SQLite metadata store at {}",
                resolved_sqlite_path.display()
            );
            Arc::new(SqliteMetadataStore::open(&resolved_sqlite_path)?)
        }
        StoreType::Postgres => {
            let url = postgres_config_url.ok_or(
                "--postgres-url (or POSTGRES_URL / DATABASE_URL) is required when \
                 --store-type=postgres",
            )?;
            info!(
                "Connecting to PostgreSQL metadata store (max_conns={}, auto_migrate={})",
                args.postgres_max_conns, args.postgres_auto_migrate
            );
            let cfg = PostgresConnectConfig {
                url,
                max_conns: args.postgres_max_conns,
                auto_migrate: args.postgres_auto_migrate,
                ca_cert: postgres_ca_cert,
                tx_retries: Some(metrics.db_tx_retries_counter()),
            };
            Arc::new(PostgresMetadataStore::connect(&cfg).await?)
        }
        StoreType::Spanner => {
            let database = spanner_database.ok_or(
                "--spanner-database (or SPANNER_DATABASE) is required when --store-type=spanner",
            )?;
            info!("Connecting to Cloud Spanner database '{database}'");
            let cfg = SpannerConnectConfig {
                database,
                endpoint: args.spanner_endpoint.clone(),
                admin_endpoint: args.spanner_admin_endpoint.clone(),
                omni: args.spanner_omni,
                omni_ca_cert: args.spanner_omni_ca_cert.clone(),
                omni_client_cert: args.spanner_omni_client_cert.clone(),
                omni_client_key: args.spanner_omni_client_key.clone(),
                auto_migrate: args.spanner_auto_migrate,
                emulator: args.spanner_emulator,
            };
            Arc::new(
                SpannerMetadataStore::connect(&cfg)
                    .await?
                    .with_tx_retries_counter(metrics.db_tx_retries_counter()),
            )
        }
    };
    if effective_blob_store == BlobStoreType::Memory && args.store_type != StoreType::Memory {
        warn!("Blob payloads are kept in memory and are lost on restart");
    }

    let primary_blob_store: Arc<dyn BlobStore> = match effective_blob_store {
        BlobStoreType::Memory => match &args.blob_location_id {
            Some(loc) => Arc::new(MemoryBlobStore::with_location(loc.clone())),
            None => Arc::new(MemoryBlobStore::new()),
        },
        BlobStoreType::Local => {
            let blob_root = args.blob_path.clone().unwrap_or_else(|| {
                if args.store_type == StoreType::Sqlite {
                    let mut p = resolved_sqlite_path.as_os_str().to_os_string();
                    p.push(".blobs");
                    PathBuf::from(p)
                } else {
                    default_server_data_dir().join("blobs")
                }
            });
            info!(
                "Opening local filesystem blob store at {}",
                blob_root.display()
            );
            match &args.blob_location_id {
                Some(loc) => Arc::new(LocalFsBlobStore::open_with_location_id(&blob_root, loc)?),
                None => Arc::new(LocalFsBlobStore::open(&blob_root)?),
            }
        }
        BlobStoreType::S3 => {
            let (cfg, location_uri) = primary_s3_config
                .ok_or("--s3-bucket (or S3_BUCKET) is required when --blob-store=s3")?;
            info!(
                "Connecting to S3-compatible bucket '{location_uri}' (region={})",
                cfg.region
            );
            Arc::new(S3BlobStore::connect(&cfg).await?)
        }
        BlobStoreType::Gcs => {
            let (cfg, location_uri) = primary_gcs_config
                .ok_or("--gcs-bucket (or GCS_BUCKET) is required when --blob-store=gcs")?;
            info!("Connecting to Cloud Storage bucket '{location_uri}'");
            Arc::new(GcsBlobStore::connect(&cfg).await?)
        }
    };

    let mut seen_peer_locations: HashSet<String> = HashSet::new();
    seen_peer_locations.insert(primary_blob_store.location_id().to_string());
    let mut peer_stores: Vec<Arc<dyn BlobStore>> = Vec::new();
    for (cfg, location_uri) in select_unique_peers(
        gcs_peer_configs,
        &mut seen_peer_locations,
        "Cloud Storage bucket",
    ) {
        info!("Registering peer Cloud Storage bucket '{location_uri}'");
        peer_stores.push(Arc::new(GcsBlobStore::connect(&cfg).await?));
    }
    for (cfg, location_uri) in select_unique_peers(
        s3_peer_configs,
        &mut seen_peer_locations,
        "S3-compatible bucket",
    ) {
        info!(
            "Registering peer S3-compatible bucket '{location_uri}' (region={})",
            cfg.region
        );
        peer_stores.push(Arc::new(S3BlobStore::connect(&cfg).await?));
    }
    for (dir_str, loc_uri) in select_unique_peers(
        local_peer_specs
            .into_iter()
            .map(|(loc, dir)| (dir, loc))
            .collect(),
        &mut seen_peer_locations,
        "local blob directory",
    ) {
        info!("Registering peer local blob directory '{loc_uri}' -> '{dir_str}'");
        peer_stores.push(Arc::new(LocalFsBlobStore::open_with_location_id(
            dir_str, loc_uri,
        )?));
    }

    // The replication layer serves multi-bucket deployments; a single blob store reads directly
    // (including native ranged reads) unless a non-zero cache or retry window is requested.
    let cache_capacity_bytes = args.blob_cache_bytes.unwrap_or(if peer_stores.is_empty() {
        0
    } else {
        DEFAULT_BLOB_CACHE_BYTES
    });
    let blob_store: Arc<dyn BlobStore> =
        if !peer_stores.is_empty() || cache_capacity_bytes > 0 || args.gcs_replication_retry_ms > 0
        {
            let replicated = ReplicatedBlobStore::new(
                primary_blob_store,
                peer_stores,
                metadata_store.clone(),
                ReplicatedBlobStoreConfig {
                    cache_capacity_bytes,
                    replication_retry_budget: Duration::from_millis(args.gcs_replication_retry_ms),
                    initial_retry_backoff: Duration::from_millis(20),
                    async_write_fanout: args.gcs_async_write_fanout,
                },
            );
            metrics.attach_replication_stats(replicated.stats().clone());
            Arc::new(replicated)
        } else {
            primary_blob_store
        };

    let store = Arc::new(CompositeStore::with_config(
        metadata_store,
        blob_store,
        CompositeStoreConfig {
            inline_blob_threshold: args.inline_blob_threshold_bytes,
            validate_dependencies: true,
        },
    ));

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

    let (shutdown_tx, mut grpc_shutdown_rx) = tokio::sync::watch::channel(false);
    let (bound_metrics_addr, metrics_task) = if let Some(metrics_addr) = metrics_listen_addr {
        let mut metrics_shutdown_rx = grpc_shutdown_rx.clone();
        let (bound, handle) = bind_and_serve_metrics(metrics_addr, metrics.clone(), async move {
            let _ = metrics_shutdown_rx.wait_for(|&stopped| stopped).await;
        })
        .await?;
        (Some(bound), Some(handle))
    } else {
        (None, None)
    };

    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = shutdown_tx.send(true);
    });

    if let Some(bound) = bound_metrics_addr {
        if !bound.ip().is_loopback() {
            warn!(
                "Observability HTTP server (--metrics-listen) bound to non-loopback address {}; \
                 /healthz, /statz, and /metrics are unauthenticated",
                bound
            );
        }
        info!("jj-cc-server metrics server bound to {}", bound);
    }
    info!("jj-cc-server listening on {}", local_addr);

    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let backend_service = CommitCloudBackendService::with_metrics(store.clone(), metrics.clone())
        .with_actor_repo_suffix_enforcement(enforce_actor_repo_suffix);
    let op_store_service = CommitCloudOpStoreService::with_metrics(store.clone(), metrics.clone())
        .with_actor_repo_suffix_enforcement(enforce_actor_repo_suffix);
    let workspace_service = CommitCloudWorkspaceService::with_metrics(store, metrics.clone())
        .with_actor_repo_suffix_enforcement(enforce_actor_repo_suffix);

    Server::builder()
        .http2_max_header_list_size(64 * 1024)
        .layer(MetricsLayer::new(metrics))
        .layer(auth_layer)
        .add_service(health_service)
        .add_service(
            BackendServiceServer::new(backend_service)
                .max_decoding_message_size(cc_common::MAX_GRPC_MESSAGE_BYTES)
                .max_encoding_message_size(cc_common::MAX_GRPC_MESSAGE_BYTES),
        )
        .add_service(
            OpStoreServiceServer::new(op_store_service)
                .max_decoding_message_size(cc_common::MAX_GRPC_MESSAGE_BYTES)
                .max_encoding_message_size(cc_common::MAX_GRPC_MESSAGE_BYTES),
        )
        .add_service(
            WorkspaceServiceServer::new(workspace_service)
                .max_decoding_message_size(cc_common::MAX_GRPC_MESSAGE_BYTES)
                .max_encoding_message_size(cc_common::MAX_GRPC_MESSAGE_BYTES),
        )
        .serve_with_incoming_shutdown(incoming, async move {
            let _ = grpc_shutdown_rx.wait_for(|&stopped| stopped).await;
        })
        .await?;

    if let Some(mut handle) = metrics_task
        && tokio::time::timeout(Duration::from_secs(5), &mut handle)
            .await
            .is_err()
    {
        warn!(
            "Observability HTTP server did not shut down within 5s; aborting remaining connections"
        );
        handle.abort();
    }

    Ok(())
}

fn parse_s3_peer_configs(
    raw_specs: &[String],
    defaults: &S3ConnectConfig,
) -> Result<Vec<(S3ConnectConfig, String)>, String> {
    let mut out = Vec::new();
    for raw_spec in raw_specs {
        if !raw_spec.bytes().any(|b| b.is_ascii_control()) && raw_spec.trim().is_empty() {
            continue;
        }
        let cfg = parse_s3_peer_spec(raw_spec, defaults).map_err(|e| {
            let safe_spec = redact_s3_peer_spec(raw_spec);
            format!("invalid --s3-peer-buckets entry '{safe_spec}': {e}")
        })?;
        let canonical_bucket = normalize_s3_bucket(&cfg.bucket).map_err(|e| {
            let safe_spec = redact_s3_peer_spec(raw_spec);
            format!("invalid --s3-peer-buckets entry '{safe_spec}': {e}")
        })?;
        out.push((cfg, format!("s3://{canonical_bucket}")));
    }
    Ok(out)
}

fn select_unique_peers<T>(
    peers: Vec<(T, String)>,
    seen_peer_locations: &mut HashSet<String>,
    kind: &str,
) -> Vec<(T, String)> {
    let mut selected = Vec::with_capacity(peers.len());
    for (cfg, location_uri) in peers {
        if !seen_peer_locations.insert(location_uri.clone()) {
            info!("Skipping local or duplicate peer {kind} '{location_uri}'");
            continue;
        }
        selected.push((cfg, location_uri));
    }
    selected
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("installing SIGTERM handler should succeed");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("SIGINT received, shutting down...");
            }
            _ = sigterm.recv() => {
                info!("SIGTERM received, shutting down...");
            }
        }
    }
    #[cfg(not(unix))]
    {
        match tokio::signal::ctrl_c().await {
            Ok(()) => info!("Shutdown signal received, shutting down..."),
            Err(e) => tracing::warn!("Failed to listen for shutdown signal: {e}, shutting down..."),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_spanner_emulator_flag_defaults_false_and_parses_true() {
        let default_args = Args::try_parse_from(["jj-cc-server"]).unwrap();
        assert!(!default_args.spanner_emulator);

        let emu_args = Args::try_parse_from([
            "jj-cc-server",
            "--store-type=spanner",
            "--spanner-database=projects/p/instances/i/databases/d",
            "--spanner-endpoint=127.0.0.1:9010",
            "--spanner-emulator",
            "--spanner-auto-migrate",
            "--blob-store=memory",
        ])
        .unwrap();
        assert!(emu_args.spanner_emulator);
        assert!(emu_args.spanner_auto_migrate);
        assert_eq!(emu_args.spanner_endpoint.as_deref(), Some("127.0.0.1:9010"));
    }

    #[test]
    fn test_cli_inline_blob_threshold_bytes_enforces_4mib_upper_bound() {
        let max_arg = format!("--inline-blob-threshold-bytes={MAX_INLINE_BLOB_THRESHOLD_BYTES}");
        let at_max = Args::try_parse_from(["jj-cc-server", &max_arg]).unwrap();
        assert_eq!(
            at_max.inline_blob_threshold_bytes,
            MAX_INLINE_BLOB_THRESHOLD_BYTES
        );

        let over_arg = format!(
            "--inline-blob-threshold-bytes={}",
            MAX_INLINE_BLOB_THRESHOLD_BYTES + 1
        );
        let err = Args::try_parse_from(["jj-cc-server", &over_arg])
            .err()
            .expect("value above 4 MiB must be rejected");
        assert!(
            err.to_string().contains("exceeds maximum allowed value"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_s3_peer_configs_skip_primary_deduplicate_and_block_local_collisions() {
        let defaults = S3ConnectConfig {
            bucket: String::new(),
            endpoint: Some("http://127.0.0.1:9000".to_string()),
            region: "us-east-1".to_string(),
            access_key: None,
            secret_key: None,
            session_token: None,
            path_style: Some(true),
            auto_create_bucket: Some(false),
        };

        // Symmetric deployment where `--s3-peer-buckets` lists only the primary bucket in
        // multiple equivalent forms must produce zero peers (so ReplicatedBlobStore is not
        // spuriously enabled).
        let primary_only_specs = vec![
            "my-bucket".to_string(),
            "s3://my-bucket".to_string(),
            "s3://my-bucket/".to_string(),
            "   ".to_string(),
        ];
        let parsed = parse_s3_peer_configs(&primary_only_specs, &defaults).unwrap();
        let mut seen = HashSet::from(["s3://my-bucket".to_string()]);
        let selected = select_unique_peers(parsed, &mut seen, "S3-compatible bucket");
        assert!(
            selected.is_empty(),
            "primary bucket in --s3-peer-buckets must be skipped"
        );

        // Duplicate S3 peers keep the first entry and record canonical `s3://<bucket>` URIs in
        // `seen_peer_locations`, blocking later collisions with `--local-peer-blob-dirs`.
        let mixed_specs = vec![
            "s3://my-bucket".to_string(),
            "s3://peer-a:us-west-2=http://127.0.0.1:9001".to_string(),
            "peer-a:eu-west-1=http://127.0.0.1:9002".to_string(),
            "s3://peer-a/".to_string(),
            "peer-b".to_string(),
        ];
        let parsed = parse_s3_peer_configs(&mixed_specs, &defaults).unwrap();
        let mut seen = HashSet::from(["s3://my-bucket".to_string()]);
        let selected = select_unique_peers(parsed, &mut seen, "S3-compatible bucket");
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].1, "s3://peer-a");
        assert_eq!(selected[0].0.region, "us-west-2");
        assert_eq!(
            selected[0].0.endpoint.as_deref(),
            Some("http://127.0.0.1:9001")
        );
        assert_eq!(selected[1].1, "s3://peer-b");
        assert_eq!(selected[1].0.region, "us-east-1");

        let local_peers = vec![
            ("/tmp/dup".to_string(), "s3://peer-a".to_string()),
            ("/tmp/ok".to_string(), "s3://local-peer".to_string()),
        ];
        let selected_local = select_unique_peers(local_peers, &mut seen, "local blob directory");
        assert_eq!(
            selected_local,
            vec![("/tmp/ok".to_string(), "s3://local-peer".to_string())]
        );
    }
}
