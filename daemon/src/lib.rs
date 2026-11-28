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

//! Local per-user caching gRPC proxy daemon (jj-cc-daemon) for jj-commit-cloud.
//!
//! # Authority and Crash-Safety Ordering
//! jj-cc-daemon exposes `BackendService`, `OpStoreService`, and `WorkspaceService` over a
//! `0600` per-user Unix socket inside a `0700` state directory (or loopback TCP authenticated
//! by the random secret in `daemon.json`). Discovery-enabled daemons hold an exclusive
//! non-blocking `daemon.instance.lock` (`0600`) across their lifetime, publish `daemon.json`
//! atomically after binding listeners, and remove discovery files on exit only while owned by
//! this instance (`pid` and `started_unix_secs`) and matching the bound socket inode.
//!
//! # Concurrency and Merge Invariants
//! Unary immutable object reads (`Commit`, `Tree`, `Symlink`, `View`, and `Operation`) cache
//! across L1 RAM and L2 disk CAS with singleflight miss coalescing, while full cold `ReadFile`
//! misses stream directly through `BlobSpooler` without singleflight coalescing (and writes
//! populate the cache on success for all kinds except `Commit`). Mutable coordination RPCs
//! (`GetOpHeads`, `UpdateOpHeads`, `ReconcileOpHeads`, `PublishOperation`, and
//! `WorkspaceService`) forward straight to jj-cc-server so convergence stays linearizable.
//!
//! # Cross-Process and RPC Boundaries
//! Clients specify their target `server_url` per call via the `x-jj-cc-upstream` metadata
//! header (or fall back to `--upstream`), allowing a single background daemon per user to
//! serve multiple local workspaces and jj-vfs mounts across local and cloud servers.

#![allow(clippy::result_large_err)]

pub mod cache;
pub mod service;
pub mod upstream;

use std::fs;
use std::future::Future;
use std::io::Write as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use cc_common::backend::backend_service_server::BackendServiceServer;
use cc_common::op_store::op_store_service_server::OpStoreServiceServer;
use cc_common::workspace::workspace_service_server::WorkspaceServiceServer;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Server;

use crate::cache::TwoTierCache;
use crate::service::DaemonProxy;
use crate::upstream::UpstreamPool;

/// Maximum gRPC message size (`64 MiB`) matching `jj-cc-server`.
pub use cc_common::MAX_GRPC_MESSAGE_BYTES;

/// Metadata header carrying [`DaemonRuntimeInfo::secret`], required on the TCP listener.
pub const DAEMON_SECRET_HEADER: &str = "x-jj-cc-daemon-secret";

#[derive(Clone, Serialize, Deserialize)]
pub struct DaemonRuntimeInfo {
    pub pid: u32,
    pub socket_path: Option<String>,
    pub tcp_addr: Option<String>,
    pub cache_dir: String,
    pub stats_path: String,
    pub default_upstream: Option<String>,
    pub started_unix_secs: u64,
    /// Random per-daemon secret that TCP clients send in [`DAEMON_SECRET_HEADER`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret: Option<String>,
}

impl std::fmt::Debug for DaemonRuntimeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DaemonRuntimeInfo")
            .field("pid", &self.pid)
            .field("socket_path", &self.socket_path)
            .field("tcp_addr", &self.tcp_addr)
            .field("cache_dir", &self.cache_dir)
            .field("stats_path", &self.stats_path)
            .field("default_upstream", &self.default_upstream)
            .field("started_unix_secs", &self.started_unix_secs)
            .field("secret", &self.secret.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct DaemonConfig {
    pub state_dir: PathBuf,
    pub socket_path: Option<PathBuf>,
    pub listen_addr: Option<SocketAddr>,
    pub cache_dir: PathBuf,
    pub default_upstream: Option<String>,
    pub allowed_upstreams: Vec<String>,
    pub memory_cache_mb: usize,
    pub idle_timeout_secs: u64,
    /// Whether to lock `state_dir` and maintain `daemon.json` / `daemon.stats.json`.
    /// Disabled for legacy `--socket`-only invocations without `--state-dir` so ad-hoc sockets
    /// do not collide with or clobber a user's primary daemon discovery files.
    pub write_discovery_files: bool,
}

pub fn default_daemon_state_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("JJ_CC_DAEMON_DIR")
        && !dir.trim().is_empty()
    {
        return PathBuf::from(dir);
    }
    if let Ok(xdg_runtime) = std::env::var("XDG_RUNTIME_DIR")
        && !xdg_runtime.trim().is_empty()
    {
        return PathBuf::from(xdg_runtime).join("jj-commit-cloud");
    }
    // A per-user directory under the user's cache/home, never a predictable path under `/tmp`.
    let cache = default_daemon_cache_dir();
    if cache.as_os_str().is_empty() {
        PathBuf::new()
    } else {
        cache.join("run")
    }
}

pub fn default_daemon_cache_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("JJ_CC_CACHE_DIR")
        && !dir.trim().is_empty()
    {
        return PathBuf::from(dir);
    }
    if let Ok(xdg_cache) = std::env::var("XDG_CACHE_HOME")
        && !xdg_cache.trim().is_empty()
    {
        return PathBuf::from(xdg_cache).join("jj-commit-cloud");
    }
    if let Some(home) = std::env::home_dir()
        && !home.as_os_str().is_empty()
    {
        return home.join(".cache").join("jj-commit-cloud");
    }
    PathBuf::new()
}

pub async fn run_daemon(
    config: DaemonConfig,
    external_shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if cfg!(not(unix)) && config.socket_path.is_some() {
        return Err("Unix domain sockets (--socket) are only supported on Unix platforms".into());
    }
    if let Some(addr) = config.listen_addr
        && !addr.ip().is_loopback()
    {
        return Err(format!("refusing to listen on non-loopback address {addr}").into());
    }
    if !config.write_discovery_files
        && (config.socket_path.is_none() || config.listen_addr.is_some())
    {
        return Err(
            "TCP listeners require discovery files (daemon.json) to publish the bind address and secret"
                .into(),
        );
    }

    let (discovery_paths, _state_lock) = if config.write_discovery_files {
        ensure_private_dir(&config.state_dir)?;
        let lock = acquire_state_dir_lock(&config.state_dir)?;
        let info_path = config.state_dir.join("daemon.json");
        let stats_path = config.state_dir.join("daemon.stats.json");
        (Some((info_path, stats_path)), Some(lock))
    } else {
        (None, None)
    };
    ensure_private_dir(&config.cache_dir)?;

    let tcp_secret: Arc<str> = generate_daemon_secret()?.into();

    let cache = Arc::new(TwoTierCache::new(
        config.cache_dir.clone(),
        config.memory_cache_mb.saturating_mul(1024 * 1024),
    )?);
    let upstream = Arc::new(UpstreamPool::with_allowlist(
        config.default_upstream.clone(),
        &config.allowed_upstreams,
    )?);
    let proxy = DaemonProxy::new(
        Arc::clone(&cache),
        upstream,
        discovery_paths.as_ref().map(|(_, s)| s.clone()),
    );

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<BackendServiceServer<DaemonProxy>>()
        .await;
    health_reporter
        .set_serving::<OpStoreServiceServer<DaemonProxy>>()
        .await;
    health_reporter
        .set_serving::<WorkspaceServiceServer<DaemonProxy>>()
        .await;

    let idle_timeout_secs = config.idle_timeout_secs;
    let idle_cache = Arc::clone(&cache);
    let idle_proxy = proxy.clone();
    let shutdown_signal = async move {
        tokio::select! {
            _ = external_shutdown => {}
            _ = async move {
                if idle_timeout_secs == 0 {
                    std::future::pending::<()>().await;
                } else {
                    let limit_ms = idle_timeout_secs.saturating_mul(1000);
                    let poll_ms = (limit_ms / 4).clamp(50, 1000);
                    loop {
                        tokio::time::sleep(Duration::from_millis(poll_ms)).await;
                        if idle_proxy.active_rpcs() == 0 && idle_cache.idle_millis() >= limit_ms {
                            tracing::info!(
                                "jj-cc-daemon idle timeout ({idle_timeout_secs}s) reached; shutting down"
                            );
                            break;
                        }
                    }
                }
            } => {}
        }
    };

    let pid = std::process::id();
    let started_unix_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    #[cfg(unix)]
    if let Some(sock_path) = &config.socket_path {
        // Create a missing parent privately, but never change the mode of an existing
        // directory that `--socket` names.
        if let Some(parent) = sock_path.parent()
            && !parent.as_os_str().is_empty()
        {
            create_private_dir_all(parent)?;
        }
        if let Ok(meta) = fs::symlink_metadata(sock_path) {
            use std::os::unix::fs::FileTypeExt;
            let file_type = meta.file_type();
            if !file_type.is_socket() && !file_type.is_symlink() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!("refusing to unlink non-socket path {}", sock_path.display()),
                )
                .into());
            }
            if tokio::net::UnixStream::connect(sock_path).await.is_ok() {
                return Err(format!(
                    "jj-cc-daemon is already listening on {}",
                    sock_path.display()
                )
                .into());
            }
            if let Err(err) = fs::remove_file(sock_path)
                && err.kind() != std::io::ErrorKind::NotFound
            {
                return Err(err.into());
            }
        }
        let listener = tokio::net::UnixListener::bind(sock_path)?;
        let mut startup_sock_guard = StartupSocketGuard::new(sock_path);
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(sock_path, fs::Permissions::from_mode(0o600))?;
        }
        let sock_identity = socket_identity(sock_path);

        let bound_tcp = if let Some(addr) = config.listen_addr {
            let tcp = tokio::net::TcpListener::bind(addr).await?;
            Some(tcp)
        } else {
            None
        };
        let tcp_addr_str = bound_tcp
            .as_ref()
            .and_then(|t| t.local_addr().ok())
            .map(|a| format!("http://{a}"));

        if let Some((info_path, stats_path)) = &discovery_paths {
            write_runtime_info(
                info_path,
                &DaemonRuntimeInfo {
                    pid,
                    socket_path: Some(sock_path.to_string_lossy().into_owned()),
                    tcp_addr: tcp_addr_str,
                    cache_dir: config.cache_dir.to_string_lossy().into_owned(),
                    stats_path: stats_path.to_string_lossy().into_owned(),
                    default_upstream: config.default_upstream.clone(),
                    started_unix_secs,
                    secret: bound_tcp.as_ref().map(|_| tcp_secret.to_string()),
                },
            )?;
            startup_sock_guard.disarm();
            proxy.flush_stats_now();
        } else {
            startup_sock_guard.disarm();
        }

        // SAFETY: `geteuid` has no preconditions and cannot fail.
        let expected_uid = unsafe { libc::geteuid() };
        let uds_incoming = futures::stream::unfold(listener, move |l| async move {
            loop {
                match l.accept().await {
                    Ok((stream, _)) => match stream.peer_cred() {
                        Ok(cred) if cred.uid() == expected_uid => {
                            return Some((Ok(UnixConn(stream)), l));
                        }
                        Ok(cred) => {
                            tracing::warn!(
                                "rejecting Unix socket client with uid {} (expected {})",
                                cred.uid(),
                                expected_uid
                            );
                        }
                        Err(err) => {
                            tracing::warn!("rejecting Unix socket client without peer_cred: {err}");
                        }
                    },
                    Err(err) => return Some((Err(err), l)),
                }
            }
        });

        const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_millis(500);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let shutdown_trigger = tokio::spawn(async move {
            shutdown_signal.await;
            let _ = shutdown_tx.send(true);
        });

        let router = build_router(&proxy, health_service.clone(), None);
        let tcp_proxy = proxy.clone();
        let mut rx_uds = shutdown_rx.clone();
        let mut rx_drain = shutdown_rx.clone();
        let serve_fut = async move {
            if let Some(tcp_listener) = bound_tcp {
                let mut rx_tcp = shutdown_rx.clone();
                let tcp_incoming = futures::stream::unfold(tcp_listener, |l| async move {
                    let res = l.accept().await.map(|(s, _)| TcpConn::new(s));
                    Some((res, l))
                });
                let tcp_router = build_router(&tcp_proxy, health_service, Some(tcp_secret));
                let uds_fut = router.serve_with_incoming_shutdown(uds_incoming, async move {
                    let _ = rx_uds.wait_for(|&v| v).await;
                });
                let tcp_fut = tcp_router.serve_with_incoming_shutdown(tcp_incoming, async move {
                    let _ = rx_tcp.wait_for(|&v| v).await;
                });
                tokio::try_join!(uds_fut, tcp_fut).map(|_| ())
            } else {
                router
                    .serve_with_incoming_shutdown(uds_incoming, async move {
                        let _ = rx_uds.wait_for(|&v| v).await;
                    })
                    .await
            }
        };
        let serve_res = tokio::select! {
            res = serve_fut => res,
            _ = async move {
                let _ = rx_drain.wait_for(|&v| v).await;
                tokio::time::sleep(SHUTDOWN_DRAIN_TIMEOUT).await;
            } => Ok(()),
        };
        shutdown_trigger.abort();

        if let Some((info_path, _)) = &discovery_paths {
            let should_flush =
                !info_path.exists() || runtime_info_is_owned(info_path, pid, started_unix_secs);
            proxy.shutdown_stats(should_flush).await;
            remove_runtime_files_if_owned(
                info_path,
                pid,
                started_unix_secs,
                Some(sock_path),
                sock_identity,
            );
        } else {
            proxy.shutdown_stats(false).await;
            remove_socket_if_owned(sock_path, sock_identity);
        }
        serve_res?;
        return Ok(());
    }

    let (info_path, stats_path) = discovery_paths
        .expect("TCP-only mode validated write_discovery_files = true before listener setup");
    let addr = config
        .listen_addr
        .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 0)));
    let tcp_listener = tokio::net::TcpListener::bind(addr).await?;
    let local_addr = tcp_listener.local_addr()?;

    write_runtime_info(
        &info_path,
        &DaemonRuntimeInfo {
            pid,
            socket_path: None,
            tcp_addr: Some(format!("http://{local_addr}")),
            cache_dir: config.cache_dir.to_string_lossy().into_owned(),
            stats_path: stats_path.to_string_lossy().into_owned(),
            default_upstream: config.default_upstream.clone(),
            started_unix_secs,
            secret: Some(tcp_secret.to_string()),
        },
    )?;
    proxy.flush_stats_now();

    const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_millis(500);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let shutdown_trigger = tokio::spawn(async move {
        shutdown_signal.await;
        let _ = shutdown_tx.send(true);
    });
    let mut rx_tcp = shutdown_rx.clone();
    let mut rx_drain = shutdown_rx;
    let tcp_incoming = futures::stream::unfold(tcp_listener, |l| async move {
        let res = l.accept().await.map(|(s, _)| TcpConn::new(s));
        Some((res, l))
    });
    let serve_fut = build_router(&proxy, health_service, Some(tcp_secret))
        .serve_with_incoming_shutdown(tcp_incoming, async move {
            let _ = rx_tcp.wait_for(|&v| v).await;
        });
    let serve_res = tokio::select! {
        res = serve_fut => res,
        _ = async move {
            let _ = rx_drain.wait_for(|&v| v).await;
            tokio::time::sleep(SHUTDOWN_DRAIN_TIMEOUT).await;
        } => Ok(()),
    };
    shutdown_trigger.abort();

    let should_flush =
        !info_path.exists() || runtime_info_is_owned(&info_path, pid, started_unix_secs);
    proxy.shutdown_stats(should_flush).await;
    remove_runtime_files_if_owned(&info_path, pid, started_unix_secs, None, None);
    serve_res?;
    Ok(())
}

/// Requires [`DAEMON_SECRET_HEADER`] to match `expected` when set (the TCP listener). Unix
/// socket clients are authenticated by the socket and state directory permissions instead.
#[derive(Clone)]
struct DaemonSecretCheck {
    expected: Option<Arc<str>>,
}

impl tonic::service::Interceptor for DaemonSecretCheck {
    fn call(&mut self, request: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        let Some(expected) = &self.expected else {
            return Ok(request);
        };
        let mut headers = request.metadata().get_all(DAEMON_SECRET_HEADER).iter();
        let presented = match (headers.next(), headers.next()) {
            (Some(v), None) => Some(v.as_bytes()),
            _ => None,
        };
        if presented.is_some_and(|p| constant_time_eq(p, expected.as_bytes())) {
            Ok(request)
        } else {
            Err(tonic::Status::unauthenticated(format!(
                "missing or invalid {DAEMON_SECRET_HEADER} metadata"
            )))
        }
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = u8::from(a.len() != b.len());
    let max_len = a.len().max(b.len());
    for i in 0..max_len {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        diff |= std::hint::black_box(x ^ y);
    }
    std::hint::black_box(diff) == 0
}

fn generate_daemon_secret() -> std::io::Result<String> {
    let mut bytes = [0u8; 32];
    getrandom::getrandom(&mut bytes).map_err(|e| std::io::Error::other(e.to_string()))?;
    Ok(hex::encode(bytes))
}

fn build_router(
    proxy: &DaemonProxy,
    health_service: tonic_health::pb::health_server::HealthServer<
        impl tonic_health::pb::health_server::Health,
    >,
    required_secret: Option<Arc<str>>,
) -> tonic::transport::server::Router {
    let check = DaemonSecretCheck {
        expected: required_secret,
    };
    Server::builder()
        .add_service(InterceptedService::new(health_service, check.clone()))
        .add_service(InterceptedService::new(
            BackendServiceServer::new(proxy.clone())
                .max_decoding_message_size(MAX_GRPC_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_GRPC_MESSAGE_BYTES),
            check.clone(),
        ))
        .add_service(InterceptedService::new(
            OpStoreServiceServer::new(proxy.clone())
                .max_decoding_message_size(MAX_GRPC_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_GRPC_MESSAGE_BYTES),
            check.clone(),
        ))
        .add_service(InterceptedService::new(
            WorkspaceServiceServer::new(proxy.clone())
                .max_decoding_message_size(MAX_GRPC_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_GRPC_MESSAGE_BYTES),
            check,
        ))
}

/// Creates `dir` (mode `0700`) if missing and verifies that it is a directory owned by the
/// current user without group or other access. Whoever controls this directory controls
/// `daemon.json` and the socket, so the daemon refuses to start otherwise.
fn ensure_private_dir(dir: &Path) -> std::io::Result<()> {
    create_private_dir_all(dir)?;
    verify_private_dir(dir)
}

/// Creates missing directories with mode `0700`; existing directories keep their mode.
fn create_private_dir_all(dir: &Path) -> std::io::Result<()> {
    if dir.as_os_str().is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "cannot determine private per-user daemon directory; set XDG_RUNTIME_DIR, HOME, or JJ_CC_DAEMON_DIR",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
        // Strip trailing slashes so `symlink_metadata` and `O_NOFOLLOW` inspect the leaf itself.
        let target: PathBuf = dir.components().collect();
        let created_leaf = fs::symlink_metadata(&target).is_err();
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&target)?;
        if created_leaf {
            // Open with `O_DIRECTORY | O_NOFOLLOW` and verify ownership on the open descriptor
            // before `fchmod` so a symlink raced into place before `DirBuilder::create` cannot
            // cause us to modify another directory's permissions.
            let fd = fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
                .open(&target)
                .map_err(|err| {
                    if matches!(err.raw_os_error(), Some(libc::ELOOP | libc::ENOTDIR)) {
                        std::io::Error::new(
                            std::io::ErrorKind::PermissionDenied,
                            format!("{} must be a real directory: {err}", target.display()),
                        )
                    } else {
                        err
                    }
                })?;
            let meta = fd.metadata()?;
            // SAFETY: `geteuid` has no preconditions and cannot fail.
            let euid = unsafe { libc::geteuid() };
            if !meta.is_dir() || meta.uid() != euid {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!(
                        "{} must be a directory owned by uid {euid} (found uid {})",
                        target.display(),
                        meta.uid()
                    ),
                ));
            }
            fd.set_permissions(fs::Permissions::from_mode(0o700))?;
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        fs::create_dir_all(dir)
    }
}

#[cfg(unix)]
fn verify_private_dir(dir: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    // Strip trailing slashes so `symlink_metadata` inspects the leaf itself instead of
    // resolving through a symlinked directory path such as `/path/to/link/`.
    let target: PathBuf = dir.components().collect();
    let meta = fs::symlink_metadata(&target)?;
    // SAFETY: `geteuid` has no preconditions and cannot fail.
    let euid = unsafe { libc::geteuid() };
    let mode = meta.permissions().mode() & 0o7777;
    if meta.file_type().is_symlink() || !meta.is_dir() || meta.uid() != euid || mode & 0o077 != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "{} must be a directory owned by uid {euid} with mode 0700 (found uid {}, mode \
                 {mode:o})",
                target.display(),
                meta.uid()
            ),
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn verify_private_dir(_dir: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(unix)]
struct StartupSocketGuard<'a> {
    path: Option<&'a Path>,
}

#[cfg(unix)]
impl<'a> StartupSocketGuard<'a> {
    fn new(path: &'a Path) -> Self {
        Self { path: Some(path) }
    }

    fn disarm(&mut self) {
        self.path = None;
    }
}

#[cfg(unix)]
impl Drop for StartupSocketGuard<'_> {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = fs::remove_file(path);
        }
    }
}

fn write_runtime_info(path: &Path, info: &DaemonRuntimeInfo) -> std::io::Result<()> {
    static TMP_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let bytes = serde_json::to_vec_pretty(info).map_err(std::io::Error::other)?;
    let seq = TMP_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let tmp = path.with_extension(format!("json.tmp.{}.{seq}", std::process::id()));
    let _ = fs::remove_file(&tmp);
    let res = (|| -> std::io::Result<()> {
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            // `daemon.json` holds the TCP secret.
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        {
            let mut file = options.open(&tmp)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
        }
        fs::rename(&tmp, path)?;
        Ok(())
    })();
    if res.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    res
}

/// Acquires an exclusive non-blocking lock on `state_dir/daemon.instance.lock` (mode `0600`)
/// so a second daemon targeting the same `state_dir` (UDS or TCP-only) fails before binding
/// listeners or modifying `daemon.json` / `daemon.stats.json`.
fn acquire_state_dir_lock(state_dir: &Path) -> std::io::Result<fs::File> {
    let lock_path = state_dir.join("daemon.instance.lock");
    let mut opts = fs::OpenOptions::new();
    opts.read(true).write(true).create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let file = opts.open(&lock_path)?;
    match file.try_lock() {
        Ok(()) => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mode = file.metadata()?.permissions().mode() & 0o777;
                if mode != 0o600 {
                    file.set_permissions(fs::Permissions::from_mode(0o600))?;
                }
            }
            Ok(file)
        }
        Err(fs::TryLockError::WouldBlock) => Err(std::io::Error::new(
            std::io::ErrorKind::AddrInUse,
            format!(
                "jj-cc-daemon is already listening in state directory {}",
                state_dir.display()
            ),
        )),
        Err(fs::TryLockError::Error(err)) => Err(err),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SocketIdentity {
    #[cfg(unix)]
    dev: u64,
    #[cfg(unix)]
    ino: u64,
}

#[cfg(unix)]
fn socket_identity(path: &Path) -> Option<SocketIdentity> {
    use std::os::unix::fs::MetadataExt;
    let meta = fs::symlink_metadata(path).ok()?;
    Some(SocketIdentity {
        dev: meta.dev(),
        ino: meta.ino(),
    })
}

fn remove_socket_if_owned(sock_path: &Path, expected_identity: Option<SocketIdentity>) {
    #[cfg(unix)]
    if let Some(expected) = expected_identity
        && socket_identity(sock_path) != Some(expected)
    {
        return;
    }
    #[cfg(not(unix))]
    let _ = expected_identity;
    let _ = fs::remove_file(sock_path);
}

fn runtime_info_is_owned(
    info_path: &Path,
    expected_pid: u32,
    expected_started_unix_secs: u64,
) -> bool {
    fs::read(info_path)
        .ok()
        .and_then(|raw| serde_json::from_slice::<DaemonRuntimeInfo>(&raw).ok())
        .is_some_and(|info| {
            info.pid == expected_pid && info.started_unix_secs == expected_started_unix_secs
        })
}

/// Removes `daemon.json` (and the socket) only while `daemon.json` still names this daemon
/// instance (`pid` and `started_unix_secs`) and `sock_path` still matches the bound socket inode,
/// so an exiting daemon never deletes the discovery files or socket of a daemon that replaced it.
fn remove_runtime_files_if_owned(
    info_path: &Path,
    expected_pid: u32,
    expected_started_unix_secs: u64,
    sock_path: Option<&Path>,
    sock_identity: Option<SocketIdentity>,
) {
    if runtime_info_is_owned(info_path, expected_pid, expected_started_unix_secs) {
        if let Some(sock_path) = sock_path {
            remove_socket_if_owned(sock_path, sock_identity);
        }
        let _ = fs::remove_file(info_path);
    } else if !info_path.exists()
        && let Some(sock_path) = sock_path
        && sock_identity.is_some()
    {
        remove_socket_if_owned(sock_path, sock_identity);
    }
}

#[cfg(unix)]
struct UnixConn(tokio::net::UnixStream);

#[cfg(unix)]
impl tonic::transport::server::Connected for UnixConn {
    type ConnectInfo = ();
    fn connect_info(&self) -> Self::ConnectInfo {}
}

#[cfg(unix)]
impl AsyncRead for UnixConn {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

#[cfg(unix)]
impl AsyncWrite for UnixConn {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

struct TcpConn(tokio::net::TcpStream);

impl TcpConn {
    fn new(stream: tokio::net::TcpStream) -> Self {
        let _ = stream.set_nodelay(true);
        Self(stream)
    }
}

impl tonic::transport::server::Connected for TcpConn {
    type ConnectInfo = Option<SocketAddr>;
    fn connect_info(&self) -> Self::ConnectInfo {
        self.0.peer_addr().ok()
    }
}

impl AsyncRead for TcpConn {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for TcpConn {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests;
