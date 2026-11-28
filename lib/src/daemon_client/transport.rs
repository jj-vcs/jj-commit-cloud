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

//! Local Unix domain socket and loopback TCP connectors, gRPC health probes, and
//! in-process route caching for jj-cc-daemon.
//!
//! # Concurrency and Merge Invariants
//! `DAEMON_ROUTE_CACHE` caches either a verified HTTP/2 `Channel` with a bounded
//! liveness TTL (`DAEMON_ROUTE_LIVENESS_TTL`) or a negative backoff window
//! (`DAEMON_UNAVAILABLE_BACKOFF`) per `state_dir`, while `SPAWN_LOCK` serializes
//! concurrent in-process auto-spawn attempts.
//!
//! # Cross-Process and RPC Boundaries
//! Unix domain socket connections verify peer UID and expected PID through
//! `SO_PEERCRED`. Loopback TCP connections validate loopback-only URIs, check PID
//! ownership, and attach `DAEMON_SECRET_HEADER` during `tonic_health` probes.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::sync::Arc;
#[cfg(unix)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex as StdMutex};
use std::time::{Duration, Instant};

use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};

use super::lifecycle::{
    PidMatch, acquire_daemon_spawn_lock, check_pid_matches_daemon, spawn_and_wait_daemon,
};
use super::{
    DAEMON_SECRET_HEADER, DAEMON_WIRE_VERSION_HEADER, DAEMON_WIRE_VERSION_STR, DaemonRuntimeInfo,
    check_daemon_wire_version, default_daemon_cache_dir, default_daemon_state_dir,
    ensure_private_state_dir, read_daemon_runtime_info, verify_private_state_dir,
};

const DAEMON_UNAVAILABLE_BACKOFF: Duration = Duration::from_secs(30);
pub(super) const DAEMON_ROUTE_LIVENESS_TTL: Duration = Duration::from_secs(2);

static SPAWN_LOCK: Mutex<()> = Mutex::const_new(());
pub(super) static DAEMON_ROUTE_CACHE: LazyLock<StdMutex<HashMap<PathBuf, CachedDaemonState>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

#[derive(Clone)]
pub(super) enum CachedDaemonState {
    Connected {
        info: DaemonRuntimeInfo,
        channel: Channel,
        verified_at: Instant,
    },
    Unavailable {
        retry_after: Instant,
    },
}

pub(super) fn is_loopback_tcp_url(raw: &str) -> bool {
    let Ok(uri) = raw.trim().parse::<tonic::transport::Uri>() else {
        return false;
    };
    if !matches!(uri.scheme_str(), Some(s) if s.eq_ignore_ascii_case("http")) {
        return false;
    }
    let Some(authority) = uri.authority() else {
        return false;
    };
    if authority.as_str().contains('@') || authority.port_u16().is_none() {
        return false;
    }
    if let Some(pq) = uri.path_and_query()
        && pq.as_str() != "/"
    {
        return false;
    }
    let host = authority.host();
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    if let Some(bracketed) = host.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
        return bracketed
            .parse::<std::net::Ipv6Addr>()
            .is_ok_and(|ip| ip.is_loopback());
    }
    host.parse::<std::net::IpAddr>()
        .is_ok_and(|ip| ip.is_loopback())
}

pub struct ExistingDaemonConnection {
    pub info: DaemonRuntimeInfo,
    pub channel: Channel,
    /// True when `info.pid` was matched against the Unix domain socket peer's
    /// `SO_PEERCRED` PID and the peer answered a `tonic_health` probe as serving.
    pub peer_pid_verified: bool,
    /// Set when connecting across version mismatches (`allow_version_mismatch = true`) to a live
    /// daemon whose `daemon.json` `wire_version` or health probe reported a wire-version mismatch.
    pub wire_version_mismatch: Option<String>,
}

pub async fn connect_existing_daemon_verified(
    state_dir: &Path,
) -> Result<Option<ExistingDaemonConnection>, Box<dyn std::error::Error + Send + Sync>> {
    connect_existing_daemon_verified_with(state_dir, check_pid_matches_daemon, false).await
}

/// Connects to a live daemon in `state_dir` without failing on a `wire_version` mismatch.
/// Used by `jj cc daemon stop`, `jj cc daemon status`, and `jj cc daemon clear-cache` so users
/// can inspect and stop a daemon running a different wire version.
pub async fn connect_existing_daemon_any_version_verified(
    state_dir: &Path,
) -> Result<Option<ExistingDaemonConnection>, Box<dyn std::error::Error + Send + Sync>> {
    connect_existing_daemon_verified_with(state_dir, check_pid_matches_daemon, true).await
}

pub(super) async fn connect_existing_daemon_verified_with<F>(
    state_dir: &Path,
    pid_check: F,
    allow_version_mismatch: bool,
) -> Result<Option<ExistingDaemonConnection>, Box<dyn std::error::Error + Send + Sync>>
where
    F: FnOnce(u32) -> PidMatch + Send + 'static,
{
    if state_dir.as_os_str().is_empty() {
        return Err("cannot determine private per-user daemon directory".into());
    }
    #[cfg(unix)]
    let meta_path = super::strip_trailing_slashes(state_dir);
    #[cfg(not(unix))]
    let meta_path = state_dir;
    match fs::symlink_metadata(meta_path) {
        Ok(_) => verify_private_state_dir(state_dir)?,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    }

    let raw = match fs::read_to_string(state_dir.join("daemon.json")) {
        Ok(s) => s,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let Ok(info) = serde_json::from_str::<DaemonRuntimeInfo>(&raw) else {
        return Ok(None);
    };
    let version_check = check_daemon_wire_version(&info);
    // Probe with an unversioned health check when `daemon.json` already advertises a different
    // wire version so we can distinguish a live mismatched daemon from a stale `daemon.json`
    // (and let `jj cc daemon stop` verify SO_PEERCRED / health before signaling).
    let probe_with_version = version_check.is_ok();

    #[cfg(unix)]
    if let Some(sock_str) = &info.socket_path {
        use std::os::unix::fs::FileTypeExt;
        let sock_path = PathBuf::from(sock_str);
        if fs::symlink_metadata(&sock_path).is_ok_and(|m| m.file_type().is_socket())
            && let Ok((ch, peer_pid_verified)) =
                connect_unix_channel(&sock_path, Some(info.pid)).await
        {
            let live_outcome =
                probe_daemon_health_inner(ch.clone(), None, probe_with_version).await;
            match live_outcome {
                HealthProbeResult::Serving => {
                    if !allow_version_mismatch && let Err(mismatch) = version_check {
                        return Err(mismatch.into());
                    }
                    return Ok(Some(ExistingDaemonConnection {
                        info,
                        channel: ch,
                        peer_pid_verified: peer_pid_verified.load(Ordering::Acquire),
                        wire_version_mismatch: version_check.err(),
                    }));
                }
                HealthProbeResult::VersionMismatch(msg) => {
                    if !allow_version_mismatch {
                        return Err(msg.into());
                    }
                    if matches!(
                        probe_daemon_health_inner(ch.clone(), None, false).await,
                        HealthProbeResult::Serving
                    ) {
                        return Ok(Some(ExistingDaemonConnection {
                            info,
                            channel: ch,
                            peer_pid_verified: peer_pid_verified.load(Ordering::Acquire),
                            wire_version_mismatch: Some(msg),
                        }));
                    }
                }
                HealthProbeResult::Unavailable => {}
            }
        }
        return Ok(None);
    }

    if let Some(tcp_url) = &info.tcp_addr
        && is_loopback_tcp_url(tcp_url)
    {
        let pid = info.pid;
        let pid_match = tokio::task::spawn_blocking(move || pid_check(pid))
            .await
            .unwrap_or(PidMatch::Unknown);
        let has_secret = info.secret.as_deref().is_some_and(|s| !s.is_empty());
        // Never send the per-daemon TCP secret to a PID confirmed not to be `jj-cc-daemon`.
        // When OS inspection is unavailable (`Unknown`), require a non-empty `info.secret` so
        // `probe_daemon_health_inner` authenticates the listener.
        let pid_ok = match pid_match {
            PidMatch::ConfirmedDaemon => true,
            PidMatch::Unknown => has_secret,
            PidMatch::NotDaemon => false,
        };
        if pid_ok
            && let Ok(ep) = Endpoint::from_shared(tcp_url.clone())
            && let Ok(ch) = ep
                .tcp_nodelay(true)
                .connect_timeout(Duration::from_secs(5))
                .http2_keep_alive_interval(Duration::from_secs(300))
                .keep_alive_timeout(Duration::from_secs(20))
                .connect()
                .await
        {
            let live_outcome =
                probe_daemon_health_inner(ch.clone(), info.secret.as_deref(), probe_with_version)
                    .await;
            match live_outcome {
                HealthProbeResult::Serving => {
                    if !allow_version_mismatch && let Err(mismatch) = version_check {
                        return Err(mismatch.into());
                    }
                    return Ok(Some(ExistingDaemonConnection {
                        info,
                        channel: ch,
                        peer_pid_verified: false,
                        wire_version_mismatch: version_check.err(),
                    }));
                }
                HealthProbeResult::VersionMismatch(msg) => {
                    if !allow_version_mismatch {
                        return Err(msg.into());
                    }
                    if matches!(
                        probe_daemon_health_inner(ch.clone(), info.secret.as_deref(), false).await,
                        HealthProbeResult::Serving
                    ) {
                        return Ok(Some(ExistingDaemonConnection {
                            info,
                            channel: ch,
                            peer_pid_verified: false,
                            wire_version_mismatch: Some(msg),
                        }));
                    }
                }
                HealthProbeResult::Unavailable => {}
            }
        }
    }

    Ok(None)
}

pub async fn connect_existing_daemon(
    state_dir: &Path,
) -> Result<Option<(DaemonRuntimeInfo, Channel)>, Box<dyn std::error::Error + Send + Sync>> {
    Ok(connect_existing_daemon_verified(state_dir)
        .await?
        .map(|conn| (conn.info, conn.channel)))
}

pub async fn probe_daemon_channel_health(channel: Channel, secret: Option<&str>) -> bool {
    matches!(
        probe_daemon_health_inner(channel, secret, true).await,
        HealthProbeResult::Serving
    )
}

/// Probes daemon health without attaching [`DAEMON_WIRE_VERSION_HEADER`] so `jj cc daemon stop`
/// can observe shutdown of a daemon running a different wire protocol version.
pub async fn probe_daemon_channel_health_any_version(
    channel: Channel,
    secret: Option<&str>,
) -> bool {
    matches!(
        probe_daemon_health_inner(channel, secret, false).await,
        HealthProbeResult::Serving
    )
}

enum HealthProbeResult {
    Serving,
    VersionMismatch(String),
    Unavailable,
}

async fn probe_daemon_health_inner(
    channel: Channel,
    secret: Option<&str>,
    send_wire_version: bool,
) -> HealthProbeResult {
    use tonic_health::pb::HealthCheckRequest;
    use tonic_health::pb::health_check_response::ServingStatus;
    use tonic_health::pb::health_client::HealthClient;

    let mut client = HealthClient::new(channel);
    let mut req = tonic::Request::new(HealthCheckRequest {
        service: String::new(),
    });
    if send_wire_version {
        req.metadata_mut().insert(
            DAEMON_WIRE_VERSION_HEADER,
            tonic::metadata::MetadataValue::from_static(DAEMON_WIRE_VERSION_STR),
        );
    }
    if let Some(secret) = secret {
        let Ok(mut val) = tonic::metadata::MetadataValue::try_from(secret) else {
            return HealthProbeResult::Unavailable;
        };
        val.set_sensitive(true);
        req.metadata_mut().insert(DAEMON_SECRET_HEADER, val);
    }
    match tokio::time::timeout(Duration::from_secs(2), client.check(req)).await {
        Ok(Ok(resp)) if resp.get_ref().status() == ServingStatus::Serving => {
            HealthProbeResult::Serving
        }
        Ok(Err(status))
            if send_wire_version && status.code() == tonic::Code::FailedPrecondition =>
        {
            HealthProbeResult::VersionMismatch(status.message().to_string())
        }
        _ => HealthProbeResult::Unavailable,
    }
}

async fn check_cached_route(
    state_dir: &Path,
) -> Option<Result<(DaemonRuntimeInfo, Channel), Box<dyn std::error::Error + Send + Sync>>> {
    check_cached_route_with(state_dir, check_pid_matches_daemon).await
}

pub(super) async fn check_cached_route_with<F>(
    state_dir: &Path,
    pid_check: F,
) -> Option<Result<(DaemonRuntimeInfo, Channel), Box<dyn std::error::Error + Send + Sync>>>
where
    F: FnOnce(u32) -> PidMatch + Send + 'static,
{
    let stale_candidate = {
        let mut guard = DAEMON_ROUTE_CACHE.lock().unwrap();
        match guard.get(state_dir)? {
            CachedDaemonState::Connected {
                info,
                channel,
                verified_at,
            } => {
                if verified_at.elapsed() < DAEMON_ROUTE_LIVENESS_TTL {
                    return Some(Ok((info.clone(), channel.clone())));
                }
                Some((info.clone(), channel.clone()))
            }
            CachedDaemonState::Unavailable { retry_after } => {
                if Instant::now() < *retry_after {
                    return Some(Err("jj-cc-daemon unavailable (cached backoff)".into()));
                }
                guard.remove(state_dir);
                return None;
            }
        }
    };

    let (info, channel) = stale_candidate?;
    // Probe disk metadata and OS process state outside `DAEMON_ROUTE_CACHE` and off the
    // Tokio worker threads so concurrent RPCs never block behind filesystem or subprocess I/O.
    let state_dir_owned = state_dir.to_path_buf();
    let expected_pid = info.pid;
    let expected_started = info.started_unix_secs;
    let liveness = tokio::task::spawn_blocking(move || {
        let same_record = read_daemon_runtime_info(&state_dir_owned).is_some_and(|on_disk| {
            on_disk.pid == expected_pid && on_disk.started_unix_secs == expected_started
        });
        if !same_record {
            PidMatch::NotDaemon
        } else {
            pid_check(expected_pid)
        }
    })
    .await
    .unwrap_or(PidMatch::Unknown);

    let still_live = match liveness {
        PidMatch::ConfirmedDaemon => true,
        PidMatch::NotDaemon => false,
        PidMatch::Unknown => {
            probe_daemon_channel_health(channel.clone(), info.secret.as_deref()).await
        }
    };

    let mut guard = DAEMON_ROUTE_CACHE.lock().unwrap();
    let matches_cached = matches!(
        guard.get(state_dir),
        Some(CachedDaemonState::Connected { info: cur, .. })
            if cur.pid == info.pid && cur.started_unix_secs == info.started_unix_secs
    );
    if still_live {
        if matches_cached
            && let Some(CachedDaemonState::Connected { verified_at, .. }) = guard.get_mut(state_dir)
        {
            *verified_at = Instant::now();
        }
        Some(Ok((info, channel)))
    } else {
        if matches_cached {
            guard.remove(state_dir);
        }
        None
    }
}

pub(super) async fn connect_or_spawn_in(
    state_dir: &Path,
    cache_dir: &Path,
    default_upstream: &str,
) -> Result<(DaemonRuntimeInfo, Channel), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(cached) = check_cached_route(state_dir).await {
        return cached;
    }

    let _in_proc_guard = SPAWN_LOCK.lock().await;
    if let Some(cached) = check_cached_route(state_dir).await {
        return cached;
    }

    let attempt: Result<(DaemonRuntimeInfo, Channel), Box<dyn std::error::Error + Send + Sync>> =
        async {
            ensure_private_state_dir(state_dir)?;
            if let Some(ready) = connect_existing_daemon(state_dir).await? {
                return Ok(ready);
            }
            let _cross_proc_guard = acquire_daemon_spawn_lock(state_dir).await?;
            if let Some(ready) = connect_existing_daemon(state_dir).await? {
                return Ok(ready);
            }
            spawn_and_wait_daemon(
                state_dir,
                cache_dir,
                Some(default_upstream),
                None,
                None,
                None,
                None,
            )
            .await
        }
        .await;

    let mut cache_guard = DAEMON_ROUTE_CACHE.lock().unwrap();
    match attempt {
        Ok((info, ch)) => {
            cache_guard.insert(
                state_dir.to_path_buf(),
                CachedDaemonState::Connected {
                    info: info.clone(),
                    channel: ch.clone(),
                    verified_at: Instant::now(),
                },
            );
            Ok((info, ch))
        }
        Err(err) => {
            tracing::warn!(
                "jj-cc-daemon unavailable in {}, falling back to direct connection: {err}",
                state_dir.display()
            );
            cache_guard.insert(
                state_dir.to_path_buf(),
                CachedDaemonState::Unavailable {
                    retry_after: Instant::now() + DAEMON_UNAVAILABLE_BACKOFF,
                },
            );
            Err(err)
        }
    }
}

pub async fn connect_or_spawn_daemon(
    default_upstream: &str,
) -> Result<(DaemonRuntimeInfo, Channel), Box<dyn std::error::Error + Send + Sync>> {
    let state_dir = default_daemon_state_dir();
    let cache_dir = default_daemon_cache_dir();
    connect_or_spawn_in(&state_dir, &cache_dir, default_upstream).await
}

#[cfg(unix)]
async fn connect_unix_channel(
    socket_path: &Path,
    expected_pid: Option<u32>,
) -> Result<(Channel, Arc<AtomicBool>), Box<dyn std::error::Error + Send + Sync>> {
    use hyper_util::rt::TokioIo;
    use tower::service_fn;

    let path = cc_common::resolve_unix_socket_path(socket_path)?;
    let check_initial_pid = Arc::new(AtomicBool::new(true));
    let peer_pid_verified = Arc::new(AtomicBool::new(false));
    let peer_pid_verified_for_conn = Arc::clone(&peer_pid_verified);
    let endpoint = Endpoint::try_from("http://[::]:50051")?
        .connect_timeout(Duration::from_secs(5))
        .http2_keep_alive_interval(Duration::from_secs(300))
        .keep_alive_timeout(Duration::from_secs(20));

    let channel = endpoint
        .connect_with_connector(service_fn(move |_: tonic::transport::Uri| {
            let p = path.clone();
            let verify_pid = Arc::clone(&check_initial_pid);
            let verified_flag = Arc::clone(&peer_pid_verified_for_conn);
            async move {
                let stream = tokio::net::UnixStream::connect(p).await?;
                let check_pid_now = verify_pid.swap(false, Ordering::Relaxed);
                let cred = stream.peer_cred()?;
                // SAFETY: `geteuid` has no preconditions and cannot fail.
                let euid = unsafe { libc::geteuid() };
                if cred.uid() != euid {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        format!(
                            "daemon socket peer uid {} does not match current uid {euid}",
                            cred.uid()
                        ),
                    ));
                }
                if check_pid_now
                    && let (Some(expected), Some(actual)) = (expected_pid, cred.pid())
                    && actual > 0
                {
                    if (actual as u32) != expected {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::ConnectionRefused,
                            format!(
                                "daemon socket peer pid {actual} does not match daemon.json pid {expected}"
                            ),
                        ));
                    }
                    verified_flag.store(true, Ordering::Release);
                }
                Ok::<_, std::io::Error>(TokioIo::new(stream))
            }
        }))
        .await?;
    Ok((channel, peer_pid_verified))
}
