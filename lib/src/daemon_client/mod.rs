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

//! Local IPC discovery, on-demand process spawning, and Unix socket or loopback TCP
//! transport connector for jj-cc-daemon.
//!
//! # Authority and Crash-Safety Ordering
//! Runtime discovery reads `<state_dir>/daemon.json` after verifying that `<state_dir>` is a
//! directory owned by the current user with mode `0700` on Unix. Cross-process auto-spawn
//! serializes through an advisory `std::fs::File::lock()` on `<state_dir>/daemon.spawn.lock`,
//! which the kernel releases if the spawning client exits or receives `SIGINT`.
//!
//! # Concurrency and Merge Invariants
//! `DAEMON_ROUTE_CACHE` records either the connected HTTP/2 `tonic::transport::Channel` or a bounded
//! negative backoff per `state_dir` so concurrent RPCs inside one process share a single
//! connection and never repeat spawn attempts on every call when the daemon cannot start.
//!
//! # Cross-Process and RPC Boundaries
//! Spawned daemons run in an independent process group (`process_group(0)` on Unix) with
//! stdout and stderr appended to `<state_dir>/daemon.log` (`0600`). Unix domain socket
//! paths are parent-canonicalized and bounded to POSIX `sun_path` limits, verify peer UID
//! and PID via `SO_PEERCRED`, and negotiate [`DAEMON_WIRE_VERSION`] alongside loopback TCP
//! [`DAEMON_SECRET_HEADER`] auth.

use std::fmt;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex as StdMutex};

use serde::{Deserialize, Serialize};

mod lifecycle;
mod transport;

pub use lifecycle::{
    DaemonSpawnLockGuard, PidMatch, acquire_daemon_spawn_lock, check_pid_matches_daemon,
    locate_daemon_binary, pid_matches_daemon, spawn_and_wait_daemon, spawn_daemon_background,
    terminate_daemon_pid, terminate_daemon_pid_with_verification,
};
pub use transport::{
    ExistingDaemonConnection, connect_existing_daemon,
    connect_existing_daemon_any_version_verified, connect_existing_daemon_verified,
    connect_or_spawn_daemon, probe_daemon_channel_health, probe_daemon_channel_health_any_version,
};

pub use cc_common::{DAEMON_WIRE_VERSION, DAEMON_WIRE_VERSION_HEADER, DAEMON_WIRE_VERSION_STR};

pub const UPSTREAM_HEADER: &str = "x-jj-cc-upstream";
pub const DAEMON_SECRET_HEADER: &str = "x-jj-cc-daemon-secret";
static DAEMON_PREF_CACHE: LazyLock<StdMutex<Option<(PathBuf, bool)>>> =
    LazyLock::new(|| StdMutex::new(None));

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UserDaemonPreference {
    pub enabled: bool,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct DaemonRuntimeInfo {
    pub pid: u32,
    pub socket_path: Option<String>,
    pub tcp_addr: Option<String>,
    pub cache_dir: String,
    pub stats_path: String,
    pub default_upstream: Option<String>,
    pub started_unix_secs: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wire_version: Option<u32>,
}

/// Checks whether a running daemon's advertised `wire_version` matches [`DAEMON_WIRE_VERSION`].
/// A missing `wire_version` (`None`) is allowed for backward compatibility with older daemons.
pub fn check_daemon_wire_version(info: &DaemonRuntimeInfo) -> Result<(), String> {
    if let Some(version) = info.wire_version
        && version != DAEMON_WIRE_VERSION
    {
        return Err(format!(
            "CLI/daemon wire version mismatch: running jj-cc-daemon (pid {}) uses wire version {version}, but this CLI expects wire version {DAEMON_WIRE_VERSION}; run `jj cc daemon stop` to restart the daemon",
            info.pid
        ));
    }
    Ok(())
}

impl fmt::Debug for DaemonRuntimeInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DaemonRuntimeInfo")
            .field("pid", &self.pid)
            .field("socket_path", &self.socket_path)
            .field("tcp_addr", &self.tcp_addr)
            .field("cache_dir", &self.cache_dir)
            .field("stats_path", &self.stats_path)
            .field("default_upstream", &self.default_upstream)
            .field("started_unix_secs", &self.started_unix_secs)
            .field("secret", &self.secret.as_ref().map(|_| "<redacted>"))
            .field("wire_version", &self.wire_version)
            .finish()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DaemonStatsView {
    pub l1_hits: u64,
    pub l2_hits: u64,
    pub misses: u64,
    pub singleflight_coalesced: u64,
    pub write_through_objects: u64,
    pub upstream_rpcs: u64,
    pub l1_bytes: usize,
    pub l1_entries: usize,
    #[serde(default)]
    pub l2_write_errors: u64,
    #[serde(default)]
    pub l2_evicted_objects: u64,
    #[serde(default)]
    pub l2_evicted_bytes: u64,
}

impl DaemonStatsView {
    pub fn hit_ratio_percent(&self) -> f64 {
        let total = self.l1_hits + self.l2_hits + self.misses;
        if total == 0 {
            0.0
        } else {
            ((self.l1_hits + self.l2_hits) as f64) * 100.0 / (total as f64)
        }
    }
}

pub fn default_daemon_state_dir() -> PathBuf {
    default_daemon_state_dir_from(
        std::env::var("JJ_CC_DAEMON_DIR").ok().as_deref(),
        std::env::var("XDG_RUNTIME_DIR").ok().as_deref(),
        &default_daemon_cache_dir(),
    )
}

fn default_daemon_state_dir_from(
    explicit_dir: Option<&str>,
    xdg_runtime: Option<&str>,
    cache_dir: &Path,
) -> PathBuf {
    if let Some(dir) = explicit_dir
        && !dir.trim().is_empty()
    {
        return PathBuf::from(dir);
    }
    if let Some(xdg) = xdg_runtime
        && !xdg.trim().is_empty()
    {
        return PathBuf::from(xdg).join("jj-commit-cloud");
    }
    if cache_dir.as_os_str().is_empty() {
        PathBuf::new()
    } else {
        cache_dir.join("run")
    }
}

pub fn default_daemon_cache_dir() -> PathBuf {
    default_daemon_cache_dir_from(
        std::env::var("JJ_CC_CACHE_DIR").ok().as_deref(),
        std::env::var("XDG_CACHE_HOME").ok().as_deref(),
        std::env::home_dir().as_deref(),
    )
}

fn default_daemon_cache_dir_from(
    explicit_dir: Option<&str>,
    xdg_cache: Option<&str>,
    home: Option<&Path>,
) -> PathBuf {
    if let Some(dir) = explicit_dir
        && !dir.trim().is_empty()
    {
        return PathBuf::from(dir);
    }
    if let Some(xdg) = xdg_cache
        && !xdg.trim().is_empty()
    {
        return PathBuf::from(xdg).join("jj-commit-cloud");
    }
    if let Some(home_dir) = home
        && !home_dir.as_os_str().is_empty()
    {
        return home_dir.join(".cache").join("jj-commit-cloud");
    }
    PathBuf::new()
}

pub fn user_daemon_config_path() -> PathBuf {
    if let Ok(p) = std::env::var("JJ_CC_DAEMON_CONFIG")
        && !p.trim().is_empty()
    {
        return PathBuf::from(p);
    }
    if let Ok(xdg_cfg) = std::env::var("XDG_CONFIG_HOME")
        && !xdg_cfg.trim().is_empty()
    {
        return PathBuf::from(xdg_cfg)
            .join("jj-commit-cloud")
            .join("daemon.toml");
    }
    if let Some(home) = std::env::home_dir()
        && !home.as_os_str().is_empty()
    {
        return home
            .join(".config")
            .join("jj-commit-cloud")
            .join("daemon.toml");
    }
    PathBuf::new()
}

pub fn set_user_daemon_enabled(
    enabled: bool,
) -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    let cfg_path = user_daemon_config_path();
    if cfg_path.as_os_str().is_empty() {
        return Err(
            "cannot determine user config path; set HOME, XDG_CONFIG_HOME, or JJ_CC_DAEMON_CONFIG"
                .into(),
        );
    }
    write_user_daemon_preference(&cfg_path, enabled)?;
    *DAEMON_PREF_CACHE.lock().unwrap() = Some((cfg_path.clone(), enabled));
    Ok(cfg_path)
}

fn write_user_daemon_preference(
    cfg_path: &Path,
    enabled: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let parent_dir = match cfg_path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => {
            create_private_dir_all(parent)?;
            verify_user_config_dir(parent)?;
            parent
        }
        _ => Path::new("."),
    };
    let content = toml::to_string_pretty(&UserDaemonPreference { enabled })?;
    let tmp_path = parent_dir.join(format!(
        ".daemon.toml.tmp.{}.{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    let write_res = (|| -> std::io::Result<()> {
        let mut opts = fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        {
            let mut file = opts.open(&tmp_path)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                file.set_permissions(fs::Permissions::from_mode(0o600))?;
            }
            file.write_all(content.as_bytes())?;
            file.sync_all()?;
        }
        fs::rename(&tmp_path, cfg_path)?;
        Ok(())
    })();
    if write_res.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }
    write_res?;
    Ok(())
}

/// Strips trailing `/` bytes (preserving `"/"`) so POSIX `lstat` and `open(..., O_NOFOLLOW)`
/// inspect a symlink path itself rather than following through to its target directory.
#[cfg(unix)]
pub(super) fn strip_trailing_slashes(path: &Path) -> &Path {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    let bytes = path.as_os_str().as_bytes();
    let mut end = bytes.len();
    while end > 1 && bytes[end - 1] == b'/' {
        end -= 1;
    }
    Path::new(OsStr::from_bytes(&bytes[..end]))
}

#[cfg(unix)]
fn verify_user_config_dir(dir: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let dir = strip_trailing_slashes(dir);
    let meta = fs::symlink_metadata(dir)?;
    // SAFETY: `geteuid` has no preconditions and cannot fail.
    let euid = unsafe { libc::geteuid() };
    let mode = meta.permissions().mode() & 0o7777;
    if !meta.is_dir() || meta.uid() != euid || mode & 0o002 != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "{} must be a non-world-writable directory owned by uid {euid} (found uid {}, mode {mode:o})",
                dir.display(),
                meta.uid()
            ),
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn verify_user_config_dir(_dir: &Path) -> std::io::Result<()> {
    Ok(())
}

pub fn is_daemon_enabled() -> bool {
    if let Ok(raw) = std::env::var("JJ_CC_USE_DAEMON") {
        let normalized = raw.trim().to_ascii_lowercase();
        if matches!(normalized.as_str(), "1" | "true" | "yes" | "on") {
            return true;
        }
        if matches!(normalized.as_str(), "0" | "false" | "no" | "off") {
            return false;
        }
    }
    let cfg_path = user_daemon_config_path();
    if cfg_path.as_os_str().is_empty() {
        return false;
    }
    if let Some((cached_path, cached_enabled)) = &*DAEMON_PREF_CACHE.lock().unwrap()
        && cached_path == &cfg_path
    {
        return *cached_enabled;
    }
    let enabled = fs::read_to_string(&cfg_path)
        .ok()
        .and_then(|raw| toml::from_str::<UserDaemonPreference>(&raw).ok())
        .is_some_and(|pref| pref.enabled);
    *DAEMON_PREF_CACHE.lock().unwrap() = Some((cfg_path, enabled));
    enabled
}

pub fn ensure_private_state_dir(dir: &Path) -> std::io::Result<()> {
    create_private_dir_all(dir)?;
    verify_private_state_dir(dir)
}

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
        let dir = strip_trailing_slashes(dir);
        let created_leaf = fs::symlink_metadata(dir).is_err();
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)?;
        if created_leaf {
            // Open the leaf directory without following symlinks and apply `fchmod` on the fd
            // rather than path-based `fs::set_permissions` to prevent symlink-swap TOCTOU.
            let dir_fd = fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
                .open(dir)?;
            let meta = dir_fd.metadata()?;
            // SAFETY: `geteuid` has no preconditions and cannot fail.
            let euid = unsafe { libc::geteuid() };
            if !meta.is_dir() || meta.uid() != euid {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!(
                        "{} must be a directory owned by uid {euid} (found uid {})",
                        dir.display(),
                        meta.uid()
                    ),
                ));
            }
            dir_fd.set_permissions(fs::Permissions::from_mode(0o700))?;
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        fs::create_dir_all(dir)
    }
}

#[cfg(unix)]
pub fn verify_private_state_dir(dir: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let dir = strip_trailing_slashes(dir);
    let meta = fs::symlink_metadata(dir)?;
    // SAFETY: `geteuid` has no preconditions and cannot fail.
    let euid = unsafe { libc::geteuid() };
    let mode = meta.permissions().mode() & 0o7777;
    if !meta.is_dir() || meta.uid() != euid || mode & 0o077 != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "{} must be a directory owned by uid {euid} with mode 0700 (found uid {}, mode {mode:o})",
                dir.display(),
                meta.uid()
            ),
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
pub fn verify_private_state_dir(dir: &Path) -> std::io::Result<()> {
    if dir.as_os_str().is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "cannot determine private per-user daemon directory",
        ));
    }
    Ok(())
}

pub fn read_daemon_runtime_info(state_dir: &Path) -> Option<DaemonRuntimeInfo> {
    verify_private_state_dir(state_dir).ok()?;
    let raw = fs::read_to_string(state_dir.join("daemon.json")).ok()?;
    serde_json::from_str(&raw).ok()
}

pub fn read_daemon_stats(state_dir: &Path) -> Option<DaemonStatsView> {
    verify_private_state_dir(state_dir).ok()?;
    let raw = fs::read_to_string(state_dir.join("daemon.stats.json")).ok()?;
    serde_json::from_str(&raw).ok()
}

#[cfg(test)]
mod tests;
