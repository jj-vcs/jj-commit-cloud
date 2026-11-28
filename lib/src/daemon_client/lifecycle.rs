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

//! Binary discovery, detached process spawning, cross-process spawn locking, OS
//! PID verification, and safe termination for jj-cc-daemon.
//!
//! # Authority and Crash-Safety Ordering
//! Cross-process auto-spawn serializes on an advisory file lock at
//! `<state_dir>/daemon.spawn.lock` (`0600`), which the kernel releases if the
//! spawning process exits. Startup polling uses `SpawnedChildGuard` to kill and
//! reap a newly spawned child that fails to become healthy before the deadline.
//!
//! # Cross-Process and RPC Boundaries
//! Spawned daemons run in an independent process group on Unix with stdout and
//! stderr appended to `0600` `<state_dir>/daemon.log` and `JJ_CC_AUTH_TOKEN`
//! cleared from the child environment. Termination checks OS process identity
//! (or Unix `SO_PEERCRED` peer verification) before signaling a PID.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tonic::transport::Channel;

use super::transport::connect_existing_daemon;
use super::{DaemonRuntimeInfo, ensure_private_state_dir};
const DAEMON_SPAWN_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

pub fn locate_daemon_binary() -> Option<PathBuf> {
    if let Ok(explicit) = std::env::var("JJ_CC_DAEMON_BIN")
        && !explicit.trim().is_empty()
    {
        let p = PathBuf::from(explicit);
        if p.is_file() {
            return Some(p);
        }
        return None;
    }
    let bin_name = format!("jj-cc-daemon{}", std::env::consts::EXE_SUFFIX);
    if let Ok(exe) = std::env::current_exe()
        && let Some(dir) = exe.parent()
    {
        let sibling = dir.join(&bin_name);
        if sibling.is_file() {
            return Some(sibling);
        }
    }
    if let Some(paths) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&paths) {
            let candidate = dir.join(&bin_name);
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

pub(super) fn open_daemon_log(state_dir: &Path) -> std::io::Result<fs::File> {
    let log_path = state_dir.join("daemon.log");
    let mut log_opts = fs::OpenOptions::new();
    log_opts.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        log_opts.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let file = log_opts.open(&log_path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = file.metadata()?.permissions().mode() & 0o7777;
        if mode != 0o600 {
            file.set_permissions(fs::Permissions::from_mode(0o600))?;
        }
    }
    Ok(file)
}

#[cfg(unix)]
pub(super) fn resolve_spawn_unix_socket_path(
    state_dir: &Path,
    socket_override: Option<&Path>,
    env_socket: Option<&std::ffi::OsStr>,
) -> PathBuf {
    if let Some(sock) = socket_override {
        return sock.to_path_buf();
    }
    if let Some(raw) = env_socket {
        let non_empty = raw
            .to_str()
            .map(|s| !s.trim().is_empty())
            .unwrap_or(!raw.is_empty());
        if non_empty {
            return PathBuf::from(raw);
        }
    }
    state_dir.join("daemon.sock")
}

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_daemon_child(
    state_dir: &Path,
    cache_dir: &Path,
    upstream: Option<&str>,
    socket_override: Option<&Path>,
    listen_override: Option<&str>,
    idle_timeout_secs: Option<u64>,
    max_disk_cache_mb: Option<u64>,
) -> Result<Child, Box<dyn std::error::Error + Send + Sync>> {
    ensure_private_state_dir(state_dir)?;
    ensure_private_state_dir(cache_dir)?;
    #[cfg(unix)]
    {
        let env_socket = std::env::var_os("JJ_CC_DAEMON_SOCKET");
        let target_socket =
            resolve_spawn_unix_socket_path(state_dir, socket_override, env_socket.as_deref());
        cc_common::resolve_unix_socket_path_or_ancestors(&target_socket)?;
    }
    let bin =
        locate_daemon_binary().ok_or("jj-cc-daemon binary not found alongside jj or in PATH")?;

    let stdout_log = open_daemon_log(state_dir)?;
    let stderr_log = stdout_log.try_clone()?;

    let mut cmd = Command::new(bin);
    cmd.arg("--state-dir")
        .arg(state_dir)
        .arg("--cache-dir")
        .arg(cache_dir)
        .env_remove("JJ_CC_AUTH_TOKEN")
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout_log))
        .stderr(Stdio::from(stderr_log));

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }

    if let Some(up) = upstream {
        cmd.arg("--upstream").arg(up);
    }
    if let Some(sock) = socket_override {
        cmd.arg("--socket").arg(sock);
    }
    if let Some(addr) = listen_override {
        cmd.arg("--listen").arg(addr);
    }
    if let Some(idle) = idle_timeout_secs {
        cmd.arg("--idle-timeout-secs").arg(idle.to_string());
    }
    if let Some(max_disk_mb) = max_disk_cache_mb {
        cmd.arg("--max-disk-cache-mb").arg(max_disk_mb.to_string());
    }

    Ok(cmd.spawn()?)
}

#[allow(clippy::too_many_arguments)]
pub fn spawn_daemon_background(
    state_dir: &Path,
    cache_dir: &Path,
    upstream: Option<&str>,
    socket_override: Option<&Path>,
    listen_override: Option<&str>,
    idle_timeout_secs: Option<u64>,
    max_disk_cache_mb: Option<u64>,
) -> Result<u32, Box<dyn std::error::Error + Send + Sync>> {
    let child = spawn_daemon_child(
        state_dir,
        cache_dir,
        upstream,
        socket_override,
        listen_override,
        idle_timeout_secs,
        max_disk_cache_mb,
    )?;
    Ok(child.id())
}

struct SpawnedChildGuard(Option<Child>);

impl SpawnedChildGuard {
    fn new(child: Child) -> Self {
        Self(Some(child))
    }

    fn child_mut(&mut self) -> &mut Child {
        self.0
            .as_mut()
            .expect("SpawnedChildGuard accessed before disarm")
    }

    fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for SpawnedChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn spawn_and_wait_daemon(
    state_dir: &Path,
    cache_dir: &Path,
    upstream: Option<&str>,
    socket_override: Option<&Path>,
    listen_override: Option<&str>,
    idle_timeout_secs: Option<u64>,
    max_disk_cache_mb: Option<u64>,
) -> Result<(DaemonRuntimeInfo, Channel), Box<dyn std::error::Error + Send + Sync>> {
    let child = spawn_daemon_child(
        state_dir,
        cache_dir,
        upstream,
        socket_override,
        listen_override,
        idle_timeout_secs,
        max_disk_cache_mb,
    )?;
    wait_for_spawned_daemon(child, state_dir, DAEMON_SPAWN_WAIT_TIMEOUT).await
}

pub(super) async fn wait_for_spawned_daemon(
    child: Child,
    state_dir: &Path,
    timeout: Duration,
) -> Result<(DaemonRuntimeInfo, Channel), Box<dyn std::error::Error + Send + Sync>> {
    let pid = child.id();
    let mut child_guard = SpawnedChildGuard::new(child);
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child_guard.child_mut().try_wait()? {
            child_guard.disarm();
            let log_tail = fs::read_to_string(state_dir.join("daemon.log"))
                .ok()
                .and_then(|s| s.lines().last().map(str::to_owned))
                .unwrap_or_default();
            return Err(format!(
                "jj-cc-daemon (pid {pid}) exited during startup ({status}){}",
                if log_tail.is_empty() {
                    String::new()
                } else {
                    format!(": {log_tail}")
                }
            )
            .into());
        }
        let Some(probe_budget) = deadline.checked_duration_since(Instant::now()) else {
            break;
        };
        match tokio::time::timeout(probe_budget, connect_existing_daemon(state_dir)).await {
            Ok(Ok(Some(ready))) if ready.0.pid == pid => {
                child_guard.disarm();
                return Ok(ready);
            }
            Ok(Ok(_)) => {}
            Ok(Err(err)) => return Err(err),
            Err(_) => break,
        }
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            break;
        };
        tokio::time::sleep(remaining.min(Duration::from_millis(25))).await;
    }
    Err(format!(
        "timed out waiting for jj-cc-daemon (pid {pid}) in {}",
        state_dir.display()
    )
    .into())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PidMatch {
    ConfirmedDaemon,
    NotDaemon,
    Unknown,
}

#[cfg(any(unix, windows, test))]
fn exe_name_matches_daemon(raw_exe: &str, case_insensitive: bool) -> bool {
    let trimmed = raw_exe.trim();
    if trimmed.is_empty() {
        return false;
    }
    let basename = trimmed.rsplit(['/', '\\']).next().unwrap_or("");
    if case_insensitive {
        basename.eq_ignore_ascii_case("jj-cc-daemon")
            || basename.eq_ignore_ascii_case("jj-cc-daemon.exe")
    } else {
        basename == "jj-cc-daemon" || basename == "jj-cc-daemon.exe"
    }
}

#[cfg(any(target_os = "linux", test))]
pub(super) fn cmdline_matches_daemon(cmdline: &[u8]) -> bool {
    let Some(argv0) = cmdline.split(|&b| b == 0).next() else {
        return false;
    };
    if argv0.is_empty() {
        return false;
    }
    let argv0_str = String::from_utf8_lossy(argv0);
    exe_name_matches_daemon(argv0_str.as_ref(), false)
}

#[cfg(any(target_os = "linux", test))]
pub(super) fn linux_proc_stat_is_live(stat: &str) -> bool {
    let Some((_, rest)) = stat.rsplit_once(')') else {
        return false;
    };
    let Some(state) = rest.split_whitespace().next() else {
        return false;
    };
    !matches!(state, "Z" | "X" | "x")
}

#[cfg(any(all(unix, not(target_os = "linux")), test))]
pub(super) fn parse_unix_ps_output(stdout: &str) -> bool {
    let Some(line) = stdout.lines().find(|l| !l.trim().is_empty()) else {
        return false;
    };
    let trimmed = line.trim();
    let Some((stat, comm)) = trimmed.split_once(char::is_whitespace) else {
        return false;
    };
    if stat.starts_with('Z') {
        return false;
    }
    exe_name_matches_daemon(comm, false)
}

#[cfg(any(windows, test))]
pub(super) fn parse_windows_tasklist_csv(stdout: &str, expected_pid: u32) -> bool {
    for line in stdout.lines() {
        let trimmed = line.trim();
        let Some(after_open) = trimmed.strip_prefix('"') else {
            continue;
        };
        let Some((image_name, rest)) = after_open.split_once("\",\"") else {
            continue;
        };
        let Some((pid_str, _)) = rest.split_once('"') else {
            continue;
        };
        if pid_str.trim().parse::<u32>().ok() == Some(expected_pid)
            && exe_name_matches_daemon(image_name, true)
        {
            return true;
        }
    }
    false
}

#[cfg(target_os = "linux")]
fn check_pid_matches_daemon_os(pid: u32) -> PidMatch {
    if pid > i32::MAX as u32 {
        return PidMatch::NotDaemon;
    }
    // SAFETY: `kill(pid, 0)` performs a kernel existence/permission probe without sending a signal.
    let rc = unsafe { libc::kill(pid as i32, 0) };
    if rc != 0 {
        return PidMatch::NotDaemon;
    }
    let stat = match fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(s) => s,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return if Path::new("/proc/self").exists() {
                PidMatch::NotDaemon
            } else {
                PidMatch::Unknown
            };
        }
        Err(_) => return PidMatch::Unknown,
    };
    if !linux_proc_stat_is_live(&stat) {
        return PidMatch::NotDaemon;
    }
    match fs::read(format!("/proc/{pid}/cmdline")) {
        Ok(cmdline) => {
            if cmdline_matches_daemon(&cmdline) {
                PidMatch::ConfirmedDaemon
            } else {
                PidMatch::NotDaemon
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            if Path::new("/proc/self").exists() {
                PidMatch::NotDaemon
            } else {
                PidMatch::Unknown
            }
        }
        Err(_) => PidMatch::Unknown,
    }
}

#[cfg(all(unix, not(target_os = "linux")))]
fn check_pid_matches_daemon_os(pid: u32) -> PidMatch {
    if pid > i32::MAX as u32 {
        return PidMatch::NotDaemon;
    }
    // SAFETY: `kill(pid, 0)` performs a kernel existence/permission probe without sending a signal.
    let rc = unsafe { libc::kill(pid as i32, 0) };
    if rc != 0 {
        return PidMatch::NotDaemon;
    }
    let output = match Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "stat=,comm="])
        .stdin(Stdio::null())
        .output()
    {
        Ok(out) => out,
        Err(_) => return PidMatch::Unknown,
    };
    if !output.status.success() {
        return if output.stdout.is_empty() && output.stderr.is_empty() {
            PidMatch::NotDaemon
        } else {
            PidMatch::Unknown
        };
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    if parse_unix_ps_output(&stdout) {
        PidMatch::ConfirmedDaemon
    } else {
        PidMatch::NotDaemon
    }
}

#[cfg(windows)]
fn check_pid_matches_daemon_os(pid: u32) -> PidMatch {
    let output = match Command::new("tasklist")
        .args(["/FI", &format!("PID eq {pid}"), "/FO", "CSV", "/NH"])
        .stdin(Stdio::null())
        .output()
    {
        Ok(out) => out,
        Err(_) => return PidMatch::Unknown,
    };
    if !output.status.success() {
        return PidMatch::Unknown;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    if parse_windows_tasklist_csv(&stdout, pid) {
        PidMatch::ConfirmedDaemon
    } else {
        PidMatch::NotDaemon
    }
}

#[cfg(not(any(unix, windows)))]
fn check_pid_matches_daemon_os(_pid: u32) -> PidMatch {
    PidMatch::Unknown
}

pub fn check_pid_matches_daemon(pid: u32) -> PidMatch {
    if pid == 0 {
        return PidMatch::NotDaemon;
    }
    check_pid_matches_daemon_os(pid)
}

pub fn pid_matches_daemon(pid: u32) -> bool {
    check_pid_matches_daemon(pid) == PidMatch::ConfirmedDaemon
}

pub struct DaemonSpawnLockGuard {
    file: fs::File,
}

impl Drop for DaemonSpawnLockGuard {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

pub async fn acquire_daemon_spawn_lock(
    state_dir: &Path,
) -> Result<DaemonSpawnLockGuard, Box<dyn std::error::Error + Send + Sync>> {
    ensure_private_state_dir(state_dir)?;
    let lock_path = state_dir.join("daemon.spawn.lock");
    let mut opts = fs::OpenOptions::new();
    opts.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let file = opts.open(&lock_path)?;
    let locked = tokio::time::timeout(
        Duration::from_secs(10),
        tokio::task::spawn_blocking(move || -> std::io::Result<fs::File> {
            file.lock()?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mode = file.metadata()?.permissions().mode() & 0o7777;
                if mode != 0o600 {
                    file.set_permissions(fs::Permissions::from_mode(0o600))?;
                }
            }
            Ok(file)
        }),
    )
    .await
    .map_err(|_| {
        format!(
            "timed out waiting for cross-process daemon spawn lock {}",
            lock_path.display()
        )
    })?
    .map_err(|e| format!("daemon spawn lock join error: {e}"))??;
    Ok(DaemonSpawnLockGuard { file: locked })
}

pub(super) fn send_terminate_signal(pid: u32) -> Result<(), String> {
    #[cfg(unix)]
    {
        if pid <= 1 {
            return Err(format!("Refusing to signal invalid daemon pid {pid}"));
        }
        let raw_pid = i32::try_from(pid)
            .map_err(|_| format!("Daemon pid {pid} exceeds platform pid range"))?;
        // SAFETY: Caller verified `pid` belongs to the live daemon process and `raw_pid > 1`.
        let rc = unsafe { libc::kill(raw_pid, libc::SIGTERM) };
        if rc == 0 {
            return Ok(());
        }
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ESRCH) {
            return Ok(());
        }
        Err(format!(
            "Failed to send SIGTERM to jj-cc-daemon (pid {pid}): {err}"
        ))
    }
    #[cfg(windows)]
    {
        if pid == 0 {
            return Err("Refusing to terminate invalid daemon pid 0".to_string());
        }
        let output = Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/F"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .map_err(|e| format!("Failed to invoke taskkill for jj-cc-daemon (pid {pid}): {e}"))?;
        if output.status.success() {
            return Ok(());
        }
        if check_pid_matches_daemon(pid) == PidMatch::NotDaemon {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        let detail = stderr.trim();
        Err(format!(
            "taskkill failed for jj-cc-daemon (pid {pid}) ({}){}",
            output.status,
            if detail.is_empty() {
                String::new()
            } else {
                format!(": {detail}")
            }
        ))
    }
    #[cfg(not(any(unix, windows)))]
    {
        Err(format!(
            "Stopping jj-cc-daemon (pid {pid}) is not supported on this platform"
        ))
    }
}

pub fn terminate_daemon_pid(pid: u32) {
    if !pid_matches_daemon(pid) {
        return;
    }
    let _ = send_terminate_signal(pid);
}

/// Signals `pid` if OS process inspection confirms it is `jj-cc-daemon`, or if OS inspection is
/// inconclusive (`PidMatch::Unknown`) and `peer_pid_verified` confirms `pid` matched UDS
/// `SO_PEERCRED` + `tonic_health`. Returns `Ok(true)` when signaled, `Ok(false)` when OS
/// inspection reports `PidMatch::NotDaemon`, and `Err` when `PidMatch::Unknown` without peer PID
/// verification or when delivering the termination signal fails.
pub fn terminate_daemon_pid_with_verification(
    pid: u32,
    peer_pid_verified: bool,
) -> Result<bool, String> {
    terminate_daemon_pid_with(
        pid,
        peer_pid_verified,
        check_pid_matches_daemon,
        send_terminate_signal,
    )
}

pub(super) fn terminate_daemon_pid_with<C, S>(
    pid: u32,
    peer_pid_verified: bool,
    pid_check: C,
    signal: S,
) -> Result<bool, String>
where
    C: FnOnce(u32) -> PidMatch,
    S: FnOnce(u32) -> Result<(), String>,
{
    match pid_check(pid) {
        PidMatch::ConfirmedDaemon => {
            signal(pid)?;
            Ok(true)
        }
        PidMatch::Unknown => {
            if peer_pid_verified {
                signal(pid)?;
                Ok(true)
            } else {
                Err(format!(
                    "Cannot verify that pid {pid} is jj-cc-daemon (OS process inspection unavailable and peer PID was not verified via Unix socket credentials); refusing to signal or remove live runtime files"
                ))
            }
        }
        PidMatch::NotDaemon => Ok(false),
    }
}
