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

//! Background process spawning and marker-verified process-tree termination for `cargo xtask`.
//!
//! # Authority and Crash-Safety Ordering
//! Starts background processes in their own Unix process group and appends output to `.jj-cc`
//! logs. Teardown inspects live command lines before signaling `SIGTERM` and escalates to
//! `SIGKILL` only when the recorded marker still matches, preventing signals to reused PIDs.

use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use super::sanitize_server_env;
use crate::state::{BoxError, EnvState, ensure_private_dir};

/// A background process monitored during health polling so early exits report status and log path.
pub struct WatchedChild {
    pub label: String,
    pub child: std::process::Child,
    pub log_path: PathBuf,
}

impl WatchedChild {
    pub fn new(
        label: impl Into<String>,
        child: std::process::Child,
        log_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            label: label.into(),
            child,
            log_path: log_path.into(),
        }
    }
}

/// Starts `bin` in the background, appending its stdout and stderr to `log_path` so startup
/// failures stay diagnosable after `up` returns.
pub fn spawn_bg_child<I, S>(
    bin: &Path,
    args: I,
    envs: &[(&str, &str)],
    log_path: &Path,
) -> Result<std::process::Child, BoxError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    spawn_bg_child_inner(bin, args, envs, false, log_path)
}

/// Starts a managed `jj-cc-server` process in the background after clearing ambient server env
/// variables so host shell overrides cannot leak into local topologies.
pub(super) fn spawn_bg_server_child<I, S>(
    bin: &Path,
    args: I,
    envs: &[(&str, &str)],
    log_path: &Path,
) -> Result<std::process::Child, BoxError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    spawn_bg_child_inner(bin, args, envs, true, log_path)
}

fn spawn_bg_child_inner<I, S>(
    bin: &Path,
    args: I,
    envs: &[(&str, &str)],
    clear_server_env: bool,
    log_path: &Path,
) -> Result<std::process::Child, BoxError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    if let Some(parent) = log_path.parent() {
        ensure_private_dir(parent)?;
    }
    let log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)?;
    let mut cmd = Command::new(bin);
    if clear_server_env {
        sanitize_server_env(&mut cmd);
    }
    cmd.args(args)
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log);
    for (k, v) in envs {
        cmd.env(k, v);
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    Ok(cmd.spawn()?)
}

#[cfg(unix)]
fn signal_pid_or_group(pid: u32, signal: &str) -> bool {
    // Background children are started with `process_group(0)`, so signaling `-pid` stops the
    // entire process tree (e.g. wrapper scripts and child daemons). Fall back to `pid` for
    // processes that are not process-group leaders.
    let pgid_arg = format!("-{pid}");
    let group_ok = Command::new("kill")
        .args(["-s", signal, "--", &pgid_arg])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success());
    if group_ok {
        return true;
    }
    Command::new("kill")
        .args(["-s", signal, "--", &pid.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

#[cfg(windows)]
fn terminate_windows_tree(pid: u32) -> bool {
    Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T", "/F"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// Command-line marker of native jj-cc-server processes.
pub(super) const SERVER_PROCESS_MARKER: &str = "jj-cc-server";

/// Markers accepted for bare PIDs recorded by earlier xtask versions.
const LEGACY_PROCESS_MARKERS: &[&str] = &[
    SERVER_PROCESS_MARKER,
    "gateway_main",
    "start-iap-tunnel",
    "port-forward",
];

/// Stops the background processes recorded in `state`, newest first. A PID is signaled only
/// while its command line still contains the marker recorded when xtask started it, so PIDs
/// that exited or were reused by another program after a crash or reboot are left alone.
/// Returns one message per process that is still running.
pub fn stop_managed_processes(state: &EnvState) -> Vec<String> {
    let tracked = state
        .resources
        .processes
        .iter()
        .map(|p| (p.pid, vec![p.command_marker.as_str()]));
    let legacy = state
        .resources
        .pids
        .iter()
        .map(|&pid| (pid, LEGACY_PROCESS_MARKERS.to_vec()));
    let all: Vec<_> = tracked.chain(legacy).collect();
    let mut failures = Vec::new();
    for (pid, markers) in all.iter().rev() {
        if let Err(e) = stop_process(*pid, markers) {
            failures.push(format!("PID {pid}: {e}"));
        }
    }
    failures
}

pub(super) fn stop_process(pid: u32, markers: &[&str]) -> Result<(), String> {
    stop_process_with_timeouts(pid, markers, Duration::from_secs(5), Duration::from_secs(2))
}

fn wait_until_process_exits(pid: u32, markers: &[&str], timeout: Duration) -> Result<bool, String> {
    let deadline = Instant::now() + timeout;
    loop {
        match process_command_line(pid)? {
            None => return Ok(true),
            Some(cur) if !markers.iter().any(|m| cur.contains(m)) => return Ok(true),
            Some(_) => {}
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

pub(super) fn stop_process_with_timeouts(
    pid: u32,
    markers: &[&str],
    term_timeout: Duration,
    kill_timeout: Duration,
) -> Result<(), String> {
    let Some(cmdline) = process_command_line(pid)? else {
        return Ok(());
    };
    if !markers.iter().any(|m| cmdline.contains(m)) {
        println!(
            "    Leaving PID {pid} alone: it now runs `{cmdline}`, not a process xtask started"
        );
        return Ok(());
    }

    #[cfg(unix)]
    {
        let _ = signal_pid_or_group(pid, "TERM");
        if wait_until_process_exits(pid, markers, term_timeout)? {
            return Ok(());
        }
        let _ = signal_pid_or_group(pid, "KILL");
        if wait_until_process_exits(pid, markers, kill_timeout)? {
            return Ok(());
        }
        Err(format!("failed to stop `{cmdline}` even after SIGKILL"))
    }
    #[cfg(windows)]
    {
        let _ = terminate_windows_tree(pid);
        if wait_until_process_exits(pid, markers, term_timeout.max(kill_timeout))? {
            return Ok(());
        }
        Err(format!("failed to stop `{cmdline}`"))
    }
}

/// Parses `ps -p <pid> -o stat=,command=` output on macOS/BSD, returning `None` for unreaped
/// zombie processes (`Z*`) or empty command lines.
#[cfg(any(all(unix, not(target_os = "linux")), test))]
pub(super) fn parse_ps_stat_and_command(output: &str) -> Option<String> {
    let line = output.lines().map(str::trim).find(|l| !l.is_empty())?;
    let (stat, rest) = line.split_once(char::is_whitespace)?;
    if stat.starts_with('Z') {
        return None;
    }
    let cmd = rest.trim();
    (!cmd.is_empty()).then(|| cmd.to_string())
}

/// Linux `errno-base.h` `ESRCH`: returned by `read(2)` on `/proc/<pid>/cmdline` when a process
/// exits between `open(2)` and `read(2)`.
#[cfg(any(target_os = "linux", test))]
pub(super) const LINUX_ESRCH: i32 = 3;

#[cfg(any(target_os = "linux", test))]
pub(super) fn is_linux_proc_gone_error(err: &std::io::Error) -> bool {
    err.kind() == std::io::ErrorKind::NotFound || err.raw_os_error() == Some(LINUX_ESRCH)
}

/// Returns the command line of `pid`, or `None` when no such process is running.
pub(super) fn process_command_line(pid: u32) -> Result<Option<String>, String> {
    #[cfg(target_os = "linux")]
    let cmdline = match fs::read(format!("/proc/{pid}/cmdline")) {
        Ok(raw) => String::from_utf8_lossy(&raw).replace('\0', " "),
        Err(e) if is_linux_proc_gone_error(&e) => return Ok(None),
        Err(e) => return Err(format!("cannot read /proc/{pid}/cmdline: {e}")),
    };
    #[cfg(all(unix, not(target_os = "linux")))]
    let cmdline = {
        let out = Command::new("ps")
            .args(["-p", &pid.to_string(), "-o", "stat=,command="])
            .output()
            .map_err(|e| format!("cannot run `ps`: {e}"))?;
        // `ps -p` exits non-zero when no process has that PID.
        if !out.status.success() {
            return Ok(None);
        }
        let Some(cmd) = parse_ps_stat_and_command(&String::from_utf8_lossy(&out.stdout)) else {
            return Ok(None);
        };
        cmd
    };
    #[cfg(windows)]
    let cmdline = {
        let query =
            format!("(Get-CimInstance Win32_Process -Filter 'ProcessId={pid}').CommandLine");
        let out = Command::new("powershell")
            .args(["-NoProfile", "-NonInteractive", "-Command", &query])
            .output()
            .map_err(|e| format!("cannot run `powershell`: {e}"))?;
        if !out.status.success() {
            return Err(format!(
                "cannot inspect PID {pid}: {}",
                String::from_utf8_lossy(&out.stderr)
            ));
        }
        String::from_utf8_lossy(&out.stdout).into_owned()
    };
    let cmdline = cmdline.trim().to_string();
    // Zombies and kernel threads have empty command lines; neither is a process to stop.
    Ok((!cmdline.is_empty()).then_some(cmdline))
}
