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

//! `jj cc daemon` subcommands for managing the local per-user `jj-cc-daemon` proxy.
//!
//! # Cross-Process and RPC Boundaries
//! Serializes `start`, `stop`, and `clear-cache` against auto-spawn using `daemon.spawn.lock`,
//! validates `0700` per-user directory ownership/permissions, and probes daemon liveness
//! via `tonic_health` before signaling so stale `daemon.json` files never signal recycled PIDs.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use cc_lib::daemon_client::{
    DaemonRuntimeInfo, ExistingDaemonConnection, PidMatch, acquire_daemon_spawn_lock,
    check_pid_matches_daemon, connect_existing_daemon, connect_existing_daemon_verified,
    default_daemon_cache_dir, default_daemon_state_dir, ensure_private_state_dir,
    is_daemon_enabled, probe_daemon_channel_health, read_daemon_runtime_info, read_daemon_stats,
    set_user_daemon_enabled, spawn_and_wait_daemon, terminate_daemon_pid_with_verification,
    user_daemon_config_path, verify_private_state_dir,
};
use cc_lib::util::run_on_tokio;
use clap::{Args, Subcommand};
use jj_cli::command_error::{CommandError, user_error};

#[derive(Args, Clone, Debug)]
pub struct DaemonArgs {
    /// Enable automatic daemon routing in user configuration
    #[arg(long, conflicts_with_all = ["disable", "status_flag"])]
    pub enable: bool,

    /// Disable automatic daemon routing in user configuration
    #[arg(long, conflicts_with_all = ["enable", "status_flag"])]
    pub disable: bool,

    /// Print live daemon status and L1/L2 cache hit metrics
    #[arg(long = "status", conflicts_with_all = ["enable", "disable"])]
    pub status_flag: bool,

    #[command(subcommand)]
    pub action: Option<DaemonSubcommand>,
}

#[derive(Subcommand, Clone, Debug)]
pub enum DaemonSubcommand {
    /// Start the local `jj-cc-daemon` background proxy
    Start {
        /// Optional default upstream `jj-cc-server` URL
        #[arg(long)]
        upstream: Option<String>,
        /// Optional Unix domain socket override
        #[arg(long)]
        socket: Option<PathBuf>,
        /// Optional loopback TCP address override (for example `127.0.0.1:50055`)
        #[arg(long)]
        listen: Option<String>,
        /// Optional idle shutdown timeout in seconds (`0` disables idle exit)
        #[arg(long)]
        idle_timeout_secs: Option<u64>,
    },
    /// Stop the running `jj-cc-daemon` process
    Stop,
    /// Show live `jj-cc-daemon` endpoint, PID, and L1/L2 cache hit statistics
    Status,
    /// Enable `jj-cc-daemon` routing for local Commit Cloud commands
    Enable,
    /// Disable `jj-cc-daemon` routing and connect directly to `jj-cc-server`
    Disable,
    /// Purge cached objects in the local L2 disk CAS directory
    ClearCache,
}

pub async fn cmd_cc_daemon(args: &DaemonArgs) -> Result<(), CommandError> {
    if (args.enable || args.disable || args.status_flag) && args.action.is_some() {
        return Err(user_error(
            "Cannot combine legacy flags (`--enable`, `--disable`, `--status`) with a `jj cc daemon` subcommand; pass either a subcommand or a single flag.",
        ));
    }
    if args.enable {
        return handle_enable(true);
    }
    if args.disable {
        return handle_enable(false);
    }
    if args.status_flag {
        return handle_status().await;
    }

    match args.action.as_ref().unwrap_or(&DaemonSubcommand::Status) {
        DaemonSubcommand::Enable => handle_enable(true),
        DaemonSubcommand::Disable => handle_enable(false),
        DaemonSubcommand::Status => handle_status().await,
        DaemonSubcommand::Start {
            upstream,
            socket,
            listen,
            idle_timeout_secs,
        } => {
            handle_start(
                upstream.as_deref(),
                socket.as_deref(),
                listen.as_deref(),
                *idle_timeout_secs,
            )
            .await
        }
        DaemonSubcommand::Stop => handle_stop().await,
        DaemonSubcommand::ClearCache => handle_clear_cache().await,
    }
}

fn handle_enable(enabled: bool) -> Result<(), CommandError> {
    let cfg_path = set_user_daemon_enabled(enabled)
        .map_err(|e| user_error(format!("Failed to update daemon preference: {e}")))?;
    println!(
        "Local jj-cc-daemon routing {} ({})",
        if enabled { "enabled" } else { "disabled" },
        cfg_path.display()
    );
    Ok(())
}

fn check_already_running_start_flags(
    info: &DaemonRuntimeInfo,
    upstream: Option<&str>,
    socket: Option<&Path>,
    listen: Option<&str>,
    idle_timeout_secs: Option<u64>,
) -> Result<(), CommandError> {
    if upstream.is_some() || socket.is_some() || listen.is_some() || idle_timeout_secs.is_some() {
        let endpoint = info
            .socket_path
            .as_deref()
            .or(info.tcp_addr.as_deref())
            .unwrap_or("<unknown>");
        return Err(user_error(format!(
            "jj-cc-daemon is already running (pid {}, endpoint {endpoint}); cannot apply startup flags (--upstream/--socket/--listen/--idle-timeout-secs) to an existing daemon. Run `jj cc daemon stop` first and then re-run `jj cc daemon start`.",
            info.pid
        )));
    }
    Ok(())
}

async fn handle_start(
    upstream: Option<&str>,
    socket: Option<&Path>,
    listen: Option<&str>,
    idle_timeout_secs: Option<u64>,
) -> Result<(), CommandError> {
    let state_dir = default_daemon_state_dir();
    let cache_dir = default_daemon_cache_dir();
    let up_owned = upstream.map(str::to_owned);
    let sock_owned = socket.map(Path::to_path_buf);
    let listen_owned = listen.map(str::to_owned);

    let (info, already_running) = run_on_tokio(async move {
        if let Some((info, _)) = connect_existing_daemon(&state_dir).await? {
            return Ok((info, true));
        }
        let _spawn_lock = acquire_daemon_spawn_lock(&state_dir).await?;
        if let Some((info, _)) = connect_existing_daemon(&state_dir).await? {
            return Ok((info, true));
        }
        let (info, _ch) = spawn_and_wait_daemon(
            &state_dir,
            &cache_dir,
            up_owned.as_deref(),
            sock_owned.as_deref(),
            listen_owned.as_deref(),
            idle_timeout_secs,
        )
        .await?;
        Ok((info, false))
    })
    .await
    .map_err(|e| user_error(format!("Failed to start jj-cc-daemon: {e}")))?;

    let endpoint = info
        .socket_path
        .as_deref()
        .or(info.tcp_addr.as_deref())
        .unwrap_or("<unknown>");
    if already_running {
        check_already_running_start_flags(&info, upstream, socket, listen, idle_timeout_secs)?;
        println!(
            "jj-cc-daemon is already running (pid {}, endpoint {endpoint})",
            info.pid
        );
    } else {
        println!("Started jj-cc-daemon (pid {}) on {endpoint}", info.pid);
    }
    Ok(())
}

enum StopOutcome {
    NotRunning,
    CleanedStale(u32),
    Stopped(u32),
}

async fn handle_stop() -> Result<(), CommandError> {
    let state_dir = default_daemon_state_dir();
    if state_dir.as_os_str().is_empty() {
        return Err(user_error(
            "Cannot determine daemon state directory (set XDG_RUNTIME_DIR, HOME, or JJ_CC_DAEMON_DIR)",
        ));
    }
    #[cfg(unix)]
    match verify_private_state_dir(&state_dir) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            println!("jj-cc-daemon is not running.");
            return Ok(());
        }
        Err(err) => {
            return Err(user_error(format!(
                "Daemon state directory {} failed permission verification: {err}",
                state_dir.display()
            )));
        }
    }
    #[cfg(not(unix))]
    match fs::symlink_metadata(&state_dir) {
        Ok(_) => verify_private_state_dir(&state_dir).map_err(|e| {
            user_error(format!(
                "Daemon state directory {} failed permission verification: {e}",
                state_dir.display()
            ))
        })?,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            println!("jj-cc-daemon is not running.");
            return Ok(());
        }
        Err(err) => {
            return Err(user_error(format!(
                "Failed to inspect daemon state directory {}: {err}",
                state_dir.display()
            )));
        }
    }

    let st_stop = state_dir.clone();
    let outcome = run_on_tokio(async move {
        // Hold `daemon.spawn.lock` across reading `daemon.json`, probing, signaling, and stale
        // file cleanup so a concurrent `start` or client auto-spawn cannot race with unlink.
        let _spawn_lock = acquire_daemon_spawn_lock(&st_stop).await?;
        let Some(recorded) = read_daemon_runtime_info(&st_stop) else {
            return Ok(StopOutcome::NotRunning);
        };

        let live = connect_existing_daemon_verified(&st_stop)
            .await
            .map_err(|e| format!("Daemon probe failed: {e}"))?;

        let Some(conn) = live else {
            // `daemon.json` is stale (for example after SIGKILL or crash); never signal
            // `recorded.pid` because the OS may have recycled it for an unrelated process.
            cleanup_stale_runtime_files(&st_stop, &recorded);
            return Ok(StopOutcome::CleanedStale(recorded.pid));
        };

        let stopped_pid = conn.info.pid;
        stop_live_daemon_with(
            &st_stop,
            &conn.info,
            conn.peer_pid_verified,
            120,
            Duration::from_millis(25),
            Duration::from_secs(3),
            check_pid_matches_daemon,
            terminate_daemon_pid_with_verification,
            || probe_daemon_still_live(&st_stop, &conn),
        )
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?;

        Ok(StopOutcome::Stopped(stopped_pid))
    })
    .await
    .map_err(|e| user_error(format!("{e}")))?;

    match outcome {
        StopOutcome::NotRunning => println!("jj-cc-daemon is not running."),
        StopOutcome::CleanedStale(pid) => {
            println!("jj-cc-daemon is not running (removed stale state for pid {pid}).");
        }
        StopOutcome::Stopped(pid) => println!("Stopped jj-cc-daemon (pid {pid})."),
    }
    Ok(())
}

async fn probe_daemon_still_live(
    state_dir: &Path,
    conn: &ExistingDaemonConnection,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    if conn.info.socket_path.is_some() {
        return Ok(connect_existing_daemon(state_dir)
            .await?
            .is_some_and(|(cur, _)| {
                cur.pid == conn.info.pid && cur.started_unix_secs == conn.info.started_unix_secs
            }));
    }
    let still_same_record = read_daemon_runtime_info(state_dir).is_some_and(|cur| {
        cur.pid == conn.info.pid && cur.started_unix_secs == conn.info.started_unix_secs
    });
    if !still_same_record {
        return Ok(false);
    }
    Ok(probe_daemon_channel_health(conn.channel.clone(), conn.info.secret.as_deref()).await)
}

#[allow(clippy::too_many_arguments)]
async fn stop_live_daemon_with<C, S, P, PFut>(
    state_dir: &Path,
    info: &DaemonRuntimeInfo,
    peer_pid_verified: bool,
    max_polls: usize,
    poll_interval: Duration,
    max_wait: Duration,
    mut pid_check: C,
    mut signal_fn: S,
    mut probe_live: P,
) -> Result<(), String>
where
    C: FnMut(u32) -> PidMatch,
    S: FnMut(u32, bool) -> Result<bool, String>,
    P: FnMut() -> PFut,
    PFut: std::future::Future<Output = Result<bool, Box<dyn std::error::Error + Send + Sync>>>,
{
    let signaled = signal_fn(info.pid, peer_pid_verified)?;
    if !signaled {
        let still_same_record = read_daemon_runtime_info(state_dir).is_some_and(|cur| {
            cur.pid == info.pid && cur.started_unix_secs == info.started_unix_secs
        });
        if !still_same_record {
            return Ok(());
        }
        return match probe_live().await {
            Ok(true) => Err(format!(
                "Refusing to stop pid {} because OS process inspection indicates it is not jj-cc-daemon; live runtime files were left untouched",
                info.pid
            )),
            Ok(false) => {
                cleanup_stale_runtime_files(state_dir, info);
                Ok(())
            }
            Err(err) => Err(format!("Daemon probe failed: {err}")),
        };
    }

    let deadline = Instant::now() + max_wait;
    for attempt in 0..max_polls {
        let record_still_present = match read_daemon_runtime_info(state_dir) {
            Some(cur) if cur.pid == info.pid && cur.started_unix_secs == info.started_unix_secs => {
                true
            }
            Some(_) => {
                // Another daemon instance already replaced `daemon.json`, which implies the
                // signaled process released `daemon.instance.lock`. Leave the new files intact.
                return Ok(());
            }
            // The signaled daemon unlinks `daemon.json` before releasing `daemon.instance.lock`;
            // keep polling process/liveness state until it exits.
            None => false,
        };

        match pid_check(info.pid) {
            PidMatch::NotDaemon => {
                if record_still_present {
                    cleanup_stale_runtime_files(state_dir, info);
                }
                return Ok(());
            }
            PidMatch::ConfirmedDaemon => {}
            PidMatch::Unknown => match probe_live().await {
                Ok(true) => {}
                Ok(false) => {
                    if record_still_present {
                        cleanup_stale_runtime_files(state_dir, info);
                    }
                    return Ok(());
                }
                Err(err) => {
                    return Err(format!(
                        "Daemon probe failed while waiting for shutdown: {err}"
                    ));
                }
            },
        }

        if attempt + 1 >= max_polls || Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(poll_interval).await;
    }

    Err(format!(
        "Timed out waiting for jj-cc-daemon (pid {}) to exit; check `jj cc daemon status`",
        info.pid
    ))
}

fn cleanup_stale_runtime_files(
    state_dir: &Path,
    expected: &cc_lib::daemon_client::DaemonRuntimeInfo,
) {
    let still_matches = read_daemon_runtime_info(state_dir).is_some_and(|cur| {
        cur.pid == expected.pid && cur.started_unix_secs == expected.started_unix_secs
    });
    if !still_matches {
        return;
    }
    if let Some(sock) = &expected.socket_path {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileTypeExt;
            if fs::symlink_metadata(sock).is_ok_and(|m| m.file_type().is_socket()) {
                let _ = fs::remove_file(sock);
            }
        }
        #[cfg(not(unix))]
        {
            let _ = fs::remove_file(sock);
        }
    }
    let _ = fs::remove_file(state_dir.join("daemon.json"));
}

async fn handle_status() -> Result<(), CommandError> {
    let state_dir = default_daemon_state_dir();
    let cache_dir = default_daemon_cache_dir();
    let enabled = is_daemon_enabled();
    let st_check = state_dir.clone();
    let probe = run_on_tokio(async move {
        Ok(connect_existing_daemon(&st_check)
            .await?
            .map(|(info, _)| info))
    })
    .await;

    println!(
        "Client routing: {}",
        if enabled {
            "enabled (via jj-cc-daemon)"
        } else {
            "direct (enable with `jj cc daemon enable` or JJ_CC_USE_DAEMON=1)"
        }
    );
    println!("Preference file: {}", user_daemon_config_path().display());

    match probe {
        Ok(Some(info)) => {
            println!("Daemon state:    RUNNING (pid {})", info.pid);
            if let Some(sock) = &info.socket_path {
                println!("Unix socket:     {sock}");
            }
            if let Some(tcp) = &info.tcp_addr {
                println!("TCP listener:    {tcp}");
            }
            println!("L2 cache dir:    {}", info.cache_dir);
            if let Some(stats) = read_daemon_stats(&state_dir) {
                println!(
                    "Cache hits:      {} L1 + {} L2 ({:.1}% hit rate, {} misses, {} coalesced)",
                    stats.l1_hits,
                    stats.l2_hits,
                    stats.hit_ratio_percent(),
                    stats.misses,
                    stats.singleflight_coalesced
                );
                println!(
                    "Write-through:   {} objects | Upstream RPCs: {}",
                    stats.write_through_objects, stats.upstream_rpcs
                );
            }
        }
        Ok(None) => {
            println!("Daemon state:    STOPPED");
            println!("L2 cache dir:    {}", cache_dir.display());
        }
        Err(err) => {
            println!("Daemon state:    ERROR ({err})");
            println!("L2 cache dir:    {}", cache_dir.display());
        }
    }
    Ok(())
}

async fn handle_clear_cache() -> Result<(), CommandError> {
    let state_dir = default_daemon_state_dir();
    let cache_dir = default_daemon_cache_dir();
    if cache_dir.as_os_str().is_empty() {
        return Err(user_error(
            "Cannot determine daemon cache directory (set HOME, XDG_CACHE_HOME, or JJ_CC_CACHE_DIR)",
        ));
    }

    let cache_dir_display = cache_dir.display().to_string();
    run_on_tokio(async move {
        let _spawn_lock = acquire_daemon_spawn_lock(&state_dir).await?;
        if let Some((info, _)) = connect_existing_daemon(&state_dir).await? {
            return Err(format!(
                "jj-cc-daemon (pid {}) is running; stop it first with `jj cc daemon stop` before clearing the L2 cache",
                info.pid
            )
            .into());
        }
        ensure_private_state_dir(&cache_dir).map_err(|e| {
            format!(
                "Failed to verify private daemon cache directory {}: {e}",
                cache_dir.display()
            )
        })?;
        for sub in ["objects", "scopes", "staging"] {
            let sub_dir = cache_dir.join(sub);
            if sub_dir.exists() {
                fs::remove_dir_all(&sub_dir)
                    .map_err(|e| format!("Failed to remove {}: {e}", sub_dir.display()))?;
            }
            ensure_private_state_dir(&sub_dir)
                .map_err(|e| format!("Failed to recreate {}: {e}", sub_dir.display()))?;
        }
        Ok(())
    })
    .await
    .map_err(|e| user_error(format!("{e}")))?;

    println!("Cleared local L2 object cache at {cache_dir_display}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cmd_cc_daemon_rejects_legacy_flags_combined_with_subcommand() {
        for args in [
            DaemonArgs {
                enable: true,
                disable: false,
                status_flag: false,
                action: Some(DaemonSubcommand::Stop),
            },
            DaemonArgs {
                enable: false,
                disable: true,
                status_flag: false,
                action: Some(DaemonSubcommand::Status),
            },
            DaemonArgs {
                enable: false,
                disable: false,
                status_flag: true,
                action: Some(DaemonSubcommand::ClearCache),
            },
        ] {
            let err = cmd_cc_daemon(&args)
                .await
                .expect_err("combining legacy flags with a subcommand must return user_error");
            let formatted = format!("{err:?}");
            assert!(
                formatted.contains("Cannot combine legacy flags"),
                "unexpected error: {formatted}"
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stop_live_daemon_with_unknown_pid_match_protects_live_runtime_files() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        use tokio::net::UnixListener;

        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().join("stop_unknown_state");
        ensure_private_state_dir(&state_dir).unwrap();
        let sock_path = state_dir.join("daemon.sock");
        let _listener = UnixListener::bind(&sock_path).unwrap();

        let info = DaemonRuntimeInfo {
            pid: std::process::id(),
            socket_path: Some(sock_path.display().to_string()),
            tcp_addr: None,
            cache_dir: dir.path().join("cache").display().to_string(),
            stats_path: state_dir.join("daemon.stats.json").display().to_string(),
            default_upstream: None,
            started_unix_secs: 12345,
            secret: None,
        };
        let daemon_json = state_dir.join("daemon.json");
        let raw_json = format!(
            r#"{{"pid":{},"socket_path":{:?},"tcp_addr":null,"cache_dir":{:?},"stats_path":{:?},"default_upstream":null,"started_unix_secs":{}}}"#,
            info.pid,
            info.socket_path.as_deref().unwrap(),
            info.cache_dir,
            info.stats_path,
            info.started_unix_secs,
        );
        fs::write(&daemon_json, raw_json).unwrap();

        // PidMatch::Unknown without peer_pid_verified returns an error without signaling
        // or deleting live runtime files.
        let signals_called = Arc::new(AtomicUsize::new(0));
        let signals_ref = Arc::clone(&signals_called);
        let err = stop_live_daemon_with(
            &state_dir,
            &info,
            false,
            5,
            Duration::from_millis(10),
            Duration::from_millis(100),
            |_| PidMatch::Unknown,
            move |_pid, verified| {
                if !verified {
                    return Err("Cannot verify daemon PID".to_string());
                }
                signals_ref.fetch_add(1, Ordering::SeqCst);
                Ok(true)
            },
            || async { Ok(true) },
        )
        .await
        .expect_err("PidMatch::Unknown without peer verification must fail");
        assert!(err.contains("Cannot verify"), "unexpected error: {err}");
        assert_eq!(signals_called.load(Ordering::SeqCst), 0);
        assert!(
            daemon_json.exists() && sock_path.exists(),
            "live runtime files must stay intact when stop refuses on PidMatch::Unknown"
        );

        // PidMatch::Unknown with peer_pid_verified signals the verified PID, re-probes live
        // daemon on each poll, and times out without unlinking runtime files while live.
        let signals_ref2 = Arc::clone(&signals_called);
        let probes_called = Arc::new(AtomicUsize::new(0));
        let probes_ref = Arc::clone(&probes_called);
        let err = stop_live_daemon_with(
            &state_dir,
            &info,
            true,
            3,
            Duration::from_millis(10),
            Duration::from_secs(2),
            |_| PidMatch::Unknown,
            move |_pid, verified| {
                assert!(verified);
                signals_ref2.fetch_add(1, Ordering::SeqCst);
                Ok(true)
            },
            || {
                let count = Arc::clone(&probes_ref);
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(true)
                }
            },
        )
        .await
        .expect_err("live daemon staying up under PidMatch::Unknown must time out, not succeed");
        assert!(err.contains("Timed out"), "unexpected error: {err}");
        assert_eq!(signals_called.load(Ordering::SeqCst), 1);
        assert_eq!(probes_called.load(Ordering::SeqCst), 3);
        assert!(
            daemon_json.exists() && sock_path.exists(),
            "live runtime files must NOT be unlinked while daemon is still serving"
        );

        // Once the daemon stops serving after being signaled, the PidMatch::Unknown
        // re-probe observes an unreachable daemon and cleans up stale files.
        let daemon_live = Arc::new(AtomicBool::new(true));
        let daemon_live_sig = Arc::clone(&daemon_live);
        let daemon_live_probe = Arc::clone(&daemon_live);
        let res = stop_live_daemon_with(
            &state_dir,
            &info,
            true,
            10,
            Duration::from_millis(10),
            Duration::from_secs(2),
            |_| PidMatch::Unknown,
            move |_pid, verified| {
                assert!(verified);
                daemon_live_sig.store(false, Ordering::SeqCst);
                Ok(true)
            },
            || {
                let live = Arc::clone(&daemon_live_probe);
                async move { Ok(live.load(Ordering::SeqCst)) }
            },
        )
        .await;
        assert!(res.is_ok(), "stopped daemon should succeed: {res:?}");
        assert!(
            !daemon_json.exists() && !sock_path.exists(),
            "stale runtime files should be cleaned up once daemon is confirmed dead"
        );
    }

    #[test]
    fn check_already_running_start_flags_rejects_override_flags() {
        let info = DaemonRuntimeInfo {
            pid: 4242,
            socket_path: Some("/tmp/daemon.sock".to_string()),
            tcp_addr: None,
            cache_dir: "/tmp/cache".to_string(),
            stats_path: "/tmp/stats.json".to_string(),
            default_upstream: None,
            started_unix_secs: 1,
            secret: None,
        };

        assert!(check_already_running_start_flags(&info, None, None, None, None).is_ok());
        assert!(
            check_already_running_start_flags(
                &info,
                Some("http://127.0.0.1:8080"),
                None,
                None,
                None,
            )
            .is_err()
        );
        assert!(
            check_already_running_start_flags(
                &info,
                None,
                Some(Path::new("/tmp/other.sock")),
                None,
                None,
            )
            .is_err()
        );
        assert!(
            check_already_running_start_flags(&info, None, None, Some("127.0.0.1:0"), None)
                .is_err()
        );
        let err = check_already_running_start_flags(&info, None, None, None, Some(60))
            .expect_err("explicit --idle-timeout-secs on running daemon must fail");
        assert!(
            format!("{err:?}").contains("jj cc daemon stop"),
            "error should guide user to stop daemon first: {err:?}"
        );
    }

    #[tokio::test]
    async fn stop_live_daemon_waits_for_pid_exit_after_daemon_json_is_unlinked() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().join("stop_unlink_race");
        ensure_private_state_dir(&state_dir).unwrap();
        let daemon_json = state_dir.join("daemon.json");

        let info = DaemonRuntimeInfo {
            pid: 4242,
            socket_path: Some(state_dir.join("daemon.sock").display().to_string()),
            tcp_addr: None,
            cache_dir: dir.path().join("cache").display().to_string(),
            stats_path: state_dir.join("daemon.stats.json").display().to_string(),
            default_upstream: None,
            started_unix_secs: 555,
            secret: None,
        };
        let format_runtime_json = |r: &DaemonRuntimeInfo| {
            format!(
                r#"{{"pid":{},"socket_path":{:?},"tcp_addr":null,"cache_dir":{:?},"stats_path":{:?},"default_upstream":null,"started_unix_secs":{}}}"#,
                r.pid,
                r.socket_path.as_deref().unwrap(),
                r.cache_dir,
                r.stats_path,
                r.started_unix_secs,
            )
        };
        fs::write(&daemon_json, format_runtime_json(&info)).unwrap();

        // Daemon unlinks `daemon.json` immediately on SIGTERM but keeps running (holding
        // `daemon.instance.lock`) for two polls before exiting.
        let pid_polls = Arc::new(AtomicUsize::new(0));
        let pid_polls_ref = Arc::clone(&pid_polls);
        let json_for_sig = daemon_json.clone();
        let res = stop_live_daemon_with(
            &state_dir,
            &info,
            true,
            10,
            Duration::from_millis(5),
            Duration::from_secs(2),
            move |_| {
                let n = pid_polls_ref.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    PidMatch::ConfirmedDaemon
                } else {
                    PidMatch::NotDaemon
                }
            },
            move |_pid, _verified| {
                fs::remove_file(&json_for_sig).unwrap();
                Ok(true)
            },
            || async { Ok(false) },
        )
        .await;
        assert!(
            res.is_ok(),
            "daemon exiting after unlinking daemon.json must succeed: {res:?}"
        );
        assert_eq!(
            pid_polls.load(Ordering::SeqCst),
            3,
            "stop must keep polling pid_check after daemon.json disappears until process exits"
        );

        // If `daemon.json` is gone and the process never exits, stop must time out rather than
        // returning early while `daemon.instance.lock` may still be held.
        let timeout_polls = Arc::new(AtomicUsize::new(0));
        let timeout_polls_ref = Arc::clone(&timeout_polls);
        let err = stop_live_daemon_with(
            &state_dir,
            &info,
            true,
            3,
            Duration::from_millis(5),
            Duration::from_secs(2),
            move |_| {
                timeout_polls_ref.fetch_add(1, Ordering::SeqCst);
                PidMatch::ConfirmedDaemon
            },
            |_pid, _verified| Ok(true),
            || async { Ok(false) },
        )
        .await
        .expect_err("stuck process after unlinking daemon.json must time out");
        assert!(err.contains("Timed out"), "unexpected error: {err}");
        assert_eq!(timeout_polls.load(Ordering::SeqCst), 3);

        // If a different daemon record replaces `daemon.json`, return Ok(()) immediately
        // without removing the replacement daemon's runtime files.
        let replacement = DaemonRuntimeInfo {
            pid: 9999,
            started_unix_secs: 666,
            ..info.clone()
        };
        let replacement_json = format_runtime_json(&replacement);
        let json_for_replace = daemon_json.clone();
        let replace_polls = Arc::new(AtomicUsize::new(0));
        let replace_polls_ref = Arc::clone(&replace_polls);
        let res = stop_live_daemon_with(
            &state_dir,
            &info,
            true,
            5,
            Duration::from_millis(5),
            Duration::from_secs(2),
            move |_| {
                replace_polls_ref.fetch_add(1, Ordering::SeqCst);
                PidMatch::NotDaemon
            },
            move |_pid, _verified| {
                fs::write(&json_for_replace, &replacement_json).unwrap();
                Ok(true)
            },
            || async { Ok(false) },
        )
        .await;
        assert!(
            res.is_ok(),
            "replaced daemon record must return Ok: {res:?}"
        );
        assert_eq!(replace_polls.load(Ordering::SeqCst), 0);
        assert_eq!(
            read_daemon_runtime_info(&state_dir).map(|r| r.pid),
            Some(9999),
            "replacement daemon.json must not be removed"
        );
    }
}
