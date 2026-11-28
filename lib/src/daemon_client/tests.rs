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

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use tonic::transport::{Channel, Endpoint};

#[cfg(unix)]
use std::sync::Arc;
#[cfg(unix)]
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(unix)]
use super::lifecycle::resolve_spawn_unix_socket_path;
use super::lifecycle::{
    cmdline_matches_daemon, linux_proc_stat_is_live, open_daemon_log, parse_unix_ps_output,
    parse_windows_tasklist_csv, send_terminate_signal, spawn_daemon_child,
    terminate_daemon_pid_with, wait_for_spawned_daemon,
};
use super::transport::{
    CachedDaemonState, DAEMON_ROUTE_CACHE, DAEMON_ROUTE_LIVENESS_TTL, check_cached_route_with,
    connect_existing_daemon_verified_with, connect_or_spawn_in, is_loopback_tcp_url,
};
use super::*;

#[test]
fn state_dir_fallback_uses_home_cache_and_never_temp_dir() {
    let home = Path::new("/home/alice");
    let cache = default_daemon_cache_dir_from(None, None, Some(home));
    assert_eq!(cache, PathBuf::from("/home/alice/.cache/jj-commit-cloud"));
    let state = default_daemon_state_dir_from(None, None, &cache);
    assert_eq!(
        state,
        PathBuf::from("/home/alice/.cache/jj-commit-cloud/run")
    );

    let missing_all_cache = default_daemon_cache_dir_from(None, None, None);
    assert!(missing_all_cache.as_os_str().is_empty());
    let missing_all_state = default_daemon_state_dir_from(None, None, &missing_all_cache);
    assert!(missing_all_state.as_os_str().is_empty());
}

#[cfg(unix)]
#[tokio::test]
async fn state_dir_rejects_group_or_world_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let insecure = dir.path().join("insecure");
    fs::create_dir_all(&insecure).unwrap();
    fs::set_permissions(&insecure, fs::Permissions::from_mode(0o755)).unwrap();

    assert!(ensure_private_state_dir(&insecure).is_err());
    assert!(connect_existing_daemon(&insecure).await.is_err());

    let secure = dir.path().join("secure");
    ensure_private_state_dir(&secure).unwrap();
    let mode = fs::metadata(&secure).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o700);
}

#[tokio::test]
async fn file_lock_blocks_concurrent_holder_and_releases_on_drop() {
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("lock_state");
    let guard = acquire_daemon_spawn_lock(&state_dir).await.unwrap();

    let lock_path = state_dir.join("daemon.spawn.lock");
    let contender = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock_path)
        .unwrap();
    assert!(
        matches!(contender.try_lock(), Err(fs::TryLockError::WouldBlock)),
        "second handle must observe active flock"
    );

    drop(guard);
    assert!(
        lock_path.exists(),
        "flock lockfile must stay in place across drops"
    );
    contender
        .try_lock()
        .expect("lock must release as soon as guard drops");
}

#[cfg(unix)]
#[tokio::test]
async fn failed_spawn_caches_negative_verdict_without_repeated_delays() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let bad_state = dir.path().join("bad_state");
    let cache_dir = dir.path().join("cache");
    fs::create_dir_all(&bad_state).unwrap();
    fs::set_permissions(&bad_state, fs::Permissions::from_mode(0o777)).unwrap();

    assert!(
        connect_or_spawn_in(&bad_state, &cache_dir, "http://127.0.0.1:1")
            .await
            .is_err()
    );

    let t0 = Instant::now();
    for _ in 0..25 {
        assert!(
            connect_or_spawn_in(&bad_state, &cache_dir, "http://127.0.0.1:1")
                .await
                .is_err()
        );
    }
    assert!(
        t0.elapsed() < Duration::from_millis(200),
        "cached negative verdict must avoid per-RPC spawn retries"
    );
}

#[test]
fn cmdline_matches_daemon_rejects_empty_and_unrelated_processes() {
    assert!(!cmdline_matches_daemon(b""));
    assert!(!cmdline_matches_daemon(b"\0jj-cc-daemon\0"));
    assert!(!cmdline_matches_daemon(b"/usr/bin/sleep\x0060\0"));
    assert!(!cmdline_matches_daemon(
        b"/usr/bin/grep\0jj-cc-daemon\0/var/log/syslog\0"
    ));
    assert!(!cmdline_matches_daemon(
        b"/home/alice/jj-cc-daemon/bin/python3\0-m\0http.server\0"
    ));
    assert!(!cmdline_matches_daemon(
        b"/usr/local/bin/jj-cc-daemon-helper\0"
    ));
    assert!(cmdline_matches_daemon(
        b"/usr/local/bin/jj-cc-daemon\0--state-dir\0/tmp\0"
    ));
    assert!(cmdline_matches_daemon(b"jj-cc-daemon\0"));
}

#[cfg(target_os = "linux")]
#[test]
fn pid_matches_daemon_returns_false_for_linux_zombie_process() {
    let mut child = Command::new("true")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("true should spawn");
    let pid = child.id();
    // Wait until the kernel has turned the un-reaped child into a zombie (empty cmdline).
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if fs::read(format!("/proc/{pid}/cmdline"))
            .map(|b| b.is_empty())
            .unwrap_or(false)
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        !pid_matches_daemon(pid),
        "Linux zombie PID with empty /proc/<pid>/cmdline must not match jj-cc-daemon"
    );
    let _ = child.wait();
}

#[test]
fn is_loopback_tcp_url_accepts_only_http_loopback_addresses() {
    for valid in [
        "http://127.0.0.1:50051",
        "http://127.0.0.2:8080",
        "http://[::1]:50051",
        "http://[0:0:0:0:0:0:0:1]:50051",
        "http://localhost:50051",
        "http://LOCALHOST:50051/",
    ] {
        assert!(
            is_loopback_tcp_url(valid),
            "expected loopback URL {valid:?} to be accepted"
        );
    }
    for invalid in [
        "http://192.168.1.10:50051",
        "http://169.254.169.254:80",
        "http://0.0.0.0:50051",
        "http://[::]:50051",
        "http://evil.example.com:50051",
        "http://localhost.evil.example.com:50051",
        "http://127.0.0.1@evil.example.com:50051",
        "https://127.0.0.1:50051",
        "http://127.0.0.1:50051/extra",
        "http://127.0.0.1",
        "not a url",
    ] {
        assert!(
            !is_loopback_tcp_url(invalid),
            "expected non-loopback or malformed URL {invalid:?} to be rejected"
        );
    }
}

#[tokio::test]
async fn connect_existing_daemon_refuses_non_loopback_tcp_addr() {
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("state");
    ensure_private_state_dir(&state_dir).unwrap();
    let info = DaemonRuntimeInfo {
        pid: std::process::id(),
        socket_path: None,
        tcp_addr: Some("http://192.0.2.1:50051".to_string()),
        cache_dir: dir.path().join("cache").display().to_string(),
        stats_path: state_dir.join("daemon.stats.json").display().to_string(),
        default_upstream: None,
        started_unix_secs: 1,
        secret: Some("top-secret-token".to_string()),
        wire_version: Some(DAEMON_WIRE_VERSION),
    };
    fs::write(
        state_dir.join("daemon.json"),
        serde_json::to_string(&info).unwrap(),
    )
    .unwrap();

    let res = connect_existing_daemon(&state_dir).await.unwrap();
    assert!(
        res.is_none(),
        "non-loopback tcp_addr must be ignored without connecting"
    );
}

#[cfg(unix)]
#[test]
fn spawn_daemon_child_rejects_unsafe_cache_dir_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let cache_dir = dir.path().join("unsafe_cache");
    ensure_private_state_dir(&state_dir).unwrap();
    fs::create_dir_all(&cache_dir).unwrap();
    fs::set_permissions(&cache_dir, fs::Permissions::from_mode(0o755)).unwrap();

    let err = spawn_daemon_child(&state_dir, &cache_dir, None, None, None, None, None)
        .expect_err("spawn must fail when cache_dir has unsafe 0755 permissions");
    assert!(
        err.to_string().contains("0700") || err.to_string().contains("mode"),
        "unexpected error: {err}"
    );
}

#[test]
fn linux_proc_stat_is_live_rejects_zombies_and_handles_parens_in_comm() {
    assert!(linux_proc_stat_is_live(
        "1234 (jj-cc-daemon) S 1 1234 1234 0 -1 4194304"
    ));
    assert!(linux_proc_stat_is_live(
        "1234 (jj (cc) daemon) R 1 1234 1234 0 -1"
    ));
    assert!(!linux_proc_stat_is_live(
        "1234 (jj-cc-daemon) Z 1 1234 1234 0 -1"
    ));
    assert!(!linux_proc_stat_is_live(
        "1234 (weird (proc)) Z 1 1234 1234 0 -1"
    ));
    assert!(!linux_proc_stat_is_live(
        "1234 (jj-cc-daemon) X 1 1234 1234 0 -1"
    ));
    assert!(!linux_proc_stat_is_live(""));
    assert!(!linux_proc_stat_is_live("1234 no-closing-paren"));
}

#[test]
fn parse_unix_ps_output_matches_live_daemon_and_rejects_zombies_and_unrelated() {
    assert!(parse_unix_ps_output("Ss   /usr/local/bin/jj-cc-daemon\n"));
    assert!(parse_unix_ps_output(
        "S    /Users/alice/My Apps/jj-cc-daemon\n"
    ));
    assert!(parse_unix_ps_output("S+   jj-cc-daemon\n"));
    assert!(!parse_unix_ps_output("Z    /usr/local/bin/jj-cc-daemon\n"));
    assert!(!parse_unix_ps_output("Z+   (jj-cc-daemon)\n"));
    assert!(!parse_unix_ps_output("S    /usr/bin/sleep\n"));
    assert!(!parse_unix_ps_output(
        "S    /usr/local/bin/jj-cc-daemon-helper\n"
    ));
    assert!(!parse_unix_ps_output(
        "S    /home/alice/jj-cc-daemon/bin/python3\n"
    ));
    assert!(!parse_unix_ps_output(""));
    assert!(!parse_unix_ps_output("S\n"));
}

#[test]
fn parse_windows_tasklist_csv_matches_exact_pid_and_image_name() {
    let valid = "\"jj-cc-daemon.exe\",\"4242\",\"Console\",\"1\",\"12,340 K\"\r\n";
    assert!(parse_windows_tasklist_csv(valid, 4242));
    assert!(!parse_windows_tasklist_csv(valid, 9999));

    let upper = "\"JJ-CC-DAEMON.EXE\",\"4242\",\"Console\",\"1\",\"12,340 K\"\r\n";
    assert!(parse_windows_tasklist_csv(upper, 4242));

    let unrelated = "\"notepad.exe\",\"4242\",\"Console\",\"1\",\"8,192 K\"\r\n";
    assert!(!parse_windows_tasklist_csv(unrelated, 4242));

    let helper = "\"jj-cc-daemon-helper.exe\",\"4242\",\"Console\",\"1\",\"8,192 K\"\r\n";
    assert!(!parse_windows_tasklist_csv(helper, 4242));

    let missing = "INFO: No tasks are running which match the specified criteria.\r\n";
    assert!(!parse_windows_tasklist_csv(missing, 4242));
    assert!(!parse_windows_tasklist_csv("", 4242));
}

#[cfg(unix)]
#[test]
fn terminate_daemon_pid_refuses_to_kill_unrelated_process() {
    let mut child = Command::new("sleep")
        .arg("60")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("sleep should spawn");
    let pid = child.id();
    assert!(!pid_matches_daemon(pid));

    terminate_daemon_pid(pid);
    std::thread::sleep(Duration::from_millis(50));
    assert!(
        child.try_wait().unwrap().is_none(),
        "terminate_daemon_pid must never signal an unrelated non-daemon PID"
    );
    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test]
async fn check_cached_route_falls_back_to_health_probe_when_pid_check_unknown() {
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("route_fallback_state");
    ensure_private_state_dir(&state_dir).unwrap();

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_service_status("", tonic_health::ServingStatus::Serving)
        .await;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming =
        tonic::transport::server::TcpIncoming::from_listener(listener, true, None).unwrap();
    let server_task = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(health_service)
            .serve_with_incoming(incoming),
    );

    let tcp_url = format!("http://{addr}");
    let channel = Endpoint::from_shared(tcp_url.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let info = DaemonRuntimeInfo {
        pid: std::process::id(),
        socket_path: None,
        tcp_addr: Some(tcp_url),
        cache_dir: dir.path().join("cache").display().to_string(),
        stats_path: state_dir.join("daemon.stats.json").display().to_string(),
        default_upstream: None,
        started_unix_secs: 42,
        secret: None,
        wire_version: Some(DAEMON_WIRE_VERSION),
    };
    fs::write(
        state_dir.join("daemon.json"),
        serde_json::to_string(&info).unwrap(),
    )
    .unwrap();

    let insert_stale = |ch: Channel| {
        DAEMON_ROUTE_CACHE.lock().unwrap().insert(
            state_dir.clone(),
            CachedDaemonState::Connected {
                info: info.clone(),
                channel: ch,
                verified_at: Instant::now() - DAEMON_ROUTE_LIVENESS_TTL - Duration::from_secs(1),
            },
        );
    };

    // When OS-level PID check is unavailable (`Unknown`), fall back to active health probe.
    insert_stale(channel.clone());
    let res = check_cached_route_with(&state_dir, |_| PidMatch::Unknown).await;
    assert!(
        matches!(res, Some(Ok(_))),
        "live channel must remain cached via health probe fallback when PID check is Unknown"
    );

    // When OS-level PID check confirms the PID is not the daemon (`NotDaemon`), evict.
    insert_stale(channel.clone());
    let evicted = check_cached_route_with(&state_dir, |_| PidMatch::NotDaemon).await;
    assert!(
        evicted.is_none(),
        "cached route must be evicted when OS PID inspection reports NotDaemon"
    );

    // When the server stops serving, `Unknown` fallback probe fails and evicts the cache.
    health_reporter
        .set_service_status("", tonic_health::ServingStatus::NotServing)
        .await;
    insert_stale(channel);
    let dead = check_cached_route_with(&state_dir, |_| PidMatch::Unknown).await;
    assert!(
        dead.is_none(),
        "cached route must be evicted when fallback health probe reports NotServing"
    );

    server_task.abort();
    DAEMON_ROUTE_CACHE.lock().unwrap().remove(&state_dir);
}

#[test]
fn daemon_runtime_info_debug_redacts_secret() {
    let with_secret = DaemonRuntimeInfo {
        pid: 42,
        socket_path: Some("/tmp/daemon.sock".to_string()),
        tcp_addr: None,
        cache_dir: "/tmp/cache".to_string(),
        stats_path: "/tmp/stats.json".to_string(),
        default_upstream: Some("http://127.0.0.1:50051".to_string()),
        started_unix_secs: 1_700_000_000,
        secret: Some("super-secret-value".to_string()),
        wire_version: Some(DAEMON_WIRE_VERSION),
    };
    let rendered = format!("{with_secret:?}");
    assert!(
        !rendered.contains("super-secret-value"),
        "Debug output must not leak daemon secret: {rendered}"
    );
    assert!(
        rendered.contains("<redacted>"),
        "Debug output should indicate secret is redacted: {rendered}"
    );

    let without_secret = DaemonRuntimeInfo {
        secret: None,
        ..with_secret
    };
    let rendered_none = format!("{without_secret:?}");
    assert!(
        rendered_none.contains("secret: None"),
        "Debug output should show None when secret is absent: {rendered_none}"
    );
}

#[test]
fn terminate_daemon_pid_with_handles_unknown_and_peer_verification() {
    let mut signaled = Vec::new();
    assert_eq!(
        terminate_daemon_pid_with(
            1234,
            false,
            |_| PidMatch::ConfirmedDaemon,
            |p| {
                signaled.push(p);
                Ok(())
            }
        ),
        Ok(true)
    );
    assert_eq!(signaled, vec![1234]);

    // PidMatch::Unknown without UDS peer PID verification must fail closed without signaling.
    signaled.clear();
    let err = terminate_daemon_pid_with(
        1234,
        false,
        |_| PidMatch::Unknown,
        |p| {
            signaled.push(p);
            Ok(())
        },
    )
    .expect_err("PidMatch::Unknown without peer PID verification must return Err");
    assert!(err.contains("Cannot verify"), "unexpected error: {err}");
    assert!(signaled.is_empty());

    // PidMatch::Unknown WITH UDS SO_PEERCRED + tonic_health verification may signal.
    assert_eq!(
        terminate_daemon_pid_with(
            1234,
            true,
            |_| PidMatch::Unknown,
            |p| {
                signaled.push(p);
                Ok(())
            }
        ),
        Ok(true)
    );
    assert_eq!(signaled, vec![1234]);

    // Signal delivery errors (for example EPERM) must propagate immediately.
    let sig_err = terminate_daemon_pid_with(
        1234,
        false,
        |_| PidMatch::ConfirmedDaemon,
        |_| Err("Operation not permitted".to_string()),
    )
    .expect_err("signal error must propagate");
    assert!(sig_err.contains("Operation not permitted"));

    // PidMatch::NotDaemon must never signal even if peer_pid_verified was previously true.
    signaled.clear();
    assert_eq!(
        terminate_daemon_pid_with(
            1234,
            true,
            |_| PidMatch::NotDaemon,
            |p| {
                signaled.push(p);
                Ok(())
            }
        ),
        Ok(false)
    );
    assert!(signaled.is_empty());
}

#[cfg(unix)]
#[test]
fn send_terminate_signal_rejects_invalid_pids_and_treats_esrch_as_ok() {
    assert!(send_terminate_signal(0).is_err());
    assert!(send_terminate_signal(1).is_err());

    // Spawn and reap a short-lived process so its PID returns ESRCH when signaled.
    let mut child = Command::new("true")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("true should spawn");
    let exited_pid = child.id();
    let _ = child.wait();
    assert_eq!(send_terminate_signal(exited_pid), Ok(()));
}

#[cfg(unix)]
#[tokio::test]
async fn wait_for_spawned_daemon_enforces_deadline_and_reaps_child_on_timeout_or_error() {
    use std::os::unix::fs::PermissionsExt;

    // Timeout path: stalled child is killed and reaped within the wall-clock deadline.
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("stall_state");
    ensure_private_state_dir(&state_dir).unwrap();

    let child = Command::new("sleep")
        .arg("60")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("sleep should spawn");
    let pid = child.id();
    let t0 = Instant::now();
    let err = wait_for_spawned_daemon(child, &state_dir, Duration::from_millis(120))
        .await
        .expect_err("stalled child must time out");
    assert!(err.to_string().contains("timed out"), "unexpected: {err}");
    assert!(
        t0.elapsed() < Duration::from_secs(2),
        "wall-clock timeout must bound wait: {:?}",
        t0.elapsed()
    );
    // Child must have been killed and reaped (ESRCH on kill(pid, 0)).
    let rc = unsafe { libc::kill(pid as i32, 0) };
    assert_ne!(rc, 0, "timed-out spawned child pid {pid} must not leak");

    // Error path: when `connect_existing_daemon` returns Err (insecure state_dir), the
    // child must still be killed and reaped by `SpawnedChildGuard`.
    let bad_state = dir.path().join("bad_state");
    fs::create_dir_all(&bad_state).unwrap();
    fs::set_permissions(&bad_state, fs::Permissions::from_mode(0o755)).unwrap();

    let child2 = Command::new("sleep")
        .arg("60")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("sleep should spawn");
    let pid2 = child2.id();
    assert!(
        wait_for_spawned_daemon(child2, &bad_state, Duration::from_secs(2))
            .await
            .is_err()
    );
    let rc2 = unsafe { libc::kill(pid2 as i32, 0) };
    assert_ne!(
        rc2, 0,
        "spawned child pid {pid2} must be killed and reaped when probe returns Err"
    );
}

#[tokio::test]
async fn connect_existing_daemon_verified_tcp_accepts_unknown_pid_with_secret() {
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("tcp_unknown_state");
    ensure_private_state_dir(&state_dir).unwrap();

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_service_status("", tonic_health::ServingStatus::Serving)
        .await;

    let expected_secret = "tcp-auth-secret-token";
    let secret_check = move |req: tonic::Request<()>| {
        let presented = req
            .metadata()
            .get(DAEMON_SECRET_HEADER)
            .and_then(|v| v.to_str().ok());
        if presented == Some(expected_secret) {
            Ok(req)
        } else {
            Err(tonic::Status::unauthenticated("missing secret"))
        }
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming =
        tonic::transport::server::TcpIncoming::from_listener(listener, true, None).unwrap();
    let server_task = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(tonic::service::interceptor::InterceptedService::new(
                health_service,
                secret_check,
            ))
            .serve_with_incoming(incoming),
    );

    let base_info = DaemonRuntimeInfo {
        pid: std::process::id(),
        socket_path: None,
        tcp_addr: Some(format!("http://{addr}")),
        cache_dir: dir.path().join("cache").display().to_string(),
        stats_path: state_dir.join("daemon.stats.json").display().to_string(),
        default_upstream: None,
        started_unix_secs: 77,
        secret: Some(expected_secret.to_string()),
        wire_version: Some(DAEMON_WIRE_VERSION),
    };
    fs::write(
        state_dir.join("daemon.json"),
        serde_json::to_string(&base_info).unwrap(),
    )
    .unwrap();

    // PidMatch::Unknown with valid secret connects and authenticates via tonic_health.
    let conn = connect_existing_daemon_verified_with(&state_dir, |_| PidMatch::Unknown, false)
        .await
        .unwrap()
        .expect("TCP daemon with valid secret should connect when PID check is Unknown");
    assert_eq!(conn.info.pid, base_info.pid);
    assert!(!conn.peer_pid_verified);
    assert!(conn.wire_version_mismatch.is_none());

    // PidMatch::NotDaemon must be rejected without connecting.
    let rejected =
        connect_existing_daemon_verified_with(&state_dir, |_| PidMatch::NotDaemon, false)
            .await
            .unwrap();
    assert!(rejected.is_none());

    // PidMatch::Unknown without a secret must fail closed.
    let no_secret = DaemonRuntimeInfo {
        secret: None,
        ..base_info
    };
    fs::write(
        state_dir.join("daemon.json"),
        serde_json::to_string(&no_secret).unwrap(),
    )
    .unwrap();
    let rejected_no_secret =
        connect_existing_daemon_verified_with(&state_dir, |_| PidMatch::Unknown, false)
            .await
            .unwrap();
    assert!(rejected_no_secret.is_none());

    server_task.abort();
}

#[cfg(unix)]
#[test]
fn write_user_daemon_preference_rejects_symlink_or_world_writable_dir_and_replaces_symlink() {
    use std::os::unix::fs::{PermissionsExt, symlink};

    let dir = tempfile::tempdir().unwrap();

    // Symlink parent directory must be rejected.
    let real_target = dir.path().join("real_target");
    fs::create_dir_all(&real_target).unwrap();
    fs::set_permissions(&real_target, fs::Permissions::from_mode(0o700)).unwrap();
    let sym_parent = dir.path().join("sym_parent");
    symlink(&real_target, &sym_parent).unwrap();
    assert!(write_user_daemon_preference(&sym_parent.join("daemon.toml"), true).is_err());

    // World-writable parent directory must be rejected.
    let world_writable = dir.path().join("world_writable");
    fs::create_dir_all(&world_writable).unwrap();
    fs::set_permissions(&world_writable, fs::Permissions::from_mode(0o777)).unwrap();
    assert!(write_user_daemon_preference(&world_writable.join("daemon.toml"), true).is_err());

    // User-owned 0755 config directory succeeds and atomically replaces a symlink target
    // file without modifying the symlinked victim file.
    let xdg_dir = dir.path().join("xdg");
    fs::create_dir_all(&xdg_dir).unwrap();
    fs::set_permissions(&xdg_dir, fs::Permissions::from_mode(0o755)).unwrap();
    let victim = dir.path().join("victim.txt");
    fs::write(&victim, "untouched").unwrap();
    let cfg_path = xdg_dir.join("daemon.toml");
    symlink(&victim, &cfg_path).unwrap();

    write_user_daemon_preference(&cfg_path, true).unwrap();
    assert_eq!(fs::read_to_string(&victim).unwrap(), "untouched");
    assert!(
        fs::symlink_metadata(&cfg_path)
            .unwrap()
            .file_type()
            .is_file()
    );
    let parsed: UserDaemonPreference =
        toml::from_str(&fs::read_to_string(&cfg_path).unwrap()).unwrap();
    assert!(parsed.enabled);
}

#[cfg(unix)]
#[tokio::test]
async fn connect_existing_daemon_verified_reports_uds_peer_pid_verification() {
    use tokio::net::UnixListener;
    use tonic::transport::server::UdsConnectInfo;

    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("uds_state");
    ensure_private_state_dir(&state_dir).unwrap();
    let sock_path = state_dir.join("daemon.sock");

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_service_status("", tonic_health::ServingStatus::Serving)
        .await;

    let listener = UnixListener::bind(&sock_path).unwrap();
    let incoming = futures::stream::unfold(listener, |l| async move {
        match l.accept().await {
            Ok((stream, _)) => Some((Ok::<_, std::io::Error>(stream), l)),
            Err(e) => Some((Err(e), l)),
        }
    });
    let server_task = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(health_service)
            .serve_with_incoming(incoming),
    );
    let _ = std::any::TypeId::of::<UdsConnectInfo>();

    let info = DaemonRuntimeInfo {
        pid: std::process::id(),
        socket_path: Some(sock_path.display().to_string()),
        tcp_addr: None,
        cache_dir: dir.path().join("cache").display().to_string(),
        stats_path: state_dir.join("daemon.stats.json").display().to_string(),
        default_upstream: None,
        started_unix_secs: 99,
        secret: None,
        wire_version: Some(DAEMON_WIRE_VERSION),
    };
    fs::write(
        state_dir.join("daemon.json"),
        serde_json::to_string(&info).unwrap(),
    )
    .unwrap();

    let conn = connect_existing_daemon_verified(&state_dir)
        .await
        .unwrap()
        .expect("live UDS health server with matching peer PID must connect");
    assert_eq!(conn.info.pid, std::process::id());
    assert!(
        conn.peer_pid_verified,
        "UDS SO_PEERCRED + tonic_health probe must report peer_pid_verified = true"
    );
    assert!(conn.wire_version_mismatch.is_none());

    server_task.abort();
}

#[cfg(unix)]
#[tokio::test]
async fn unix_symlink_hardening_rejects_trailing_slash_symlinks_and_log_symlink() {
    use std::os::unix::fs::{PermissionsExt, symlink};

    let dir = tempfile::tempdir().unwrap();
    let real_dir = dir.path().join("real_0700");
    fs::create_dir_all(&real_dir).unwrap();
    fs::set_permissions(&real_dir, fs::Permissions::from_mode(0o700)).unwrap();

    // Real 0700 directory with trailing slashes remains accepted.
    let real_with_slash = PathBuf::from(format!("{}/", real_dir.display()));
    verify_private_state_dir(&real_with_slash).unwrap();
    verify_user_config_dir(&real_with_slash).unwrap();
    assert_eq!(strip_trailing_slashes(Path::new("/")), Path::new("/"));
    assert_eq!(strip_trailing_slashes(Path::new("///")), Path::new("/"));

    // Symlink to a 0700 directory with trailing slashes must still be rejected.
    let sym_dir = dir.path().join("sym_state");
    symlink(&real_dir, &sym_dir).unwrap();
    for suffix in ["/", "///"] {
        let with_slash = PathBuf::from(format!("{}{suffix}", sym_dir.display()));
        let err = verify_private_state_dir(&with_slash)
            .expect_err("trailing-slash symlink state_dir must be rejected");
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(ensure_private_state_dir(&with_slash).is_err());
        assert!(connect_existing_daemon(&with_slash).await.is_err());
        let cfg_err = verify_user_config_dir(&with_slash)
            .expect_err("trailing-slash symlink config dir must be rejected");
        assert_eq!(cfg_err.kind(), std::io::ErrorKind::PermissionDenied);
    }

    // `open_daemon_log` must refuse to follow a symlink at `daemon.log` (`O_NOFOLLOW`).
    let victim = dir.path().join("victim.log");
    fs::write(&victim, "untouched").unwrap();
    symlink(&victim, real_dir.join("daemon.log")).unwrap();
    assert!(
        open_daemon_log(&real_dir).is_err(),
        "open_daemon_log must reject symlinked daemon.log via O_NOFOLLOW"
    );
    assert_eq!(fs::read_to_string(&victim).unwrap(), "untouched");
}

#[cfg(unix)]
#[tokio::test]
async fn mismatched_wire_version_is_rejected_on_connect_but_stoppable_via_any_version_probe() {
    use tokio::net::UnixListener;

    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("wire_mismatch_state");
    let cache_dir = dir.path().join("wire_mismatch_cache");
    ensure_private_state_dir(&state_dir).unwrap();
    ensure_private_state_dir(&cache_dir).unwrap();
    let sock_path = state_dir.join("daemon.sock");

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_service_status("", tonic_health::ServingStatus::Serving)
        .await;

    // Simulate a future v99 daemon whose health service rejects mismatched wire-version
    // headers while permitting unversioned probes.
    let saw_v1_header = Arc::new(AtomicBool::new(false));
    let saw_v1_ref = Arc::clone(&saw_v1_header);
    let v99_check = move |req: tonic::Request<()>| {
        if let Some(val) = req.metadata().get(DAEMON_WIRE_VERSION_HEADER) {
            if val.to_str().ok() == Some(DAEMON_WIRE_VERSION_STR) {
                saw_v1_ref.store(true, Ordering::Release);
            }
            if val.to_str().ok() != Some("99") {
                return Err(tonic::Status::failed_precondition(
                    "CLI/daemon wire version mismatch: expected 99; run `jj cc daemon stop`",
                ));
            }
        }
        Ok(req)
    };

    let listener = UnixListener::bind(&sock_path).unwrap();
    let incoming = futures::stream::unfold(listener, |l| async move {
        match l.accept().await {
            Ok((stream, _)) => Some((Ok::<_, std::io::Error>(stream), l)),
            Err(e) => Some((Err(e), l)),
        }
    });
    let server_task = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(tonic::service::interceptor::InterceptedService::new(
                health_service,
                v99_check,
            ))
            .serve_with_incoming(incoming),
    );

    let mismatched_info = DaemonRuntimeInfo {
        pid: std::process::id(),
        socket_path: Some(sock_path.display().to_string()),
        tcp_addr: None,
        cache_dir: cache_dir.display().to_string(),
        stats_path: state_dir.join("daemon.stats.json").display().to_string(),
        default_upstream: None,
        started_unix_secs: 123,
        secret: None,
        wire_version: Some(99),
    };
    fs::write(
        state_dir.join("daemon.json"),
        serde_json::to_string(&mismatched_info).unwrap(),
    )
    .unwrap();

    // Standard client connect fails fast with a clear wire-version mismatch error.
    let err = match connect_existing_daemon(&state_dir).await {
        Ok(_) => panic!("mismatched wire_version in daemon.json must return Err"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("wire version mismatch")
            && err.to_string().contains("jj cc daemon stop"),
        "unexpected mismatch error: {err}"
    );

    // `connect_or_spawn_in` surfaces the mismatch error so `connect_routed_channel` falls
    // back to direct mode instead of routing mismatched RPCs.
    let route_err = match connect_or_spawn_in(&state_dir, &cache_dir, "http://127.0.0.1:1").await {
        Ok(_) => panic!("connect_or_spawn_in must not route through a mismatched daemon"),
        Err(err) => err,
    };
    assert!(route_err.to_string().contains("wire version mismatch"));
    DAEMON_ROUTE_CACHE.lock().unwrap().remove(&state_dir);

    // `connect_existing_daemon_any_version_verified` (used by `jj cc daemon stop` and
    // `status`) still connects, verifies SO_PEERCRED peer PID, and surfaces the mismatch.
    let stoppable = connect_existing_daemon_any_version_verified(&state_dir)
        .await
        .unwrap()
        .expect("stop/status probe must still reach a live mismatched daemon");
    assert_eq!(stoppable.info.wire_version, Some(99));
    assert!(stoppable.peer_pid_verified);
    assert!(
        stoppable
            .wire_version_mismatch
            .as_deref()
            .is_some_and(|m| m.contains("wire version mismatch")),
        "mismatched daemon.json wire_version must be recorded on ExistingDaemonConnection: {:?}",
        stoppable.wire_version_mismatch
    );

    // `probe_daemon_channel_health` attaches `x-jj-cc-daemon-wire-version: 1` (and sees
    // the v99 server reject it), whereas `probe_daemon_channel_health_any_version` succeeds.
    assert!(
        !probe_daemon_channel_health(stoppable.channel.clone(), None).await,
        "versioned health probe must fail against v99 server"
    );
    assert!(
        saw_v1_header.load(Ordering::Acquire),
        "probe_daemon_channel_health must attach x-jj-cc-daemon-wire-version: 1"
    );
    assert!(
        probe_daemon_channel_health_any_version(stoppable.channel, None).await,
        "unversioned health probe must succeed so `jj cc daemon stop` can poll shutdown"
    );

    // Even if `daemon.json` omitted `wire_version` (None), a v99 server rejecting our
    // versioned health probe still surfaces as a mismatch error on normal connect while
    // remaining stoppable and reporting `wire_version_mismatch` for `jj cc daemon status`.
    let unversioned_record = DaemonRuntimeInfo {
        wire_version: None,
        ..mismatched_info.clone()
    };
    fs::write(
        state_dir.join("daemon.json"),
        serde_json::to_string(&unversioned_record).unwrap(),
    )
    .unwrap();
    let probe_err = match connect_existing_daemon(&state_dir).await {
        Ok(_) => panic!("server FAILED_PRECONDITION on versioned health probe must fail"),
        Err(err) => err,
    };
    assert!(probe_err.to_string().contains("wire version mismatch"));
    let unversioned_conn = connect_existing_daemon_any_version_verified(&state_dir)
        .await
        .unwrap()
        .expect("any-version probe must connect when daemon.json omits wire_version");
    assert!(
        unversioned_conn
            .wire_version_mismatch
            .as_deref()
            .is_some_and(|m| m.contains("wire version mismatch")),
        "server-reported wire version mismatch must be surfaced on ExistingDaemonConnection: {:?}",
        unversioned_conn.wire_version_mismatch
    );

    server_task.abort();
    let _ = fs::remove_file(&sock_path);

    // Stale `daemon.json` with a mismatched `wire_version` and no live listener returns
    // `Ok(None)` so stale state can be cleaned up or replaced.
    fs::write(
        state_dir.join("daemon.json"),
        serde_json::to_string(&mismatched_info).unwrap(),
    )
    .unwrap();
    assert!(connect_existing_daemon(&state_dir).await.unwrap().is_none());

    // A server returning `FAILED_PRECONDITION` even on an unversioned probe (when
    // `send_wire_version = false`) is treated as unavailable rather than a version mismatch.
    let (_, bad_health_service) = tonic_health::server::health_reporter();
    let always_failed_precondition =
        |_req: tonic::Request<()>| Err(tonic::Status::failed_precondition("unrelated failure"));
    let bad_listener = UnixListener::bind(&sock_path).unwrap();
    let bad_incoming = futures::stream::unfold(bad_listener, |l| async move {
        match l.accept().await {
            Ok((stream, _)) => Some((Ok::<_, std::io::Error>(stream), l)),
            Err(e) => Some((Err(e), l)),
        }
    });
    let bad_server_task = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(tonic::service::interceptor::InterceptedService::new(
                bad_health_service,
                always_failed_precondition,
            ))
            .serve_with_incoming(bad_incoming),
    );
    fs::write(
        state_dir.join("daemon.json"),
        serde_json::to_string(&mismatched_info).unwrap(),
    )
    .unwrap();
    assert!(
        connect_existing_daemon(&state_dir)
            .await
            .expect("unversioned FAILED_PRECONDITION must not masquerade as live mismatch")
            .is_none()
    );
    assert!(
        connect_existing_daemon_any_version_verified(&state_dir)
            .await
            .unwrap()
            .is_none()
    );
    bad_server_task.abort();
}

#[cfg(unix)]
#[test]
fn spawn_daemon_child_fails_fast_on_overlong_unix_socket_path() {
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let cache_dir = dir.path().join("cache");
    ensure_private_state_dir(&state_dir).unwrap();
    ensure_private_state_dir(&cache_dir).unwrap();

    let overlong_sock = dir
        .path()
        .join("s".repeat(cc_common::MAX_UNIX_SOCKET_PATH_BYTES))
        .join("daemon.sock");
    let err = spawn_daemon_child(
        &state_dir,
        &cache_dir,
        None,
        Some(&overlong_sock),
        None,
        None,
        None,
    )
    .expect_err("overlong Unix socket path must fail fast before spawning");
    assert!(
        err.to_string().contains("JJ_CC_DAEMON_SOCKET"),
        "unexpected overlong socket error: {err}"
    );
}

#[cfg(unix)]
#[test]
fn resolve_spawn_unix_socket_path_honors_env_var_override_and_filters_blanks() {
    use std::ffi::OsStr;

    let dir = tempfile::tempdir().unwrap();
    let overlong_state = dir
        .path()
        .join("s".repeat(cc_common::MAX_UNIX_SOCKET_PATH_BYTES));
    let short_env_sock = dir.path().join("short.sock");

    // Short `JJ_CC_DAEMON_SOCKET` rescues an overlong `state_dir`.
    let resolved_env =
        resolve_spawn_unix_socket_path(&overlong_state, None, Some(short_env_sock.as_os_str()));
    assert_eq!(resolved_env, short_env_sock);
    cc_common::resolve_unix_socket_path_or_ancestors(&resolved_env)
        .expect("short JJ_CC_DAEMON_SOCKET must validate even with overlong state_dir");

    // Overlong `JJ_CC_DAEMON_SOCKET` fails fast when validated.
    let overlong_env_sock = overlong_state.join("env.sock");
    let resolved_overlong =
        resolve_spawn_unix_socket_path(dir.path(), None, Some(overlong_env_sock.as_os_str()));
    assert_eq!(resolved_overlong, overlong_env_sock);
    assert!(cc_common::resolve_unix_socket_path_or_ancestors(&resolved_overlong).is_err());

    // Whitespace/empty `JJ_CC_DAEMON_SOCKET` falls back to `state_dir/daemon.sock`.
    assert_eq!(
        resolve_spawn_unix_socket_path(dir.path(), None, Some(OsStr::new("   "))),
        dir.path().join("daemon.sock")
    );
    assert_eq!(
        resolve_spawn_unix_socket_path(dir.path(), None, Some(OsStr::new(""))),
        dir.path().join("daemon.sock")
    );

    // Explicit `--socket` override takes precedence over `JJ_CC_DAEMON_SOCKET`.
    let explicit_sock = dir.path().join("explicit.sock");
    assert_eq!(
        resolve_spawn_unix_socket_path(
            dir.path(),
            Some(&explicit_sock),
            Some(short_env_sock.as_os_str()),
        ),
        explicit_sock
    );
}

#[cfg(unix)]
#[tokio::test]
async fn stale_or_malformed_socket_path_in_daemon_json_returns_none_instead_of_error() {
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("stale_sock_state");
    ensure_private_state_dir(&state_dir).unwrap();
    let regular_file = state_dir.join("not_a_socket");
    fs::write(&regular_file, b"plain").unwrap();
    let overlong_nonexistent = dir
        .path()
        .join("x".repeat(cc_common::MAX_UNIX_SOCKET_PATH_BYTES))
        .join("missing.sock");

    for bad_sock in [
        "/".to_string(),
        regular_file.display().to_string(),
        overlong_nonexistent.display().to_string(),
    ] {
        let info = DaemonRuntimeInfo {
            pid: std::process::id(),
            socket_path: Some(bad_sock.clone()),
            tcp_addr: None,
            cache_dir: dir.path().join("cache").display().to_string(),
            stats_path: state_dir.join("daemon.stats.json").display().to_string(),
            default_upstream: None,
            started_unix_secs: 1,
            secret: None,
            wire_version: Some(DAEMON_WIRE_VERSION),
        };
        fs::write(
            state_dir.join("daemon.json"),
            serde_json::to_string(&info).unwrap(),
        )
        .unwrap();

        assert!(
            connect_existing_daemon(&state_dir)
                .await
                .expect("stale/malformed socket_path must not wedge discovery")
                .is_none(),
            "expected Ok(None) for bad socket_path {bad_sock:?}"
        );
        assert!(
            connect_existing_daemon_any_version_verified(&state_dir)
                .await
                .expect("stale/malformed socket_path must not wedge stop/status")
                .is_none(),
            "expected Ok(None) from any-version probe for bad socket_path {bad_sock:?}"
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn acquire_daemon_spawn_lock_restores_strict_0600_mode() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("lock_mode_state");
    ensure_private_state_dir(&state_dir).unwrap();
    let lock_path = state_dir.join("daemon.spawn.lock");
    fs::write(&lock_path, b"").unwrap();
    fs::set_permissions(&lock_path, fs::Permissions::from_mode(0o1644)).unwrap();

    let _guard = acquire_daemon_spawn_lock(&state_dir).await.unwrap();
    let mode = fs::metadata(&lock_path).unwrap().permissions().mode() & 0o7777;
    assert_eq!(mode, 0o600, "daemon.spawn.lock must be enforced to 0600");

    let log_path = state_dir.join("daemon.log");
    fs::write(&log_path, b"").unwrap();
    fs::set_permissions(&log_path, fs::Permissions::from_mode(0o1600)).unwrap();
    let _log = open_daemon_log(&state_dir).unwrap();
    let log_mode = fs::metadata(&log_path).unwrap().permissions().mode() & 0o7777;
    assert_eq!(
        log_mode, 0o600,
        "daemon.log must clear special bits and enforce 0600"
    );
}
