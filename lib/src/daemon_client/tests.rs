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

    let err = spawn_daemon_child(&state_dir, &cache_dir, None, None, None, None)
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
    };
    fs::write(
        state_dir.join("daemon.json"),
        serde_json::to_string(&base_info).unwrap(),
    )
    .unwrap();

    // 1. PidMatch::Unknown with valid secret connects and authenticates via tonic_health.
    let conn = connect_existing_daemon_verified_with(&state_dir, |_| PidMatch::Unknown)
        .await
        .unwrap()
        .expect("TCP daemon with valid secret should connect when PID check is Unknown");
    assert_eq!(conn.info.pid, base_info.pid);
    assert!(!conn.peer_pid_verified);

    // 2. PidMatch::NotDaemon must be rejected without connecting.
    let rejected = connect_existing_daemon_verified_with(&state_dir, |_| PidMatch::NotDaemon)
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
        connect_existing_daemon_verified_with(&state_dir, |_| PidMatch::Unknown)
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
