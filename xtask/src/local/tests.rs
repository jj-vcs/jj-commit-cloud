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

use std::ffi::OsStr;
use std::time::{Duration, Instant};

use super::health::{
    build_health_endpoint, check_grpc_health_with_token, is_https_url, resolve_bearer_token_with,
    wait_until_healthy_with_containers_poll,
};
use super::native::wait_for_native_spanner_emulator;
use super::process::{
    LINUX_ESRCH, is_linux_proc_gone_error, parse_ps_stat_and_command, process_command_line,
    stop_process_with_timeouts,
};
use crate::state::{AuthMode, ManagedProcess};

use super::*;

#[test]
fn stop_managed_processes_skips_unrelated_live_process() {
    let _proc_guard = crate::lock_test_proc();
    let self_pid = std::process::id();
    let mut st = EnvState::new(
        "default",
        "memory",
        "native",
        "http://127.0.0.1:8080",
        None,
        AuthMode::None,
    );
    st.resources.processes.push(ManagedProcess::new(
        self_pid,
        "jj-cc-server-marker-never-matches-unit-test",
    ));
    st.resources.pids.push(self_pid);
    let failures = stop_managed_processes(&st);
    assert!(
        failures.is_empty(),
        "expected no failures, got {failures:?}"
    );
}

#[cfg(unix)]
#[test]
fn spawn_bg_logs_output_and_stop_managed_processes_terminates_matching_child() {
    let _proc_guard = crate::lock_test_proc();
    let tmp = tempfile::tempdir().unwrap();
    let log_path = tmp.path().join(".jj-cc").join("dev.log");
    let mut child = spawn_bg_child(
        Path::new("sh"),
        ["-c", "echo native-server-booted; exec sleep 30"],
        &[],
        &log_path,
    )
    .unwrap();
    let pid = child.id();

    for _ in 0..200 {
        let logged = fs::read_to_string(&log_path).unwrap_or_default();
        let cmdline = process_command_line(pid).unwrap().unwrap_or_default();
        if logged.contains("native-server-booted") && cmdline.contains("sleep") {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let logged = fs::read_to_string(&log_path).unwrap();
    assert!(
        logged.contains("native-server-booted"),
        "unexpected log: {logged:?}"
    );

    let mut st = EnvState::new(
        "dev",
        "memory",
        "native",
        "http://127.0.0.1:8080",
        None,
        AuthMode::None,
    );
    st.resources
        .processes
        .push(ManagedProcess::new(pid, "sleep"));
    let failures = stop_managed_processes(&st);
    assert!(failures.is_empty(), "expected clean stop, got {failures:?}");
    let exit = child.wait().unwrap();
    assert!(!exit.success());
}

#[cfg(unix)]
#[test]
fn stop_process_escalates_to_sigkill_when_sigterm_is_ignored() {
    let _proc_guard = crate::lock_test_proc();
    let tmp = tempfile::tempdir().unwrap();
    let log_path = tmp.path().join(".jj-cc").join("stubborn.log");
    let mut child = spawn_bg_child(
        Path::new("sh"),
        [
            "-c",
            "trap '' TERM; echo stubborn-ready; while true; do sleep 1; done",
        ],
        &[],
        &log_path,
    )
    .unwrap();
    let pid = child.id();

    for _ in 0..200 {
        let logged = fs::read_to_string(&log_path).unwrap_or_default();
        if logged.contains("stubborn-ready") {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    stop_process_with_timeouts(
        pid,
        &["stubborn-ready"],
        Duration::from_millis(150),
        Duration::from_secs(2),
    )
    .unwrap();
    let exit = child.wait().unwrap();
    assert!(!exit.success());
}

#[test]
fn resolve_bearer_token_prefers_recorded_and_filters_blank_env_tokens() {
    let tok = resolve_bearer_token_with(
        AuthMode::None,
        Some("  generated-secret  "),
        Some("env-secret".to_string()),
        || Ok("minted".to_string()),
    )
    .unwrap();
    assert_eq!(tok.as_deref(), Some("generated-secret"));

    let blank_none =
        resolve_bearer_token_with(AuthMode::None, None, Some("   \t\n ".to_string()), || {
            unreachable!("AuthMode::None must not mint")
        })
        .unwrap();
    assert_eq!(blank_none, None);

    let blank_mints = resolve_bearer_token_with(
        AuthMode::GcloudIdentityToken,
        Some("  "),
        Some("  ".to_string()),
        || Ok("minted-id-token".to_string()),
    )
    .unwrap();
    assert_eq!(blank_mints.as_deref(), Some("minted-id-token"));
}

#[test]
fn is_https_url_checks_scheme_case_insensitively() {
    assert!(is_https_url("https://example.run.app"));
    assert!(is_https_url("HTTPS://example.run.app"));
    assert!(is_https_url("HtTpS://example.run.app:443"));
    assert!(!is_https_url("http://127.0.0.1:8080"));
    assert!(!is_https_url("https:"));
}

#[test]
fn resolve_local_ports_rejects_zero_and_duplicate_multiregion_ports() {
    assert_eq!(
        resolve_local_ports(LocalMode::Sqlite, 8080, 8082).unwrap(),
        (8080, None)
    );
    assert_eq!(
        resolve_local_ports(LocalMode::Multiregion, 8080, 8082).unwrap(),
        (8081, Some(8082))
    );
    assert!(resolve_local_ports(LocalMode::Sqlite, 0, 8082).is_err());
    assert!(resolve_local_ports(LocalMode::Multiregion, 0, 8082).is_err());
    assert!(resolve_local_ports(LocalMode::Multiregion, 8080, 0).is_err());
    assert!(resolve_local_ports(LocalMode::Multiregion, 8080, 8081).is_err());
    assert!(resolve_local_ports(LocalMode::Multiregion, 9000, 9000).is_err());
}

#[test]
fn sanitize_server_env_marks_all_server_vars_removed() {
    let mut cmd = Command::new("jj-cc-server");
    sanitize_server_env(&mut cmd);
    let removed: std::collections::HashSet<&OsStr> = cmd
        .get_envs()
        .filter_map(|(k, v)| v.is_none().then_some(k))
        .collect();
    for required in [
        "HOST",
        "PORT",
        "STORE_TYPE",
        "BLOB_STORE",
        "BLOB_PATH",
        "SQLITE_PATH",
        "POSTGRES_URL",
        "DATABASE_URL",
        "POSTGRES_MAX_CONNS",
        "POSTGRES_AUTO_MIGRATE",
        "POSTGRES_CA_CERT",
        "GCS_BUCKET",
        "GCS_ENDPOINT",
        "GCS_PEER_BUCKETS",
        "GCS_REPLICATION_RETRY_MS",
        "GCS_ASYNC_WRITE_FANOUT",
        "S3_BUCKET",
        "S3_ENDPOINT",
        "S3_REGION",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
        "S3_ACCESS_KEY_ID",
        "AWS_ACCESS_KEY_ID",
        "S3_SECRET_ACCESS_KEY",
        "AWS_SECRET_ACCESS_KEY",
        "S3_SESSION_TOKEN",
        "AWS_SESSION_TOKEN",
        "S3_PATH_STYLE",
        "S3_AUTO_CREATE_BUCKET",
        "S3_PEER_BUCKETS",
        "LOCAL_PEER_BLOB_DIRS",
        "BLOB_CACHE_BYTES",
        "BLOB_LOCATION_ID",
        "INLINE_BLOB_THRESHOLD_BYTES",
        "SPANNER_DATABASE",
        "SPANNER_EMULATOR",
        "SPANNER_EMULATOR_HOST",
        "SPANNER_OMNI",
        "SPANNER_OMNI_ENDPOINT",
        "SPANNER_AUTO_MIGRATE",
        "JJ_CC_SERVER_AUTH_TOKEN",
        "JJ_CC_SERVER_AUTH_TOKEN_FILE",
        "JJ_CC_ALLOW_UNAUTHENTICATED",
        "JJ_CC_AUTH_OIDC_ISSUER",
        "JJ_CC_AUTH_OIDC_AUDIENCE",
        "JJ_CC_AUTH_OIDC_JWKS_URI",
        "JJ_CC_AUTH_OIDC_CA_CERT",
        "JJ_CC_AUTH_ENFORCE_ACTOR_REPO_SUFFIX",
        "JJ_CC_AUTH_K8S_TOKENREVIEW_URL",
        "JJ_CC_AUTH_K8S_TOKENREVIEW_AUD",
        "JJ_CC_AUTH_K8S_CA_CERT",
        "JJ_CC_AUTH_K8S_SA_TOKEN_FILE",
        "METRICS_LISTEN",
    ] {
        assert!(
            removed.contains(OsStr::new(required)),
            "expected {required} to be cleared"
        );
    }
}

#[test]
fn is_linux_proc_gone_error_matches_not_found_and_esrch() {
    assert!(is_linux_proc_gone_error(&std::io::Error::from(
        std::io::ErrorKind::NotFound
    )));
    assert!(is_linux_proc_gone_error(
        &std::io::Error::from_raw_os_error(LINUX_ESRCH)
    ));
    assert!(!is_linux_proc_gone_error(&std::io::Error::from(
        std::io::ErrorKind::PermissionDenied
    )));
}

#[tokio::test]
async fn https_health_endpoint_installs_crypto_provider_and_unbrackets_ipv6() {
    let ep = build_health_endpoint("https://[::1]:50051")
        .expect("https://[::1]:50051 should configure TLS cleanly");
    assert_eq!(ep.uri().host(), Some("[::1]"));
    let _lazy = ep.connect_lazy();

    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let err = check_grpc_health_with_token(&format!("https://127.0.0.1:{port}"), None)
        .await
        .expect_err("closed local HTTPS port should return a transport error without panicking");
    assert!(!err.to_string().is_empty());
}

#[test]
fn ensure_loopback_port_free_detects_bound_port_and_succeeds_after_drop() {
    let _proc_guard = crate::lock_test_proc();
    assert!(ensure_loopback_port_free(0).is_err());
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    assert!(ensure_loopback_port_free(port).is_err());
    drop(listener);
    assert!(ensure_loopback_port_free(port).is_ok());
}

#[test]
fn parse_ps_stat_and_command_ignores_zombies_and_empty_output() {
    assert_eq!(parse_ps_stat_and_command("Z+ (sleep)\n"), None);
    assert_eq!(parse_ps_stat_and_command("Z (jj-cc-server)\n"), None);
    assert_eq!(parse_ps_stat_and_command("  Z+   (sleep)  \n"), None);
    assert_eq!(
        parse_ps_stat_and_command("S  /usr/bin/sleep 60\n"),
        Some("/usr/bin/sleep 60".to_string())
    );
    assert_eq!(
        parse_ps_stat_and_command("  S+   /usr/local/bin/jj-cc-server --port=8080  \n"),
        Some("/usr/local/bin/jj-cc-server --port=8080".to_string())
    );
    assert_eq!(parse_ps_stat_and_command(""), None);
    assert_eq!(parse_ps_stat_and_command("   \n  "), None);
    assert_eq!(parse_ps_stat_and_command("S   \n"), None);
}

#[test]
fn resolve_spanner_emulator_endpoints_ignores_blank_host_and_rest_vars() {
    assert_eq!(resolve_spanner_emulator_endpoints(None, None), None);
    assert_eq!(
        resolve_spanner_emulator_endpoints(Some("   ".to_string()), None),
        None
    );
    assert_eq!(
        resolve_spanner_emulator_endpoints(
            Some(" 127.0.0.1:9010 ".to_string()),
            Some("   \t ".to_string()),
        ),
        Some((
            "127.0.0.1:9010".to_string(),
            "http://127.0.0.1:9020".to_string()
        ))
    );
    assert_eq!(
        resolve_spanner_emulator_endpoints(
            Some("127.0.0.1:9010".to_string()),
            Some(" http://127.0.0.1:19020 ".to_string()),
        ),
        Some((
            "127.0.0.1:9010".to_string(),
            "http://127.0.0.1:19020".to_string()
        ))
    );
}

#[test]
fn prepare_local_up_validates_ports_driver_and_native_env_upfront() {
    // Duplicate multiregion ports fail before driver or teardown.
    assert!(
        prepare_local_up_with(
            LocalMode::Multiregion,
            LocalDriver::Auto,
            ContainerRuntimeChoice::Auto,
            8080,
            8081,
            |_| Ok("docker".to_string()),
            |_| None,
        )
        .is_err()
    );

    // Explicit container driver fails upfront when no container runtime is available.
    let err = prepare_local_up_with(
        LocalMode::Sqlite,
        LocalDriver::Container,
        ContainerRuntimeChoice::Auto,
        8080,
        8082,
        |_| Err("no running docker/podman".into()),
        |_| None,
    )
    .unwrap_err();
    assert!(err.to_string().contains("no running docker/podman"));

    // Native Postgres requires POSTGRES_URL or DATABASE_URL upfront.
    let err = prepare_local_up_with(
        LocalMode::Postgres,
        LocalDriver::Native,
        ContainerRuntimeChoice::Auto,
        8080,
        8082,
        |_| unreachable!("native driver must not probe container runtime"),
        |_| None,
    )
    .unwrap_err();
    assert!(err.to_string().contains("POSTGRES_URL"));

    let pg_plan = prepare_local_up_with(
        LocalMode::Postgres,
        LocalDriver::Native,
        ContainerRuntimeChoice::Auto,
        8080,
        8082,
        |_| unreachable!(),
        |k| (k == "DATABASE_URL").then(|| "postgres://jj:jj@127.0.0.1/db".to_string()),
    )
    .unwrap();
    assert_eq!(
        pg_plan,
        LocalUpPlan {
            mode: LocalMode::Postgres,
            primary_port: 8080,
            peer_port: None,
            target: LocalUpTarget::Native {
                native_pg_url: Some("postgres://jj:jj@127.0.0.1/db".to_string()),
                native_spanner: None,
            },
        }
    );

    // Native Spanner fails upfront when neither SPANNER_EMULATOR_HOST nor emulator dir exists.
    let err = prepare_local_up_with(
        LocalMode::Spanner,
        LocalDriver::Native,
        ContainerRuntimeChoice::Auto,
        8080,
        8082,
        |_| unreachable!(),
        |_| None,
    )
    .unwrap_err();
    assert!(err.to_string().contains("SPANNER_EMULATOR_HOST"));
}

#[cfg(unix)]
#[test]
fn wait_until_healthy_with_containers_fails_fast_when_container_exits() {
    use std::os::unix::fs::PermissionsExt;
    let _proc_guard = crate::lock_test_proc();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let fake_rt = tmp.path().join("fake-docker.sh");
    fs::write(
        &fake_rt,
        r#"#!/bin/sh
if [ "$1" = "inspect" ]; then
  if [ "$4" = "jj-cc-dev-server" ]; then
echo "false"
  else
echo "true"
  fi
  exit 0
fi
exit 1
"#,
    )
    .unwrap();
    fs::set_permissions(&fake_rt, fs::Permissions::from_mode(0o755)).unwrap();

    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let unused_port = listener.local_addr().unwrap().port();
    drop(listener);

    let started = Instant::now();
    let err = rt
        .block_on(wait_until_healthy_with_containers_poll(
            &format!("http://127.0.0.1:{unused_port}"),
            AuthMode::None,
            None,
            Duration::from_secs(10),
            Duration::from_millis(20),
            &fake_rt,
            &[
                "jj-cc-dev-spanner".to_string(),
                "jj-cc-dev-server".to_string(),
            ],
        ))
        .unwrap_err();
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "expected immediate fail-fast on exited container, took {:?}",
        started.elapsed()
    );
    let msg = err.to_string();
    assert!(
        msg.contains("jj-cc-dev-server") && msg.contains("logs jj-cc-dev-server"),
        "unexpected error: {msg}"
    );
}

#[cfg(unix)]
#[test]
fn wait_for_native_spanner_emulator_fails_fast_on_exit_and_reaps_on_timeout() {
    let _proc_guard = crate::lock_test_proc();
    let tmp = tempfile::tempdir().unwrap();
    let log_path = tmp.path().join("emu.log");

    let l1 = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let p1 = l1.local_addr().unwrap().port();
    let l2 = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let p2 = l2.local_addr().unwrap().port();
    drop(l1);
    drop(l2);
    let grpc_addr = std::net::SocketAddr::from(([127, 0, 0, 1], p1));
    let rest_addr = std::net::SocketAddr::from(([127, 0, 0, 1], p2));

    // Immediate crash fails fast with exit status and log path.
    let mut crashing = spawn_bg_child(Path::new("sh"), ["-c", "exit 7"], &[], &log_path).unwrap();
    let started = Instant::now();
    let err = wait_for_native_spanner_emulator(
        &mut crashing,
        grpc_addr,
        rest_addr,
        Duration::from_secs(10),
        &log_path,
    )
    .unwrap_err();
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "expected fast failure on exit, took {:?}",
        started.elapsed()
    );
    let msg = err.to_string();
    assert!(
        msg.contains("exited during startup") && msg.contains(&log_path.display().to_string()),
        "unexpected crash error: {msg}"
    );

    // Non-listening process times out, is terminated, and is reaped without leaving a zombie.
    let mut hanging =
        spawn_bg_child(Path::new("sh"), ["-c", "exec sleep 30"], &[], &log_path).unwrap();
    let hanging_pid = hanging.id();
    let err = wait_for_native_spanner_emulator(
        &mut hanging,
        grpc_addr,
        rest_addr,
        Duration::from_millis(150),
        &log_path,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("did not listen"),
        "unexpected timeout error: {err}"
    );
    assert!(
        process_command_line(hanging_pid).unwrap().is_none(),
        "timed-out emulator process (PID {hanging_pid}) should be terminated and reaped"
    );
}
