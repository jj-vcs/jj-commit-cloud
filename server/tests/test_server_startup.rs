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

use cc_common::backend::RegisterRepositoryRequest;
use cc_common::backend::backend_service_client::BackendServiceClient;
use cc_common::op_store::GetOpHeadsRequest;
use cc_common::op_store::op_store_service_client::OpStoreServiceClient;
use tonic::{Code, Request};
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_client::HealthClient;

#[tokio::test]
async fn test_server_startup_succeeds() {
    let server = testutils::spawn_server().await;

    let channel = tonic::transport::Endpoint::from_shared(server.url().to_string())
        .expect("The gRPC endpoint URI should be valid")
        .connect()
        .await
        .expect("The gRPC connection to the health service should have succeeded");

    let mut client = HealthClient::new(channel);

    let response = client
        .check(HealthCheckRequest {
            service: "".to_string(),
        })
        .await
        .expect("The gRPC health check request should have succeeded");

    assert_eq!(
        response.into_inner().status(),
        tonic_health::pb::health_check_response::ServingStatus::Serving,
        "Server is not in SERVING state"
    );
}

#[tokio::test]
async fn test_server_bearer_auth_enforces_tokens_while_keeping_health_open() {
    let server = testutils::spawn_server_with_env(
        &[("JJ_CC_SERVER_AUTH_TOKEN", "active-token-1,rotated-token-2")],
        &[],
    )
    .await;

    let channel = tonic::transport::Endpoint::from_shared(server.url().to_string())
        .expect("valid endpoint")
        .connect()
        .await
        .expect("connect succeeded");

    // Kubernetes / Cloud Run readiness probes (both overall and service-scoped) stay open.
    let mut health = HealthClient::new(channel.clone());
    for service in [
        "",
        "commit_cloud.backend.BackendService",
        "commit_cloud.op_store.OpStoreService",
    ] {
        let health_res = health
            .check(HealthCheckRequest {
                service: service.to_string(),
            })
            .await
            .expect("health check should succeed without auth token");
        assert_eq!(
            health_res.into_inner().status(),
            tonic_health::pb::health_check_response::ServingStatus::Serving,
            "service {service:?} should report SERVING"
        );
    }

    // Unauthenticated BackendService and OpStoreService calls are rejected.
    let mut backend = BackendServiceClient::new(channel.clone());
    let unauth_err = backend
        .register_repository(Request::new(RegisterRepositoryRequest { name: None }))
        .await
        .expect_err("unauthenticated RegisterRepository must fail");
    assert_eq!(unauth_err.code(), Code::Unauthenticated);

    let mut op_store = OpStoreServiceClient::new(channel);
    let mut bad_req = Request::new(GetOpHeadsRequest {
        repo_id: "any".to_string(),
    });
    bad_req
        .metadata_mut()
        .insert("authorization", "Bearer wrong-token".parse().unwrap());
    let bad_err = op_store
        .get_op_heads(bad_req)
        .await
        .expect_err("wrong Bearer token must fail");
    assert_eq!(bad_err.code(), Code::Unauthenticated);

    // Both active and rotated tokens succeed.
    let mut req1 = Request::new(RegisterRepositoryRequest { name: None });
    req1.metadata_mut()
        .insert("authorization", "Bearer active-token-1".parse().unwrap());
    let repo_id = backend
        .register_repository(req1)
        .await
        .expect("primary token should succeed")
        .into_inner()
        .repo_id;

    let mut req2 = Request::new(GetOpHeadsRequest { repo_id });
    req2.metadata_mut()
        .insert("authorization", "Bearer rotated-token-2".parse().unwrap());
    let heads = op_store
        .get_op_heads(req2)
        .await
        .expect("rotated token should succeed")
        .into_inner()
        .op_head_ids;
    assert_eq!(heads.len(), 1);
}

fn run_server_cli(env_vars: &[(&str, &str)], args: &[&str]) -> std::process::Output {
    use std::io::Read as _;
    use std::process::Stdio;
    use std::time::{Duration, Instant};

    let mut cmd = std::process::Command::new(testutils::server_binary_path());
    for var in testutils::SANITIZED_SERVER_ENV_VARS {
        cmd.env_remove(var);
    }
    for (k, v) in env_vars {
        cmd.env(k, v);
    }
    let mut child = cmd
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("jj-cc-server binary should spawn");
    let mut child_stdout = child.stdout.take().expect("piped stdout");
    let mut child_stderr = child.stderr.take().expect("piped stderr");
    let out_handle = std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = child_stdout.read_to_end(&mut buf);
        buf
    });
    let err_handle = std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = child_stderr.read_to_end(&mut buf);
        buf
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = child.try_wait().expect("try_wait failed") {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("jj-cc-server {args:?} did not exit within 10s (unexpectedly started serving)");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    std::process::Output {
        status,
        stdout: out_handle.join().unwrap_or_default(),
        stderr: err_handle.join().unwrap_or_default(),
    }
}

#[test]
fn test_server_auth_rejects_empty_tokens_and_hides_env_in_help() {
    let secret = "super-secret-env-token-xyz";
    let help = run_server_cli(&[("JJ_CC_SERVER_AUTH_TOKEN", secret)], &["--help"]);
    assert!(help.status.success());
    let help_stdout = String::from_utf8_lossy(&help.stdout);
    let help_stderr = String::from_utf8_lossy(&help.stderr);
    assert!(
        !help_stdout.contains(secret) && !help_stderr.contains(secret),
        "--help leaked JJ_CC_SERVER_AUTH_TOKEN value"
    );

    for bad_env in ["", " , "] {
        let out = run_server_cli(&[("JJ_CC_SERVER_AUTH_TOKEN", bad_env)], &["--port=0"]);
        assert!(
            !out.status.success(),
            "empty JJ_CC_SERVER_AUTH_TOKEN={bad_env:?} must be rejected"
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("contains no non-empty token"),
            "unexpected stderr: {stderr}"
        );
    }

    for bad_arg in ["--auth-token=", "--auth-token= , "] {
        let out = run_server_cli(&[], &["--port=0", bad_arg]);
        assert!(!out.status.success(), "{bad_arg:?} must be rejected");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("contains no non-empty token"),
            "unexpected stderr: {stderr}"
        );
    }

    let non_loopback = run_server_cli(&[], &["--host=0.0.0.0", "--port=0"]);
    assert!(
        !non_loopback.status.success(),
        "unauthenticated non-loopback bind must fail closed"
    );
    assert!(
        String::from_utf8_lossy(&non_loopback.stderr).contains("refusing to bind non-loopback"),
        "unexpected stderr: {}",
        String::from_utf8_lossy(&non_loopback.stderr)
    );

    let whitespace_tok = run_server_cli(
        &[("JJ_CC_SERVER_AUTH_TOKEN", "Bearer leak-secret-99")],
        &["--port=0"],
    );
    assert!(!whitespace_tok.status.success());
    let ws_stderr = String::from_utf8_lossy(&whitespace_tok.stderr);
    assert!(
        !ws_stderr.contains("leak-secret-99"),
        "startup error must not echo invalid token secret: {ws_stderr}"
    );

    let embedded_hash_tok = run_server_cli(
        &[("JJ_CC_SERVER_AUTH_TOKEN", "K#leak-secret-hash-99")],
        &["--port=0"],
    );
    assert!(!embedded_hash_tok.status.success());
    let hash_stderr = String::from_utf8_lossy(&embedded_hash_tok.stderr);
    assert!(
        !hash_stderr.contains("leak-secret-hash-99"),
        "startup error must not echo embedded-# token secret: {hash_stderr}"
    );
}

struct TempTokenFileGuard(std::path::PathBuf);
impl Drop for TempTokenFileGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[tokio::test]
async fn test_server_host_localhost_and_ipv6_and_token_file_inline_comments() {
    let token_guard = TempTokenFileGuard(std::env::temp_dir().join(format!(
        "jj-cc-startup-tokens-{}-{}.txt",
        std::process::id(),
        uuid::Uuid::new_v4()
    )));
    std::fs::write(
        &token_guard.0,
        "\u{feff}# primary token\nfile-secret-1, # inline rotation note\n",
    )
    .expect("write token file");

    let file_arg = format!("--auth-token-file={}", token_guard.0.display());
    let server = testutils::spawn_server_with_args(&["--host=localhost", &file_arg]).await;
    drop(token_guard);
    let channel = tonic::transport::Endpoint::from_shared(server.url().to_string())
        .expect("valid endpoint")
        .connect()
        .await
        .expect("connect to localhost listener succeeded");

    let mut backend = BackendServiceClient::new(channel);
    for invalid_header in ["Bearer #", "Bearer # inline rotation note"] {
        let mut req = Request::new(RegisterRepositoryRequest { name: None });
        req.metadata_mut()
            .insert("authorization", invalid_header.parse().unwrap());
        let err = backend.register_repository(req).await.unwrap_err();
        assert_eq!(err.code(), Code::Unauthenticated, "{invalid_header}");
    }

    let mut ok_req = Request::new(RegisterRepositoryRequest { name: None });
    ok_req
        .metadata_mut()
        .insert("authorization", "Bearer file-secret-1".parse().unwrap());
    backend
        .register_repository(ok_req)
        .await
        .expect("file-secret-1 should succeed");

    if std::net::TcpListener::bind("[::1]:0").is_ok() {
        let v6_server = testutils::spawn_server_with_args(&["--host=::1"]).await;
        let v6_channel = tonic::transport::Endpoint::from_shared(v6_server.url().to_string())
            .expect("valid IPv6 endpoint")
            .connect()
            .await
            .expect("connect to ::1 listener succeeded");
        let mut health = HealthClient::new(v6_channel);
        let res = health
            .check(HealthCheckRequest {
                service: String::new(),
            })
            .await
            .expect("IPv6 health check should succeed");
        assert_eq!(
            res.into_inner().status(),
            tonic_health::pb::health_check_response::ServingStatus::Serving
        );
    }
}
