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

use cc_common::backend::backend_service_client::BackendServiceClient;
use cc_common::backend::{ReadFileRequest, RegisterRepositoryRequest, WriteFileRequest};
use cc_common::op_store::GetOpHeadsRequest;
use cc_common::op_store::op_store_service_client::OpStoreServiceClient;
use cc_common::workspace::ListWorkspacesRequest;
use cc_common::workspace::workspace_service_client::WorkspaceServiceClient;
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
        "commit_cloud.workspace.WorkspaceService",
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

    // Unauthenticated BackendService (unary and streaming), OpStoreService, and WorkspaceService calls are rejected.
    let mut backend = BackendServiceClient::new(channel.clone());
    let unauth_err = backend
        .register_repository(Request::new(RegisterRepositoryRequest {
            project_id: "auth-test-proj".to_string(),
            repo_id: None,
            name: None,
            hash_algorithm: 0,
        }))
        .await
        .expect_err("unauthenticated RegisterRepository must fail");
    assert_eq!(unauth_err.code(), Code::Unauthenticated);

    let read_file_unauth_err = backend
        .read_file(Request::new(ReadFileRequest {
            project_id: "auth-test-proj".to_string(),
            file_id: vec![0u8; 20],
            offset: 0,
            max_bytes: 0,
        }))
        .await
        .expect_err("unauthenticated streaming ReadFile must fail");
    assert_eq!(read_file_unauth_err.code(), Code::Unauthenticated);

    let mut workspace = WorkspaceServiceClient::new(channel.clone());
    let ws_unauth_err = workspace
        .list_workspaces(Request::new(ListWorkspacesRequest {
            repo_id: "any".to_string(),
            page_size: 0,
            page_token: String::new(),
        }))
        .await
        .expect_err("unauthenticated ListWorkspaces must fail");
    assert_eq!(ws_unauth_err.code(), Code::Unauthenticated);

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

    // Both active and rotated tokens succeed across all three services.
    let mut req1 = Request::new(RegisterRepositoryRequest {
        project_id: "auth-test-proj".to_string(),
        repo_id: None,
        name: None,
        hash_algorithm: 0,
    });
    req1.metadata_mut()
        .insert("authorization", "Bearer active-token-1".parse().unwrap());
    let repo_id = backend
        .register_repository(req1)
        .await
        .expect("primary token should succeed")
        .into_inner()
        .repo_id;

    let mut write_file_req = Request::new(WriteFileRequest {
        project_id: "auth-test-proj".to_string(),
        content: b"hello bearer auth".to_vec(),
    });
    write_file_req
        .metadata_mut()
        .insert("authorization", "Bearer active-token-1".parse().unwrap());
    let file_id = backend
        .write_file(write_file_req)
        .await
        .expect("authenticated WriteFile should succeed")
        .into_inner()
        .file_id;

    let read_existing_unauth_err = backend
        .read_file(Request::new(ReadFileRequest {
            project_id: "auth-test-proj".to_string(),
            file_id: file_id.clone(),
            offset: 0,
            max_bytes: 0,
        }))
        .await
        .expect_err("unauthenticated streaming ReadFile for an existing file must fail");
    assert_eq!(read_existing_unauth_err.code(), Code::Unauthenticated);

    let mut read_file_req = Request::new(ReadFileRequest {
        project_id: "auth-test-proj".to_string(),
        file_id,
        offset: 0,
        max_bytes: 0,
    });
    read_file_req
        .metadata_mut()
        .insert("authorization", "Bearer rotated-token-2".parse().unwrap());
    let mut stream = backend
        .read_file(read_file_req)
        .await
        .expect("authenticated streaming ReadFile should succeed")
        .into_inner();
    let chunk = stream
        .message()
        .await
        .expect("ReadFile stream should not error")
        .expect("ReadFile stream should yield a chunk")
        .chunk;
    assert_eq!(chunk, b"hello bearer auth");

    let mut sha256_req = Request::new(RegisterRepositoryRequest {
        project_id: String::new(),
        repo_id: None,
        name: None,
        hash_algorithm: cc_common::backend::HashAlgorithm::Sha256 as i32,
    });
    sha256_req
        .metadata_mut()
        .insert("authorization", "Bearer active-token-1".parse().unwrap());
    let sha256_err = backend
        .register_repository(sha256_req)
        .await
        .expect_err("SHA-256 repository registration must return UNIMPLEMENTED in M1");
    assert_eq!(sha256_err.code(), Code::Unimplemented);
    let mut req2 = Request::new(GetOpHeadsRequest {
        repo_id: repo_id.clone(),
    });
    req2.metadata_mut()
        .insert("authorization", "Bearer rotated-token-2".parse().unwrap());
    let heads = op_store
        .get_op_heads(req2)
        .await
        .expect("rotated token should succeed")
        .into_inner()
        .op_head_ids;
    assert_eq!(heads.len(), 1);

    let mut req3 = Request::new(ListWorkspacesRequest {
        repo_id,
        page_size: 0,
        page_token: String::new(),
    });
    req3.metadata_mut()
        .insert("authorization", "Bearer active-token-1".parse().unwrap());
    let ws_list = workspace
        .list_workspaces(req3)
        .await
        .expect("authenticated ListWorkspaces should succeed")
        .into_inner()
        .workspaces;
    assert!(ws_list.is_empty());
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
fn test_server_spanner_refuses_to_start_without_explicit_blob_store() {
    let db_arg = "--spanner-database=projects/p/instances/i/databases/d";

    let out = run_server_cli(&[], &["--port=0", "--store-type=spanner", db_arg]);
    assert!(
        !out.status.success(),
        "spanner without blob store must fail at startup"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("requires an explicit blob store"),
        "unexpected stderr: {stderr}"
    );

    // Empty env var placeholders must not bypass the durable blob store requirement.
    let out_empty_env = run_server_cli(
        &[("GCS_BUCKET", ""), ("BLOB_PATH", "")],
        &["--port=0", "--store-type=spanner", db_arg],
    );
    assert!(!out_empty_env.status.success());

    // `--spanner-emulator` / `SPANNER_EMULATOR` flag is advertised and accepted by the CLI.
    let help = run_server_cli(&[], &["--help"]);
    assert!(help.status.success());
    let help_stdout = String::from_utf8_lossy(&help.stdout);
    assert!(
        help_stdout.contains("--spanner-emulator") && help_stdout.contains("SPANNER_EMULATOR"),
        "--help missing --spanner-emulator / SPANNER_EMULATOR: {help_stdout}"
    );
    let out_emu = run_server_cli(
        &[],
        &[
            "--port=0",
            "--store-type=spanner",
            "--spanner-emulator",
            "--spanner-endpoint=127.0.0.1:9010",
            db_arg,
        ],
    );
    assert!(!out_emu.status.success());
    let emu_stderr = String::from_utf8_lossy(&out_emu.stderr);
    assert!(
        emu_stderr.contains("requires an explicit blob store"),
        "expected startup to parse --spanner-emulator and reach blob-store validation, got: {emu_stderr}"
    );
}

#[tokio::test]
async fn test_server_rejects_oversized_inline_blob_threshold_bytes() {
    let over_cli = run_server_cli(&[], &["--port=0", "--inline-blob-threshold-bytes=4194305"]);
    assert!(
        !over_cli.status.success(),
        "--inline-blob-threshold-bytes above 4 MiB must fail at startup"
    );
    let cli_stderr = String::from_utf8_lossy(&over_cli.stderr);
    assert!(
        cli_stderr.contains("exceeds maximum allowed value"),
        "unexpected stderr: {cli_stderr}"
    );

    let over_env = run_server_cli(
        &[("INLINE_BLOB_THRESHOLD_BYTES", "10485760")],
        &["--port=0"],
    );
    assert!(
        !over_env.status.success(),
        "INLINE_BLOB_THRESHOLD_BYTES above 4 MiB must fail at startup"
    );
    let env_stderr = String::from_utf8_lossy(&over_env.stderr);
    assert!(
        env_stderr.contains("exceeds maximum allowed value"),
        "unexpected stderr: {env_stderr}"
    );

    // Exact 4 MiB boundary is accepted and serves health checks.
    let server =
        testutils::spawn_server_with_args(&["--inline-blob-threshold-bytes=4194304"]).await;
    let channel = tonic::transport::Endpoint::from_shared(server.url().to_string())
        .expect("valid endpoint")
        .connect()
        .await
        .expect("connect to 4 MiB threshold server succeeded");
    let mut health = HealthClient::new(channel);
    let res = health
        .check(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect("health check should succeed");
    assert_eq!(
        res.into_inner().status(),
        tonic_health::pb::health_check_response::ServingStatus::Serving
    );
}

#[test]
fn test_server_gcs_and_peer_flags_fail_fast_before_opening_metadata_store() {
    let dir = tempfile::tempdir().unwrap();
    let sqlite_path = dir.path().join("must_not_be_created.db");
    let sqlite_arg = format!("--sqlite-path={}", sqlite_path.display());

    // --blob-store=gcs without --gcs-bucket (or with empty/whitespace GCS_BUCKET) fails fast
    // before touching SQLite or Spanner.
    for env_vars in [
        &[][..],
        &[("GCS_BUCKET", "")][..],
        &[("GCS_BUCKET", "   ")][..],
    ] {
        let out = run_server_cli(
            env_vars,
            &[
                "--port=0",
                "--store-type=sqlite",
                &sqlite_arg,
                "--blob-store=gcs",
            ],
        );
        assert!(
            !out.status.success(),
            "missing/empty --gcs-bucket with --blob-store=gcs must fail (env={env_vars:?})"
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("--gcs-bucket (or GCS_BUCKET) is required"),
            "unexpected stderr: {stderr}"
        );
        assert!(
            !sqlite_path.exists(),
            "SQLite DB should not be created when --gcs-bucket validation fails"
        );
    }

    // Spanner + --blob-store=gcs without --gcs-bucket also fails fast before connecting.
    let spanner_out = run_server_cli(
        &[],
        &[
            "--port=0",
            "--store-type=spanner",
            "--spanner-database=projects/p/instances/i/databases/d",
            "--blob-store=gcs",
        ],
    );
    assert!(!spanner_out.status.success());
    let spanner_stderr = String::from_utf8_lossy(&spanner_out.stderr);
    assert!(
        spanner_stderr.contains("--gcs-bucket (or GCS_BUCKET) is required"),
        "unexpected stderr: {spanner_stderr}"
    );

    // Malformed --gcs-peer-buckets entries (empty LHS or RHS around '=') are rejected.
    for bad_peer in [
        "=",
        "=http://127.0.0.1:4443",
        " =http://127.0.0.1:4443",
        "gs://peer=",
        "gs://peer=   ",
    ] {
        let arg = format!("--gcs-peer-buckets={bad_peer}");
        let out = run_server_cli(&[], &["--port=0", "--store-type=sqlite", &sqlite_arg, &arg]);
        assert!(
            !out.status.success(),
            "malformed --gcs-peer-buckets={bad_peer:?} must be rejected"
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("invalid --gcs-peer-buckets entry"),
            "unexpected stderr for {bad_peer:?}: {stderr}"
        );
        assert!(!sqlite_path.exists());
    }

    // Malformed --local-peer-blob-dirs entries (missing '=', empty LHS, or empty RHS) are rejected.
    for bad_peer in [
        "no-equals",
        "=",
        "=/tmp/dir",
        "  =/tmp/dir",
        "gs://peer=",
        "gs://peer=  ",
    ] {
        let arg = format!("--local-peer-blob-dirs={bad_peer}");
        let out = run_server_cli(&[], &["--port=0", "--store-type=sqlite", &sqlite_arg, &arg]);
        assert!(
            !out.status.success(),
            "malformed --local-peer-blob-dirs={bad_peer:?} must be rejected"
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("invalid --local-peer-blob-dirs entry"),
            "unexpected stderr for {bad_peer:?}: {stderr}"
        );
        assert!(!sqlite_path.exists());
    }

    // Invalid GCS bucket identifiers and endpoints fail fast before opening SQLite/Spanner.
    for bad_args in [
        vec!["--gcs-bucket=INVALID_UPPER"],
        vec!["--gcs-bucket=ab"],
        vec![
            "--gcs-bucket=valid-bucket",
            "--gcs-endpoint=http://remote.example:4443",
        ],
        vec![
            "--gcs-bucket=valid-bucket",
            "--gcs-endpoint=http://127.0.0.1:4443/prefix",
        ],
        vec!["--gcs-peer-buckets=INVALID_PEER"],
        vec!["--gcs-peer-buckets=gs://valid-peer=http://remote.example:4443"],
        vec!["--gcs-endpoint=http://remote.example:4443"],
        vec!["--gcs-bucket=valid-bucket\n"],
        vec!["--gcs-bucket=valid-bucket\r\n"],
        vec!["--gcs-bucket=\tvalid-bucket"],
        vec!["--gcs-peer-buckets=gs://valid-peer\r\n"],
        vec!["--gcs-peer-buckets=gs://valid-peer\n=http://127.0.0.1:4443"],
        vec!["--gcs-peer-buckets=\tgs://valid-peer=http://127.0.0.1:4443"],
    ] {
        let mut cli_args = vec!["--port=0", "--store-type=sqlite", &sqlite_arg];
        cli_args.extend(bad_args.iter().copied());
        let out = run_server_cli(&[], &cli_args);
        assert!(
            !out.status.success(),
            "invalid GCS flags {bad_args:?} must fail fast at startup"
        );
        assert!(
            !sqlite_path.exists(),
            "SQLite DB should not be created when GCS validation fails for {bad_args:?}"
        );
    }

    // Control-character-only or control-padded GCS_BUCKET / GCS_PEER_BUCKETS env vars must fail fast
    // rather than being silently trimmed as unset or accepted.
    for env_vars in [
        &[("GCS_BUCKET", "\r\n")][..],
        &[("GCS_BUCKET", "valid-bucket\n")][..],
        &[("GCS_BUCKET", "\tvalid-bucket")][..],
        &[("GCS_PEER_BUCKETS", "gs://valid-peer\r\n")][..],
        &[(
            "GCS_PEER_BUCKETS",
            "gs://valid-peer\n=http://127.0.0.1:4443",
        )][..],
    ] {
        let out = run_server_cli(env_vars, &["--port=0", "--store-type=sqlite", &sqlite_arg]);
        assert!(
            !out.status.success(),
            "control-char GCS env {env_vars:?} must fail fast at startup"
        );
        assert!(
            !sqlite_path.exists(),
            "SQLite DB should not be created when GCS env validation fails for {env_vars:?}"
        );
    }

    // --blob-store help text documents --gcs-bucket auto-detection and Spanner eligibility.
    let help = run_server_cli(&[], &["--help"]);
    assert!(help.status.success());
    let help_stdout = String::from_utf8_lossy(&help.stdout);
    let blob_store_section = help_stdout
        .split("--blob-store")
        .nth(1)
        .expect("--help should document --blob-store")
        .split("--inline-blob-threshold-bytes")
        .next()
        .unwrap_or("");
    assert!(
        blob_store_section.contains("--gcs-bucket"),
        "--blob-store help text should mention --gcs-bucket: {blob_store_section}"
    );
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

    let empty_file_env = run_server_cli(&[("JJ_CC_SERVER_AUTH_TOKEN_FILE", "")], &["--port=0"]);
    assert!(
        !empty_file_env.status.success(),
        "empty JJ_CC_SERVER_AUTH_TOKEN_FILE must be rejected"
    );

    let non_loopback = run_server_cli(&[], &["--host=0.0.0.0", "--port=0"]);
    assert!(
        !non_loopback.status.success(),
        "unauthenticated non-loopback bind must fail closed"
    );
    let non_loopback_stderr = String::from_utf8_lossy(&non_loopback.stderr);
    for expected in [
        "refusing to bind non-loopback",
        "--auth-token-file",
        "JJ_CC_SERVER_AUTH_TOKEN_FILE",
        "--auth-token",
        "JJ_CC_SERVER_AUTH_TOKEN",
        "--allow-unauthenticated",
        "JJ_CC_ALLOW_UNAUTHENTICATED=true",
    ] {
        assert!(
            non_loopback_stderr.contains(expected),
            "missing {expected:?} in stderr: {non_loopback_stderr}"
        );
    }

    let allow_unauth_false = run_server_cli(
        &[("JJ_CC_ALLOW_UNAUTHENTICATED", "false")],
        &["--host=0.0.0.0", "--port=0"],
    );
    assert!(
        !allow_unauth_false.status.success(),
        "JJ_CC_ALLOW_UNAUTHENTICATED=false on 0.0.0.0 must fail closed"
    );
    let allow_unauth_false_stderr = String::from_utf8_lossy(&allow_unauth_false.stderr);
    assert!(
        allow_unauth_false_stderr.contains("refusing to bind non-loopback"),
        "unexpected stderr: {allow_unauth_false_stderr}"
    );

    let allow_unauth_empty = run_server_cli(
        &[("JJ_CC_ALLOW_UNAUTHENTICATED", "")],
        &["--host=0.0.0.0", "--port=0"],
    );
    assert!(
        !allow_unauth_empty.status.success(),
        "empty JJ_CC_ALLOW_UNAUTHENTICATED on 0.0.0.0 must fail closed"
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
        let mut req = Request::new(RegisterRepositoryRequest {
            project_id: "auth-file-proj".to_string(),
            repo_id: None,
            name: None,
            hash_algorithm: 0,
        });
        req.metadata_mut()
            .insert("authorization", invalid_header.parse().unwrap());
        let err = backend.register_repository(req).await.unwrap_err();
        assert_eq!(err.code(), Code::Unauthenticated, "{invalid_header}");
    }

    let mut ok_req = Request::new(RegisterRepositoryRequest {
        project_id: "auth-file-proj".to_string(),
        repo_id: None,
        name: None,
        hash_algorithm: 0,
    });
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

#[tokio::test]
async fn test_server_env_token_file_and_allow_unauthenticated_non_loopback() {
    let token_guard = TempTokenFileGuard(std::env::temp_dir().join(format!(
        "jj-cc-env-token-file-{}-{}.txt",
        std::process::id(),
        uuid::Uuid::new_v4()
    )));
    std::fs::write(&token_guard.0, "# env token file\nenv-file-secret-1\n")
        .expect("write env token file");
    let token_path = token_guard.0.to_str().expect("valid UTF-8 temp path");

    let env_file_server =
        testutils::spawn_server_with_env(&[("JJ_CC_SERVER_AUTH_TOKEN_FILE", token_path)], &[])
            .await;
    drop(token_guard);

    let env_file_channel =
        tonic::transport::Endpoint::from_shared(env_file_server.url().to_string())
            .expect("valid endpoint")
            .connect()
            .await
            .expect("connect to env token file server succeeded");
    let mut env_file_backend = BackendServiceClient::new(env_file_channel);

    let unauth_err = env_file_backend
        .register_repository(Request::new(RegisterRepositoryRequest {
            project_id: "env-token-file-proj".to_string(),
            repo_id: None,
            name: None,
            hash_algorithm: 0,
        }))
        .await
        .expect_err("unauthenticated request must fail when JJ_CC_SERVER_AUTH_TOKEN_FILE is set");
    assert_eq!(unauth_err.code(), Code::Unauthenticated);

    let mut authed_req = Request::new(RegisterRepositoryRequest {
        project_id: "env-token-file-proj".to_string(),
        repo_id: None,
        name: None,
        hash_algorithm: 0,
    });
    authed_req
        .metadata_mut()
        .insert("authorization", "Bearer env-file-secret-1".parse().unwrap());
    env_file_backend
        .register_repository(authed_req)
        .await
        .expect("token loaded via JJ_CC_SERVER_AUTH_TOKEN_FILE should succeed");

    let unauth_remote = testutils::spawn_server_with_env(
        &[("JJ_CC_ALLOW_UNAUTHENTICATED", "true")],
        &["--host=0.0.0.0"],
    )
    .await;
    assert!(
        unauth_remote.url().starts_with("http://0.0.0.0:"),
        "expected 0.0.0.0 bind address, got {}",
        unauth_remote.url()
    );
    let connect_url = unauth_remote
        .url()
        .replacen("http://0.0.0.0:", "http://127.0.0.1:", 1);
    let remote_channel = tonic::transport::Endpoint::from_shared(connect_url)
        .expect("valid loopback endpoint for 0.0.0.0 listener")
        .connect()
        .await
        .expect("connect to 0.0.0.0 listener via 127.0.0.1 succeeded");

    let mut health = HealthClient::new(remote_channel.clone());
    let health_res = health
        .check(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect("health check on 0.0.0.0 listener should succeed");
    assert_eq!(
        health_res.into_inner().status(),
        tonic_health::pb::health_check_response::ServingStatus::Serving
    );

    let mut remote_backend = BackendServiceClient::new(remote_channel);
    remote_backend
        .register_repository(Request::new(RegisterRepositoryRequest {
            project_id: "unauth-remote-proj".to_string(),
            repo_id: None,
            name: None,
            hash_algorithm: 0,
        }))
        .await
        .expect("unauthenticated RPC should succeed when JJ_CC_ALLOW_UNAUTHENTICATED=true");
}
