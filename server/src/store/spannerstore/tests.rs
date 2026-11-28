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

use super::codec::*;
use super::repo::*;
use super::schema::*;
use super::*;

use crate::store::StoreError;
use google_cloud_gax::error::rpc::{Code as RpcCode, Status as RpcStatus};
use google_cloud_spanner::key;
use google_cloud_spanner::key::KeySet;
use google_cloud_spanner::mutation::Mutation;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

fn service_error(code: RpcCode) -> google_cloud_spanner::Error {
    google_cloud_gax::error::Error::service(
        RpcStatus::default().set_code(code).set_message("injected"),
    )
}

#[test]
fn spanner_errors_map_to_retryable_and_specific_codes() {
    let cases = [
        (RpcCode::Aborted, tonic::Code::Unavailable),
        (RpcCode::Unavailable, tonic::Code::Unavailable),
        (RpcCode::DeadlineExceeded, tonic::Code::Unavailable),
        (RpcCode::ResourceExhausted, tonic::Code::ResourceExhausted),
        (RpcCode::PermissionDenied, tonic::Code::PermissionDenied),
        (RpcCode::Unauthenticated, tonic::Code::Unauthenticated),
        (RpcCode::InvalidArgument, tonic::Code::InvalidArgument),
        (RpcCode::Internal, tonic::Code::Internal),
    ];
    for (spanner_code, grpc_code) in cases {
        let status = tonic::Status::from(map_spanner_err(service_error(spanner_code)));
        assert_eq!(
            status.code(),
            grpc_code,
            "Spanner {spanner_code:?}: {status:?}"
        );
    }
}

#[test]
fn finished_admin_op_surfaces_operation_errors() {
    assert!(finished_admin_op(false, None).is_none());
    assert!(matches!(finished_admin_op(true, None), Some(Ok(()))));

    let failed = RpcStatus::default()
        .set_code(RpcCode::FailedPrecondition)
        .set_message("Duplicate name in schema: Commits.");
    let outcome = finished_admin_op(true, Some(failed)).expect("finished operation");
    let err = outcome.expect_err("DDL failure must not report success");
    assert!(
        err.to_string().contains("Duplicate name in schema"),
        "{err}"
    );

    assert!(check_emulator_lro_json("HTTP/1.0 200 OK\r\n\r\n{\"done\":true}").is_ok());
    assert!(check_emulator_lro_json("HTTP/1.0 200 OK\n\n{\"done\":true}").is_ok());
    let emu_failed =
        "HTTP/1.0 200 OK\r\n\r\n{\"done\":true,\"error\":{\"code\":9,\"message\":\"bad ddl\"}}";
    assert!(check_emulator_lro_json(emu_failed).is_err());
    assert!(
        check_emulator_lro_json("HTTP/1.0 200 OK\r\n\r\n{\"done\":false}").is_err(),
        "incomplete LRO must fail"
    );
    assert!(
        check_emulator_lro_json("HTTP/1.0 200 OK\r\n\r\n{}").is_err(),
        "missing done:true must fail"
    );
    assert!(
        check_emulator_lro_json("HTTP/1.0 200 OK\r\n\r\nnot json").is_err(),
        "non-JSON body must fail"
    );
    assert!(
        check_emulator_lro_json("HTTP/1.0 200 OK\r\n\r\n").is_err(),
        "empty body must fail"
    );
}

#[test]
fn parse_ddl_statements_strips_comments_and_parses_schema() {
    let stmts = parse_ddl_statements(
        "-- leading comment\nCREATE TABLE A(Id INT64) PRIMARY KEY(Id);\n\n  -- inline comment\nCREATE TABLE B(Id INT64) PRIMARY KEY(Id);\n  ;\n",
    );
    assert_eq!(
        stmts,
        vec![
            "CREATE TABLE A(Id INT64) PRIMARY KEY(Id)",
            "CREATE TABLE B(Id INT64) PRIMARY KEY(Id)",
        ]
    );

    let schema_stmts = parse_ddl_statements(SPANNER_SCHEMA);
    assert!(!schema_stmts.is_empty());
    assert!(
        schema_stmts
            .iter()
            .any(|s| s.contains("CREATE TABLE IF NOT EXISTS Operations")
                && s.contains("Published BOOL")),
        "Operations table must declare nullable Published BOOL"
    );
    // Selective interleaving: only TreeEntries (in Trees) and Workspaces (in Repositories)
    // are interleaved; unbounded object/history tables and secondary indexes remain top-level.
    let interleaved: Vec<&str> = schema_stmts
        .iter()
        .filter(|s| s.contains("INTERLEAVE"))
        .map(String::as_str)
        .collect();
    assert_eq!(
        interleaved.len(),
        2,
        "expected exactly 2 interleaved DDL statements (TreeEntries, Workspaces), got: {interleaved:#?}"
    );
    assert!(
        interleaved
            .iter()
            .any(|s| s.contains("CREATE TABLE IF NOT EXISTS TreeEntries")
                && s.contains("INTERLEAVE IN PARENT Trees")),
        "TreeEntries must interleave in Trees"
    );
    assert!(
        interleaved
            .iter()
            .any(|s| s.contains("CREATE TABLE IF NOT EXISTS Workspaces")
                && s.contains("INTERLEAVE IN PARENT Repositories")),
        "Workspaces must interleave in Repositories"
    );
}

#[test]
fn split_database_resource_validates_paths() {
    assert_eq!(
        split_database_resource("projects/p1/instances/i1/databases/d1").unwrap(),
        ("projects/p1/instances/i1", "d1")
    );
    assert_eq!(
        split_database_resource("projects/my-proj_1/instances/inst-2/databases/db_3").unwrap(),
        ("projects/my-proj_1/instances/inst-2", "db_3")
    );
    for bad in [
        "",
        "projects/p1/instances/i1",
        "projects/p1/instances/i1/databases/",
        "projects/p1/instances/i1/databases/d1/",
        "/projects/p1/instances/i1/databases/d1",
        "projects/p1/instances/i1/databases/d1/extra",
        "projects//instances/i1/databases/d1",
        "projects/p1/instances//databases/d1",
        "bad/p1/instances/i1/databases/d1",
        "projects/p1/databases/d1",
        "projects/../instances/i1/databases/d1",
        "projects/p1/instances/../databases/d1",
        "projects/p1/instances/i1/databases/..",
        "projects/p1/instances/i1/databases/d1?alt=json",
        "projects/p1/instances/i1/databases/d1#frag",
        "projects/p1?x=1/instances/i1/databases/d1",
        "projects/p1/instances/i1#frag/databases/d1",
        "projects/p1/instances/i1/databases/`d1`",
        "projects/p1/instances/i1/databases/d 1",
        "projects/p1/instances/i1/databases/d\n1",
    ] {
        assert!(
            matches!(
                split_database_resource(bad),
                Err(StoreError::InvalidArgument(_))
            ),
            "expected InvalidArgument for {bad:?}"
        );
    }
}

#[test]
fn normalize_endpoint_respects_tls_default_and_strips_trailing_slashes() {
    // Explicit emulator mode (`default_https = false`) uses http:// for schemeless hosts.
    assert_eq!(
        normalize_endpoint("127.0.0.1:9010", false).unwrap(),
        "http://127.0.0.1:9010"
    );
    assert_eq!(
        normalize_endpoint("  127.0.0.1:9010/// ", false).unwrap(),
        "http://127.0.0.1:9010"
    );
    assert_eq!(
        normalize_endpoint("http://localhost:9020/", false).unwrap(),
        "http://localhost:9020"
    );

    // TLS / Omni / production mode (`default_https = true`) never downgrades schemeless
    // endpoints to plaintext http://.
    assert_eq!(
        normalize_endpoint("spanner-omni.internal:443", true).unwrap(),
        "https://spanner-omni.internal:443"
    );
    assert_eq!(
        normalize_endpoint("  spanner.googleapis.com/// ", true).unwrap(),
        "https://spanner.googleapis.com"
    );
    assert_eq!(
        normalize_endpoint("https://spanner.googleapis.com///", true).unwrap(),
        "https://spanner.googleapis.com"
    );

    // Empty or unsupported schemes fail with InvalidArgument.
    for bad in ["", "   ", "///", "ftp://spanner.example:443"] {
        assert!(
            matches!(
                normalize_endpoint(bad, true),
                Err(StoreError::InvalidArgument(_))
            ),
            "expected InvalidArgument for {bad:?}"
        );
    }
}

#[test]
fn emulator_rest_derivation_and_admin_gating() {
    // Trailing slash on :9010 must still rewrite to :9020.
    assert_eq!(
        derive_emulator_rest_endpoint("http://127.0.0.1:9010/").unwrap(),
        "http://127.0.0.1:9020"
    );
    assert_eq!(
        derive_emulator_rest_endpoint("127.0.0.1:9010///").unwrap(),
        "http://127.0.0.1:9020"
    );
    assert_eq!(
        derive_emulator_rest_endpoint("http://127.0.0.1:9025/").unwrap(),
        "http://127.0.0.1:9025"
    );

    let base_cfg = SpannerConnectConfig {
        database: "projects/p1/instances/i1/databases/d1".to_string(),
        ..Default::default()
    };

    // Explicit emulator env routes to EmulatorHttp10 with rewritten :9020 port.
    assert_eq!(
        resolve_admin_endpoint(&base_cfg, Some("http://127.0.0.1:9010/"), None).unwrap(),
        AdminEndpointTarget::EmulatorHttp10 {
            host_port: "127.0.0.1:9020".to_string()
        }
    );
    assert_eq!(
        resolve_admin_endpoint(&base_cfg, None, Some("127.0.0.1:9020/")).unwrap(),
        AdminEndpointTarget::EmulatorHttp10 {
            host_port: "127.0.0.1:9020".to_string()
        }
    );

    // Non-emulator plaintext http:// admin_endpoint is rejected instead of invoking
    // unauthenticated emulator HTTP/1.0 admin requests.
    let mut http_admin_cfg = base_cfg.clone();
    http_admin_cfg.admin_endpoint = Some("http://internal-admin:8080".to_string());
    assert!(matches!(
        resolve_admin_endpoint(&http_admin_cfg, None, None),
        Err(StoreError::InvalidArgument(_))
    ));

    // Omni / TLS mode also rejects plaintext http:// admin_endpoint even if emulator env
    // is set in the process environment.
    let mut omni_http_cfg = http_admin_cfg.clone();
    omni_http_cfg.omni = true;
    assert!(matches!(
        resolve_admin_endpoint(&omni_http_cfg, Some("127.0.0.1:9010"), None),
        Err(StoreError::InvalidArgument(_))
    ));

    // Explicit `config.emulator = true` enables EmulatorHttp10 without process-global env vars.
    let mut explicit_emu_cfg = base_cfg.clone();
    explicit_emu_cfg.emulator = true;
    explicit_emu_cfg.admin_endpoint = Some("http://127.0.0.1:19020/".to_string());
    assert_eq!(
        resolve_admin_endpoint(&explicit_emu_cfg, None, None).unwrap(),
        AdminEndpointTarget::EmulatorHttp10 {
            host_port: "127.0.0.1:19020".to_string()
        }
    );

    // `config.emulator = true` with `config.endpoint` and `admin_endpoint = None` falls back
    // to deriving the REST admin port from `config.endpoint`.
    let mut endpoint_fallback_cfg = base_cfg.clone();
    endpoint_fallback_cfg.emulator = true;
    endpoint_fallback_cfg.endpoint = Some("127.0.0.1:9010/".to_string());
    assert_eq!(
        resolve_admin_endpoint(&endpoint_fallback_cfg, None, None).unwrap(),
        AdminEndpointTarget::EmulatorHttp10 {
            host_port: "127.0.0.1:9020".to_string()
        }
    );

    // `config.emulator = true` with an https:// endpoint is rejected for emulator REST derivation.
    let mut https_emu_ep_cfg = base_cfg.clone();
    https_emu_ep_cfg.emulator = true;
    https_emu_ep_cfg.endpoint = Some("https://spanner.example:9010".to_string());
    assert!(matches!(
        resolve_admin_endpoint(&https_emu_ep_cfg, None, None),
        Err(StoreError::InvalidArgument(_))
    ));

    // `config.emulator = true` without any admin, env, or endpoint falls back to default DatabaseAdmin.
    let mut empty_emu_cfg = base_cfg.clone();
    empty_emu_cfg.emulator = true;
    empty_emu_cfg.endpoint = Some("   ".to_string());
    assert_eq!(
        resolve_admin_endpoint(&empty_emu_cfg, None, None).unwrap(),
        AdminEndpointTarget::DatabaseAdmin { endpoint: None }
    );

    // Omni / TLS mode still rejects plaintext http:// even when `config.emulator = true`.
    let mut omni_explicit_emu_cfg = explicit_emu_cfg;
    omni_explicit_emu_cfg.omni = true;
    assert!(matches!(
        resolve_admin_endpoint(&omni_explicit_emu_cfg, None, None),
        Err(StoreError::InvalidArgument(_))
    ));

    // Non-emulator schemeless admin_endpoint defaults to https:// on DatabaseAdmin.
    let mut https_admin_cfg = base_cfg;
    https_admin_cfg.admin_endpoint = Some("spanner-admin.internal:443/".to_string());
    assert_eq!(
        resolve_admin_endpoint(&https_admin_cfg, None, None).unwrap(),
        AdminEndpointTarget::DatabaseAdmin {
            endpoint: Some("https://spanner-admin.internal:443".to_string())
        }
    );
}

#[test]
fn data_endpoint_resolution_and_plaintext_gating() {
    let base_cfg = SpannerConnectConfig {
        database: "projects/p1/instances/i1/databases/d1".to_string(),
        ..Default::default()
    };

    // Omitted or blank endpoint returns None.
    assert_eq!(resolve_data_endpoint(&base_cfg, None).unwrap(), None);
    let mut blank_cfg = base_cfg.clone();
    blank_cfg.endpoint = Some("   ".to_string());
    assert_eq!(resolve_data_endpoint(&blank_cfg, None).unwrap(), None);

    // Explicit emulator flag permits schemeless and http:// endpoints.
    let mut emu_cfg = base_cfg.clone();
    emu_cfg.emulator = true;
    emu_cfg.endpoint = Some("127.0.0.1:9010/".to_string());
    assert_eq!(
        resolve_data_endpoint(&emu_cfg, None).unwrap(),
        Some("http://127.0.0.1:9010".to_string())
    );
    emu_cfg.endpoint = Some("http://127.0.0.1:9010/".to_string());
    assert_eq!(
        resolve_data_endpoint(&emu_cfg, None).unwrap(),
        Some("http://127.0.0.1:9010".to_string())
    );

    // SPANNER_EMULATOR_HOST env permits plaintext http:// when not in Omni/TLS mode.
    let mut http_cfg = base_cfg.clone();
    http_cfg.endpoint = Some("http://127.0.0.1:9010/".to_string());
    assert_eq!(
        resolve_data_endpoint(&http_cfg, Some("127.0.0.1:9010")).unwrap(),
        Some("http://127.0.0.1:9010".to_string())
    );

    // Non-emulator plaintext http:// endpoint is rejected.
    assert!(matches!(
        resolve_data_endpoint(&http_cfg, None),
        Err(StoreError::InvalidArgument(_))
    ));
    assert!(matches!(
        resolve_data_endpoint(&http_cfg, Some("   ")),
        Err(StoreError::InvalidArgument(_))
    ));

    // Spanner Omni rejects plaintext http:// even without custom certs or when emulator flags/env
    // are also set.
    let mut omni_http_cfg = http_cfg.clone();
    omni_http_cfg.omni = true;
    assert!(matches!(
        resolve_data_endpoint(&omni_http_cfg, None),
        Err(StoreError::InvalidArgument(_))
    ));
    assert!(matches!(
        resolve_data_endpoint(&omni_http_cfg, Some("127.0.0.1:9010")),
        Err(StoreError::InvalidArgument(_))
    ));
    omni_http_cfg.emulator = true;
    assert!(matches!(
        resolve_data_endpoint(&omni_http_cfg, None),
        Err(StoreError::InvalidArgument(_))
    ));

    // Custom TLS certs also reject plaintext http:// even when config.omni is false.
    let mut tls_http_cfg = http_cfg;
    tls_http_cfg.emulator = true;
    tls_http_cfg.omni_ca_cert = Some(std::path::PathBuf::from("/tmp/ca.pem"));
    assert!(matches!(
        resolve_data_endpoint(&tls_http_cfg, Some("127.0.0.1:9010")),
        Err(StoreError::InvalidArgument(_))
    ));

    // Non-emulator and Omni schemeless endpoints default to https://.
    let mut prod_cfg = base_cfg.clone();
    prod_cfg.endpoint = Some("spanner.googleapis.com///".to_string());
    assert_eq!(
        resolve_data_endpoint(&prod_cfg, None).unwrap(),
        Some("https://spanner.googleapis.com".to_string())
    );
    let mut omni_cfg = base_cfg;
    omni_cfg.omni = true;
    omni_cfg.endpoint = Some("spanner-omni.internal:443/".to_string());
    assert_eq!(
        resolve_data_endpoint(&omni_cfg, Some("127.0.0.1:9010")).unwrap(),
        Some("https://spanner-omni.internal:443".to_string())
    );
}

#[test]
fn spanner_watermark_encode_decode_roundtrip_and_rejects_malformed() {
    let ts = "2026-11-27T07:00:00.123456789Z";
    let cid = vec![0xde; cc_common::COMMIT_ID_LENGTH];
    let encoded = encode_spanner_watermark(ts, &cid);
    assert_eq!(
        decode_spanner_watermark(&encoded).unwrap(),
        (ts.to_string(), cid)
    );

    let epoch = encode_spanner_watermark(EPOCH_RFC3339_NANOS, &[]);
    assert_eq!(
        decode_spanner_watermark(&epoch).unwrap(),
        (EPOCH_RFC3339_NANOS.to_string(), Vec::new())
    );

    // Whole-second, shorter-fractional, and leap-day UTC timestamps are valid.
    for valid_ts in [
        "2026-11-27T07:00:00Z",
        "2026-11-27T07:00:00.5Z",
        "2024-02-29T23:59:59.999999999Z",
        "2000-02-29T00:00:00Z",
    ] {
        let tok = encode_spanner_watermark(valid_ts, &[]);
        assert_eq!(
            decode_spanner_watermark(&tok).unwrap(),
            (valid_ts.to_string(), Vec::new())
        );
    }

    for bad in [
        vec![],
        vec![0, 1, 2],
        vec![0, 0, 0, 0],              // empty timestamp
        vec![0, 0, 0, 10, b'a', b'b'], // truncated length
        vec![0, 0, 0, 2, 0xff, 0xfe],  // invalid UTF-8
        encode_spanner_watermark("not-a-timestamp", &[]),
        encode_spanner_watermark("2026-11-27 07:00:00Z", &[]), // space instead of T
        encode_spanner_watermark("2026-11-27T07:00:00+00:00", &[]), // offset instead of Z
        encode_spanner_watermark("2026-11-27T07:00:00.Z", &[]), // empty fractional digits
        encode_spanner_watermark("2026-11-27T07:00:00.1234567890Z", &[]), // >9 frac digits
        encode_spanner_watermark("0000-01-01T00:00:00Z", &[]), // year 0000
        encode_spanner_watermark("2026-13-01T00:00:00Z", &[]), // month 13
        encode_spanner_watermark("2026-02-29T00:00:00Z", &[]), // non-leap year Feb 29
        encode_spanner_watermark("1900-02-29T00:00:00Z", &[]), // century non-leap Feb 29
        encode_spanner_watermark("2026-04-31T00:00:00Z", &[]), // Apr 31
        encode_spanner_watermark("2026-01-01T24:00:00Z", &[]), // hour 24
        encode_spanner_watermark("2026-01-01T00:60:00Z", &[]), // minute 60
        encode_spanner_watermark("2026-01-01T00:00:60Z", &[]), // leap second 60
        encode_spanner_watermark(ts, &[0xde, 0xad]),           // short after_id
        encode_spanner_watermark(ts, &[0xde; 19]),             // 19-byte after_id
        encode_spanner_watermark(ts, &[0xde; 21]),             // 21-byte after_id
    ] {
        assert!(
            matches!(
                decode_spanner_watermark(&bad),
                Err(StoreError::InvalidArgument(_))
            ),
            "expected InvalidArgument for {bad:?}"
        );
    }
}

#[test]
fn validate_existing_spanner_ddl_accepts_current_and_rejects_pre_split_poc() {
    // Empty database and current schema pass.
    validate_existing_spanner_ddl(&[]).unwrap();
    let current_stmts = parse_ddl_statements(SPANNER_SCHEMA);
    validate_existing_spanner_ddl(&current_stmts).unwrap();

    // Spanner-normalized DDL (without IF NOT EXISTS and with backticks) also passes.
    let normalized = [
        "CREATE TABLE `Projects` (ProjectId STRING(128) NOT NULL, Name STRING(256)) PRIMARY KEY(ProjectId)".to_string(),
        "CREATE TABLE Commits (ProjectId STRING(128) NOT NULL, CommitId BYTES(64) NOT NULL, ChangeId BYTES(64) NOT NULL, Generation INT64 NOT NULL, ProtoBytes BYTES(MAX) NOT NULL, InsertedAt TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY(ProjectId, CommitId)".to_string(),
        "CREATE TABLE Trees (ProjectId STRING(128) NOT NULL, TreeId BYTES(64) NOT NULL, Ready BOOL NOT NULL) PRIMARY KEY(ProjectId, TreeId)".to_string(),
    ];
    validate_existing_spanner_ddl(&normalized).unwrap();

    // Pre-split PoC markers must fail fast with InvalidArgument.
    let legacy_cases = [
        (
            "legacy repos table",
            [
                "CREATE TABLE repos (repo_id STRING(MAX) NOT NULL, project_id STRING(MAX) NOT NULL) PRIMARY KEY(repo_id)".to_string(),
            ],
        ),
        (
            "legacy commits without ProtoBytes/Generation",
            [
                "CREATE TABLE commits (project_id STRING(MAX) NOT NULL, commit_id BYTES(MAX) NOT NULL, data BYTES(MAX) NOT NULL) PRIMARY KEY(project_id, commit_id)".to_string(),
            ],
        ),
        (
            "legacy trees without Ready",
            [
                "CREATE TABLE trees (project_id STRING(MAX) NOT NULL, tree_id BYTES(MAX) NOT NULL, data BYTES(MAX) NOT NULL) PRIMARY KEY(project_id, tree_id)".to_string(),
            ],
        ),
        (
            "legacy operations without ProtoBytes",
            [
                "CREATE TABLE operations (repo_id STRING(MAX) NOT NULL, op_id BYTES(MAX) NOT NULL, data BYTES(MAX) NOT NULL) PRIMARY KEY(repo_id, op_id)".to_string(),
            ],
        ),
        (
            "legacy workspaces with user instead of UserEmail",
            [
                "CREATE TABLE workspaces (repo_id STRING(MAX) NOT NULL, user STRING(MAX) NOT NULL, workspace_name STRING(MAX) NOT NULL) PRIMARY KEY(repo_id, user, workspace_name)".to_string(),
            ],
        ),
    ];
    for (label, stmts) in legacy_cases {
        let err = validate_existing_spanner_ddl(&stmts).unwrap_err();
        assert!(
            matches!(err, StoreError::InvalidArgument(_)),
            "{label}: expected InvalidArgument, got {err:?}"
        );
    }

    // Emulator GET /ddl JSON parser handles empty object and statements list.
    assert!(
        parse_emulator_ddl_statements("HTTP/1.0 200 OK\r\n\r\n{}")
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        parse_emulator_ddl_statements(
            "HTTP/1.0 200 OK\r\n\r\n{\"statements\":[\"CREATE TABLE A (Id INT64) PRIMARY KEY(Id)\"]}"
        )
        .unwrap(),
        vec!["CREATE TABLE A (Id INT64) PRIMARY KEY(Id)".to_string()]
    );
}

#[test]
fn tx_retry_tracker_counts_retries_only_after_initial_attempt() {
    let shared = Arc::new(AtomicU64::new(0));
    let tracker = TxRetryTracker {
        attempts: AtomicUsize::new(0),
        counter: Some(Arc::clone(&shared)),
    };

    tracker.record_attempt();
    assert_eq!(shared.load(Ordering::Relaxed), 0);

    tracker.record_attempt();
    assert_eq!(shared.load(Ordering::Relaxed), 1);

    tracker.record_attempt();
    assert_eq!(shared.load(Ordering::Relaxed), 2);

    let no_counter = TxRetryTracker {
        attempts: AtomicUsize::new(0),
        counter: None,
    };
    no_counter.record_attempt();
    no_counter.record_attempt();
    assert_eq!(no_counter.attempts.load(Ordering::Relaxed), 2);
}

#[test]
fn delete_batch_helpers_clamp_limits_evaluate_project_binding_and_build_key_sets() {
    assert_eq!(clamp_delete_batch_limit(-10), 1);
    assert_eq!(clamp_delete_batch_limit(0), 1);
    assert_eq!(clamp_delete_batch_limit(2), 2);
    assert_eq!(
        clamp_delete_batch_limit(SPANNER_DELETE_BATCH_LIMIT),
        SPANNER_DELETE_BATCH_LIMIT
    );
    assert_eq!(
        clamp_delete_batch_limit(SPANNER_DELETE_BATCH_LIMIT + 10_000),
        SPANNER_DELETE_BATCH_LIMIT
    );

    assert_eq!(clamp_max_delete_batch_iterations(0), 1);
    assert_eq!(clamp_max_delete_batch_iterations(5), 5);
    assert_eq!(
        clamp_max_delete_batch_iterations(MAX_DELETE_BATCH_ITERATIONS + 500),
        MAX_DELETE_BATCH_ITERATIONS
    );

    // Absent repo returns RepoGone for both conditional and unconditional deletes.
    assert_eq!(
        evaluate_delete_repo_project("r", None, None, None).unwrap(),
        DeleteRepoProjectCheck::RepoGone
    );
    assert_eq!(
        evaluate_delete_repo_project("r", None, Some("proj-a"), Some("proj-a")).unwrap(),
        DeleteRepoProjectCheck::RepoGone
    );

    // Explicit expected_project_id matches or fails with FailedPrecondition.
    assert_eq!(
        evaluate_delete_repo_project("r", Some("proj-a"), Some("proj-a"), None).unwrap(),
        DeleteRepoProjectCheck::Proceed("proj-a")
    );
    assert!(matches!(
        evaluate_delete_repo_project("r", Some("proj-b"), Some("proj-a"), None),
        Err(StoreError::FailedPrecondition(_))
    ));
    assert!(matches!(
        evaluate_delete_repo_project("r", Some("proj-b"), Some("proj-a"), Some("proj-a")),
        Err(StoreError::FailedPrecondition(_))
    ));

    // Unconditional delete proceeds on batch 0 and matching pinned batches, and returns
    // RepoGone (Ok(false)) instead of FailedPrecondition if replaced under another project.
    assert_eq!(
        evaluate_delete_repo_project("r", Some("proj-a"), None, None).unwrap(),
        DeleteRepoProjectCheck::Proceed("proj-a")
    );
    assert_eq!(
        evaluate_delete_repo_project("r", Some("proj-a"), None, Some("proj-a")).unwrap(),
        DeleteRepoProjectCheck::Proceed("proj-a")
    );
    assert_eq!(
        evaluate_delete_repo_project("r", Some("proj-b"), None, Some("proj-a")).unwrap(),
        DeleteRepoProjectCheck::RepoGone
    );

    let ws_mut = build_workspace_delete_mutation(
        "repo-del",
        &[
            ("alice@example.com".to_string(), "ws-1".to_string()),
            ("bob@example.com".to_string(), "ws-2".to_string()),
        ],
    );
    let expected_ws_keys = KeySet::builder()
        .add_key(key!["repo-del", "alice@example.com", "ws-1"])
        .add_key(key!["repo-del", "bob@example.com", "ws-2"])
        .build();
    assert_eq!(ws_mut, Mutation::delete("Workspaces", expected_ws_keys));

    let op_ids = vec![vec![0x01; 16], vec![0x02; 16]];
    let op_mut = build_binary_child_delete_mutation("Operations", "repo-del", &op_ids);
    let expected_op_keys = KeySet::builder()
        .add_key(key!["repo-del", op_ids[0].as_slice()])
        .add_key(key!["repo-del", op_ids[1].as_slice()])
        .build();
    assert_eq!(op_mut, Mutation::delete("Operations", expected_op_keys));

    let view_ids = vec![vec![0xaa; 16]];
    let view_mut = build_binary_child_delete_mutation("Views", "repo-del", &view_ids);
    let expected_view_keys = KeySet::builder()
        .add_key(key!["repo-del", view_ids[0].as_slice()])
        .build();
    assert_eq!(view_mut, Mutation::delete("Views", expected_view_keys));
}
