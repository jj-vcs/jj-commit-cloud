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

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use cc_common::backend::backend_service_client::BackendServiceClient;
use cc_common::backend::backend_service_server::BackendServiceServer;
use cc_common::backend::*;
use cc_common::op_store::op_store_service_client::OpStoreServiceClient;
use cc_common::op_store::op_store_service_server::OpStoreServiceServer;
use cc_common::op_store::*;
use cc_common::workspace::workspace_service_client::WorkspaceServiceClient;
use cc_common::workspace::workspace_service_server::WorkspaceServiceServer;
use cc_common::workspace::*;
use jj_commit_cloud_server::backend::CommitCloudBackendService;
use jj_commit_cloud_server::metrics::{
    MetricsLayer, ServerMetrics, StatzSnapshot, bind_and_serve_metrics,
};
use jj_commit_cloud_server::op_store::CommitCloudOpStoreService;
use jj_commit_cloud_server::store::{
    CompositeStore, MemoryBlobStore, MemoryMetadataStore, MetadataStore, ReplicatedBlobStore,
    ReplicatedBlobStoreConfig,
};
use jj_commit_cloud_server::workspace::CommitCloudWorkspaceService;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tonic::{Code, Request};
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_client::HealthClient;

async fn http_get(addr: SocketAddr, path: &str) -> (u16, HashMap<String, String>, String) {
    let mut stream = TcpStream::connect(addr)
        .await
        .expect("connect to metrics HTTP server");
    let req = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    let text = String::from_utf8(raw).expect("HTTP response should be valid UTF-8");
    let (head, body) = text
        .split_once("\r\n\r\n")
        .expect("HTTP response must contain header/body separator");
    let mut lines = head.lines();
    let status_line = lines.next().expect("status line");
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("numeric status code");
    let mut headers = HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    (status, headers, body.to_string())
}

fn sample_commit(desc: &str, change_byte: u8) -> Commit {
    Commit {
        commit_id: Vec::new(),
        change_id: vec![change_byte; cc_common::CHANGE_ID_LENGTH],
        parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
        description: desc.to_string(),
        author: Some(Signature {
            name: "Alice".to_string(),
            email: "alice@example.com".to_string(),
            timestamp: Some(Timestamp {
                millis_since_epoch: 1_730_000_000_000,
                tz_offset: 0,
            }),
        }),
        committer: Some(Signature {
            name: "Alice".to_string(),
            email: "alice@example.com".to_string(),
            timestamp: Some(Timestamp {
                millis_since_epoch: 1_730_000_000_000,
                tz_offset: 0,
            }),
        }),
        ..Default::default()
    }
}

fn parse_prometheus_samples(body: &str) -> HashMap<String, f64> {
    assert!(
        body.ends_with('\n'),
        "Prometheus exposition payload must end with a newline"
    );
    let mut samples = HashMap::new();
    for line in body.lines() {
        assert!(!line.is_empty(), "unexpected blank line in Prometheus body");
        if line.starts_with('#') {
            assert!(
                line.starts_with("# HELP ") || line.starts_with("# TYPE "),
                "unexpected comment line: {line}"
            );
            continue;
        }
        let (metric, value_str) = line
            .split_once(' ')
            .unwrap_or_else(|| panic!("malformed Prometheus sample line: {line}"));
        let val: f64 = value_str
            .parse()
            .unwrap_or_else(|e| panic!("non-numeric Prometheus value in {line:?}: {e}"));
        assert!(val.is_finite(), "non-finite metric value in {line:?}");
        samples.insert(metric.to_string(), val);
    }
    samples
}

#[tokio::test]
async fn test_observability_endpoints_live_metrics_and_prometheus_format() {
    let metrics = ServerMetrics::new();
    let meta: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    let local_blobs = Arc::new(MemoryBlobStore::new());
    let replicated = ReplicatedBlobStore::new(
        local_blobs,
        Vec::new(),
        meta.clone(),
        ReplicatedBlobStoreConfig {
            cache_capacity_bytes: 1024 * 1024,
            ..Default::default()
        },
    );
    metrics.attach_replication_stats(replicated.stats().clone());
    let store = Arc::new(CompositeStore::new(meta, Arc::new(replicated)));

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let (metrics_addr, metrics_task) = bind_and_serve_metrics(
        "127.0.0.1:0".parse().unwrap(),
        metrics.clone(),
        async move {
            let _ = shutdown_rx.wait_for(|&stopped| stopped).await;
        },
    )
    .await
    .unwrap();

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_service_status("", tonic_health::ServingStatus::Serving)
        .await;
    let grpc_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let grpc_addr = grpc_listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(grpc_listener);

    let backend_svc = CommitCloudBackendService::with_metrics(store.clone(), metrics.clone());
    let op_store_svc = CommitCloudOpStoreService::with_metrics(store.clone(), metrics.clone());
    let workspace_svc = CommitCloudWorkspaceService::with_metrics(store.clone(), metrics.clone());
    let mut grpc_shutdown = shutdown_tx.subscribe();
    let grpc_metrics = metrics.clone();
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .layer(MetricsLayer::new(grpc_metrics))
            .add_service(health_service)
            .add_service(BackendServiceServer::new(backend_svc))
            .add_service(OpStoreServiceServer::new(op_store_svc))
            .add_service(WorkspaceServiceServer::new(workspace_svc))
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = grpc_shutdown.wait_for(|&stopped| stopped).await;
            })
            .await;
    });

    let (status, _, health_body) = http_get(metrics_addr, "/healthz").await;
    assert_eq!(status, 200);
    assert_eq!(health_body, "ok\n");

    // Health probes over gRPC must not count toward RPC duration histogram samples.
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{grpc_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut health_client = HealthClient::new(channel.clone());
    health_client
        .check(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .unwrap();
    let (_, _, prom_after_health) = http_get(metrics_addr, "/metrics").await;
    let samples_after_health = parse_prometheus_samples(&prom_after_health);
    assert_eq!(
        samples_after_health.get("cloudjj_rpc_duration_seconds_count"),
        Some(&0.0),
        "gRPC health checks must be excluded from RPC latency metrics"
    );

    let mut backend = BackendServiceClient::new(channel.clone());
    let mut op_store = OpStoreServiceClient::new(channel.clone());
    let mut workspace = WorkspaceServiceClient::new(channel);

    backend
        .register_repository(Request::new(RegisterRepositoryRequest {
            project_id: "obs-proj".to_string(),
            repo_id: Some("obs-repo".to_string()),
            name: None,
            hash_algorithm: HashAlgorithm::Sha1 as i32,
        }))
        .await
        .unwrap();

    // Write + read a file twice so ReplicatedBlobStore records 1 cache miss + 1 cache hit.
    let file_id = backend
        .write_file(Request::new(WriteFileRequest {
            project_id: "obs-proj".to_string(),
            content: b"observability cached blob".to_vec(),
        }))
        .await
        .unwrap()
        .into_inner()
        .file_id;
    for _ in 0..2 {
        let mut stream = backend
            .read_file(Request::new(ReadFileRequest {
                project_id: "obs-proj".to_string(),
                file_id: file_id.clone(),
                offset: 0,
                max_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        let mut data = Vec::new();
        while let Some(frame) = stream.message().await.unwrap() {
            data.extend_from_slice(&frame.chunk);
        }
        assert_eq!(data, b"observability cached blob");
    }

    let mut commit_ids = Vec::new();
    for idx in 1u8..=3u8 {
        let cid = backend
            .write_commit(Request::new(WriteCommitRequest {
                project_id: "obs-proj".to_string(),
                commit: Some(sample_commit(&format!("commit {idx}\n"), idx)),
            }))
            .await
            .unwrap()
            .into_inner()
            .commit_id;
        commit_ids.push(cid);
    }

    // Create 2 workspaces (and update ws-1 twice to verify idempotent tracking).
    #[allow(deprecated)]
    let make_ws = |name: &str| WorkspaceState {
        repo_id: "obs-repo".to_string(),
        user: "alice@example.com".to_string(),
        workspace_name: name.to_string(),
        commit_id: commit_ids[0].clone(),
        operation_id: cc_common::ROOT_OPERATION_ID_BYTES.to_vec(),
        tree_id: cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
        tree_ids: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
        conflict_labels: Vec::new(),
        sparse_patterns: vec![String::new()],
        sparse_patterns_set: true,
    };
    workspace
        .update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(make_ws("ws-1")),
            expected_operation_id: None,
        }))
        .await
        .unwrap();
    workspace
        .update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(make_ws("ws-1")),
            expected_operation_id: None,
        }))
        .await
        .unwrap();
    workspace
        .update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(make_ws("ws-2")),
            expected_operation_id: None,
        }))
        .await
        .unwrap();

    // Verify active_workspaces == 2 before deleting ws-1 -> 1.
    let (statz_status, statz_headers, statz_body) = http_get(metrics_addr, "/statz").await;
    assert_eq!(statz_status, 200);
    assert!(
        statz_headers
            .get("content-type")
            .is_some_and(|v| v.contains("application/json")),
        "unexpected /statz content-type: {statz_headers:?}"
    );
    let mid_snap: StatzSnapshot = serde_json::from_str(&statz_body).unwrap();
    assert_eq!(mid_snap.active_workspaces, 2);

    workspace
        .delete_workspace(Request::new(DeleteWorkspaceRequest {
            repo_id: "obs-repo".to_string(),
            user: "alice@example.com".to_string(),
            workspace_name: "ws-1".to_string(),
        }))
        .await
        .unwrap();

    // Converged ReconcileOpHeads call (outcome="merged").
    let rec_ok = op_store
        .reconcile_op_heads(Request::new(ReconcileOpHeadsRequest {
            repo_id: "obs-repo".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!rec_ok.client_resolution_required);

    // Divergent heads without commit_predecessors_set -> client_resolution_required = true.
    for (head_cid, desc) in [(&commit_ids[0], "branch-a"), (&commit_ids[1], "branch-b")] {
        op_store
            .publish_operation(Request::new(PublishOperationRequest {
                repo_id: "obs-repo".to_string(),
                view: Some(View {
                    head_ids: vec![head_cid.clone()],
                    ..Default::default()
                }),
                operation: Some(Operation {
                    view_id: Vec::new(),
                    parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                    metadata: Some(OperationMetadata {
                        description: desc.to_string(),
                        ..Default::default()
                    }),
                    commit_predecessors: Vec::new(),
                    commit_predecessors_set: false,
                }),
                old_op_head_ids: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
            }))
            .await
            .unwrap();
    }
    let rec_fallback = op_store
        .reconcile_op_heads(Request::new(ReconcileOpHeadsRequest {
            repo_id: "obs-repo".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(rec_fallback.client_resolution_required);

    metrics
        .db_tx_retries_counter()
        .fetch_add(2, Ordering::Relaxed);

    // Delete repository -> removes remaining ws-2 from active_workspaces -> 0.
    let del_resp = backend
        .delete_repository(Request::new(DeleteRepositoryRequest {
            repo_id: "obs-repo".to_string(),
            project_id: Some("obs-proj".to_string()),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(del_resp.existed);

    let (status, _, statz_body) = http_get(metrics_addr, "/statz").await;
    assert_eq!(status, 200);
    let snap: StatzSnapshot = serde_json::from_str(&statz_body).unwrap();
    assert_eq!(snap.total_commits, 3);
    assert!(
        snap.commits_per_second > 0.0,
        "got {}",
        snap.commits_per_second
    );
    assert_eq!(snap.active_workspaces, 0);
    assert!(snap.rpc_p50_ms >= 0.0);
    assert!(snap.rpc_p95_ms >= snap.rpc_p50_ms);
    assert_eq!(snap.cas_mirror_dropped_total, 0);
    assert_eq!(snap.db_tx_retries_total, 2);
    assert_eq!(snap.reconcile_op_heads_total, 2);
    assert_eq!(snap.reconcile_client_fallback_total, 1);
    assert_eq!(snap.blob_cache_hits, 1);
    assert_eq!(snap.blob_cache_misses, 1);

    // Prometheus 0.0.4 /metrics output and parity with /statz.
    let (prom_status, prom_headers, prom_body) = http_get(metrics_addr, "/metrics").await;
    assert_eq!(prom_status, 200);
    assert_eq!(
        prom_headers.get("content-type").map(String::as_str),
        Some("text/plain; version=0.0.4; charset=utf-8")
    );
    let samples = parse_prometheus_samples(&prom_body);
    assert_eq!(samples.get("cloudjj_commits_total"), Some(&3.0));
    assert_eq!(samples.get("cloudjj_active_workspaces"), Some(&0.0));
    assert_eq!(samples.get("cloudjj_cas_mirror_dropped_total"), Some(&0.0));
    assert_eq!(samples.get("cloudjj_sql_tx_retries_total"), Some(&2.0));
    assert_eq!(
        samples.get("cloudjj_reconcile_op_heads_total{outcome=\"merged\"}"),
        Some(&1.0)
    );
    assert_eq!(
        samples.get("cloudjj_reconcile_op_heads_total{outcome=\"client_fallback\"}"),
        Some(&1.0)
    );
    assert_eq!(
        samples.get("cloudjj_blob_cache_requests_total{result=\"hit\"}"),
        Some(&1.0)
    );
    assert_eq!(
        samples.get("cloudjj_blob_cache_requests_total{result=\"miss\"}"),
        Some(&1.0)
    );

    let rpc_count = *samples
        .get("cloudjj_rpc_duration_seconds_count")
        .expect("rpc histogram count");
    assert!(
        rpc_count >= 14.0,
        "expected >= 14 recorded RPCs, got {rpc_count}"
    );
    assert_eq!(
        samples.get("cloudjj_rpc_duration_seconds_bucket{le=\"+Inf\"}"),
        Some(&rpc_count)
    );
    assert!(
        *samples
            .get("cloudjj_rpc_duration_seconds_sum")
            .expect("rpc histogram sum")
            >= 0.0
    );

    let mut prev_bucket = 0.0;
    for le in [
        "0.001", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10",
        "+Inf",
    ] {
        let key = format!("cloudjj_rpc_duration_seconds_bucket{{le=\"{le}\"}}");
        let val = *samples
            .get(&key)
            .unwrap_or_else(|| panic!("missing bucket {key}"));
        assert!(val >= prev_bucket, "histogram bucket {key} decreased");
        prev_bucket = val;
    }

    let _ = shutdown_tx.send(true);
    metrics_task.await.unwrap();
}

#[tokio::test]
async fn test_server_cli_metrics_listen_unauthenticated_and_fail_fast() {
    // Invalid --metrics-listen fails fast before creating SQLite DB.
    let dir = tempfile::tempdir().unwrap();
    let sqlite_path = dir.path().join("must_not_exist.db");
    let mut cmd = std::process::Command::new(testutils::server_binary_path());
    for var in testutils::SANITIZED_SERVER_ENV_VARS {
        cmd.env_remove(var);
    }
    let out = cmd
        .args([
            "--port=0",
            "--store-type=sqlite",
            &format!("--sqlite-path={}", sqlite_path.display()),
            "--metrics-listen=not-a-valid-addr",
        ])
        .output()
        .expect("spawn jj-cc-server");
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("invalid --metrics-listen"),
        "unexpected stderr: {stderr}"
    );
    assert!(!sqlite_path.exists());

    // Blank METRICS_LISTEN disables the observability server.
    let blank_server = testutils::spawn_server_with_env(&[("METRICS_LISTEN", "   ")], &[]).await;
    let blank_channel = tonic::transport::Endpoint::from_shared(blank_server.url().to_string())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut blank_health = HealthClient::new(blank_channel);
    blank_health
        .check(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .unwrap();

    // Subprocess with Bearer auth + --metrics-listen=127.0.0.1:0 serves /healthz, /statz, /metrics unauthenticated.
    let server = testutils::spawn_server_with_env(
        &[("JJ_CC_SERVER_AUTH_TOKEN", "obs-secret-token")],
        &["--metrics-listen=127.0.0.1:0"],
    )
    .await;
    let metrics_addr = server
        .metrics_addr()
        .expect("metrics listener address should be logged on startup");

    let (h_status, _, h_body) = http_get(metrics_addr, "/healthz").await;
    assert_eq!(h_status, 200);
    assert_eq!(h_body, "ok\n");

    let channel = tonic::transport::Endpoint::from_shared(server.url().to_string())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut backend = BackendServiceClient::new(channel.clone());

    // Unauthenticated gRPC still fails with UNAUTHENTICATED and must not skew RPC latency metrics.
    let err = backend
        .register_repository(Request::new(RegisterRepositoryRequest {
            project_id: "subproc-proj".to_string(),
            repo_id: None,
            name: None,
            hash_algorithm: HashAlgorithm::Sha1 as i32,
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);

    // ServerAuthLayer rejects unauthenticated requests to unknown gRPC paths and trailing-slash
    // service roots before routing; authenticated requests to those unrouted paths return
    // UNIMPLEMENTED. MetricsLayer must ignore both so they cannot pollute latency samples.
    let mut raw_grpc = tonic::client::Grpc::new(channel);
    for unrouted_path in [
        "/unknown.Service/Probe",
        "/commit_cloud.backend.BackendService/",
    ] {
        raw_grpc.ready().await.unwrap();
        let codec = tonic::codec::ProstCodec::<
            HealthCheckRequest,
            tonic_health::pb::HealthCheckResponse,
        >::default();
        let unauth_err = raw_grpc
            .unary(
                Request::new(HealthCheckRequest {
                    service: String::new(),
                }),
                http::uri::PathAndQuery::from_static(unrouted_path),
                codec,
            )
            .await
            .unwrap_err();
        assert_eq!(
            unauth_err.code(),
            Code::Unauthenticated,
            "unauthenticated path={unrouted_path}"
        );

        raw_grpc.ready().await.unwrap();
        let mut authed_probe = Request::new(HealthCheckRequest {
            service: String::new(),
        });
        authed_probe
            .metadata_mut()
            .insert("authorization", "Bearer obs-secret-token".parse().unwrap());
        let codec = tonic::codec::ProstCodec::<
            HealthCheckRequest,
            tonic_health::pb::HealthCheckResponse,
        >::default();
        let unknown_err = raw_grpc
            .unary(
                authed_probe,
                http::uri::PathAndQuery::from_static(unrouted_path),
                codec,
            )
            .await
            .unwrap_err();
        assert_eq!(
            unknown_err.code(),
            Code::Unimplemented,
            "authenticated path={unrouted_path}"
        );
    }

    let (unauth_status, _, unauth_body) = http_get(metrics_addr, "/metrics").await;
    assert_eq!(unauth_status, 200);
    let unauth_samples = parse_prometheus_samples(&unauth_body);
    assert_eq!(
        unauth_samples.get("cloudjj_rpc_duration_seconds_count"),
        Some(&0.0),
        "UNAUTHENTICATED rejections and unknown paths must not be recorded in RPC duration metrics"
    );

    // Authenticated RegisterRepository + WriteCommit increments /statz and /metrics.
    let mut reg_req = Request::new(RegisterRepositoryRequest {
        project_id: "subproc-proj".to_string(),
        repo_id: Some("subproc-repo".to_string()),
        name: None,
        hash_algorithm: HashAlgorithm::Sha1 as i32,
    });
    reg_req
        .metadata_mut()
        .insert("authorization", "Bearer obs-secret-token".parse().unwrap());
    backend.register_repository(reg_req).await.unwrap();

    let mut commit_req = Request::new(WriteCommitRequest {
        project_id: "subproc-proj".to_string(),
        commit: Some(sample_commit("subprocess commit\n", 0x99)),
    });
    commit_req
        .metadata_mut()
        .insert("authorization", "Bearer obs-secret-token".parse().unwrap());
    backend.write_commit(commit_req).await.unwrap();

    // Poll briefly in case the final response future drops right after client decode.
    let mut snap = StatzSnapshot {
        total_commits: 0,
        commits_per_second: 0.0,
        active_workspaces: 0,
        rpc_p50_ms: 0.0,
        rpc_p95_ms: 0.0,
        cas_mirror_dropped_total: 0,
        db_tx_retries_total: 0,
        reconcile_op_heads_total: 0,
        reconcile_client_fallback_total: 0,
        blob_cache_hits: 0,
        blob_cache_misses: 0,
    };
    for _ in 0..20 {
        let (st, _, body) = http_get(metrics_addr, "/statz").await;
        assert_eq!(st, 200);
        snap = serde_json::from_str(&body).unwrap();
        if snap.total_commits == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(snap.total_commits, 1);
    assert!(snap.commits_per_second > 0.0);
    assert_eq!(snap.cas_mirror_dropped_total, 0);

    let (m_status, _, m_body) = http_get(metrics_addr, "/metrics").await;
    assert_eq!(m_status, 200);
    let samples = parse_prometheus_samples(&m_body);
    assert_eq!(samples.get("cloudjj_commits_total"), Some(&1.0));
}

#[tokio::test]
async fn test_multi_replica_delete_and_reregister_immediately_isolates_projects_over_grpc() {
    let meta: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    let blobs = Arc::new(MemoryBlobStore::new());
    let store_a = Arc::new(CompositeStore::new(meta.clone(), blobs.clone()));
    let store_b = Arc::new(CompositeStore::new(meta, blobs));

    let (shutdown_tx, _) = tokio::sync::watch::channel(false);
    let mut channels = Vec::new();
    for store in [store_a, store_b] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let mut rx = shutdown_tx.subscribe();
        tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(BackendServiceServer::new(CommitCloudBackendService::new(
                    store.clone(),
                )))
                .add_service(OpStoreServiceServer::new(CommitCloudOpStoreService::new(
                    store.clone(),
                )))
                .add_service(WorkspaceServiceServer::new(
                    CommitCloudWorkspaceService::new(store),
                ))
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = rx.wait_for(|&stopped| stopped).await;
                })
                .await;
        });
        let ch = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        channels.push(ch);
    }

    let mut backend_a = BackendServiceClient::new(channels[0].clone());
    let mut backend_b = BackendServiceClient::new(channels[1].clone());
    let mut op_store_b = OpStoreServiceClient::new(channels[1].clone());

    // Register repo-1 in proj-a via Replica A, write a file + commit, and warm Replica B.
    backend_a
        .register_repository(Request::new(RegisterRepositoryRequest {
            project_id: "proj-a".to_string(),
            repo_id: Some("repo-1".to_string()),
            name: None,
            hash_algorithm: HashAlgorithm::Sha1 as i32,
        }))
        .await
        .unwrap();
    let file_a = backend_a
        .write_file(Request::new(WriteFileRequest {
            project_id: "repo-1".to_string(),
            content: b"replica proj-a secret".to_vec(),
        }))
        .await
        .unwrap()
        .into_inner()
        .file_id;
    let commit_a = backend_a
        .write_commit(Request::new(WriteCommitRequest {
            project_id: "repo-1".to_string(),
            commit: Some(sample_commit("proj-a commit\n", 0x31)),
        }))
        .await
        .unwrap()
        .into_inner()
        .commit_id;

    // Warm Replica B by reading the file through repo-1.
    let mut stream = backend_b
        .read_file(Request::new(ReadFileRequest {
            project_id: "repo-1".to_string(),
            file_id: file_a.clone(),
            offset: 0,
            max_bytes: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    let mut buf = Vec::new();
    while let Some(frame) = stream.message().await.unwrap() {
        buf.extend_from_slice(&frame.chunk);
    }
    assert_eq!(buf, b"replica proj-a secret");

    // Replica A deletes repo-1 (proj-a) and immediately re-registers repo-1 in proj-b.
    let deleted = backend_a
        .delete_repository(Request::new(DeleteRepositoryRequest {
            repo_id: "repo-1".to_string(),
            project_id: Some("proj-a".to_string()),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(deleted.existed);
    backend_a
        .register_repository(Request::new(RegisterRepositoryRequest {
            project_id: "proj-b".to_string(),
            repo_id: Some("repo-1".to_string()),
            name: None,
            hash_algorithm: HashAlgorithm::Sha1 as i32,
        }))
        .await
        .unwrap();

    // Replica B must immediately reject reading proj-a's file via repo-1 (NOT_FOUND).
    let read_err = backend_b
        .read_file(Request::new(ReadFileRequest {
            project_id: "repo-1".to_string(),
            file_id: file_a,
            offset: 0,
            max_bytes: 0,
        }))
        .await
        .unwrap_err();
    assert_eq!(read_err.code(), Code::NotFound);

    // Replica B must reject publishing a view referencing proj-a's commit into repo-1 (proj-b).
    let pub_err = op_store_b
        .publish_operation(Request::new(PublishOperationRequest {
            repo_id: "repo-1".to_string(),
            view: Some(View {
                head_ids: vec![commit_a],
                ..Default::default()
            }),
            operation: Some(Operation {
                view_id: Vec::new(),
                parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                ..Default::default()
            }),
            old_op_head_ids: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
        }))
        .await
        .unwrap_err();
    assert_eq!(pub_err.code(), Code::FailedPrecondition);

    let _ = shutdown_tx.send(true);
}
