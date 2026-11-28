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

use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use cc_common::backend::RegisterRepositoryRequest;
use cc_common::backend::backend_service_client::BackendServiceClient;
use cc_common::op_store::op_store_service_client::OpStoreServiceClient;
use cc_common::op_store::{
    GetOpHeadsRequest, Operation, OperationMetadata, ReadOperationRequest, ReconcileOpHeadsRequest,
    UpdateOpHeadsRequest, WriteOperationRequest,
};
use tempfile::NamedTempFile;
use testutils::{spawn_server, spawn_sqlite_server};
use tokio::sync::Barrier;

/// Helper to register a new repository and return the repo_id.
async fn register_test_repo(server_url: &str) -> String {
    let mut backend_client = BackendServiceClient::connect(server_url.to_string())
        .await
        .expect("Failed to connect backend client");
    let response = backend_client
        .register_repository(tonic::Request::new(RegisterRepositoryRequest {
            project_id: "test-project".to_string(),
            repo_id: None,
            name: None,
            hash_algorithm: cc_common::backend::HashAlgorithm::Sha1 as i32,
        }))
        .await
        .expect("Failed to register repository");
    response.into_inner().repo_id
}

fn make_test_operation(view_id: Vec<u8>, parents: Vec<Vec<u8>>, description: &str) -> Operation {
    Operation {
        view_id,
        parents,
        metadata: Some(OperationMetadata {
            start_time_millis: 1000,
            end_time_millis: 1000,
            description: description.to_string(),
            is_snapshot: false,
            workspace_name: None,
            hostname: "test-host".to_string(),
            username: "test-user".to_string(),
            attributes: std::collections::HashMap::new(),
        }),
        commit_predecessors: vec![],
        commit_predecessors_set: true,
    }
}

/// Verifies that concurrent UpdateOpHeads calls using stale old_op_head_ids preserve sibling
/// heads additively without ABORTED on SQLite (matching lock-free op_store.proto semantics).
#[tokio::test]
async fn test_sqlite_concurrent_write_race_preserves_sibling_heads_additively() {
    let db_file = NamedTempFile::new().unwrap();
    let server = spawn_sqlite_server(db_file.path()).await;
    let repo_id = register_test_repo(server.url()).await;
    let mut op_client_1 = OpStoreServiceClient::connect(server.url().to_string())
        .await
        .unwrap();
    let mut op_client_2 = OpStoreServiceClient::connect(server.url().to_string())
        .await
        .unwrap();

    let root_head = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let make_op = |parent: Vec<u8>, desc: &str| cc_common::op_store::Operation {
        parents: vec![parent],
        view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
        metadata: Some(cc_common::op_store::OperationMetadata {
            start_time_millis: 1000,
            end_time_millis: 1000,
            description: desc.to_string(),
            hostname: "test".to_string(),
            username: "test".to_string(),
            is_snapshot: false,
            workspace_name: None,
            attributes: Default::default(),
        }),
        commit_predecessors: vec![],
        commit_predecessors_set: false,
    };

    // Start from a non-root head: a root-op parent is allowed to fork (a new
    // workspace joining the repository), so it cannot be stale.
    let base_head = op_client_1
        .write_operation(cc_common::op_store::WriteOperationRequest {
            repo_id: repo_id.clone(),
            operation: Some(make_op(root_head.clone(), "base op")),
        })
        .await
        .unwrap()
        .into_inner()
        .operation_id;
    op_client_1
        .update_op_heads(UpdateOpHeadsRequest {
            repo_id: repo_id.clone(),
            old_op_head_ids: vec![root_head],
            new_op_head_id: base_head.clone(),
        })
        .await
        .unwrap();

    // Client 1 persists op_1 and advances base_head -> op_1
    let op_1 = op_client_1
        .write_operation(cc_common::op_store::WriteOperationRequest {
            repo_id: repo_id.clone(),
            operation: Some(make_op(base_head.clone(), "op 1")),
        })
        .await
        .unwrap()
        .into_inner()
        .operation_id;
    op_client_1
        .update_op_heads(UpdateOpHeadsRequest {
            repo_id: repo_id.clone(),
            old_op_head_ids: vec![base_head.clone()],
            new_op_head_id: op_1.clone(),
        })
        .await
        .unwrap();

    // Client 2 persists op_2 and updates using stale base_head: op_1 is preserved alongside op_2.
    let op_2 = op_client_2
        .write_operation(cc_common::op_store::WriteOperationRequest {
            repo_id: repo_id.clone(),
            operation: Some(make_op(base_head.clone(), "op 2")),
        })
        .await
        .unwrap()
        .into_inner()
        .operation_id;
    let resp = op_client_2
        .update_op_heads(UpdateOpHeadsRequest {
            repo_id: repo_id.clone(),
            old_op_head_ids: vec![base_head],
            new_op_head_id: op_2.clone(),
        })
        .await
        .expect("additive lock-free UpdateOpHeads must preserve concurrent sibling heads")
        .into_inner();

    let expected_heads = HashSet::from([op_1, op_2]);
    let returned_heads: HashSet<Vec<u8>> = resp.current_op_head_ids.into_iter().collect();
    assert_eq!(returned_heads, expected_heads);

    let persisted_heads: HashSet<Vec<u8>> = op_client_1
        .get_op_heads(GetOpHeadsRequest { repo_id })
        .await
        .unwrap()
        .into_inner()
        .op_head_ids
        .into_iter()
        .collect();
    assert_eq!(persisted_heads, expected_heads);
}

/// Stress test: 20 concurrent tasks firing simultaneous UpdateOpHeads RPCs against SQLite backend.
#[tokio::test]
async fn test_stress_concurrent_op_heads_writes_sqlite() {
    let db_file = NamedTempFile::new().unwrap();
    let server = spawn_sqlite_server(db_file.path()).await;
    let repo_id = register_test_repo(server.url()).await;

    const NUM_CONCURRENT_WORKERS: usize = 20;
    let barrier = Arc::new(Barrier::new(NUM_CONCURRENT_WORKERS));

    let mut handles = vec![];
    for worker_idx in 0..NUM_CONCURRENT_WORKERS {
        let server_url = server.url().to_string();
        let repo_id = repo_id.clone();
        let barrier = barrier.clone();

        handles.push(tokio::spawn(async move {
            let mut op_client = OpStoreServiceClient::connect(server_url)
                .await
                .expect("Failed to connect client in worker");

            let worker_op = op_client
                .write_operation(cc_common::op_store::WriteOperationRequest {
                    repo_id: repo_id.clone(),
                    operation: Some(cc_common::op_store::Operation {
                        parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                        view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
                        metadata: Some(cc_common::op_store::OperationMetadata {
                            start_time_millis: (worker_idx as i64) + 1,
                            end_time_millis: (worker_idx as i64) + 1,
                            description: format!("worker {worker_idx}"),
                            hostname: "test".to_string(),
                            username: "test".to_string(),
                            is_snapshot: false,
                            workspace_name: None,
                            attributes: Default::default(),
                        }),
                        commit_predecessors: vec![],
                        commit_predecessors_set: false,
                    }),
                })
                .await
                .expect("write_operation should succeed")
                .into_inner()
                .operation_id;

            // Synchronize all tasks to hit the SQLite database simultaneously
            barrier.wait().await;

            let response = op_client
                .update_op_heads(UpdateOpHeadsRequest {
                    repo_id,
                    old_op_head_ids: vec![],
                    new_op_head_id: worker_op.clone(),
                })
                .await;

            assert!(
                response.is_ok(),
                "SQLite Worker {} update_op_heads failed: {:?}",
                worker_idx,
                response.err()
            );
            worker_op
        }));
    }

    let mut inserted_ops = HashSet::new();
    for handle in handles {
        let op_id = handle.await.expect("Task should not panic");
        inserted_ops.insert(op_id);
    }

    let mut op_client = OpStoreServiceClient::connect(server.url().to_string())
        .await
        .unwrap();
    let final_heads = op_client
        .get_op_heads(GetOpHeadsRequest { repo_id })
        .await
        .unwrap()
        .into_inner()
        .op_head_ids;

    let final_set: HashSet<Vec<u8>> = final_heads.into_iter().collect();
    for op in &inserted_ops {
        assert!(
            final_set.contains(op),
            "Expected op head {:?} in SQLite",
            op
        );
    }
}

/// Verifies that ReconcileOpHeads is a no-op fast-path when only a single op head exists.
#[tokio::test]
async fn test_reconcile_op_heads_idempotent_on_single_head() {
    let server = spawn_server().await;
    let repo_id = register_test_repo(server.url()).await;
    let mut op_client = OpStoreServiceClient::connect(server.url().to_string())
        .await
        .expect("Failed to connect op store client");

    let root_head = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();

    let response = op_client
        .reconcile_op_heads(ReconcileOpHeadsRequest {
            repo_id: repo_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.op_head, root_head);
}

/// Verifies that running a jj CLI command (e.g. `jj op log`) in a repo with
/// divergent server op heads automatically triggers reconciliation and succeeds without error.
#[tokio::test]
async fn test_cli_command_auto_reconciles_divergent_op_heads() {
    let workspace = testutils::TestWorkspace::init().await;
    let config = cc_lib::util::CommitCloudConfig::load_from_store(
        &workspace.repo_path().join(".jj/repo/store"),
    )
    .expect("Failed to load repo config");

    let repo_id = config.repo_id;
    let server_url = config.server_url;
    let mut op_client = OpStoreServiceClient::connect(server_url)
        .await
        .expect("Failed to connect op client");

    let current_heads = op_client
        .get_op_heads(GetOpHeadsRequest {
            repo_id: repo_id.clone(),
        })
        .await
        .unwrap()
        .into_inner()
        .op_head_ids;
    assert_eq!(current_heads.len(), 1);
    let base_head = current_heads[0].clone();

    let base_op = op_client
        .read_operation(ReadOperationRequest {
            repo_id: repo_id.clone(),
            operation_id: base_head.clone(),
        })
        .await
        .unwrap()
        .into_inner()
        .operation
        .expect("Base operation should exist");

    let base_view_id = base_op.view_id;

    let op_a_proto = make_test_operation(
        base_view_id.clone(),
        vec![base_head.clone()],
        "concurrent cli branch a",
    );
    let op_branch_a = op_client
        .write_operation(WriteOperationRequest {
            repo_id: repo_id.clone(),
            operation: Some(op_a_proto),
        })
        .await
        .unwrap()
        .into_inner()
        .operation_id;

    let op_b_proto = make_test_operation(
        base_view_id.clone(),
        vec![base_head.clone()],
        "concurrent cli branch b",
    );
    let op_branch_b = op_client
        .write_operation(WriteOperationRequest {
            repo_id: repo_id.clone(),
            operation: Some(op_b_proto),
        })
        .await
        .unwrap()
        .into_inner()
        .operation_id;

    // Publish two divergent heads: Branch A replaces base_head while Branch B adds concurrently.
    op_client
        .update_op_heads(UpdateOpHeadsRequest {
            repo_id: repo_id.clone(),
            old_op_head_ids: vec![base_head.clone()],
            new_op_head_id: op_branch_a.clone(),
        })
        .await
        .unwrap();

    op_client
        .update_op_heads(UpdateOpHeadsRequest {
            repo_id: repo_id.clone(),
            old_op_head_ids: vec![],
            new_op_head_id: op_branch_b.clone(),
        })
        .await
        .unwrap();

    let raw_heads = op_client
        .get_op_heads(GetOpHeadsRequest {
            repo_id: repo_id.clone(),
        })
        .await
        .unwrap()
        .into_inner()
        .op_head_ids;
    assert_eq!(raw_heads.len(), 2);

    workspace
        .jj_cmd()
        .args(["op", "log", "--no-graph", "-T", "description"])
        .assert()
        .success();

    let post_heads = op_client
        .get_op_heads(GetOpHeadsRequest {
            repo_id: repo_id.clone(),
        })
        .await
        .unwrap()
        .into_inner()
        .op_head_ids;
    assert_eq!(post_heads.len(), 1);
    let final_head = &post_heads[0];
    assert_ne!(final_head, &op_branch_a);
    assert_ne!(final_head, &op_branch_b);

    let _merged_op = op_client
        .read_operation(ReadOperationRequest {
            repo_id: repo_id.clone(),
            operation_id: final_head.clone(),
        })
        .await
        .unwrap()
        .into_inner()
        .operation
        .expect("Merged op should exist");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_agents_commit_and_reconcile_server_side_without_locks() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg_dir = dir.path().join("xdg_config");
    fs::create_dir_all(&xdg_dir).unwrap();
    let server = testutils::spawn_server().await;
    let server_url = server.url().to_string();
    let daemon_sandbox_dir = server.daemon_socket_dir().to_path_buf();
    let apply_daemon_sandbox = |cmd: &mut tokio::process::Command, sandbox: &std::path::Path| {
        cmd.env("JJ_CC_DAEMON_SOCKET_DIR", sandbox)
            .env("JJ_CC_DAEMON_DIR", sandbox.join("daemon"))
            .env("JJ_CC_CACHE_DIR", sandbox.join("cache"))
            .env("JJ_CC_DAEMON_CONFIG", sandbox.join("daemon.toml"));
    };

    let ws0 = dir.path().join("ws-0");
    let mut init_cmd = tokio::process::Command::new(assert_cmd::cargo::cargo_bin!("jj"));
    apply_daemon_sandbox(&mut init_cmd, &daemon_sandbox_dir);
    let init_status = init_cmd
        .args([
            "cc",
            "init",
            "--server",
            &server_url,
            "--create",
            ws0.to_str().unwrap(),
        ])
        .env("XDG_CONFIG_HOME", &xdg_dir)
        .env("JJ_USER", "Agent 0")
        .env("JJ_EMAIL", "agent0@example.com")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await
        .unwrap();
    assert!(init_status.success());

    let cfg: cc_lib::util::CommitCloudConfig =
        cc_lib::util::CommitCloudConfig::load_from_store(&ws0).unwrap();
    let repo_id = cfg.repo_id;

    let agent_count = 6usize;
    let mut ws_paths: Vec<PathBuf> = Vec::with_capacity(agent_count);
    for idx in 1..=agent_count {
        let ws = dir.path().join(format!("agent-{idx}"));
        let mut ws_init_cmd = tokio::process::Command::new(assert_cmd::cargo::cargo_bin!("jj"));
        apply_daemon_sandbox(&mut ws_init_cmd, &daemon_sandbox_dir);
        let st = ws_init_cmd
            .args([
                "cc",
                "init",
                "--server",
                &server_url,
                "--repo-id",
                &repo_id,
                "--workspace-name",
                &format!("agent-{idx}"),
                ws.to_str().unwrap(),
            ])
            .env("XDG_CONFIG_HOME", &xdg_dir)
            .env("JJ_USER", format!("Agent {idx}"))
            .env("JJ_EMAIL", format!("agent{idx}@example.com"))
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await
            .unwrap();
        assert!(st.success());
        ws_paths.push(ws);
    }

    // All 6 agents write distinct files and bookmarks concurrently behind a barrier.
    let barrier = Arc::new(Barrier::new(agent_count));
    let mut handles = Vec::with_capacity(agent_count);
    for (idx, ws) in ws_paths.iter().cloned().enumerate() {
        let b = barrier.clone();
        let xdg = xdg_dir.clone();
        let sandbox = daemon_sandbox_dir.clone();
        handles.push(tokio::spawn(async move {
            let agent_num = idx + 1;
            fs::write(
                ws.join(format!("agent_{agent_num}.txt")),
                format!("work by agent {agent_num}\n"),
            )
            .unwrap();
            b.wait().await;
            let st1 = tokio::process::Command::new(assert_cmd::cargo::cargo_bin!("jj"))
                .current_dir(&ws)
                .args(["describe", "-m", &format!("agent-{agent_num} commit")])
                .env("JJ_CC_DAEMON_SOCKET_DIR", &sandbox)
                .env("JJ_CC_DAEMON_DIR", sandbox.join("daemon"))
                .env("JJ_CC_CACHE_DIR", sandbox.join("cache"))
                .env("JJ_CC_DAEMON_CONFIG", sandbox.join("daemon.toml"))
                .env("XDG_CONFIG_HOME", &xdg)
                .env("JJ_USER", format!("Agent {agent_num}"))
                .env("JJ_EMAIL", format!("agent{agent_num}@example.com"))
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .await
                .unwrap();
            assert!(st1.success());

            let st2 = tokio::process::Command::new(assert_cmd::cargo::cargo_bin!("jj"))
                .current_dir(&ws)
                .args([
                    "bookmark",
                    "create",
                    &format!("bm-agent-{agent_num}"),
                    "-r",
                    "@",
                ])
                .env("JJ_CC_DAEMON_SOCKET_DIR", &sandbox)
                .env("JJ_CC_DAEMON_DIR", sandbox.join("daemon"))
                .env("JJ_CC_CACHE_DIR", sandbox.join("cache"))
                .env("JJ_CC_DAEMON_CONFIG", sandbox.join("daemon.toml"))
                .env("XDG_CONFIG_HOME", &xdg)
                .env("JJ_USER", format!("Agent {agent_num}"))
                .env("JJ_EMAIL", format!("agent{agent_num}@example.com"))
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .await
                .unwrap();
            assert!(st2.success());
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    // Next read invokes server-side `ReconcileOpHeads`, merging all 6 concurrent agent views
    // into a single operation head with zero lost commits or bookmarks.
    let mut bm_cmd = tokio::process::Command::new(assert_cmd::cargo::cargo_bin!("jj"));
    apply_daemon_sandbox(&mut bm_cmd, &daemon_sandbox_dir);
    let out = bm_cmd
        .current_dir(&ws0)
        .args(["bookmark", "list", "--no-pager"])
        .env("XDG_CONFIG_HOME", &xdg_dir)
        .env("JJ_USER", "Agent 0")
        .env("JJ_EMAIL", "agent0@example.com")
        .output()
        .await
        .unwrap();
    assert!(out.status.success());
    let bm_stdout = String::from_utf8_lossy(&out.stdout);
    for idx in 1..=agent_count {
        assert!(
            bm_stdout.contains(&format!("bm-agent-{idx}")),
            "missing bookmark bm-agent-{idx} in reconciled view:\n{bm_stdout}"
        );
    }

    let heads_store = cc_lib::cc_op_heads_store::CommitCloudOpHeadsStore::new(repo_id, server_url);
    let raw_heads = heads_store
        .get_op_heads_without_reconciliation()
        .await
        .unwrap();
    assert_eq!(raw_heads.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_rewrite_rebases_descendant_without_divergent_change() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg_dir = dir.path().join("xdg_config");
    fs::create_dir_all(&xdg_dir).unwrap();
    let server = testutils::spawn_server().await;
    let server_url = server.url().to_string();

    let run_jj = |cwd: &std::path::Path, args: &[&str]| {
        let mut cmd = server.jj_cmd();
        cmd.current_dir(cwd)
            .args(args)
            .env("XDG_CONFIG_HOME", &xdg_dir)
            .env("JJ_USER", "Rewrite Tester")
            .env("JJ_EMAIL", "tester@example.com");
        let out = cmd.output().unwrap();
        assert!(
            out.status.success(),
            "jj {:?} failed:\nstdout: {}\nstderr: {}",
            args,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap()
    };

    let ws1 = dir.path().join("ws-1");
    let ws2 = dir.path().join("ws-2");
    run_jj(
        dir.path(),
        &[
            "cc",
            "init",
            "--server",
            &server_url,
            "--create",
            ws1.to_str().unwrap(),
        ],
    );
    let cfg = cc_lib::util::CommitCloudConfig::load_from_store(&ws1).unwrap();
    let repo_id = cfg.repo_id;

    // Attach ws-2 before X is authored so ws-2 starts on root().
    run_jj(
        dir.path(),
        &[
            "cc",
            "init",
            "--server",
            &server_url,
            "--repo-id",
            &repo_id,
            "--workspace-name",
            "ws-2",
            ws2.to_str().unwrap(),
        ],
    );

    // Author commit X in ws-1 and capture the shared fork operation `op_sync`.
    fs::write(ws1.join("shared.txt"), "v1\n").unwrap();
    run_jj(&ws1, &["describe", "-m", "commit X v1"]);
    let x_cid = run_jj(&ws1, &["log", "--no-graph", "-r", "@", "-T", "commit_id"])
        .trim()
        .to_string();
    let x_change_id = run_jj(&ws1, &["log", "--no-graph", "-r", "@", "-T", "change_id"])
        .trim()
        .to_string();
    let op_sync = run_jj(
        &ws1,
        &[
            "op",
            "log",
            "--no-graph",
            "--limit",
            "1",
            "-T",
            "id.short(32)",
        ],
    )
    .trim()
    .to_string();

    // Sync ws-2's local index up to `op_sync`.
    run_jj(&ws2, &["status"]);

    // Branch 1 (ws-1): rewrite X -> X' (`shared.txt = "v2\n"`, description `"commit X v2"`).
    fs::write(ws1.join("shared.txt"), "v2\n").unwrap();
    run_jj(&ws1, &["describe", "-m", "commit X v2"]);
    let x_rewritten_cid = run_jj(&ws1, &["log", "--no-graph", "-r", "@", "-T", "commit_id"])
        .trim()
        .to_string();
    assert_ne!(x_cid, x_rewritten_cid);

    // Branch 2 (ws-2): starting from `op_sync` (before X -> X'), create a child Y on top of X.
    run_jj(&ws2, &["--at-op", &op_sync, "new", &x_cid, "-m", "child Y"]);

    let heads_store = cc_lib::cc_op_heads_store::CommitCloudOpHeadsStore::new(repo_id, server_url);
    let divergent_heads = heads_store
        .get_op_heads_without_reconciliation()
        .await
        .unwrap();
    assert_eq!(
        divergent_heads.len(),
        2,
        "expected 2 divergent op heads before reconciliation"
    );

    // Reconciling via any workspace defers cross-branch rewrites to `DefaultIndex`, rebasing Y
    // onto X' so X's change_id resolves unambiguously with zero divergence.
    let resolved_x = run_jj(
        &ws1,
        &["log", "--no-graph", "-r", &x_change_id, "-T", "commit_id"],
    )
    .trim()
    .to_string();
    assert_eq!(resolved_x, x_rewritten_cid);

    let y_parent_cid = run_jj(
        &ws1,
        &[
            "log",
            "--no-graph",
            "-r",
            "description(substring:\"child Y\")-",
            "-T",
            "commit_id",
        ],
    )
    .trim()
    .to_string();
    assert_eq!(
        y_parent_cid, x_rewritten_cid,
        "child Y must be rebased onto rewritten X'"
    );

    let y_file = run_jj(
        &ws1,
        &[
            "file",
            "show",
            "shared.txt",
            "-r",
            "description(substring:\"child Y\")",
        ],
    );
    assert_eq!(y_file, "v2\n");

    let converged_heads = heads_store
        .get_op_heads_without_reconciliation()
        .await
        .unwrap();
    assert_eq!(converged_heads.len(), 1);
}
