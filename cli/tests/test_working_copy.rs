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

use std::fs;
use std::path::Path;

use assert_cmd::Command;
use cc_common::workspace::workspace_service_client::WorkspaceServiceClient;
use cc_common::workspace::{
    CheckWorkingCopyChangesRequest, DeleteWorkspaceRequest, GetWorkspaceRequest,
    ListWorkspacesRequest, UpdateWorkspaceRequest, WorkspaceState,
};
use cc_lib::util::{CommitCloudConfig, connect_workspace_client, run_on_tokio};
use predicates::prelude::*;
use testutils::TestWorkspace;

fn configure_jj_cmd(mut cmd: Command, cwd: &Path, xdg: &Path) -> Command {
    cmd.current_dir(cwd)
        .env("XDG_CONFIG_HOME", xdg)
        .env("JJ_USER", "Workspace Tester")
        .env("JJ_EMAIL", "ws.tester@example.com");
    cmd
}

fn jj_cmd(server: &testutils::ServerGuard, cwd: &Path, xdg: &Path) -> Command {
    configure_jj_cmd(server.jj_cmd(), cwd, xdg)
}

#[tokio::test]
async fn test_commit_cloud_working_copy_syncs_state_sparse_patterns_and_cli_commands() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_sqlite_server(&dir.path().join("wc.db")).await;

    let ws_main = dir.path().join("ws_main");
    jj_cmd(&server, dir.path(), &xdg)
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--project-id",
            "proj-wc",
            "--create",
            ws_main.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Default working copy type is `commit_cloud`.
    let wc_type = fs::read_to_string(ws_main.join(".jj/working_copy/type")).unwrap();
    assert_eq!(wc_type.trim(), "commit_cloud");

    let cfg = CommitCloudConfig::load_from_store(&ws_main).unwrap();
    let repo_id = cfg.repo_id.clone();
    let server_url = cfg.server_url.clone();

    // Edit files, configure sparse patterns, and describe the commit.
    fs::create_dir_all(ws_main.join("src")).unwrap();
    fs::write(ws_main.join("src/main.rs"), "fn main() {}\n").unwrap();
    jj_cmd(&server, &ws_main, &xdg)
        .args(["sparse", "set", "--clear", "--add", "src"])
        .assert()
        .success();
    jj_cmd(&server, &ws_main, &xdg)
        .args(["describe", "-m", "sparse wc commit"])
        .assert()
        .success();

    let commit_hex = String::from_utf8(
        jj_cmd(&server, &ws_main, &xdg)
            .args(["log", "--no-graph", "-r", "@", "-T", "commit_id"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap()
    .trim()
    .to_string();

    let url_clone = server_url.clone();
    let repo_clone = repo_id.clone();
    let (ws_state, change_check) = run_on_tokio(async move {
        let mut client = connect_workspace_client(url_clone).await?;
        let ws = client
            .get_workspace(GetWorkspaceRequest {
                repo_id: repo_clone.clone(),
                user: "ws.tester@example.com".to_string(),
                workspace_name: "default".to_string(),
            })
            .await?
            .into_inner()
            .workspace
            .expect("workspace state should exist on server");
        let ch = client
            .check_working_copy_changes(CheckWorkingCopyChangesRequest {
                repo_id: repo_clone,
                user: "ws.tester@example.com".to_string(),
                workspace_name: "default".to_string(),
            })
            .await?
            .into_inner();
        Ok((ws, ch))
    })
    .await
    .unwrap();

    assert_eq!(hex::encode(&ws_state.commit_id), commit_hex);
    assert!(!ws_state.operation_id.is_empty());
    assert!(!ws_state.tree_ids.is_empty());
    assert!(ws_state.sparse_patterns.iter().any(|p| p == "src"));
    assert!(!change_check.has_changes);

    let ws_clone = dir.path().join("ws_clone");
    jj_cmd(&server, dir.path(), &xdg)
        .args([
            "cc",
            "clone",
            "--server",
            server.url(),
            "--workspace-name",
            "agent-sandbox",
            &repo_id,
            ws_clone.to_str().unwrap(),
        ])
        .assert()
        .success();

    jj_cmd(&server, &ws_main, &xdg)
        .args(["cc", "workspace", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("default (ws.tester@example.com)"))
        .stdout(predicate::str::contains(
            "agent-sandbox (ws.tester@example.com)",
        ));

    jj_cmd(&server, &ws_main, &xdg)
        .args(["cc", "workspace", "status", "--workspace-name", "default"])
        .assert()
        .success()
        .stdout(predicate::str::contains(&commit_hex))
        .stdout(predicate::str::contains("Sparse:    src"))
        .stdout(predicate::str::contains("Modified:  false"));

    // Omitting --workspace-name inside a non-default Commit Cloud workspace resolves its active name.
    jj_cmd(&server, &ws_clone, &xdg)
        .args(["cc", "workspace", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Workspace: agent-sandbox"))
        .stdout(predicate::str::contains("Sparse:    (all)"));

    jj_cmd(&server, &ws_main, &xdg)
        .args(["cc", "workspace", "delete", "agent-sandbox"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Deleted cloud workspace 'agent-sandbox'",
        ));
}

#[tokio::test]
async fn test_cc_init_supports_explicit_local_working_copy_type() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_server().await;

    let ws_local = dir.path().join("ws_local");
    jj_cmd(&server, dir.path(), &xdg)
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--working-copy-type",
            "local",
            "--create",
            ws_local.to_str().unwrap(),
        ])
        .assert()
        .success();

    let wc_type = fs::read_to_string(ws_local.join(".jj/working_copy/type")).unwrap();
    assert_eq!(wc_type.trim(), "local");

    fs::write(ws_local.join("note.txt"), "local mode\n").unwrap();
    jj_cmd(&server, &ws_local, &xdg)
        .args(["describe", "-m", "local wc mode"])
        .assert()
        .success();
}

#[tokio::test]
async fn test_commit_cloud_working_copy_preserves_conflicted_merge_trees_in_workspace_service() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_sqlite_server(&dir.path().join("wc_conflict.db")).await;

    let ws_dir = dir.path().join("ws_conflict");
    jj_cmd(&server, dir.path(), &xdg)
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--project-id",
            "proj-wc-conflict",
            "--create",
            ws_dir.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Base commit
    fs::write(ws_dir.join("conflict.txt"), "base\n").unwrap();
    jj_cmd(&server, &ws_dir, &xdg)
        .args(["bookmark", "create", "base", "-r", "@"])
        .assert()
        .success();
    jj_cmd(&server, &ws_dir, &xdg)
        .args(["describe", "-m", "base"])
        .assert()
        .success();

    // Left branch
    jj_cmd(&server, &ws_dir, &xdg)
        .args(["new", "base"])
        .assert()
        .success();
    fs::write(ws_dir.join("conflict.txt"), "left side\n").unwrap();
    jj_cmd(&server, &ws_dir, &xdg)
        .args(["bookmark", "create", "left", "-r", "@"])
        .assert()
        .success();
    jj_cmd(&server, &ws_dir, &xdg)
        .args(["describe", "-m", "left"])
        .assert()
        .success();

    // Right branch
    jj_cmd(&server, &ws_dir, &xdg)
        .args(["new", "base"])
        .assert()
        .success();
    fs::write(ws_dir.join("conflict.txt"), "right side\n").unwrap();
    jj_cmd(&server, &ws_dir, &xdg)
        .args(["bookmark", "create", "right", "-r", "@"])
        .assert()
        .success();
    jj_cmd(&server, &ws_dir, &xdg)
        .args(["describe", "-m", "right"])
        .assert()
        .success();

    // Conflicted merge checkout (`Merge<TreeId>` with 3 tree terms: `[left, base, right]`).
    jj_cmd(&server, &ws_dir, &xdg)
        .args(["new", "left", "right", "-m", "conflicted merge"])
        .assert()
        .success();

    let cfg = CommitCloudConfig::load_from_store(&ws_dir).unwrap();
    let (ws_state, change_check) = run_on_tokio(async move {
        let mut client = connect_workspace_client(cfg.server_url).await?;
        let ws = client
            .get_workspace(GetWorkspaceRequest {
                repo_id: cfg.repo_id.clone(),
                user: "ws.tester@example.com".to_string(),
                workspace_name: "default".to_string(),
            })
            .await?
            .into_inner()
            .workspace
            .expect("workspace state should exist");
        let ch = client
            .check_working_copy_changes(CheckWorkingCopyChangesRequest {
                repo_id: cfg.repo_id,
                user: "ws.tester@example.com".to_string(),
                workspace_name: "default".to_string(),
            })
            .await?
            .into_inner();
        Ok((ws, ch))
    })
    .await
    .unwrap();

    assert_eq!(
        ws_state.tree_ids.len(),
        3,
        "conflicted checkout must persist all 3 Merge<TreeId> sides instead of EMPTY_TREE_ID"
    );
    assert!(
        !change_check.has_changes,
        "fresh conflicted merge checkout must match its checked-out commit's Merge<TreeId>"
    );
    assert_eq!(change_check.current_tree_ids, change_check.commit_tree_ids);
}

#[tokio::test]
async fn test_snapshot_only_command_keeps_workspace_commit_current() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_sqlite_server(&dir.path().join("wc_snapshot.db")).await;

    let ws_dir = dir.path().join("ws_snapshot");
    jj_cmd(&server, dir.path(), &xdg)
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--project-id",
            "proj-wc-snapshot",
            "--create",
            ws_dir.to_str().unwrap(),
        ])
        .assert()
        .success();
    jj_cmd(&server, &ws_dir, &xdg)
        .args(["describe", "-m", "checked out by describe"])
        .assert()
        .success();

    // Snapshot-only commands (`jj st`, `jj diff`, `jj log`) rewrite `@` without a checkout.
    for (idx, snapshot_cmd) in [
        (1, &["st"][..]),
        (2, &["diff"][..]),
        (3, &["log", "-n", "1"][..]),
    ] {
        fs::write(ws_dir.join("edited.txt"), format!("edited step {idx}\n")).unwrap();
        jj_cmd(&server, &ws_dir, &xdg)
            .args(snapshot_cmd)
            .assert()
            .success();

        let commit_hex = String::from_utf8(
            jj_cmd(&server, &ws_dir, &xdg)
                .args([
                    "log",
                    "--ignore-working-copy",
                    "--no-graph",
                    "-r",
                    "@",
                    "-T",
                    "commit_id",
                ])
                .assert()
                .success()
                .get_output()
                .stdout
                .clone(),
        )
        .unwrap()
        .trim()
        .to_string();

        let cfg = CommitCloudConfig::load_from_store(&ws_dir).unwrap();
        let (ws_state, change_check) = run_on_tokio(async move {
            let mut client = connect_workspace_client(cfg.server_url).await?;
            let ws = client
                .get_workspace(GetWorkspaceRequest {
                    repo_id: cfg.repo_id.clone(),
                    user: "ws.tester@example.com".to_string(),
                    workspace_name: "default".to_string(),
                })
                .await?
                .into_inner()
                .workspace
                .expect("workspace state should exist");
            let ch = client
                .check_working_copy_changes(CheckWorkingCopyChangesRequest {
                    repo_id: cfg.repo_id,
                    user: "ws.tester@example.com".to_string(),
                    workspace_name: "default".to_string(),
                })
                .await?
                .into_inner();
            Ok((ws, ch))
        })
        .await
        .unwrap();

        assert_eq!(
            hex::encode(&ws_state.commit_id),
            commit_hex,
            "record must follow snapshot from {snapshot_cmd:?}"
        );
        assert!(
            !change_check.has_changes,
            "snapshotted tree must match the recorded commit"
        );

        jj_cmd(&server, &ws_dir, &xdg)
            .args(["cc", "workspace", "status", "--workspace-name", "default"])
            .assert()
            .success()
            .stdout(predicate::str::contains(format!("Commit:    {commit_hex}")))
            .stdout(predicate::str::contains("Modified:  false"));
    }
}

#[tokio::test]
async fn test_workspace_status_surfaces_check_working_copy_changes_errors() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_server().await;

    let ws_dir = dir.path().join("ws_err");
    jj_cmd(&server, dir.path(), &xdg)
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--create",
            ws_dir.to_str().unwrap(),
        ])
        .assert()
        .success();

    let cfg = CommitCloudConfig::load_from_store(&ws_dir).unwrap();
    #[allow(deprecated)]
    run_on_tokio(async move {
        let mut client = connect_workspace_client(cfg.server_url).await?;
        let missing_op_err = client
            .update_workspace(UpdateWorkspaceRequest {
                workspace: Some(WorkspaceState {
                    repo_id: cfg.repo_id.clone(),
                    user: "ws.tester@example.com".to_string(),
                    workspace_name: "missing-op".to_string(),
                    commit_id: Vec::new(),
                    operation_id: vec![0xab; 16],
                    tree_id: cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                    sparse_patterns: Vec::new(),
                    tree_ids: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
                    conflict_labels: Vec::new(),
                    sparse_patterns_set: true,
                }),
                expected_operation_id: None,
            })
            .await
            .expect_err("non-existent operation_id must fail with FAILED_PRECONDITION");
        assert_eq!(missing_op_err.code(), tonic::Code::FailedPrecondition);

        let missing_commit_err = client
            .update_workspace(UpdateWorkspaceRequest {
                workspace: Some(WorkspaceState {
                    repo_id: cfg.repo_id.clone(),
                    user: "ws.tester@example.com".to_string(),
                    workspace_name: "broken".to_string(),
                    commit_id: vec![0xde; 20],
                    operation_id: Vec::new(),
                    tree_id: cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                    sparse_patterns: Vec::new(),
                    tree_ids: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
                    conflict_labels: Vec::new(),
                    sparse_patterns_set: true,
                }),
                expected_operation_id: None,
            })
            .await
            .expect_err("non-existent commit_id must fail with FAILED_PRECONDITION");
        assert_eq!(missing_commit_err.code(), tonic::Code::FailedPrecondition);

        // expected_operation_id CAS: create-only (`Some(vec![])`), exact retry idempotency,
        // and stale `expected_operation_id` conflict (`Code::Aborted`).
        let cas_ws = WorkspaceState {
            repo_id: cfg.repo_id.clone(),
            user: "ws.tester@example.com".to_string(),
            workspace_name: "vfs-cas".to_string(),
            commit_id: cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
            operation_id: cc_common::ROOT_OPERATION_ID_BYTES.to_vec(),
            tree_id: cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
            sparse_patterns: vec![String::new()],
            tree_ids: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
            conflict_labels: Vec::new(),
            sparse_patterns_set: true,
        };
        let created = client
            .update_workspace(UpdateWorkspaceRequest {
                workspace: Some(cas_ws.clone()),
                expected_operation_id: Some(Vec::new()),
            })
            .await
            .expect("create-only CAS on absent workspace should succeed")
            .into_inner();
        assert!(created.success);
        assert_eq!(created.workspace.as_ref(), Some(&cas_ws));
        client
            .update_workspace(UpdateWorkspaceRequest {
                workspace: Some(cas_ws.clone()),
                expected_operation_id: Some(Vec::new()),
            })
            .await
            .expect("exact retry of already-applied CAS workspace state should be idempotent");
        let mut conflicting_ws = cas_ws;
        conflicting_ws.sparse_patterns = Vec::new();
        let cas_conflict = client
            .update_workspace(UpdateWorkspaceRequest {
                workspace: Some(conflicting_ws),
                expected_operation_id: Some(Vec::new()),
            })
            .await
            .expect_err("create-only CAS on existing workspace with different state must abort");
        assert_eq!(cas_conflict.code(), tonic::Code::Aborted);
        Ok(())
    })
    .await
    .unwrap();

    jj_cmd(&server, &ws_dir, &xdg)
        .args(["cc", "workspace", "status", "--workspace-name", "broken"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Cloud workspace 'broken' is not registered",
        ));

    drop(server);
    let offline_sandbox = testutils::DaemonSandbox::new();
    configure_jj_cmd(offline_sandbox.jj_cmd(), &ws_dir, &xdg)
        .args(["cc", "workspace", "status"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Failed to query cloud workspace status",
        ));
}

#[tokio::test]
#[ignore = "needs jjfsd from the jj-vfs repo and FUSE; run with JJFSD_BIN=/path/to/jjfsd cargo test -p cli --test test_working_copy test_vfs_commit_cloud_working_copy_succeeds -- --ignored"]
async fn test_vfs_commit_cloud_working_copy_succeeds() {
    let ws = TestWorkspace::init().await;

    let cloud_file = ws.repo_path().join("cloud_file.txt");
    fs::write(&cloud_file, "hello from commit cloud vfs\n").unwrap();
    let src_dir = ws.repo_path().join("src");
    fs::create_dir_all(&src_dir).unwrap();
    let lib_file = src_dir.join("lib.rs");
    fs::write(&lib_file, "pub fn cloud_fn() {}\n").unwrap();

    ws.jj_cmd()
        .args(["describe", "-m", "commit cloud vfs test"])
        .assert()
        .success();

    let log_output = ws
        .jj_cmd()
        .args(["log", "-r", "@", "-T", "commit_id", "--no-graph"])
        .assert()
        .success();
    let commit_hex = String::from_utf8_lossy(&log_output.get_output().stdout)
        .trim()
        .to_string();
    assert!(!commit_hex.is_empty(), "commit_id should not be empty");

    ws.jj_cmd()
        .args(["sparse", "set", "--clear"])
        .assert()
        .success();

    assert!(
        !cloud_file.exists(),
        "cloud_file.txt should be removed from disk after sparse clear"
    );
    assert!(
        !src_dir.exists(),
        "src/ directory should be removed from disk after sparse clear"
    );

    let vfs = ws.mount_vfs().await;
    let mountpoint = vfs.mountpoint();
    let commit_dir = mountpoint.join("commits").join(&commit_hex);

    let vfs_file1 = commit_dir.join("cloud_file.txt");
    assert!(
        vfs_file1.exists(),
        "cloud_file.txt should exist in /commits/<id>/ via VFS"
    );
    let vfs_content1 =
        fs::read_to_string(&vfs_file1).expect("Failed to read cloud_file.txt via VFS");
    assert_eq!(vfs_content1, "hello from commit cloud vfs\n");

    let vfs_lib_file = commit_dir.join("src").join("lib.rs");
    assert!(
        vfs_lib_file.exists(),
        "src/lib.rs should exist in /commits/<id>/ via VFS"
    );
    let vfs_lib_content =
        fs::read_to_string(&vfs_lib_file).expect("Failed to read src/lib.rs via VFS");
    assert_eq!(vfs_lib_content, "pub fn cloud_fn() {}\n");

    let entries: Vec<String> = fs::read_dir(&commit_dir)
        .expect("Failed to read commit dir")
        .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
        .collect();
    assert!(
        entries.contains(&"cloud_file.txt".to_string()),
        "Directory listing should contain cloud_file.txt"
    );
    assert!(
        entries.contains(&"src".to_string()),
        "Directory listing should contain src"
    );
    assert!(
        !cloud_file.exists(),
        "Physical disk should still have 0 files"
    );
    assert!(
        !lib_file.exists(),
        "Physical disk should still have 0 files"
    );
}

#[tokio::test]
async fn test_delete_workspace_succeeds() {
    let ws = TestWorkspace::init().await;

    let config_path = ws.repo_path().join(".jj/repo/store/config.toml");
    let config_str = fs::read_to_string(&config_path).unwrap();
    let config: toml::Value = toml::from_str(&config_str).unwrap();
    let repo_id = config.get("repo_id").unwrap().as_str().unwrap().to_string();

    let mut client = WorkspaceServiceClient::connect(ws.server_url().to_string())
        .await
        .expect("Failed to connect to WorkspaceService");

    let user = "test.user@example.com".to_string();

    let response = client
        .get_workspace(GetWorkspaceRequest {
            repo_id: repo_id.clone(),
            user: user.clone(),
            workspace_name: "default".to_string(),
        })
        .await
        .expect("Failed to get workspace")
        .into_inner();
    assert!(response.workspace.is_some());

    let del_resp = client
        .delete_workspace(DeleteWorkspaceRequest {
            repo_id: repo_id.clone(),
            user: user.clone(),
            workspace_name: "default".to_string(),
        })
        .await
        .expect("Failed to delete workspace")
        .into_inner();
    assert!(del_resp.existed);

    let response = client
        .get_workspace(GetWorkspaceRequest {
            repo_id: repo_id.clone(),
            user: user.clone(),
            workspace_name: "default".to_string(),
        })
        .await
        .expect("Failed to query workspace")
        .into_inner();
    assert!(response.workspace.is_none());

    let list_resp = client
        .list_workspaces(ListWorkspacesRequest {
            repo_id: repo_id.clone(),
            page_size: 0,
            page_token: String::new(),
        })
        .await
        .expect("Failed to list workspaces")
        .into_inner();
    assert!(list_resp.workspaces.is_empty());
}

#[tokio::test]
async fn test_delete_workspace_nonexistent_returns_false() {
    let ws = TestWorkspace::init().await;

    let config_path = ws.repo_path().join(".jj/repo/store/config.toml");
    let config_str = fs::read_to_string(&config_path).unwrap();
    let config: toml::Value = toml::from_str(&config_str).unwrap();
    let repo_id = config.get("repo_id").unwrap().as_str().unwrap().to_string();

    let mut client = WorkspaceServiceClient::connect(ws.server_url().to_string())
        .await
        .expect("Failed to connect to WorkspaceService");

    let del_resp = client
        .delete_workspace(DeleteWorkspaceRequest {
            repo_id: repo_id.clone(),
            user: "test.user@example.com".to_string(),
            workspace_name: "nonexistent_workspace".to_string(),
        })
        .await
        .expect("Failed to call delete_workspace")
        .into_inner();
    assert!(
        !del_resp.existed,
        "Deleting a nonexistent workspace should return existed = false"
    );

    let del_resp2 = client
        .delete_workspace(DeleteWorkspaceRequest {
            repo_id: "nonexistent_repo".to_string(),
            user: "test.user@example.com".to_string(),
            workspace_name: "default".to_string(),
        })
        .await
        .expect("Failed to call delete_workspace")
        .into_inner();
    assert!(
        !del_resp2.existed,
        "Deleting from a nonexistent repository should return existed = false"
    );
}

#[tokio::test]
async fn test_check_working_copy_changes_succeeds() {
    let ws = TestWorkspace::init().await;

    let test_file = ws.repo_path().join("file.txt");
    fs::write(&test_file, "content").unwrap();
    let mut cmd = ws.jj_cmd();
    cmd.args(["describe", "-m", "test commit"])
        .assert()
        .success();

    let config_path = ws.repo_path().join(".jj/repo/store/config.toml");
    let config_str = fs::read_to_string(&config_path).unwrap();
    let config: toml::Value = toml::from_str(&config_str).unwrap();
    let repo_id = config.get("repo_id").unwrap().as_str().unwrap().to_string();

    let mut client = WorkspaceServiceClient::connect(ws.server_url().to_string())
        .await
        .expect("Failed to connect to WorkspaceService");

    let response = client
        .check_working_copy_changes(CheckWorkingCopyChangesRequest {
            repo_id,
            user: "test.user@example.com".to_string(),
            workspace_name: "default".to_string(),
        })
        .await
        .expect("Failed to check working copy changes")
        .into_inner();

    assert!(
        !response.has_changes,
        "Workspace should have no un-snapshotted changes after describe"
    );
    #[allow(deprecated)]
    {
        assert_eq!(response.current_tree_id, response.commit_tree_id);
    }
}

#[tokio::test]
async fn test_check_working_copy_changes_nonexistent_workspace_fails() {
    let ws = TestWorkspace::init().await;

    let config_path = ws.repo_path().join(".jj/repo/store/config.toml");
    let config_str = fs::read_to_string(&config_path).unwrap();
    let config: toml::Value = toml::from_str(&config_str).unwrap();
    let repo_id = config.get("repo_id").unwrap().as_str().unwrap().to_string();

    let mut client = WorkspaceServiceClient::connect(ws.server_url().to_string())
        .await
        .expect("Failed to connect to WorkspaceService");

    let result = client
        .check_working_copy_changes(CheckWorkingCopyChangesRequest {
            repo_id,
            user: "test.user@example.com".to_string(),
            workspace_name: "nonexistent".to_string(),
        })
        .await;

    assert!(
        result.is_err(),
        "Expected error when checking changes for nonexistent workspace"
    );
    let status = result.unwrap_err();
    assert_eq!(status.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn test_workspace_forget_cli() {
    let ws = TestWorkspace::init().await;

    let mut list_cmd = ws.jj_cmd();
    let output = list_cmd.args(["workspace", "list"]).assert().success();
    let stdout = String::from_utf8_lossy(&output.get_output().stdout);
    assert!(
        stdout.contains("default"),
        "Workspace list should include default"
    );

    let mut forget_cmd = ws.jj_cmd();
    forget_cmd
        .args(["workspace", "forget", "default"])
        .assert()
        .success();

    let mut list_cmd2 = ws.jj_cmd();
    let output2 = list_cmd2.args(["workspace", "list"]).assert().success();
    let stdout2 = String::from_utf8_lossy(&output2.get_output().stdout);
    assert!(
        !stdout2.contains("default:"),
        "Workspace list should not show active default workspace"
    );
}

#[tokio::test]
async fn test_workspace_forget_nonexistent_cli_warning() {
    let ws = TestWorkspace::init().await;

    let mut cmd = ws.jj_cmd();
    let output = cmd
        .args(["workspace", "forget", "nonexistent_workspace"])
        .assert()
        .success();
    let stderr = String::from_utf8_lossy(&output.get_output().stderr);
    assert!(
        stderr.contains("No such workspace: nonexistent_workspace"),
        "stderr should warn about nonexistent workspace, got: {stderr}"
    );
}

#[tokio::test]
#[allow(deprecated)]
async fn test_workspace_cas_conflicts_sparse_pagination_and_preconditions() {
    let ws = TestWorkspace::init_sqlite().await;
    let config_path = ws.repo_path().join(".jj/repo/store/config.toml");
    let config_str = fs::read_to_string(&config_path).unwrap();
    let config: toml::Value = toml::from_str(&config_str).unwrap();
    let repo_id = config.get("repo_id").unwrap().as_str().unwrap().to_string();

    let mut client = WorkspaceServiceClient::connect(ws.server_url().to_string())
        .await
        .expect("Failed to connect to WorkspaceService");

    // Create-only CAS (`expected_operation_id = Some(vec![])`) succeeds on a new workspace
    // and is idempotent on exact retry, but returns ABORTED when attempting to overwrite.
    let agent_ws = WorkspaceState {
        repo_id: repo_id.clone(),
        user: "agent@example.com".to_string(),
        workspace_name: "agent-1".to_string(),
        commit_id: cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
        operation_id: cc_common::ROOT_OPERATION_ID_BYTES.to_vec(),
        tree_id: cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
        sparse_patterns: Vec::new(),
        tree_ids: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
        conflict_labels: Vec::new(),
        sparse_patterns_set: true,
    };
    client
        .update_workspace(UpdateWorkspaceRequest {
            workspace: Some(agent_ws.clone()),
            expected_operation_id: Some(Vec::new()),
        })
        .await
        .expect("create-only CAS should succeed");
    // Exact retry is idempotent.
    client
        .update_workspace(UpdateWorkspaceRequest {
            workspace: Some(agent_ws.clone()),
            expected_operation_id: Some(Vec::new()),
        })
        .await
        .expect("idempotent create-only CAS retry should succeed");

    // Empty sparse patterns (`sparse_patterns_set = true`) are preserved rather than reset to `[""]`.
    let fetched_empty_sparse = client
        .get_workspace(GetWorkspaceRequest {
            repo_id: repo_id.clone(),
            user: "agent@example.com".to_string(),
            workspace_name: "agent-1".to_string(),
        })
        .await
        .unwrap()
        .into_inner()
        .workspace
        .unwrap();
    assert!(fetched_empty_sparse.sparse_patterns.is_empty());
    assert!(fetched_empty_sparse.sparse_patterns_set);

    // Different state with create-only precondition fails with ABORTED.
    let mut modified_ws = agent_ws.clone();
    modified_ws.sparse_patterns = vec!["src".to_string()];
    let create_conflict = client
        .update_workspace(UpdateWorkspaceRequest {
            workspace: Some(modified_ws.clone()),
            expected_operation_id: Some(Vec::new()),
        })
        .await
        .unwrap_err();
    assert_eq!(create_conflict.code(), tonic::Code::Aborted);

    // Wrong `expected_operation_id` fails with ABORTED, while matching `ROOT_OPERATION_ID` succeeds.
    let wrong_op = vec![0xab; cc_common::OPERATION_ID_LENGTH];
    let op_conflict = client
        .update_workspace(UpdateWorkspaceRequest {
            workspace: Some(modified_ws.clone()),
            expected_operation_id: Some(wrong_op),
        })
        .await
        .unwrap_err();
    assert_eq!(op_conflict.code(), tonic::Code::Aborted);

    client
        .update_workspace(UpdateWorkspaceRequest {
            workspace: Some(modified_ws),
            expected_operation_id: Some(cc_common::ROOT_OPERATION_ID_BYTES.to_vec()),
        })
        .await
        .expect("matching expected_operation_id CAS should succeed");

    // FAILED_PRECONDITION when referencing an unpersisted operation, commit, or tree.
    let missing_op_err = client
        .update_workspace(UpdateWorkspaceRequest {
            workspace: Some(WorkspaceState {
                operation_id: vec![0x11; cc_common::OPERATION_ID_LENGTH],
                ..agent_ws.clone()
            }),
            expected_operation_id: None,
        })
        .await
        .unwrap_err();
    assert_eq!(missing_op_err.code(), tonic::Code::FailedPrecondition);

    let missing_tree_err = client
        .update_workspace(UpdateWorkspaceRequest {
            workspace: Some(WorkspaceState {
                tree_id: vec![0x22; cc_common::EMPTY_TREE_ID_BYTES.len()],
                tree_ids: vec![vec![0x22; cc_common::EMPTY_TREE_ID_BYTES.len()]],
                ..agent_ws.clone()
            }),
            expected_operation_id: None,
        })
        .await
        .unwrap_err();
    assert_eq!(missing_tree_err.code(), tonic::Code::FailedPrecondition);

    // Conflicted 3-term `Merge<TreeId>` (`[EMPTY, EMPTY, EMPTY]` with labels) round-trips
    // on SQLite and reports `has_changes = true` against the resolved single-tree root commit.
    let conflicted_ws = WorkspaceState {
        tree_id: cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
        tree_ids: vec![
            cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
            cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
            cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
        ],
        conflict_labels: vec![
            "side-1".to_string(),
            "base".to_string(),
            "side-2".to_string(),
        ],
        ..agent_ws
    };
    client
        .update_workspace(UpdateWorkspaceRequest {
            workspace: Some(conflicted_ws),
            expected_operation_id: None,
        })
        .await
        .expect("conflicted 3-term WorkspaceState should persist");
    let check_resp = client
        .check_working_copy_changes(CheckWorkingCopyChangesRequest {
            repo_id: repo_id.clone(),
            user: "agent@example.com".to_string(),
            workspace_name: "agent-1".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(check_resp.has_changes);
    assert_eq!(check_resp.current_tree_ids.len(), 3);
    assert_eq!(check_resp.commit_tree_ids.len(), 1);

    // Cursor pagination on ListWorkspaces (`agent-1` + `default` = 2 workspaces).
    let page1 = client
        .list_workspaces(ListWorkspacesRequest {
            repo_id: repo_id.clone(),
            page_size: 1,
            page_token: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(page1.workspaces.len(), 1);
    assert!(!page1.next_page_token.is_empty());

    let page2 = client
        .list_workspaces(ListWorkspacesRequest {
            repo_id: repo_id.clone(),
            page_size: 1,
            page_token: page1.next_page_token,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(page2.workspaces.len(), 1);
    assert!(page2.next_page_token.is_empty());
    assert_ne!(
        (
            &page1.workspaces[0].user,
            &page1.workspaces[0].workspace_name
        ),
        (
            &page2.workspaces[0].user,
            &page2.workspaces[0].workspace_name
        )
    );

    let bad_cursor = client
        .list_workspaces(ListWorkspacesRequest {
            repo_id,
            page_size: 10,
            page_token: "not-hex!".to_string(),
        })
        .await
        .unwrap_err();
    assert_eq!(bad_cursor.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
#[allow(deprecated)]
async fn test_working_copy_sparse_after_snapshot_rename_and_unregistered_repo_not_found() {
    let ws = TestWorkspace::init().await;
    let config_path = ws.repo_path().join(".jj/repo/store/config.toml");
    let config_str = fs::read_to_string(&config_path).unwrap();
    let config: toml::Value = toml::from_str(&config_str).unwrap();
    let repo_id = config.get("repo_id").unwrap().as_str().unwrap().to_string();

    let mut client = WorkspaceServiceClient::connect(ws.server_url().to_string())
        .await
        .expect("Failed to connect to WorkspaceService");

    // Unregistered repo_id returns NOT_FOUND on GetWorkspace, ListWorkspaces,
    // and CheckWorkingCopyChanges.
    assert_eq!(
        client
            .get_workspace(GetWorkspaceRequest {
                repo_id: "missing-repo".to_string(),
                user: "test.user@example.com".to_string(),
                workspace_name: "default".to_string(),
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    assert_eq!(
        client
            .list_workspaces(ListWorkspacesRequest {
                repo_id: "missing-repo".to_string(),
                page_size: 10,
                page_token: String::new(),
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    assert_eq!(
        client
            .check_working_copy_changes(CheckWorkingCopyChangesRequest {
                repo_id: "missing-repo".to_string(),
                user: "test.user@example.com".to_string(),
                workspace_name: "default".to_string(),
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );

    // Snapshot a file change, then run same-operation `jj status` and `jj sparse set --clear`.
    // Regression test ensuring `WorkspaceState.commit_id` never reverts to a stale commit ID
    // and `CheckWorkingCopyChanges` stays clean (`has_changes = false`).
    fs::write(ws.repo_path().join("notes.txt"), "hello world\n").unwrap();
    ws.jj_cmd()
        .args(["describe", "-m", "add notes"])
        .assert()
        .success();
    let commit_hex = String::from_utf8_lossy(
        &ws.jj_cmd()
            .args(["log", "-r", "@", "-T", "commit_id", "--no-graph"])
            .assert()
            .success()
            .get_output()
            .stdout,
    )
    .trim()
    .to_string();
    ws.jj_cmd().args(["status"]).assert().success();
    ws.jj_cmd()
        .args(["sparse", "set", "--clear"])
        .assert()
        .success();

    let after_sparse = client
        .get_workspace(GetWorkspaceRequest {
            repo_id: repo_id.clone(),
            user: "test.user@example.com".to_string(),
            workspace_name: "default".to_string(),
        })
        .await
        .unwrap()
        .into_inner()
        .workspace
        .expect("workspace should exist");
    assert_eq!(
        jj_lib::hex_util::encode_hex(&after_sparse.commit_id),
        commit_hex,
        "same-op jj status / sparse set must not revert commit_id"
    );
    assert!(after_sparse.sparse_patterns.is_empty());
    assert!(after_sparse.sparse_patterns_set);
    ws.jj_cmd()
        .args(["cc", "workspace", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Sparse:    (none)"));

    let clean_check = client
        .check_working_copy_changes(CheckWorkingCopyChangesRequest {
            repo_id: repo_id.clone(),
            user: "test.user@example.com".to_string(),
            workspace_name: "default".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!clean_check.has_changes);

    // `jj workspace rename` publishes the new record and removes the old name.
    ws.jj_cmd()
        .args(["workspace", "rename", "renamed-ws"])
        .assert()
        .success();
    let old_ws = client
        .get_workspace(GetWorkspaceRequest {
            repo_id: repo_id.clone(),
            user: "test.user@example.com".to_string(),
            workspace_name: "default".to_string(),
        })
        .await
        .unwrap()
        .into_inner()
        .workspace;
    assert!(old_ws.is_none(), "old workspace name should be removed");
    let new_ws = client
        .get_workspace(GetWorkspaceRequest {
            repo_id: repo_id.clone(),
            user: "test.user@example.com".to_string(),
            workspace_name: "renamed-ws".to_string(),
        })
        .await
        .unwrap()
        .into_inner()
        .workspace;
    assert!(new_ws.is_some(), "renamed workspace should be persisted");
    ws.jj_cmd()
        .args(["cc", "workspace", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Workspace: renamed-ws"));

    // Tree count ceiling (`> 1025`) is rejected with INVALID_ARGUMENT.
    let huge_trees = vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec(); 1027];
    let err = client
        .update_workspace(UpdateWorkspaceRequest {
            workspace: Some(WorkspaceState {
                repo_id,
                user: "test.user@example.com".to_string(),
                workspace_name: "renamed-ws".to_string(),
                commit_id: cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
                operation_id: cc_common::ROOT_OPERATION_ID_BYTES.to_vec(),
                tree_id: cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                sparse_patterns: vec![String::new()],
                tree_ids: huge_trees,
                conflict_labels: Vec::new(),
                sparse_patterns_set: true,
            }),
            expected_operation_id: None,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
#[allow(deprecated)]
async fn test_working_copy_real_conflicted_checkout_and_slash_workspace_name() {
    use cc_common::workspace::CheckWorkingCopyChangesRequest;

    let ws = TestWorkspace::init_sqlite().await;
    let config_path = ws.repo_path().join(".jj/repo/store/config.toml");
    let config_str = fs::read_to_string(&config_path).unwrap();
    let config: toml::Value = toml::from_str(&config_str).unwrap();
    let repo_id = config.get("repo_id").unwrap().as_str().unwrap().to_string();

    let conflict_file = ws.repo_path().join("conflict.txt");
    fs::write(&conflict_file, "base\n").unwrap();
    ws.jj_cmd()
        .args(["describe", "-m", "base"])
        .assert()
        .success();
    ws.jj_cmd()
        .args(["bookmark", "create", "base", "-r", "@"])
        .assert()
        .success();

    ws.jj_cmd()
        .args(["new", "base", "-m", "side-a"])
        .assert()
        .success();
    fs::write(&conflict_file, "from side a\n").unwrap();
    ws.jj_cmd()
        .args(["bookmark", "create", "side-a", "-r", "@"])
        .assert()
        .success();

    ws.jj_cmd()
        .args(["new", "base", "-m", "side-b"])
        .assert()
        .success();
    fs::write(&conflict_file, "from side b\n").unwrap();
    ws.jj_cmd()
        .args(["bookmark", "create", "side-b", "-r", "@"])
        .assert()
        .success();

    ws.jj_cmd()
        .args(["new", "side-a", "side-b", "-m", "conflicted merge"])
        .assert()
        .success();

    let mut client = WorkspaceServiceClient::connect(ws.server_url().to_string())
        .await
        .expect("Failed to connect to WorkspaceService");

    let ws_state = client
        .get_workspace(GetWorkspaceRequest {
            repo_id: repo_id.clone(),
            user: "test.user@example.com".to_string(),
            workspace_name: "default".to_string(),
        })
        .await
        .unwrap()
        .into_inner()
        .workspace
        .expect("conflicted workspace should be persisted");
    assert_eq!(ws_state.tree_ids.len(), 3);
    assert_eq!(ws_state.tree_id, ws_state.tree_ids[0]);
    assert_eq!(ws_state.conflict_labels.len(), 3);
    assert!(ws_state.conflict_labels[0].contains("side-a"));
    assert!(ws_state.conflict_labels[1].contains("base"));
    assert!(ws_state.conflict_labels[2].contains("side-b"));

    let check_resp = client
        .check_working_copy_changes(CheckWorkingCopyChangesRequest {
            repo_id: repo_id.clone(),
            user: "test.user@example.com".to_string(),
            workspace_name: "default".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!check_resp.has_changes);
    assert_eq!(check_resp.current_tree_ids, ws_state.tree_ids);
    assert_eq!(check_resp.commit_tree_ids, ws_state.tree_ids);
    assert_eq!(check_resp.current_tree_id, ws_state.tree_id);
    assert_eq!(check_resp.commit_tree_id, ws_state.tree_id);

    // Renaming to a slash-containing upstream jj workspace name syncs and removes the old name.
    ws.jj_cmd()
        .args(["workspace", "rename", "feature/conflict-test"])
        .assert()
        .success();
    let old_ws = client
        .get_workspace(GetWorkspaceRequest {
            repo_id: repo_id.clone(),
            user: "test.user@example.com".to_string(),
            workspace_name: "default".to_string(),
        })
        .await
        .unwrap()
        .into_inner()
        .workspace;
    assert!(old_ws.is_none());
    let renamed_ws = client
        .get_workspace(GetWorkspaceRequest {
            repo_id,
            user: "test.user@example.com".to_string(),
            workspace_name: "feature/conflict-test".to_string(),
        })
        .await
        .unwrap()
        .into_inner()
        .workspace
        .expect("slash-named workspace should persist");
    assert_eq!(renamed_ws.tree_ids, ws_state.tree_ids);
    assert_eq!(renamed_ws.conflict_labels, ws_state.conflict_labels);
}

#[tokio::test]
async fn test_cc_workspace_resolves_repo_dir_dot_and_deep_subdirectories() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_server().await;

    let ws_dir = dir.path().join("ws_nested");
    jj_cmd(&server, dir.path(), &xdg)
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--workspace-name",
            "nested-ws",
            "--create",
            ws_dir.to_str().unwrap(),
        ])
        .assert()
        .success();

    let nested_sub = ws_dir.join("a/b");
    let deep_sub = ws_dir.join("d1/d2/d3/d4/d5/d6/d7");
    fs::create_dir_all(&nested_sub).unwrap();
    fs::create_dir_all(&deep_sub).unwrap();

    // `--repo-dir .` from a nested subdirectory resolves against cwd() and finds the active
    // non-default workspace name (`nested-ws`).
    jj_cmd(&server, &nested_sub, &xdg)
        .args(["cc", "workspace", "list", "--repo-dir", "."])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "nested-ws (ws.tester@example.com)",
        ));

    jj_cmd(&server, &nested_sub, &xdg)
        .args(["cc", "workspace", "status", "--repo-dir", "."])
        .assert()
        .success()
        .stdout(predicate::str::contains("Workspace: nested-ws"));

    // A deep (> 5 levels) subdirectory path works both via `--repo-dir .` and explicit path
    // from outside the repository.
    jj_cmd(&server, &deep_sub, &xdg)
        .args(["cc", "workspace", "status", "--repo-dir", "."])
        .assert()
        .success()
        .stdout(predicate::str::contains("Workspace: nested-ws"));

    jj_cmd(&server, dir.path(), &xdg)
        .args([
            "cc",
            "workspace",
            "list",
            "--repo-dir",
            deep_sub.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "nested-ws (ws.tester@example.com)",
        ));

    jj_cmd(&server, dir.path(), &xdg)
        .args([
            "cc",
            "workspace",
            "status",
            "--repo-dir",
            deep_sub.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Workspace: nested-ws"));
}

#[tokio::test]
async fn test_cc_workspace_rejects_traversal_and_nonexistent_repo_dir() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_server().await;

    let ws_dir = dir.path().join("ws_main");
    jj_cmd(&server, dir.path(), &xdg)
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--create",
            ws_dir.to_str().unwrap(),
        ])
        .assert()
        .success();

    let sub_dir = ws_dir.join("sub");
    let not_a_repo = dir.path().join("not_a_repo");
    fs::create_dir_all(&sub_dir).unwrap();
    fs::create_dir_all(&not_a_repo).unwrap();

    // From `ws_main/sub`, `../../not_a_repo` escapes outside `ws_main` to an existing non-repo
    // directory, and `../../does_not_exist` does not exist at all. Neither may lexically walk
    // back into `ws_main` or affect the CWD workspace.
    for (bad_dir, expected_err) in [
        ("../../not_a_repo", "Failed to load Commit Cloud config"),
        (
            "../../does_not_exist",
            "Failed to access repository directory",
        ),
    ] {
        jj_cmd(&server, &sub_dir, &xdg)
            .args(["cc", "workspace", "list", "--repo-dir", bad_dir])
            .assert()
            .failure()
            .stderr(predicate::str::contains(expected_err));

        jj_cmd(&server, &sub_dir, &xdg)
            .args(["cc", "workspace", "status", "--repo-dir", bad_dir])
            .assert()
            .failure()
            .stderr(predicate::str::contains(expected_err));

        jj_cmd(&server, &sub_dir, &xdg)
            .args([
                "cc",
                "workspace",
                "delete",
                "default",
                "--repo-dir",
                bad_dir,
            ])
            .assert()
            .failure()
            .stderr(predicate::str::contains(expected_err));
    }

    // The CWD Commit Cloud workspace must remain registered and untouched.
    jj_cmd(&server, &sub_dir, &xdg)
        .args(["cc", "workspace", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("default (ws.tester@example.com)"));

    jj_cmd(&server, &sub_dir, &xdg)
        .args(["cc", "workspace", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Workspace: default"));
}

#[tokio::test]
async fn test_working_copy_fails_without_user_identity() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_server().await;

    // 1. Missing user.email with empty config and cleared env vars fails closed (no fallback).
    let dest_missing = dir.path().join("ws_no_identity");
    let mut cmd = server.jj_cmd();
    cmd.env("XDG_CONFIG_HOME", &xdg)
        .env_remove("JJ_USER")
        .env_remove("JJ_EMAIL")
        .env_remove("USER")
        .args([
            "cc",
            "init",
            "--create",
            "--server",
            server.url(),
            "--project-id",
            "test-project",
            "--config",
            "user.email=''",
            "--config",
            "user.name=''",
            dest_missing.to_str().unwrap(),
        ]);
    let output = cmd.assert().failure();
    let stderr = String::from_utf8_lossy(&output.get_output().stderr);
    assert!(
        stderr.contains("user.email") || stderr.contains("User email"),
        "stderr should mention missing user.email, got: {stderr}"
    );
    assert!(
        !dest_missing.exists(),
        "failed init without user.email must not leave destination behind"
    );

    // 2. Explicit `--config user.email=''` must override `JJ_EMAIL` instead of reading env directly.
    let dest_override = dir.path().join("ws_env_override");
    let mut override_cmd = server.jj_cmd();
    override_cmd
        .env("XDG_CONFIG_HOME", &xdg)
        .env("JJ_EMAIL", "env.bypass@example.com")
        .args([
            "cc",
            "init",
            "--create",
            "--server",
            server.url(),
            "--project-id",
            "test-project",
            "--config",
            "user.email=''",
            dest_override.to_str().unwrap(),
        ]);
    let override_out = override_cmd.assert().failure();
    let override_stderr = String::from_utf8_lossy(&override_out.get_output().stderr);
    assert!(
        override_stderr.contains("user.email") || override_stderr.contains("User email"),
        "explicit empty user.email config must fail closed even when JJ_EMAIL is set, got: {override_stderr}"
    );
    assert!(!dest_override.exists());

    // 3. Loading an existing commit_cloud working copy with `--config user.email=''` also fails closed.
    let ws_valid = dir.path().join("ws_valid");
    jj_cmd(&server, dir.path(), &xdg)
        .args([
            "cc",
            "init",
            "--create",
            "--server",
            server.url(),
            "--project-id",
            "test-project",
            ws_valid.to_str().unwrap(),
        ])
        .assert()
        .success();

    let load_out = jj_cmd(&server, &ws_valid, &xdg)
        .args(["--config", "user.email=''", "status"])
        .assert()
        .failure();
    let load_stderr = String::from_utf8_lossy(&load_out.get_output().stderr);
    assert!(
        load_stderr.contains("user.email") || load_stderr.contains("User email"),
        "loading commit_cloud working copy without user.email must fail closed, got: {load_stderr}"
    );
}

#[tokio::test]
async fn test_cc_workspace_status_reads_non_default_name_without_user_email() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg_configured = dir.path().join("xdg_configured");
    let xdg_empty = dir.path().join("xdg_empty");
    fs::create_dir_all(&xdg_configured).unwrap();
    fs::create_dir_all(&xdg_empty).unwrap();
    let server = testutils::spawn_sqlite_server(&dir.path().join("ws_no_email.db")).await;

    let ws_main = dir.path().join("ws_main");
    jj_cmd(&server, dir.path(), &xdg_configured)
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--project-id",
            "proj-ws-status",
            "--create",
            ws_main.to_str().unwrap(),
        ])
        .assert()
        .success();

    let cfg = CommitCloudConfig::load_from_store(&ws_main).unwrap();
    let ws_clone = dir.path().join("ws_clone");
    jj_cmd(&server, dir.path(), &xdg_configured)
        .args([
            "cc",
            "clone",
            "--server",
            server.url(),
            "--workspace-name",
            "ws-1234",
            &cfg.repo_id,
            ws_clone.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Run `jj cc workspace status --user <email>` inside the non-default workspace with an empty
    // XDG_CONFIG_HOME and no `JJ_EMAIL`/`user.email`: must resolve `ws-1234` from `.jj/working_copy`
    // metadata rather than silently falling back to `"default"`.
    let mut cmd = server.jj_cmd();
    cmd.current_dir(&ws_clone)
        .env("XDG_CONFIG_HOME", &xdg_empty)
        .env_remove("JJ_USER")
        .env_remove("JJ_EMAIL")
        .env_remove("USER")
        .args([
            "cc",
            "workspace",
            "status",
            "--user",
            "ws.tester@example.com",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Workspace: ws-1234"))
        .stdout(predicate::str::contains("User:      ws.tester@example.com"));

    // Also verify `--repo-dir` from outside the workspace with explicit empty `user.email=''`.
    let mut repo_dir_cmd = server.jj_cmd();
    repo_dir_cmd
        .current_dir(dir.path())
        .env("XDG_CONFIG_HOME", &xdg_empty)
        .env_remove("JJ_USER")
        .env_remove("JJ_EMAIL")
        .env_remove("USER")
        .args([
            "--config",
            "user.email=''",
            "cc",
            "workspace",
            "status",
            "--user",
            "ws.tester@example.com",
            "--repo-dir",
            ws_clone.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Workspace: ws-1234"));

    // When local `.jj/working_copy/checkout` is missing, omitting `--workspace-name` must surface
    // the metadata read error instead of silently querying `"default"`.
    fs::remove_file(ws_clone.join(".jj/working_copy/checkout")).unwrap();
    let mut broken_cmd = server.jj_cmd();
    broken_cmd
        .current_dir(&ws_clone)
        .env("XDG_CONFIG_HOME", &xdg_empty)
        .env_remove("JJ_USER")
        .env_remove("JJ_EMAIL")
        .env_remove("USER")
        .args([
            "cc",
            "workspace",
            "status",
            "--user",
            "ws.tester@example.com",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Failed to read local workspace metadata",
        ));
}
