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

#![allow(deprecated)]

use cc_common::workspace::GetWorkspaceRequest;
use cc_common::workspace::workspace_service_client::WorkspaceServiceClient;
use std::fs;
use testutils::TestWorkspace;

#[tokio::test]
async fn test_cc_init_working_copy_type_returns_commit_cloud() {
    let ws = TestWorkspace::init().await;

    let working_copy_type_path = ws.repo_path().join(".jj/working_copy/type");
    assert!(
        working_copy_type_path.exists(),
        "Working copy type file should exist at .jj/working_copy/type"
    );

    let working_copy_type = fs::read_to_string(&working_copy_type_path).unwrap();
    assert_eq!(
        working_copy_type.trim(),
        "commit_cloud",
        "Working copy type should be 'commit_cloud'"
    );
}

#[tokio::test]
async fn test_cc_init_working_copy_type_local() {
    let server = testutils::spawn_server().await;
    let temp_dir = tempfile::tempdir().unwrap();

    let mut cmd = server.jj_cmd();
    cmd.env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args([
            "cc",
            "init",
            "--create",
            "--server",
            server.url(),
            "--project-id",
            "test-project",
            "--working-copy-type",
            "local",
            temp_dir.path().to_str().unwrap(),
        ]);
    cmd.assert().success();

    let working_copy_type_path = temp_dir.path().join(".jj/working_copy/type");
    assert!(working_copy_type_path.exists());
    let working_copy_type = fs::read_to_string(&working_copy_type_path).unwrap();
    assert_eq!(working_copy_type.trim(), "local");

    // Verify commands like describe and log work cleanly with local working copy
    let mut describe_cmd = server.jj_cmd();
    describe_cmd
        .current_dir(temp_dir.path())
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args(["describe", "-m", "local working copy commit"])
        .assert()
        .success();

    let mut log_cmd = server.jj_cmd();
    let output = log_cmd
        .current_dir(temp_dir.path())
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args(["log"])
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&output.get_output().stdout);
    assert!(stdout.contains("local working copy commit"));
}

#[tokio::test]
async fn test_working_copy_syncs_state_to_server() {
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

    let ws_state = response
        .workspace
        .expect("Workspace should be registered on server");
    assert_eq!(ws_state.repo_id, repo_id);
    assert_eq!(ws_state.user, user);
    assert_eq!(ws_state.workspace_name, "default");
    assert!(!ws_state.operation_id.is_empty());
}

#[tokio::test]
async fn test_working_copy_snapshot_and_change_detection() {
    let ws = TestWorkspace::init().await;

    // Create a file in working directory and run describe to trigger a snapshot
    let test_file = ws.repo_path().join("hello.txt");
    fs::write(&test_file, "hello from commit cloud working copy").unwrap();

    let mut cmd = ws.jj_cmd();
    cmd.args(["describe", "-m", "add hello.txt"])
        .assert()
        .success();

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

    let ws_state = response
        .workspace
        .expect("Workspace should exist on server");
    assert!(
        !ws_state.tree_id.is_empty(),
        "Working tree ID should not be empty"
    );
    assert!(
        !ws_state.commit_id.is_empty(),
        "commit_id should not be empty"
    );
}

#[tokio::test]
async fn test_working_copy_fails_without_user_identity() {
    let server = testutils::spawn_server().await;
    let temp_dir = tempfile::tempdir().unwrap();

    let mut cmd = server.jj_cmd();
    // Intentionally pass an empty config without user.email or user.name, and clear env vars
    cmd.env_remove("JJ_USER")
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
            temp_dir.path().to_str().unwrap(),
        ]);
    let output = cmd.assert().failure();
    let stderr = String::from_utf8_lossy(&output.get_output().stderr);
    assert!(
        stderr.contains("user.email") || stderr.contains("User email"),
        "stderr should mention missing user.email, got: {stderr}"
    );
}

#[tokio::test]
#[ignore = "needs jjfsd from the jj-vfs repo and FUSE; run with JJFSD_BIN=/path/to/jjfsd cargo test -p cli --test test_working_copy test_vfs_commit_cloud_working_copy_succeeds -- --ignored"]
async fn test_vfs_commit_cloud_working_copy_succeeds() {
    // Initialize a real Commit Cloud repository with jj-cc-server
    let ws = testutils::TestWorkspace::init().await;

    // Create files and make a commit
    let cloud_file = ws.repo_path().join("cloud_file.txt");
    fs::write(&cloud_file, "hello from commit cloud vfs\n").unwrap();
    let src_dir = ws.repo_path().join("src");
    fs::create_dir_all(&src_dir).unwrap();
    let lib_file = src_dir.join("lib.rs");
    fs::write(&lib_file, "pub fn cloud_fn() {}\n").unwrap();

    let mut describe_cmd = ws.jj_cmd();
    describe_cmd
        .args(["describe", "-m", "commit cloud vfs test"])
        .assert()
        .success();

    // Retrieve the commit ID of @
    let mut log_cmd = ws.jj_cmd();
    let log_output = log_cmd
        .args(["log", "-r", "@", "-T", "commit_id", "--no-graph"])
        .assert()
        .success();
    let commit_hex = String::from_utf8_lossy(&log_output.get_output().stdout)
        .trim()
        .to_string();
    assert!(!commit_hex.is_empty(), "commit_id should not be empty");

    // Perform sparse set --clear to verify 0 source files exist on disk
    let mut sparse_cmd = ws.jj_cmd();
    sparse_cmd
        .args(["sparse", "set", "--clear"])
        .assert()
        .success();

    // Verify physical files are no longer on local disk
    assert!(
        !cloud_file.exists(),
        "cloud_file.txt should be removed from disk after sparse clear"
    );
    assert!(
        !src_dir.exists(),
        "src/ directory should be removed from disk after sparse clear"
    );

    // Mount the VFS using the shared testutils helper
    let vfs = ws.mount_vfs().await;
    let mountpoint = vfs.mountpoint();

    // Verify browsing /commits/<commit_hex>/ and reading files through VFS
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

    // Verify directory listing on /commits/<commit_hex>
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

    // Verify again that the physical local disk has 0 source files!
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
    use cc_common::workspace::{DeleteWorkspaceRequest, ListWorkspacesRequest};

    let ws = TestWorkspace::init().await;

    let config_path = ws.repo_path().join(".jj/repo/store/config.toml");
    let config_str = fs::read_to_string(&config_path).unwrap();
    let config: toml::Value = toml::from_str(&config_str).unwrap();
    let repo_id = config.get("repo_id").unwrap().as_str().unwrap().to_string();

    let mut client = WorkspaceServiceClient::connect(ws.server_url().to_string())
        .await
        .expect("Failed to connect to WorkspaceService");

    let user = "test.user@example.com".to_string();

    // Verify workspace exists
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

    // Delete workspace
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

    // Verify workspace is gone from database
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
    use cc_common::workspace::DeleteWorkspaceRequest;

    let ws = TestWorkspace::init().await;

    let config_path = ws.repo_path().join(".jj/repo/store/config.toml");
    let config_str = fs::read_to_string(&config_path).unwrap();
    let config: toml::Value = toml::from_str(&config_str).unwrap();
    let repo_id = config.get("repo_id").unwrap().as_str().unwrap().to_string();

    let mut client = WorkspaceServiceClient::connect(ws.server_url().to_string())
        .await
        .expect("Failed to connect to WorkspaceService");

    // Attempt to delete a nonexistent workspace name
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

    // Attempt to delete from a nonexistent repository
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
    use cc_common::workspace::CheckWorkingCopyChangesRequest;

    let ws = TestWorkspace::init().await;

    // Create a commit so the commit exists in the store
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

    // Query changes on a workspace after snapshot
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
    assert_eq!(response.current_tree_id, response.commit_tree_id);
}

#[tokio::test]
async fn test_check_working_copy_changes_nonexistent_workspace_fails() {
    use cc_common::workspace::CheckWorkingCopyChangesRequest;

    let ws = TestWorkspace::init().await;

    let config_path = ws.repo_path().join(".jj/repo/store/config.toml");
    let config_str = fs::read_to_string(&config_path).unwrap();
    let config: toml::Value = toml::from_str(&config_str).unwrap();
    let repo_id = config.get("repo_id").unwrap().as_str().unwrap().to_string();

    let mut client = WorkspaceServiceClient::connect(ws.server_url().to_string())
        .await
        .expect("Failed to connect to WorkspaceService");

    // Check changes for a nonexistent workspace
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

    // Verify workspace list shows default workspace
    let mut list_cmd = ws.jj_cmd();
    let output = list_cmd.args(["workspace", "list"]).assert().success();
    let stdout = String::from_utf8_lossy(&output.get_output().stdout);
    assert!(
        stdout.contains("default"),
        "Workspace list should include default"
    );

    // Forget the default workspace
    let mut forget_cmd = ws.jj_cmd();
    forget_cmd
        .args(["workspace", "forget", "default"])
        .assert()
        .success();

    // Verify workspace list is now empty or default is removed
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

    // Attempt to forget a non-existent workspace name via CLI
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
async fn test_working_copy_shared_project_sqlite_integration() {
    use cc_common::workspace::CheckWorkingCopyChangesRequest;

    let db_dir = tempfile::tempdir().unwrap();
    let db_path = db_dir.path().join("commit_cloud.db");
    let server = testutils::spawn_sqlite_server(&db_path).await;

    let dir_client_1 = tempfile::tempdir().unwrap();
    let dir_client_2 = tempfile::tempdir().unwrap();

    // Initialize client_1 and client_2 sharing the same project_id on SQLite
    let mut init_cmd_1 = server.jj_cmd();
    init_cmd_1
        .current_dir(dir_client_1.path())
        .env("JJ_USER", "Client 1")
        .env("JJ_EMAIL", "client_1@example.com")
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--project-id",
            "shared-proj",
            "--repo-id",
            "repo-client-1",
            "--create",
            ".",
        ])
        .assert()
        .success();

    let mut init_cmd_2 = server.jj_cmd();
    init_cmd_2
        .current_dir(dir_client_2.path())
        .env("JJ_USER", "Client 2")
        .env("JJ_EMAIL", "client_2@example.com")
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--project-id",
            "shared-proj",
            "--repo-id",
            "repo-client-2",
            "--create",
            ".",
        ])
        .assert()
        .success();

    // client_1 creates a file and snapshots a commit into the shared project CAS
    let file_1 = dir_client_1.path().join("shared.txt");
    fs::write(&file_1, "hello from client_1 in shared project").unwrap();

    let mut describe_cmd_1 = server.jj_cmd();
    describe_cmd_1
        .current_dir(dir_client_1.path())
        .env("JJ_USER", "Client 1")
        .env("JJ_EMAIL", "client_1@example.com")
        .args(["describe", "-m", "commit from client_1"])
        .assert()
        .success();

    let mut log_cmd_1 = server.jj_cmd();
    let log_output_1 = log_cmd_1
        .current_dir(dir_client_1.path())
        .env("JJ_USER", "Client 1")
        .env("JJ_EMAIL", "client_1@example.com")
        .args(["log", "-r", "@", "-T", "commit_id", "--no-graph"])
        .assert()
        .success();
    let client_1_commit_id = String::from_utf8_lossy(&log_output_1.get_output().stdout)
        .trim()
        .to_string();

    // client_2 checks out a new working copy commit on top of client_1's shared commit
    let mut new_cmd_2 = server.jj_cmd();
    new_cmd_2
        .current_dir(dir_client_2.path())
        .env("JJ_USER", "Client 2")
        .env("JJ_EMAIL", "client_2@example.com")
        .args([
            "new",
            &client_1_commit_id,
            "-m",
            "working copy on top of client_1",
        ])
        .assert()
        .success();

    // Verify client_2's working copy automatically materialized shared.txt on disk
    let file_2 = dir_client_2.path().join("shared.txt");
    assert!(
        file_2.exists(),
        "shared.txt from client_1 should materialize in client_2 working copy"
    );
    assert_eq!(
        fs::read_to_string(&file_2).unwrap(),
        "hello from client_1 in shared project"
    );

    // Verify WorkspaceService check_working_copy_changes resolves repo-client-2 -> shared-proj -> commits table
    let mut client = WorkspaceServiceClient::connect(server.url().to_string())
        .await
        .expect("Failed to connect to WorkspaceService");

    let change_resp = client
        .check_working_copy_changes(CheckWorkingCopyChangesRequest {
            repo_id: "repo-client-2".to_string(),
            user: "client_2@example.com".to_string(),
            workspace_name: "default".to_string(),
        })
        .await
        .expect("check_working_copy_changes should succeed for shared project commit")
        .into_inner();
    assert!(!change_resp.has_changes);
    assert_eq!(change_resp.current_tree_id, change_resp.commit_tree_id);

    // Verify SQLite database tables (projects, repos, workspaces, commits) directly
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let project_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM projects WHERE project_id = 'shared-proj'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(project_count, 1);

    let repo_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM repos WHERE project_id = 'shared-proj'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(repo_count, 2);

    let ws_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM workspaces WHERE repo_id IN ('repo-client-1', 'repo-client-2')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(ws_count, 2);
}

#[tokio::test]
async fn test_workspace_cas_conflicts_sparse_pagination_and_preconditions() {
    use cc_common::workspace::{
        CheckWorkingCopyChangesRequest, ListWorkspacesRequest, UpdateWorkspaceRequest,
        WorkspaceState,
    };

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
async fn test_working_copy_sparse_after_snapshot_rename_and_unregistered_repo_not_found() {
    use cc_common::workspace::{
        CheckWorkingCopyChangesRequest, ListWorkspacesRequest, UpdateWorkspaceRequest,
        WorkspaceState,
    };

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
