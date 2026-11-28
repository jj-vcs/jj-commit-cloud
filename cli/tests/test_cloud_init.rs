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

#[tokio::test]
async fn test_cc_init_creates_local_workspace() {
    let workspace = testutils::TestWorkspace::init().await;
    let repo_path = workspace.repo_path();

    let jj_store_path = repo_path.join(".jj/repo/store");
    assert!(
        jj_store_path.exists(),
        "The .jj/repo/store directory should exist"
    );

    let store_type = fs::read_to_string(jj_store_path.join("type"))
        .expect("The store type file should be readable");
    assert_eq!(store_type, "commit_cloud");

    let index_type = fs::read_to_string(repo_path.join(".jj/repo/index/type"))
        .expect("The index type file should be readable");
    assert_eq!(index_type, "commit_cloud");

    let config_content = fs::read_to_string(jj_store_path.join("config.toml"))
        .expect("The config.toml file should be readable");

    let parsed_config: toml::Value =
        toml::from_str(&config_content).expect("The config.toml file should be valid TOML");

    let server_url = parsed_config
        .get("server_url")
        .and_then(|v| v.as_str())
        .expect("The server_url field should exist and be a string");
    assert_eq!(server_url, workspace.server_url());

    let repo_id_str = parsed_config
        .get("repo_id")
        .and_then(|v| v.as_str())
        .expect("The repo_id field should exist and be a string");

    uuid::Uuid::parse_str(repo_id_str).expect("The repo_id should be a valid UUID string");

    let project_id_str = parsed_config
        .get("project_id")
        .and_then(|v| v.as_str())
        .expect("The project_id field should exist and be a string");
    assert_eq!(project_id_str, "test-project");
}

#[tokio::test]
async fn test_cc_init_registers_repository() {
    let workspace = testutils::TestWorkspace::init().await;
    let repo_path = workspace.repo_path();

    let jj_store_path = repo_path.join(".jj/repo/store");
    let config_content = fs::read_to_string(jj_store_path.join("config.toml"))
        .expect("The config.toml file should be readable");
    let parsed_config: toml::Value =
        toml::from_str(&config_content).expect("The config.toml file should be valid TOML");
    let repo_id_str = parsed_config
        .get("repo_id")
        .and_then(|v| v.as_str())
        .expect("The repo_id field should exist and be a string");

    let mut client = cc_common::backend::backend_service_client::BackendServiceClient::connect(
        workspace.server_url().to_string(),
    )
    .await
    .expect("gRPC connection to test server should have succeeded");

    let request = tonic::Request::new(cc_common::backend::ReadCommitRequest {
        project_id: repo_id_str.to_string(),
        commit_id: vec![1u8; cc_common::COMMIT_ID_LENGTH], // Dummy commit ID
    });

    let err = client.read_commit(request).await.unwrap_err();
    assert_eq!(
        err.message(),
        "commit should have been present in cloud database",
        "Repository was not registered in the cloud server!"
    );
}

#[tokio::test]
async fn test_cc_init_fails_on_invalid_server_addr() {
    let daemons = testutils::DaemonSandbox::new();
    let mut cmd = daemons.jj_cmd();

    cmd.env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args([
            "cc",
            "init",
            "--server",
            "http://invalid-server-domain-does-not-exist:9999",
            "--create",
            "/invalid_path_dest_dir",
        ]);

    cmd.assert().failure();
}

#[tokio::test]
async fn test_cc_init_rejects_colliding_workspace_name_on_attach_and_create() {
    let ws_a = testutils::TestWorkspace::init().await;
    let config_content =
        fs::read_to_string(ws_a.repo_path().join(".jj/repo/store/config.toml")).unwrap();
    let parsed_config: toml::Value = toml::from_str(&config_content).unwrap();
    let repo_id = parsed_config["repo_id"].as_str().unwrap().to_string();

    fs::write(ws_a.repo_path().join("sentinel.txt"), "keep default\n").unwrap();
    ws_a.jj_cmd()
        .args(["describe", "-m", "default workspace commit"])
        .assert()
        .success();
    let orig_commit = String::from_utf8(
        ws_a.jj_cmd()
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

    let parent = tempfile::tempdir().unwrap();

    // Attaching with a colliding `--workspace-name default` (default flag value) must fail
    // without creating destination directories or clobbering the existing workspace.
    let attach_dest = parent.path().join("attach-collide");
    let attach_out = ws_a
        .server()
        .jj_cmd()
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args([
            "cc",
            "init",
            "--server",
            ws_a.server_url(),
            "--repo-id",
            &repo_id,
            attach_dest.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!attach_out.status.success());
    let attach_stderr = String::from_utf8_lossy(&attach_out.stderr);
    assert!(
        attach_stderr.contains("already exists") && attach_stderr.contains("--workspace-name"),
        "expected actionable collision error on attach, got: {attach_stderr}"
    );
    assert!(
        !attach_dest.exists(),
        "colliding attach init must not leave destination directory behind"
    );

    // Passing `--create --repo-id <existing>` with colliding `--workspace-name default`
    // must also fail with the same collision error.
    let create_dest = parent.path().join("create-collide");
    let create_out = ws_a
        .server()
        .jj_cmd()
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args([
            "cc",
            "init",
            "--server",
            ws_a.server_url(),
            "--create",
            "--repo-id",
            &repo_id,
            "--project-id",
            "test-project",
            "--workspace-name",
            "default",
            create_dest.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!create_out.status.success());
    let create_stderr = String::from_utf8_lossy(&create_out.stderr);
    assert!(
        create_stderr.contains("already exists") && create_stderr.contains("--workspace-name"),
        "expected actionable collision error on --create --repo-id, got: {create_stderr}"
    );
    assert!(
        !create_dest.exists(),
        "colliding --create --repo-id init must not leave destination directory behind"
    );

    // Existing `default` workspace remains intact in both the shared view and WorkspaceService.
    let status_out = String::from_utf8(
        ws_a.jj_cmd()
            .args(["cc", "workspace", "status", "--workspace-name", "default"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap();
    assert!(
        status_out.contains(&orig_commit),
        "existing default workspace must stay intact: {status_out}"
    );

    // `--create --repo-id <brand-new>` treats remote NotFound as absent and succeeds.
    let brand_new_dest = parent.path().join("brand-new-repo");
    ws_a.server()
        .jj_cmd()
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args([
            "cc",
            "init",
            "--server",
            ws_a.server_url(),
            "--create",
            "--repo-id",
            "explicit-new-repo-id",
            "--project-id",
            "test-project",
            brand_new_dest.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Attaching with a unique `--workspace-name` succeeds.
    let unique_dest = parent.path().join("unique-ws");
    ws_a.server()
        .jj_cmd()
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args([
            "cc",
            "init",
            "--server",
            ws_a.server_url(),
            "--repo-id",
            &repo_id,
            "--workspace-name",
            "unique-ws",
            unique_dest.to_str().unwrap(),
        ])
        .assert()
        .success();
}

#[tokio::test]
async fn test_cc_init_rolls_back_workspace_on_index_commit_failure() {
    let ws_a = testutils::TestWorkspace::init().await;
    let config_content =
        fs::read_to_string(ws_a.repo_path().join(".jj/repo/store/config.toml")).unwrap();
    let parsed_config: toml::Value = toml::from_str(&config_content).unwrap();
    let repo_id = parsed_config["repo_id"].as_str().unwrap().to_string();

    let parent = tempfile::tempdir().unwrap();
    let failed_dest = parent.path().join("failed-index-init");
    let missing_commit_hex = "1111111111111111111111111111111111111111";

    ws_a.server()
        .jj_cmd()
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args([
            "cc",
            "init",
            "--server",
            ws_a.server_url(),
            "--repo-id",
            &repo_id,
            "--workspace-name",
            "ws-failed-index",
            "--index-commit",
            missing_commit_hex,
            failed_dest.to_str().unwrap(),
        ])
        .assert()
        .failure();

    assert!(
        !failed_dest.exists(),
        "failed --index-commit init must remove newly created destination directory"
    );

    // Rolled-back workspace must be absent from both the shared operation view and WorkspaceService.
    let ws_list = String::from_utf8(
        ws_a.jj_cmd()
            .args(["workspace", "list"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap();
    assert!(
        ws_list
            .lines()
            .any(|line| line.starts_with("default:") || line.starts_with("default ("))
            && !ws_list.contains("ws-failed-index"),
        "failed init workspace must be rolled back from shared view: {ws_list}"
    );

    let cc_ws_list = String::from_utf8(
        ws_a.jj_cmd()
            .args(["cc", "workspace", "list"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap();
    assert!(
        cc_ws_list.contains("default (") && !cc_ws_list.contains("ws-failed-index"),
        "failed init workspace must be deleted from WorkspaceService: {cc_ws_list}"
    );
}
