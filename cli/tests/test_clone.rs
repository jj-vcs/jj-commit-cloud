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

use std::fs;

#[tokio::test]
async fn test_cc_clone_checks_out_latest_and_historical_revisions() {
    let ws_a = testutils::TestWorkspace::init().await;
    let config_toml =
        fs::read_to_string(ws_a.repo_path().join(".jj/repo/store/config.toml")).unwrap();
    let parsed: toml::Value = toml::from_str(&config_toml).unwrap();
    let repo_id = parsed["repo_id"].as_str().unwrap().to_string();

    fs::write(ws_a.repo_path().join("version.txt"), "v1\n").unwrap();
    ws_a.jj_cmd()
        .args(["describe", "-m", "release v1"])
        .assert()
        .success();
    let v1_commit = String::from_utf8(
        ws_a.jj_cmd()
            .args(["log", "--no-graph", "-r", "@", "-T", "commit_id"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap();
    let v1_commit = v1_commit.trim().to_string();

    // Include a 2 MiB file to verify default snapshot limits without --config snapshot.max-new-file-size.
    ws_a.jj_cmd().args(["new"]).assert().success();
    fs::write(ws_a.repo_path().join("version.txt"), "v2\n").unwrap();
    let two_mib = vec![0x5au8; 2 * 1024 * 1024];
    fs::write(ws_a.repo_path().join("bundle.bin"), &two_mib).unwrap();
    ws_a.jj_cmd()
        .args(["describe", "-m", "release v2"])
        .assert()
        .success();

    // Default `jj cc clone` materializes the latest head (v2) immediately on disk.
    let clone_parent = tempfile::tempdir().unwrap();
    let clone_latest = clone_parent.path().join("clone-latest");
    ws_a.server()
        .jj_cmd()
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args([
            "cc",
            "clone",
            "--server",
            ws_a.server_url(),
            &repo_id,
            clone_latest.to_str().unwrap(),
        ])
        .assert()
        .success();

    assert_eq!(
        fs::read_to_string(clone_latest.join("version.txt")).unwrap(),
        "v2\n"
    );
    assert_eq!(
        fs::read(clone_latest.join("bundle.bin")).unwrap().len(),
        two_mib.len()
    );

    // `jj cc clone -r <v1>` checks out historical revision v1 directly on clone.
    let clone_v1 = clone_parent.path().join("clone-v1");
    ws_a.server()
        .jj_cmd()
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args([
            "cc",
            "clone",
            "--server",
            ws_a.server_url(),
            "-r",
            &v1_commit,
            &repo_id,
            clone_v1.to_str().unwrap(),
        ])
        .assert()
        .success();

    assert_eq!(
        fs::read_to_string(clone_v1.join("version.txt")).unwrap(),
        "v1\n"
    );
    assert!(!clone_v1.join("bundle.bin").exists());

    // `jj cc clone -r nonexistent_rev` fails without panicking, removes the
    // destination directory, and forgets the temporary workspace from the shared view.
    let clone_bad = clone_parent.path().join("clone-bad");
    let bad_out = ws_a
        .server()
        .jj_cmd()
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args([
            "cc",
            "clone",
            "--server",
            ws_a.server_url(),
            "--workspace-name",
            "ws-failed-clone",
            "-r",
            "nonexistent_rev_for_test",
            &repo_id,
            clone_bad.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!bad_out.status.success());
    let stderr = String::from_utf8_lossy(&bad_out.stderr);
    assert!(
        !stderr.contains("panicked"),
        "failed clone must not panic: {stderr}"
    );
    assert!(!clone_bad.exists(), "failed clone must remove destination");

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
        !ws_list.contains("ws-failed-clone"),
        "failed clone workspace must not leak into shared view: {ws_list}"
    );
}

#[tokio::test]
async fn test_cc_clone_rejects_colliding_workspace_name_and_verifies_project_id() {
    let ws_a = testutils::TestWorkspace::init().await;
    let config_toml =
        fs::read_to_string(ws_a.repo_path().join(".jj/repo/store/config.toml")).unwrap();
    let parsed: toml::Value = toml::from_str(&config_toml).unwrap();
    let repo_id = parsed["repo_id"].as_str().unwrap().to_string();

    fs::write(ws_a.repo_path().join("keep.txt"), "existing workspace\n").unwrap();
    ws_a.jj_cmd()
        .args(["describe", "-m", "existing default commit"])
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

    let clone_parent = tempfile::tempdir().unwrap();

    // Cloning with a colliding `--workspace-name default` must fail without creating the
    // destination or clobbering the existing `default` workspace remotely.
    let collide_dest = clone_parent.path().join("clone-collide");
    let collide_out = ws_a
        .server()
        .jj_cmd()
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args([
            "cc",
            "clone",
            "--server",
            ws_a.server_url(),
            "--workspace-name",
            "default",
            &repo_id,
            collide_dest.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!collide_out.status.success());
    let collide_stderr = String::from_utf8_lossy(&collide_out.stderr);
    assert!(
        collide_stderr.contains("already exists") && collide_stderr.contains("--workspace-name"),
        "expected actionable collision error, got: {collide_stderr}"
    );
    assert!(
        !collide_dest.exists(),
        "colliding clone must not leave destination directory behind"
    );

    // Existing `default` workspace must remain intact in both the shared view and WorkspaceService.
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
            .any(|line| line.starts_with("default:") || line.starts_with("default (")),
        "existing default workspace must survive rejected colliding clone: {ws_list}"
    );
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
        "existing default workspace commit must stay intact: {status_out}"
    );

    // `--project-id` mismatch must fail closed without creating destination.
    let bad_proj_dest = clone_parent.path().join("clone-bad-proj");
    ws_a.server()
        .jj_cmd()
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args([
            "cc",
            "clone",
            "--server",
            ws_a.server_url(),
            "--project-id",
            "wrong-project",
            &repo_id,
            bad_proj_dest.to_str().unwrap(),
        ])
        .assert()
        .failure();
    assert!(!bad_proj_dest.exists());

    // Matching `--project-id test-project` succeeds and materializes files.
    let good_proj_dest = clone_parent.path().join("clone-good-proj");
    ws_a.server()
        .jj_cmd()
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args([
            "cc",
            "clone",
            "--server",
            ws_a.server_url(),
            "--project-id",
            "test-project",
            &repo_id,
            good_proj_dest.to_str().unwrap(),
        ])
        .assert()
        .success();
    assert_eq!(
        fs::read_to_string(good_proj_dest.join("keep.txt")).unwrap(),
        "existing workspace\n"
    );
}
