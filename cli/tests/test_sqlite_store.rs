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
async fn test_sqlite_store_init_and_snapshot_succeeds() {
    let db_dir = tempfile::tempdir().expect("Failed to create temp dir for sqlite db");
    let db_path = db_dir.path().join("test_store.db");

    let workspace = testutils::TestWorkspace::init_sqlite_at(&db_path).await;
    let repo_path = workspace.repo_path();

    fs::write(repo_path.join("sqlite_test.txt"), "hello sqlite store!\n").unwrap();

    workspace
        .jj_cmd()
        .args(["describe", "-m", "sqlite store test commit"])
        .assert()
        .success();

    workspace
        .jj_cmd()
        .args(["log", "--no-graph", "-r", "@", "-T", "description"])
        .assert()
        .success()
        .stdout("sqlite store test commit\n");
}

#[tokio::test]
async fn test_sqlite_and_local_blobs_persist_across_server_restarts() {
    let db_dir = tempfile::tempdir().unwrap();
    let db_path = db_dir.path().join("restart_test.db");

    let server_1 = testutils::spawn_sqlite_server(&db_path).await;
    let ws_dir = tempfile::tempdir().unwrap();
    server_1
        .jj_cmd()
        .current_dir(ws_dir.path())
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args(["cc", "init", "--server", server_1.url(), "--create", "."])
        .assert()
        .success();

    fs::write(
        ws_dir.path().join("durable.txt"),
        "persisted payload across restart\n",
    )
    .unwrap();
    server_1
        .jj_cmd()
        .current_dir(ws_dir.path())
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args(["describe", "-m", "durable commit before restart"])
        .assert()
        .success();

    let commit_id = String::from_utf8(
        server_1
            .jj_cmd()
            .current_dir(ws_dir.path())
            .env("JJ_USER", "Test User")
            .env("JJ_EMAIL", "test.user@example.com")
            .args(["log", "--no-graph", "-r", "@", "-T", "commit_id"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap();
    let commit_id = commit_id.trim().to_string();

    let config_path = ws_dir.path().join(".jj/repo/store/config.toml");
    let mut parsed_cfg: toml::Value =
        toml::from_str(&fs::read_to_string(&config_path).unwrap()).unwrap();
    let repo_id = parsed_cfg["repo_id"].as_str().unwrap().to_string();

    // Shut down server 1 and launch a fresh server 2 pointing at the same SQLite path.
    drop(server_1);
    let server_2 = testutils::spawn_sqlite_server(&db_path).await;

    parsed_cfg["server_url"] = toml::Value::String(server_2.url().to_string());
    fs::write(&config_path, toml::to_string_pretty(&parsed_cfg).unwrap()).unwrap();

    // A new workspace attached after restart still reads the persisted commit and blob.
    let ws_2_dir = tempfile::tempdir().unwrap();
    server_2
        .jj_cmd()
        .current_dir(ws_2_dir.path())
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args([
            "cc",
            "init",
            "--server",
            server_2.url(),
            "--repo-id",
            &repo_id,
            "--workspace-name",
            "after-restart",
            ".",
        ])
        .assert()
        .success();

    server_2
        .jj_cmd()
        .current_dir(ws_2_dir.path())
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args(["new", &commit_id])
        .assert()
        .success();

    assert_eq!(
        fs::read_to_string(ws_2_dir.path().join("durable.txt")).unwrap(),
        "persisted payload across restart\n"
    );
}

#[test]
fn test_server_fails_when_database_parent_path_is_not_a_directory() {
    let temp_dir = tempfile::tempdir().unwrap();
    let blocker_file = temp_dir.path().join("blocking_file");
    fs::write(&blocker_file, "blocking content").unwrap();

    let invalid_db_path = blocker_file.join("store.db");
    let expected_io_err = fs::create_dir_all(&blocker_file).unwrap_err();
    let expected_stderr = format!(
        "Error: storage backend error: failed to create SQLite parent directory {}: {}\n",
        blocker_file.display(),
        expected_io_err
    );

    let mut cmd = assert_cmd::Command::new(testutils::server_binary_path());
    cmd.args([
        "--port=0",
        "--store-type=sqlite",
        &format!("--sqlite-path={}", invalid_db_path.display()),
    ]);

    cmd.assert()
        .failure()
        .stderr(predicates::ord::eq(expected_stderr.as_str()));
}

#[test]
fn test_server_fails_when_home_directory_is_unwritable_for_default_sqlite_path() {
    let temp_dir = tempfile::tempdir().unwrap();
    let blocker_file = temp_dir.path().join("fake_home_file");
    fs::write(&blocker_file, "blocking").unwrap();

    let expected_default_dir = blocker_file.join(".jj-cc-server");
    let expected_io_err = fs::create_dir_all(&expected_default_dir).unwrap_err();
    let expected_stderr = format!(
        "Error: storage backend error: failed to create SQLite parent directory {}: {}\n",
        expected_default_dir.display(),
        expected_io_err
    );

    let mut cmd = assert_cmd::Command::new(testutils::server_binary_path());
    cmd.env("HOME", blocker_file.to_str().unwrap());
    cmd.args(["--port=0", "--store-type=sqlite"]);

    cmd.assert()
        .failure()
        .stderr(predicates::ord::eq(expected_stderr.as_str()));
}
