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
async fn test_cli_workflow_against_spanner_emulator_backend() {
    let Some(emulator) = testutils::SpannerEmulatorGuard::start_or_skip(
        "test_cli_workflow_against_spanner_emulator_backend",
        false,
    )
    .await
    else {
        return;
    };

    let blob_dir = tempfile::tempdir().unwrap();
    let db_uri = emulator.database_uri("cli_e2e_db");
    let db_arg = format!("--spanner-database={db_uri}");
    let blob_arg = format!("--blob-path={}", blob_dir.path().display());

    let server = testutils::spawn_server_with_env(
        &[
            ("SPANNER_EMULATOR_HOST", emulator.grpc_host_port()),
            ("SPANNER_EMULATOR_REST_HOST", emulator.rest_endpoint()),
        ],
        &[
            "--store-type=spanner",
            &db_arg,
            "--spanner-auto-migrate",
            "--blob-store=local",
            &blob_arg,
        ],
    )
    .await;

    let ws_a = testutils::TestWorkspace::init_against_server(server).await;
    fs::write(
        ws_a.repo_path().join("spanner_doc.md"),
        "# Stored in Cloud Spanner\n",
    )
    .unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink("spanner_doc.md", ws_a.repo_path().join("doc_alias.md")).unwrap();

    ws_a.jj_cmd()
        .args(["describe", "-m", "spanner-backed commit"])
        .assert()
        .success();

    let commit_id = String::from_utf8(
        ws_a.jj_cmd()
            .args(["log", "--no-graph", "-r", "@", "-T", "commit_id"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap();
    let commit_id = commit_id.trim().to_string();

    let cfg_str = fs::read_to_string(ws_a.repo_path().join(".jj/repo/store/config.toml")).unwrap();
    let parsed_cfg: toml::Value = toml::from_str(&cfg_str).unwrap();
    let repo_id = parsed_cfg["repo_id"].as_str().unwrap();

    let ws_b_dir = tempfile::tempdir().unwrap();
    ws_a.server()
        .jj_cmd()
        .current_dir(ws_b_dir.path())
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args([
            "cc",
            "init",
            "--server",
            ws_a.server_url(),
            "--repo-id",
            repo_id,
            "--workspace-name",
            "spanner-ws-b",
            ".",
        ])
        .assert()
        .success();

    ws_a.server()
        .jj_cmd()
        .current_dir(ws_b_dir.path())
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args(["new", &commit_id])
        .assert()
        .success();

    assert_eq!(
        fs::read_to_string(ws_b_dir.path().join("spanner_doc.md")).unwrap(),
        "# Stored in Cloud Spanner\n"
    );
    #[cfg(unix)]
    assert_eq!(
        fs::read_link(ws_b_dir.path().join("doc_alias.md"))
            .unwrap()
            .to_str(),
        Some("spanner_doc.md")
    );
}
