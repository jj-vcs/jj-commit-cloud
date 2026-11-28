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
use std::time::Duration;

#[tokio::test]
async fn test_multiregion_cli_checkout_recovers_from_lagging_blob_replica_and_repairs_local() {
    let Some(emulator) = testutils::SpannerEmulatorGuard::start_or_skip(
        "test_multiregion_cli_checkout_recovers_from_lagging_blob_replica_and_repairs_local",
        false,
    )
    .await
    else {
        return;
    };

    let uc1_blob_dir = tempfile::tempdir().unwrap();
    let ew1_blob_dir = tempfile::tempdir().unwrap();
    let db_uri = emulator.database_uri("multiregion_cli_db");
    let db_arg = format!("--spanner-database={db_uri}");

    // Region A (`us-central1`) server: writes blobs only to uc1_blob_dir (no eager fanout to ew1).
    let uc1_path_arg = format!("--blob-path={}", uc1_blob_dir.path().display());
    let server_us = testutils::spawn_server_with_env(
        &[
            ("SPANNER_EMULATOR_HOST", emulator.grpc_host_port()),
            ("SPANNER_EMULATOR_REST_HOST", emulator.rest_endpoint()),
        ],
        &[
            "--store-type=spanner",
            &db_arg,
            "--spanner-auto-migrate",
            "--blob-store=local",
            &uc1_path_arg,
            "--blob-location-id=gs://jj-blobs-uc1",
        ],
    )
    .await;

    // Region B (`europe-west1`) server: shares the global Spanner database, reads from empty
    // ew1_blob_dir first, and falls back to gs://jj-blobs-uc1 on cross-region replication lag.
    let ew1_path_arg = format!("--blob-path={}", ew1_blob_dir.path().display());
    let peer_arg = format!(
        "--local-peer-blob-dirs=gs://jj-blobs-uc1={}",
        uc1_blob_dir.path().display()
    );
    let server_eu = testutils::spawn_server_with_env(
        &[
            ("SPANNER_EMULATOR_HOST", emulator.grpc_host_port()),
            ("SPANNER_EMULATOR_REST_HOST", emulator.rest_endpoint()),
        ],
        &[
            "--store-type=spanner",
            &db_arg,
            "--spanner-auto-migrate",
            "--blob-store=local",
            &ew1_path_arg,
            "--blob-location-id=gs://jj-blobs-ew1",
            &peer_arg,
        ],
    )
    .await;

    // Agent 1 in us-central1 initializes the repo and snapshots a text file + 1.5 MiB binary file.
    let ws_us = testutils::TestWorkspace::init_against_server(server_us).await;
    fs::write(
        ws_us.repo_path().join("src_lib.rs"),
        "pub fn region() -> &'static str { \"us-central1\" }\n",
    )
    .unwrap();
    let binary_payload: Vec<u8> = (0..(1536 * 1024))
        .map(|i| ((i * 19 + 3) & 0xff) as u8)
        .collect();
    fs::write(ws_us.repo_path().join("model_weights.bin"), &binary_payload).unwrap();

    ws_us
        .jj_cmd()
        .args([
            "--config",
            "snapshot.max-new-file-size=4MiB",
            "describe",
            "-m",
            "commit in us-central1 before ew1 blob replication",
        ])
        .assert()
        .success();

    let commit_us = String::from_utf8(
        ws_us
            .jj_cmd()
            .args(["log", "--no-graph", "-r", "@", "-T", "commit_id"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap();
    let commit_us = commit_us.trim().to_string();

    let cfg_str = fs::read_to_string(ws_us.repo_path().join(".jj/repo/store/config.toml")).unwrap();
    let parsed_cfg: toml::Value = toml::from_str(&cfg_str).unwrap();
    let repo_id = parsed_cfg["repo_id"].as_str().unwrap();

    // europe-west1's local blob store must still be empty before Agent 2 checks out.
    let ew1_entries_before: usize = fs::read_dir(ew1_blob_dir.path().join("blobs"))
        .unwrap()
        .count();
    assert_eq!(ew1_entries_before, 0);

    // Agent 2 in europe-west1 attaches immediately to server_eu and checks out commit_us.
    let ws_eu_dir = tempfile::tempdir().unwrap();
    server_eu
        .jj_cmd()
        .current_dir(ws_eu_dir.path())
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args([
            "cc",
            "init",
            "--server",
            server_eu.url(),
            "--repo-id",
            repo_id,
            "--workspace-name",
            "eu-agent",
            ".",
        ])
        .assert()
        .success();

    server_eu
        .jj_cmd()
        .current_dir(ws_eu_dir.path())
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args(["new", &commit_us])
        .assert()
        .success();

    // Both files hydrated in europe-west1 via Spanner origin fallback.
    assert_eq!(
        fs::read_to_string(ws_eu_dir.path().join("src_lib.rs")).unwrap(),
        "pub fn region() -> &'static str { \"us-central1\" }\n"
    );
    assert_eq!(
        fs::read(ws_eu_dir.path().join("model_weights.bin")).unwrap(),
        binary_payload
    );

    // Background read-repair populates europe-west1's local blob store.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let repaired_shards = fs::read_dir(ew1_blob_dir.path().join("blobs"))
            .unwrap()
            .count();
        if repaired_shards >= 2 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "expected read-repair to populate europe-west1 blob shards"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
