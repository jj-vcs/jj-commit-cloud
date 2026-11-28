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
async fn test_large_file_streaming_exceeds_default_grpc_limit() {
    let workspace = testutils::TestWorkspace::init().await;
    let repo_path = workspace.repo_path();

    // 5.25 MiB payload exceeds Tonic's default 4 MiB unary gRPC message limit.
    let payload_len = 5 * 1024 * 1024 + 256 * 1024;
    let mut payload = Vec::with_capacity(payload_len);
    for i in 0..payload_len {
        payload.push(((i * 31 + 17) & 0xff) as u8);
    }
    fs::write(repo_path.join("large_asset.bin"), &payload).unwrap();

    workspace
        .jj_cmd()
        .args([
            "--config",
            "snapshot.max-new-file-size=16MiB",
            "describe",
            "-m",
            "add 5.25 MiB binary asset",
        ])
        .assert()
        .success();

    workspace.jj_cmd().args(["new"]).assert().success();

    let output = workspace
        .jj_cmd()
        .args(["file", "show", "-r", "@-", "large_asset.bin"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    assert_eq!(output.len(), payload.len());
    assert_eq!(output, payload);
}

#[cfg(unix)]
#[tokio::test]
async fn test_symlink_roundtrip_and_op_prefix_resolution() {
    let workspace = testutils::TestWorkspace::init().await;
    let repo_path = workspace.repo_path();

    fs::write(repo_path.join("target.txt"), "symlink target body\n").unwrap();
    std::os::unix::fs::symlink("target.txt", repo_path.join("pointer_link")).unwrap();

    workspace
        .jj_cmd()
        .args(["describe", "-m", "add symlink"])
        .assert()
        .success();

    workspace.jj_cmd().args(["new"]).assert().success();
    let link_target = fs::read_link(repo_path.join("pointer_link")).unwrap();
    assert_eq!(link_target.to_str(), Some("target.txt"));

    // Resolve a short 8-char operation ID prefix via `jj op show <prefix>`.
    let op_id_out = workspace
        .jj_cmd()
        .args(["op", "log", "--no-graph", "-n", "1", "-T", "id.short(8)"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let short_prefix = String::from_utf8(op_id_out).unwrap();
    let short_prefix = short_prefix.trim();
    assert_eq!(short_prefix.len(), 8);

    workspace
        .jj_cmd()
        .args(["op", "show", short_prefix, "--no-op-diff"])
        .assert()
        .success();
}

#[tokio::test]
async fn test_second_workspace_attaches_to_existing_repo_id_and_hydrates_files() {
    let ws_a = testutils::TestWorkspace::init().await;
    let config_toml =
        fs::read_to_string(ws_a.repo_path().join(".jj/repo/store/config.toml")).unwrap();
    let parsed: toml::Value = toml::from_str(&config_toml).unwrap();
    let repo_id = parsed["repo_id"].as_str().unwrap().to_string();

    fs::write(ws_a.repo_path().join("shared.txt"), "written in region A\n").unwrap();
    ws_a.jj_cmd()
        .args(["describe", "-m", "commit from workspace A"])
        .assert()
        .success();
    let commit_a = String::from_utf8(
        ws_a.jj_cmd()
            .args(["log", "--no-graph", "-r", "@", "-T", "commit_id"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap();
    let commit_a = commit_a.trim();

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
            &repo_id,
            "--workspace-name",
            "workspace-b",
            ".",
        ])
        .assert()
        .success();

    // Workspace B reconciles operation heads and checks out a new change atop Workspace A's commit.
    ws_a.server()
        .jj_cmd()
        .current_dir(ws_b_dir.path())
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args(["new", commit_a])
        .assert()
        .success();

    let hydrated = fs::read_to_string(ws_b_dir.path().join("shared.txt")).unwrap();
    assert_eq!(hydrated, "written in region A\n");
}

#[tokio::test]
async fn test_backend_prefix_resolution_tree_pagination_file_sizes_and_commit_signing() {
    use cc_common::backend::*;
    use jj_lib::backend::{Backend, Commit, CommitId, MillisSinceEpoch, Signature, Timestamp};
    use jj_lib::merge::Merge;
    use jj_lib::object_id::ObjectId;

    let ws = testutils::TestWorkspace::init().await;
    let repo_path = ws.repo_path();

    for (name, body) in [
        ("a.txt", "alpha"),
        ("b.txt", "bravo-two"),
        ("c.txt", "charlie-three"),
        ("d.txt", "delta-four-bytes"),
    ] {
        fs::write(repo_path.join(name), body).unwrap();
    }
    ws.jj_cmd()
        .args(["commit", "-m", "four files for pagination and prefix tests"])
        .assert()
        .success();

    let commit_hex = String::from_utf8(
        ws.jj_cmd()
            .args(["log", "--no-graph", "-r", "@-", "-T", "commit_id"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap()
    .trim()
    .to_string();
    let change_zk = String::from_utf8(
        ws.jj_cmd()
            .args(["log", "--no-graph", "-r", "@-", "-T", "change_id.short(8)"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap()
    .trim()
    .to_string();

    let cfg = cc_lib::util::CommitCloudConfig::load_from_store(&repo_path.join(".jj/repo/store"))
        .unwrap();
    let server_url = cfg.server_url.clone();
    let repo_id = cfg.repo_id.clone();
    let commit_bytes = CommitId::try_from_hex(&commit_hex).unwrap().to_bytes();
    let commit_prefix = commit_hex[..8].to_string();
    let change_prefix = change_zk.clone();

    let mut client =
        cc_common::backend::backend_service_client::BackendServiceClient::connect(server_url)
            .await
            .unwrap();

    // ResolveCommitPrefix and ResolveChangePrefix (`z-k` reverse-hex).
    let resolved_commit = client
        .resolve_commit_prefix(ResolveCommitPrefixRequest {
            project_id: repo_id.clone(),
            hex_prefix: commit_prefix,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        resolved_commit.status,
        ObjectPrefixStatus::ObjectPrefixSingleMatch as i32
    );
    assert_eq!(resolved_commit.matched_commit_id, commit_bytes);

    let resolved_change = client
        .resolve_change_prefix(ResolveChangePrefixRequest {
            project_id: repo_id.clone(),
            prefix: change_prefix,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        resolved_change.status,
        ObjectPrefixStatus::ObjectPrefixSingleMatch as i32
    );
    assert!(resolved_change.commit_ids.contains(&commit_bytes));

    // Paginated `ReadTree` (`page_size = 2`) with `File.size_bytes` populated.
    let stored = client
        .read_commit(ReadCommitRequest {
            project_id: repo_id.clone(),
            commit_id: commit_bytes.clone(),
        })
        .await
        .unwrap()
        .into_inner()
        .commit
        .unwrap();
    let root_tree_id = stored.root_tree_id[0].clone();

    let page1 = client
        .read_tree(ReadTreeRequest {
            project_id: repo_id.clone(),
            path: String::new(),
            tree_id: root_tree_id.clone(),
            page_size: 2,
            page_token: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(page1.entries.len(), 2);
    assert_eq!(page1.entries[0].name, "a.txt");
    assert_eq!(page1.entries[1].name, "b.txt");
    assert_eq!(page1.next_page_token, "b.txt");

    let page2 = client
        .read_tree(ReadTreeRequest {
            project_id: repo_id.clone(),
            path: String::new(),
            tree_id: root_tree_id,
            page_size: 2,
            page_token: page1.next_page_token,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(page2.entries.len(), 2);
    assert_eq!(page2.entries[0].name, "c.txt");
    assert_eq!(page2.entries[1].name, "d.txt");
    assert!(page2.next_page_token.is_empty());

    for (entry, expected_len) in page1
        .entries
        .iter()
        .chain(page2.entries.iter())
        .zip([5u64, 9, 13, 16])
    {
        let Some(tree_value::Value::File(f)) = entry.value.as_ref().and_then(|v| v.value.as_ref())
        else {
            panic!("expected File entry");
        };
        assert_eq!(f.size_bytes, Some(expected_len));
    }

    // Forged client `commit_id` on `WriteCommit` is rejected with INVALID_ARGUMENT.
    let mut forged = stored.clone();
    forged.commit_id = vec![0xde; 20];
    let err = client
        .write_commit(WriteCommitRequest {
            project_id: repo_id,
            commit: Some(forged),
        })
        .await
        .expect_err("forged commit_id must be rejected by server");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    // `CommitCloudBackend::write_commit` invokes `sign_with` and round-trips `secure_sig`.
    let backend =
        cc_lib::cc_backend::CommitCloudBackend::load(&repo_path.join(".jj/repo/store")).unwrap();
    let sig_author = Signature {
        name: "Agent Signer".to_string(),
        email: "signer@example.com".to_string(),
        timestamp: Timestamp {
            timestamp: MillisSinceEpoch(1_730_000_000_000),
            tz_offset: 0,
        },
    };
    let unsigned_commit = Commit {
        parents: vec![backend.root_commit_id().clone()],
        predecessors: vec![],
        root_tree: Merge::resolved(backend.empty_tree_id().clone()),
        conflict_labels: Merge::resolved(String::new()),
        change_id: jj_lib::backend::ChangeId::from_bytes(&[0x42; 16]),
        description: "signed agent commit\n".to_string(),
        author: sig_author.clone(),
        committer: sig_author,
        secure_sig: None,
    };
    let expected_sig =
        b"-----BEGIN SSH SIGNATURE-----\ntest-agent-sig\n-----END SSH SIGNATURE-----\n";
    let mut sign_cb = |data: &[u8]| -> Result<Vec<u8>, jj_lib::signing::SignError> {
        assert!(
            std::str::from_utf8(data)
                .unwrap()
                .contains("signed agent commit\n"),
            "unsigned Git commit payload passed to SigningFn must include commit description"
        );
        Ok(expected_sig.to_vec())
    };
    let mut sign_ref: &mut jj_lib::backend::SigningFn = &mut sign_cb;
    let (written_id, written_commit) = backend
        .write_commit(unsigned_commit, Some(&mut sign_ref))
        .await
        .unwrap();
    assert_eq!(
        written_commit.secure_sig.as_ref().unwrap().sig,
        expected_sig
    );
    let read_back = backend.read_commit(&written_id).await.unwrap();
    assert_eq!(read_back.secure_sig.as_ref().unwrap().sig, expected_sig);
}
