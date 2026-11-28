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

use cc_common::backend::backend_service_client::BackendServiceClient;
use cc_common::backend::{
    Commit, ReadCommitRequest, ReadTreeRequest, WriteCommitRequest, tree_value,
};
use cc_common::{EMPTY_TREE_ID_BYTES, ROOT_CHANGE_ID_BYTES, ROOT_COMMIT_ID_BYTES};
use std::fs;
use std::process::Command;
use tempfile::tempdir;
use testutils::spawn_server;

#[tokio::test]
async fn test_import_git_repository_succeeds() {
    // Initialize a TestWorkspace
    let ws = testutils::TestWorkspace::init().await;
    let config_path = ws.repo_path().join(".jj/repo/store/config.toml");
    let config_str = fs::read_to_string(&config_path).unwrap();
    let config: toml::Value = toml::from_str(&config_str).unwrap();
    let project_id = config
        .get("project_id")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let repo_id = config.get("repo_id").unwrap().as_str().unwrap().to_string();

    // Create a real Git repository with commits
    let git_temp_dir = tempdir().expect("Failed to create temporary git repo dir");
    let git_path = git_temp_dir.path();

    let init_status = Command::new("git")
        .args(["init", "-b", "main"])
        .current_dir(git_path)
        .status()
        .expect("Failed to execute git init");
    assert!(init_status.success());

    Command::new("git")
        .args(["checkout", "-b", "main"])
        .current_dir(git_path)
        .status()
        .unwrap();

    Command::new("git")
        .args(["config", "user.name", "Test User"])
        .current_dir(git_path)
        .status()
        .unwrap();
    Command::new("git")
        .args(["config", "user.email", "test@example.com"])
        .current_dir(git_path)
        .status()
        .unwrap();

    let file_path = git_path.join("README.md");
    fs::write(&file_path, "# Commit Cloud Git Import Test\n").unwrap();

    Command::new("git")
        .args(["add", "README.md"])
        .current_dir(git_path)
        .status()
        .unwrap();

    let commit_status = Command::new("git")
        .args(["commit", "-m", "initial git commit"])
        .current_dir(git_path)
        .status()
        .unwrap();
    assert!(commit_status.success());

    // Run `jj cc import-git` with required --project-id and --repo-id
    let mut cmd = ws.server().jj_cmd();
    cmd.args([
        "cc",
        "import-git",
        "--git-dir",
        git_path.to_str().unwrap(),
        "--project-id",
        &project_id,
        "--repo-id",
        &repo_id,
        "--server",
        ws.server_url(),
    ]);

    cmd.assert().success();

    // Verify `jj log` in the workspace succeeds without panic
    let mut log_cmd = ws.jj_cmd();
    log_cmd.args(["log"]).assert().success();
}

#[tokio::test]
async fn test_import_git_preserves_existing_unrelated_workspace_history() {
    let ws = testutils::TestWorkspace::init().await;
    let config_path = ws.repo_path().join(".jj/repo/store/config.toml");
    let config_str = fs::read_to_string(&config_path).unwrap();
    let config: toml::Value = toml::from_str(&config_str).unwrap();
    let project_id = config
        .get("project_id")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let repo_id = config.get("repo_id").unwrap().as_str().unwrap().to_string();

    // Create existing local commit in the workspace
    let local_file = ws.repo_path().join("local.txt");
    fs::write(&local_file, "hello from existing local commit\n").unwrap();
    let mut describe_cmd = ws.jj_cmd();
    describe_cmd
        .args(["describe", "-m", "my local workspace commit"])
        .assert()
        .success();

    // Create a separate Git repository with unrelated history
    let git_temp_dir = tempdir().expect("Failed to create temporary git repo dir");
    let git_path = git_temp_dir.path();

    let init_status = Command::new("git")
        .args(["init", "-b", "main"])
        .current_dir(git_path)
        .status()
        .expect("Failed to execute git init");
    assert!(init_status.success());

    Command::new("git")
        .args(["config", "user.name", "Test User"])
        .current_dir(git_path)
        .status()
        .unwrap();
    Command::new("git")
        .args(["config", "user.email", "test@example.com"])
        .current_dir(git_path)
        .status()
        .unwrap();

    let file_path = git_path.join("git_file.txt");
    fs::write(&file_path, "hello from git commit\n").unwrap();

    Command::new("git")
        .args(["add", "git_file.txt"])
        .current_dir(git_path)
        .status()
        .unwrap();

    let commit_status = Command::new("git")
        .args(["commit", "-m", "unrelated imported git commit"])
        .current_dir(git_path)
        .status()
        .unwrap();
    assert!(commit_status.success());

    // Import the Git repository
    let mut cmd = ws.server().jj_cmd();
    cmd.args([
        "cc",
        "import-git",
        "--git-dir",
        git_path.to_str().unwrap(),
        "--project-id",
        &project_id,
        "--repo-id",
        &repo_id,
        "--server",
        ws.server_url(),
    ]);
    cmd.assert().success();

    // Check `jj log` shows both independent branches
    let mut log_cmd = ws.jj_cmd();
    let output = log_cmd.args(["log", "-r", "all()"]).assert().success();
    let stdout = String::from_utf8_lossy(&output.get_output().stdout);

    assert!(
        stdout.contains("my local workspace commit"),
        "Log output should contain existing workspace commit:\n{}",
        stdout
    );
    assert!(
        stdout.contains("unrelated imported git commit"),
        "Log output should contain imported git commit:\n{}",
        stdout
    );
    assert!(
        stdout.contains("main"),
        "Log output should contain main bookmark:\n{}",
        stdout
    );
}

#[tokio::test]
async fn test_import_git_preserves_non_utc_timezone_offsets() {
    let ws = testutils::TestWorkspace::init().await;
    let config_path = ws.repo_path().join(".jj/repo/store/config.toml");
    let config_str = fs::read_to_string(&config_path).unwrap();
    let config: toml::Value = toml::from_str(&config_str).unwrap();
    let project_id = config
        .get("project_id")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let repo_id = config.get("repo_id").unwrap().as_str().unwrap().to_string();

    let git_temp_dir = tempdir().expect("Failed to create temporary git repo dir");
    let git_path = git_temp_dir.path();

    let init_status = Command::new("git")
        .args(["init", "-b", "main"])
        .current_dir(git_path)
        .status()
        .expect("Failed to execute git init");
    assert!(init_status.success());

    fs::write(git_path.join("git_file.txt"), "hello\n").unwrap();
    Command::new("git")
        .args(["add", "git_file.txt"])
        .current_dir(git_path)
        .status()
        .unwrap();

    // Author and committer deliberately use different, non-UTC offsets
    // (positive half-hour and negative) to catch sign/unit regressions.
    let commit_status = Command::new("git")
        .args(["commit", "-m", "commit with timezone offsets"])
        .env("GIT_AUTHOR_NAME", "Author")
        .env("GIT_AUTHOR_EMAIL", "author@example.com")
        .env("GIT_AUTHOR_DATE", "2024-01-02T03:04:05+0530")
        .env("GIT_COMMITTER_NAME", "Committer")
        .env("GIT_COMMITTER_EMAIL", "committer@example.com")
        .env("GIT_COMMITTER_DATE", "2024-03-04T05:06:07-0800")
        .current_dir(git_path)
        .status()
        .unwrap();
    assert!(commit_status.success());

    let mut cmd = ws.server().jj_cmd();
    cmd.args([
        "cc",
        "import-git",
        "--git-dir",
        git_path.to_str().unwrap(),
        "--project-id",
        &project_id,
        "--repo-id",
        &repo_id,
        "--server",
        ws.server_url(),
    ]);
    cmd.assert().success();

    let mut log_cmd = ws.jj_cmd();
    let output = log_cmd
        .args([
            "log",
            "--no-graph",
            "-r",
            "description(substring:\"commit with timezone offsets\")",
            "-T",
            "author.timestamp() ++ \"|\" ++ committer.timestamp()",
        ])
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&output.get_output().stdout);
    assert_eq!(
        stdout.trim(),
        "2024-01-02 03:04:05.000 +05:30|2024-03-04 05:06:07.000 -08:00"
    );
}

#[tokio::test]
async fn test_import_git_rejects_commit_with_missing_timestamp() {
    let ws = testutils::TestWorkspace::init().await;
    let config_path = ws.repo_path().join(".jj/repo/store/config.toml");
    let config_str = fs::read_to_string(&config_path).unwrap();
    let config: toml::Value = toml::from_str(&config_str).unwrap();
    let project_id = config
        .get("project_id")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let repo_id = config.get("repo_id").unwrap().as_str().unwrap().to_string();

    let git_temp_dir = tempdir().expect("Failed to create temporary git repo dir");
    let git_path = git_temp_dir.path();

    let init_status = Command::new("git")
        .args(["init", "-b", "main"])
        .current_dir(git_path)
        .status()
        .expect("Failed to execute git init");
    assert!(init_status.success());

    // The empty tree is always resolvable, so no files are needed.
    let tree_out = Command::new("git")
        .args(["hash-object", "-t", "tree", "-w", "--stdin"])
        .stdin(std::process::Stdio::null())
        .current_dir(git_path)
        .output()
        .unwrap();
    assert!(tree_out.status.success());
    let tree_id = String::from_utf8(tree_out.stdout)
        .unwrap()
        .trim()
        .to_string();

    // Signatures without a timestamp are malformed; the importer must refuse
    // them rather than invent a date.
    let raw_commit = format!(
        "tree {tree_id}\nauthor A U Thor <a@example.com>\ncommitter A U Thor <a@example.com>\n\nno timestamp\n"
    );
    let mut hash_cmd = Command::new("git")
        .args([
            "hash-object",
            "-t",
            "commit",
            "-w",
            "--literally",
            "--stdin",
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .current_dir(git_path)
        .spawn()
        .unwrap();
    std::io::Write::write_all(hash_cmd.stdin.as_mut().unwrap(), raw_commit.as_bytes()).unwrap();
    let commit_out = hash_cmd.wait_with_output().unwrap();
    assert!(commit_out.status.success());
    let commit_id = String::from_utf8(commit_out.stdout)
        .unwrap()
        .trim()
        .to_string();

    let ref_status = Command::new("git")
        .args(["update-ref", "refs/heads/main", &commit_id])
        .current_dir(git_path)
        .status()
        .unwrap();
    assert!(ref_status.success());

    let mut cmd = ws.server().jj_cmd();
    cmd.args([
        "cc",
        "import-git",
        "--git-dir",
        git_path.to_str().unwrap(),
        "--project-id",
        &project_id,
        "--repo-id",
        &repo_id,
        "--server",
        ws.server_url(),
    ]);
    let output = cmd.assert().failure();
    let stderr = String::from_utf8_lossy(&output.get_output().stderr);
    assert!(
        stderr.contains(&commit_id),
        "Error should name the offending commit {commit_id}:\n{stderr}"
    );
    assert!(
        stderr.contains("invalid author"),
        "Error should explain the malformed signature:\n{stderr}"
    );
}

#[tokio::test]
async fn test_import_git_fails_on_unregistered_repo() {
    let server = spawn_server().await;

    let git_temp_dir = tempdir().expect("Failed to create temporary git repo dir");
    let git_path = git_temp_dir.path();

    let _ = Command::new("git")
        .args(["init", "-b", "main"])
        .current_dir(git_path)
        .status();

    let mut cmd = server.jj_cmd();
    cmd.args([
        "cc",
        "import-git",
        "--git-dir",
        git_path.to_str().unwrap(),
        "--project-id",
        "test-project",
        "--repo-id",
        "non-existent-repo-id",
        "--server",
        server.url(),
    ]);

    cmd.assert().failure();
}

#[tokio::test]
async fn test_rejects_merge_commit_with_root_commit_as_parent() {
    let ws = testutils::TestWorkspace::init().await;
    let config_path = ws.repo_path().join(".jj/repo/store/config.toml");
    let config_str = fs::read_to_string(&config_path).unwrap();
    let config: toml::Value = toml::from_str(&config_str).unwrap();
    let project_id = config
        .get("project_id")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();

    let mut client = BackendServiceClient::connect(ws.server_url().to_string())
        .await
        .expect("Failed to connect to BackendService");

    // Attempt to write a merge commit where one parent is the root commit
    let invalid_merge = Commit {
        commit_id: vec![],
        change_id: ROOT_CHANGE_ID_BYTES.to_vec(),
        parent_commit_ids: vec![ROOT_COMMIT_ID_BYTES.to_vec(), vec![1u8; 20]],
        root_tree_id: vec![EMPTY_TREE_ID_BYTES.to_vec()],
        description: "invalid merge with root commit".to_string(),
        author: None,
        committer: None,
        predecessors: vec![],
        conflict_labels: vec![],
        secure_sig: None,
        raw_git_commit: vec![],
        external_ids: Default::default(),
    };

    let result = client
        .write_commit(WriteCommitRequest {
            project_id,
            commit: Some(invalid_merge),
        })
        .await;

    assert!(
        result.is_err(),
        "Server should reject merge commit with root commit as a parent"
    );
    let status = result.unwrap_err();
    assert!(
        status
            .message()
            .contains("does not support creating merge commits with the root commit"),
        "Unexpected error message: {}",
        status.message()
    );
}

#[tokio::test]
async fn test_import_git_diamond_merge_change_id_header_and_symlink() {
    let ws = testutils::TestWorkspace::init().await;
    let config_path = ws.repo_path().join(".jj/repo/store/config.toml");
    let config_str = fs::read_to_string(&config_path).unwrap();
    let config: toml::Value = toml::from_str(&config_str).unwrap();
    let project_id = config
        .get("project_id")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let repo_id = config.get("repo_id").unwrap().as_str().unwrap().to_string();

    let git_dir = tempfile::tempdir().unwrap();
    let git_path = git_dir.path();
    let run_git = |args: &[&str]| {
        let status = Command::new("git")
            .args(args)
            .current_dir(git_path)
            .env("GIT_AUTHOR_NAME", "Tester")
            .env("GIT_AUTHOR_EMAIL", "tester@example.com")
            .env("GIT_COMMITTER_NAME", "Tester")
            .env("GIT_COMMITTER_EMAIL", "tester@example.com")
            .status()
            .unwrap();
        assert!(status.success(), "git {args:?} failed");
    };

    run_git(&["init", "-b", "main"]);
    fs::write(git_path.join("base.txt"), "base\n").unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink("base.txt", git_path.join("base.link")).unwrap();
    run_git(&["add", "."]);
    run_git(&["commit", "-m", "base commit with symlink"]);

    // Branch A
    run_git(&["checkout", "-b", "branch-a"]);
    fs::write(git_path.join("a.txt"), "a\n").unwrap();
    run_git(&["add", "a.txt"]);
    run_git(&["commit", "-m", "branch A"]);

    // Branch B with a jj-style `change-id` header created via git commit-tree
    run_git(&["checkout", "main"]);
    run_git(&["checkout", "-b", "branch-b"]);
    fs::write(git_path.join("b.txt"), "b\n").unwrap();
    run_git(&["add", "b.txt"]);
    let tree_hex = String::from_utf8(
        Command::new("git")
            .args(["write-tree"])
            .current_dir(git_path)
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_string();
    let main_hex = String::from_utf8(
        Command::new("git")
            .args(["rev-parse", "main"])
            .current_dir(git_path)
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_string();
    let raw_commit = format!(
        "tree {tree_hex}\nparent {main_hex}\nauthor Tester <tester@example.com> 1700000000 +0000\ncommitter Tester <tester@example.com> 1700000000 +0000\nchange-id zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzyy\n\nbranch B with change-id\n"
    );
    let mut hash_child = Command::new("git")
        .args(["hash-object", "-t", "commit", "-w", "--stdin"])
        .current_dir(git_path)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    use std::io::Write as _;
    hash_child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(raw_commit.as_bytes())
        .unwrap();
    let out = hash_child.wait_with_output().unwrap();
    assert!(out.status.success());
    let branch_b_hex = String::from_utf8(out.stdout).unwrap().trim().to_string();
    run_git(&["reset", "--hard", &branch_b_hex]);

    // Diamond merge: merge branch-a into branch-b
    run_git(&["merge", "--no-ff", "branch-a", "-m", "diamond merge"]);

    let mut cmd = ws.server().jj_cmd();
    cmd.args([
        "cc",
        "import-git",
        "--git-dir",
        git_path.to_str().unwrap(),
        "--project-id",
        &project_id,
        "--repo-id",
        &repo_id,
        "--server",
        ws.server_url(),
    ]);
    cmd.assert().success();

    let log_out = ws
        .jj_cmd()
        .args(["log", "--no-graph", "-T", "description"])
        .output()
        .unwrap();
    assert!(log_out.status.success());
    let log_str = String::from_utf8_lossy(&log_out.stdout);
    assert!(
        log_str.contains("diamond merge"),
        "log missing merge: {log_str}"
    );
    assert!(
        log_str.contains("branch B with change-id"),
        "log missing branch B: {log_str}"
    );

    let change_id_out = ws
        .jj_cmd()
        .args([
            "log",
            "--no-graph",
            "-r",
            "description(substring:\"branch B with change-id\")",
            "-T",
            "change_id",
        ])
        .output()
        .unwrap();
    assert!(change_id_out.status.success());
    assert_eq!(
        String::from_utf8_lossy(&change_id_out.stdout).trim(),
        "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzyy",
        "branch B should preserve its Git change-id header"
    );

    #[cfg(unix)]
    {
        ws.jj_cmd()
            .args(["new", "description(substring:\"diamond merge\")"])
            .assert()
            .success();
        let target = fs::read_link(ws.repo_path().join("base.link")).unwrap();
        assert_eq!(
            target,
            std::path::Path::new("base.txt"),
            "imported symlink target should round-trip cleanly"
        );
    }
}

#[tokio::test]
async fn test_import_git_preserves_signed_commit() {
    let ws = testutils::TestWorkspace::init().await;
    let config_path = ws.repo_path().join(".jj/repo/store/config.toml");
    let config_str = fs::read_to_string(&config_path).unwrap();
    let config: toml::Value = toml::from_str(&config_str).unwrap();
    let project_id = config
        .get("project_id")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let repo_id = config.get("repo_id").unwrap().as_str().unwrap().to_string();

    let git_dir = tempdir().unwrap();
    let git_path = git_dir.path();
    let init_status = Command::new("git")
        .args(["init", "-b", "main"])
        .current_dir(git_path)
        .status()
        .unwrap();
    assert!(init_status.success());

    fs::write(git_path.join("signed.txt"), "signed payload\n").unwrap();
    assert!(
        Command::new("git")
            .args(["add", "signed.txt"])
            .current_dir(git_path)
            .status()
            .unwrap()
            .success()
    );
    let tree_out = Command::new("git")
        .args(["write-tree"])
        .current_dir(git_path)
        .output()
        .unwrap();
    assert!(tree_out.status.success());
    let tree_hex = String::from_utf8(tree_out.stdout)
        .unwrap()
        .trim()
        .to_string();

    let raw_commit = format!(
        "tree {tree_hex}\nauthor Signer <signer@example.com> 1700000000 +0000\ncommitter Signer <signer@example.com> 1700000000 +0000\ngpgsig -----BEGIN PGP SIGNATURE-----\n \n wsBcBAABCAAQ\n -----END PGP SIGNATURE-----\n\nsigned imported commit\n"
    );
    let mut hash_child = Command::new("git")
        .args(["hash-object", "-t", "commit", "-w", "--stdin"])
        .current_dir(git_path)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    use std::io::Write as _;
    hash_child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(raw_commit.as_bytes())
        .unwrap();
    let commit_out = hash_child.wait_with_output().unwrap();
    assert!(commit_out.status.success());
    let commit_hex = String::from_utf8(commit_out.stdout)
        .unwrap()
        .trim()
        .to_string();
    assert!(
        Command::new("git")
            .args(["update-ref", "refs/heads/main", &commit_hex])
            .current_dir(git_path)
            .status()
            .unwrap()
            .success()
    );

    let mut cmd = ws.server().jj_cmd();
    cmd.args([
        "cc",
        "import-git",
        "--git-dir",
        git_path.to_str().unwrap(),
        "--project-id",
        &project_id,
        "--repo-id",
        &repo_id,
        "--server",
        ws.server_url(),
    ]);
    cmd.assert().success();

    let log_out = ws
        .jj_cmd()
        .args([
            "log",
            "--no-graph",
            "-r",
            &commit_hex,
            "-T",
            "commit_id ++ \"|\" ++ if(signature, \"signed\", \"unsigned\") ++ \"|\" ++ description.first_line()",
        ])
        .output()
        .unwrap();
    assert!(
        log_out.status.success(),
        "jj log failed: {}",
        String::from_utf8_lossy(&log_out.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&log_out.stdout).trim(),
        format!("{commit_hex}|signed|signed imported commit")
    );

    let mut client = BackendServiceClient::connect(ws.server_url().to_string())
        .await
        .unwrap();
    let commit_bytes = jj_lib::hex_util::decode_hex(commit_hex.as_bytes()).unwrap();
    let stored = client
        .read_commit(ReadCommitRequest {
            project_id,
            commit_id: commit_bytes,
        })
        .await
        .unwrap()
        .into_inner()
        .commit
        .expect("imported signed commit should be readable from BackendService");
    assert_eq!(stored.raw_git_commit, raw_commit.as_bytes());
    let sig = stored
        .secure_sig
        .expect("imported signed commit should populate secure_sig");
    assert!(
        sig.sig.starts_with(b"-----BEGIN PGP SIGNATURE-----"),
        "unexpected signature bytes: {:?}",
        String::from_utf8_lossy(&sig.sig)
    );
    assert!(!sig.data.is_empty(), "server must derive secure_sig.data");
}

#[tokio::test]
async fn test_import_git_preserves_submodule_gitlink() {
    let ws = testutils::TestWorkspace::init().await;
    let config_path = ws.repo_path().join(".jj/repo/store/config.toml");
    let config_str = fs::read_to_string(&config_path).unwrap();
    let config: toml::Value = toml::from_str(&config_str).unwrap();
    let project_id = config
        .get("project_id")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let repo_id = config.get("repo_id").unwrap().as_str().unwrap().to_string();

    let git_dir = tempdir().unwrap();
    let git_path = git_dir.path();
    let run_git = |args: &[&str]| {
        let status = Command::new("git")
            .args(args)
            .current_dir(git_path)
            .env("GIT_AUTHOR_NAME", "Tester")
            .env("GIT_AUTHOR_EMAIL", "tester@example.com")
            .env("GIT_COMMITTER_NAME", "Tester")
            .env("GIT_COMMITTER_EMAIL", "tester@example.com")
            .status()
            .unwrap();
        assert!(status.success(), "git {args:?} failed");
    };

    run_git(&["init", "-b", "main"]);
    fs::write(git_path.join("README.md"), "repo with submodule\n").unwrap();
    run_git(&["add", "README.md"]);
    let submod_hex = "1234567890abcdef1234567890abcdef12345678";
    let cacheinfo = format!("160000,{submod_hex},submod");
    run_git(&["update-index", "--add", "--cacheinfo", &cacheinfo]);
    run_git(&["commit", "-m", "commit with submodule"]);

    let commit_out = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(git_path)
        .output()
        .unwrap();
    assert!(commit_out.status.success());
    let commit_hex = String::from_utf8(commit_out.stdout)
        .unwrap()
        .trim()
        .to_string();

    let mut cmd = ws.server().jj_cmd();
    cmd.args([
        "cc",
        "import-git",
        "--git-dir",
        git_path.to_str().unwrap(),
        "--project-id",
        &project_id,
        "--repo-id",
        &repo_id,
        "--server",
        ws.server_url(),
    ]);
    cmd.assert().success();

    let log_out = ws
        .jj_cmd()
        .args([
            "log",
            "--no-graph",
            "-r",
            &commit_hex,
            "-T",
            "commit_id ++ \"|\" ++ description.first_line()",
        ])
        .output()
        .unwrap();
    assert!(
        log_out.status.success(),
        "jj log failed: {}",
        String::from_utf8_lossy(&log_out.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&log_out.stdout).trim(),
        format!("{commit_hex}|commit with submodule")
    );

    let mut client = BackendServiceClient::connect(ws.server_url().to_string())
        .await
        .unwrap();
    let commit_bytes = jj_lib::hex_util::decode_hex(commit_hex.as_bytes()).unwrap();
    let stored = client
        .read_commit(ReadCommitRequest {
            project_id: project_id.clone(),
            commit_id: commit_bytes,
        })
        .await
        .unwrap()
        .into_inner()
        .commit
        .expect("imported submodule commit should exist");
    assert_eq!(stored.root_tree_id.len(), 1);

    let tree_resp = client
        .read_tree(ReadTreeRequest {
            project_id,
            path: String::new(),
            tree_id: stored.root_tree_id[0].clone(),
            page_size: 0,
            page_token: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    let submod_entry = tree_resp
        .entries
        .iter()
        .find(|e| e.name == "submod")
        .expect("imported tree should contain 'submod' gitlink entry");
    let expected_submod_bytes = jj_lib::hex_util::decode_hex(submod_hex.as_bytes()).unwrap();
    assert_eq!(
        submod_entry.value.as_ref().and_then(|v| v.value.as_ref()),
        Some(&tree_value::Value::GitSubmoduleId(expected_submod_bytes))
    );
}
