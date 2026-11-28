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

//! Two clones with distinct workspace names share one repository: a commit made
//! in clone A shows up in clone B's `jj log` and `jj op log`. Clone B's first
//! operation starts from the root operation after A has already moved the head,
//! so the server must accept it as a fork and reconcile it.

use assert_cmd::Command;
use std::path::Path;
use tempfile::{NamedTempFile, tempdir};
use testutils::{ServerGuard, spawn_server, spawn_sqlite_server};

fn jj(server: &ServerGuard, dir: &Path) -> Command {
    let mut cmd = server.jj_cmd();
    cmd.current_dir(dir)
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com");
    cmd
}

fn init_clone(server: &ServerGuard, dir: &Path, workspace_name: &str, create: bool) {
    let mut args = vec![
        "cc",
        "init",
        "--server",
        server.url(),
        "--project-id",
        "two-clones-project",
        "--repo-id",
        "two-clones-repo",
        "--workspace-name",
        workspace_name,
        "--working-copy-type",
        "local",
    ];
    if create {
        args.push("--create");
    }
    args.push(".");
    jj(server, dir).args(&args).assert().success();
}

fn stdout(cmd: &mut Command) -> String {
    let output = cmd.output().unwrap();
    assert!(
        output.status.success(),
        "command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

fn run_two_clones(server: &ServerGuard) {
    let clone_a = tempdir().unwrap();
    let clone_b = tempdir().unwrap();

    init_clone(server, clone_a.path(), "A", true);
    std::fs::write(clone_a.path().join("hello.txt"), "hello from A\n").unwrap();
    jj(server, clone_a.path())
        .args(["describe", "-m", "commit from A"])
        .assert()
        .success();

    init_clone(server, clone_b.path(), "B", false);

    let log = stdout(jj(server, clone_b.path()).args([
        "log",
        "--no-graph",
        "-r",
        "all()",
        "-T",
        "description ++ \"\\n\"",
    ]));
    assert!(log.contains("commit from A"), "clone B's jj log:\n{log}");

    let op_log = stdout(jj(server, clone_b.path()).args([
        "op",
        "log",
        "--no-graph",
        "-T",
        "description ++ \"\\n\"",
    ]));
    for expected in ["describe commit", "add workspace 'A'", "add workspace 'B'"] {
        assert!(
            op_log.contains(expected),
            "clone B's jj op log lacks {expected:?}:\n{op_log}"
        );
    }
}

#[tokio::test]
async fn test_two_clones_share_one_repository_memory() {
    let server = spawn_server().await;
    run_two_clones(&server);
}

#[tokio::test]
async fn test_two_clones_share_one_repository_sqlite() {
    let db_file = NamedTempFile::new().unwrap();
    let server = spawn_sqlite_server(db_file.path()).await;
    run_two_clones(&server);
}
