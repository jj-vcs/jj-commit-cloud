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

// Initialize a commit cloud repository and verify that the type file returns commit_cloud for the op store and op heads store.
#[tokio::test]
async fn test_cc_init_op_store_type_returns_commit_cloud() {
    let workspace = testutils::TestWorkspace::init().await;
    let repo_path = workspace.repo_path();

    let op_store_type = fs::read_to_string(repo_path.join(".jj/repo/op_store/type"))
        .expect("op_store type file should exist");
    assert_eq!(op_store_type.trim(), "commit_cloud");

    let op_heads_type = fs::read_to_string(repo_path.join(".jj/repo/op_heads/type"))
        .expect("op_heads type file should exist");
    assert_eq!(op_heads_type.trim(), "commit_cloud");
}

// Run jj operation, modify file, and verify that the commit cloud operation log returns the accurate result
#[tokio::test]
async fn test_op_log_succeeds_on_snapshot() {
    let workspace = testutils::TestWorkspace::init().await;
    let repo_path = workspace.repo_path();

    workspace
        .jj_cmd()
        .args(["op", "log", "--no-graph", "-T", "description"])
        .assert()
        .success()
        .stdout("add workspace 'default'root()");

    fs::write(repo_path.join("file.txt"), "testing op log\n").unwrap();

    workspace
        .jj_cmd()
        .args(["describe", "-m", "description 0"])
        .assert()
        .success();

    workspace
        .jj_cmd()
        .args(["log", "--no-graph", "-r", "@", "-T", "description"])
        .assert()
        .success()
        .stdout("description 0\n");
}
