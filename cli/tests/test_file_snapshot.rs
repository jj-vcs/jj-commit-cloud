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
async fn test_log_snapshots_working_copy() {
    let workspace = testutils::TestWorkspace::init().await;
    let repo_path = workspace.repo_path();

    // Create a file in the working copy
    fs::write(repo_path.join("hello.txt"), "hello commit cloud!\n").unwrap();

    workspace
        .jj_cmd()
        .args(["describe", "-m", "snapshot test"])
        .assert()
        .success();

    // Run jj log with -T description to only print the commit description and verify string match to the set description above
    workspace
        .jj_cmd()
        .args(["log", "--no-graph", "-r", "@", "-T", "description"])
        .assert()
        .success()
        .stdout("snapshot test\n");
}
