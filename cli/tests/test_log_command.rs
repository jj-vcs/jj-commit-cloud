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

#[tokio::test]
async fn test_log_root_commit_exists() {
    let workspace = testutils::TestWorkspace::init().await;

    // Special character is needed to assert accurate log output. see cli/tests/test_log_command.rs upstream
    workspace
        .jj_cmd()
        .args(["log", "-r", "root()", "-T", "commit_id"])
        .assert()
        .success()
        .stdout("◆  0000000000000000000000000000000000000000\n");
}
