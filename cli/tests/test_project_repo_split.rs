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
use std::path::Path;

use assert_cmd::Command;

fn jj_cmd(server: &testutils::ServerGuard, repo_dir: &Path) -> Command {
    let mut cmd = server.jj_cmd();
    cmd.current_dir(repo_dir);
    cmd.env("JJ_USER", "Test Agent");
    cmd.env("JJ_EMAIL", "agent@example.com");
    cmd
}

#[tokio::test]
async fn test_shared_project_deduplicates_commits_and_syncs_index_while_isolating_other_projects() {
    let db_dir = tempfile::tempdir().expect("tempdir");
    let server = testutils::spawn_sqlite_server(&db_dir.path().join("shared_proj.db")).await;

    let agent_a_dir = db_dir.path().join("agent_a");
    let agent_b_dir = db_dir.path().join("agent_b");
    let other_proj_dir = db_dir.path().join("other_proj");

    // Agent A creates a repository in project `monorepo-alpha`.
    jj_cmd(&server, db_dir.path())
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--project-id",
            "monorepo-alpha",
            "--create",
            agent_a_dir.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Agent B creates an independent repository (distinct OpLog) in the same `monorepo-alpha`
    // project.
    jj_cmd(&server, db_dir.path())
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--project-id",
            "monorepo-alpha",
            "--create",
            agent_b_dir.to_str().unwrap(),
        ])
        .assert()
        .success();

    // A third workspace creates a repository in a separate `isolated-beta` project.
    jj_cmd(&server, db_dir.path())
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--project-id",
            "isolated-beta",
            "--create",
            other_proj_dir.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Agent A authors a commit with a file in `monorepo-alpha`.
    fs::write(agent_a_dir.join("engine.rs"), "pub fn run() -> u32 { 7 }\n").unwrap();
    jj_cmd(&server, &agent_a_dir)
        .args(["describe", "-m", "agent-a feature"])
        .assert()
        .success();

    let commit_a_out = jj_cmd(&server, &agent_a_dir)
        .args(["log", "--no-graph", "-r", "@", "-T", "commit_id"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let commit_a = String::from_utf8(commit_a_out).unwrap().trim().to_string();

    // Agent B's default view (`jj log`) remains clean and independent (only its own working copy),
    // yet `CommitCloudIndexStore` automatically indexes `commit_a` from the shared project pool so
    // Agent B can inspect or branch directly off `commit_a` by commit ID.
    let b_default_log = jj_cmd(&server, &agent_b_dir)
        .args(["log", "--no-graph", "-T", "description"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    assert!(
        !String::from_utf8_lossy(&b_default_log).contains("agent-a feature"),
        "sibling repository OpLog/View must stay isolated until explicitly referenced"
    );

    jj_cmd(&server, &agent_b_dir)
        .args(["new", &commit_a, "-m", "agent-b stacked on agent-a"])
        .assert()
        .success();
    assert_eq!(
        fs::read_to_string(agent_b_dir.join("engine.rs")).unwrap(),
        "pub fn run() -> u32 { 7 }\n"
    );

    // Meanwhile, `isolated-beta` cannot resolve or read `commit_a` across the project boundary.
    jj_cmd(&server, &other_proj_dir)
        .args(["log", "-r", &commit_a])
        .assert()
        .failure()
        .stderr(predicates::str::contains("doesn't exist"));
}

#[tokio::test]
async fn test_opt_out_project_sync_skips_sibling_commits_and_resolves_explicit_commit_ids() {
    let db_dir = tempfile::tempdir().expect("tempdir");
    let server = testutils::spawn_sqlite_server(&db_dir.path().join("opt_out_proj.db")).await;

    let seed_dir = db_dir.path().join("seed_ws");
    let actor_lazy_dir = db_dir.path().join("actor_lazy");
    let actor_flag_dir = db_dir.path().join("actor_flag");

    jj_cmd(&server, db_dir.path())
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--project-id",
            "fleet-proj",
            "--create",
            seed_dir.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Seed workspace creates `base_cid` and an unrelated sibling commit `sibling_cid`.
    fs::write(seed_dir.join("base.txt"), "fleet baseline\n").unwrap();
    jj_cmd(&server, &seed_dir)
        .args(["describe", "-m", "base commit"])
        .assert()
        .success();
    let base_cid = String::from_utf8(
        jj_cmd(&server, &seed_dir)
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

    jj_cmd(&server, &seed_dir)
        .args(["new", "root()", "-m", "unrelated sibling"])
        .assert()
        .success();
    let sibling_cid = String::from_utf8(
        jj_cmd(&server, &seed_dir)
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
    let sibling_prefix = &sibling_cid[..12];

    // Actor 1 initializes with `JJ_CC_SYNC_PROJECT_COMMITS=0` (no `--index-commit`).
    jj_cmd(&server, db_dir.path())
        .env("JJ_CC_SYNC_PROJECT_COMMITS", "0")
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--project-id",
            "fleet-proj",
            "--create",
            actor_lazy_dir.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Sibling commit is NOT synced eagerly into the index when project sync is disabled
    // (12-hex prefix would resolve any indexed commit, even hidden ones).
    jj_cmd(&server, &actor_lazy_dir)
        .env("JJ_CC_SYNC_PROJECT_COMMITS", "0")
        .args(["log", "-r", sibling_prefix])
        .assert()
        .failure()
        .stderr(predicates::str::contains("doesn't exist"));

    // Yet `jj new <base_cid>` lazily fetches and indexes full 40-hex `base_cid` on demand.
    jj_cmd(&server, &actor_lazy_dir)
        .env("JJ_CC_SYNC_PROJECT_COMMITS", "0")
        .args(["new", &base_cid, "-m", "actor work"])
        .assert()
        .success();
    assert_eq!(
        fs::read_to_string(actor_lazy_dir.join("base.txt")).unwrap(),
        "fleet baseline\n"
    );

    // Actor 2 initializes with `commit-cloud.sync-project-commits=false` and explicit `--index-commit`.
    jj_cmd(&server, db_dir.path())
        .args([
            "--config",
            "commit-cloud.sync-project-commits=false",
            "cc",
            "init",
            "--server",
            server.url(),
            "--project-id",
            "fleet-proj",
            "--create",
            "--index-commit",
            &sibling_cid,
            actor_flag_dir.to_str().unwrap(),
        ])
        .assert()
        .success();

    jj_cmd(&server, &actor_flag_dir)
        .args([
            "--config",
            "commit-cloud.sync-project-commits=false",
            "log",
            "--no-graph",
            "-r",
            sibling_prefix,
            "-T",
            "description",
        ])
        .assert()
        .success()
        .stdout(predicates::str::contains("unrelated sibling"));

    // Failed `jj cc init` / `jj cc clone` cleans up partial `.jj` so retrying the same path works.
    let retry_dir = db_dir.path().join("retry_ws");
    jj_cmd(&server, db_dir.path())
        .args([
            "cc",
            "clone",
            "--server",
            server.url(),
            "nonexistent-repo-id",
            retry_dir.to_str().unwrap(),
        ])
        .assert()
        .failure();
    assert!(
        !retry_dir.exists(),
        "failed clone must clean up newly created destination directory"
    );

    jj_cmd(&server, db_dir.path())
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--repo-id",
            "nonexistent-repo-id",
            retry_dir.to_str().unwrap(),
        ])
        .assert()
        .failure();
    assert!(
        !retry_dir.exists(),
        "failed init must clean up newly created destination directory"
    );

    jj_cmd(&server, db_dir.path())
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--project-id",
            "fleet-proj",
            "--create",
            retry_dir.to_str().unwrap(),
        ])
        .assert()
        .success();
}
