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

use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::process::Command as StdCommand;

use assert_cmd::Command;
use cc_common::op_store::{ReadOperationRequest, ReadViewRequest, ReconcileOpHeadsRequest, View};
use jj_lib::backend::CommitId;
use jj_lib::git_backend::synthetic_change_id_from_git_commit_id;
use jj_lib::object_id::ObjectId as _;

fn run_git(repo_dir: &Path, args: &[&str]) -> String {
    let out = StdCommand::new("git")
        .current_dir(repo_dir)
        .args(args)
        .env("GIT_AUTHOR_NAME", "Git Author")
        .env("GIT_AUTHOR_EMAIL", "author@example.com")
        .env("GIT_AUTHOR_DATE", "1730000000 +0000")
        .env("GIT_COMMITTER_NAME", "Git Committer")
        .env("GIT_COMMITTER_EMAIL", "committer@example.com")
        .env("GIT_COMMITTER_DATE", "1730000000 +0000")
        .output()
        .expect("git CLI should execute");
    assert!(
        out.status.success(),
        "git {args:?} failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().to_string()
}

fn run_git_bare(bare_dir: &Path, args: &[&str]) -> String {
    let out = StdCommand::new("git")
        .arg("--git-dir")
        .arg(bare_dir)
        .args(args)
        .output()
        .expect("git CLI should execute");
    assert!(
        out.status.success(),
        "git --git-dir {} {args:?} failed:\nstdout: {}\nstderr: {}",
        bare_dir.display(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().to_string()
}

fn jj_cmd(cwd: &Path, xdg: &Path) -> Command {
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("jj");
    cmd.current_dir(cwd)
        .env("XDG_CONFIG_HOME", xdg)
        .env("JJ_USER", "Cloud Developer")
        .env("JJ_EMAIL", "dev@example.com");
    cmd
}

/// Runs `git init` with the given ref storage format. Returns false if this Git cannot create
/// that format (`--ref-format` needs Git 2.45; older versions only have the files format).
fn git_init_with_ref_format(repo_dir: &Path, ref_format: &str) -> bool {
    fs::create_dir_all(repo_dir).unwrap();
    let out = StdCommand::new("git")
        .current_dir(repo_dir)
        .args(["init", "-b", "main", &format!("--ref-format={ref_format}")])
        .output()
        .expect("git CLI should execute");
    if out.status.success() {
        return true;
    }
    if ref_format == "files" {
        run_git(repo_dir, &["init", "-b", "main"]);
        return true;
    }
    false
}

async fn read_head_view(server_url: &str, repo_id: &str) -> View {
    let server_url = server_url.to_string();
    let repo_id = repo_id.to_string();
    cc_lib::util::run_on_tokio(async move {
        let mut client = cc_lib::util::connect_op_store_client(server_url).await?;
        let op_head = client
            .reconcile_op_heads(ReconcileOpHeadsRequest {
                repo_id: repo_id.clone(),
            })
            .await?
            .into_inner()
            .op_head;
        let op = client
            .read_operation(ReadOperationRequest {
                repo_id: repo_id.clone(),
                operation_id: op_head,
            })
            .await?
            .into_inner()
            .operation
            .expect("operation present");
        let view = client
            .read_view(ReadViewRequest {
                repo_id,
                view_id: op.view_id,
            })
            .await?
            .into_inner()
            .view
            .expect("view present");
        Ok(view)
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn test_git_import_and_export_preserve_exact_sha1_commits_trees_blobs_symlinks_and_merges() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_sqlite_server(&dir.path().join("git_bridge.db")).await;

    // Construct a multi-branch Git repository with nested dirs, >4 MiB blob, executable,
    // symlink, and merge.
    let src_git = dir.path().join("src_git");
    fs::create_dir_all(src_git.join("src")).unwrap();
    fs::create_dir_all(src_git.join("bin")).unwrap();
    run_git(&src_git, &["init", "-b", "main"]);

    fs::write(src_git.join("src/lib.rs"), "pub fn base() -> u32 { 1 }\n").unwrap();
    let script_path = src_git.join("bin/deploy.sh");
    fs::write(&script_path, "#!/bin/sh\necho ok\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&script_path, fs::Permissions::from_mode(0o755)).unwrap();
        std::os::unix::fs::symlink("../src/lib.rs", src_git.join("bin/link.rs")).unwrap();
    }
    let large_payload: Vec<u8> = (0..(4 * 1024 * 1024 + 128 * 1024))
        .map(|i| ((i * 17 + 3) & 0xff) as u8)
        .collect();
    fs::write(src_git.join("model.bin"), &large_payload).unwrap();

    run_git(&src_git, &["add", "."]);
    run_git(&src_git, &["commit", "-m", "initial git commit"]);
    let git_c1 = run_git(&src_git, &["rev-parse", "HEAD"]);

    run_git(&src_git, &["checkout", "-b", "feature/auth"]);
    fs::write(
        src_git.join("src/auth.rs"),
        "pub fn auth() -> bool { true }\n",
    )
    .unwrap();
    run_git(&src_git, &["add", "."]);
    run_git(&src_git, &["commit", "-m", "add auth module"]);
    let git_feature = run_git(&src_git, &["rev-parse", "HEAD"]);

    // Merge feature/auth into main to form a diamond DAG.
    run_git(&src_git, &["checkout", "main"]);
    fs::write(src_git.join("README.md"), "# Monorepo\n").unwrap();
    run_git(&src_git, &["add", "."]);
    run_git(&src_git, &["commit", "-m", "add readme on main"]);
    run_git(
        &src_git,
        &[
            "merge",
            "--no-ff",
            "feature/auth",
            "-m",
            "merge feature/auth",
        ],
    );
    let git_main_merge = run_git(&src_git, &["rev-parse", "main"]);
    let git_main_tree = run_git(&src_git, &["rev-parse", "main^{tree}"]);

    let import_stdout = String::from_utf8(
        jj_cmd(dir.path(), &xdg)
            .args([
                "cc",
                "import-git",
                "--server",
                server.url(),
                "--project-id",
                "proj-git",
                "--repo-id",
                "repo-git-1",
                "--create",
                "--git-dir",
                src_git.to_str().unwrap(),
            ])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap();
    assert!(import_stdout.contains("Commits: 4"));

    // Clone into a Commit Cloud workspace and verify exact Git SHA-1 preservation.
    let ws = dir.path().join("cc_ws");
    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "clone",
            "--server",
            server.url(),
            "-r",
            "main",
            "repo-git-1",
            ws.to_str().unwrap(),
        ])
        .assert()
        .success();

    let resolved_main = String::from_utf8(
        jj_cmd(&ws, &xdg)
            .args(["log", "--no-graph", "-r", "main", "-T", "commit_id"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap()
    .trim()
    .to_string();
    assert_eq!(resolved_main, git_main_merge);

    let resolved_feature = String::from_utf8(
        jj_cmd(&ws, &xdg)
            .args(["log", "--no-graph", "-r", "feature/auth", "-T", "commit_id"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap()
    .trim()
    .to_string();
    assert_eq!(resolved_feature, git_feature);
    assert_eq!(fs::read(ws.join("model.bin")).unwrap(), large_payload);
    assert_eq!(
        fs::read_to_string(ws.join("src/auth.rs")).unwrap(),
        "pub fn auth() -> bool { true }\n"
    );

    // Author a new Commit Cloud commit stacked on `main` and advance `main`.
    fs::write(ws.join("src/cloud.rs"), "pub fn cloud() {}\n").unwrap();
    jj_cmd(&ws, &xdg)
        .args(["describe", "-m", "authored in Commit Cloud"])
        .assert()
        .success();
    jj_cmd(&ws, &xdg)
        .args(["bookmark", "set", "main", "-r", "@"])
        .assert()
        .success();
    let cc_new_head = String::from_utf8(
        jj_cmd(&ws, &xdg)
            .args(["log", "--no-graph", "-r", "main", "-T", "commit_id"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap()
    .trim()
    .to_string();

    // Export back to a fresh bare Git repository and verify `git fsck` and 1:1 SHA-1 parity.
    let dst_git = dir.path().join("exported.git");
    jj_cmd(&ws, &xdg)
        .args([
            "cc",
            "export-git",
            "--bare",
            "--git-dir",
            dst_git.to_str().unwrap(),
        ])
        .assert()
        .success();

    run_git_bare(&dst_git, &["fsck", "--full"]);
    assert_eq!(run_git_bare(&dst_git, &["rev-parse", "main"]), cc_new_head);
    assert_eq!(
        run_git_bare(&dst_git, &["rev-parse", "main~1"]),
        git_main_merge
    );
    assert_eq!(
        run_git_bare(&dst_git, &["rev-parse", "main~1^{tree}"]),
        git_main_tree
    );
    assert_eq!(
        run_git_bare(&dst_git, &["rev-parse", "feature/auth"]),
        git_feature
    );
    assert_eq!(
        run_git_bare(&dst_git, &["rev-parse", "feature/auth~1"]),
        git_c1
    );
}

#[tokio::test]
async fn test_git_import_and_export_roundtrip_submodule_gitlinks_and_external_ids() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    // Exercise `MemoryMetadataStore` too (verifying UTF-8 sort order on Git-imported trees).
    let server = testutils::spawn_server().await;

    // Standalone Git repository commit referenced as a `160000` gitlink submodule.
    let sub_git = dir.path().join("sub_git");
    fs::create_dir_all(&sub_git).unwrap();
    run_git(&sub_git, &["init", "-b", "main"]);
    fs::write(sub_git.join("sub.txt"), "submodule v1\n").unwrap();
    run_git(&sub_git, &["add", "."]);
    run_git(&sub_git, &["commit", "-m", "submodule commit"]);
    let sub_commit_hex = run_git(&sub_git, &["rev-parse", "HEAD"]);

    // Parent Git repo with `vendor/sub` (`160000`) plus `src/foo/mod.rs` and `src/foo.rs`.
    let parent_git = dir.path().join("parent_git");
    fs::create_dir_all(parent_git.join("src/foo")).unwrap();
    run_git(&parent_git, &["init", "-b", "main"]);
    fs::write(parent_git.join("src/foo/mod.rs"), "pub mod inner;\n").unwrap();
    fs::write(parent_git.join("src/foo.rs"), "pub fn top() {}\n").unwrap();
    fs::write(
        parent_git.join(".gitmodules"),
        "[submodule \"vendor/sub\"]\n\tpath = vendor/sub\n\turl = https://example.com/sub.git\n",
    )
    .unwrap();
    run_git(&parent_git, &["add", "."]);
    run_git(
        &parent_git,
        &[
            "update-index",
            "--add",
            "--cacheinfo",
            &format!("160000,{sub_commit_hex},vendor/sub"),
        ],
    );
    run_git(
        &parent_git,
        &[
            "commit",
            "-m",
            "parent commit with submodule and foo/foo.rs",
        ],
    );
    let expected_commit = run_git(&parent_git, &["rev-parse", "main"]);
    let expected_tree = run_git(&parent_git, &["rev-parse", "main^{tree}"]);

    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "import-git",
            "--server",
            server.url(),
            "--project-id",
            "proj-submod",
            "--repo-id",
            "repo-submod",
            "--create",
            "--git-dir",
            parent_git.to_str().unwrap(),
        ])
        .assert()
        .success();

    // `Commit.external_ids["git-sha1"]` is recorded on import.
    let expected_commit_bytes = hex::decode(&expected_commit).unwrap();
    let server_url = server.url().to_string();
    let stored_commit = cc_lib::util::run_on_tokio(async move {
        let mut client = cc_lib::util::connect_backend_client(server_url).await?;
        let resp = client
            .read_commit(cc_common::backend::ReadCommitRequest {
                project_id: "repo-submod".to_string(),
                commit_id: expected_commit_bytes.clone(),
            })
            .await?
            .into_inner();
        Ok(resp.commit.expect("commit present"))
    })
    .await
    .unwrap();
    assert_eq!(
        stored_commit.external_ids.get("git-sha1").map(hex::encode),
        Some(expected_commit.clone())
    );

    // Clone workspace and export back to bare Git, verifying `160000` gitlink and 1:1 SHA-1.
    let ws = dir.path().join("ws_submod");
    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "clone",
            "--server",
            server.url(),
            "-r",
            "main",
            "repo-submod",
            ws.to_str().unwrap(),
        ])
        .assert()
        .success();

    let exported_git = dir.path().join("exported_submod.git");
    jj_cmd(&ws, &xdg)
        .args([
            "cc",
            "export-git",
            "--bare",
            "--git-dir",
            exported_git.to_str().unwrap(),
        ])
        .assert()
        .success();

    run_git_bare(&exported_git, &["fsck", "--full"]);
    assert_eq!(
        run_git_bare(&exported_git, &["rev-parse", "main"]),
        expected_commit
    );
    assert_eq!(
        run_git_bare(&exported_git, &["rev-parse", "main^{tree}"]),
        expected_tree
    );
    let ls_sub = run_git_bare(&exported_git, &["ls-tree", "main", "vendor/sub"]);
    assert!(
        ls_sub.contains(&format!("160000 commit {sub_commit_hex}\tvendor/sub")),
        "expected 160000 gitlink entry in exported tree, got: {ls_sub}"
    );
}

#[tokio::test]
async fn test_export_git_skips_bookmarks_that_are_not_valid_git_branch_names() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_server().await;

    let ws = dir.path().join("ws_names");
    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--create",
            ws.to_str().unwrap(),
        ])
        .assert()
        .success();
    fs::write(ws.join("file.txt"), "bookmarked\n").unwrap();
    jj_cmd(&ws, &xdg)
        .args(["describe", "-m", "bookmarked commit"])
        .assert()
        .success();
    let commit_hex = String::from_utf8(
        jj_cmd(&ws, &xdg)
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

    // Files that a path-joined bookmark name would overwrite: an absolute path, and
    // `<dst>/refs/heads/../../../victim_rel`, which resolves to `<tempdir>/victim_rel`.
    let victim_abs = dir.path().join("victim_abs");
    let victim_rel = dir.path().join("victim_rel");
    fs::write(&victim_abs, "original\n").unwrap();
    fs::write(&victim_rel, "original\n").unwrap();
    let invalid_names = [
        victim_abs.to_str().unwrap().to_string(),
        "../x".to_string(),
        "../../../victim_rel".to_string(),
        "has space".to_string(),
        "tilde~1".to_string(),
        "ends.lock".to_string(),
        "HEAD".to_string(),
    ];
    for name in invalid_names.iter().map(String::as_str).chain(["good"]) {
        jj_cmd(&ws, &xdg)
            .args(["bookmark", "create", "-r", "@", &format!("{name:?}")])
            .assert()
            .success();
    }

    let dst_git = dir.path().join("exported_names.git");
    let export_stderr = String::from_utf8(
        jj_cmd(&ws, &xdg)
            .args([
                "cc",
                "export-git",
                "--bare",
                "--git-dir",
                dst_git.to_str().unwrap(),
            ])
            .assert()
            .success()
            .get_output()
            .stderr
            .clone(),
    )
    .unwrap();

    for name in &invalid_names {
        assert!(
            export_stderr.contains(&format!("Skipped bookmark {name:?}")),
            "expected a warning for {name:?}, got: {export_stderr}"
        );
    }
    assert_eq!(fs::read_to_string(&victim_abs).unwrap(), "original\n");
    assert_eq!(fs::read_to_string(&victim_rel).unwrap(), "original\n");
    assert!(!dst_git.join("refs/heads/has space").exists());
    run_git_bare(&dst_git, &["fsck", "--full"]);
    assert_eq!(
        run_git_bare(&dst_git, &["for-each-ref", "--format=%(refname)"]),
        "refs/heads/good"
    );
    assert_eq!(
        run_git_bare(&dst_git, &["symbolic-ref", "HEAD"]),
        "refs/heads/good"
    );
    assert_eq!(run_git_bare(&dst_git, &["rev-parse", "good"]), commit_hex);
}

#[tokio::test]
async fn test_import_git_reads_only_jj_visible_refs_and_leaves_source_untouched() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_server().await;

    for ref_format in ["files", "reftable"] {
        let src_git = dir.path().join(format!("src_{ref_format}"));
        if !git_init_with_ref_format(&src_git, ref_format) {
            eprintln!("skipping {ref_format}: this Git cannot create it");
            continue;
        }
        fs::write(src_git.join("a.txt"), "a\n").unwrap();
        run_git(&src_git, &["add", "."]);
        run_git(&src_git, &["commit", "-m", "main commit"]);
        let main = run_git(&src_git, &["rev-parse", "HEAD"]);
        let tree = run_git(&src_git, &["rev-parse", "HEAD^{tree}"]);
        let child = |msg: &str| run_git(&src_git, &["commit-tree", &tree, "-p", &main, "-m", msg]);
        let tagged = child("reachable only from a lightweight tag");
        let annotated = child("reachable only from an annotated tag");
        let remote = child("reachable only from origin/feature");
        let kept = child("reachable only from refs/jj/keep");
        let pulled = child("reachable only from refs/pull");
        run_git(&src_git, &["tag", "v1", &tagged]);
        run_git(
            &src_git,
            &["tag", "-a", "v2", "-m", "annotated v2", &annotated],
        );
        run_git(
            &src_git,
            &["update-ref", "refs/remotes/origin/feature", &remote],
        );
        run_git(
            &src_git,
            &[
                "symbolic-ref",
                "refs/remotes/origin/HEAD",
                "refs/remotes/origin/feature",
            ],
        );
        run_git(
            &src_git,
            &["update-ref", &format!("refs/jj/keep/{kept}"), &kept],
        );
        run_git(&src_git, &["update-ref", "refs/pull/1/head", &pulled]);
        run_git(&src_git, &["notes", "add", "-m", "a note", "HEAD"]);
        fs::write(src_git.join("a.txt"), "stashed edit\n").unwrap();
        run_git(&src_git, &["stash"]);

        let refs_before = run_git(
            &src_git,
            &["for-each-ref", "--format=%(refname) %(objectname)"],
        );
        let head_file_before = fs::read(src_git.join(".git/HEAD")).unwrap();

        let repo_id = format!("repo-refs-{ref_format}");
        let import_stdout = String::from_utf8(
            jj_cmd(dir.path(), &xdg)
                .args([
                    "cc",
                    "import-git",
                    "--server",
                    server.url(),
                    "--repo-id",
                    &repo_id,
                    "--create",
                    "--git-dir",
                    src_git.to_str().unwrap(),
                ])
                .assert()
                .success()
                .get_output()
                .stdout
                .clone(),
        )
        .unwrap();
        assert!(
            import_stdout.contains("Commits: 4"),
            "{ref_format}: {import_stdout}"
        );

        // The importer must not rewrite HEAD (reftable keeps a `refs/heads/.invalid` stub) or refs.
        assert_eq!(
            run_git(
                &src_git,
                &["for-each-ref", "--format=%(refname) %(objectname)"]
            ),
            refs_before
        );
        assert_eq!(
            fs::read(src_git.join(".git/HEAD")).unwrap(),
            head_file_before
        );

        let view = read_head_view(server.url(), &repo_id).await;
        let heads: HashSet<String> = view.head_ids.iter().map(hex::encode).collect();
        // `main` is an ancestor of `tagged`, `annotated`, and `remote`, so minimal-heads pruning
        // keeps only the three leaf heads in `view.head_ids`.
        assert_eq!(
            heads,
            HashSet::from([tagged.clone(), annotated.clone(), remote.clone()])
        );
        assert_eq!(view.local_bookmarks.keys().collect::<Vec<_>>(), ["main"]);
        assert_eq!(
            view.remote_bookmarks.keys().collect::<Vec<_>>(),
            ["feature@origin"]
        );
        assert!(
            !view.remote_bookmarks["feature@origin"].is_tracked,
            "jj git import leaves new remote bookmarks untracked"
        );

        // Creating a local `feature` bookmark must succeed because `feature@origin` is untracked.
        let ws = dir.path().join(format!("ws_{ref_format}"));
        jj_cmd(dir.path(), &xdg)
            .args([
                "cc",
                "clone",
                "--server",
                server.url(),
                "-r",
                "main",
                &repo_id,
                ws.to_str().unwrap(),
            ])
            .assert()
            .success();
        jj_cmd(&ws, &xdg)
            .args(["bookmark", "create", "feature", "-r", "@"])
            .assert()
            .success();

        // Commits without a `change-id` header get the same change ID as in jj's GitBackend.
        let server_url = server.url().to_string();
        let main_id = hex::decode(&main).unwrap();
        let stored = cc_lib::util::run_on_tokio(async move {
            let mut client = cc_lib::util::connect_backend_client(server_url).await?;
            let resp = client
                .read_commit(cc_common::backend::ReadCommitRequest {
                    project_id: repo_id,
                    commit_id: main_id,
                })
                .await?
                .into_inner();
            Ok(resp.commit.expect("commit present"))
        })
        .await
        .unwrap();
        let expected_change_id =
            synthetic_change_id_from_git_commit_id(&CommitId::try_from_hex(&main).unwrap());
        assert_eq!(stored.change_id, expected_change_id.as_bytes());
    }
}

#[tokio::test]
async fn test_import_git_from_jj_colocated_repo_excludes_keep_refs_and_fabricates_no_main() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_server().await;

    let colocated = dir.path().join("colocated_src");
    jj_cmd(dir.path(), &xdg)
        .args(["git", "init", "--colocate", colocated.to_str().unwrap()])
        .assert()
        .success();
    fs::write(colocated.join("note.txt"), "obsolete v1\n").unwrap();
    jj_cmd(&colocated, &xdg)
        .args(["describe", "-m", "v1"])
        .assert()
        .success();
    fs::write(colocated.join("note.txt"), "live v2\n").unwrap();
    jj_cmd(&colocated, &xdg)
        .args(["describe", "-m", "v2"])
        .assert()
        .success();
    // In a colocated jj workspace, `.git/HEAD` tracks `@-` while `refs/jj/keep/*` retains `@`
    // and every rewritten predecessor (`v1`).
    jj_cmd(&colocated, &xdg).args(["new"]).assert().success();

    let live_commit = String::from_utf8(
        jj_cmd(&colocated, &xdg)
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
    let keep_refs = run_git(&colocated, &["for-each-ref", "refs/jj/keep"]);
    assert!(
        keep_refs.lines().count() >= 2,
        "rewriting `@` in a colocated jj repo should leave multiple refs/jj/keep/* entries: {keep_refs}"
    );
    let head_before = fs::read(colocated.join(".git/HEAD")).unwrap();

    let import_stdout = String::from_utf8(
        jj_cmd(dir.path(), &xdg)
            .args([
                "cc",
                "import-git",
                "--server",
                server.url(),
                "--repo-id",
                "repo-colocated",
                "--create",
                "--git-dir",
                colocated.to_str().unwrap(),
            ])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap();
    assert!(import_stdout.contains("Commits: 1"), "got: {import_stdout}");
    assert_eq!(fs::read(colocated.join(".git/HEAD")).unwrap(), head_before);

    let view = read_head_view(server.url(), "repo-colocated").await;
    let heads: HashSet<String> = view.head_ids.iter().map(hex::encode).collect();
    assert_eq!(heads, HashSet::from([live_commit.clone()]));
    assert!(
        view.local_bookmarks.is_empty(),
        "importer must not fabricate a `main` bookmark when none exist: {:?}",
        view.local_bookmarks.keys().collect::<Vec<_>>()
    );

    let cloned = dir.path().join("cloned_colocated");
    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "clone",
            "--server",
            server.url(),
            "repo-colocated",
            cloned.to_str().unwrap(),
        ])
        .assert()
        .success();
    assert_eq!(
        fs::read_to_string(cloned.join("note.txt")).unwrap(),
        "live v2\n"
    );
}

#[tokio::test]
async fn test_export_git_empty_commits_pass_fsck() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_server().await;

    let ws = dir.path().join("ws_empty");
    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--repo-id",
            "repo-empty-commits",
            "--create",
            ws.to_str().unwrap(),
        ])
        .assert()
        .success();

    jj_cmd(&ws, &xdg)
        .args(["describe", "-m", "empty root commit"])
        .assert()
        .success();
    let c1 = String::from_utf8(
        jj_cmd(&ws, &xdg)
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

    jj_cmd(&ws, &xdg)
        .args(["new", "-m", "chained empty commit"])
        .assert()
        .success();
    jj_cmd(&ws, &xdg)
        .args(["bookmark", "create", "main", "-r", "@"])
        .assert()
        .success();
    let c2 = String::from_utf8(
        jj_cmd(&ws, &xdg)
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

    let dst_git = dir.path().join("exported_empty.git");
    jj_cmd(&ws, &xdg)
        .args([
            "cc",
            "export-git",
            "--bare",
            "--git-dir",
            dst_git.to_str().unwrap(),
        ])
        .assert()
        .success();

    run_git_bare(&dst_git, &["fsck", "--full"]);
    assert_eq!(run_git_bare(&dst_git, &["rev-parse", "main"]), c2);
    assert_eq!(run_git_bare(&dst_git, &["rev-parse", "main~1"]), c1);
    assert_eq!(
        run_git_bare(&dst_git, &["rev-parse", "main^{tree}"]),
        hex::encode(cc_common::EMPTY_TREE_ID_BYTES)
    );
}

#[tokio::test]
async fn test_export_git_preserves_unbookmarked_commit_beneath_empty_working_copy_and_empty_merge()
{
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_server().await;

    let ws = dir.path().join("ws_unbookmarked");
    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--repo-id",
            "repo-unbookmarked",
            "--create",
            ws.to_str().unwrap(),
        ])
        .assert()
        .success();

    fs::write(ws.join("work.txt"), "unbookmarked\n").unwrap();
    jj_cmd(&ws, &xdg)
        .args(["describe", "-m", "unbookmarked work"])
        .assert()
        .success();
    let work_id = String::from_utf8(
        jj_cmd(&ws, &xdg)
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

    // Leave an empty working-copy commit on top with zero bookmarks anywhere.
    jj_cmd(&ws, &xdg).args(["new"]).assert().success();

    let dst_git = dir.path().join("exported_unbookmarked.git");
    jj_cmd(&ws, &xdg)
        .args([
            "cc",
            "export-git",
            "--bare",
            "--git-dir",
            dst_git.to_str().unwrap(),
        ])
        .assert()
        .success();

    run_git_bare(&dst_git, &["fsck", "--full"]);
    assert_eq!(run_git_bare(&dst_git, &["rev-parse", "main"]), work_id);
    assert_eq!(
        run_git_bare(&dst_git, &["show", "main:work.txt"]),
        "unbookmarked"
    );

    // An empty-description merge commit whose tree equals its first parent's tree must NOT be
    // dropped as an empty working-copy commit.
    jj_cmd(&ws, &xdg)
        .args(["new", "root()", "-m", "empty side branch"])
        .assert()
        .success();
    let empty_side = String::from_utf8(
        jj_cmd(&ws, &xdg)
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

    jj_cmd(&ws, &xdg)
        .args(["new", &work_id, &empty_side])
        .assert()
        .success();
    let merge_wc = String::from_utf8(
        jj_cmd(&ws, &xdg)
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

    let dst_merge = dir.path().join("exported_empty_merge.git");
    jj_cmd(&ws, &xdg)
        .args([
            "cc",
            "export-git",
            "--bare",
            "--git-dir",
            dst_merge.to_str().unwrap(),
        ])
        .assert()
        .success();

    run_git_bare(&dst_merge, &["fsck", "--full"]);
    assert_eq!(run_git_bare(&dst_merge, &["rev-parse", "main"]), merge_wc);
    assert_eq!(run_git_bare(&dst_merge, &["rev-parse", "main^1"]), work_id);
    assert_eq!(
        run_git_bare(&dst_merge, &["rev-parse", "main^2"]),
        empty_side
    );
}

#[tokio::test]
async fn test_import_git_rejects_mismatched_project_id_and_unregistered_repo_without_create() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_server().await;

    let src_a = dir.path().join("src_a");
    fs::create_dir_all(&src_a).unwrap();
    run_git(&src_a, &["init", "-b", "main"]);
    fs::write(src_a.join("a.txt"), "a\n").unwrap();
    run_git(&src_a, &["add", "."]);
    run_git(&src_a, &["commit", "-m", "commit a"]);
    let commit_a = run_git(&src_a, &["rev-parse", "HEAD"]);

    let src_b = dir.path().join("src_b");
    fs::create_dir_all(&src_b).unwrap();
    run_git(&src_b, &["init", "-b", "main"]);
    fs::write(src_b.join("b.txt"), "b\n").unwrap();
    run_git(&src_b, &["add", "."]);
    run_git(&src_b, &["commit", "-m", "commit b"]);
    let commit_b = run_git(&src_b, &["rev-parse", "HEAD"]);

    // Importing into an unregistered `--repo-id` without `--create` must fail and must not
    // register the repo.
    let unreg_stderr = String::from_utf8(
        jj_cmd(dir.path(), &xdg)
            .args([
                "cc",
                "import-git",
                "--server",
                server.url(),
                "--repo-id",
                "repo-missing",
                "--git-dir",
                src_a.to_str().unwrap(),
            ])
            .assert()
            .failure()
            .get_output()
            .stderr
            .clone(),
    )
    .unwrap();
    assert!(
        unreg_stderr.contains("not registered"),
        "unexpected stderr: {unreg_stderr}"
    );
    let server_url = server.url().to_string();
    let missing_status = cc_lib::util::run_on_tokio(async move {
        let mut client = cc_lib::util::connect_op_store_client(server_url).await?;
        let res = client
            .get_op_heads(cc_common::op_store::GetOpHeadsRequest {
                repo_id: "repo-missing".to_string(),
            })
            .await;
        Ok(res.err().map(|s| s.code()))
    })
    .await
    .unwrap();
    assert_eq!(missing_status, Some(tonic::Code::NotFound));

    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "import-git",
            "--server",
            server.url(),
            "--project-id",
            "proj-a",
            "--repo-id",
            "repo-bound",
            "--create",
            "--git-dir",
            src_a.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Re-importing into `repo-bound` with a mismatched `--project-id proj-b` (and no
    // `--create`) must fail closed before uploading any objects.
    let mismatch_stderr = String::from_utf8(
        jj_cmd(dir.path(), &xdg)
            .args([
                "cc",
                "import-git",
                "--server",
                server.url(),
                "--project-id",
                "proj-b",
                "--repo-id",
                "repo-bound",
                "--git-dir",
                src_b.to_str().unwrap(),
            ])
            .assert()
            .failure()
            .get_output()
            .stderr
            .clone(),
    )
    .unwrap();
    assert!(
        mismatch_stderr.contains("proj-a") || mismatch_stderr.contains("already bound"),
        "expected project mismatch error, got: {mismatch_stderr}"
    );

    // Neither `proj-a` nor `proj-b` should contain `commit_b`, and `repo-bound`'s view must stay
    // at `commit_a`.
    let view = read_head_view(server.url(), "repo-bound").await;
    let heads: HashSet<String> = view.head_ids.iter().map(hex::encode).collect();
    assert_eq!(heads, HashSet::from([commit_a.clone()]));
    let server_url = server.url().to_string();
    let commit_b_bytes = hex::decode(&commit_b).unwrap();
    let (in_proj_a, in_proj_b) = cc_lib::util::run_on_tokio(async move {
        let mut client = cc_lib::util::connect_backend_client(server_url).await?;
        let res_a = client
            .read_commit(cc_common::backend::ReadCommitRequest {
                project_id: "proj-a".to_string(),
                commit_id: commit_b_bytes.clone(),
            })
            .await;
        let res_b = client
            .read_commit(cc_common::backend::ReadCommitRequest {
                project_id: "proj-b".to_string(),
                commit_id: commit_b_bytes,
            })
            .await;
        Ok((res_a.is_ok(), res_b.is_ok()))
    })
    .await
    .unwrap();
    assert!(!in_proj_a, "commit_b must not be uploaded to proj-a");
    assert!(!in_proj_b, "commit_b must not be uploaded to proj-b");

    // Matching `--project-id proj-a` without `--create` succeeds.
    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "import-git",
            "--server",
            server.url(),
            "--project-id",
            "proj-a",
            "--repo-id",
            "repo-bound",
            "--git-dir",
            src_b.to_str().unwrap(),
        ])
        .assert()
        .success();
    let view_after = read_head_view(server.url(), "repo-bound").await;
    let heads_after: HashSet<String> = view_after.head_ids.iter().map(hex::encode).collect();
    assert_eq!(heads_after, HashSet::from([commit_a, commit_b]));
}

#[tokio::test]
async fn test_import_git_into_existing_repo_preserves_unrelated_commits_and_prunes_ancestor_heads()
{
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_server().await;

    // Existing Commit Cloud repository with its own unrelated commit.
    let ws = dir.path().join("ws_existing");
    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--repo-id",
            "repo-existing",
            "--create",
            ws.to_str().unwrap(),
        ])
        .assert()
        .success();
    fs::write(ws.join("cloud.txt"), "cloud\n").unwrap();
    jj_cmd(&ws, &xdg)
        .args(["describe", "-m", "cloud commit"])
        .assert()
        .success();
    jj_cmd(&ws, &xdg)
        .args(["bookmark", "create", "cc-main", "-r", "@"])
        .assert()
        .success();
    let cc_commit = String::from_utf8(
        jj_cmd(&ws, &xdg)
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

    // Import a two-commit Git repo (`g1` -> `g2`) with branches at both commits.
    let src_git = dir.path().join("src_existing");
    fs::create_dir_all(&src_git).unwrap();
    run_git(&src_git, &["init", "-b", "git-main"]);
    fs::write(src_git.join("g.txt"), "v1\n").unwrap();
    run_git(&src_git, &["add", "."]);
    run_git(&src_git, &["commit", "-m", "g1"]);
    run_git(&src_git, &["branch", "git-old"]);
    fs::write(src_git.join("g.txt"), "v2\n").unwrap();
    run_git(&src_git, &["add", "."]);
    run_git(&src_git, &["commit", "-m", "g2"]);
    let g2 = run_git(&src_git, &["rev-parse", "git-main"]);

    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "import-git",
            "--server",
            server.url(),
            "--repo-id",
            "repo-existing",
            "--git-dir",
            src_git.to_str().unwrap(),
        ])
        .assert()
        .success();

    let view1 = read_head_view(server.url(), "repo-existing").await;
    let heads1: HashSet<String> = view1.head_ids.iter().map(hex::encode).collect();
    assert_eq!(heads1, HashSet::from([cc_commit.clone(), g2.clone()]));
    assert!(view1.local_bookmarks.contains_key("cc-main"));
    assert!(view1.local_bookmarks.contains_key("git-old"));
    assert!(view1.local_bookmarks.contains_key("git-main"));

    // Advance `git-main` to `g3` and re-import: `g2` must be pruned from `view.head_ids`.
    fs::write(src_git.join("g.txt"), "v3\n").unwrap();
    run_git(&src_git, &["add", "."]);
    run_git(&src_git, &["commit", "-m", "g3"]);
    let g3 = run_git(&src_git, &["rev-parse", "git-main"]);

    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "import-git",
            "--server",
            server.url(),
            "--repo-id",
            "repo-existing",
            "--git-dir",
            src_git.to_str().unwrap(),
        ])
        .assert()
        .success();

    let view2 = read_head_view(server.url(), "repo-existing").await;
    let heads2: HashSet<String> = view2.head_ids.iter().map(hex::encode).collect();
    assert_eq!(heads2, HashSet::from([cc_commit.clone(), g3.clone()]));

    // Author a Commit Cloud commit on top of `g3`, then re-import the older Git tip `g3`:
    // `g3` must NOT be re-added to `view.head_ids` because it is now an ancestor of `cc_on_g3`.
    jj_cmd(&ws, &xdg)
        .args(["new", "git-main", "-m", "cloud child of g3"])
        .assert()
        .success();
    let cc_on_g3 = String::from_utf8(
        jj_cmd(&ws, &xdg)
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

    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "import-git",
            "--server",
            server.url(),
            "--repo-id",
            "repo-existing",
            "--git-dir",
            src_git.to_str().unwrap(),
        ])
        .assert()
        .success();

    let view3 = read_head_view(server.url(), "repo-existing").await;
    let heads3: HashSet<String> = view3.head_ids.iter().map(hex::encode).collect();
    assert_eq!(heads3, HashSet::from([cc_commit, cc_on_g3]));
    assert!(
        !heads3.contains(&g3),
        "ancestor g3 must not be re-added as a head"
    );
}

#[tokio::test]
async fn test_import_and_export_preserve_non_utc_timezones_and_change_id_header() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_server().await;

    let src_git = dir.path().join("src_tz");
    fs::create_dir_all(&src_git).unwrap();
    run_git(&src_git, &["init", "-b", "main"]);
    fs::write(src_git.join("tz.txt"), "hello tz\n").unwrap();
    run_git(&src_git, &["add", "."]);
    let tree_hex = run_git(&src_git, &["write-tree"]);

    let change_id_rev_hex = "zyxwvutsrqponmlkklmnopqrstuvwxyz";
    let expected_change_id =
        jj_lib::hex_util::decode_reverse_hex(change_id_rev_hex.as_bytes()).unwrap();
    let raw_commit = format!(
        "tree {tree_hex}\n\
         author TZ Author <author@example.com> 1730000000 +0530\n\
         committer TZ Committer <committer@example.com> 1730000100 -0700\n\
         change-id {change_id_rev_hex}\n\
         \n\
         commit with non-UTC offsets and explicit change-id\n"
    );

    let mut hash_child = StdCommand::new("git")
        .current_dir(&src_git)
        .args(["hash-object", "-t", "commit", "-w", "--stdin"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("git hash-object should spawn");
    {
        use std::io::Write as _;
        hash_child
            .stdin
            .as_mut()
            .unwrap()
            .write_all(raw_commit.as_bytes())
            .unwrap();
    }
    let hash_out = hash_child.wait_with_output().unwrap();
    assert!(hash_out.status.success());
    let commit_hex = String::from_utf8(hash_out.stdout)
        .unwrap()
        .trim()
        .to_string();
    run_git(&src_git, &["update-ref", "refs/heads/main", &commit_hex]);

    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "import-git",
            "--server",
            server.url(),
            "--repo-id",
            "repo-tz",
            "--create",
            "--git-dir",
            src_git.to_str().unwrap(),
        ])
        .assert()
        .success();

    let server_url = server.url().to_string();
    let commit_bytes = hex::decode(&commit_hex).unwrap();
    let stored = cc_lib::util::run_on_tokio(async move {
        let mut client = cc_lib::util::connect_backend_client(server_url).await?;
        let resp = client
            .read_commit(cc_common::backend::ReadCommitRequest {
                project_id: "repo-tz".to_string(),
                commit_id: commit_bytes,
            })
            .await?
            .into_inner();
        Ok(resp.commit.expect("commit present"))
    })
    .await
    .unwrap();
    assert_eq!(stored.change_id, expected_change_id);
    assert_eq!(
        stored
            .author
            .as_ref()
            .unwrap()
            .timestamp
            .as_ref()
            .unwrap()
            .tz_offset,
        330
    );
    assert_eq!(
        stored
            .committer
            .as_ref()
            .unwrap()
            .timestamp
            .as_ref()
            .unwrap()
            .tz_offset,
        -420
    );

    let dst_git = dir.path().join("exported_tz.git");
    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "export-git",
            "--server",
            server.url(),
            "--repo-id",
            "repo-tz",
            "--bare",
            "--git-dir",
            dst_git.to_str().unwrap(),
        ])
        .assert()
        .success();

    run_git_bare(&dst_git, &["fsck", "--full"]);
    assert_eq!(run_git_bare(&dst_git, &["rev-parse", "main"]), commit_hex);
    let exported_raw = run_git_bare(&dst_git, &["cat-file", "commit", "main"]);
    assert_eq!(exported_raw, raw_commit.trim_end());
}

#[tokio::test]
async fn test_roundtrip_conflicted_jj_commit_with_synthetic_tree_passes_fsck_and_reports_conflicted_bookmarks()
 {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_server().await;

    // Construct a colocated jj Git repository containing a conflicted merge commit with
    // `jj:trees` + synthetic `.jjconflict` Git tree.
    let colocated = dir.path().join("colocated_conflict");
    jj_cmd(dir.path(), &xdg)
        .args(["git", "init", "--colocate", colocated.to_str().unwrap()])
        .assert()
        .success();
    fs::write(colocated.join("conflict.txt"), "base\n").unwrap();
    jj_cmd(&colocated, &xdg)
        .args(["describe", "-m", "base"])
        .assert()
        .success();
    let base_id = String::from_utf8(
        jj_cmd(&colocated, &xdg)
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

    jj_cmd(&colocated, &xdg)
        .args(["new", &base_id, "-m", "left"])
        .assert()
        .success();
    fs::write(colocated.join("conflict.txt"), "left\n").unwrap();
    let left_id = String::from_utf8(
        jj_cmd(&colocated, &xdg)
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

    jj_cmd(&colocated, &xdg)
        .args(["new", &base_id, "-m", "right"])
        .assert()
        .success();
    fs::write(colocated.join("conflict.txt"), "right\n").unwrap();
    let right_id = String::from_utf8(
        jj_cmd(&colocated, &xdg)
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

    jj_cmd(&colocated, &xdg)
        .args(["new", &left_id, &right_id, "-m", "conflicted merge"])
        .assert()
        .success();
    jj_cmd(&colocated, &xdg)
        .args(["bookmark", "create", "conflicted", "-r", "@"])
        .assert()
        .success();
    // Advance `@` so jj exports `refs/heads/conflicted` to the colocated Git repo.
    jj_cmd(&colocated, &xdg).args(["new"]).assert().success();
    let conflicted_commit = run_git(&colocated, &["rev-parse", "conflicted"]);
    let orig_raw_commit = run_git(&colocated, &["cat-file", "commit", "conflicted"]);
    let git_tree_header = orig_raw_commit
        .lines()
        .find_map(|l| l.strip_prefix("tree "))
        .expect("tree header present");
    let jj_trees_header = orig_raw_commit
        .lines()
        .find_map(|l| l.strip_prefix("jj:trees "))
        .expect("jj:trees header present on conflicted jj commit");
    let jj_tree_tokens: Vec<&str> = jj_trees_header.split(' ').collect();
    assert_eq!(jj_tree_tokens.len(), 3);
    assert!(
        !jj_tree_tokens.contains(&git_tree_header),
        "expected synthetic `.jjconflict` tree {git_tree_header} distinct from jj:trees {jj_tree_tokens:?}"
    );

    // Import into Commit Cloud and verify `root_tree_id` + `raw_git_commit`.
    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "import-git",
            "--server",
            server.url(),
            "--repo-id",
            "repo-conflict",
            "--create",
            "--git-dir",
            colocated.to_str().unwrap(),
        ])
        .assert()
        .success();

    let server_url = server.url().to_string();
    let conflicted_bytes = hex::decode(&conflicted_commit).unwrap();
    let stored = cc_lib::util::run_on_tokio(async move {
        let mut client = cc_lib::util::connect_backend_client(server_url).await?;
        let resp = client
            .read_commit(cc_common::backend::ReadCommitRequest {
                project_id: "repo-conflict".to_string(),
                commit_id: conflicted_bytes,
            })
            .await?
            .into_inner();
        Ok(resp.commit.expect("commit present"))
    })
    .await
    .unwrap();
    assert_eq!(stored.root_tree_id.len(), 3);
    assert!(!stored.raw_git_commit.is_empty());

    // Add a conflicted bookmark `diverge` to the repository `View` to verify
    // conflicted bookmark reporting alongside the conflicted commit round-trip.
    let server_url = server.url().to_string();
    let base_bytes = hex::decode(&base_id).unwrap();
    let left_bytes = hex::decode(&left_id).unwrap();
    let right_bytes = hex::decode(&right_id).unwrap();
    cc_lib::util::run_on_tokio(async move {
        let mut op_client = cc_lib::util::connect_op_store_client(server_url).await?;
        let op_head = op_client
            .reconcile_op_heads(ReconcileOpHeadsRequest {
                repo_id: "repo-conflict".to_string(),
            })
            .await?
            .into_inner()
            .op_head;
        let op = op_client
            .read_operation(ReadOperationRequest {
                repo_id: "repo-conflict".to_string(),
                operation_id: op_head.clone(),
            })
            .await?
            .into_inner()
            .operation
            .unwrap();
        let mut view = op_client
            .read_view(ReadViewRequest {
                repo_id: "repo-conflict".to_string(),
                view_id: op.view_id,
            })
            .await?
            .into_inner()
            .view
            .unwrap();
        view.local_bookmarks.insert(
            "diverge".to_string(),
            cc_common::op_store::RefTarget {
                removes: vec![cc_common::op_store::RefTargetTerm {
                    commit_id: base_bytes,
                }],
                adds: vec![
                    cc_common::op_store::RefTargetTerm {
                        commit_id: left_bytes,
                    },
                    cc_common::op_store::RefTargetTerm {
                        commit_id: right_bytes,
                    },
                ],
            },
        );
        op_client
            .publish_operation(cc_common::op_store::PublishOperationRequest {
                repo_id: "repo-conflict".to_string(),
                view: Some(view),
                operation: Some(cc_common::op_store::Operation {
                    view_id: Vec::new(),
                    parents: vec![op_head.clone()],
                    metadata: Some(cc_common::op_store::OperationMetadata {
                        description: "add conflicted bookmark".to_string(),
                        ..Default::default()
                    }),
                    commit_predecessors: Vec::new(),
                    commit_predecessors_set: false,
                }),
                old_op_head_ids: vec![op_head],
            })
            .await?;
        Ok(())
    })
    .await
    .unwrap();

    // Export back to bare Git and verify `git fsck --full`, bit-for-bit commit round-trip, and
    // conflicted bookmark warning.
    let dst_git = dir.path().join("exported_conflict.git");
    let export_stderr = String::from_utf8(
        jj_cmd(dir.path(), &xdg)
            .args([
                "cc",
                "export-git",
                "--server",
                server.url(),
                "--repo-id",
                "repo-conflict",
                "--bare",
                "--git-dir",
                dst_git.to_str().unwrap(),
            ])
            .assert()
            .success()
            .get_output()
            .stderr
            .clone(),
    )
    .unwrap();

    assert!(
        export_stderr.contains("Skipped conflicted bookmark \"diverge\""),
        "expected conflicted bookmark warning, got: {export_stderr}"
    );
    run_git_bare(&dst_git, &["fsck", "--full"]);
    assert_eq!(
        run_git_bare(&dst_git, &["rev-parse", "conflicted"]),
        conflicted_commit
    );
    assert_eq!(
        run_git_bare(&dst_git, &["cat-file", "commit", "conflicted"]),
        orig_raw_commit
    );
    assert_eq!(
        run_git_bare(&dst_git, &["for-each-ref", "--format=%(refname)"]),
        "refs/heads/conflicted"
    );
}

#[tokio::test]
async fn test_import_git_reftable_relative_git_dir_and_linked_worktree() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_server().await;

    let main_repo = dir.path().join("reftable_rel");
    if !git_init_with_ref_format(&main_repo, "reftable") {
        eprintln!("skipping reftable relative git-dir test: this Git cannot create reftable repos");
        return;
    }
    fs::write(main_repo.join("hello.txt"), "reftable relative\n").unwrap();
    run_git(&main_repo, &["add", "."]);
    run_git(&main_repo, &["commit", "-m", "reftable main commit"]);
    let main_commit = run_git(&main_repo, &["rev-parse", "main"]);

    // Import using a relative `--git-dir` path (`reftable_rel`) from its parent directory.
    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "import-git",
            "--server",
            server.url(),
            "--repo-id",
            "repo-reftable-rel",
            "--create",
            "--git-dir",
            "reftable_rel",
        ])
        .assert()
        .success();

    let view = read_head_view(server.url(), "repo-reftable-rel").await;
    assert_eq!(
        hex::encode(&view.local_bookmarks["main"].adds[0].commit_id),
        main_commit
    );

    // Linked Git worktree (`.git` pointer file + `commondir`) with a relative path.
    let wt_dir = dir.path().join("reftable_wt");
    run_git(
        &main_repo,
        &[
            "worktree",
            "add",
            "-b",
            "wt-branch",
            wt_dir.to_str().unwrap(),
        ],
    );
    fs::write(wt_dir.join("wt.txt"), "from worktree\n").unwrap();
    run_git(&wt_dir, &["add", "."]);
    run_git(&wt_dir, &["commit", "-m", "worktree commit"]);
    let wt_commit = run_git(&wt_dir, &["rev-parse", "wt-branch"]);

    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "import-git",
            "--server",
            server.url(),
            "--repo-id",
            "repo-reftable-wt",
            "--create",
            "--git-dir",
            "./reftable_wt",
        ])
        .assert()
        .success();

    let wt_view = read_head_view(server.url(), "repo-reftable-wt").await;
    assert_eq!(
        hex::encode(&wt_view.local_bookmarks["main"].adds[0].commit_id),
        main_commit
    );
    assert_eq!(
        hex::encode(&wt_view.local_bookmarks["wt-branch"].adds[0].commit_id),
        wt_commit
    );
}

#[tokio::test]
async fn test_export_git_does_not_synthesize_main_when_only_bookmark_is_conflicted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_server().await;

    let ws = dir.path().join("ws_conflicted_main");
    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--repo-id",
            "repo-conflicted-main",
            "--create",
            ws.to_str().unwrap(),
        ])
        .assert()
        .success();

    fs::write(ws.join("f.txt"), "base\n").unwrap();
    jj_cmd(&ws, &xdg)
        .args(["describe", "-m", "base"])
        .assert()
        .success();
    let base_id = String::from_utf8(
        jj_cmd(&ws, &xdg)
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

    jj_cmd(&ws, &xdg)
        .args(["new", &base_id, "-m", "left"])
        .assert()
        .success();
    fs::write(ws.join("f.txt"), "left\n").unwrap();
    let left_id = String::from_utf8(
        jj_cmd(&ws, &xdg)
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

    jj_cmd(&ws, &xdg)
        .args(["new", &base_id, "-m", "right"])
        .assert()
        .success();
    fs::write(ws.join("f.txt"), "right\n").unwrap();
    let right_id = String::from_utf8(
        jj_cmd(&ws, &xdg)
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

    // Publish a View whose sole local bookmark is a conflicted `main`.
    let server_url = server.url().to_string();
    let base_bytes = hex::decode(&base_id).unwrap();
    let left_bytes = hex::decode(&left_id).unwrap();
    let right_bytes = hex::decode(&right_id).unwrap();
    cc_lib::util::run_on_tokio(async move {
        let mut op_client = cc_lib::util::connect_op_store_client(server_url).await?;
        let op_head = op_client
            .reconcile_op_heads(ReconcileOpHeadsRequest {
                repo_id: "repo-conflicted-main".to_string(),
            })
            .await?
            .into_inner()
            .op_head;
        let op = op_client
            .read_operation(ReadOperationRequest {
                repo_id: "repo-conflicted-main".to_string(),
                operation_id: op_head.clone(),
            })
            .await?
            .into_inner()
            .operation
            .unwrap();
        let mut view = op_client
            .read_view(ReadViewRequest {
                repo_id: "repo-conflicted-main".to_string(),
                view_id: op.view_id,
            })
            .await?
            .into_inner()
            .view
            .unwrap();
        view.local_bookmarks.clear();
        view.local_bookmarks.insert(
            "main".to_string(),
            cc_common::op_store::RefTarget {
                removes: vec![cc_common::op_store::RefTargetTerm {
                    commit_id: base_bytes,
                }],
                adds: vec![
                    cc_common::op_store::RefTargetTerm {
                        commit_id: left_bytes,
                    },
                    cc_common::op_store::RefTargetTerm {
                        commit_id: right_bytes,
                    },
                ],
            },
        );
        op_client
            .publish_operation(cc_common::op_store::PublishOperationRequest {
                repo_id: "repo-conflicted-main".to_string(),
                view: Some(view),
                operation: Some(cc_common::op_store::Operation {
                    view_id: Vec::new(),
                    parents: vec![op_head.clone()],
                    metadata: Some(cc_common::op_store::OperationMetadata {
                        description: "add conflicted main bookmark".to_string(),
                        ..Default::default()
                    }),
                    commit_predecessors: Vec::new(),
                    commit_predecessors_set: true,
                }),
                old_op_head_ids: vec![op_head],
            })
            .await?;
        Ok(())
    })
    .await
    .unwrap();

    let dst_git = dir.path().join("exported_conflicted_main.git");
    let export_stderr = String::from_utf8(
        jj_cmd(&ws, &xdg)
            .args([
                "cc",
                "export-git",
                "--bare",
                "--git-dir",
                dst_git.to_str().unwrap(),
            ])
            .assert()
            .success()
            .get_output()
            .stderr
            .clone(),
    )
    .unwrap();

    assert!(
        export_stderr.contains(
            "Skipped conflicted bookmark \"main\": resolve with `jj bookmark set \"main\" -r <rev>`"
        ),
        "expected quoted conflicted bookmark hint, got: {export_stderr}"
    );
    run_git_bare(&dst_git, &["fsck", "--full"]);
    assert_eq!(
        run_git_bare(&dst_git, &["for-each-ref", "--format=%(refname)"]),
        "",
        "export-git must NOT synthesize refs/heads/main when main was conflicted"
    );
}

#[tokio::test]
async fn test_import_git_concurrent_workspace_op_reconciles_automatically() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_server().await;

    let ws = dir.path().join("ws_race");
    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--repo-id",
            "repo-race",
            "--create",
            ws.to_str().unwrap(),
        ])
        .assert()
        .success();
    fs::write(ws.join("base.txt"), "base\n").unwrap();
    jj_cmd(&ws, &xdg)
        .args(["describe", "-m", "initial cloud commit"])
        .assert()
        .success();
    jj_cmd(&ws, &xdg)
        .args(["bookmark", "create", "cc-main", "-r", "@"])
        .assert()
        .success();
    let base_commit_hex = String::from_utf8(
        jj_cmd(&ws, &xdg)
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

    // Snapshot the pre-import op head (`op0`) and view (`view0`).
    let server_url = server.url().to_string();
    let (op0, view0) = cc_lib::util::run_on_tokio(async move {
        let mut op_client = cc_lib::util::connect_op_store_client(server_url).await?;
        let heads = op_client
            .get_op_heads(cc_common::op_store::GetOpHeadsRequest {
                repo_id: "repo-race".to_string(),
            })
            .await?
            .into_inner()
            .op_head_ids;
        assert_eq!(heads.len(), 1);
        let op0 = heads[0].clone();
        let op = op_client
            .read_operation(ReadOperationRequest {
                repo_id: "repo-race".to_string(),
                operation_id: op0.clone(),
            })
            .await?
            .into_inner()
            .operation
            .unwrap();
        let view0 = op_client
            .read_view(ReadViewRequest {
                repo_id: "repo-race".to_string(),
                view_id: op.view_id,
            })
            .await?
            .into_inner()
            .view
            .unwrap();
        Ok((op0, view0))
    })
    .await
    .unwrap();

    // Import a Git repo on top of `op0`.
    let src_git = dir.path().join("src_race");
    fs::create_dir_all(&src_git).unwrap();
    run_git(&src_git, &["init", "-b", "git-main"]);
    fs::write(src_git.join("git.txt"), "from git\n").unwrap();
    run_git(&src_git, &["add", "."]);
    run_git(&src_git, &["commit", "-m", "git commit"]);
    let git_commit_hex = run_git(&src_git, &["rev-parse", "git-main"]);

    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "import-git",
            "--server",
            server.url(),
            "--repo-id",
            "repo-race",
            "--git-dir",
            src_git.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Publish a concurrent workspace operation parented at `op0` (racing with `import-git`) and
    // verify `ReconcileOpHeads` merges both branches automatically.
    let server_url = server.url().to_string();
    let base_commit_bytes = hex::decode(&base_commit_hex).unwrap();
    let (reconciled_resp, ws_commit_hex) = cc_lib::util::run_on_tokio(async move {
        let mut backend_client = cc_lib::util::connect_backend_client(server_url.clone()).await?;
        let mut op_client = cc_lib::util::connect_op_store_client(server_url).await?;

        let after_import_heads = op_client
            .get_op_heads(cc_common::op_store::GetOpHeadsRequest {
                repo_id: "repo-race".to_string(),
            })
            .await?
            .into_inner()
            .op_head_ids;
        assert_eq!(after_import_heads.len(), 1);
        let import_op = op_client
            .read_operation(ReadOperationRequest {
                repo_id: "repo-race".to_string(),
                operation_id: after_import_heads[0].clone(),
            })
            .await?
            .into_inner()
            .operation
            .unwrap();
        assert!(
            import_op.commit_predecessors_set,
            "import-git operation must set commit_predecessors_set = true"
        );

        let ws_commit_id = backend_client
            .write_commit(cc_common::backend::WriteCommitRequest {
                project_id: "repo-race".to_string(),
                commit: Some(cc_common::backend::Commit {
                    commit_id: Vec::new(),
                    change_id: vec![0x42; cc_common::CHANGE_ID_LENGTH],
                    parent_commit_ids: vec![base_commit_bytes.clone()],
                    root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
                    description: "concurrent workspace commit\n".to_string(),
                    author: Some(cc_common::backend::Signature {
                        name: "Cloud Developer".to_string(),
                        email: "dev@example.com".to_string(),
                        timestamp: Some(cc_common::backend::Timestamp {
                            millis_since_epoch: 1_730_000_200_000,
                            tz_offset: 0,
                        }),
                    }),
                    committer: Some(cc_common::backend::Signature {
                        name: "Cloud Developer".to_string(),
                        email: "dev@example.com".to_string(),
                        timestamp: Some(cc_common::backend::Timestamp {
                            millis_since_epoch: 1_730_000_200_000,
                            tz_offset: 0,
                        }),
                    }),
                    predecessors: Vec::new(),
                    conflict_labels: Vec::new(),
                    secure_sig: None,
                    raw_git_commit: Vec::new(),
                    external_ids: Default::default(),
                }),
            })
            .await?
            .into_inner()
            .commit_id;

        let mut view_ws = view0;
        view_ws.head_ids.retain(|h| h != &base_commit_bytes);
        view_ws.head_ids.push(ws_commit_id.clone());
        view_ws.local_bookmarks.insert(
            "ws-branch".to_string(),
            cc_common::op_store::RefTarget {
                removes: Vec::new(),
                adds: vec![cc_common::op_store::RefTargetTerm {
                    commit_id: ws_commit_id.clone(),
                }],
            },
        );
        view_ws
            .wc_commit_ids
            .insert("other-ws".to_string(), ws_commit_id.clone());

        let pub_resp = op_client
            .publish_operation(cc_common::op_store::PublishOperationRequest {
                repo_id: "repo-race".to_string(),
                view: Some(view_ws),
                operation: Some(cc_common::op_store::Operation {
                    view_id: Vec::new(),
                    parents: vec![op0.clone()],
                    metadata: Some(cc_common::op_store::OperationMetadata {
                        description: "concurrent workspace op".to_string(),
                        workspace_name: Some("other-ws".to_string()),
                        ..Default::default()
                    }),
                    commit_predecessors: vec![cc_common::op_store::CommitPredecessors {
                        commit_id: ws_commit_id.clone(),
                        predecessor_ids: Vec::new(),
                    }],
                    commit_predecessors_set: true,
                }),
                old_op_head_ids: vec![op0],
            })
            .await?
            .into_inner();
        assert_eq!(
            pub_resp.current_op_head_ids.len(),
            2,
            "expected two divergent op heads before reconciliation"
        );

        let reconciled = op_client
            .reconcile_op_heads(ReconcileOpHeadsRequest {
                repo_id: "repo-race".to_string(),
            })
            .await?
            .into_inner();
        Ok((reconciled, hex::encode(&ws_commit_id)))
    })
    .await
    .unwrap();

    assert!(
        !reconciled_resp.client_resolution_required && !reconciled_resp.op_head.is_empty(),
        "ReconcileOpHeads must reconcile concurrent import-git + workspace op automatically: {reconciled_resp:?}"
    );

    let merged_view = read_head_view(server.url(), "repo-race").await;
    let heads: HashSet<String> = merged_view.head_ids.iter().map(hex::encode).collect();
    assert_eq!(
        heads,
        HashSet::from([git_commit_hex.clone(), ws_commit_hex.clone()])
    );
    assert_eq!(
        hex::encode(&merged_view.local_bookmarks["git-main"].adds[0].commit_id),
        git_commit_hex
    );
    assert_eq!(
        hex::encode(&merged_view.local_bookmarks["ws-branch"].adds[0].commit_id),
        ws_commit_hex
    );
    assert_eq!(
        hex::encode(&merged_view.local_bookmarks["cc-main"].adds[0].commit_id),
        base_commit_hex
    );

    jj_cmd(&ws, &xdg).args(["log"]).assert().success();
}

#[tokio::test]
async fn test_import_git_rejects_non_utf8_tree_filename_before_uploads() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_server().await;

    let ws = dir.path().join("ws_utf8");
    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--repo-id",
            "repo-non-utf8",
            "--create",
            ws.to_str().unwrap(),
        ])
        .assert()
        .success();

    let src_git = dir.path().join("src_non_utf8");
    fs::create_dir_all(&src_git).unwrap();
    run_git(&src_git, &["init", "-b", "main"]);

    let blob_path = src_git.join("payload.bin");
    fs::write(&blob_path, b"blob behind non-utf8 filename\n").unwrap();
    let blob_hex = run_git(&src_git, &["hash-object", "-w", "payload.bin"]);
    let blob_bytes = hex::decode(&blob_hex).unwrap();

    // Construct a raw Git tree containing a non-UTF-8 entry name `bad\xffname.txt`.
    let mut raw_tree = Vec::new();
    raw_tree.extend_from_slice(b"100644 bad\xffname.txt\0");
    raw_tree.extend_from_slice(&blob_bytes);

    let mut tree_cmd = StdCommand::new("git")
        .current_dir(&src_git)
        .args(["hash-object", "-t", "tree", "-w", "--stdin"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("git hash-object should spawn");
    {
        use std::io::Write as _;
        tree_cmd
            .stdin
            .as_mut()
            .unwrap()
            .write_all(&raw_tree)
            .unwrap();
    }
    let tree_out = tree_cmd.wait_with_output().unwrap();
    assert!(tree_out.status.success());
    let tree_hex = String::from_utf8(tree_out.stdout)
        .unwrap()
        .trim()
        .to_string();

    let commit_hex = run_git(&src_git, &["commit-tree", &tree_hex, "-m", "non-utf8 tree"]);
    run_git(&src_git, &["update-ref", "refs/heads/main", &commit_hex]);

    let stderr = String::from_utf8(
        jj_cmd(dir.path(), &xdg)
            .args([
                "cc",
                "import-git",
                "--server",
                server.url(),
                "--repo-id",
                "repo-non-utf8",
                "--git-dir",
                src_git.to_str().unwrap(),
            ])
            .assert()
            .failure()
            .get_output()
            .stderr
            .clone(),
    )
    .unwrap();
    assert!(
        stderr.contains(&tree_hex),
        "error should name the offending tree OID {tree_hex}, got: {stderr}"
    );
    assert!(
        stderr.contains("Non-UTF-8") && stderr.contains("bad"),
        "error should name the non-UTF-8 filename, got: {stderr}"
    );

    // Validation must run before issuing any upload RPCs, so the blob must not exist on the server.
    let server_url = server.url().to_string();
    let blob_status = cc_lib::util::run_on_tokio(async move {
        let mut client = cc_lib::util::connect_backend_client(server_url).await?;
        let res = client
            .read_file(cc_common::backend::ReadFileRequest {
                project_id: "repo-non-utf8".to_string(),
                file_id: blob_bytes,
                offset: 0,
                max_bytes: 0,
            })
            .await;
        Ok(res.err().map(|s| s.code()))
    })
    .await
    .unwrap();
    assert_eq!(
        blob_status,
        Some(tonic::Code::NotFound),
        "blob must not be uploaded when tree contains a non-UTF-8 filename"
    );
}

#[tokio::test]
async fn test_cli_rejects_blank_ids_and_export_verifies_project_binding() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_server().await;

    let src_git = dir.path().join("src_proj");
    fs::create_dir_all(&src_git).unwrap();
    run_git(&src_git, &["init", "-b", "main"]);
    fs::write(src_git.join("a.txt"), "hello\n").unwrap();
    run_git(&src_git, &["add", "."]);
    run_git(&src_git, &["commit", "-m", "initial"]);

    // Blank `--repo-id` and `--project-id` are rejected on both `import-git` and `export-git`.
    for (flag, val) in [
        ("--repo-id", ""),
        ("--repo-id", "   "),
        ("--project-id", ""),
        ("--project-id", "\t "),
    ] {
        let import_stderr = String::from_utf8(
            jj_cmd(dir.path(), &xdg)
                .args([
                    "cc",
                    "import-git",
                    "--server",
                    server.url(),
                    flag,
                    val,
                    "--create",
                    "--git-dir",
                    src_git.to_str().unwrap(),
                ])
                .assert()
                .failure()
                .get_output()
                .stderr
                .clone(),
        )
        .unwrap();
        assert!(
            import_stderr.contains("must not be empty or whitespace"),
            "import-git {flag}={val:?} stderr: {import_stderr}"
        );

        let unused_git = dir.path().join("unused.git");
        let mut export_args = vec![
            "cc",
            "export-git",
            "--server",
            server.url(),
            "--bare",
            "--git-dir",
            unused_git.to_str().unwrap(),
            flag,
            val,
        ];
        if flag != "--repo-id" {
            export_args.extend(["--repo-id", "repo-proj-a"]);
        }
        let export_stderr = String::from_utf8(
            jj_cmd(dir.path(), &xdg)
                .args(&export_args)
                .assert()
                .failure()
                .get_output()
                .stderr
                .clone(),
        )
        .unwrap();
        assert!(
            export_stderr.contains("must not be empty or whitespace"),
            "export-git {flag}={val:?} stderr: {export_stderr}"
        );
    }

    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "import-git",
            "--server",
            server.url(),
            "--project-id",
            "proj-owner",
            "--repo-id",
            "repo-proj-a",
            "--create",
            "--git-dir",
            src_git.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Exporting `repo-proj-a` with a mismatched `--project-id proj-wrong` must fail closed
    // before creating the target Git directory.
    let bad_dst = dir.path().join("bad_proj_export.git");
    let mismatch_stderr = String::from_utf8(
        jj_cmd(dir.path(), &xdg)
            .args([
                "cc",
                "export-git",
                "--server",
                server.url(),
                "--repo-id",
                "repo-proj-a",
                "--project-id",
                "proj-wrong",
                "--bare",
                "--git-dir",
                bad_dst.to_str().unwrap(),
            ])
            .assert()
            .failure()
            .get_output()
            .stderr
            .clone(),
    )
    .unwrap();
    assert!(
        mismatch_stderr.contains("proj-owner") || mismatch_stderr.contains("already bound"),
        "expected project mismatch error, got: {mismatch_stderr}"
    );
    assert!(
        !bad_dst.exists(),
        "failed export with wrong --project-id must not initialize destination git dir"
    );

    // Matching `--project-id proj-owner` succeeds.
    let good_dst = dir.path().join("good_proj_export.git");
    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "export-git",
            "--server",
            server.url(),
            "--repo-id",
            "repo-proj-a",
            "--project-id",
            "proj-owner",
            "--bare",
            "--git-dir",
            good_dst.to_str().unwrap(),
        ])
        .assert()
        .success();
    run_git_bare(&good_dst, &["fsck", "--full"]);
}

#[tokio::test]
async fn test_export_git_non_bare_checks_out_worktree_and_refuses_checked_out_branch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_server().await;

    let ws = dir.path().join("ws_nonbare");
    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--repo-id",
            "repo-nonbare",
            "--create",
            ws.to_str().unwrap(),
        ])
        .assert()
        .success();
    fs::create_dir_all(ws.join("src")).unwrap();
    fs::write(ws.join("src/hello.txt"), "hello v1\n").unwrap();
    fs::write(ws.join("README.md"), "# Exported\n").unwrap();
    jj_cmd(&ws, &xdg)
        .args(["describe", "-m", "v1"])
        .assert()
        .success();
    jj_cmd(&ws, &xdg)
        .args(["bookmark", "create", "main", "-r", "@"])
        .assert()
        .success();
    let c1 = String::from_utf8(
        jj_cmd(&ws, &xdg)
            .args(["log", "--no-graph", "-r", "main", "-T", "commit_id"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap()
    .trim()
    .to_string();

    // Default non-bare `jj cc export-git` materializes the checked-out worktree and clean index.
    let dst_nonbare = dir.path().join("exported_nonbare");
    jj_cmd(&ws, &xdg)
        .args([
            "cc",
            "export-git",
            "--git-dir",
            dst_nonbare.to_str().unwrap(),
        ])
        .assert()
        .success();

    run_git(&dst_nonbare, &["fsck", "--full"]);
    assert_eq!(
        run_git(&dst_nonbare, &["symbolic-ref", "HEAD"]),
        "refs/heads/main"
    );
    assert_eq!(run_git(&dst_nonbare, &["rev-parse", "main"]), c1);
    assert_eq!(
        run_git(&dst_nonbare, &["status", "--porcelain"]),
        "",
        "newly exported non-bare repository must have a clean git status"
    );
    assert_eq!(
        fs::read_to_string(dst_nonbare.join("src/hello.txt")).unwrap(),
        "hello v1\n"
    );
    assert_eq!(
        fs::read_to_string(dst_nonbare.join("README.md")).unwrap(),
        "# Exported\n"
    );

    // Advance `main` in Commit Cloud and try to re-export into the existing non-bare repo while
    // `main` is checked out: must fail without modifying `main` or desynchronizing the worktree.
    jj_cmd(&ws, &xdg)
        .args(["new", "-m", "v2"])
        .assert()
        .success();
    fs::write(ws.join("src/hello.txt"), "hello v2\n").unwrap();
    jj_cmd(&ws, &xdg)
        .args(["bookmark", "set", "main", "-r", "@"])
        .assert()
        .success();
    let c2 = String::from_utf8(
        jj_cmd(&ws, &xdg)
            .args(["log", "--no-graph", "-r", "main", "-T", "commit_id"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap()
    .trim()
    .to_string();

    let refuse_stderr = String::from_utf8(
        jj_cmd(&ws, &xdg)
            .args([
                "cc",
                "export-git",
                "--git-dir",
                dst_nonbare.to_str().unwrap(),
            ])
            .assert()
            .failure()
            .get_output()
            .stderr
            .clone(),
    )
    .unwrap();
    assert!(
        refuse_stderr.contains("checked-out branch \"main\"") && refuse_stderr.contains("--bare"),
        "expected checked-out branch refusal with remediation hint, got: {refuse_stderr}"
    );
    assert_eq!(run_git(&dst_nonbare, &["rev-parse", "main"]), c1);
    assert_eq!(run_git(&dst_nonbare, &["status", "--porcelain"]), "");
    assert_eq!(
        fs::read_to_string(dst_nonbare.join("src/hello.txt")).unwrap(),
        "hello v1\n"
    );

    // After detaching HEAD, re-exporting `main` succeeds while leaving HEAD and worktree intact.
    run_git(&dst_nonbare, &["checkout", "--detach", "HEAD"]);
    jj_cmd(&ws, &xdg)
        .args([
            "cc",
            "export-git",
            "--git-dir",
            dst_nonbare.to_str().unwrap(),
        ])
        .assert()
        .success();
    assert_eq!(run_git(&dst_nonbare, &["rev-parse", "main"]), c2);
    assert_eq!(run_git(&dst_nonbare, &["rev-parse", "HEAD"]), c1);
    assert_eq!(run_git(&dst_nonbare, &["status", "--porcelain"]), "");
}

#[tokio::test]
async fn test_import_git_rejects_commit_with_missing_timestamp() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_server().await;

    let ws = dir.path().join("ws_bad_ts");
    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--repo-id",
            "repo-bad-ts",
            "--create",
            ws.to_str().unwrap(),
        ])
        .assert()
        .success();

    let src_git = dir.path().join("src_bad_ts");
    fs::create_dir_all(&src_git).unwrap();
    run_git(&src_git, &["init", "-b", "main"]);

    let tree_out = StdCommand::new("git")
        .args(["hash-object", "-t", "tree", "-w", "--stdin"])
        .stdin(std::process::Stdio::null())
        .current_dir(&src_git)
        .output()
        .unwrap();
    assert!(tree_out.status.success());
    let tree_id = String::from_utf8(tree_out.stdout)
        .unwrap()
        .trim()
        .to_string();

    // Signatures without a timestamp are malformed; the importer must refuse them rather than
    // invent a date.
    let raw_commit = format!(
        "tree {tree_id}\nauthor A U Thor <a@example.com>\ncommitter A U Thor <a@example.com>\n\nno timestamp\n"
    );
    let mut hash_cmd = StdCommand::new("git")
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
        .current_dir(&src_git)
        .spawn()
        .unwrap();
    {
        use std::io::Write as _;
        hash_cmd
            .stdin
            .as_mut()
            .unwrap()
            .write_all(raw_commit.as_bytes())
            .unwrap();
    }
    let commit_out = hash_cmd.wait_with_output().unwrap();
    assert!(commit_out.status.success());
    let commit_id = String::from_utf8(commit_out.stdout)
        .unwrap()
        .trim()
        .to_string();
    run_git(&src_git, &["update-ref", "refs/heads/main", &commit_id]);

    let stderr = String::from_utf8(
        jj_cmd(dir.path(), &xdg)
            .args([
                "cc",
                "import-git",
                "--git-dir",
                src_git.to_str().unwrap(),
                "--repo-id",
                "repo-bad-ts",
                "--server",
                server.url(),
            ])
            .assert()
            .failure()
            .get_output()
            .stderr
            .clone(),
    )
    .unwrap();
    assert!(
        stderr.contains(&commit_id),
        "error should name the offending commit {commit_id}:\n{stderr}"
    );
    assert!(
        stderr.to_ascii_lowercase().contains("invalid git author"),
        "error should explain the malformed signature:\n{stderr}"
    );
}

#[tokio::test]
async fn test_roundtrip_gpg_and_ssh_signed_commits_preserve_signature_and_sha1() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_server().await;

    let src_git = dir.path().join("src_signed");
    fs::create_dir_all(&src_git).unwrap();
    run_git(&src_git, &["init", "-b", "main"]);
    fs::write(src_git.join("signed.txt"), "verified content\n").unwrap();
    run_git(&src_git, &["add", "."]);
    let tree_hex = run_git(&src_git, &["write-tree"]);

    let write_raw_commit = |raw: &str| -> String {
        let mut child = StdCommand::new("git")
            .current_dir(&src_git)
            .args(["hash-object", "-t", "commit", "-w", "--stdin"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        {
            use std::io::Write as _;
            child
                .stdin
                .as_mut()
                .unwrap()
                .write_all(raw.as_bytes())
                .unwrap();
        }
        let out = child.wait_with_output().unwrap();
        assert!(out.status.success());
        String::from_utf8(out.stdout).unwrap().trim().to_string()
    };

    let pgp_sig_body = "-----BEGIN PGP SIGNATURE-----\n\
                        iQIzBAABCAAdFiEEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n\
                        =AAAA\n\
                        -----END PGP SIGNATURE-----\n";
    let pgp_unsigned = format!(
        "tree {tree_hex}\n\
         author PGP Author <pgp@example.com> 1730000000 +0000\n\
         committer PGP Committer <pgp@example.com> 1730000000 +0000\n\
         \n\
         gpg signed commit\n"
    );
    let pgp_raw = format!(
        "tree {tree_hex}\n\
         author PGP Author <pgp@example.com> 1730000000 +0000\n\
         committer PGP Committer <pgp@example.com> 1730000000 +0000\n\
         gpgsig -----BEGIN PGP SIGNATURE-----\n \
         iQIzBAABCAAdFiEEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n \
         =AAAA\n \
         -----END PGP SIGNATURE-----\n\
         \n\
         gpg signed commit\n"
    );
    let pgp_commit = write_raw_commit(&pgp_raw);

    let ssh_sig_body = "-----BEGIN SSH SIGNATURE-----\n\
                        U1NIU0lHAAAAAQAAADMAAAALc3NoLWVkMjU1MTkAAAAgAAAA\n\
                        -----END SSH SIGNATURE-----\n";
    let ssh_unsigned = format!(
        "tree {tree_hex}\n\
         parent {pgp_commit}\n\
         author SSH Author <ssh@example.com> 1730000100 +0000\n\
         committer SSH Committer <ssh@example.com> 1730000100 +0000\n\
         \n\
         ssh signed commit\n"
    );
    let ssh_raw = format!(
        "tree {tree_hex}\n\
         parent {pgp_commit}\n\
         author SSH Author <ssh@example.com> 1730000100 +0000\n\
         committer SSH Committer <ssh@example.com> 1730000100 +0000\n\
         gpgsig -----BEGIN SSH SIGNATURE-----\n \
         U1NIU0lHAAAAAQAAADMAAAALc3NoLWVkMjU1MTkAAAAgAAAA\n \
         -----END SSH SIGNATURE-----\n\
         \n\
         ssh signed commit\n"
    );
    let ssh_commit = write_raw_commit(&ssh_raw);
    run_git(&src_git, &["update-ref", "refs/heads/main", &ssh_commit]);

    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "import-git",
            "--server",
            server.url(),
            "--repo-id",
            "repo-signed",
            "--create",
            "--git-dir",
            src_git.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Server stores both signatures and derives exact unsigned signed payloads.
    let server_url = server.url().to_string();
    let pgp_bytes = hex::decode(&pgp_commit).unwrap();
    let ssh_bytes = hex::decode(&ssh_commit).unwrap();
    let (stored_pgp, stored_ssh) = cc_lib::util::run_on_tokio(async move {
        let mut client = cc_lib::util::connect_backend_client(server_url).await?;
        let pgp = client
            .read_commit(cc_common::backend::ReadCommitRequest {
                project_id: "repo-signed".to_string(),
                commit_id: pgp_bytes,
            })
            .await?
            .into_inner()
            .commit
            .unwrap();
        let ssh = client
            .read_commit(cc_common::backend::ReadCommitRequest {
                project_id: "repo-signed".to_string(),
                commit_id: ssh_bytes,
            })
            .await?
            .into_inner()
            .commit
            .unwrap();
        Ok((pgp, ssh))
    })
    .await
    .unwrap();

    let pgp_secure_sig = stored_pgp.secure_sig.expect("pgp secure_sig stored");
    assert_eq!(pgp_secure_sig.sig, pgp_sig_body.as_bytes());
    assert_eq!(pgp_secure_sig.data, pgp_unsigned.as_bytes());

    let ssh_secure_sig = stored_ssh.secure_sig.expect("ssh secure_sig stored");
    assert_eq!(ssh_secure_sig.sig, ssh_sig_body.as_bytes());
    assert_eq!(ssh_secure_sig.data, ssh_unsigned.as_bytes());

    // Export back to bare Git and verify exact SHA-1s and bit-for-bit `gpgsig` headers.
    let dst_git = dir.path().join("exported_signed.git");
    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "export-git",
            "--server",
            server.url(),
            "--repo-id",
            "repo-signed",
            "--bare",
            "--git-dir",
            dst_git.to_str().unwrap(),
        ])
        .assert()
        .success();

    run_git_bare(&dst_git, &["fsck", "--full"]);
    assert_eq!(run_git_bare(&dst_git, &["rev-parse", "main"]), ssh_commit);
    assert_eq!(run_git_bare(&dst_git, &["rev-parse", "main~1"]), pgp_commit);
    assert_eq!(
        run_git_bare(&dst_git, &["cat-file", "commit", "main"]),
        ssh_raw.trim_end()
    );
    assert_eq!(
        run_git_bare(&dst_git, &["cat-file", "commit", "main~1"]),
        pgp_raw.trim_end()
    );
}

#[tokio::test]
async fn test_roundtrip_natively_authored_conflicted_commit_cloud_commit() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_server().await;

    // Author a 3-way conflicted merge commit natively in Commit Cloud (empty `raw_git_commit`).
    let ws = dir.path().join("ws_native_conflict");
    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--repo-id",
            "repo-native-conflict",
            "--create",
            ws.to_str().unwrap(),
        ])
        .assert()
        .success();

    fs::write(ws.join("conflict.txt"), "base line\n").unwrap();
    jj_cmd(&ws, &xdg)
        .args(["describe", "-m", "base"])
        .assert()
        .success();
    let base_id = String::from_utf8(
        jj_cmd(&ws, &xdg)
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

    jj_cmd(&ws, &xdg)
        .args(["new", &base_id, "-m", "left side"])
        .assert()
        .success();
    fs::write(ws.join("conflict.txt"), "left line\n").unwrap();
    let left_id = String::from_utf8(
        jj_cmd(&ws, &xdg)
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

    jj_cmd(&ws, &xdg)
        .args(["new", &base_id, "-m", "right side"])
        .assert()
        .success();
    fs::write(ws.join("conflict.txt"), "right line\n").unwrap();
    let right_id = String::from_utf8(
        jj_cmd(&ws, &xdg)
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

    jj_cmd(&ws, &xdg)
        .args(["new", &left_id, &right_id, "-m", "native cloud conflict"])
        .assert()
        .success();
    jj_cmd(&ws, &xdg)
        .args(["bookmark", "create", "native-conflict", "-r", "@"])
        .assert()
        .success();
    let native_conflict_id = String::from_utf8(
        jj_cmd(&ws, &xdg)
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

    let server_url = server.url().to_string();
    let conflict_bytes = hex::decode(&native_conflict_id).unwrap();
    let stored = cc_lib::util::run_on_tokio(async move {
        let mut client = cc_lib::util::connect_backend_client(server_url).await?;
        let resp = client
            .read_commit(cc_common::backend::ReadCommitRequest {
                project_id: "repo-native-conflict".to_string(),
                commit_id: conflict_bytes,
            })
            .await?
            .into_inner();
        Ok(resp.commit.unwrap())
    })
    .await
    .unwrap();
    assert!(
        stored.raw_git_commit.is_empty(),
        "natively authored Commit Cloud commit should not have raw_git_commit"
    );
    assert_eq!(stored.root_tree_id.len(), 3);
    assert_eq!(stored.conflict_labels.len(), 3);
    assert!(
        stored.conflict_labels.iter().any(|l| !l.is_empty()),
        "expected non-empty conflict_labels on native merge conflict, got {:?}",
        stored.conflict_labels
    );

    // Export to bare Git and verify `git fsck --full`, exact commit SHA-1, `jj:trees`, and
    // `jj:conflict-labels`.
    let dst_git = dir.path().join("exported_native_conflict.git");
    jj_cmd(&ws, &xdg)
        .args([
            "cc",
            "export-git",
            "--bare",
            "--git-dir",
            dst_git.to_str().unwrap(),
        ])
        .assert()
        .success();

    run_git_bare(&dst_git, &["fsck", "--full"]);
    assert_eq!(
        run_git_bare(&dst_git, &["rev-parse", "native-conflict"]),
        native_conflict_id
    );
    let exported_raw = run_git_bare(&dst_git, &["cat-file", "commit", "native-conflict"]);
    let expected_jj_trees = stored
        .root_tree_id
        .iter()
        .map(hex::encode)
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        exported_raw.contains(&format!("jj:trees {expected_jj_trees}")),
        "expected jj:trees header in exported commit:\n{exported_raw}"
    );
    assert!(
        exported_raw.contains("jj:conflict-labels "),
        "expected jj:conflict-labels header in exported commit:\n{exported_raw}"
    );

    // Round-trip back into a second Commit Cloud repository and verify exact commit ID,
    // 3-term `root_tree_id`, `conflict_labels`, and materialized conflict markers on clone.
    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "import-git",
            "--server",
            server.url(),
            "--repo-id",
            "repo-native-conflict-rt",
            "--create",
            "--git-dir",
            dst_git.to_str().unwrap(),
        ])
        .assert()
        .success();

    let server_url = server.url().to_string();
    let conflict_bytes = hex::decode(&native_conflict_id).unwrap();
    let reimported = cc_lib::util::run_on_tokio(async move {
        let mut client = cc_lib::util::connect_backend_client(server_url).await?;
        let resp = client
            .read_commit(cc_common::backend::ReadCommitRequest {
                project_id: "repo-native-conflict-rt".to_string(),
                commit_id: conflict_bytes,
            })
            .await?
            .into_inner();
        Ok(resp.commit.unwrap())
    })
    .await
    .unwrap();
    assert_eq!(reimported.commit_id, stored.commit_id);
    assert_eq!(reimported.root_tree_id, stored.root_tree_id);
    assert_eq!(reimported.conflict_labels, stored.conflict_labels);

    let ws_rt = dir.path().join("ws_native_conflict_rt");
    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "clone",
            "--server",
            server.url(),
            "-r",
            "native-conflict",
            "repo-native-conflict-rt",
            ws_rt.to_str().unwrap(),
        ])
        .assert()
        .success();
    let is_conflict = String::from_utf8(
        jj_cmd(&ws_rt, &xdg)
            .args([
                "log",
                "--no-graph",
                "-r",
                "native-conflict",
                "-T",
                "conflict",
            ])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap()
    .trim()
    .to_string();
    assert_eq!(is_conflict, "true");
    let conflict_contents = fs::read_to_string(ws_rt.join("conflict.txt")).unwrap();
    assert!(
        conflict_contents.contains("left line")
            && conflict_contents.contains("base line")
            && conflict_contents.contains("right line"),
        "expected materialized conflict markers in cloned workspace, got:\n{conflict_contents}"
    );
}

#[tokio::test]
async fn test_export_git_non_bare_refuses_existing_non_empty_non_git_dir() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_server().await;

    let ws = dir.path().join("ws_nonempty_guard");
    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--repo-id",
            "repo-nonempty-guard",
            "--create",
            ws.to_str().unwrap(),
        ])
        .assert()
        .success();
    fs::write(ws.join("important.txt"), "from cloud\n").unwrap();
    jj_cmd(&ws, &xdg)
        .args(["describe", "-m", "cloud commit"])
        .assert()
        .success();
    jj_cmd(&ws, &xdg)
        .args(["bookmark", "create", "main", "-r", "@"])
        .assert()
        .success();

    // Existing non-empty non-Git directory must fail closed without creating `.git` or
    // clobbering existing files.
    let existing_dir = dir.path().join("user_files");
    fs::create_dir_all(&existing_dir).unwrap();
    fs::write(existing_dir.join("important.txt"), "precious user data\n").unwrap();
    fs::write(existing_dir.join("keep.txt"), "keep me\n").unwrap();

    let stderr = String::from_utf8(
        jj_cmd(&ws, &xdg)
            .args([
                "cc",
                "export-git",
                "--git-dir",
                existing_dir.to_str().unwrap(),
            ])
            .assert()
            .failure()
            .get_output()
            .stderr
            .clone(),
    )
    .unwrap();
    assert!(
        stderr.contains("not an empty directory"),
        "expected non-empty directory error, got: {stderr}"
    );
    assert!(
        !existing_dir.join(".git").exists(),
        "failed export must not create .git inside existing non-empty directory"
    );
    assert_eq!(
        fs::read_to_string(existing_dir.join("important.txt")).unwrap(),
        "precious user data\n"
    );
    assert_eq!(
        fs::read_to_string(existing_dir.join("keep.txt")).unwrap(),
        "keep me\n"
    );

    // Existing regular file at destination must also fail closed without modifying it.
    let existing_file = dir.path().join("existing_file");
    fs::write(&existing_file, "file contents\n").unwrap();
    jj_cmd(&ws, &xdg)
        .args([
            "cc",
            "export-git",
            "--git-dir",
            existing_file.to_str().unwrap(),
        ])
        .assert()
        .failure();
    assert_eq!(
        fs::read_to_string(&existing_file).unwrap(),
        "file contents\n"
    );

    // Existing empty directory is allowed and checked out.
    let empty_dir = dir.path().join("empty_dst");
    fs::create_dir_all(&empty_dir).unwrap();
    jj_cmd(&ws, &xdg)
        .args(["cc", "export-git", "--git-dir", empty_dir.to_str().unwrap()])
        .assert()
        .success();
    assert_eq!(
        fs::read_to_string(empty_dir.join("important.txt")).unwrap(),
        "from cloud\n"
    );
    assert_eq!(run_git(&empty_dir, &["status", "--porcelain"]), "");
}

#[tokio::test]
async fn test_export_git_non_bare_scrubs_ambient_git_env_vars() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_server().await;

    let ws = dir.path().join("ws_env_scrub");
    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--repo-id",
            "repo-env-scrub",
            "--create",
            ws.to_str().unwrap(),
        ])
        .assert()
        .success();
    fs::write(ws.join("exported.txt"), "exported payload\n").unwrap();
    jj_cmd(&ws, &xdg)
        .args(["describe", "-m", "cloud commit for env test"])
        .assert()
        .success();
    jj_cmd(&ws, &xdg)
        .args(["bookmark", "create", "main", "-r", "@"])
        .assert()
        .success();
    let expected_commit = String::from_utf8(
        jj_cmd(&ws, &xdg)
            .args(["log", "--no-graph", "-r", "main", "-T", "commit_id"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap()
    .trim()
    .to_string();

    // Dirty sibling Git repository with unstaged, staged, and untracked edits.
    let sibling = dir.path().join("dirty_sibling");
    fs::create_dir_all(&sibling).unwrap();
    run_git(&sibling, &["init", "-b", "sibling-main"]);
    fs::write(sibling.join("sibling.txt"), "committed sibling\n").unwrap();
    run_git(&sibling, &["add", "."]);
    run_git(&sibling, &["commit", "-m", "sibling initial"]);
    let sibling_head_before = run_git(&sibling, &["rev-parse", "HEAD"]);

    fs::write(sibling.join("sibling.txt"), "dirty unstaged edit\n").unwrap();
    fs::write(sibling.join("staged.txt"), "staged sibling edit\n").unwrap();
    run_git(&sibling, &["add", "staged.txt"]);
    fs::write(sibling.join("untracked.txt"), "untracked sibling note\n").unwrap();
    let sibling_status_before = run_git(&sibling, &["status", "--porcelain"]);

    // Run non-bare `jj cc export-git` with ambient GIT_* env vars pointing at the dirty sibling
    // plus an ambient `GIT_NAMESPACE` and `GIT_CONFIG_COUNT` override.
    let dst = dir.path().join("clean_export_dst");
    jj_cmd(&ws, &xdg)
        .env("GIT_DIR", sibling.join(".git"))
        .env("GIT_WORK_TREE", &sibling)
        .env("GIT_INDEX_FILE", sibling.join(".git/index"))
        .env("GIT_NAMESPACE", "ambient-ns")
        .env("GIT_CONFIG_COUNT", "1")
        .env("GIT_CONFIG_KEY_0", "init.defaultBranch")
        .env("GIT_CONFIG_VALUE_0", "poisoned")
        .args(["cc", "export-git", "--git-dir", dst.to_str().unwrap()])
        .assert()
        .success();

    // The target destination must be checked out at the exported commit with refs written
    // directly under `refs/heads/` (never redirected into `refs/namespaces/ambient-ns/`).
    run_git(&dst, &["fsck", "--full"]);
    assert_eq!(run_git(&dst, &["rev-parse", "main"]), expected_commit);
    assert_eq!(run_git(&dst, &["symbolic-ref", "HEAD"]), "refs/heads/main");
    assert_eq!(
        run_git(&dst, &["for-each-ref", "--format=%(refname)"]),
        "refs/heads/main"
    );
    assert!(
        !dst.join(".git/refs/namespaces").exists(),
        "ambient GIT_NAMESPACE must not redirect exported refs"
    );
    assert_eq!(run_git(&dst, &["status", "--porcelain"]), "");
    assert_eq!(
        fs::read_to_string(dst.join("exported.txt")).unwrap(),
        "exported payload\n"
    );

    // Re-exporting into an existing bare repo must also ignore ambient GIT_NAMESPACE and
    // GIT_WORK_TREE / GIT_DIR when opening the target repository.
    let existing_bare = dir.path().join("existing_bare.git");
    fs::create_dir_all(&existing_bare).unwrap();
    run_git_bare(
        &existing_bare,
        &[
            "-c",
            "init.defaultRefFormat=files",
            "init",
            "--bare",
            "-b",
            "main",
        ],
    );
    jj_cmd(&ws, &xdg)
        .env("GIT_DIR", sibling.join(".git"))
        .env("GIT_WORK_TREE", &sibling)
        .env("GIT_INDEX_FILE", sibling.join(".git/index"))
        .env("GIT_NAMESPACE", "ambient-ns")
        .args([
            "cc",
            "export-git",
            "--bare",
            "--git-dir",
            existing_bare.to_str().unwrap(),
        ])
        .assert()
        .success();
    run_git_bare(&existing_bare, &["fsck", "--full"]);
    assert_eq!(
        run_git_bare(&existing_bare, &["rev-parse", "main"]),
        expected_commit
    );
    assert_eq!(
        run_git_bare(&existing_bare, &["for-each-ref", "--format=%(refname)"]),
        "refs/heads/main"
    );
    assert!(
        !existing_bare.join("refs/namespaces").exists(),
        "ambient GIT_NAMESPACE must not redirect refs in existing target repos"
    );

    // The dirty sibling repo must remain untouched.
    assert_eq!(
        run_git(&sibling, &["rev-parse", "HEAD"]),
        sibling_head_before
    );
    assert_eq!(
        run_git(&sibling, &["status", "--porcelain"]),
        sibling_status_before
    );
    assert_eq!(
        fs::read_to_string(sibling.join("sibling.txt")).unwrap(),
        "dirty unstaged edit\n"
    );
    assert_eq!(
        fs::read_to_string(sibling.join("staged.txt")).unwrap(),
        "staged sibling edit\n"
    );
    assert_eq!(
        fs::read_to_string(sibling.join("untracked.txt")).unwrap(),
        "untracked sibling note\n"
    );
    assert!(
        !sibling.join("exported.txt").exists(),
        "exported files must not leak into ambient GIT_WORK_TREE"
    );
}

#[tokio::test]
async fn test_reimport_git_merges_local_bookmarks_without_rewinding_or_clobbering_conflicts() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_sqlite_server(&dir.path().join("reimport_bookmarks.db")).await;

    // Seed a Git repo with 4 branches all starting at `base`:
    // - `cc-ahead`: will advance in Commit Cloud while Git stays at `base`
    // - `git-ff`: will fast-forward in Git while Commit Cloud stays at `base`
    // - `diverged`: will advance on both sides independently
    // - `already-conflicted`: will already be conflicted in Commit Cloud before re-import
    let src_git = dir.path().join("src_git");
    fs::create_dir_all(&src_git).unwrap();
    run_git(&src_git, &["init", "-b", "cc-ahead"]);
    fs::write(src_git.join("base.txt"), "base\n").unwrap();
    run_git(&src_git, &["add", "."]);
    run_git(&src_git, &["commit", "-m", "base"]);
    let base_hex = run_git(&src_git, &["rev-parse", "HEAD"]);
    run_git(&src_git, &["branch", "git-ff"]);
    run_git(&src_git, &["branch", "diverged"]);
    run_git(&src_git, &["branch", "already-conflicted"]);

    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "import-git",
            "--server",
            server.url(),
            "--repo-id",
            "repo-reimport-bm",
            "--create",
            "--git-dir",
            src_git.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Clone into Commit Cloud, advance `cc-ahead` and `diverged`, and create two diverged
    // commits so `already-conflicted` is a conflicted bookmark in the View.
    let ws = dir.path().join("cc_ws");
    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "clone",
            "--server",
            server.url(),
            "-r",
            "cc-ahead",
            "repo-reimport-bm",
            ws.to_str().unwrap(),
        ])
        .assert()
        .success();

    fs::write(ws.join("cc_ahead.txt"), "cloud ahead\n").unwrap();
    jj_cmd(&ws, &xdg)
        .args(["describe", "-m", "cloud ahead commit"])
        .assert()
        .success();
    jj_cmd(&ws, &xdg)
        .args(["bookmark", "set", "cc-ahead", "-r", "@"])
        .assert()
        .success();
    let cc_ahead_hex = String::from_utf8(
        jj_cmd(&ws, &xdg)
            .args(["log", "--no-graph", "-r", "cc-ahead", "-T", "commit_id"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap()
    .trim()
    .to_string();

    jj_cmd(&ws, &xdg)
        .args(["new", "diverged", "-m", "cloud diverged commit"])
        .assert()
        .success();
    fs::write(ws.join("cc_div.txt"), "cloud side\n").unwrap();
    jj_cmd(&ws, &xdg)
        .args(["bookmark", "set", "diverged", "-r", "@"])
        .assert()
        .success();
    let cc_div_hex = String::from_utf8(
        jj_cmd(&ws, &xdg)
            .args(["log", "--no-graph", "-r", "diverged", "-T", "commit_id"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap()
    .trim()
    .to_string();

    // Create two commits on top of `base` and mark `already-conflicted` conflicted in the View.
    jj_cmd(&ws, &xdg)
        .args(["new", "already-conflicted", "-m", "conflict side 1"])
        .assert()
        .success();
    fs::write(ws.join("c1.txt"), "side 1\n").unwrap();
    let conf1_hex = String::from_utf8(
        jj_cmd(&ws, &xdg)
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
    jj_cmd(&ws, &xdg)
        .args(["new", "already-conflicted", "-m", "conflict side 2"])
        .assert()
        .success();
    fs::write(ws.join("c2.txt"), "side 2\n").unwrap();
    let conf2_hex = String::from_utf8(
        jj_cmd(&ws, &xdg)
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

    {
        let server_url = server.url().to_string();
        let base_bytes = hex::decode(&base_hex).unwrap();
        let conf1_bytes = hex::decode(&conf1_hex).unwrap();
        let conf2_bytes = hex::decode(&conf2_hex).unwrap();
        cc_lib::util::run_on_tokio(async move {
            let mut op_client = cc_lib::util::connect_op_store_client(server_url).await?;
            let head = op_client
                .reconcile_op_heads(ReconcileOpHeadsRequest {
                    repo_id: "repo-reimport-bm".to_string(),
                })
                .await?
                .into_inner()
                .op_head;
            let op = op_client
                .read_operation(ReadOperationRequest {
                    repo_id: "repo-reimport-bm".to_string(),
                    operation_id: head.clone(),
                })
                .await?
                .into_inner()
                .operation
                .unwrap();
            let mut view = op_client
                .read_view(ReadViewRequest {
                    repo_id: "repo-reimport-bm".to_string(),
                    view_id: op.view_id,
                })
                .await?
                .into_inner()
                .view
                .unwrap();
            view.local_bookmarks.insert(
                "already-conflicted".to_string(),
                cc_common::op_store::RefTarget {
                    removes: vec![cc_common::op_store::RefTargetTerm {
                        commit_id: base_bytes,
                    }],
                    adds: vec![
                        cc_common::op_store::RefTargetTerm {
                            commit_id: conf1_bytes,
                        },
                        cc_common::op_store::RefTargetTerm {
                            commit_id: conf2_bytes,
                        },
                    ],
                },
            );
            op_client
                .publish_operation(cc_common::op_store::PublishOperationRequest {
                    repo_id: "repo-reimport-bm".to_string(),
                    view: Some(view),
                    operation: Some(cc_common::op_store::Operation {
                        view_id: Vec::new(),
                        parents: vec![head.clone()],
                        metadata: Some(cc_common::op_store::OperationMetadata {
                            description: "seed conflicted bookmark".to_string(),
                            ..Default::default()
                        }),
                        commit_predecessors: Vec::new(),
                        commit_predecessors_set: true,
                    }),
                    old_op_head_ids: vec![head],
                })
                .await?;
            Ok(())
        })
        .await
        .unwrap();
    }

    // Advance `git-ff` and `diverged` in the Git repo (while leaving `cc-ahead` and
    // `already-conflicted` at `base`).
    run_git(&src_git, &["checkout", "git-ff"]);
    fs::write(src_git.join("git_ff.txt"), "git fast forward\n").unwrap();
    run_git(&src_git, &["add", "."]);
    run_git(&src_git, &["commit", "-m", "git ff commit"]);
    let git_ff_hex = run_git(&src_git, &["rev-parse", "git-ff"]);

    run_git(&src_git, &["checkout", "diverged"]);
    fs::write(src_git.join("git_div.txt"), "git side\n").unwrap();
    run_git(&src_git, &["add", "."]);
    run_git(&src_git, &["commit", "-m", "git diverged commit"]);
    let git_div_hex = run_git(&src_git, &["rev-parse", "diverged"]);

    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "import-git",
            "--server",
            server.url(),
            "--repo-id",
            "repo-reimport-bm",
            "--git-dir",
            src_git.to_str().unwrap(),
        ])
        .assert()
        .success();

    let view = read_head_view(server.url(), "repo-reimport-bm").await;

    // `cc-ahead` must remain at Commit Cloud's commit (NOT rewound to `base`).
    let bm_ahead = &view.local_bookmarks["cc-ahead"];
    assert!(bm_ahead.removes.is_empty());
    assert_eq!(bm_ahead.adds.len(), 1);
    assert_eq!(hex::encode(&bm_ahead.adds[0].commit_id), cc_ahead_hex);

    // `git-ff` must fast-forward to Git's newer commit.
    let bm_ff = &view.local_bookmarks["git-ff"];
    assert!(bm_ff.removes.is_empty());
    assert_eq!(bm_ff.adds.len(), 1);
    assert_eq!(hex::encode(&bm_ff.adds[0].commit_id), git_ff_hex);

    // `diverged` must be conflicted and preserve both Commit Cloud and Git tips.
    let bm_div = &view.local_bookmarks["diverged"];
    let div_adds: HashSet<String> = bm_div
        .adds
        .iter()
        .map(|t| hex::encode(&t.commit_id))
        .collect();
    assert_eq!(div_adds, HashSet::from([cc_div_hex, git_div_hex]));

    // `already-conflicted` must remain conflicted without erasing prior sides.
    let bm_conf = &view.local_bookmarks["already-conflicted"];
    assert_eq!(bm_conf.removes.len(), 1);
    assert_eq!(hex::encode(&bm_conf.removes[0].commit_id), base_hex);
    let conf_adds: HashSet<String> = bm_conf
        .adds
        .iter()
        .map(|t| hex::encode(&t.commit_id))
        .collect();
    assert_eq!(conf_adds, HashSet::from([conf1_hex, conf2_hex]));

    // `jj bookmark list` loads the updated View and reports both conflicts.
    let bm_list = String::from_utf8(
        jj_cmd(&ws, &xdg)
            .args(["bookmark", "list"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap();
    assert!(
        bm_list.contains("diverged (conflicted):"),
        "expected conflicted `diverged` in `jj bookmark list`, got:\n{bm_list}"
    );
    assert!(
        bm_list.contains("already-conflicted (conflicted):"),
        "expected conflicted `already-conflicted` in `jj bookmark list`, got:\n{bm_list}"
    );
}

#[tokio::test]
async fn test_export_git_non_bare_cleans_up_new_destination_on_failure_and_retries_cleanly() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_server().await;

    let ws = dir.path().join("ws_export_rollback");
    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--repo-id",
            "repo-export-rollback",
            "--create",
            ws.to_str().unwrap(),
        ])
        .assert()
        .success();
    fs::write(ws.join("hello.txt"), "hello rollback\n").unwrap();
    jj_cmd(&ws, &xdg)
        .args(["describe", "-m", "good commit"])
        .assert()
        .success();
    jj_cmd(&ws, &xdg)
        .args(["bookmark", "create", "foo", "-r", "@"])
        .assert()
        .success();
    jj_cmd(&ws, &xdg)
        .args(["bookmark", "create", "foo/bar", "-r", "@"])
        .assert()
        .success();
    let good_commit = String::from_utf8(
        jj_cmd(&ws, &xdg)
            .args(["log", "--no-graph", "-r", "foo", "-T", "commit_id"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap()
    .trim()
    .to_string();

    // Batch-internal D/F conflict (`foo` vs `foo/bar`) must fail preflight before creating
    // the destination directory or initializing `.git` in an existing empty directory.
    let nested_df_root = dir.path().join("nested_df");
    let nested_df_dst = nested_df_root.join("sub/dst");
    let df_stderr = String::from_utf8(
        jj_cmd(&ws, &xdg)
            .args([
                "cc",
                "export-git",
                "--git-dir",
                nested_df_dst.to_str().unwrap(),
            ])
            .assert()
            .failure()
            .get_output()
            .stderr
            .clone(),
    )
    .unwrap();
    assert!(
        df_stderr.contains("directory/file conflict"),
        "expected D/F conflict error, got: {df_stderr}"
    );
    assert!(
        !nested_df_root.exists(),
        "failed export must not leave newly created destination directories behind"
    );

    let empty_df_dst = dir.path().join("empty_df");
    fs::create_dir_all(&empty_df_dst).unwrap();
    jj_cmd(&ws, &xdg)
        .args([
            "cc",
            "export-git",
            "--git-dir",
            empty_df_dst.to_str().unwrap(),
        ])
        .assert()
        .failure();
    assert!(empty_df_dst.is_dir());
    assert!(
        fs::read_dir(&empty_df_dst).unwrap().next().is_none(),
        "failed export into existing empty directory must leave it empty"
    );

    // Resolve the D/F conflict and verify retrying both destinations succeeds.
    jj_cmd(&ws, &xdg)
        .args(["bookmark", "delete", "foo/bar"])
        .assert()
        .success();
    for dst in [&nested_df_dst, &empty_df_dst] {
        jj_cmd(&ws, &xdg)
            .args(["cc", "export-git", "--git-dir", dst.to_str().unwrap()])
            .assert()
            .success();
        run_git(dst, &["fsck", "--full"]);
        assert_eq!(run_git(dst, &["rev-parse", "foo"]), good_commit);
        assert_eq!(run_git(dst, &["status", "--porcelain"]), "");
        assert_eq!(
            fs::read_to_string(dst.join("hello.txt")).unwrap(),
            "hello rollback\n"
        );
    }

    // Post-init failure (objects and refs succeed via gix, then `checkout_git_worktree` fails
    // because `git` is unavailable in `PATH`) must roll back newly created destinations and
    // existing empty dirs so a retry succeeds, while leaving pre-existing Git repos untouched.
    let empty_bin = dir.path().join("empty_bin");
    fs::create_dir_all(&empty_bin).unwrap();

    let nested_post_root = dir.path().join("nested_post_fail");
    let nested_post_dst = nested_post_root.join("inner/dst");
    let post_stderr = String::from_utf8(
        jj_cmd(&ws, &xdg)
            .env("PATH", &empty_bin)
            .args([
                "cc",
                "export-git",
                "--git-dir",
                nested_post_dst.to_str().unwrap(),
            ])
            .assert()
            .failure()
            .get_output()
            .stderr
            .clone(),
    )
    .unwrap();
    assert!(
        post_stderr.contains("checkout --force HEAD"),
        "expected post-init checkout failure, got: {post_stderr}"
    );
    assert!(
        !nested_post_root.exists(),
        "post-init failure must roll back all newly created directories"
    );

    let empty_post_dst = dir.path().join("empty_post_fail");
    fs::create_dir_all(&empty_post_dst).unwrap();
    jj_cmd(&ws, &xdg)
        .env("PATH", &empty_bin)
        .args([
            "cc",
            "export-git",
            "--git-dir",
            empty_post_dst.to_str().unwrap(),
        ])
        .assert()
        .failure();
    assert!(empty_post_dst.is_dir());
    assert!(
        fs::read_dir(&empty_post_dst).unwrap().next().is_none(),
        "post-init failure into existing empty dir must restore it to empty"
    );

    // Pre-existing non-bare repo with `foo` checked out must fail post-open without deleting or
    // modifying the existing repository.
    let existing_git = dir.path().join("existing_git");
    fs::create_dir_all(&existing_git).unwrap();
    run_git(
        &existing_git,
        &["-c", "init.defaultRefFormat=files", "init", "-b", "foo"],
    );
    fs::write(existing_git.join("existing.txt"), "keep existing\n").unwrap();
    run_git(&existing_git, &["add", "."]);
    run_git(&existing_git, &["commit", "-m", "existing commit"]);
    let existing_head = run_git(&existing_git, &["rev-parse", "HEAD"]);
    jj_cmd(&ws, &xdg)
        .args([
            "cc",
            "export-git",
            "--git-dir",
            existing_git.to_str().unwrap(),
        ])
        .assert()
        .failure();
    assert_eq!(
        run_git(&existing_git, &["rev-parse", "HEAD"]),
        existing_head
    );
    assert_eq!(
        fs::read_to_string(existing_git.join("existing.txt")).unwrap(),
        "keep existing\n"
    );

    // Retrying both rolled-back destinations with a working `PATH` must succeed.
    for dst in [&nested_post_dst, &empty_post_dst] {
        jj_cmd(&ws, &xdg)
            .args(["cc", "export-git", "--git-dir", dst.to_str().unwrap()])
            .assert()
            .success();
        run_git(dst, &["fsck", "--full"]);
        assert_eq!(run_git(dst, &["rev-parse", "foo"]), good_commit);
        assert_eq!(run_git(dst, &["status", "--porcelain"]), "");
        assert_eq!(
            fs::read_to_string(dst.join("hello.txt")).unwrap(),
            "hello rollback\n"
        );
    }
}

#[tokio::test]
async fn test_export_git_isolates_global_gitconfig_hooks_and_rejects_missing_dotdot_git_dir() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    fs::create_dir_all(&xdg).unwrap();
    let server = testutils::spawn_server().await;

    let ws = dir.path().join("ws_hooks_and_dotdot");
    jj_cmd(dir.path(), &xdg)
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--repo-id",
            "repo-hooks-dotdot",
            "--create",
            ws.to_str().unwrap(),
        ])
        .assert()
        .success();
    fs::write(ws.join("payload.txt"), "safe checkout\n").unwrap();
    jj_cmd(&ws, &xdg)
        .args(["describe", "-m", "commit for config isolation test"])
        .assert()
        .success();
    jj_cmd(&ws, &xdg)
        .args(["bookmark", "create", "main", "-r", "@"])
        .assert()
        .success();
    jj_cmd(&ws, &xdg)
        .args(["bookmark", "create", "at-root", "-r", "root()"])
        .assert()
        .success();

    // Configure a hostile `core.hooksPath` in `$HOME/.gitconfig`, `$XDG_CONFIG_HOME/git/config`,
    // and `$GIT_CONFIG_SYSTEM` with a `post-checkout` hook that writes a marker file and exits 1.
    let fake_home = dir.path().join("fake_home");
    let hooks_dir = dir.path().join("hooks");
    let hook_marker = dir.path().join("hook_executed_marker");
    fs::create_dir_all(&fake_home).unwrap();
    fs::create_dir_all(&hooks_dir).unwrap();
    fs::create_dir_all(xdg.join("git")).unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let hook_script = hooks_dir.join("post-checkout");
        fs::write(
            &hook_script,
            format!(
                "#!/bin/sh\necho ran > \"{}\"\nexit 1\n",
                hook_marker.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&hook_script, fs::Permissions::from_mode(0o755)).unwrap();
    }

    let hostile_gitconfig = format!("[core]\n\thooksPath = {}\n", hooks_dir.display());
    fs::write(fake_home.join(".gitconfig"), &hostile_gitconfig).unwrap();
    fs::write(xdg.join("git/config"), &hostile_gitconfig).unwrap();
    let fake_system_gitconfig = dir.path().join("fake_system_gitconfig");
    fs::write(&fake_system_gitconfig, &hostile_gitconfig).unwrap();

    let dst = dir.path().join("exported_no_hooks");
    let export_stderr = String::from_utf8(
        jj_cmd(&ws, &xdg)
            .env("HOME", &fake_home)
            .env("GIT_CONFIG_SYSTEM", &fake_system_gitconfig)
            .env("GIT_SHALLOW_FILE", dir.path().join("nonexistent-shallow"))
            .env("GIT_REPLACE_REF_BASE", "refs/poisoned-replace/")
            .env("GIT_NO_REPLACE_OBJECTS", "1")
            .args(["cc", "export-git", "--git-dir", dst.to_str().unwrap()])
            .assert()
            .success()
            .get_output()
            .stderr
            .clone(),
    )
    .unwrap();
    assert!(
        !hook_marker.exists(),
        "export-git must disable global/system Git config so post-checkout hooks never run"
    );
    assert_eq!(
        fs::read_to_string(dst.join("payload.txt")).unwrap(),
        "safe checkout\n"
    );
    assert_eq!(run_git(&dst, &["status", "--porcelain"]), "");
    assert!(
        export_stderr.contains("Skipped bookmark \"at-root\""),
        "expected root-targeting bookmark to be reported in skipped bookmarks, got: {export_stderr}"
    );
    assert_eq!(
        run_git(&dst, &["for-each-ref", "--format=%(refname)"]),
        "refs/heads/main"
    );

    // `<existing_non_empty_dir>/missing/..` must be rejected without creating `missing` or
    // initializing `.git` inside `<existing_non_empty_dir>`.
    let existing_non_empty = dir.path().join("user_data");
    fs::create_dir_all(&existing_non_empty).unwrap();
    fs::write(
        existing_non_empty.join("payload.txt"),
        "precious user file\n",
    )
    .unwrap();

    let bypass_path = existing_non_empty.join("missing/..");
    jj_cmd(&ws, &xdg)
        .args([
            "cc",
            "export-git",
            "--git-dir",
            bypass_path.to_str().unwrap(),
        ])
        .assert()
        .failure();
    assert!(
        !existing_non_empty.join("missing").exists(),
        "rejected path must not create intermediate `missing` directory"
    );
    assert!(
        !existing_non_empty.join(".git").exists(),
        "rejected path must not initialize `.git` inside existing non-empty directory"
    );
    assert_eq!(
        fs::read_to_string(existing_non_empty.join("payload.txt")).unwrap(),
        "precious user file\n"
    );
}

#[tokio::test]
async fn test_rejects_merge_commit_with_root_commit_as_parent() {
    use cc_common::backend::backend_service_client::BackendServiceClient;
    use cc_common::backend::{Commit, WriteCommitRequest};
    use cc_common::{EMPTY_TREE_ID_BYTES, ROOT_CHANGE_ID_BYTES, ROOT_COMMIT_ID_BYTES};

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
