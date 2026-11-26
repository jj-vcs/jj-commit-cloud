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

use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

use cc_common::backend::Commit;
use cc_common::op_store::op_store_service_server::OpStoreServiceServer;
use cc_common::op_store::{RefTarget, RefTargetTerm};
use cc_common::{CHANGE_ID_LENGTH, COMMIT_ID_LENGTH, ROOT_COMMIT_ID_BYTES};
use gix::objs::WriteTo as _;
use gix::prelude::Write as _;

use super::*;

#[test]
fn git_branch_ref_name_rejects_paths_and_invalid_git_names() {
    for name in ["main", "feature/auth", "release-1.2", "user@host"] {
        assert!(
            git_branch_ref_name(name).is_some(),
            "{name:?} should be exported"
        );
    }
    for name in [
        "../x",
        "../../config",
        "a/../../b",
        "/home/alice/.ssh/authorized_keys",
        "a//b",
        "trailing/",
        ".hidden",
        "has space",
        "tilde~1",
        "caret^",
        "colon:name",
        "question?",
        "star*",
        "bracket[",
        "back\\slash",
        "ends.lock",
        "ends.",
        "at@{1}",
        "HEAD",
        "-flag",
        "-",
        "",
    ] {
        assert!(
            git_branch_ref_name(name).is_none(),
            "{name:?} should be skipped"
        );
    }
}

#[test]
fn importable_git_refs_match_jj_git_import() {
    for name in [
        "refs/heads/main",
        "refs/heads/feature/auth",
        "refs/remotes/origin/main",
        "refs/remotes/origin/feature/auth",
        "refs/remotes/origin/user@host",
        "refs/tags/v1.0",
    ] {
        assert!(is_importable_git_ref(name), "{name} should be imported");
    }
    for name in [
        "refs/heads/HEAD",
        "refs/heads/.invalid",
        "refs/remotes/origin/HEAD",
        "refs/remotes/team@corp/main",
        "refs/remotes/git/main",
        "refs/jj/keep/0123456789abcdef0123456789abcdef01234567",
        "refs/stash",
        "refs/notes/commits",
        "refs/pull/1/head",
        "refs/changes/01/1/1",
        "HEAD",
    ] {
        assert!(!is_importable_git_ref(name), "{name} should be ignored");
    }
}

#[test]
fn resolve_dot_git_dir_and_common_dir_handle_worktrees_and_submodules() {
    let dir = tempfile::tempdir().unwrap();
    let main_git = dir.path().join("main/.git");
    let wt_git = main_git.join("worktrees/wt");
    fs::create_dir_all(&wt_git).unwrap();
    fs::write(wt_git.join("HEAD"), "ref: refs/heads/wt\n").unwrap();
    fs::write(wt_git.join("commondir"), "../..\n").unwrap();

    let wt = dir.path().join("wt");
    fs::create_dir_all(&wt).unwrap();
    fs::write(wt.join(".git"), "gitdir: ../main/.git/worktrees/wt\n").unwrap();

    let resolved = resolve_dot_git_dir(&wt);
    assert_eq!(
        fs::canonicalize(&resolved).unwrap(),
        fs::canonicalize(&wt_git).unwrap()
    );
    // Passing the `.git` file directly resolves the same way.
    assert_eq!(
        fs::canonicalize(resolve_dot_git_dir(&wt.join(".git"))).unwrap(),
        fs::canonicalize(&wt_git).unwrap()
    );
    assert_eq!(
        fs::canonicalize(resolve_git_common_dir(&resolved)).unwrap(),
        fs::canonicalize(&main_git).unwrap()
    );
}

#[tokio::test]
async fn discover_git_refs_propagates_non_commit_branch_and_skips_non_commit_tag() {
    let dir = tempfile::tempdir().unwrap();
    let repo = gix::init_bare(dir.path()).unwrap();
    let blob_oid = repo
        .objects
        .write_buf(gix::objs::Kind::Blob, b"hello\n")
        .unwrap();
    let empty_tree = repo.objects.write_buf(gix::objs::Kind::Tree, &[]).unwrap();
    let commit_obj = gix::objs::Commit {
        tree: empty_tree,
        parents: Default::default(),
        author: gix::actor::Signature {
            name: "Test".into(),
            email: "test@example.com".into(),
            time: gix::date::Time::new(1_730_000_000, 0),
        },
        committer: gix::actor::Signature {
            name: "Test".into(),
            email: "test@example.com".into(),
            time: gix::date::Time::new(1_730_000_000, 0),
        },
        encoding: None,
        extra_headers: Vec::new(),
        message: "root\n".into(),
    };
    let mut commit_buf = Vec::new();
    commit_obj.write_to(&mut commit_buf).unwrap();
    let commit_oid = repo
        .objects
        .write_buf(gix::objs::Kind::Commit, &commit_buf)
        .unwrap();

    // Valid branch + non-commit blob tag + non-importable `refs/heads/HEAD` pointing at a blob.
    write_git_refs(
        &repo,
        vec![
            (
                "refs/heads/main".try_into().unwrap(),
                gix::refs::Target::Object(commit_oid),
            ),
            (
                "refs/tags/blob-tag".try_into().unwrap(),
                gix::refs::Target::Object(blob_oid),
            ),
            (
                "refs/heads/HEAD".try_into().unwrap(),
                gix::refs::Target::Object(blob_oid),
            ),
        ],
    )
    .unwrap();

    let ts_repo = repo.into_sync();
    let (heads, local_branches, _) = discover_git_refs(&ts_repo, dir.path(), dir.path(), false)
        .await
        .expect("non-commit tag and refs/heads/HEAD must be skipped");
    assert_eq!(heads, vec![commit_oid.as_bytes().to_vec()]);
    assert_eq!(local_branches.keys().collect::<Vec<_>>(), ["main"]);

    // Now point a real branch at a blob: `discover_git_refs` must fail loudly.
    let repo = ts_repo.to_thread_local();
    write_git_refs(
        &repo,
        vec![(
            "refs/heads/bad-branch".try_into().unwrap(),
            gix::refs::Target::Object(blob_oid),
        )],
    )
    .unwrap();
    let err = discover_git_refs(&ts_repo, dir.path(), dir.path(), false)
        .await
        .expect_err("branch pointing at blob must error");
    assert!(
        err.to_string().contains("refs/heads/bad-branch"),
        "unexpected error: {err}"
    );
}

#[test]
fn write_git_refs_is_atomic_on_df_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let repo = gix::init_bare(dir.path()).unwrap();
    let empty_tree = repo.objects.write_buf(gix::objs::Kind::Tree, &[]).unwrap();
    let commit_obj = gix::objs::Commit {
        tree: empty_tree,
        parents: Default::default(),
        author: gix::actor::Signature {
            name: "Test".into(),
            email: "test@example.com".into(),
            time: gix::date::Time::new(1_730_000_000, 0),
        },
        committer: gix::actor::Signature {
            name: "Test".into(),
            email: "test@example.com".into(),
            time: gix::date::Time::new(1_730_000_000, 0),
        },
        encoding: None,
        extra_headers: Vec::new(),
        message: "atomic ref test\n".into(),
    };
    let mut commit_buf = Vec::new();
    commit_obj.write_to(&mut commit_buf).unwrap();
    let commit_oid = repo
        .objects
        .write_buf(gix::objs::Kind::Commit, &commit_buf)
        .unwrap();

    let updates = vec![
        (
            git_branch_ref_name("aaa").unwrap(),
            gix::refs::Target::Object(commit_oid),
        ),
        (
            git_branch_ref_name("conflict").unwrap(),
            gix::refs::Target::Object(commit_oid),
        ),
        (
            git_branch_ref_name("conflict/nested").unwrap(),
            gix::refs::Target::Object(commit_oid),
        ),
    ];
    assert!(
        write_git_refs(&repo, updates).is_err(),
        "D/F ref conflict must fail the transaction"
    );
    assert!(
        repo.try_find_reference("refs/heads/aaa").unwrap().is_none(),
        "atomic ref transaction must not leave partial refs on disk"
    );
    assert!(
        repo.try_find_reference("refs/heads/conflict")
            .unwrap()
            .is_none(),
        "atomic ref transaction must not leave partial refs on disk"
    );
}

#[test]
fn open_or_init_git_repo_propagates_errors_on_corrupt_existing_repo() {
    let dir = tempfile::tempdir().unwrap();
    let corrupt = dir.path().join("corrupt.git");
    fs::create_dir_all(&corrupt).unwrap();
    // A directory containing `HEAD` looks like a Git repo, so `open_or_init_git_repo` must
    // fail closed instead of silently re-initializing it.
    fs::write(corrupt.join("HEAD"), "not a valid repo\n").unwrap();
    assert!(
        open_or_init_git_repo(&corrupt, true).is_err(),
        "corrupt existing Git dir must return an error"
    );
}

#[test]
fn resolve_dot_git_dir_prefers_dot_git_when_worktree_has_tracked_head_file() {
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path().join("repo_with_head_file");
    fs::create_dir_all(repo.join(".git")).unwrap();
    fs::write(repo.join(".git/HEAD"), "ref: refs/heads/main\n").unwrap();
    // A tracked working-tree file named `HEAD` must not trick resolution into treating the
    // worktree root as a bare Git dir.
    fs::write(repo.join("HEAD"), "tracked file contents\n").unwrap();

    assert_eq!(
        fs::canonicalize(resolve_dot_git_dir(&repo)).unwrap(),
        fs::canonicalize(repo.join(".git")).unwrap()
    );
}

#[test]
fn open_or_init_git_repo_rejects_non_empty_non_git_dir_and_reftable_target() {
    let dir = tempfile::tempdir().unwrap();

    // Existing non-empty non-Git directory fails closed without creating `.git`.
    let non_empty = dir.path().join("non_empty");
    fs::create_dir_all(&non_empty).unwrap();
    fs::write(non_empty.join("README.txt"), "keep\n").unwrap();
    let err = match open_or_init_git_repo(&non_empty, false) {
        Ok(_) => panic!("non-empty non-Git dir must fail"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("not an empty directory"),
        "unexpected error: {err}"
    );
    assert!(!non_empty.join(".git").exists());

    // Existing regular file fails closed.
    let regular_file = dir.path().join("plain_file");
    fs::write(&regular_file, "hello\n").unwrap();
    assert!(open_or_init_git_repo(&regular_file, false).is_err());

    // Existing empty directory succeeds.
    let empty = dir.path().join("empty_dir");
    fs::create_dir_all(&empty).unwrap();
    let target = open_or_init_git_repo(&empty, false).expect("empty dir must succeed");
    assert!(target.newly_created);
    assert!(!target.is_bare);

    // Existing reftable target repo is rejected upfront before writing objects.
    let reftable_bare = dir.path().join("reftable.git");
    fs::create_dir_all(reftable_bare.join("reftable")).unwrap();
    fs::write(reftable_bare.join("HEAD"), "ref: refs/heads/.invalid\n").unwrap();
    let reftable_err = match open_or_init_git_repo(&reftable_bare, true) {
        Ok(_) => panic!("reftable target repo must fail upfront"),
        Err(err) => err,
    };
    assert!(
        reftable_err.to_string().contains("reftable"),
        "unexpected error: {reftable_err}"
    );

    let real_reftable = dir.path().join("real_reftable");
    fs::create_dir_all(&real_reftable).unwrap();
    let out = std::process::Command::new("git")
        .current_dir(&real_reftable)
        .args(["init", "-b", "main", "--ref-format=reftable"])
        .output()
        .unwrap();
    if out.status.success() {
        let err = match open_or_init_git_repo(&real_reftable, false) {
            Ok(_) => panic!("real reftable target repo must fail upfront"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("reftable"),
            "unexpected error for real reftable repo: {err}"
        );
    }
}

#[test]
fn write_git_refs_propagates_corrupt_ref_iteration_errors() {
    let dir = tempfile::tempdir().unwrap();
    let repo = gix::init_bare(dir.path()).unwrap();
    let empty_tree = repo.objects.write_buf(gix::objs::Kind::Tree, &[]).unwrap();
    let commit_obj = gix::objs::Commit {
        tree: empty_tree,
        parents: Default::default(),
        author: gix::actor::Signature {
            name: "Test".into(),
            email: "test@example.com".into(),
            time: gix::date::Time::new(1_730_000_000, 0),
        },
        committer: gix::actor::Signature {
            name: "Test".into(),
            email: "test@example.com".into(),
            time: gix::date::Time::new(1_730_000_000, 0),
        },
        encoding: None,
        extra_headers: Vec::new(),
        message: "corrupt ref iteration test\n".into(),
    };
    let mut commit_buf = Vec::new();
    commit_obj.write_to(&mut commit_buf).unwrap();
    let commit_oid = repo
        .objects
        .write_buf(gix::objs::Kind::Commit, &commit_buf)
        .unwrap();

    // Write a malformed loose ref so `references().all()` yields an iteration error.
    fs::create_dir_all(dir.path().join("refs/heads")).unwrap();
    fs::write(dir.path().join("refs/heads/corrupt"), "not-a-valid-oid\n").unwrap();

    let err = write_git_refs(
        &repo,
        vec![(
            git_branch_ref_name("main").unwrap(),
            gix::refs::Target::Object(commit_oid),
        )],
    )
    .expect_err("corrupt existing ref must fail D/F conflict preflight");
    assert!(
        err.to_string()
            .contains("Failed to read existing Git reference"),
        "unexpected error: {err}"
    );
    assert!(
        repo.try_find_reference("refs/heads/main")
            .unwrap()
            .is_none(),
        "new ref must not be written when existing ref iteration fails"
    );
}

#[tokio::test]
async fn discover_git_refs_rejects_non_utf8_importable_refs_strictly() {
    assert_eq!(
        decode_importable_git_ref_name(b"refs/heads/main").unwrap(),
        Some("refs/heads/main")
    );
    // Non-importable non-UTF-8 refs are ignored.
    assert_eq!(
        decode_importable_git_ref_name(b"refs/notes/bad\xff").unwrap(),
        None
    );
    // Importable non-UTF-8 refs return a clear error.
    let err = decode_importable_git_ref_name(b"refs/heads/bad\xff")
        .expect_err("non-UTF-8 branch name must error");
    assert!(
        err.to_string().contains("Non-UTF-8 Git reference name"),
        "unexpected error: {err}"
    );

    #[cfg(unix)]
    {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;

        let dir = tempfile::tempdir().unwrap();
        let repo = gix::init_bare(dir.path()).unwrap();
        let empty_tree = repo.objects.write_buf(gix::objs::Kind::Tree, &[]).unwrap();
        let commit_obj = gix::objs::Commit {
            tree: empty_tree,
            parents: Default::default(),
            author: gix::actor::Signature {
                name: "Test".into(),
                email: "test@example.com".into(),
                time: gix::date::Time::new(1_730_000_000, 0),
            },
            committer: gix::actor::Signature {
                name: "Test".into(),
                email: "test@example.com".into(),
                time: gix::date::Time::new(1_730_000_000, 0),
            },
            encoding: None,
            extra_headers: Vec::new(),
            message: "utf8 ref test\n".into(),
        };
        let mut commit_buf = Vec::new();
        commit_obj.write_to(&mut commit_buf).unwrap();
        let commit_oid = repo
            .objects
            .write_buf(gix::objs::Kind::Commit, &commit_buf)
            .unwrap();

        // Non-importable non-UTF-8 ref in `refs/notes/` must not break discovery.
        let notes_dir = dir.path().join("refs/notes");
        fs::create_dir_all(&notes_dir).unwrap();
        fs::write(
            notes_dir.join(OsStr::from_bytes(b"ignored\xff")),
            format!("{commit_oid}\n"),
        )
        .unwrap();
        let ts_repo = repo.into_sync();
        discover_git_refs(&ts_repo, dir.path(), dir.path(), false)
            .await
            .expect("non-importable non-UTF-8 ref must be ignored");

        // Importable non-UTF-8 ref in `refs/heads/` must fail strictly.
        let heads_dir = dir.path().join("refs/heads");
        fs::create_dir_all(&heads_dir).unwrap();
        fs::write(
            heads_dir.join(OsStr::from_bytes(b"bad\xffbranch")),
            format!("{commit_oid}\n"),
        )
        .unwrap();
        let err = discover_git_refs(&ts_repo, dir.path(), dir.path(), false)
            .await
            .expect_err("non-UTF-8 refs/heads/* must error");
        assert!(
            err.to_string().contains("Non-UTF-8 Git reference name"),
            "unexpected error: {err}"
        );
    }
}

#[test]
fn reftable_objects_dir_does_not_delete_preexisting_path_on_collision() {
    let dir = tempfile::tempdir().unwrap();
    let dot_git = dir.path().join("reftable.git");
    fs::create_dir_all(dot_git.join("reftable")).unwrap();
    fs::create_dir_all(dot_git.join("objects")).unwrap();
    fs::write(dot_git.join("HEAD"), "ref: refs/heads/.invalid\n").unwrap();

    // Pre-existing directory with a sentinel file at the colliding target path.
    let existing = dir.path().join("preexisting.git");
    fs::create_dir_all(&existing).unwrap();
    let sentinel = existing.join("sentinel.txt");
    fs::write(&sentinel, "do not delete\n").unwrap();

    let err = match ReftableObjectsDir::create_at(&dot_git, existing.clone()) {
        Ok(_) => panic!("collision on pre-existing path must fail with AlreadyExists"),
        Err(err) => err,
    };
    assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
    assert!(
        existing.is_dir(),
        "Drop must NOT delete a pre-existing directory on AlreadyExists"
    );
    assert_eq!(
        fs::read_to_string(&sentinel).unwrap(),
        "do not delete\n",
        "sentinel file inside pre-existing directory must remain intact"
    );

    // Failure AFTER exclusive directory creation (missing `objects/` in source repo) MUST
    // clean up the newly created directory via Drop.
    let broken_dot_git = dir.path().join("broken_reftable.git");
    fs::create_dir_all(broken_dot_git.join("reftable")).unwrap();
    fs::write(broken_dot_git.join("HEAD"), "ref: refs/heads/.invalid\n").unwrap();
    let partial_target = dir.path().join("partial_created.git");
    assert!(
        ReftableObjectsDir::create_at(&broken_dot_git, partial_target.clone()).is_err(),
        "missing source objects/ dir must fail"
    );
    assert!(
        !partial_target.exists(),
        "partially initialized directory owned by ReftableObjectsDir must be cleaned up on error"
    );

    // Happy path creates valid alternates and removes the temporary dir on Drop.
    let created_path = {
        let guard = ReftableObjectsDir::create_if_reftable(&dot_git)
            .unwrap()
            .expect("reftable dir must create guard");
        assert!(guard.path.join("HEAD").is_file());
        assert!(guard.path.join("objects/info/alternates").is_file());
        guard.path.clone()
    };
    assert!(
        !created_path.exists(),
        "Drop must remove the temporary directory created by ReftableObjectsDir"
    );
}

#[tokio::test]
async fn target_git_repo_rollback_cleans_newly_created_dirs_and_preserves_existing() {
    let dir = tempfile::tempdir().unwrap();

    // Newly created nested non-bare path rolls back all newly created ancestors on drop.
    let nested_root = dir.path().join("nested_uncommitted");
    let nested_dst = nested_root.join("sub/repo");
    {
        let target = open_or_init_git_repo(&nested_dst, false).unwrap();
        assert!(target.newly_created);
        assert!(nested_dst.join(".git").is_dir());
    }
    assert!(
        !nested_root.exists(),
        "dropping uncommitted TargetGitRepo must remove first newly created ancestor"
    );

    // Existing empty directory is restored to empty without removing the directory itself.
    let empty_dir = dir.path().join("preexisting_empty");
    fs::create_dir_all(&empty_dir).unwrap();
    {
        let target = open_or_init_git_repo(&empty_dir, false).unwrap();
        assert!(target.newly_created);
        assert!(empty_dir.join(".git").is_dir());
        fs::write(empty_dir.join("partial_checkout.txt"), "partial\n").unwrap();
    }
    assert!(
        empty_dir.is_dir(),
        "pre-existing empty directory itself must remain after rollback"
    );
    assert!(
        fs::read_dir(&empty_dir).unwrap().next().is_none(),
        "pre-existing empty directory must have all created contents removed on rollback"
    );

    // Calling `commit()` keeps the newly created repo intact after drop.
    let committed_dst = dir.path().join("committed_nested/repo");
    {
        let mut target = open_or_init_git_repo(&committed_dst, false).unwrap();
        assert!(target.newly_created);
        target.commit();
    }
    assert!(committed_dst.join(".git").is_dir());

    // Existing Git repository is never deleted on drop without `commit()`.
    fs::write(committed_dst.join("keep.txt"), "keep\n").unwrap();
    {
        let target = open_or_init_git_repo(&committed_dst, false).unwrap();
        assert!(!target.newly_created);
    }
    assert!(committed_dst.join(".git").is_dir());
    assert_eq!(
        fs::read_to_string(committed_dst.join("keep.txt")).unwrap(),
        "keep\n"
    );

    // Post-init failure in `GitExporter::run` (missing tree) rolls back newly created
    // destinations while leaving an existing Git repo untouched.
    let missing_tree_id = vec![0xcd; COMMIT_ID_LENGTH];
    let mut bad_commit = Commit {
        commit_id: Vec::new(),
        change_id: vec![0x11; CHANGE_ID_LENGTH],
        parent_commit_ids: vec![ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![missing_tree_id],
        description: "commit with missing tree".to_string(),
        ..Default::default()
    };
    let bad_commit_bytes = serialize_proto_commit_to_git(&bad_commit, true).unwrap();
    let commit_id = gix::objs::compute_hash(
        gix::hash::Kind::Sha1,
        gix::objs::Kind::Commit,
        &bad_commit_bytes,
    )
    .unwrap()
    .as_bytes()
    .to_vec();
    bad_commit.commit_id = commit_id.clone();
    let initial_view = cc_common::op_store::View {
        head_ids: vec![commit_id.clone()],
        wc_commit_ids: HashMap::new(),
        local_bookmarks: HashMap::from([(
            "main".to_string(),
            RefTarget {
                removes: Vec::new(),
                adds: vec![RefTargetTerm {
                    commit_id: commit_id.clone(),
                }],
            },
        )]),
        remote_bookmarks: HashMap::new(),
    };
    let op_store = MockOpStoreService {
        initial_view,
        published_view: Arc::new(std::sync::Mutex::new(None)),
    };
    let backend = MockBackendService {
        commits: HashMap::from([(commit_id, bad_commit)]),
        trees: HashMap::new(),
        files: HashMap::new(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let incoming = futures::stream::unfold(listener, |listener| async move {
        Some((listener.accept().await.map(|(stream, _)| stream), listener))
    });
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(OpStoreServiceServer::new(op_store))
            .add_service(
                cc_common::backend::backend_service_server::BackendServiceServer::new(backend),
            )
            .serve_with_incoming(incoming),
    );

    let failed_nested_root = dir.path().join("exporter_fail_nested");
    let failed_nested_dst = failed_nested_root.join("inner/dst");
    let err = GitExporter::new(GitExportOptions {
        git_dir: failed_nested_dst,
        server_url: url.clone(),
        repo_id: "repo".to_string(),
        project_id: None,
        bare: false,
    })
    .run()
    .await
    .expect_err("missing tree must fail export after init");
    assert!(
        err.to_string().contains("missing tree"),
        "unexpected error: {err}"
    );
    assert!(
        !failed_nested_root.exists(),
        "post-init export failure must remove newly created destination directory tree"
    );

    let failed_empty = dir.path().join("exporter_fail_empty");
    fs::create_dir_all(&failed_empty).unwrap();
    GitExporter::new(GitExportOptions {
        git_dir: failed_empty.clone(),
        server_url: url.clone(),
        repo_id: "repo".to_string(),
        project_id: None,
        bare: false,
    })
    .run()
    .await
    .expect_err("missing tree must fail export into empty dir");
    assert!(failed_empty.is_dir());
    assert!(
        fs::read_dir(&failed_empty).unwrap().next().is_none(),
        "post-init export failure into existing empty dir must leave it empty"
    );

    // Existing bare repo must remain intact after post-init failure.
    let existing_bare = dir.path().join("existing_bare.git");
    gix::init_bare(&existing_bare).unwrap();
    fs::write(existing_bare.join("sentinel"), "keep\n").unwrap();
    GitExporter::new(GitExportOptions {
        git_dir: existing_bare.clone(),
        server_url: url,
        repo_id: "repo".to_string(),
        project_id: None,
        bare: true,
    })
    .run()
    .await
    .expect_err("missing tree must fail export into existing bare repo");
    assert!(existing_bare.join("HEAD").is_file());
    assert_eq!(
        fs::read_to_string(existing_bare.join("sentinel")).unwrap(),
        "keep\n"
    );
}

#[test]
fn open_or_init_git_repo_rejects_unnormalized_parent_traversal_and_handles_valid_dotdot() {
    let dir = tempfile::tempdir().unwrap();

    // `<existing_non_empty_dir>/missing/..` must fail without creating `missing` or `.git`.
    let non_empty = dir.path().join("non_empty");
    fs::create_dir_all(&non_empty).unwrap();
    fs::write(non_empty.join("precious.txt"), "keep\n").unwrap();
    let missing_dotdot = non_empty.join("missing/..");
    let err = match open_or_init_git_repo(&missing_dotdot, false) {
        Ok(_) => panic!("non_empty/missing/.. must be rejected"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("cannot traverse '..'"),
        "unexpected error: {err}"
    );
    assert!(
        !non_empty.join("missing").exists(),
        "missing directory must not be created"
    );
    assert!(
        !non_empty.join(".git").exists(),
        ".git must not be initialized inside existing non-empty dir"
    );
    assert_eq!(
        fs::read_to_string(non_empty.join("precious.txt")).unwrap(),
        "keep\n"
    );

    // `<existing_non_empty_dir>/existing_sub/..` normalizes to `<existing_non_empty_dir>`
    // and fails the non-empty-directory guard.
    fs::create_dir_all(non_empty.join("existing_sub")).unwrap();
    let existing_sub_dotdot = non_empty.join("existing_sub/..");
    let err = match open_or_init_git_repo(&existing_sub_dotdot, false) {
        Ok(_) => panic!("non_empty/existing_sub/.. must fail non-empty guard"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("not an empty directory"),
        "unexpected error: {err}"
    );
    assert!(!non_empty.join(".git").exists());

    // `<existing_empty_dir>/missing/..` and `<non_empty>/precious.txt/..` also fail.
    let empty = dir.path().join("empty");
    fs::create_dir_all(&empty).unwrap();
    assert!(open_or_init_git_repo(&empty.join("missing/.."), false).is_err());
    assert!(!empty.join("missing").exists());
    assert!(!empty.join(".git").exists());

    assert!(open_or_init_git_repo(&non_empty.join("precious.txt/.."), false).is_err());
    assert!(!non_empty.join(".git").exists());

    // Symlink followed by `..` resolves to the symlink target's parent (matching OS
    // semantics) rather than collapsing lexically.
    #[cfg(unix)]
    {
        let other_non_empty = dir.path().join("other_non_empty");
        fs::create_dir_all(other_non_empty.join("sub")).unwrap();
        fs::write(other_non_empty.join("sentinel.txt"), "do not touch\n").unwrap();
        std::os::unix::fs::symlink(other_non_empty.join("sub"), empty.join("link_to_sub")).unwrap();
        let err = match open_or_init_git_repo(&empty.join("link_to_sub/.."), false) {
            Ok(_) => panic!("symlink/.. targeting non-empty parent must be rejected"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("not an empty directory"),
            "unexpected error for symlink/..: {err}"
        );
        assert!(!other_non_empty.join(".git").exists());
        assert!(!empty.join(".git").exists());
    }

    // Valid `..` through an existing directory initializes and rolls back.
    let sibling_anchor = dir.path().join("anchor");
    fs::create_dir_all(&sibling_anchor).unwrap();
    let valid_dotdot_dst = sibling_anchor.join("../fresh_via_dotdot");
    let expected_dst = dir.path().join("fresh_via_dotdot");
    {
        let target = open_or_init_git_repo(&valid_dotdot_dst, false)
            .expect("valid .. through existing directory must succeed");
        assert!(target.newly_created);
        assert_eq!(target.git_dir, expected_dst);
        assert!(expected_dst.join(".git").is_dir());
    }
    assert!(
        !expected_dst.exists(),
        "dropping uncommitted TargetGitRepo initialized via valid .. must roll back cleanly"
    );
    assert!(sibling_anchor.is_dir());
}

#[tokio::test]
async fn discover_git_refs_skips_unborn_head_and_rejects_corrupt_or_dangling_head() {
    let dir = tempfile::tempdir().unwrap();

    // Files backend: unborn symbolic HEAD succeeds with zero heads.
    let unborn_dir = dir.path().join("unborn_files.git");
    let unborn_repo = gix::init_bare(&unborn_dir).unwrap().into_sync();
    let (heads, local, remote) = discover_git_refs(&unborn_repo, &unborn_dir, &unborn_dir, false)
        .await
        .expect("unborn symbolic HEAD must be skipped cleanly");
    assert!(heads.is_empty());
    assert!(local.is_empty());
    assert!(remote.is_empty());

    // Files backend: dangling detached HEAD pointing at nonexistent OID must error.
    let dangling_dir = dir.path().join("dangling_files.git");
    let dangling_repo = gix::init_bare(&dangling_dir).unwrap();
    fs::write(
        dangling_dir.join("HEAD"),
        "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef\n",
    )
    .unwrap();
    let dangling_ts = dangling_repo.into_sync();
    let err = discover_git_refs(&dangling_ts, &dangling_dir, &dangling_dir, false)
        .await
        .expect_err("dangling detached HEAD must fail");
    assert!(
        err.to_string().contains("HEAD"),
        "unexpected error for dangling HEAD: {err}"
    );

    // Files backend: detached HEAD pointing at a blob must error.
    let blob_head_dir = dir.path().join("blob_head_files.git");
    let blob_head_repo = gix::init_bare(&blob_head_dir).unwrap();
    let blob_oid = blob_head_repo
        .objects
        .write_buf(gix::objs::Kind::Blob, b"not a commit\n")
        .unwrap();
    fs::write(blob_head_dir.join("HEAD"), format!("{blob_oid}\n")).unwrap();
    let blob_head_ts = blob_head_repo.into_sync();
    let err = discover_git_refs(&blob_head_ts, &blob_head_dir, &blob_head_dir, false)
        .await
        .expect_err("detached HEAD pointing to blob must fail");
    assert!(
        err.to_string().contains("HEAD"),
        "unexpected error for blob HEAD: {err}"
    );

    // Files backend: corrupt HEAD file must error.
    let corrupt_dir = dir.path().join("corrupt_head_files.git");
    let corrupt_repo = gix::init_bare(&corrupt_dir).unwrap();
    fs::write(corrupt_dir.join("HEAD"), "totally-corrupt-head\n").unwrap();
    let corrupt_ts = corrupt_repo.into_sync();
    let err = discover_git_refs(&corrupt_ts, &corrupt_dir, &corrupt_dir, false)
        .await
        .expect_err("corrupt HEAD file must fail");
    assert!(
        err.to_string().contains("HEAD"),
        "unexpected error for corrupt HEAD: {err}"
    );

    // Reftable backend: unborn symbolic HEAD succeeds, while dangling detached HEAD errors.
    let reftable_dir = dir.path().join("reftable_head");
    fs::create_dir_all(&reftable_dir).unwrap();
    let init_out = std::process::Command::new("git")
        .current_dir(&reftable_dir)
        .args(["init", "-b", "main", "--ref-format=reftable"])
        .output()
        .unwrap();
    if init_out.status.success() {
        let dot_git = reftable_dir.join(".git");
        let guard = ReftableObjectsDir::create_if_reftable(&dot_git)
            .unwrap()
            .expect("reftable dir must create helper");
        let ts_repo =
            gix::ThreadSafeRepository::open_opts(&guard.path, gix::open::Options::isolated())
                .unwrap();
        let (heads, local, _) = discover_git_refs(&ts_repo, &dot_git, &reftable_dir, true)
            .await
            .expect("unborn reftable HEAD must be skipped cleanly");
        assert!(heads.is_empty());
        assert!(local.is_empty());

        // Create a commit, detach HEAD to it, delete `main`, then delete its loose commit
        // object so HEAD becomes a dangling detached HEAD in the reftable store.
        fs::write(reftable_dir.join("a.txt"), "hello\n").unwrap();
        let add_status = std::process::Command::new("git")
            .current_dir(&reftable_dir)
            .args(["add", "a.txt"])
            .status()
            .unwrap();
        assert!(add_status.success());
        let commit_status = std::process::Command::new("git")
            .current_dir(&reftable_dir)
            .args([
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.com",
                "commit",
                "-m",
                "init",
            ])
            .status()
            .unwrap();
        assert!(commit_status.success());
        let cid_out = std::process::Command::new("git")
            .current_dir(&reftable_dir)
            .args(["rev-parse", "HEAD"])
            .output()
            .unwrap();
        assert!(cid_out.status.success());
        let cid_hex = String::from_utf8(cid_out.stdout)
            .unwrap()
            .trim()
            .to_string();
        let detach_status = std::process::Command::new("git")
            .current_dir(&reftable_dir)
            .args(["checkout", "--detach", "HEAD"])
            .status()
            .unwrap();
        assert!(detach_status.success());
        let del_branch_status = std::process::Command::new("git")
            .current_dir(&reftable_dir)
            .args(["branch", "-D", "main"])
            .status()
            .unwrap();
        assert!(del_branch_status.success());

        // Remove the loose commit object so detached HEAD is now dangling.
        let loose_obj = dot_git
            .join("objects")
            .join(&cid_hex[..2])
            .join(&cid_hex[2..]);
        fs::remove_file(&loose_obj).unwrap();

        let err = discover_git_refs(&ts_repo, &dot_git, &reftable_dir, true)
            .await
            .expect_err("dangling detached HEAD in reftable repo must fail");
        assert!(
            err.to_string().contains("HEAD"),
            "unexpected error for dangling reftable HEAD: {err}"
        );
    }
}

#[tokio::test]
async fn reftable_for_each_ref_parser_rejects_dangling_importable_refs() {
    let fake_git_dir = Path::new("/tmp/fake.git");
    let oid_hex = "1111111111111111111111111111111111111111";
    let expected_oid = gix::hash::ObjectId::from_hex(oid_hex.as_bytes()).unwrap();

    // Valid branch + dangling non-importable refs (`refs/remotes/origin/HEAD`, `refs/notes/*`)
    // must succeed and ignore non-importable refs.
    let valid_out = format!(
        "refs/heads/main\0{oid_hex}\r\nrefs/remotes/origin/HEAD\0\nrefs/notes/dangling\0\n"
    );
    let parsed = parse_reftable_for_each_ref_output(valid_out.as_bytes(), fake_git_dir).unwrap();
    assert_eq!(parsed, vec![("refs/heads/main".to_string(), expected_oid)]);

    // Dangling symbolic branch ref (`refs/heads/broken\0\n`) must fail loudly rather than
    // being silently skipped.
    let err = parse_reftable_for_each_ref_output(b"refs/heads/broken\0\n", fake_git_dir)
        .expect_err("dangling refs/heads/* must fail");
    assert!(
        err.to_string().contains("refs/heads/broken") && err.to_string().contains("dangling"),
        "unexpected error: {err}"
    );

    // Malformed line missing NUL delimiter must fail.
    assert!(parse_reftable_for_each_ref_output(b"refs/heads/no_nul\n", fake_git_dir).is_err());

    // Non-UTF-8 importable ref name must fail, while non-UTF-8 non-importable is ignored.
    assert!(
        parse_reftable_for_each_ref_output(
            format!("refs/notes/bad\x00{oid_hex}\n").as_bytes(),
            fake_git_dir
        )
        .unwrap()
        .is_empty()
    );
    let mut non_utf8_branch = b"refs/heads/bad\xff\0".to_vec();
    non_utf8_branch.extend_from_slice(oid_hex.as_bytes());
    non_utf8_branch.push(b'\n');
    assert!(parse_reftable_for_each_ref_output(&non_utf8_branch, fake_git_dir).is_err());

    // Real reftable repo: symbolic ref pointing to a valid branch works, dangling
    // `refs/remotes/origin/HEAD` is ignored, and a branch whose commit object is missing
    // fails loudly.
    let dir = tempfile::tempdir().unwrap();
    let rt_dir = dir.path().join("reftable_symref");
    fs::create_dir_all(&rt_dir).unwrap();
    let init_out = std::process::Command::new("git")
        .current_dir(&rt_dir)
        .args(["init", "-b", "main", "--ref-format=reftable"])
        .output()
        .unwrap();
    if init_out.status.success() {
        fs::write(rt_dir.join("a.txt"), "hello\n").unwrap();
        assert!(
            std::process::Command::new("git")
                .current_dir(&rt_dir)
                .args(["add", "a.txt"])
                .status()
                .unwrap()
                .success()
        );
        assert!(
            std::process::Command::new("git")
                .current_dir(&rt_dir)
                .args([
                    "-c",
                    "user.name=Test",
                    "-c",
                    "user.email=test@example.com",
                    "commit",
                    "-m",
                    "init",
                ])
                .status()
                .unwrap()
                .success()
        );
        assert!(
            std::process::Command::new("git")
                .current_dir(&rt_dir)
                .args([
                    "symbolic-ref",
                    "refs/remotes/origin/HEAD",
                    "refs/remotes/origin/nonexistent",
                ])
                .status()
                .unwrap()
                .success()
        );
        assert!(
            std::process::Command::new("git")
                .current_dir(&rt_dir)
                .args(["symbolic-ref", "refs/heads/alias", "refs/heads/main"])
                .status()
                .unwrap()
                .success()
        );
        let dot_git = rt_dir.join(".git");
        let guard = ReftableObjectsDir::create_if_reftable(&dot_git)
            .unwrap()
            .expect("reftable helper");
        let ts_repo =
            gix::ThreadSafeRepository::open_opts(&guard.path, gix::open::Options::isolated())
                .unwrap();
        let (heads, local, _) = discover_git_refs(&ts_repo, &dot_git, &rt_dir, true)
            .await
            .expect("valid reftable symref and ignored dangling remote HEAD must succeed");
        assert_eq!(heads.len(), 1);
        assert!(local.contains_key("main"));
        assert!(local.contains_key("alias"));
    }
}

#[test]
fn target_git_repo_rollback_preserves_concurrent_siblings_in_created_ancestors() {
    let dir = tempfile::tempdir().unwrap();

    // Non-bare target under multi-level newly created ancestors: concurrent siblings under
    // newly created ancestors survive rollback while the target dir and empty newly created
    // intermediate ancestors are removed.
    let non_bare_root = dir.path().join("concurrent_non_bare");
    let non_bare_mid = non_bare_root.join("mid");
    let non_bare_empty_parent = non_bare_mid.join("empty_parent");
    let non_bare_dst = non_bare_empty_parent.join("repo");
    let sibling_at_root = non_bare_root.join("root_sibling.txt");
    let sibling_dir = non_bare_mid.join("other_repo");
    let sibling_file = sibling_dir.join("keep.txt");
    {
        let target = open_or_init_git_repo(&non_bare_dst, false).unwrap();
        assert!(target.newly_created);
        assert!(non_bare_dst.join(".git").is_dir());
        fs::write(&sibling_at_root, "keep root\n").unwrap();
        fs::create_dir_all(&sibling_dir).unwrap();
        fs::write(&sibling_file, "keep mid\n").unwrap();
    }
    assert!(!non_bare_dst.exists(), "failed target repo must be removed");
    assert!(
        !non_bare_empty_parent.exists(),
        "empty newly created intermediate ancestor must be pruned"
    );
    assert!(
        non_bare_mid.is_dir(),
        "non-empty ancestor holding sibling dir must survive"
    );
    assert_eq!(fs::read_to_string(&sibling_file).unwrap(), "keep mid\n");
    assert_eq!(fs::read_to_string(&sibling_at_root).unwrap(), "keep root\n");

    // Bare target under newly created ancestors: empty intermediate ancestor is removed while
    // sibling under top created ancestor survives.
    let bare_root = dir.path().join("concurrent_bare");
    let bare_empty_mid = bare_root.join("empty_mid");
    let bare_dst = bare_empty_mid.join("target.git");
    let bare_sibling = bare_root.join("peer.txt");
    {
        let target = open_or_init_git_repo(&bare_dst, true).unwrap();
        assert!(target.newly_created);
        assert!(target.is_bare);
        assert!(bare_dst.join("HEAD").is_file());
        fs::write(&bare_sibling, "keep bare sibling\n").unwrap();
    }
    assert!(!bare_dst.exists(), "failed bare repo must be removed");
    assert!(
        !bare_empty_mid.exists(),
        "empty newly created intermediate dir must be pruned"
    );
    assert!(
        bare_root.is_dir(),
        "newly created root ancestor with sibling must survive"
    );
    assert_eq!(
        fs::read_to_string(&bare_sibling).unwrap(),
        "keep bare sibling\n"
    );
}

#[tokio::test]
async fn ensure_no_checked_out_branch_conflict_protects_linked_and_primary_worktrees() {
    use std::collections::BTreeMap;

    let dir = tempfile::tempdir().unwrap();
    let run_git = |cwd: &Path, args: &[&str]| {
        let out = std::process::Command::new("git")
            .current_dir(cwd)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env(
                "GIT_CONFIG_GLOBAL",
                if cfg!(windows) { "NUL" } else { "/dev/null" },
            )
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .args(args)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    };

    // Primary non-bare repo on `main` with two linked worktrees on `feature` and `other`.
    let main_wt = dir.path().join("main_wt");
    fs::create_dir_all(&main_wt).unwrap();
    run_git(&main_wt, &["init", "-b", "main"]);
    fs::write(main_wt.join("file.txt"), "hello\n").unwrap();
    run_git(&main_wt, &["add", "file.txt"]);
    run_git(
        &main_wt,
        &[
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "-m",
            "init",
        ],
    );
    run_git(&main_wt, &["branch", "feature"]);
    run_git(&main_wt, &["branch", "unrelated"]);
    let linked_feature = dir.path().join("linked_feature");
    let linked_other = dir.path().join("linked_other");
    run_git(
        &main_wt,
        &[
            "worktree",
            "add",
            linked_feature.to_str().unwrap(),
            "feature",
        ],
    );
    run_git(
        &main_wt,
        &[
            "worktree",
            "add",
            "-b",
            "other",
            linked_other.to_str().unwrap(),
        ],
    );

    let dummy_oid = vec![0xaa; COMMIT_ID_LENGTH];
    let branch_map = |name: &str| BTreeMap::from([(name.to_string(), dummy_oid.clone())]);

    // Opened via primary worktree: refuses `main`, `feature`, and `other`; allows `unrelated`.
    let main_target = open_or_init_git_repo(&main_wt, false).unwrap();
    assert!(!main_target.is_bare);
    let main_repo = main_target.repo.to_thread_local();
    for protected in ["main", "feature", "other"] {
        let err = ensure_no_checked_out_branch_conflict(
            &main_repo,
            &main_target.git_dir,
            &branch_map(protected),
        )
        .expect_err("checked-out branch must be refused");
        assert!(
            err.to_string()
                .contains(&format!("checked-out branch {protected:?}")),
            "protected {protected}: unexpected error: {err}"
        );
    }
    ensure_no_checked_out_branch_conflict(
        &main_repo,
        &main_target.git_dir,
        &branch_map("unrelated"),
    )
    .expect("unchecked-out branch must be allowed");

    // Opened via linked worktree `linked_feature`: refuses primary (`main`), self (`feature`),
    // and sibling (`other`); allows `unrelated`.
    let linked_target = open_or_init_git_repo(&linked_feature, false).unwrap();
    assert!(!linked_target.is_bare);
    let linked_repo = linked_target.repo.to_thread_local();
    for protected in ["main", "feature", "other"] {
        let err = ensure_no_checked_out_branch_conflict(
            &linked_repo,
            &linked_target.git_dir,
            &branch_map(protected),
        )
        .expect_err("export via linked worktree must protect primary and sibling worktrees");
        assert!(
            err.to_string()
                .contains(&format!("checked-out branch {protected:?}")),
            "via linked wt, protected {protected}: unexpected error: {err}"
        );
    }
    ensure_no_checked_out_branch_conflict(
        &linked_repo,
        &linked_target.git_dir,
        &branch_map("unrelated"),
    )
    .expect("unchecked-out branch must be allowed via linked worktree");

    // Bare repository with a linked worktree on `wt-branch`: bare `HEAD` (`main`) is allowed,
    // while `wt-branch` checked out in the linked worktree is refused (both via direct check
    // and end-to-end `GitExporter::run` before any `ReadTree` RPC).
    let bare_repo_dir = dir.path().join("bare_with_wt.git");
    run_git(
        dir.path(),
        &[
            "clone",
            "--bare",
            main_wt.to_str().unwrap(),
            bare_repo_dir.to_str().unwrap(),
        ],
    );
    let bare_linked_wt = dir.path().join("bare_linked_wt");
    run_git(
        &bare_repo_dir,
        &[
            "worktree",
            "add",
            "-b",
            "wt-branch",
            bare_linked_wt.to_str().unwrap(),
            "main",
        ],
    );

    let bare_target = open_or_init_git_repo(&bare_repo_dir, true).unwrap();
    assert!(bare_target.is_bare);
    let bare_gix = bare_target.repo.to_thread_local();
    ensure_no_checked_out_branch_conflict(&bare_gix, &bare_target.git_dir, &branch_map("main"))
        .expect("bare repo HEAD without a main worktree must not block updating main");
    let bare_err = ensure_no_checked_out_branch_conflict(
        &bare_gix,
        &bare_target.git_dir,
        &branch_map("wt-branch"),
    )
    .expect_err("bare repo must still refuse branch checked out in a linked worktree");
    assert!(
        bare_err
            .to_string()
            .contains("checked-out branch \"wt-branch\""),
        "unexpected error: {bare_err}"
    );

    // Opening the bare repo's linked worktree directly must treat it as non-bare (`!is_bare`)
    // and still allow `main` (bare HEAD) while refusing `wt-branch`.
    let bare_wt_target = open_or_init_git_repo(&bare_linked_wt, false).unwrap();
    assert!(
        !bare_wt_target.is_bare,
        "linked worktree of a bare repo must not be marked bare"
    );
    let bare_wt_gix = bare_wt_target.repo.to_thread_local();
    ensure_no_checked_out_branch_conflict(
        &bare_wt_gix,
        &bare_wt_target.git_dir,
        &branch_map("main"),
    )
    .expect("main is only bare HEAD, not checked out in a worktree");
    assert!(
        ensure_no_checked_out_branch_conflict(
            &bare_wt_gix,
            &bare_wt_target.git_dir,
            &branch_map("wt-branch"),
        )
        .is_err()
    );

    // End-to-end `GitExporter::run` on `bare_repo_dir` targeting `wt-branch` fails before any
    // `ReadTree` RPC.
    let fake_tree = vec![0xee; cc_common::TREE_ID_LENGTH];
    let mut exp_commit = Commit {
        commit_id: Vec::new(),
        change_id: vec![0x51; CHANGE_ID_LENGTH],
        parent_commit_ids: vec![ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![fake_tree],
        description: "linked worktree guard\n".to_string(),
        ..Default::default()
    };
    let raw = serialize_proto_commit_to_git(&exp_commit, true).unwrap();
    exp_commit.commit_id =
        gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Commit, &raw)
            .unwrap()
            .as_bytes()
            .to_vec();
    let read_tree_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let backend = ValidationMockBackendService {
        commits: HashMap::from([(exp_commit.commit_id.clone(), exp_commit.clone())]),
        trees: HashMap::new(),
        files: HashMap::new(),
        chunked_files: HashMap::new(),
        symlinks: HashMap::new(),
        read_commit_calls: Arc::new(std::sync::Mutex::new(HashMap::new())),
        read_tree_calls: Arc::clone(&read_tree_calls),
        read_file_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    };
    let op_store = MockOpStoreService {
        initial_view: cc_common::op_store::View {
            head_ids: vec![exp_commit.commit_id.clone()],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::from([(
                "wt-branch".to_string(),
                RefTarget {
                    removes: Vec::new(),
                    adds: vec![RefTargetTerm {
                        commit_id: exp_commit.commit_id,
                    }],
                },
            )]),
            remote_bookmarks: HashMap::new(),
        },
        published_view: Arc::new(std::sync::Mutex::new(None)),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let incoming = futures::stream::unfold(listener, |listener| async move {
        Some((listener.accept().await.map(|(stream, _)| stream), listener))
    });
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(OpStoreServiceServer::new(op_store))
            .add_service(
                cc_common::backend::backend_service_server::BackendServiceServer::new(backend),
            )
            .serve_with_incoming(incoming),
    );

    let err = GitExporter::new(GitExportOptions {
        git_dir: bare_repo_dir.clone(),
        server_url: url,
        repo_id: "repo".to_string(),
        project_id: None,
        bare: true,
    })
    .run()
    .await
    .expect_err("export to bare repo with linked worktree on wt-branch must fail");
    assert!(
        err.to_string().contains("checked-out branch \"wt-branch\""),
        "unexpected error: {err}"
    );
    assert_eq!(read_tree_calls.load(Ordering::SeqCst), 0);

    // `--separate-git-dir` non-bare repository opened through a linked worktree must still
    // detect that it is a linked worktree and refuse the primary worktree's checked-out branch.
    let sep_git_dir = dir.path().join("sep_git_store");
    let sep_main_wt = dir.path().join("sep_main_wt");
    fs::create_dir_all(&sep_main_wt).unwrap();
    run_git(
        &sep_main_wt,
        &[
            "init",
            "-b",
            "primary-branch",
            "--separate-git-dir",
            sep_git_dir.to_str().unwrap(),
        ],
    );
    fs::write(sep_main_wt.join("sep.txt"), "sep\n").unwrap();
    run_git(&sep_main_wt, &["add", "sep.txt"]);
    run_git(
        &sep_main_wt,
        &[
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "-m",
            "sep init",
        ],
    );
    let sep_linked_wt = dir.path().join("sep_linked_wt");
    run_git(
        &sep_main_wt,
        &[
            "worktree",
            "add",
            "-b",
            "sep-linked-branch",
            sep_linked_wt.to_str().unwrap(),
        ],
    );
    let sep_linked_target = open_or_init_git_repo(&sep_linked_wt, false).unwrap();
    assert!(!sep_linked_target.is_bare);
    let sep_linked_gix = sep_linked_target.repo.to_thread_local();
    let sep_primary_err = ensure_no_checked_out_branch_conflict(
        &sep_linked_gix,
        &sep_linked_target.git_dir,
        &branch_map("primary-branch"),
    )
    .expect_err("linked worktree of --separate-git-dir repo must protect primary worktree branch");
    assert!(
        sep_primary_err
            .to_string()
            .contains("checked-out branch \"primary-branch\""),
        "unexpected error: {sep_primary_err}"
    );
    let sep_self_err = ensure_no_checked_out_branch_conflict(
        &sep_linked_gix,
        &sep_linked_target.git_dir,
        &branch_map("sep-linked-branch"),
    )
    .expect_err("linked worktree of --separate-git-dir repo must protect its own branch");
    assert!(
        sep_self_err
            .to_string()
            .contains("checked-out branch \"sep-linked-branch\""),
        "unexpected error: {sep_self_err}"
    );
    ensure_no_checked_out_branch_conflict(
        &sep_linked_gix,
        &sep_linked_target.git_dir,
        &branch_map("unrelated"),
    )
    .expect("unchecked-out branch on --separate-git-dir linked worktree must be allowed");

    // Bare repo's linked worktree with an absolute `commondir`, opened directly via
    // `bare.git/worktrees/<name>`, must report `!is_bare` and refuse its checked-out branch.
    let bare_wt_git_dir = bare_repo_dir.join("worktrees/bare_linked_wt");
    fs::write(
        bare_wt_git_dir.join("commondir"),
        format!("{}\n", fs::canonicalize(&bare_repo_dir).unwrap().display()),
    )
    .unwrap();
    let direct_bare_wt_target = open_or_init_git_repo(&bare_wt_git_dir, false).unwrap();
    assert!(
        !direct_bare_wt_target.is_bare,
        "opening bare.git/worktrees/<name> directly must not classify it as bare"
    );
    let direct_bare_wt_gix = direct_bare_wt_target.repo.to_thread_local();
    let direct_wt_err = ensure_no_checked_out_branch_conflict(
        &direct_bare_wt_gix,
        &direct_bare_wt_target.git_dir,
        &branch_map("wt-branch"),
    )
    .expect_err(
        "direct open of bare.git/worktrees/<name> with absolute commondir must refuse its branch",
    );
    assert!(
        direct_wt_err
            .to_string()
            .contains("checked-out branch \"wt-branch\""),
        "unexpected error: {direct_wt_err}"
    );
    ensure_no_checked_out_branch_conflict(
        &direct_bare_wt_gix,
        &direct_bare_wt_target.git_dir,
        &branch_map("main"),
    )
    .expect("bare HEAD without a main worktree must still be allowed");

    // Corrupt linked worktree `HEAD` fails closed.
    let wt_head_file = bare_wt_git_dir.join("HEAD");
    fs::write(&wt_head_file, "corrupt-head-content\n").unwrap();
    let corrupt_err = ensure_no_checked_out_branch_conflict(
        &bare_gix,
        &bare_target.git_dir,
        &branch_map("unrelated"),
    )
    .expect_err("corrupt linked worktree HEAD must fail closed");
    assert!(
        corrupt_err.to_string().contains("Failed to inspect HEAD"),
        "unexpected error for corrupt linked HEAD: {corrupt_err}"
    );
}
