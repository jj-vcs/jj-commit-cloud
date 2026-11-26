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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use cc_common::backend::{Commit, Signature, Timestamp};
use cc_common::op_store::op_store_service_server::OpStoreServiceServer;
use cc_common::op_store::{RefTarget, RefTargetTerm};
use cc_common::{CHANGE_ID_LENGTH, COMMIT_ID_LENGTH, EMPTY_TREE_ID_BYTES, ROOT_COMMIT_ID_BYTES};
use gix::objs::WriteTo as _;
use gix::prelude::Write as _;
use jj_lib::backend::CommitId;
use jj_lib::hex_util::encode_reverse_hex;
use jj_lib::object_id::ObjectId as _;

use super::*;
use crate::util::{connect_backend_client, connect_op_store_client};

#[tokio::test]
async fn importer_and_exporter_reject_blank_repo_and_project_ids() {
    let dir = tempfile::tempdir().unwrap();
    for (repo_id, project_id) in [
        (Some("".to_string()), None),
        (Some("   ".to_string()), None),
        (Some("repo-1".to_string()), Some("".to_string())),
        (None, Some(" \t ".to_string())),
    ] {
        let err = GitImporter::new(GitImportOptions {
            git_dir: dir.path().to_path_buf(),
            server_url: "http://127.0.0.1:1".to_string(),
            project_id,
            repo_id,
            create_repo: true,
        })
        .run()
        .await
        .expect_err("blank repo_id/project_id must fail");
        assert!(
            err.to_string().contains("must not be empty or whitespace"),
            "unexpected error: {err}"
        );
    }

    for (repo_id, project_id) in [
        ("".to_string(), None),
        ("   ".to_string(), None),
        ("repo-1".to_string(), Some("".to_string())),
        ("repo-1".to_string(), Some("  ".to_string())),
    ] {
        let err = GitExporter::new(GitExportOptions {
            git_dir: dir.path().to_path_buf(),
            server_url: "http://127.0.0.1:1".to_string(),
            repo_id,
            project_id,
            bare: true,
        })
        .run()
        .await
        .expect_err("blank repo_id/project_id must fail");
        assert!(
            err.to_string().contains("must not be empty or whitespace"),
            "unexpected error: {err}"
        );
    }
}

#[test]
fn decode_change_id_header_uses_jj_reverse_hex() {
    let expected = vec![
        0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32,
        0x10,
    ];
    let rev = encode_reverse_hex(&expected);
    assert_eq!(
        decode_change_id_header(rev.as_bytes()),
        Some(expected.clone())
    );
    assert_eq!(
        decode_change_id_header(rev.to_ascii_uppercase().as_bytes()),
        Some(expected)
    );
    // Forward hex and wrong-length strings must not decode as jj change-ids.
    assert_eq!(
        decode_change_id_header(b"0123456789abcdeffedcba9876543210"),
        None
    );
    assert_eq!(decode_change_id_header(b"zzzz"), None);
}

#[tokio::test]
async fn git_importer_rejects_non_git_subdir_and_sha256_repo() {
    let dir = tempfile::tempdir().unwrap();

    // Subdirectory inside a valid Git repo must NOT silently discover and import the parent.
    let parent_repo = dir.path().join("parent");
    fs::create_dir_all(parent_repo.join("nested")).unwrap();
    let status = std::process::Command::new("git")
        .current_dir(&parent_repo)
        .args(["init", "-b", "main"])
        .status()
        .unwrap();
    assert!(status.success());

    let subdir_err = GitImporter::new(GitImportOptions {
        git_dir: parent_repo.join("nested"),
        server_url: "http://127.0.0.1:1".to_string(),
        project_id: None,
        repo_id: None,
        create_repo: true,
    })
    .run()
    .await
    .expect_err("non-git subdir inside a parent git repo must fail without upward discover");
    assert!(
        subdir_err
            .to_string()
            .contains("Failed to open Git repository"),
        "unexpected error: {subdir_err}"
    );

    // SHA-256 Git repository must be rejected upfront before contacting any server.
    let sha256_repo = dir.path().join("sha256");
    fs::create_dir_all(&sha256_repo).unwrap();
    let out = std::process::Command::new("git")
        .current_dir(&sha256_repo)
        .args(["init", "-b", "main", "--object-format=sha256"])
        .output()
        .unwrap();
    if out.status.success() {
        let err = GitImporter::new(GitImportOptions {
            git_dir: sha256_repo.clone(),
            server_url: "http://127.0.0.1:1".to_string(),
            project_id: None,
            repo_id: None,
            create_repo: true,
        })
        .run()
        .await
        .expect_err("SHA-256 Git repo must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("SHA-1") || msg.contains("Failed to open Git repository"),
            "unexpected error for SHA-256 repo: {msg}"
        );
        assert!(
            open_or_init_git_repo(&sha256_repo, false).is_err(),
            "exporting into an existing SHA-256 repo must fail"
        );
    }
}

#[tokio::test]
async fn publish_imported_refs_fails_without_publishing_when_op_or_view_read_fails() {
    for (fail_read_op, expected_msg) in [
        (true, "injected ReadOperation failure"),
        (false, "injected ReadView failure"),
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let op_store = FailingReadOpStore {
            fail_read_op,
            ..Default::default()
        };
        let published = op_store.published.clone();
        let incoming = futures::stream::unfold(listener, |listener| async move {
            Some((listener.accept().await.map(|(stream, _)| stream), listener))
        });
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(OpStoreServiceServer::new(op_store))
                .serve_with_incoming(incoming),
        );

        let result = run_on_tokio(async move {
            let mut op_client = connect_op_store_client(url).await?;
            let heads = vec![vec![3; COMMIT_ID_LENGTH]];
            let no_ancestors = HashSet::new();
            let no_parents = HashMap::new();
            let no_branches = BTreeMap::new();
            publish_imported_refs(
                &mut op_client,
                None,
                "repo",
                &heads,
                &no_ancestors,
                &no_parents,
                &no_branches,
                &no_branches,
            )
            .await
        })
        .await;

        let err = result.expect_err("a failed read must abort the import");
        assert!(
            err.to_string().contains(expected_msg),
            "unexpected error: {err}"
        );
        assert!(
            !published.load(Ordering::SeqCst),
            "nothing may be published after a failed read"
        );
    }
}

#[test]
fn parse_jj_trees_header_enforces_strict_sha1_odd_tree_list() {
    let t1 = "1111111111111111111111111111111111111111";
    let t2 = "2222222222222222222222222222222222222222";
    let t3 = "3333333333333333333333333333333333333333";
    let t4 = "4444444444444444444444444444444444444444";
    let t5 = "5555555555555555555555555555555555555555";
    let sha256_1 = "1111111111111111111111111111111111111111111111111111111111111111";
    let sha256_2 = "2222222222222222222222222222222222222222222222222222222222222222";
    let sha256_3 = "3333333333333333333333333333333333333333333333333333333333333333";

    let make_commit_bytes = |extra_headers: Vec<(&str, &str)>| -> Vec<u8> {
        let mut buf = format!(
            "tree {t1}\n\
             author Test <test@example.com> 1730000000 +0000\n\
             committer Test <test@example.com> 1730000000 +0000\n"
        );
        for (k, v) in extra_headers {
            buf.push_str(&format!("{k} {v}\n"));
        }
        buf.push_str("\nmsg\n");
        buf.into_bytes()
    };

    // Absent header -> Ok(None).
    let no_hdr_bytes = make_commit_bytes(vec![]);
    let no_hdr = gix::objs::CommitRef::from_bytes(&no_hdr_bytes, gix::hash::Kind::Sha1).unwrap();
    assert_eq!(parse_jj_trees_header(&no_hdr).unwrap(), None);

    // Valid 3-tree SHA-1 header -> Ok(Some([t1, t2, t3])).
    let three_val = format!("{t1} {t2} {t3}");
    let valid3_bytes = make_commit_bytes(vec![("jj:trees", &three_val)]);
    let valid3 = gix::objs::CommitRef::from_bytes(&valid3_bytes, gix::hash::Kind::Sha1).unwrap();
    assert_eq!(
        parse_jj_trees_header(&valid3).unwrap(),
        Some(vec![
            gix::hash::ObjectId::from_hex(t1.as_bytes()).unwrap(),
            gix::hash::ObjectId::from_hex(t2.as_bytes()).unwrap(),
            gix::hash::ObjectId::from_hex(t3.as_bytes()).unwrap(),
        ])
    );

    // Valid 5-tree SHA-1 header -> Ok(Some([t1..t5])).
    let five_val = format!("{t1} {t2} {t3} {t4} {t5}");
    let valid5_bytes = make_commit_bytes(vec![("jj:trees", &five_val)]);
    let valid5 = gix::objs::CommitRef::from_bytes(&valid5_bytes, gix::hash::Kind::Sha1).unwrap();
    assert_eq!(parse_jj_trees_header(&valid5).unwrap().unwrap().len(), 5);

    // Only the first `jj:trees` header is parsed (ignoring any trailing duplicate).
    let dup_valid_first =
        make_commit_bytes(vec![("jj:trees", &three_val), ("jj:trees", "garbage")]);
    let dup_ref =
        gix::objs::CommitRef::from_bytes(&dup_valid_first, gix::hash::Kind::Sha1).unwrap();
    assert_eq!(parse_jj_trees_header(&dup_ref).unwrap().unwrap().len(), 3);

    // Invalid headers must all return an error.
    let cases = [
        ("empty", "".to_string()),
        ("malformed hex", format!("{t1} not-hex {t3}")),
        ("1-tree", t1.to_string()),
        ("2-tree (even)", format!("{t1} {t2}")),
        ("4-tree (even)", format!("{t1} {t2} {t3} {t4}")),
        (
            "SHA-256 64-char hex",
            format!("{sha256_1} {sha256_2} {sha256_3}"),
        ),
        ("tab separator", format!("{t1}\t{t2}\t{t3}")),
        ("double space", format!("{t1}  {t2} {t3}")),
        ("leading space", format!(" {t1} {t2} {t3}")),
        ("trailing space", format!("{t1} {t2} {t3} ")),
    ];
    for (label, value) in cases {
        let bytes = make_commit_bytes(vec![("jj:trees", &value)]);
        let commit_ref = gix::objs::CommitRef::from_bytes(&bytes, gix::hash::Kind::Sha1).unwrap();
        assert!(
            parse_jj_trees_header(&commit_ref).is_err(),
            "jj:trees case {label:?} ({value:?}) must be rejected"
        );
    }
}

#[tokio::test]
async fn import_tree_recursive_rejects_cyclic_git_trees() {
    let dir = tempfile::tempdir().unwrap();
    let repo = gix::init_bare(dir.path()).unwrap();

    let self_oid =
        gix::hash::ObjectId::from_hex(b"cccccccccccccccccccccccccccccccccccccccc").unwrap();
    let a_oid = gix::hash::ObjectId::from_hex(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
    let b_oid = gix::hash::ObjectId::from_hex(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();

    write_crafted_loose_tree(dir.path(), self_oid, "loop", self_oid);
    write_crafted_loose_tree(dir.path(), a_oid, "b", b_oid);
    write_crafted_loose_tree(dir.path(), b_oid, "a", a_oid);

    let backend = MockBackendService {
        commits: HashMap::new(),
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
            .add_service(
                cc_common::backend::backend_service_server::BackendServiceServer::new(backend),
            )
            .serve_with_incoming(incoming),
    );

    let ts_repo = repo.into_sync();
    let mut backend_client = connect_backend_client(url).await.unwrap();
    let mut tree_cache = HashSet::new();
    let mut in_progress = HashSet::new();

    let self_err = import_tree_recursive(
        &ts_repo,
        self_oid,
        &mut backend_client,
        "proj",
        &mut tree_cache,
        &mut in_progress,
    )
    .await
    .expect_err("self-cyclic Git tree must fail");
    assert!(
        self_err.to_string().contains("Cycle detected"),
        "unexpected error: {self_err}"
    );
    assert!(in_progress.is_empty());

    let two_err = import_tree_recursive(
        &ts_repo,
        a_oid,
        &mut backend_client,
        "proj",
        &mut tree_cache,
        &mut in_progress,
    )
    .await
    .expect_err("A -> B -> A Git tree cycle must fail");
    assert!(
        two_err.to_string().contains("Cycle detected"),
        "unexpected error: {two_err}"
    );
    assert!(in_progress.is_empty());

    // `collect_git_graph` must also reject commits whose root tree contains a cycle before
    // issuing any uploads.
    let repo = ts_repo.to_thread_local();
    let commit_obj = gix::objs::Commit {
        tree: a_oid,
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
        message: "cyclic tree commit\n".into(),
    };
    let mut commit_buf = Vec::new();
    commit_obj.write_to(&mut commit_buf).unwrap();
    let commit_oid = repo
        .objects
        .write_buf(gix::objs::Kind::Commit, &commit_buf)
        .unwrap();
    write_git_refs(
        &repo,
        vec![(
            "refs/heads/main".try_into().unwrap(),
            gix::refs::Target::Object(commit_oid),
        )],
    )
    .unwrap();
    let ts_repo = repo.into_sync();
    let collect_err = match collect_git_graph(&ts_repo, dir.path(), dir.path(), false).await {
        Ok(_) => panic!("collect_git_graph must reject cyclic Git trees"),
        Err(err) => err,
    };
    assert!(
        collect_err.to_string().contains("Cycle detected"),
        "unexpected error: {collect_err}"
    );
}

#[tokio::test]
async fn publish_imported_refs_merges_local_bookmarks_without_rewinding_or_erasing_conflicts() {
    let make_id = |b: u8| vec![b; COMMIT_ID_LENGTH];
    let base = make_id(0x10);
    let cc_ahead = make_id(0x11);
    let git_ff = make_id(0x12);
    let cc_div = make_id(0x13);
    let git_div = make_id(0x14);
    let conf_side_a = make_id(0x15);
    let conf_side_b = make_id(0x16);
    let conf_side_b_ff = make_id(0x17);
    let conf_side_c = make_id(0x18);
    let new_branch_tip = make_id(0x19);

    let make_commit = |id: &Vec<u8>, parents: Vec<Vec<u8>>| Commit {
        commit_id: id.clone(),
        change_id: vec![id[0]; CHANGE_ID_LENGTH],
        parent_commit_ids: parents,
        root_tree_id: vec![EMPTY_TREE_ID_BYTES.to_vec()],
        ..Default::default()
    };

    let commits = HashMap::from([
        (
            base.clone(),
            make_commit(&base, vec![ROOT_COMMIT_ID_BYTES.to_vec()]),
        ),
        (cc_ahead.clone(), make_commit(&cc_ahead, vec![base.clone()])),
        (git_ff.clone(), make_commit(&git_ff, vec![base.clone()])),
        (cc_div.clone(), make_commit(&cc_div, vec![base.clone()])),
        (git_div.clone(), make_commit(&git_div, vec![base.clone()])),
        (
            conf_side_a.clone(),
            make_commit(&conf_side_a, vec![base.clone()]),
        ),
        (
            conf_side_b.clone(),
            make_commit(&conf_side_b, vec![base.clone()]),
        ),
        (
            conf_side_b_ff.clone(),
            make_commit(&conf_side_b_ff, vec![conf_side_b.clone()]),
        ),
        (
            conf_side_c.clone(),
            make_commit(&conf_side_c, vec![base.clone()]),
        ),
        (
            new_branch_tip.clone(),
            make_commit(&new_branch_tip, vec![ROOT_COMMIT_ID_BYTES.to_vec()]),
        ),
    ]);

    let resolved = |id: &Vec<u8>| RefTarget {
        removes: Vec::new(),
        adds: vec![RefTargetTerm {
            commit_id: id.clone(),
        }],
    };

    let initial_view = cc_common::op_store::View {
        head_ids: vec![
            cc_ahead.clone(),
            base.clone(),
            cc_div.clone(),
            conf_side_a.clone(),
            conf_side_b.clone(),
        ],
        wc_commit_ids: HashMap::new(),
        local_bookmarks: HashMap::from([
            // Commit Cloud is ahead of Git (`base -> cc_ahead`), Git imports `base`.
            ("cc-ahead".to_string(), resolved(&cc_ahead)),
            // Git fast-forwards ahead of Commit Cloud (`base -> git_ff`).
            ("git-ff".to_string(), resolved(&base)),
            // Commit Cloud (`cc_div`) and Git (`git_div`) diverged from `base`.
            ("diverged".to_string(), resolved(&cc_div)),
            // Pre-existing conflict where Git re-imports an ancestor (`base`).
            (
                "conflicted-behind".to_string(),
                RefTarget {
                    removes: vec![RefTargetTerm {
                        commit_id: base.clone(),
                    }],
                    adds: vec![
                        RefTargetTerm {
                            commit_id: conf_side_a.clone(),
                        },
                        RefTargetTerm {
                            commit_id: conf_side_b.clone(),
                        },
                    ],
                },
            ),
            // Pre-existing conflict where Git fast-forwards one side (`conf_side_b -> conf_side_b_ff`).
            (
                "conflicted-ff-side".to_string(),
                RefTarget {
                    removes: vec![RefTargetTerm {
                        commit_id: base.clone(),
                    }],
                    adds: vec![
                        RefTargetTerm {
                            commit_id: conf_side_a.clone(),
                        },
                        RefTargetTerm {
                            commit_id: conf_side_b.clone(),
                        },
                    ],
                },
            ),
            // Pre-existing conflict where Git imports an unrelated 3rd side (`conf_side_c`).
            (
                "conflicted-third-side".to_string(),
                RefTarget {
                    removes: Vec::new(),
                    adds: vec![
                        RefTargetTerm {
                            commit_id: conf_side_a.clone(),
                        },
                        RefTargetTerm {
                            commit_id: conf_side_b.clone(),
                        },
                    ],
                },
            ),
            // Pre-existing modify/delete conflict (`removes: [base], adds: [conf_side_b]`).
            (
                "conflicted-mod-del".to_string(),
                RefTarget {
                    removes: vec![RefTargetTerm {
                        commit_id: base.clone(),
                    }],
                    adds: vec![RefTargetTerm {
                        commit_id: conf_side_b.clone(),
                    }],
                },
            ),
        ]),
        remote_bookmarks: HashMap::new(),
    };

    let published_view = Arc::new(std::sync::Mutex::new(None));
    let op_store = MockOpStoreService {
        initial_view,
        published_view: Arc::clone(&published_view),
    };
    let backend = MockBackendService {
        commits,
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

    let local_branches = BTreeMap::from([
        ("brand-new".to_string(), new_branch_tip.clone()),
        ("cc-ahead".to_string(), base.clone()),
        ("git-ff".to_string(), git_ff.clone()),
        ("diverged".to_string(), git_div.clone()),
        ("conflicted-behind".to_string(), base.clone()),
        ("conflicted-ff-side".to_string(), conf_side_b_ff.clone()),
        ("conflicted-third-side".to_string(), conf_side_c.clone()),
        ("conflicted-mod-del".to_string(), conf_side_b_ff.clone()),
    ]);
    let heads = vec![
        git_ff.clone(),
        git_div.clone(),
        conf_side_b_ff.clone(),
        conf_side_c.clone(),
        new_branch_tip.clone(),
    ];
    let imported_ancestors = HashSet::from([base.clone(), conf_side_b.clone()]);
    let imported_parents = HashMap::from([
        (base.clone(), vec![ROOT_COMMIT_ID_BYTES.to_vec()]),
        (git_ff.clone(), vec![base.clone()]),
        (git_div.clone(), vec![base.clone()]),
        (conf_side_b.clone(), vec![base.clone()]),
        (conf_side_b_ff.clone(), vec![conf_side_b.clone()]),
        (conf_side_c.clone(), vec![base.clone()]),
        (new_branch_tip.clone(), vec![ROOT_COMMIT_ID_BYTES.to_vec()]),
    ]);

    let mut op_client = connect_op_store_client(url.clone()).await.unwrap();
    let mut backend_client = connect_backend_client(url).await.unwrap();
    let no_remotes = BTreeMap::new();
    let count = publish_imported_refs(
        &mut op_client,
        Some((&mut backend_client, "proj")),
        "repo",
        &heads,
        &imported_ancestors,
        &imported_parents,
        &local_branches,
        &no_remotes,
    )
    .await
    .expect("publish_imported_refs must succeed");
    assert_eq!(count, local_branches.len());

    let view = published_view
        .lock()
        .unwrap()
        .clone()
        .expect("view must be published");

    let jj_view = crate::cc_op_store::view_from_proto(view.clone())
        .expect("published view must be valid jj_lib::op_store::View");

    // New bookmark inserted.
    assert_eq!(view.local_bookmarks["brand-new"], resolved(&new_branch_tip));

    // Commit Cloud ahead of Git: must NOT rewind.
    assert_eq!(view.local_bookmarks["cc-ahead"], resolved(&cc_ahead));

    // Git fast-forwarded ahead of Commit Cloud: must fast-forward.
    assert_eq!(view.local_bookmarks["git-ff"], resolved(&git_ff));

    // Diverged: must record conflicted RefTarget preserving both sides.
    let diverged = &view.local_bookmarks["diverged"];
    assert!(diverged.removes.is_empty());
    assert_eq!(
        diverged
            .adds
            .iter()
            .map(|t| t.commit_id.clone())
            .collect::<Vec<_>>(),
        vec![cc_div.clone(), git_div.clone()]
    );
    let jj_div = &jj_view.local_bookmarks[jj_lib::ref_name::RefName::new("diverged")];
    assert!(!jj_div.is_resolved());
    assert_eq!(
        jj_div
            .present_adds()
            .map(|id| id.as_bytes().to_vec())
            .collect::<Vec<_>>(),
        vec![cc_div, git_div]
    );

    // Already conflicted with Git behind: conflict preserved verbatim.
    let conf_behind = &view.local_bookmarks["conflicted-behind"];
    assert_eq!(conf_behind.removes.len(), 1);
    assert_eq!(conf_behind.removes[0].commit_id, base);
    assert_eq!(
        conf_behind
            .adds
            .iter()
            .map(|t| t.commit_id.clone())
            .collect::<Vec<_>>(),
        vec![conf_side_a.clone(), conf_side_b.clone()]
    );

    // Already conflicted with Git fast-forwarding one side: stays conflicted and
    // advances only the Git side.
    let conf_ff = &view.local_bookmarks["conflicted-ff-side"];
    assert_eq!(conf_ff.removes.len(), 1);
    assert_eq!(conf_ff.removes[0].commit_id, base);
    assert_eq!(
        conf_ff
            .adds
            .iter()
            .map(|t| t.commit_id.clone())
            .collect::<Vec<_>>(),
        vec![conf_side_a.clone(), conf_side_b_ff.clone()]
    );

    // Already conflicted with unrelated 3rd Git commit: preserves conflict + adds 3rd side.
    let conf_third = &view.local_bookmarks["conflicted-third-side"];
    assert!(conf_third.removes.is_empty());
    assert_eq!(
        conf_third
            .adds
            .iter()
            .map(|t| t.commit_id.clone())
            .collect::<Vec<_>>(),
        vec![conf_side_a, conf_side_b, conf_side_c]
    );

    // Pre-existing modify/delete conflict: stays conflicted (removes retained).
    let conf_mod_del = &view.local_bookmarks["conflicted-mod-del"];
    assert_eq!(conf_mod_del.removes.len(), 1);
    assert_eq!(conf_mod_del.removes[0].commit_id, base);
    assert_eq!(
        conf_mod_del
            .adds
            .iter()
            .map(|t| t.commit_id.clone())
            .collect::<Vec<_>>(),
        vec![conf_side_b_ff]
    );
    assert!(
        !jj_view.local_bookmarks[jj_lib::ref_name::RefName::new("conflicted-mod-del")]
            .is_resolved()
    );
}

#[tokio::test]
async fn merge_imported_local_bookmark_preserves_arity_without_phantom_deletions() {
    let make_id = |b: u8| vec![b; COMMIT_ID_LENGTH];
    let base = make_id(0x20);
    let side_a = make_id(0x21);
    let side_b = make_id(0x22);
    let merge_ab = make_id(0x23);

    let r1 = make_id(0x30);
    let r2 = make_id(0x31);
    let a1 = make_id(0x32);
    let a2 = make_id(0x33);
    let a2_ff = make_id(0x34);
    let a3 = make_id(0x35);
    let merge_a1_a2 = make_id(0x36);

    let parents = HashMap::from([
        (base.clone(), vec![ROOT_COMMIT_ID_BYTES.to_vec()]),
        (side_a.clone(), vec![base.clone()]),
        (side_b.clone(), vec![base.clone()]),
        (merge_ab.clone(), vec![side_a.clone(), side_b.clone()]),
        (r1.clone(), vec![ROOT_COMMIT_ID_BYTES.to_vec()]),
        (r2.clone(), vec![ROOT_COMMIT_ID_BYTES.to_vec()]),
        (a1.clone(), vec![r1.clone()]),
        (a2.clone(), vec![r1.clone(), r2.clone()]),
        (a2_ff.clone(), vec![a2.clone()]),
        (a3.clone(), vec![r2.clone()]),
        (merge_a1_a2.clone(), vec![a1.clone(), a2.clone()]),
    ]);
    let mut cache = CommitParentCache::new(&parents);
    let mut no_client = None;

    let decode_legacy = |target: &RefTarget| {
        jj_lib::op_store::RefTarget::from_legacy_form(
            target
                .removes
                .iter()
                .map(|t| CommitId::from_bytes(&t.commit_id)),
            target
                .adds
                .iter()
                .map(|t| CommitId::from_bytes(&t.commit_id)),
        )
    };

    // 2-way conflict `removes: [base], adds: [side_a, side_b]` + merge commit `merge_ab`
    // superseding both `side_a` and `side_b`: must NOT shrink `adds` to len 1 (which would
    // decode as `[Some(merge_ab), Some(base), None]` and fabricate a bookmark deletion).
    let two_way = RefTarget {
        removes: vec![RefTargetTerm {
            commit_id: base.clone(),
        }],
        adds: vec![
            RefTargetTerm {
                commit_id: side_a.clone(),
            },
            RefTargetTerm {
                commit_id: side_b.clone(),
            },
        ],
    };
    let merged_two_way =
        merge_imported_local_bookmark(Some(&two_way), &merge_ab, &mut cache, &mut no_client)
            .await
            .unwrap();
    assert_eq!(merged_two_way.removes, two_way.removes);
    assert_eq!(
        merged_two_way
            .adds
            .iter()
            .map(|t| t.commit_id.clone())
            .collect::<Vec<_>>(),
        vec![side_a.clone(), side_b.clone(), merge_ab.clone()]
    );
    let jj_two_way = decode_legacy(&merged_two_way);
    assert!(!jj_two_way.is_resolved());
    assert!(
        jj_two_way.adds().all(Option::is_some),
        "2-way conflict merge must never fabricate a phantom None add term: {jj_two_way:?}"
    );
    assert_eq!(
        jj_two_way
            .present_adds()
            .map(|id| id.as_bytes().to_vec())
            .collect::<Vec<_>>(),
        vec![side_a, side_b, merge_ab.clone()]
    );
    // Re-importing the same merge commit again is idempotent.
    let merged_two_way_again =
        merge_imported_local_bookmark(Some(&merged_two_way), &merge_ab, &mut cache, &mut no_client)
            .await
            .unwrap();
    assert_eq!(merged_two_way_again, merged_two_way);

    // 3-way conflict `removes: [r1, r2], adds: [a1, a2, a3]` + merge commit `merge_a1_a2`
    // superseding `a1` and `a2` (but NOT `a3`): must NOT shrink `adds` below
    // `removes.len() + 1`.
    let three_way = RefTarget {
        removes: vec![
            RefTargetTerm {
                commit_id: r1.clone(),
            },
            RefTargetTerm {
                commit_id: r2.clone(),
            },
        ],
        adds: vec![
            RefTargetTerm {
                commit_id: a1.clone(),
            },
            RefTargetTerm {
                commit_id: a2.clone(),
            },
            RefTargetTerm {
                commit_id: a3.clone(),
            },
        ],
    };
    let merged_three_way =
        merge_imported_local_bookmark(Some(&three_way), &merge_a1_a2, &mut cache, &mut no_client)
            .await
            .unwrap();
    assert_eq!(merged_three_way.removes, three_way.removes);
    assert_eq!(
        merged_three_way
            .adds
            .iter()
            .map(|t| t.commit_id.clone())
            .collect::<Vec<_>>(),
        vec![a1.clone(), a2.clone(), a3.clone(), merge_a1_a2.clone()]
    );
    let jj_three_way = decode_legacy(&merged_three_way);
    assert!(!jj_three_way.is_resolved());
    assert!(
        jj_three_way.adds().all(Option::is_some),
        "3-way conflict merge must never fabricate a phantom None add term: {jj_three_way:?}"
    );
    assert_eq!(
        jj_three_way
            .present_adds()
            .map(|id| id.as_bytes().to_vec())
            .collect::<Vec<_>>(),
        vec![a1.clone(), a2, a3.clone(), merge_a1_a2]
    );

    // 3-way conflict + single-side fast-forward `a2_ff`: strictly one superseded side is
    // substituted in place, preserving exact `adds.len() == removes.len() + 1` arity.
    let merged_three_way_ff =
        merge_imported_local_bookmark(Some(&three_way), &a2_ff, &mut cache, &mut no_client)
            .await
            .unwrap();
    assert_eq!(merged_three_way_ff.removes, three_way.removes);
    assert_eq!(
        merged_three_way_ff
            .adds
            .iter()
            .map(|t| t.commit_id.clone())
            .collect::<Vec<_>>(),
        vec![a1.clone(), a2_ff.clone(), a3.clone()]
    );
    let jj_three_way_ff = decode_legacy(&merged_three_way_ff);
    assert!(!jj_three_way_ff.is_resolved());
    assert!(jj_three_way_ff.adds().all(Option::is_some));
    assert!(jj_three_way_ff.removes().all(Option::is_some));
}

#[tokio::test]
async fn commit_graph_dfs_rejects_cycles_and_allows_diamonds_both_directions() {
    let dir = tempfile::tempdir().unwrap();
    let empty_tree_oid = gix::hash::ObjectId::try_from(EMPTY_TREE_ID_BYTES.as_slice()).unwrap();

    // Import direction (`collect_git_graph`).
    // Self-cycle `A -> A`.
    let self_dir = dir.path().join("import_self_cycle.git");
    let self_repo = gix::init_bare(&self_dir).unwrap();
    let self_oid =
        gix::hash::ObjectId::from_hex(b"1111111111111111111111111111111111111111").unwrap();
    write_crafted_loose_commit(&self_dir, self_oid, empty_tree_oid, &[self_oid]);
    write_git_refs(
        &self_repo,
        vec![(
            "refs/heads/main".try_into().unwrap(),
            gix::refs::Target::Object(self_oid),
        )],
    )
    .unwrap();
    let err = match collect_git_graph(&self_repo.into_sync(), &self_dir, &self_dir, false).await {
        Ok(_) => panic!("self-cyclic Git commit must fail collect_git_graph"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("Cycle detected in Git commit graph"),
        "unexpected error: {err}"
    );

    // Two-commit cycle `A -> B -> A`.
    let two_dir = dir.path().join("import_two_cycle.git");
    let two_repo = gix::init_bare(&two_dir).unwrap();
    let a_oid = gix::hash::ObjectId::from_hex(b"2222222222222222222222222222222222222222").unwrap();
    let b_oid = gix::hash::ObjectId::from_hex(b"3333333333333333333333333333333333333333").unwrap();
    write_crafted_loose_commit(&two_dir, a_oid, empty_tree_oid, &[b_oid]);
    write_crafted_loose_commit(&two_dir, b_oid, empty_tree_oid, &[a_oid]);
    write_git_refs(
        &two_repo,
        vec![(
            "refs/heads/main".try_into().unwrap(),
            gix::refs::Target::Object(a_oid),
        )],
    )
    .unwrap();
    let err = match collect_git_graph(&two_repo.into_sync(), &two_dir, &two_dir, false).await {
        Ok(_) => panic!("2-cycle Git commits must fail collect_git_graph"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("Cycle detected in Git commit graph"),
        "unexpected error: {err}"
    );

    // Valid diamond DAG `D -> [B, A], B -> [A]` (same ancestor queued twice) succeeds.
    let diamond_dir = dir.path().join("import_diamond.git");
    let diamond_repo = gix::init_bare(&diamond_dir).unwrap();
    let empty_tree = diamond_repo
        .objects
        .write_buf(gix::objs::Kind::Tree, &[])
        .unwrap();
    let write_commit = |parents: Vec<gix::hash::ObjectId>, msg: &str| {
        let c = gix::objs::Commit {
            tree: empty_tree,
            parents: parents.into(),
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
            message: msg.into(),
        };
        let mut buf = Vec::new();
        c.write_to(&mut buf).unwrap();
        diamond_repo
            .objects
            .write_buf(gix::objs::Kind::Commit, &buf)
            .unwrap()
    };
    let ca = write_commit(vec![], "A\n");
    let cb = write_commit(vec![ca], "B\n");
    let cd = write_commit(vec![cb, ca], "D\n");
    write_git_refs(
        &diamond_repo,
        vec![(
            "refs/heads/main".try_into().unwrap(),
            gix::refs::Target::Object(cd),
        )],
    )
    .unwrap();
    let collected = collect_git_graph(&diamond_repo.into_sync(), &diamond_dir, &diamond_dir, false)
        .await
        .expect("diamond commit DAG must not trigger false cycle");
    assert_eq!(collected.topo_commits, vec![ca, cb, cd]);

    // Export direction (`GitExporter::run`).
    let make_export_commit = |id: &[u8], parents: Vec<Vec<u8>>, msg: &str| Commit {
        commit_id: id.to_vec(),
        change_id: vec![0x71; CHANGE_ID_LENGTH],
        parent_commit_ids: parents,
        root_tree_id: vec![EMPTY_TREE_ID_BYTES.to_vec()],
        description: msg.to_string(),
        author: Some(Signature {
            name: "Test".to_string(),
            email: "test@example.com".to_string(),
            timestamp: Some(Timestamp {
                millis_since_epoch: 1_730_000_000_000,
                tz_offset: 0,
            }),
        }),
        committer: Some(Signature {
            name: "Test".to_string(),
            email: "test@example.com".to_string(),
            timestamp: Some(Timestamp {
                millis_since_epoch: 1_730_000_000_000,
                tz_offset: 0,
            }),
        }),
        ..Default::default()
    };

    let cyc_a = vec![0xaa; COMMIT_ID_LENGTH];
    let cyc_b = vec![0xbb; COMMIT_ID_LENGTH];
    for (label, head, commits) in [
        (
            "export self-cycle",
            cyc_a.clone(),
            HashMap::from([(
                cyc_a.clone(),
                make_export_commit(&cyc_a, vec![cyc_a.clone()], "self\n"),
            )]),
        ),
        (
            "export two-cycle",
            cyc_a.clone(),
            HashMap::from([
                (
                    cyc_a.clone(),
                    make_export_commit(&cyc_a, vec![cyc_b.clone()], "a\n"),
                ),
                (
                    cyc_b.clone(),
                    make_export_commit(&cyc_b, vec![cyc_a.clone()], "b\n"),
                ),
            ]),
        ),
    ] {
        let op_store = MockOpStoreService {
            initial_view: cc_common::op_store::View {
                head_ids: vec![head.clone()],
                wc_commit_ids: HashMap::new(),
                local_bookmarks: HashMap::from([(
                    "main".to_string(),
                    RefTarget {
                        removes: Vec::new(),
                        adds: vec![RefTargetTerm { commit_id: head }],
                    },
                )]),
                remote_bookmarks: HashMap::new(),
            },
            published_view: Arc::new(std::sync::Mutex::new(None)),
        };
        let backend = MockBackendService {
            commits,
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
        let dst = dir.path().join(label.replace(' ', "_"));
        let err = GitExporter::new(GitExportOptions {
            git_dir: dst.clone(),
            server_url: url,
            repo_id: "repo".to_string(),
            project_id: None,
            bare: true,
        })
        .run()
        .await
        .expect_err("cyclic commit graph must fail export");
        assert!(
            err.to_string()
                .contains("Cycle detected in Commit Cloud commit graph"),
            "{label}: unexpected error: {err}"
        );
        assert!(!dst.exists());
    }

    // Valid export diamond DAG `D -> [B, A], B -> [A]`.
    let mut exp_a = make_export_commit(&[], vec![ROOT_COMMIT_ID_BYTES.to_vec()], "A\n");
    let exp_a_id = gix::objs::compute_hash(
        gix::hash::Kind::Sha1,
        gix::objs::Kind::Commit,
        &serialize_proto_commit_to_git(&exp_a, true).unwrap(),
    )
    .unwrap()
    .as_bytes()
    .to_vec();
    exp_a.commit_id = exp_a_id.clone();

    let mut exp_b = make_export_commit(&[], vec![exp_a_id.clone()], "B\n");
    let exp_b_id = gix::objs::compute_hash(
        gix::hash::Kind::Sha1,
        gix::objs::Kind::Commit,
        &serialize_proto_commit_to_git(&exp_b, true).unwrap(),
    )
    .unwrap()
    .as_bytes()
    .to_vec();
    exp_b.commit_id = exp_b_id.clone();

    let mut exp_d = make_export_commit(&[], vec![exp_b_id.clone(), exp_a_id.clone()], "D\n");
    let exp_d_id = gix::objs::compute_hash(
        gix::hash::Kind::Sha1,
        gix::objs::Kind::Commit,
        &serialize_proto_commit_to_git(&exp_d, true).unwrap(),
    )
    .unwrap()
    .as_bytes()
    .to_vec();
    exp_d.commit_id = exp_d_id.clone();

    let op_store = MockOpStoreService {
        initial_view: cc_common::op_store::View {
            head_ids: vec![exp_d_id.clone()],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::from([(
                "main".to_string(),
                RefTarget {
                    removes: Vec::new(),
                    adds: vec![RefTargetTerm {
                        commit_id: exp_d_id.clone(),
                    }],
                },
            )]),
            remote_bookmarks: HashMap::new(),
        },
        published_view: Arc::new(std::sync::Mutex::new(None)),
    };
    let backend = MockBackendService {
        commits: HashMap::from([(exp_a_id, exp_a), (exp_b_id, exp_b), (exp_d_id, exp_d)]),
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
    let diamond_dst = dir.path().join("export_diamond.git");
    let report = GitExporter::new(GitExportOptions {
        git_dir: diamond_dst,
        server_url: url,
        repo_id: "repo".to_string(),
        project_id: None,
        bare: true,
    })
    .run()
    .await
    .expect("diamond commit DAG must export without false cycle");
    assert_eq!(report.commits_exported, 3);
}

#[tokio::test]
async fn collect_git_graph_preflight_validates_commit_metadata_object_kinds_symlinks_and_trees() {
    let dir = tempfile::tempdir().unwrap();

    let make_bare_with_commit_bytes = |name: &str, commit_bytes: &[u8]| {
        let path = dir.path().join(name);
        let repo = gix::init_bare(&path).unwrap();
        let oid = repo
            .objects
            .write_buf(gix::objs::Kind::Commit, commit_bytes)
            .unwrap();
        write_git_refs(
            &repo,
            vec![(
                "refs/heads/main".try_into().unwrap(),
                gix::refs::Target::Object(oid),
            )],
        )
        .unwrap();
        (path, repo.into_sync(), oid)
    };

    let empty_tree_hex = hex::encode(EMPTY_TREE_ID_BYTES);

    // Missing author timestamp fails in `collect_git_graph` preflight with commit OID and
    // "Invalid Git author".
    let bad_author_bytes = format!(
        "tree {empty_tree_hex}\nauthor A U Thor <a@example.com>\ncommitter A U Thor <a@example.com> 1730000000 +0000\n\nmsg\n"
    );
    let (p1, ts1, bad_author_oid) =
        make_bare_with_commit_bytes("bad_author.git", bad_author_bytes.as_bytes());
    let err = match collect_git_graph(&ts1, &p1, &p1, false).await {
        Ok(_) => panic!("missing author timestamp must fail preflight"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(msg.contains(&bad_author_oid.to_string()), "got: {msg}");
    assert!(
        msg.to_ascii_lowercase().contains("invalid git author"),
        "got: {msg}"
    );

    // Malformed `jj:conflict-labels` headers (without `jj:trees`, wrong count, non-UTF-8,
    // all-empty) fail in `collect_git_graph` preflight.
    let t1 = hex::encode(EMPTY_TREE_ID_BYTES);
    let label_cases: Vec<(&str, Vec<u8>)> = vec![
        (
            "labels without jj:trees",
            format!(
                "tree {t1}\nauthor T <t@e> 1730000000 +0000\ncommitter T <t@e> 1730000000 +0000\njj:conflict-labels a\n\nmsg\n"
            )
            .into_bytes(),
        ),
        (
            "wrong label count",
            format!(
                "tree {t1}\nauthor T <t@e> 1730000000 +0000\ncommitter T <t@e> 1730000000 +0000\njj:trees {t1} {t1} {t1}\njj:conflict-labels a\n b\n\nmsg\n"
            )
            .into_bytes(),
        ),
        (
            "non-utf8 conflict labels",
            [
                format!(
                    "tree {t1}\nauthor T <t@e> 1730000000 +0000\ncommitter T <t@e> 1730000000 +0000\njj:trees {t1} {t1} {t1}\njj:conflict-labels "
                )
                .as_bytes(),
                b"\xff\n b\n c\n\nmsg\n",
            ]
            .concat(),
        ),
        (
            "all-empty conflict labels",
            format!(
                "tree {t1}\nauthor T <t@e> 1730000000 +0000\ncommitter T <t@e> 1730000000 +0000\njj:trees {t1} {t1} {t1}\njj:conflict-labels \n \n \n\nmsg\n"
            )
            .into_bytes(),
        ),
    ];
    for (idx, (label, bytes)) in label_cases.into_iter().enumerate() {
        let (p, ts, _) = make_bare_with_commit_bytes(&format!("bad_labels_{idx}.git"), &bytes);
        let err = match collect_git_graph(&ts, &p, &p, false).await {
            Ok(_) => panic!("{label} must fail preflight"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("jj:conflict-labels"),
            "{label}: unexpected error: {err}"
        );
    }

    // Commit pointing `tree` at a Blob object fails object-kind preflight.
    let wrong_tree_kind_dir = dir.path().join("wrong_tree_kind.git");
    let r3 = gix::init_bare(&wrong_tree_kind_dir).unwrap();
    let blob_oid = r3
        .objects
        .write_buf(gix::objs::Kind::Blob, b"not a tree\n")
        .unwrap();
    let c_wrong_tree = format!(
        "tree {blob_oid}\nauthor T <t@e> 1730000000 +0000\ncommitter T <t@e> 1730000000 +0000\n\nmsg\n"
    );
    let c3_oid = r3
        .objects
        .write_buf(gix::objs::Kind::Commit, c_wrong_tree.as_bytes())
        .unwrap();
    write_git_refs(
        &r3,
        vec![(
            "refs/heads/main".try_into().unwrap(),
            gix::refs::Target::Object(c3_oid),
        )],
    )
    .unwrap();
    let err = match collect_git_graph(
        &r3.into_sync(),
        &wrong_tree_kind_dir,
        &wrong_tree_kind_dir,
        false,
    )
    .await
    {
        Ok(_) => panic!("blob referenced as tree must fail preflight"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("Expected Git tree"),
        "unexpected error: {err}"
    );

    // Helper to build a bare repo with a single crafted tree entry (raw mode + name + oid).
    let run_raw_tree_case =
        |case_name: &str, raw_tree_body: &[u8], setup_extra: Option<&dyn Fn(&gix::Repository)>| {
            let p = dir.path().join(case_name);
            let repo = gix::init_bare(&p).unwrap();
            if let Some(f) = setup_extra {
                f(&repo);
            }
            let tree_oid = repo
                .objects
                .write_buf(gix::objs::Kind::Tree, raw_tree_body)
                .unwrap();
            let c_bytes = format!(
                "tree {tree_oid}\nauthor T <t@e> 1730000000 +0000\ncommitter T <t@e> 1730000000 +0000\n\nmsg\n"
            );
            let c_oid = repo
                .objects
                .write_buf(gix::objs::Kind::Commit, c_bytes.as_bytes())
                .unwrap();
            write_git_refs(
                &repo,
                vec![(
                    "refs/heads/main".try_into().unwrap(),
                    gix::refs::Target::Object(c_oid),
                )],
            )
            .unwrap();
            (p, repo.into_sync())
        };

    let make_raw_entry = |mode: &[u8], name: &[u8], oid: gix::hash::ObjectId| -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(mode);
        b.push(b' ');
        b.extend_from_slice(name);
        b.push(0);
        b.extend_from_slice(oid.as_bytes());
        b
    };

    // Symlink target containing NUL byte fails preflight.
    let nul_link_oid =
        gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Blob, b"bad\0link")
            .unwrap();
    let (p_nul, ts_nul) = run_raw_tree_case(
        "nul_symlink.git",
        &make_raw_entry(b"120000", b"lnk", nul_link_oid),
        Some(&|r: &gix::Repository| {
            r.objects
                .write_buf(gix::objs::Kind::Blob, b"bad\0link")
                .unwrap();
        }),
    );
    let err = match collect_git_graph(&ts_nul, &p_nul, &p_nul, false).await {
        Ok(_) => panic!("NUL symlink must fail preflight"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("NUL"), "unexpected error: {err}");

    // Symlink pointing at a Tree object fails object-kind preflight.
    let empty_tree_oid = gix::hash::ObjectId::try_from(EMPTY_TREE_ID_BYTES.as_slice()).unwrap();
    let (p_link_kind, ts_link_kind) = run_raw_tree_case(
        "link_wrong_kind.git",
        &make_raw_entry(b"120000", b"lnk", empty_tree_oid),
        Some(&|r: &gix::Repository| {
            r.objects.write_buf(gix::objs::Kind::Tree, &[]).unwrap();
        }),
    );
    let err = match collect_git_graph(&ts_link_kind, &p_link_kind, &p_link_kind, false).await {
        Ok(_) => panic!("symlink pointing to tree must fail preflight"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("Expected Git blob for symlink"),
        "unexpected error: {err}"
    );

    // Blob entry pointing at a Tree object fails `find_header` object-kind preflight.
    let (p_blob_kind, ts_blob_kind) = run_raw_tree_case(
        "blob_wrong_kind.git",
        &make_raw_entry(b"100644", b"file.txt", empty_tree_oid),
        Some(&|r: &gix::Repository| {
            r.objects.write_buf(gix::objs::Kind::Tree, &[]).unwrap();
        }),
    );
    let err = match collect_git_graph(&ts_blob_kind, &p_blob_kind, &p_blob_kind, false).await {
        Ok(_) => panic!("blob entry pointing to tree must fail preflight"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("Expected Git blob"),
        "unexpected error: {err}"
    );

    // Missing blob object fails preflight.
    let missing_oid =
        gix::hash::ObjectId::from_hex(b"9999999999999999999999999999999999999999").unwrap();
    let (p_missing, ts_missing) = run_raw_tree_case(
        "missing_blob.git",
        &make_raw_entry(b"100644", b"missing.txt", missing_oid),
        None,
    );
    assert!(
        collect_git_graph(&ts_missing, &p_missing, &p_missing, false)
            .await
            .is_err(),
        "missing blob object must fail preflight"
    );

    // Invalid tree entry names (`""`, `.`, `..`, `a/b`) and duplicate entry names fail.
    let ok_blob_oid =
        gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Blob, b"ok").unwrap();
    for (idx, bad_name) in [b"".as_slice(), b".", b"..", b"a/b"]
        .into_iter()
        .enumerate()
    {
        let (p_bad, ts_bad) = run_raw_tree_case(
            &format!("bad_name_{idx}.git"),
            &make_raw_entry(b"100644", bad_name, ok_blob_oid),
            Some(&|r: &gix::Repository| {
                r.objects.write_buf(gix::objs::Kind::Blob, b"ok").unwrap();
            }),
        );
        let err = match collect_git_graph(&ts_bad, &p_bad, &p_bad, false).await {
            Ok(_) => panic!("invalid tree entry name must fail preflight"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("Invalid filename"),
            "unexpected error: {err}"
        );
    }

    let dup_body = [
        make_raw_entry(b"100644", b"dup.txt", ok_blob_oid),
        make_raw_entry(b"100644", b"dup.txt", ok_blob_oid),
    ]
    .concat();
    let (p_dup, ts_dup) = run_raw_tree_case(
        "dup_name.git",
        &dup_body,
        Some(&|r: &gix::Repository| {
            r.objects.write_buf(gix::objs::Kind::Blob, b"ok").unwrap();
        }),
    );
    let err = match collect_git_graph(&ts_dup, &p_dup, &p_dup, false).await {
        Ok(_) => panic!("duplicate tree entry name must fail preflight"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("Duplicate filename"),
        "unexpected error: {err}"
    );

    // Non-canonical / unsupported Git tree entry mode (`100664`) fails preflight.
    let (p_mode, ts_mode) = run_raw_tree_case(
        "bad_mode.git",
        &make_raw_entry(b"100664", b"legacy.txt", ok_blob_oid),
        Some(&|r: &gix::Repository| {
            r.objects.write_buf(gix::objs::Kind::Blob, b"ok").unwrap();
        }),
    );
    let err = match collect_git_graph(&ts_mode, &p_mode, &p_mode, false).await {
        Ok(_) => panic!("unsupported tree mode 100664 must fail preflight"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("Unsupported Git tree entry mode"),
        "unexpected error: {err}"
    );

    // Empty `gpgsig` header fails in `collect_git_graph` preflight with commit OID before
    // any uploads run.
    let empty_gpgsig_bytes = format!(
        "tree {empty_tree_hex}\nauthor T <t@e> 1730000000 +0000\ncommitter T <t@e> 1730000000 +0000\ngpgsig \n\nmsg\n"
    );
    let (p_sig, ts_sig, empty_sig_oid) =
        make_bare_with_commit_bytes("empty_gpgsig.git", empty_gpgsig_bytes.as_bytes());
    let err = match collect_git_graph(&ts_sig, &p_sig, &p_sig, false).await {
        Ok(_) => panic!("empty gpgsig header must fail preflight"),
        Err(err) => err,
    };
    let sig_msg = err.to_string();
    assert!(
        sig_msg.contains(&empty_sig_oid.to_string()) && sig_msg.contains("gpgsig"),
        "unexpected error for empty gpgsig: {sig_msg}"
    );
}

#[tokio::test]
async fn op_store_digest_length_and_read_view_id_validation() {
    use std::sync::atomic::AtomicUsize;

    struct ConfigurableOpStore {
        reconcile_unimplemented: bool,
        reconcile_client_res_required: bool,
        reconcile_op_head: Vec<u8>,
        reconcile_current_heads: Vec<Vec<u8>>,
        get_op_heads: Vec<Vec<u8>>,
        read_op_view_id: Vec<u8>,
        read_view_resp_id: Vec<u8>,
        publish_unimplemented: bool,
        write_view_resp_id: Vec<u8>,
        write_op_resp_id: Vec<u8>,
        read_view_calls: Arc<AtomicUsize>,
        write_op_calls: Arc<AtomicUsize>,
        update_op_heads_calls: Arc<AtomicUsize>,
    }

    impl Default for ConfigurableOpStore {
        fn default() -> Self {
            Self {
                reconcile_unimplemented: false,
                reconcile_client_res_required: false,
                reconcile_op_head: vec![2; cc_common::OPERATION_ID_LENGTH],
                reconcile_current_heads: vec![vec![2; cc_common::OPERATION_ID_LENGTH]],
                get_op_heads: vec![vec![2; cc_common::OPERATION_ID_LENGTH]],
                read_op_view_id: vec![1; cc_common::VIEW_ID_LENGTH],
                read_view_resp_id: vec![1; cc_common::VIEW_ID_LENGTH],
                publish_unimplemented: false,
                write_view_resp_id: vec![3; cc_common::VIEW_ID_LENGTH],
                write_op_resp_id: vec![4; cc_common::OPERATION_ID_LENGTH],
                read_view_calls: Arc::new(AtomicUsize::new(0)),
                write_op_calls: Arc::new(AtomicUsize::new(0)),
                update_op_heads_calls: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    #[tonic::async_trait]
    impl OpStoreService for ConfigurableOpStore {
        async fn read_operation(
            &self,
            _request: Request<ReadOperationRequest>,
        ) -> Result<Response<ReadOperationResponse>, Status> {
            Ok(Response::new(ReadOperationResponse {
                operation: Some(Operation {
                    view_id: self.read_op_view_id.clone(),
                    parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                    ..Default::default()
                }),
            }))
        }

        async fn write_operation(
            &self,
            _request: Request<WriteOperationRequest>,
        ) -> Result<Response<WriteOperationResponse>, Status> {
            self.write_op_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Response::new(WriteOperationResponse {
                operation_id: self.write_op_resp_id.clone(),
            }))
        }

        async fn resolve_operation_prefix(
            &self,
            _request: Request<ResolveOperationPrefixRequest>,
        ) -> Result<Response<ResolveOperationPrefixResponse>, Status> {
            Err(Status::unimplemented("resolve_operation_prefix"))
        }

        async fn read_view(
            &self,
            _request: Request<ReadViewRequest>,
        ) -> Result<Response<ReadViewResponse>, Status> {
            self.read_view_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Response::new(ReadViewResponse {
                view_id: self.read_view_resp_id.clone(),
                view: Some(cc_common::op_store::View {
                    head_ids: vec![ROOT_COMMIT_ID_BYTES.to_vec()],
                    wc_commit_ids: HashMap::new(),
                    local_bookmarks: HashMap::new(),
                    remote_bookmarks: HashMap::new(),
                }),
            }))
        }

        async fn write_view(
            &self,
            _request: Request<WriteViewRequest>,
        ) -> Result<Response<WriteViewResponse>, Status> {
            Ok(Response::new(WriteViewResponse {
                view_id: self.write_view_resp_id.clone(),
            }))
        }

        async fn get_op_heads(
            &self,
            _request: Request<GetOpHeadsRequest>,
        ) -> Result<Response<GetOpHeadsResponse>, Status> {
            Ok(Response::new(GetOpHeadsResponse {
                op_head_ids: self.get_op_heads.clone(),
            }))
        }

        async fn update_op_heads(
            &self,
            _request: Request<UpdateOpHeadsRequest>,
        ) -> Result<Response<UpdateOpHeadsResponse>, Status> {
            self.update_op_heads_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Response::new(UpdateOpHeadsResponse::default()))
        }

        async fn reconcile_op_heads(
            &self,
            _request: Request<ReconcileOpHeadsRequest>,
        ) -> Result<Response<ReconcileOpHeadsResponse>, Status> {
            if self.reconcile_unimplemented {
                return Err(Status::unimplemented("reconcile_op_heads"));
            }
            Ok(Response::new(ReconcileOpHeadsResponse {
                op_head: self.reconcile_op_head.clone(),
                client_resolution_required: self.reconcile_client_res_required,
                current_op_head_ids: self.reconcile_current_heads.clone(),
            }))
        }

        async fn publish_operation(
            &self,
            _request: Request<PublishOperationRequest>,
        ) -> Result<Response<PublishOperationResponse>, Status> {
            if self.publish_unimplemented {
                return Err(Status::unimplemented("publish_operation"));
            }
            Ok(Response::new(PublishOperationResponse {
                view_id: vec![3; cc_common::VIEW_ID_LENGTH],
                operation_id: vec![4; cc_common::OPERATION_ID_LENGTH],
                current_op_head_ids: vec![vec![4; cc_common::OPERATION_ID_LENGTH]],
            }))
        }
    }

    let spawn_store = |store: ConfigurableOpStore| async move {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let incoming = futures::stream::unfold(listener, |listener| async move {
            Some((listener.accept().await.map(|(stream, _)| stream), listener))
        });
        let backend = MockBackendService {
            commits: HashMap::new(),
            trees: HashMap::new(),
            files: HashMap::new(),
        };
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(OpStoreServiceServer::new(store))
                .add_service(
                    cc_common::backend::backend_service_server::BackendServiceServer::new(backend),
                )
                .serve_with_incoming(incoming),
        );
        url
    };

    // `resolve_single_op_head` rejects wrong-length operation IDs from ReconcileOpHeads and GetOpHeads.
    for (label, store) in [
        (
            "bad ReconcileOpHeads.op_head",
            ConfigurableOpStore {
                reconcile_op_head: vec![2; 4],
                ..Default::default()
            },
        ),
        (
            "bad ReconcileOpHeads.current_op_head_ids",
            ConfigurableOpStore {
                reconcile_client_res_required: true,
                reconcile_current_heads: vec![vec![2; 64]],
                ..Default::default()
            },
        ),
        (
            "bad GetOpHeads.op_head_ids",
            ConfigurableOpStore {
                reconcile_unimplemented: true,
                get_op_heads: vec![vec![2; 8]],
                ..Default::default()
            },
        ),
    ] {
        let url = spawn_store(store).await;
        let mut client = connect_op_store_client(url).await.unwrap();
        let err = resolve_single_op_head(&mut client, "repo")
            .await
            .expect_err("wrong-length operation ID must fail");
        assert!(
            err.to_string().contains("must be 16 bytes"),
            "{label}: unexpected error: {err}"
        );
    }

    // Wrong-length `op.view_id` fails before `ReadView` for both import publish and export.
    {
        let read_view_calls = Arc::new(AtomicUsize::new(0));
        let url = spawn_store(ConfigurableOpStore {
            read_op_view_id: vec![1; 64],
            read_view_calls: Arc::clone(&read_view_calls),
            ..Default::default()
        })
        .await;
        let mut op_client = connect_op_store_client(url.clone()).await.unwrap();
        let err = publish_imported_refs(
            &mut op_client,
            None,
            "repo",
            &[],
            &HashSet::new(),
            &HashMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .await
        .expect_err("wrong-length op.view_id must fail publish_imported_refs");
        assert!(
            err.to_string().contains("must be 16 bytes"),
            "unexpected error: {err}"
        );

        let dir = tempfile::tempdir().unwrap();
        let exp_err = GitExporter::new(GitExportOptions {
            git_dir: dir.path().join("bad_view_id.git"),
            server_url: url,
            repo_id: "repo".to_string(),
            project_id: None,
            bare: true,
        })
        .run()
        .await
        .expect_err("wrong-length op.view_id must fail GitExporter");
        assert!(
            exp_err.to_string().contains("must be 16 bytes"),
            "unexpected error: {exp_err}"
        );
        assert_eq!(
            read_view_calls.load(Ordering::SeqCst),
            0,
            "ReadView must not be called when op.view_id has invalid length"
        );
    }

    // Mismatched `ReadViewResponse.view_id` fails both import publish and export; empty
    // `ReadViewResponse.view_id` remains accepted for backwards compatibility.
    {
        let url = spawn_store(ConfigurableOpStore {
            read_view_resp_id: vec![9; cc_common::VIEW_ID_LENGTH],
            ..Default::default()
        })
        .await;
        let mut op_client = connect_op_store_client(url.clone()).await.unwrap();
        let err = publish_imported_refs(
            &mut op_client,
            None,
            "repo",
            &[],
            &HashSet::new(),
            &HashMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .await
        .expect_err("mismatched ReadViewResponse.view_id must fail publish_imported_refs");
        assert!(
            err.to_string().contains("did not match requested"),
            "unexpected error: {err}"
        );

        let dir = tempfile::tempdir().unwrap();
        let exp_err = GitExporter::new(GitExportOptions {
            git_dir: dir.path().join("mismatched_view_id.git"),
            server_url: url,
            repo_id: "repo".to_string(),
            project_id: None,
            bare: true,
        })
        .run()
        .await
        .expect_err("mismatched ReadViewResponse.view_id must fail GitExporter");
        assert!(
            exp_err.to_string().contains("did not match requested"),
            "unexpected error: {exp_err}"
        );

        let compat_url = spawn_store(ConfigurableOpStore {
            read_view_resp_id: Vec::new(),
            ..Default::default()
        })
        .await;
        let mut compat_client = connect_op_store_client(compat_url).await.unwrap();
        publish_imported_refs(
            &mut compat_client,
            None,
            "repo",
            &[],
            &HashSet::new(),
            &HashMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .await
        .expect("empty ReadViewResponse.view_id must stay accepted");
    }

    // Fallback WriteView / WriteOperation path rejects wrong-length returned IDs before the
    // subsequent write RPC runs.
    {
        let write_op_calls = Arc::new(AtomicUsize::new(0));
        let url = spawn_store(ConfigurableOpStore {
            publish_unimplemented: true,
            write_view_resp_id: vec![3; 5],
            write_op_calls: Arc::clone(&write_op_calls),
            ..Default::default()
        })
        .await;
        let mut op_client = connect_op_store_client(url).await.unwrap();
        let err = publish_imported_refs(
            &mut op_client,
            None,
            "repo",
            &[],
            &HashSet::new(),
            &HashMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .await
        .expect_err("wrong-length WriteViewResponse.view_id must fail");
        assert!(
            err.to_string().contains("WriteViewResponse.view_id"),
            "unexpected error: {err}"
        );
        assert_eq!(write_op_calls.load(Ordering::SeqCst), 0);
    }
    {
        let update_calls = Arc::new(AtomicUsize::new(0));
        let url = spawn_store(ConfigurableOpStore {
            publish_unimplemented: true,
            write_op_resp_id: vec![4; 5],
            update_op_heads_calls: Arc::clone(&update_calls),
            ..Default::default()
        })
        .await;
        let mut op_client = connect_op_store_client(url).await.unwrap();
        let err = publish_imported_refs(
            &mut op_client,
            None,
            "repo",
            &[],
            &HashSet::new(),
            &HashMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .await
        .expect_err("wrong-length WriteOperationResponse.operation_id must fail");
        assert!(
            err.to_string()
                .contains("WriteOperationResponse.operation_id"),
            "unexpected error: {err}"
        );
        assert_eq!(update_calls.load(Ordering::SeqCst), 0);
    }
}
