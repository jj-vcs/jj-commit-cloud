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
use std::sync::Arc;

use cc_common::backend::{
    Commit, File, SecureSig, Signature, Timestamp, TreeEntry, TreeValue, tree_value,
};
use cc_common::op_store::op_store_service_server::OpStoreServiceServer;
use cc_common::op_store::{RefTarget, RefTargetTerm};
use cc_common::{
    CHANGE_ID_LENGTH, COMMIT_ID_LENGTH, EMPTY_TREE_ID_BYTES, ROOT_COMMIT_ID_BYTES, TREE_ID_LENGTH,
};
use gix::objs::WriteTo as _;
use gix::prelude::Write as _;

use super::*;
use crate::util::connect_backend_client;

#[tokio::test]
async fn export_tree_recursive_rejects_cyclic_trees_and_allows_diamonds() {
    let dir = tempfile::tempdir().unwrap();
    let repo = gix::init_bare(dir.path()).unwrap();

    let tree_a = vec![0xaa; COMMIT_ID_LENGTH];
    let tree_b = vec![0xbb; COMMIT_ID_LENGTH];
    let tree_self = vec![0xcc; COMMIT_ID_LENGTH];

    // Also build a valid diamond DAG (`root -> {left, right} -> shared`) with real Git SHA-1s.
    let blob_bytes = b"shared leaf\n".to_vec();
    let blob_oid =
        gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Blob, &blob_bytes).unwrap();
    let shared_gix = gix::objs::Tree {
        entries: vec![gix::objs::tree::Entry {
            mode: gix::objs::tree::EntryKind::Blob.into(),
            filename: "leaf.txt".into(),
            oid: blob_oid,
        }],
    };
    let mut shared_buf = Vec::new();
    shared_gix.write_to(&mut shared_buf).unwrap();
    let shared_oid =
        gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Tree, &shared_buf).unwrap();
    let root_gix = gix::objs::Tree {
        entries: vec![
            gix::objs::tree::Entry {
                mode: gix::objs::tree::EntryKind::Tree.into(),
                filename: "left".into(),
                oid: shared_oid,
            },
            gix::objs::tree::Entry {
                mode: gix::objs::tree::EntryKind::Tree.into(),
                filename: "right".into(),
                oid: shared_oid,
            },
        ],
    };
    let mut root_buf = Vec::new();
    root_gix.write_to(&mut root_buf).unwrap();
    let root_oid =
        gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Tree, &root_buf).unwrap();

    let tree_entry = |name: &str, id: &[u8]| TreeEntry {
        name: name.to_string(),
        value: Some(TreeValue {
            value: Some(tree_value::Value::TreeId(id.to_vec())),
        }),
    };

    let mut trees = HashMap::new();
    // Self-cycle: tree_self -> tree_self.
    trees.insert(tree_self.clone(), vec![tree_entry("loop", &tree_self)]);
    // 2-cycle: tree_a -> tree_b -> tree_a.
    trees.insert(tree_a.clone(), vec![tree_entry("b", &tree_b)]);
    trees.insert(tree_b.clone(), vec![tree_entry("a", &tree_a)]);
    // Valid diamond DAG.
    trees.insert(
        shared_oid.as_bytes().to_vec(),
        vec![TreeEntry {
            name: "leaf.txt".to_string(),
            value: Some(TreeValue {
                value: Some(tree_value::Value::File(File {
                    id: blob_oid.as_bytes().to_vec(),
                    executable: false,
                    copy_id: Vec::new(),
                    size_bytes: None,
                })),
            }),
        }],
    );
    trees.insert(
        root_oid.as_bytes().to_vec(),
        vec![
            tree_entry("left", shared_oid.as_bytes()),
            tree_entry("right", shared_oid.as_bytes()),
        ],
    );

    let backend = MockBackendService {
        commits: HashMap::new(),
        trees,
        files: HashMap::from([(blob_oid.as_bytes().to_vec(), blob_bytes)]),
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
    let mut tree_state = ExportTreeState::default();

    // Self-cycle `A -> A` must return an explicit cycle error.
    let self_err = export_tree_recursive(
        &ts_repo,
        &mut backend_client,
        "proj",
        &tree_self,
        &mut tree_state,
    )
    .await
    .expect_err("self-cyclic tree must fail with cycle error");
    assert!(
        self_err.to_string().contains("Cycle detected"),
        "unexpected error: {self_err}"
    );
    assert!(tree_state.in_progress.is_empty());

    // Two-node cycle `A -> B -> A` must return an explicit cycle error.
    let two_cycle_err = export_tree_recursive(
        &ts_repo,
        &mut backend_client,
        "proj",
        &tree_a,
        &mut tree_state,
    )
    .await
    .expect_err("A -> B -> A tree cycle must fail with cycle error");
    assert!(
        two_cycle_err.to_string().contains("Cycle detected"),
        "unexpected error: {two_cycle_err}"
    );
    assert!(tree_state.in_progress.is_empty());

    // Valid diamond DAG must still succeed.
    export_tree_recursive(
        &ts_repo,
        &mut backend_client,
        "proj",
        root_oid.as_bytes(),
        &mut tree_state,
    )
    .await
    .expect("diamond tree DAG must export without false cycle errors");
    assert!(tree_state.written_trees.contains(root_oid.as_bytes()));
    assert!(tree_state.written_trees.contains(shared_oid.as_bytes()));
    assert!(tree_state.in_progress.is_empty());
}

#[tokio::test]
async fn export_tree_recursive_validates_pagination_and_entries_before_fetching_children() {
    let dir = tempfile::tempdir().unwrap();
    let repo = gix::init_bare(dir.path()).unwrap();

    let blob_bytes = b"valid blob\n".to_vec();
    let blob_oid =
        gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Blob, &blob_bytes).unwrap();
    let file_entry = |name: &str| TreeEntry {
        name: name.to_string(),
        value: Some(TreeValue {
            value: Some(tree_value::Value::File(File {
                id: blob_oid.as_bytes().to_vec(),
                executable: false,
                copy_id: Vec::new(),
                size_bytes: None,
            })),
        }),
    };

    let tid_mismatch = vec![0x01; TREE_ID_LENGTH];
    let tid_empty_page = vec![0x02; TREE_ID_LENGTH];
    let tid_oversized = vec![0x03; TREE_ID_LENGTH];
    let tid_missing_val = vec![0x04; TREE_ID_LENGTH];
    let tid_missing_inner = vec![0x05; TREE_ID_LENGTH];
    let tid_dup_same = vec![0x06; TREE_ID_LENGTH];
    let tid_dup_paged = vec![0x07; TREE_ID_LENGTH];
    let tid_bad_oid_len = vec![0x08; TREE_ID_LENGTH];
    let tid_non_empty_copy_id = vec![0x09; TREE_ID_LENGTH];

    let mut trees = HashMap::new();
    trees.insert(
        tid_mismatch.clone(),
        MockTreeBehavior::Pages(vec![cc_common::backend::ReadTreeResponse {
            tree_id: vec![0xff; TREE_ID_LENGTH],
            entries: vec![file_entry("ok.txt")],
            next_page_token: String::new(),
        }]),
    );
    trees.insert(
        tid_empty_page.clone(),
        MockTreeBehavior::Pages(vec![cc_common::backend::ReadTreeResponse {
            tree_id: tid_empty_page.clone(),
            entries: Vec::new(),
            next_page_token: "0001".to_string(),
        }]),
    );
    // 11 pages x 50,000 entries = 550,000 > MAX_TREE_ENTRIES (500,000).
    trees.insert(
        tid_oversized.clone(),
        MockTreeBehavior::Oversized {
            page_entries: 50_000,
            total_pages: 11,
        },
    );
    trees.insert(
        tid_missing_val.clone(),
        MockTreeBehavior::Pages(vec![cc_common::backend::ReadTreeResponse {
            tree_id: tid_missing_val.clone(),
            entries: vec![
                file_entry("valid_first.txt"),
                TreeEntry {
                    name: "no_val.txt".to_string(),
                    value: None,
                },
            ],
            next_page_token: String::new(),
        }]),
    );
    trees.insert(
        tid_missing_inner.clone(),
        MockTreeBehavior::Pages(vec![cc_common::backend::ReadTreeResponse {
            tree_id: tid_missing_inner.clone(),
            entries: vec![
                file_entry("valid_first.txt"),
                TreeEntry {
                    name: "empty_val.txt".to_string(),
                    value: Some(TreeValue { value: None }),
                },
            ],
            next_page_token: String::new(),
        }]),
    );
    trees.insert(
        tid_dup_same.clone(),
        MockTreeBehavior::Pages(vec![cc_common::backend::ReadTreeResponse {
            tree_id: tid_dup_same.clone(),
            entries: vec![file_entry("dup.txt"), file_entry("dup.txt")],
            next_page_token: String::new(),
        }]),
    );
    trees.insert(
        tid_dup_paged.clone(),
        MockTreeBehavior::Pages(vec![
            cc_common::backend::ReadTreeResponse {
                tree_id: tid_dup_paged.clone(),
                entries: vec![file_entry("a.txt"), file_entry("dup.txt")],
                next_page_token: "0001".to_string(),
            },
            cc_common::backend::ReadTreeResponse {
                tree_id: tid_dup_paged.clone(),
                entries: vec![file_entry("dup.txt")],
                next_page_token: String::new(),
            },
        ]),
    );
    trees.insert(
        tid_bad_oid_len.clone(),
        MockTreeBehavior::Pages(vec![cc_common::backend::ReadTreeResponse {
            tree_id: tid_bad_oid_len.clone(),
            entries: vec![
                file_entry("valid_first.txt"),
                TreeEntry {
                    name: "short_id.txt".to_string(),
                    value: Some(TreeValue {
                        value: Some(tree_value::Value::File(File {
                            id: vec![0x11; 4],
                            executable: false,
                            copy_id: Vec::new(),
                            size_bytes: None,
                        })),
                    }),
                },
            ],
            next_page_token: String::new(),
        }]),
    );
    trees.insert(
        tid_non_empty_copy_id.clone(),
        MockTreeBehavior::Pages(vec![cc_common::backend::ReadTreeResponse {
            tree_id: tid_non_empty_copy_id.clone(),
            entries: vec![
                file_entry("valid_first.txt"),
                TreeEntry {
                    name: "copied.txt".to_string(),
                    value: Some(TreeValue {
                        value: Some(tree_value::Value::File(File {
                            id: blob_oid.as_bytes().to_vec(),
                            executable: false,
                            copy_id: vec![0x42; 16],
                            size_bytes: None,
                        })),
                    }),
                },
            ],
            next_page_token: String::new(),
        }]),
    );

    let invalid_names = ["", ".", "..", ".git", ".GIT", "nested/file", "nul\0byte"];
    let child_subtree_id = vec![0x99; TREE_ID_LENGTH];
    let mut invalid_name_cases = Vec::new();
    for (i, bad_name) in invalid_names.iter().enumerate() {
        let tid = vec![0x20 + i as u8; TREE_ID_LENGTH];
        trees.insert(
            tid.clone(),
            MockTreeBehavior::Pages(vec![cc_common::backend::ReadTreeResponse {
                tree_id: tid.clone(),
                entries: vec![
                    // Put valid file and subtree entries first to verify validation runs BEFORE child fetches.
                    file_entry("valid_sibling.txt"),
                    TreeEntry {
                        name: "valid_subdir".to_string(),
                        value: Some(TreeValue {
                            value: Some(tree_value::Value::TreeId(child_subtree_id.clone())),
                        }),
                    },
                    file_entry(bad_name),
                ],
                next_page_token: String::new(),
            }]),
        );
        invalid_name_cases.push((*bad_name, tid));
    }

    // Symlink whose target contains a NUL byte must be rejected even if its SHA-1 matches.
    let nul_symlink_target = "bad\0target".to_string();
    let nul_symlink_oid = gix::objs::compute_hash(
        gix::hash::Kind::Sha1,
        gix::objs::Kind::Blob,
        nul_symlink_target.as_bytes(),
    )
    .unwrap();
    let tid_nul_symlink = vec![0x40; TREE_ID_LENGTH];
    trees.insert(
        tid_nul_symlink.clone(),
        MockTreeBehavior::Pages(vec![cc_common::backend::ReadTreeResponse {
            tree_id: tid_nul_symlink.clone(),
            entries: vec![TreeEntry {
                name: "bad.link".to_string(),
                value: Some(TreeValue {
                    value: Some(tree_value::Value::SymlinkId(
                        nul_symlink_oid.as_bytes().to_vec(),
                    )),
                }),
            }],
            next_page_token: String::new(),
        }]),
    );

    // Valid multi-page tree to confirm pagination still succeeds when valid.
    let valid_gix_tree = gix::objs::Tree {
        entries: vec![
            gix::objs::tree::Entry {
                mode: gix::objs::tree::EntryKind::Blob.into(),
                filename: "a.txt".into(),
                oid: blob_oid,
            },
            gix::objs::tree::Entry {
                mode: gix::objs::tree::EntryKind::Blob.into(),
                filename: "b.txt".into(),
                oid: blob_oid,
            },
        ],
    };
    let mut valid_tree_buf = Vec::new();
    valid_gix_tree.write_to(&mut valid_tree_buf).unwrap();
    let valid_tree_oid = gix::objs::compute_hash(
        gix::hash::Kind::Sha1,
        gix::objs::Kind::Tree,
        &valid_tree_buf,
    )
    .unwrap();
    let valid_tid = valid_tree_oid.as_bytes().to_vec();
    trees.insert(
        valid_tid.clone(),
        MockTreeBehavior::Pages(vec![
            cc_common::backend::ReadTreeResponse {
                tree_id: valid_tid.clone(),
                entries: vec![file_entry("a.txt")],
                next_page_token: "0001".to_string(),
            },
            cc_common::backend::ReadTreeResponse {
                tree_id: valid_tid.clone(),
                entries: vec![file_entry("b.txt")],
                next_page_token: String::new(),
            },
        ]),
    );

    let read_tree_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let read_file_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let backend = ValidationMockBackendService {
        commits: HashMap::new(),
        trees,
        files: HashMap::from([(blob_oid.as_bytes().to_vec(), blob_bytes)]),
        chunked_files: HashMap::new(),
        symlinks: HashMap::from([(nul_symlink_oid.as_bytes().to_vec(), nul_symlink_target)]),
        read_commit_calls: Arc::new(std::sync::Mutex::new(HashMap::new())),
        read_tree_calls: Arc::clone(&read_tree_calls),
        read_file_calls: Arc::clone(&read_file_calls),
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
    let mut tree_state = ExportTreeState::default();

    for (label, tid, expected_substr) in [
        ("mismatched tree_id", &tid_mismatch, "did not match"),
        (
            "empty page with continuation token",
            &tid_empty_page,
            "empty page",
        ),
        (
            "exceeds MAX_TREE_ENTRIES",
            &tid_oversized,
            "exceeded maximum",
        ),
        ("missing TreeValue", &tid_missing_val, "missing TreeValue"),
        (
            "missing inner TreeValue::value",
            &tid_missing_inner,
            "missing TreeValue",
        ),
        (
            "duplicate name in single page",
            &tid_dup_same,
            "duplicate tree entry name",
        ),
        (
            "duplicate name across pages",
            &tid_dup_paged,
            "duplicate tree entry name",
        ),
        (
            "invalid file OID length",
            &tid_bad_oid_len,
            "must have 20-byte",
        ),
        ("non-empty file copy_id", &tid_non_empty_copy_id, "copy_id"),
    ] {
        let err = match export_tree_recursive(
            &ts_repo,
            &mut backend_client,
            "proj",
            tid,
            &mut tree_state,
        )
        .await
        {
            Ok(()) => panic!("{label} should have failed"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains(expected_substr),
            "{label}: expected {expected_substr:?} in error, got: {err}"
        );
        assert_eq!(
            read_file_calls.load(Ordering::SeqCst),
            0,
            "{label}: must not fetch child blobs before tree validation succeeds"
        );
        assert!(tree_state.in_progress.is_empty());
    }

    for (bad_name, tid) in invalid_name_cases {
        let tree_calls_before = read_tree_calls.load(Ordering::SeqCst);
        let err =
            export_tree_recursive(&ts_repo, &mut backend_client, "proj", &tid, &mut tree_state)
                .await
                .expect_err("invalid tree entry name must fail");
        assert!(
            err.to_string().contains("invalid tree entry name"),
            "bad name {bad_name:?}: unexpected error: {err}"
        );
        assert_eq!(
            read_file_calls.load(Ordering::SeqCst),
            0,
            "bad name {bad_name:?}: must not fetch sibling blobs before name validation"
        );
        assert_eq!(
            read_tree_calls.load(Ordering::SeqCst),
            tree_calls_before + 1,
            "bad name {bad_name:?}: must not fetch sibling subtrees before name validation"
        );
    }

    // Symlink target with NUL byte must be rejected without writing to `.git/objects/`.
    let nul_sym_err = export_tree_recursive(
        &ts_repo,
        &mut backend_client,
        "proj",
        &tid_nul_symlink,
        &mut tree_state,
    )
    .await
    .expect_err("ReadSymlink target with NUL byte must fail");
    assert!(
        nul_sym_err.to_string().contains("NUL byte"),
        "unexpected error for NUL symlink: {nul_sym_err}"
    );
    assert!(
        ts_repo
            .to_thread_local()
            .try_find_object(nul_symlink_oid)
            .unwrap()
            .is_none(),
        "rejected NUL symlink must not be written into target ODB"
    );

    // Valid multi-page tree exports and fetches the shared blob once.
    export_tree_recursive(
        &ts_repo,
        &mut backend_client,
        "proj",
        &valid_tid,
        &mut tree_state,
    )
    .await
    .expect("valid paginated tree must succeed");
    assert!(tree_state.written_trees.contains(&valid_tid));
    assert_eq!(read_file_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn export_fails_closed_on_corrupt_or_mismatched_raw_git_commit() {
    let dir = tempfile::tempdir().unwrap();
    let repo = gix::init_bare(dir.path()).unwrap();

    let parent_a = vec![0x11; COMMIT_ID_LENGTH];
    let parent_b = vec![0x22; COMMIT_ID_LENGTH];
    let fake_raw_tree = vec![0xde; TREE_ID_LENGTH];

    let base_commit = Commit {
        commit_id: Vec::new(),
        change_id: vec![0x33; CHANGE_ID_LENGTH],
        parent_commit_ids: vec![ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![EMPTY_TREE_ID_BYTES.to_vec()],
        description: "structured commit\n".to_string(),
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
    let canonical_bytes = serialize_proto_commit_to_git(&base_commit, true).unwrap();
    let canonical_oid = gix::objs::compute_hash(
        gix::hash::Kind::Sha1,
        gix::objs::Kind::Commit,
        &canonical_bytes,
    )
    .unwrap();

    // Non-empty `raw_git_commit` whose SHA-1 does NOT match `expected_id` must fail closed
    // even though structured fields serialize to `canonical_oid`.
    let mut bad_sha_commit = base_commit.clone();
    bad_sha_commit.commit_id = canonical_oid.as_bytes().to_vec();
    bad_sha_commit.raw_git_commit = format!(
        "tree {}\nauthor Test <test@example.com> 1730000000 +0000\ncommitter Test <test@example.com> 1730000000 +0000\n\ntampered\n",
        hex::encode(&fake_raw_tree)
    )
    .into_bytes();

    let ts_repo = repo.into_sync();
    let err = export_single_commit(&ts_repo, canonical_oid.as_bytes(), &bad_sha_commit)
        .expect_err("SHA-1 mismatch in raw_git_commit must fail closed");
    assert!(
        err.to_string().contains("raw_git_commit SHA-1 mismatch"),
        "unexpected error: {err}"
    );

    // Non-empty `raw_git_commit` whose SHA-1 matches `expected_id` but whose `parent`
    // headers disagree with `commit.parent_commit_ids` must also fail closed.
    let raw_with_parent_a = format!(
        "tree {}\nparent {}\nauthor Test <test@example.com> 1730000000 +0000\ncommitter Test <test@example.com> 1730000000 +0000\n\nwith parent a\n",
        hex::encode(EMPTY_TREE_ID_BYTES),
        hex::encode(&parent_a)
    )
    .into_bytes();
    let raw_parent_a_oid = gix::objs::compute_hash(
        gix::hash::Kind::Sha1,
        gix::objs::Kind::Commit,
        &raw_with_parent_a,
    )
    .unwrap();
    let mismatched_parents_commit = Commit {
        commit_id: raw_parent_a_oid.as_bytes().to_vec(),
        change_id: vec![0x44; CHANGE_ID_LENGTH],
        parent_commit_ids: vec![parent_b.clone()],
        root_tree_id: vec![EMPTY_TREE_ID_BYTES.to_vec()],
        description: "with parent a\n".to_string(),
        raw_git_commit: raw_with_parent_a,
        ..Default::default()
    };
    let err = export_single_commit(
        &ts_repo,
        raw_parent_a_oid.as_bytes(),
        &mismatched_parents_commit,
    )
    .expect_err("parent mismatch in raw_git_commit must fail closed");
    assert!(
        err.to_string()
            .contains("raw_git_commit parents do not match"),
        "unexpected error: {err}"
    );

    // Non-conflicted `raw_git_commit` whose `tree` header disagrees with `root_tree_id`.
    let raw_mismatched_tree = format!(
        "tree {}\nauthor Test <test@example.com> 1730000000 +0000\ncommitter Test <test@example.com> 1730000000 +0000\n\nmismatched tree\n",
        hex::encode(&fake_raw_tree)
    )
    .into_bytes();
    let raw_mismatched_tree_oid = gix::objs::compute_hash(
        gix::hash::Kind::Sha1,
        gix::objs::Kind::Commit,
        &raw_mismatched_tree,
    )
    .unwrap();
    let mismatched_tree_commit = Commit {
        commit_id: raw_mismatched_tree_oid.as_bytes().to_vec(),
        change_id: vec![0x45; CHANGE_ID_LENGTH],
        parent_commit_ids: vec![ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![EMPTY_TREE_ID_BYTES.to_vec()],
        description: "mismatched tree\n".to_string(),
        raw_git_commit: raw_mismatched_tree,
        ..Default::default()
    };
    let err = export_single_commit(
        &ts_repo,
        raw_mismatched_tree_oid.as_bytes(),
        &mismatched_tree_commit,
    )
    .expect_err("tree mismatch in raw_git_commit must fail closed");
    assert!(
        err.to_string()
            .contains("raw_git_commit tree header does not match"),
        "unexpected error: {err}"
    );

    // `raw_git_commit` carrying `jj:trees` that disagrees with `commit.root_tree_id`.
    let t1 = vec![0xa1; TREE_ID_LENGTH];
    let t2 = vec![0xa2; TREE_ID_LENGTH];
    let t3 = vec![0xa3; TREE_ID_LENGTH];
    let raw_mismatched_jj_trees = format!(
        "tree {}\nauthor Test <test@example.com> 1730000000 +0000\ncommitter Test <test@example.com> 1730000000 +0000\njj:trees {} {} {}\n\nmismatched jj trees\n",
        hex::encode(&fake_raw_tree),
        hex::encode(&t1),
        hex::encode(&t2),
        hex::encode(&t3),
    )
    .into_bytes();
    let raw_mismatched_jj_trees_oid = gix::objs::compute_hash(
        gix::hash::Kind::Sha1,
        gix::objs::Kind::Commit,
        &raw_mismatched_jj_trees,
    )
    .unwrap();
    let mismatched_jj_trees_commit = Commit {
        commit_id: raw_mismatched_jj_trees_oid.as_bytes().to_vec(),
        change_id: vec![0x46; CHANGE_ID_LENGTH],
        parent_commit_ids: vec![ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![EMPTY_TREE_ID_BYTES.to_vec()],
        description: "mismatched jj trees\n".to_string(),
        raw_git_commit: raw_mismatched_jj_trees,
        ..Default::default()
    };
    let err = export_single_commit(
        &ts_repo,
        raw_mismatched_jj_trees_oid.as_bytes(),
        &mismatched_jj_trees_commit,
    )
    .expect_err("jj:trees mismatch in raw_git_commit must fail closed");
    assert!(
        err.to_string()
            .contains("raw_git_commit jj:trees does not match"),
        "unexpected error: {err}"
    );

    // `raw_git_commit` carrying a malformed (even-length) `jj:trees` header.
    let raw_malformed_jj_trees = format!(
        "tree {}\nauthor Test <test@example.com> 1730000000 +0000\ncommitter Test <test@example.com> 1730000000 +0000\njj:trees {} {}\n\nmalformed jj trees\n",
        hex::encode(EMPTY_TREE_ID_BYTES),
        hex::encode(&t1),
        hex::encode(&t2),
    )
    .into_bytes();
    let raw_malformed_jj_trees_oid = gix::objs::compute_hash(
        gix::hash::Kind::Sha1,
        gix::objs::Kind::Commit,
        &raw_malformed_jj_trees,
    )
    .unwrap();
    let malformed_jj_trees_commit = Commit {
        commit_id: raw_malformed_jj_trees_oid.as_bytes().to_vec(),
        change_id: vec![0x47; CHANGE_ID_LENGTH],
        parent_commit_ids: vec![ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![EMPTY_TREE_ID_BYTES.to_vec()],
        description: "malformed jj trees\n".to_string(),
        raw_git_commit: raw_malformed_jj_trees,
        ..Default::default()
    };
    let err = export_single_commit(
        &ts_repo,
        raw_malformed_jj_trees_oid.as_bytes(),
        &malformed_jj_trees_commit,
    )
    .expect_err("malformed jj:trees in raw_git_commit must fail closed");
    assert!(
        err.to_string().contains("jj:trees"),
        "unexpected error: {err}"
    );

    // Conflicted `commit.root_tree_id` (3 trees) when `raw_git_commit` has no `jj:trees`.
    let raw_plain_tree = format!(
        "tree {}\nauthor Test <test@example.com> 1730000000 +0000\ncommitter Test <test@example.com> 1730000000 +0000\n\nplain tree\n",
        hex::encode(&t1),
    )
    .into_bytes();
    let raw_plain_tree_oid = gix::objs::compute_hash(
        gix::hash::Kind::Sha1,
        gix::objs::Kind::Commit,
        &raw_plain_tree,
    )
    .unwrap();
    let missing_jj_trees_commit = Commit {
        commit_id: raw_plain_tree_oid.as_bytes().to_vec(),
        change_id: vec![0x48; CHANGE_ID_LENGTH],
        parent_commit_ids: vec![ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![t1.clone(), t2.clone(), t3.clone()],
        description: "plain tree\n".to_string(),
        raw_git_commit: raw_plain_tree,
        ..Default::default()
    };
    let err = export_single_commit(
        &ts_repo,
        raw_plain_tree_oid.as_bytes(),
        &missing_jj_trees_commit,
    )
    .expect_err("3-tree root_tree_id without jj:trees in raw_git_commit must fail closed");
    assert!(
        err.to_string()
            .contains("raw_git_commit tree header does not match"),
        "unexpected error: {err}"
    );

    // End-to-end `GitExporter::run` must fail closed BEFORE issuing any `ReadTree` RPC
    // for `fake_raw_tree`.
    for (label, commit_id, bad_commit, expected_msg) in [
        (
            "bad raw_git_commit SHA-1",
            canonical_oid.as_bytes().to_vec(),
            bad_sha_commit,
            "raw_git_commit SHA-1 mismatch",
        ),
        (
            "mismatched raw_git_commit parents",
            raw_parent_a_oid.as_bytes().to_vec(),
            mismatched_parents_commit,
            "raw_git_commit parents do not match",
        ),
        (
            "mismatched raw_git_commit tree",
            raw_mismatched_tree_oid.as_bytes().to_vec(),
            mismatched_tree_commit,
            "raw_git_commit tree header does not match",
        ),
        (
            "mismatched raw_git_commit jj:trees",
            raw_mismatched_jj_trees_oid.as_bytes().to_vec(),
            mismatched_jj_trees_commit,
            "raw_git_commit jj:trees does not match",
        ),
        (
            "malformed raw_git_commit jj:trees",
            raw_malformed_jj_trees_oid.as_bytes().to_vec(),
            malformed_jj_trees_commit,
            "jj:trees",
        ),
        (
            "missing raw_git_commit jj:trees",
            raw_plain_tree_oid.as_bytes().to_vec(),
            missing_jj_trees_commit,
            "raw_git_commit tree header does not match",
        ),
    ] {
        let mut commits = HashMap::from([(commit_id.clone(), bad_commit)]);
        // Also include parent_b so the commit graph walk reaches `bad_commit`.
        commits.insert(
            parent_b.clone(),
            Commit {
                commit_id: parent_b.clone(),
                change_id: vec![0x55; CHANGE_ID_LENGTH],
                parent_commit_ids: vec![ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: vec![EMPTY_TREE_ID_BYTES.to_vec()],
                description: "parent b\n".to_string(),
                ..Default::default()
            },
        );
        // Make parent_b's real commit SHA-1 not fail before child if parent_b is exported first:
        // use a single-commit graph with `parent_commit_ids = [ROOT_COMMIT_ID_BYTES]` for the
        // parent mismatch case (raw has parent_a, proto has ROOT).
        let (test_cid, test_commits) = if label.contains("parents") {
            let mut c = commits.remove(&commit_id).unwrap();
            c.parent_commit_ids = vec![ROOT_COMMIT_ID_BYTES.to_vec()];
            (commit_id, HashMap::from([(c.commit_id.clone(), c)]))
        } else {
            (commit_id, commits)
        };

        let read_tree_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let backend = ValidationMockBackendService {
            commits: test_commits,
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
                head_ids: vec![test_cid.clone()],
                wc_commit_ids: HashMap::new(),
                local_bookmarks: HashMap::from([(
                    "main".to_string(),
                    RefTarget {
                        removes: Vec::new(),
                        adds: vec![RefTargetTerm {
                            commit_id: test_cid,
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

        let dst = dir.path().join(format!(
            "fail_closed_{}.git",
            label.replace(|c: char| !c.is_ascii_alphanumeric(), "_")
        ));
        let err = match GitExporter::new(GitExportOptions {
            git_dir: dst.clone(),
            server_url: url,
            repo_id: "repo".to_string(),
            project_id: None,
            bare: true,
        })
        .run()
        .await
        {
            Ok(_) => panic!("{label} must fail export"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains(expected_msg),
            "{label}: unexpected error: {err}"
        );
        assert_eq!(
            read_tree_calls.load(Ordering::SeqCst),
            0,
            "{label}: must fail closed before fetching raw_tree_id"
        );
        assert!(!dst.exists(), "{label}: failed export must roll back dst");
    }
}

#[tokio::test]
async fn export_git_reports_root_commit_bookmarks_in_skipped_bookmarks() {
    let dir = tempfile::tempdir().unwrap();

    let good_commit = Commit {
        commit_id: Vec::new(),
        change_id: vec![0x77; CHANGE_ID_LENGTH],
        parent_commit_ids: vec![ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![EMPTY_TREE_ID_BYTES.to_vec()],
        description: "good commit\n".to_string(),
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
    let raw_good = serialize_proto_commit_to_git(&good_commit, true).unwrap();
    let good_oid =
        gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Commit, &raw_good).unwrap();
    let good_cid = good_oid.as_bytes().to_vec();
    let mut stored_good = good_commit;
    stored_good.commit_id = good_cid.clone();

    let initial_view = cc_common::op_store::View {
        head_ids: vec![good_cid.clone()],
        wc_commit_ids: HashMap::new(),
        local_bookmarks: HashMap::from([
            (
                "good".to_string(),
                RefTarget {
                    removes: Vec::new(),
                    adds: vec![RefTargetTerm {
                        commit_id: good_cid.clone(),
                    }],
                },
            ),
            (
                "at-root".to_string(),
                RefTarget {
                    removes: Vec::new(),
                    adds: vec![RefTargetTerm {
                        commit_id: ROOT_COMMIT_ID_BYTES.to_vec(),
                    }],
                },
            ),
        ]),
        remote_bookmarks: HashMap::new(),
    };

    let op_store = MockOpStoreService {
        initial_view,
        published_view: Arc::new(std::sync::Mutex::new(None)),
    };
    let backend = MockBackendService {
        commits: HashMap::from([(good_cid.clone(), stored_good)]),
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

    let dst = dir.path().join("root_bm_export.git");
    let report = GitExporter::new(GitExportOptions {
        git_dir: dst.clone(),
        server_url: url,
        repo_id: "repo".to_string(),
        project_id: None,
        bare: true,
    })
    .run()
    .await
    .expect("export with root-targeting bookmark must succeed");
    assert_eq!(report.bookmarks_exported, 1);
    assert_eq!(report.skipped_bookmarks, vec!["at-root".to_string()]);
    assert!(report.skipped_conflicted_bookmarks.is_empty());

    let exported_repo = gix::open_opts(&dst, gix::open::Options::isolated()).unwrap();
    assert!(
        exported_repo
            .try_find_reference("refs/heads/good")
            .unwrap()
            .is_some()
    );
    assert!(
        exported_repo
            .try_find_reference("refs/heads/at-root")
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn export_tree_recursive_rejects_oversized_read_file_stream() {
    let dir = tempfile::tempdir().unwrap();
    let repo = gix::init_bare(dir.path()).unwrap();

    let ok_bytes = b"0123456789abcdef".to_vec(); // 16 bytes (exact limit)
    let ok_oid =
        gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Blob, &ok_bytes).unwrap();
    let multi_chunk_bytes = b"0123456789abcdefX".to_vec(); // 17 bytes (> 16 across 3 chunks)
    let multi_oid = gix::objs::compute_hash(
        gix::hash::Kind::Sha1,
        gix::objs::Kind::Blob,
        &multi_chunk_bytes,
    )
    .unwrap();
    let single_big_bytes = b"0123456789abcdef0123456789".to_vec(); // 26 bytes in 1 chunk
    let single_big_oid = gix::objs::compute_hash(
        gix::hash::Kind::Sha1,
        gix::objs::Kind::Blob,
        &single_big_bytes,
    )
    .unwrap();

    let make_tree = |name: &str, oid: gix::hash::ObjectId| {
        let gix_tree = gix::objs::Tree {
            entries: vec![gix::objs::tree::Entry {
                mode: gix::objs::tree::EntryKind::Blob.into(),
                filename: name.into(),
                oid,
            }],
        };
        let mut buf = Vec::new();
        gix_tree.write_to(&mut buf).unwrap();
        let tid = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Tree, &buf)
            .unwrap()
            .as_bytes()
            .to_vec();
        let resp = cc_common::backend::ReadTreeResponse {
            tree_id: tid.clone(),
            entries: vec![TreeEntry {
                name: name.to_string(),
                value: Some(TreeValue {
                    value: Some(tree_value::Value::File(File {
                        id: oid.as_bytes().to_vec(),
                        executable: false,
                        copy_id: Vec::new(),
                        size_bytes: None,
                    })),
                }),
            }],
            next_page_token: String::new(),
        };
        (tid, MockTreeBehavior::Pages(vec![resp]))
    };

    let (ok_tid, ok_tree) = make_tree("ok.bin", ok_oid);
    let (multi_tid, multi_tree) = make_tree("multi.bin", multi_oid);
    let (single_tid, single_tree) = make_tree("single.bin", single_big_oid);

    let backend = ValidationMockBackendService {
        commits: HashMap::new(),
        trees: HashMap::from([
            (ok_tid.clone(), ok_tree),
            (multi_tid.clone(), multi_tree),
            (single_tid.clone(), single_tree),
        ]),
        files: HashMap::from([(single_big_oid.as_bytes().to_vec(), single_big_bytes)]),
        chunked_files: HashMap::from([
            (
                ok_oid.as_bytes().to_vec(),
                vec![b"01234567".to_vec(), b"89abcdef".to_vec()],
            ),
            (
                multi_oid.as_bytes().to_vec(),
                vec![b"01234567".to_vec(), b"89abcdef".to_vec(), b"X".to_vec()],
            ),
        ]),
        symlinks: HashMap::new(),
        read_commit_calls: Arc::new(std::sync::Mutex::new(HashMap::new())),
        read_tree_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        read_file_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
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
    let mut tree_state = ExportTreeState::default();

    // Exact limit (16 bytes across 2 chunks) succeeds.
    export_tree_recursive_with_max_file_bytes(
        &ts_repo,
        &mut backend_client,
        "proj",
        &ok_tid,
        &mut tree_state,
        16,
    )
    .await
    .expect("file matching exact byte cap must succeed");

    // Multi-chunk overflow (17 > 16 bytes) and single-chunk overflow (26 > 16 bytes) fail
    // without writing blobs into `.git/objects/`.
    for (label, tid, oid) in [
        ("multi-chunk overflow", &multi_tid, multi_oid),
        ("single-chunk overflow", &single_tid, single_big_oid),
    ] {
        let err = export_tree_recursive_with_max_file_bytes(
            &ts_repo,
            &mut backend_client,
            "proj",
            tid,
            &mut tree_state,
            16,
        )
        .await
        .expect_err("oversized ReadFile stream must fail");
        assert!(
            err.to_string().contains("exceeded maximum allowed size"),
            "{label}: unexpected error: {err}"
        );
        assert!(
            ts_repo
                .to_thread_local()
                .try_find_object(oid)
                .unwrap()
                .is_none(),
            "{label}: oversized blob must not be written to target ODB"
        );
        assert!(tree_state.in_progress.is_empty());
    }
}

#[tokio::test]
async fn export_rejects_malformed_commit_structure_without_silent_fallbacks() {
    let dir = tempfile::tempdir().unwrap();
    let repo = gix::init_bare(dir.path()).unwrap();

    let valid_commit = Commit {
        commit_id: Vec::new(),
        change_id: vec![0x61; CHANGE_ID_LENGTH],
        parent_commit_ids: vec![ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![EMPTY_TREE_ID_BYTES.to_vec()],
        description: "empty tree commit\n".to_string(),
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
    let valid_raw = serialize_proto_commit_to_git(&valid_commit, true).unwrap();
    let valid_oid =
        gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Commit, &valid_raw)
            .unwrap();

    // Empty `root_tree_id`: must NOT fall back to `empty_tree` (which would match
    // `valid_oid` and write a commit object referencing an unwritten empty tree).
    let mut empty_trees = valid_commit.clone();
    empty_trees.commit_id = valid_oid.as_bytes().to_vec();
    empty_trees.root_tree_id = Vec::new();

    // Even-length `root_tree_id`.
    let mut even_trees = valid_commit.clone();
    even_trees.commit_id = valid_oid.as_bytes().to_vec();
    even_trees.root_tree_id = vec![EMPTY_TREE_ID_BYTES.to_vec(), vec![0x11; TREE_ID_LENGTH]];

    // Invalid-length `root_tree_id` entry.
    let mut short_tree = valid_commit.clone();
    short_tree.commit_id = valid_oid.as_bytes().to_vec();
    short_tree.root_tree_id = vec![vec![0x11; 4]];

    // Invalid-length `parent_commit_ids` entry (must NOT be silently dropped).
    let mut short_parent = valid_commit.clone();
    short_parent.commit_id = valid_oid.as_bytes().to_vec();
    short_parent.parent_commit_ids = vec![vec![0x22; 4]];

    // Empty `parent_commit_ids`.
    let mut empty_parents = valid_commit.clone();
    empty_parents.commit_id = valid_oid.as_bytes().to_vec();
    empty_parents.parent_commit_ids = Vec::new();

    // `ROOT_COMMIT_ID_BYTES` mixed with another parent.
    let mut root_merge = valid_commit.clone();
    root_merge.commit_id = valid_oid.as_bytes().to_vec();
    root_merge.parent_commit_ids =
        vec![ROOT_COMMIT_ID_BYTES.to_vec(), vec![0x33; COMMIT_ID_LENGTH]];

    // Empty and wrong-length `change_id`.
    let mut empty_change_id = valid_commit.clone();
    empty_change_id.commit_id = valid_oid.as_bytes().to_vec();
    empty_change_id.change_id = Vec::new();

    let mut short_change_id = valid_commit.clone();
    short_change_id.commit_id = valid_oid.as_bytes().to_vec();
    short_change_id.change_id = vec![0x61; 4];

    let ts_repo = repo.into_sync();
    for (label, bad_commit) in [
        ("empty root_tree_id", empty_trees.clone()),
        ("even root_tree_id", even_trees),
        ("short root_tree_id", short_tree),
        ("short parent_commit_id", short_parent),
        ("empty parent_commit_ids", empty_parents),
        ("root mixed with parent", root_merge),
        ("empty change_id", empty_change_id),
        ("short change_id", short_change_id),
    ] {
        assert!(
            serialize_proto_commit_to_git(&bad_commit, true).is_err(),
            "{label}: serialize_proto_commit_to_git(true) must error"
        );
        assert!(
            serialize_proto_commit_to_git(&bad_commit, false).is_err(),
            "{label}: serialize_proto_commit_to_git(false) must error"
        );
        assert!(
            export_single_commit(&ts_repo, valid_oid.as_bytes(), &bad_commit).is_err(),
            "{label}: export_single_commit must error"
        );
    }

    // End-to-end `GitExporter::run` with `empty_trees` must fail before creating dst.
    let op_store = MockOpStoreService {
        initial_view: cc_common::op_store::View {
            head_ids: vec![valid_oid.as_bytes().to_vec()],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::from([(
                "main".to_string(),
                RefTarget {
                    removes: Vec::new(),
                    adds: vec![RefTargetTerm {
                        commit_id: valid_oid.as_bytes().to_vec(),
                    }],
                },
            )]),
            remote_bookmarks: HashMap::new(),
        },
        published_view: Arc::new(std::sync::Mutex::new(None)),
    };
    let backend = MockBackendService {
        commits: HashMap::from([(valid_oid.as_bytes().to_vec(), empty_trees)]),
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
    let dst = dir.path().join("empty_trees_export.git");
    let err = GitExporter::new(GitExportOptions {
        git_dir: dst.clone(),
        server_url: url,
        repo_id: "repo".to_string(),
        project_id: None,
        bare: true,
    })
    .run()
    .await
    .expect_err("empty root_tree_id must fail GitExporter::run");
    assert!(
        err.to_string().contains("root_tree_id"),
        "unexpected error: {err}"
    );
    assert!(!dst.exists());
}

#[tokio::test]
async fn export_rejects_existing_repo_df_ref_conflict_before_fetching_objects() {
    let dir = tempfile::tempdir().unwrap();

    let make_tree_commit = |tree_id: Vec<u8>| {
        let mut c = Commit {
            commit_id: Vec::new(),
            change_id: vec![0x81; CHANGE_ID_LENGTH],
            parent_commit_ids: vec![ROOT_COMMIT_ID_BYTES.to_vec()],
            root_tree_id: vec![tree_id],
            description: "df conflict test\n".to_string(),
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
        let raw = serialize_proto_commit_to_git(&c, true).unwrap();
        c.commit_id = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Commit, &raw)
            .unwrap()
            .as_bytes()
            .to_vec();
        c
    };

    // Use a non-empty tree ID so any premature `ReadTree` RPC increments `read_tree_calls`.
    let fake_tree_id = vec![0xef; TREE_ID_LENGTH];
    let commit = make_tree_commit(fake_tree_id);

    for (label, bare, existing_ref, exported_bm) in [
        (
            "bare existing nested vs exported parent",
            true,
            "refs/heads/foo/bar",
            "foo",
        ),
        (
            "bare existing parent vs exported nested",
            true,
            "refs/heads/foo",
            "foo/bar",
        ),
        (
            "non-bare existing nested vs exported parent",
            false,
            "refs/heads/foo/bar",
            "foo",
        ),
    ] {
        let repo_dir = dir.path().join(label.replace(' ', "_"));
        let repo = if bare {
            gix::init_bare(&repo_dir).unwrap()
        } else {
            gix::init(&repo_dir).unwrap()
        };
        let empty_tree = repo.objects.write_buf(gix::objs::Kind::Tree, &[]).unwrap();
        let existing_c = gix::objs::Commit {
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
            message: "existing\n".into(),
        };
        let mut buf = Vec::new();
        existing_c.write_to(&mut buf).unwrap();
        let existing_oid = repo
            .objects
            .write_buf(gix::objs::Kind::Commit, &buf)
            .unwrap();
        write_git_refs(
            &repo,
            vec![
                (
                    existing_ref.try_into().unwrap(),
                    gix::refs::Target::Object(existing_oid),
                ),
                // Detach HEAD on non-bare repo so checked-out branch guard doesn't mask D/F check.
                (
                    "HEAD".try_into().unwrap(),
                    gix::refs::Target::Object(existing_oid),
                ),
            ],
        )
        .unwrap();

        let read_tree_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let read_file_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let backend = ValidationMockBackendService {
            commits: HashMap::from([(commit.commit_id.clone(), commit.clone())]),
            trees: HashMap::new(),
            files: HashMap::new(),
            chunked_files: HashMap::new(),
            symlinks: HashMap::new(),
            read_commit_calls: Arc::new(std::sync::Mutex::new(HashMap::new())),
            read_tree_calls: Arc::clone(&read_tree_calls),
            read_file_calls: Arc::clone(&read_file_calls),
        };
        let op_store = MockOpStoreService {
            initial_view: cc_common::op_store::View {
                head_ids: vec![commit.commit_id.clone()],
                wc_commit_ids: HashMap::new(),
                local_bookmarks: HashMap::from([(
                    exported_bm.to_string(),
                    RefTarget {
                        removes: Vec::new(),
                        adds: vec![RefTargetTerm {
                            commit_id: commit.commit_id.clone(),
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
            git_dir: repo_dir,
            server_url: url,
            repo_id: "repo".to_string(),
            project_id: None,
            bare,
        })
        .run()
        .await
        .expect_err("existing-repo D/F ref conflict must fail");
        assert!(
            err.to_string().contains("directory/file conflict"),
            "{label}: unexpected error: {err}"
        );
        assert_eq!(
            read_tree_calls.load(Ordering::SeqCst),
            0,
            "{label}: must fail before any ReadTree RPC"
        );
        assert_eq!(
            read_file_calls.load(Ordering::SeqCst),
            0,
            "{label}: must fail before any ReadFile RPC"
        );
    }
}

#[tokio::test]
async fn export_deduplicates_read_commit_for_working_copy_heads() {
    let dir = tempfile::tempdir().unwrap();

    let make_commit = |parents: Vec<Vec<u8>>, desc: &str| {
        let mut c = Commit {
            commit_id: Vec::new(),
            change_id: vec![0x91; CHANGE_ID_LENGTH],
            parent_commit_ids: parents,
            root_tree_id: vec![EMPTY_TREE_ID_BYTES.to_vec()],
            description: desc.to_string(),
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
        let raw = serialize_proto_commit_to_git(&c, true).unwrap();
        c.commit_id = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Commit, &raw)
            .unwrap()
            .as_bytes()
            .to_vec();
        c
    };

    let base = make_commit(vec![ROOT_COMMIT_ID_BYTES.to_vec()], "base\n");
    let empty_wc = make_commit(vec![base.commit_id.clone()], "");
    let described_wc = make_commit(vec![base.commit_id.clone()], "wip description\n");

    let read_commit_calls = Arc::new(std::sync::Mutex::new(HashMap::new()));
    let backend = ValidationMockBackendService {
        commits: HashMap::from([
            (base.commit_id.clone(), base.clone()),
            (empty_wc.commit_id.clone(), empty_wc.clone()),
            (described_wc.commit_id.clone(), described_wc.clone()),
        ]),
        trees: HashMap::new(),
        files: HashMap::new(),
        chunked_files: HashMap::new(),
        symlinks: HashMap::new(),
        read_commit_calls: Arc::clone(&read_commit_calls),
        read_tree_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        read_file_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    };
    let op_store = MockOpStoreService {
        initial_view: cc_common::op_store::View {
            head_ids: vec![empty_wc.commit_id.clone(), described_wc.commit_id.clone()],
            wc_commit_ids: HashMap::from([
                ("ws1".to_string(), empty_wc.commit_id.clone()),
                ("ws2".to_string(), described_wc.commit_id.clone()),
            ]),
            local_bookmarks: HashMap::new(),
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

    let dst = dir.path().join("wc_dedup.git");
    let report = GitExporter::new(GitExportOptions {
        git_dir: dst,
        server_url: url,
        repo_id: "repo".to_string(),
        project_id: None,
        bare: true,
    })
    .run()
    .await
    .expect("export must succeed");
    assert_eq!(report.commits_exported, 2);

    let calls = read_commit_calls.lock().unwrap();
    assert_eq!(calls.get(&base.commit_id), Some(&1));
    assert_eq!(calls.get(&empty_wc.commit_id), Some(&1));
    assert_eq!(calls.get(&described_wc.commit_id), Some(&1));
}

#[tokio::test]
async fn export_validates_symlink_even_when_regular_file_shares_sha1() {
    let dir = tempfile::tempdir().unwrap();
    let repo = gix::init_bare(dir.path()).unwrap();

    // Regular binary file containing a NUL byte followed by a SymlinkId with the exact same
    // SHA-1 must NOT bypass ReadSymlink or NUL-byte validation via `written_blobs`.
    let nul_bytes = b"binary\0payload".to_vec();
    let nul_oid =
        gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Blob, &nul_bytes).unwrap();
    let nul_tree_gix = gix::objs::Tree {
        entries: vec![
            gix::objs::tree::Entry {
                mode: gix::objs::tree::EntryKind::Blob.into(),
                filename: "a_file.bin".into(),
                oid: nul_oid,
            },
            gix::objs::tree::Entry {
                mode: gix::objs::tree::EntryKind::Link.into(),
                filename: "b_link".into(),
                oid: nul_oid,
            },
        ],
    };
    let mut nul_tree_buf = Vec::new();
    nul_tree_gix.write_to(&mut nul_tree_buf).unwrap();
    let nul_tree_id =
        gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Tree, &nul_tree_buf)
            .unwrap()
            .as_bytes()
            .to_vec();

    // Regular file followed by a SymlinkId of the same SHA-1 when ReadSymlink returns NotFound
    // must also fail closed rather than reusing `written_blobs`.
    let missing_sym_bytes = b"no-symlink-record".to_vec();
    let missing_sym_oid = gix::objs::compute_hash(
        gix::hash::Kind::Sha1,
        gix::objs::Kind::Blob,
        &missing_sym_bytes,
    )
    .unwrap();
    let missing_sym_tree_id = vec![0x62; TREE_ID_LENGTH];

    // Regular file and valid symlink sharing the same SHA-1 both succeed and deduplicate
    // `written_blobs` count to 1.
    let shared_bytes = b"valid-target".to_vec();
    let shared_oid =
        gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Blob, &shared_bytes)
            .unwrap();
    let shared_tree_gix = gix::objs::Tree {
        entries: vec![
            gix::objs::tree::Entry {
                mode: gix::objs::tree::EntryKind::Blob.into(),
                filename: "a_file.txt".into(),
                oid: shared_oid,
            },
            gix::objs::tree::Entry {
                mode: gix::objs::tree::EntryKind::Link.into(),
                filename: "b_link".into(),
                oid: shared_oid,
            },
        ],
    };
    let mut shared_tree_buf = Vec::new();
    shared_tree_gix.write_to(&mut shared_tree_buf).unwrap();
    let shared_tree_id = gix::objs::compute_hash(
        gix::hash::Kind::Sha1,
        gix::objs::Kind::Tree,
        &shared_tree_buf,
    )
    .unwrap()
    .as_bytes()
    .to_vec();

    let file_and_link_entries = |oid: gix::hash::ObjectId| {
        vec![
            TreeEntry {
                name: "a_file.bin".to_string(),
                value: Some(TreeValue {
                    value: Some(tree_value::Value::File(File {
                        id: oid.as_bytes().to_vec(),
                        executable: false,
                        copy_id: Vec::new(),
                        size_bytes: None,
                    })),
                }),
            },
            TreeEntry {
                name: "b_link".to_string(),
                value: Some(TreeValue {
                    value: Some(tree_value::Value::SymlinkId(oid.as_bytes().to_vec())),
                }),
            },
        ]
    };

    let mut trees = HashMap::new();
    trees.insert(
        nul_tree_id.clone(),
        MockTreeBehavior::Pages(vec![cc_common::backend::ReadTreeResponse {
            tree_id: nul_tree_id.clone(),
            entries: file_and_link_entries(nul_oid),
            next_page_token: String::new(),
        }]),
    );
    trees.insert(
        missing_sym_tree_id.clone(),
        MockTreeBehavior::Pages(vec![cc_common::backend::ReadTreeResponse {
            tree_id: missing_sym_tree_id.clone(),
            entries: file_and_link_entries(missing_sym_oid),
            next_page_token: String::new(),
        }]),
    );
    trees.insert(
        shared_tree_id.clone(),
        MockTreeBehavior::Pages(vec![cc_common::backend::ReadTreeResponse {
            tree_id: shared_tree_id.clone(),
            entries: vec![
                TreeEntry {
                    name: "a_file.txt".to_string(),
                    value: Some(TreeValue {
                        value: Some(tree_value::Value::File(File {
                            id: shared_oid.as_bytes().to_vec(),
                            executable: false,
                            copy_id: Vec::new(),
                            size_bytes: None,
                        })),
                    }),
                },
                TreeEntry {
                    name: "b_link".to_string(),
                    value: Some(TreeValue {
                        value: Some(tree_value::Value::SymlinkId(shared_oid.as_bytes().to_vec())),
                    }),
                },
            ],
            next_page_token: String::new(),
        }]),
    );

    let backend = ValidationMockBackendService {
        commits: HashMap::new(),
        trees,
        files: HashMap::from([
            (nul_oid.as_bytes().to_vec(), nul_bytes),
            (missing_sym_oid.as_bytes().to_vec(), missing_sym_bytes),
            (shared_oid.as_bytes().to_vec(), shared_bytes),
        ]),
        chunked_files: HashMap::new(),
        symlinks: HashMap::from([
            (nul_oid.as_bytes().to_vec(), "binary\0payload".to_string()),
            (shared_oid.as_bytes().to_vec(), "valid-target".to_string()),
        ]),
        read_commit_calls: Arc::new(std::sync::Mutex::new(HashMap::new())),
        read_tree_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        read_file_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
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

    let mut state1 = ExportTreeState::default();
    let err = export_tree_recursive(
        &ts_repo,
        &mut backend_client,
        "proj",
        &nul_tree_id,
        &mut state1,
    )
    .await
    .expect_err("SymlinkId sharing SHA-1 with NUL binary file must fail closed");
    assert!(
        err.to_string().contains("NUL byte"),
        "unexpected error: {err}"
    );

    let mut state2 = ExportTreeState::default();
    let err = export_tree_recursive(
        &ts_repo,
        &mut backend_client,
        "proj",
        &missing_sym_tree_id,
        &mut state2,
    )
    .await
    .expect_err("SymlinkId missing from ReadSymlink must fail even when file blob exists");
    assert!(
        err.to_string().contains("missing symlink"),
        "unexpected error: {err}"
    );

    let mut state3 = ExportTreeState::default();
    export_tree_recursive(
        &ts_repo,
        &mut backend_client,
        "proj",
        &shared_tree_id,
        &mut state3,
    )
    .await
    .expect("valid file and symlink sharing SHA-1 must succeed");
    assert_eq!(state3.written_blobs.len(), 1);
    assert_eq!(state3.validated_symlinks.len(), 1);
}

#[tokio::test]
async fn export_verifies_blob_symlink_and_tree_sha1_before_writing_odb() {
    let dir = tempfile::tempdir().unwrap();
    let repo = gix::init_bare(dir.path()).unwrap();

    // Mismatched blob payload.
    let expected_blob_oid = gix::objs::compute_hash(
        gix::hash::Kind::Sha1,
        gix::objs::Kind::Blob,
        b"expected blob",
    )
    .unwrap();
    let tampered_blob = b"tampered blob".to_vec();
    let tampered_blob_oid =
        gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Blob, &tampered_blob)
            .unwrap();
    let tid_bad_blob = vec![0x71; TREE_ID_LENGTH];

    // Mismatched symlink payload.
    let expected_sym_oid = gix::objs::compute_hash(
        gix::hash::Kind::Sha1,
        gix::objs::Kind::Blob,
        b"expected-link",
    )
    .unwrap();
    let tampered_sym = "tampered-link".to_string();
    let tampered_sym_oid = gix::objs::compute_hash(
        gix::hash::Kind::Sha1,
        gix::objs::Kind::Blob,
        tampered_sym.as_bytes(),
    )
    .unwrap();
    let tid_bad_sym = vec![0x72; TREE_ID_LENGTH];

    // Mismatched tree payload (entry points to valid empty subtree, so only the tree hash mismatches).
    let expected_tree_oid =
        gix::hash::ObjectId::from_hex(b"7373737373737373737373737373737373737373").unwrap();
    let empty_tree_oid = gix::hash::ObjectId::try_from(EMPTY_TREE_ID_BYTES.as_slice()).unwrap();
    let actual_gix_tree = gix::objs::Tree {
        entries: vec![gix::objs::tree::Entry {
            mode: gix::objs::tree::EntryKind::Tree.into(),
            filename: "sub".into(),
            oid: empty_tree_oid,
        }],
    };
    let mut actual_tree_buf = Vec::new();
    actual_gix_tree.write_to(&mut actual_tree_buf).unwrap();
    let actual_tree_oid = gix::objs::compute_hash(
        gix::hash::Kind::Sha1,
        gix::objs::Kind::Tree,
        &actual_tree_buf,
    )
    .unwrap();

    let mut trees = HashMap::new();
    trees.insert(
        tid_bad_blob.clone(),
        MockTreeBehavior::Pages(vec![cc_common::backend::ReadTreeResponse {
            tree_id: tid_bad_blob.clone(),
            entries: vec![TreeEntry {
                name: "corrupt.bin".to_string(),
                value: Some(TreeValue {
                    value: Some(tree_value::Value::File(File {
                        id: expected_blob_oid.as_bytes().to_vec(),
                        executable: false,
                        copy_id: Vec::new(),
                        size_bytes: None,
                    })),
                }),
            }],
            next_page_token: String::new(),
        }]),
    );
    trees.insert(
        tid_bad_sym.clone(),
        MockTreeBehavior::Pages(vec![cc_common::backend::ReadTreeResponse {
            tree_id: tid_bad_sym.clone(),
            entries: vec![TreeEntry {
                name: "corrupt.link".to_string(),
                value: Some(TreeValue {
                    value: Some(tree_value::Value::SymlinkId(
                        expected_sym_oid.as_bytes().to_vec(),
                    )),
                }),
            }],
            next_page_token: String::new(),
        }]),
    );
    trees.insert(
        expected_tree_oid.as_bytes().to_vec(),
        MockTreeBehavior::Pages(vec![cc_common::backend::ReadTreeResponse {
            tree_id: expected_tree_oid.as_bytes().to_vec(),
            entries: vec![TreeEntry {
                name: "sub".to_string(),
                value: Some(TreeValue {
                    value: Some(tree_value::Value::TreeId(EMPTY_TREE_ID_BYTES.to_vec())),
                }),
            }],
            next_page_token: String::new(),
        }]),
    );

    let backend = ValidationMockBackendService {
        commits: HashMap::new(),
        trees,
        files: HashMap::from([(expected_blob_oid.as_bytes().to_vec(), tampered_blob)]),
        chunked_files: HashMap::new(),
        symlinks: HashMap::from([(expected_sym_oid.as_bytes().to_vec(), tampered_sym)]),
        read_commit_calls: Arc::new(std::sync::Mutex::new(HashMap::new())),
        read_tree_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        read_file_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
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
    let mut state = ExportTreeState::default();

    let blob_err = export_tree_recursive(
        &ts_repo,
        &mut backend_client,
        "proj",
        &tid_bad_blob,
        &mut state,
    )
    .await
    .expect_err("mismatched blob must fail");
    assert!(
        blob_err.to_string().contains("blob SHA-1 mismatch"),
        "unexpected error: {blob_err}"
    );
    {
        let r = ts_repo.to_thread_local();
        assert!(r.try_find_object(tampered_blob_oid).unwrap().is_none());
        assert!(r.try_find_object(expected_blob_oid).unwrap().is_none());
    }

    let sym_err = export_tree_recursive(
        &ts_repo,
        &mut backend_client,
        "proj",
        &tid_bad_sym,
        &mut state,
    )
    .await
    .expect_err("mismatched symlink must fail");
    assert!(
        sym_err.to_string().contains("symlink SHA-1 mismatch"),
        "unexpected error: {sym_err}"
    );
    {
        let r = ts_repo.to_thread_local();
        assert!(r.try_find_object(tampered_sym_oid).unwrap().is_none());
        assert!(r.try_find_object(expected_sym_oid).unwrap().is_none());
    }

    let tree_err = export_tree_recursive(
        &ts_repo,
        &mut backend_client,
        "proj",
        expected_tree_oid.as_bytes(),
        &mut state,
    )
    .await
    .expect_err("mismatched tree must fail");
    assert!(
        tree_err.to_string().contains("tree SHA-1 mismatch"),
        "unexpected error: {tree_err}"
    );
    {
        let r = ts_repo.to_thread_local();
        assert!(r.try_find_object(actual_tree_oid).unwrap().is_none());
        assert!(r.try_find_object(expected_tree_oid).unwrap().is_none());
    }
}

#[test]
fn export_verifies_raw_git_commit_metadata_and_conflict_labels_parity() {
    let dir = tempfile::tempdir().unwrap();
    let ts_repo = gix::init_bare(dir.path()).unwrap().into_sync();

    let t1 = vec![0xb1; TREE_ID_LENGTH];
    let t2 = vec![0xb2; TREE_ID_LENGTH];
    let t3 = vec![0xb3; TREE_ID_LENGTH];
    let sig_bytes = b"-----BEGIN PGP SIGNATURE-----\nfake\n-----END PGP SIGNATURE-----\n".to_vec();

    let valid_conflicted = Commit {
        commit_id: Vec::new(),
        change_id: vec![0x55; CHANGE_ID_LENGTH],
        parent_commit_ids: vec![ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![t1.clone(), t2.clone(), t3.clone()],
        description: "conflicted signed commit\n".to_string(),
        author: Some(Signature {
            name: "Alice".to_string(),
            email: "alice@example.com".to_string(),
            timestamp: Some(Timestamp {
                millis_since_epoch: 1_730_000_000_000,
                tz_offset: 60,
            }),
        }),
        committer: Some(Signature {
            name: "Bob".to_string(),
            email: "bob@example.com".to_string(),
            timestamp: Some(Timestamp {
                millis_since_epoch: 1_730_000_100_000,
                tz_offset: -120,
            }),
        }),
        conflict_labels: vec![
            "side a".to_string(),
            "base".to_string(),
            "side b".to_string(),
        ],
        secure_sig: Some(SecureSig {
            data: Vec::new(),
            sig: sig_bytes.clone(),
        }),
        ..Default::default()
    };
    let raw_bytes = serialize_proto_commit_to_git(&valid_conflicted, true).unwrap();
    let oid = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Commit, &raw_bytes)
        .unwrap();
    let mut valid_with_raw = valid_conflicted.clone();
    valid_with_raw.commit_id = oid.as_bytes().to_vec();
    valid_with_raw.raw_git_commit = raw_bytes;

    // Matching raw_git_commit with all metadata, conflict labels, change-id, and gpgsig passes.
    export_single_commit(&ts_repo, oid.as_bytes(), &valid_with_raw)
        .expect("matching raw_git_commit must succeed");

    // Tampering with description fails.
    let mut bad_desc = valid_with_raw.clone();
    bad_desc.description = "tampered\n".to_string();
    assert!(
        export_single_commit(&ts_repo, oid.as_bytes(), &bad_desc)
            .unwrap_err()
            .to_string()
            .contains("message does not match")
    );

    // Tampering with author timestamp fails.
    let mut bad_author_ts = valid_with_raw.clone();
    bad_author_ts
        .author
        .as_mut()
        .unwrap()
        .timestamp
        .as_mut()
        .unwrap()
        .millis_since_epoch += 1_000;
    assert!(
        export_single_commit(&ts_repo, oid.as_bytes(), &bad_author_ts)
            .unwrap_err()
            .to_string()
            .contains("author does not match")
    );

    // Tampering with committer tz_offset fails.
    let mut bad_committer_tz = valid_with_raw.clone();
    bad_committer_tz
        .committer
        .as_mut()
        .unwrap()
        .timestamp
        .as_mut()
        .unwrap()
        .tz_offset = 0;
    assert!(
        export_single_commit(&ts_repo, oid.as_bytes(), &bad_committer_tz)
            .unwrap_err()
            .to_string()
            .contains("committer does not match")
    );

    // Tampering with change_id fails.
    let mut bad_change_id = valid_with_raw.clone();
    bad_change_id.change_id = vec![0x99; CHANGE_ID_LENGTH];
    assert!(
        export_single_commit(&ts_repo, oid.as_bytes(), &bad_change_id)
            .unwrap_err()
            .to_string()
            .contains("change-id does not match")
    );

    // Tampering with conflict_labels fails.
    let mut bad_labels = valid_with_raw.clone();
    bad_labels.conflict_labels[0] = "other".to_string();
    assert!(
        export_single_commit(&ts_repo, oid.as_bytes(), &bad_labels)
            .unwrap_err()
            .to_string()
            .contains("jj:conflict-labels does not match")
    );

    // Dropping or tampering with secure_sig fails.
    let mut missing_sig = valid_with_raw.clone();
    missing_sig.secure_sig = None;
    assert!(
        export_single_commit(&ts_repo, oid.as_bytes(), &missing_sig)
            .unwrap_err()
            .to_string()
            .contains("gpgsig")
    );
    let mut wrong_sig = valid_with_raw.clone();
    wrong_sig.secure_sig = Some(SecureSig {
        data: Vec::new(),
        sig: b"different".to_vec(),
    });
    assert!(
        export_single_commit(&ts_repo, oid.as_bytes(), &wrong_sig)
            .unwrap_err()
            .to_string()
            .contains("gpgsig does not match")
    );

    // Invalid `conflict_labels` arity, single-tree labels, and newlines are rejected.
    let single_tree_commit = Commit {
        commit_id: Vec::new(),
        change_id: vec![0x66; CHANGE_ID_LENGTH],
        parent_commit_ids: vec![ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![EMPTY_TREE_ID_BYTES.to_vec()],
        description: "single tree\n".to_string(),
        ..Default::default()
    };
    let mut single_tree_with_labels = single_tree_commit.clone();
    single_tree_with_labels.conflict_labels = vec!["a".to_string(), "b".to_string()];
    assert!(serialize_proto_commit_to_git(&single_tree_with_labels, true).is_err());

    let mut single_tree_with_one_label = single_tree_commit.clone();
    single_tree_with_one_label.conflict_labels = vec!["only".to_string()];
    assert!(serialize_proto_commit_to_git(&single_tree_with_one_label, true).is_err());

    let mut newline_label = valid_conflicted.clone();
    newline_label.conflict_labels = vec!["a\nb".to_string(), "base".to_string(), "c".to_string()];
    assert!(
        serialize_proto_commit_to_git(&newline_label, true)
            .unwrap_err()
            .to_string()
            .contains("newlines")
    );

    let mut wrong_arity = valid_conflicted;
    wrong_arity.conflict_labels = vec!["a".to_string(), "b".to_string()];
    assert!(serialize_proto_commit_to_git(&wrong_arity, true).is_err());

    // Legacy single-empty-label `[""]` on a 1-tree commit succeeds without emitting `jj:conflict-labels`.
    let mut legacy_unlabeled = single_tree_commit;
    legacy_unlabeled.conflict_labels = vec![String::new()];
    let serialized = serialize_proto_commit_to_git(&legacy_unlabeled, true).unwrap();
    let parsed = gix::objs::CommitRef::from_bytes(&serialized, gix::hash::Kind::Sha1).unwrap();
    assert!(parsed.extra_headers().find("jj:conflict-labels").is_none());
}

#[tokio::test]
async fn export_preflights_non_raw_commits_before_init_and_tree_fetches() {
    let dir = tempfile::tempdir().unwrap();
    let fake_tree_id = vec![0xf1; TREE_ID_LENGTH];

    // Non-raw commit whose SHA-1 does not match `commit_id`.
    let mismatched_cid = vec![0xe1; COMMIT_ID_LENGTH];
    let mismatched_commit = Commit {
        commit_id: mismatched_cid.clone(),
        change_id: vec![0x71; CHANGE_ID_LENGTH],
        parent_commit_ids: vec![ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![fake_tree_id.clone()],
        description: "corrupt non-raw commit\n".to_string(),
        ..Default::default()
    };

    // Non-raw commit with an out-of-range timezone offset.
    let bad_tz_cid = vec![0xe2; COMMIT_ID_LENGTH];
    let bad_tz_commit = Commit {
        commit_id: bad_tz_cid.clone(),
        change_id: vec![0x72; CHANGE_ID_LENGTH],
        parent_commit_ids: vec![ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![fake_tree_id],
        description: "bad tz non-raw commit\n".to_string(),
        author: Some(Signature {
            name: "Test".to_string(),
            email: "test@example.com".to_string(),
            timestamp: Some(Timestamp {
                millis_since_epoch: 1_730_000_000_000,
                tz_offset: 100 * 60,
            }),
        }),
        ..Default::default()
    };

    for (label, cid, commit, expected_substr) in [
        (
            "mismatched non-raw SHA-1",
            mismatched_cid,
            mismatched_commit,
            "Exported Git commit SHA-1 mismatch",
        ),
        (
            "out-of-range non-raw tz_offset",
            bad_tz_cid,
            bad_tz_commit,
            "exceeds Git +-9959 limit",
        ),
    ] {
        let read_tree_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let read_file_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let backend = ValidationMockBackendService {
            commits: HashMap::from([(cid.clone(), commit)]),
            trees: HashMap::new(),
            files: HashMap::new(),
            chunked_files: HashMap::new(),
            symlinks: HashMap::new(),
            read_commit_calls: Arc::new(std::sync::Mutex::new(HashMap::new())),
            read_tree_calls: Arc::clone(&read_tree_calls),
            read_file_calls: Arc::clone(&read_file_calls),
        };
        let op_store = MockOpStoreService {
            initial_view: cc_common::op_store::View {
                head_ids: vec![cid.clone()],
                wc_commit_ids: HashMap::new(),
                local_bookmarks: HashMap::from([(
                    "main".to_string(),
                    RefTarget {
                        removes: Vec::new(),
                        adds: vec![RefTargetTerm { commit_id: cid }],
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

        let dst = dir.path().join(format!(
            "preflight_non_raw_{}.git",
            label.replace(|c: char| !c.is_ascii_alphanumeric(), "_")
        ));
        let err = GitExporter::new(GitExportOptions {
            git_dir: dst.clone(),
            server_url: url,
            repo_id: "repo".to_string(),
            project_id: None,
            bare: true,
        })
        .run()
        .await
        .expect_err("corrupt non-raw commit must fail in preflight");
        assert!(
            err.to_string().contains(expected_substr),
            "{label}: unexpected error: {err}"
        );
        assert!(
            !dst.exists(),
            "{label}: must fail before initializing target directory"
        );
        assert_eq!(
            read_tree_calls.load(Ordering::SeqCst),
            0,
            "{label}: must fail before fetching any trees"
        );
        assert_eq!(
            read_file_calls.load(Ordering::SeqCst),
            0,
            "{label}: must fail before fetching any files"
        );
    }
}
