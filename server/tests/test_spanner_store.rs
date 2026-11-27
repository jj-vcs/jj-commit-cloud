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
use std::sync::Arc;

use bytes::Bytes;
use cc_common::backend::*;
use cc_common::op_store::*;
use jj_commit_cloud_server::store::{
    ChangePrefixResolution, CompositeStore, IdPrefixResolution, MemoryBlobStore, MetadataStore,
    SpannerConnectConfig, SpannerMetadataStore, StoreError,
};

fn unique_db_id(prefix: &str) -> String {
    let max_prefix = 30 - 9; // '_' + 8 random hex chars within Spanner's 30-char DB ID limit
    let trimmed = &prefix[..prefix.len().min(max_prefix)];
    let db_id = format!("{trimmed}_{:08x}", rand::random::<u32>());
    debug_assert!(db_id.len() <= 30);
    db_id
}

#[tokio::test]
async fn test_spanner_metadata_store_end_to_end_entities_and_isolation() {
    let Some(emulator) = testutils::SpannerEmulatorGuard::start_or_skip(
        "test_spanner_metadata_store_end_to_end_entities_and_isolation",
        false,
    )
    .await
    else {
        return;
    };

    let cfg = SpannerConnectConfig {
        database: emulator.database_uri(&unique_db_id("cc_meta_test")),
        endpoint: Some(emulator.grpc_endpoint()),
        admin_endpoint: Some(emulator.rest_endpoint().to_string()),
        auto_migrate: true,
        emulator: true,
        ..Default::default()
    };
    let meta = Arc::new(SpannerMetadataStore::connect(&cfg).await.unwrap());
    let blobs = Arc::new(MemoryBlobStore::with_location("gs://test-bucket-uc1"));
    let store = CompositeStore::new(meta.clone(), blobs);

    store
        .register_repo("repo-alpha", Some("alpha"))
        .await
        .unwrap();
    store
        .register_repo("repo-beta", Some("beta"))
        .await
        .unwrap();

    // Repository display names are not globally unique; distinct repos may share a name.
    store
        .register_repo("repo-gamma", Some("alpha"))
        .await
        .unwrap();

    // Blob write + per-repo authorization isolation + replica location tracking.
    let file_id = vec![0xab; 20];
    store
        .put_file(
            "repo-alpha",
            &file_id,
            Bytes::from_static(b"spanner blob test"),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .get_file_range("repo-alpha", &file_id, 0, Some(7))
            .await
            .unwrap()
            .as_ref(),
        b"spanner"
    );
    assert!(matches!(
        store.get_file_range("repo-beta", &file_id, 0, None).await,
        Err(StoreError::ObjectNotFound { .. })
    ));

    meta.add_blob_replica_location(&file_id, "gs://test-bucket-ew1")
        .await
        .unwrap();
    let global_rec = meta
        .get_global_blob_record(&file_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        global_rec.primary_location.as_deref(),
        Some("gs://test-bucket-uc1")
    );
    assert_eq!(
        global_rec.replicated_locations,
        vec![
            "gs://test-bucket-uc1".to_string(),
            "gs://test-bucket-ew1".to_string()
        ]
    );

    // Symlink, interleaved Tree + TreeEntries, and Commit generation tracking.
    let sym_id = vec![0xcd; 20];
    store
        .put_symlink("repo-alpha", &sym_id, "../shared/config.toml")
        .await
        .unwrap();
    assert_eq!(
        store.get_symlink("repo-alpha", &sym_id).await.unwrap(),
        "../shared/config.toml"
    );

    let tree_id = vec![0xef; 20];
    let entries = vec![
        TreeEntry {
            name: "a_link".to_string(),
            value: Some(TreeValue {
                value: Some(tree_value::Value::SymlinkId(sym_id.clone())),
            }),
        },
        TreeEntry {
            name: "z_file.txt".to_string(),
            value: Some(TreeValue {
                value: Some(tree_value::Value::File(File {
                    id: file_id.clone(),
                    executable: true,
                    copy_id: vec![],
                    size_bytes: None,
                })),
            }),
        },
    ];
    store
        .put_tree("repo-alpha", &tree_id, &entries)
        .await
        .unwrap();
    let read_entries = store.get_tree("repo-alpha", &tree_id).await.unwrap();
    let expected_entries = vec![
        TreeEntry {
            name: "a_link".to_string(),
            value: Some(TreeValue {
                value: Some(tree_value::Value::SymlinkId(sym_id)),
            }),
        },
        TreeEntry {
            name: "z_file.txt".to_string(),
            value: Some(TreeValue {
                value: Some(tree_value::Value::File(File {
                    id: file_id,
                    executable: true,
                    copy_id: vec![],
                    size_bytes: Some(b"spanner blob test".len() as u64),
                })),
            }),
        },
    ];
    assert_eq!(read_entries, expected_entries);
    let (page1, next_tok) = store
        .get_tree_page("repo-alpha", &tree_id, "", 1)
        .await
        .unwrap();
    assert_eq!(page1.len(), 1);
    assert_eq!(next_tok, "a_link");
    let (page2, next_tok2) = store
        .get_tree_page("repo-alpha", &tree_id, &next_tok, 1)
        .await
        .unwrap();
    assert_eq!(page2.len(), 1);
    assert!(next_tok2.is_empty());

    let c1_id = vec![0x01; 20];
    let c1 = Commit {
        commit_id: c1_id.clone(),
        change_id: vec![0x91; 16],
        parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![tree_id.clone()],
        description: "first commit\n".to_string(),
        author: None,
        committer: None,
        predecessors: vec![],
        conflict_labels: vec![],
        secure_sig: None,
        raw_git_commit: vec![],
        external_ids: [("git-sha1".to_string(), c1_id.clone())]
            .into_iter()
            .collect(),
    };
    store.put_commit("repo-alpha", &c1_id, &c1).await.unwrap();

    // Immutable insert-only semantics: re-inserting an existing CommitId cannot mutate it.
    let mut forged = c1.clone();
    forged.description = "forged overwrite attempt\n".to_string();
    store
        .put_commit("repo-alpha", &c1_id, &forged)
        .await
        .unwrap();
    let stored_c1 = store.get_commit("repo-alpha", &c1_id).await.unwrap();
    assert_eq!(stored_c1.description, "first commit\n");
    assert_eq!(
        stored_c1.external_ids.get("git-sha1"),
        Some(&c1_id),
        "external_ids map should round-trip through Spanner"
    );
    assert_eq!(
        meta.get_commit_generation("repo-alpha", &c1_id)
            .await
            .unwrap(),
        Some(1)
    );

    let c2_id = vec![0x02; 20];
    let mut c2 = c1.clone();
    c2.commit_id = c2_id.clone();
    c2.change_id = vec![0xa2; 16];
    c2.parent_commit_ids = vec![c1_id.clone()];
    c2.external_ids.clear();
    store.put_commit("repo-alpha", &c2_id, &c2).await.unwrap();
    assert_eq!(
        meta.get_commit_generation("repo-alpha", &c2_id)
            .await
            .unwrap(),
        Some(2)
    );

    // Commit and ChangeId (`z-k` reverse-hex: 0x91 -> "qy") prefix resolution on Spanner.
    assert_eq!(
        store
            .resolve_commit_prefix("repo-alpha", "0")
            .await
            .unwrap(),
        IdPrefixResolution::AmbiguousMatch
    );
    assert_eq!(
        store
            .resolve_commit_prefix("repo-alpha", "0101")
            .await
            .unwrap(),
        IdPrefixResolution::SingleMatch(c1_id.clone())
    );
    assert_eq!(
        store
            .resolve_change_prefix("repo-alpha", "qyqy", 10)
            .await
            .unwrap(),
        ChangePrefixResolution::SingleMatch {
            change_id: vec![0x91; 16],
            commit_ids: vec![c1_id]
        }
    );

    // Operation prefix range scans (even and odd hex nibble lengths).
    let view_id = vec![0x55; 16];
    store
        .put_view(
            "repo-alpha",
            &view_id,
            &View {
                head_ids: vec![c2_id],
                wc_commit_ids: Default::default(),
                local_bookmarks: Default::default(),
                remote_bookmarks: Default::default(),
            },
        )
        .await
        .unwrap();

    let op1_id = hex::decode("a7b10000000000000000000000000001").unwrap();
    let op2_id = hex::decode("a7b20000000000000000000000000002").unwrap();
    let op = Operation {
        view_id,
        parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
        metadata: None,
        commit_predecessors: vec![],
        commit_predecessors_set: false,
    };
    store
        .put_operation("repo-alpha", &op1_id, &op)
        .await
        .unwrap();
    store
        .put_operation("repo-alpha", &op2_id, &op)
        .await
        .unwrap();

    assert_eq!(
        store
            .resolve_operation_prefix("repo-alpha", "a7b")
            .await
            .unwrap(),
        IdPrefixResolution::AmbiguousMatch
    );
    assert_eq!(
        store
            .resolve_operation_prefix("repo-alpha", "a7b1")
            .await
            .unwrap(),
        IdPrefixResolution::SingleMatch(op1_id)
    );
}

#[tokio::test]
async fn test_spanner_concurrent_op_heads_under_fault_injection_preserves_all_siblings() {
    let Some(emulator) = testutils::SpannerEmulatorGuard::start_or_skip(
        "test_spanner_concurrent_op_heads_under_fault_injection_preserves_all_siblings",
        true,
    )
    .await
    else {
        return;
    };

    let cfg = SpannerConnectConfig {
        database: emulator.database_uri(&unique_db_id("cc_fault_test")),
        endpoint: Some(emulator.grpc_endpoint()),
        admin_endpoint: Some(emulator.rest_endpoint().to_string()),
        auto_migrate: true,
        emulator: true,
        ..Default::default()
    };
    let meta = Arc::new(SpannerMetadataStore::connect(&cfg).await.unwrap());
    let store = Arc::new(CompositeStore::new(meta, Arc::new(MemoryBlobStore::new())));

    store.register_repo("race-repo", None).await.unwrap();
    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let view_id = vec![0x77; 16];
    store
        .put_view(
            "race-repo",
            &view_id,
            &View {
                head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                wc_commit_ids: Default::default(),
                local_bookmarks: Default::default(),
                remote_bookmarks: Default::default(),
            },
        )
        .await
        .unwrap();

    let worker_count = 10usize;
    let mut expected_ops = Vec::with_capacity(worker_count);
    for i in 0..worker_count {
        let mut op_id = vec![0u8; 16];
        op_id[0] = 0x80;
        op_id[15] = (i + 1) as u8;
        let op = Operation {
            view_id: view_id.clone(),
            parents: vec![root_op.clone()],
            metadata: None,
            commit_predecessors: vec![],
            commit_predecessors_set: false,
        };
        store.put_operation("race-repo", &op_id, &op).await.unwrap();
        expected_ops.push(op_id);
    }

    // All 10 workers race concurrently from [root_op] under emulator transaction-abort injection.
    let barrier = Arc::new(tokio::sync::Barrier::new(worker_count));
    let mut handles = Vec::with_capacity(worker_count);
    for op_id in expected_ops.clone() {
        let store_cloned = store.clone();
        let barrier_cloned = barrier.clone();
        let root_cloned = root_op.clone();
        handles.push(tokio::spawn(async move {
            barrier_cloned.wait().await;
            store_cloned
                .update_op_heads("race-repo", &[root_cloned], &op_id)
                .await
                .unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let final_heads = store.get_op_heads("race-repo").await.unwrap();
    let final_set: HashSet<Vec<u8>> = final_heads.iter().cloned().collect();
    let expected_set: HashSet<Vec<u8>> = expected_ops.iter().cloned().collect();
    assert_eq!(final_set, expected_set);

    // Reconcile all 10 divergent heads into a single merged operation head.
    let merged_op = vec![0xfe; 16];
    store
        .put_operation(
            "race-repo",
            &merged_op,
            &Operation {
                view_id,
                parents: expected_ops.clone(),
                metadata: None,
                commit_predecessors: vec![],
                commit_predecessors_set: false,
            },
        )
        .await
        .unwrap();
    let collapsed = store
        .update_op_heads("race-repo", &expected_ops, &merged_op)
        .await
        .unwrap();
    assert_eq!(collapsed, vec![merged_op]);
}

#[tokio::test]
async fn test_spanner_shared_project_deduplicates_objects_and_paginates_list_commits() {
    let Some(emulator) = testutils::SpannerEmulatorGuard::start_or_skip(
        "test_spanner_shared_project_deduplicates_objects_and_paginates_list_commits",
        false,
    )
    .await
    else {
        return;
    };

    let cfg = SpannerConnectConfig {
        database: emulator.database_uri(&unique_db_id("cc_project_split")),
        endpoint: Some(emulator.grpc_endpoint()),
        admin_endpoint: Some(emulator.rest_endpoint().to_string()),
        auto_migrate: true,
        emulator: true,
        ..Default::default()
    };
    let meta = Arc::new(SpannerMetadataStore::connect(&cfg).await.unwrap());
    let store = CompositeStore::new(meta, Arc::new(MemoryBlobStore::new()));

    store
        .register_project("shared-proj", Some("Monorepo Alpha"))
        .await
        .unwrap();
    store
        .register_repo_in_project("agent-1", Some("shared-proj"), None)
        .await
        .unwrap();
    store
        .register_repo_in_project("agent-2", Some("shared-proj"), None)
        .await
        .unwrap();
    store
        .register_repo_in_project("other-agent", Some("isolated-proj"), None)
        .await
        .unwrap();

    for idx in 1u8..=5u8 {
        let mut cid = vec![0u8; 20];
        cid[19] = idx;
        let commit = Commit {
            commit_id: cid.clone(),
            change_id: vec![idx; 16],
            parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
            root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
            description: format!("commit {idx}\n"),
            author: None,
            committer: None,
            predecessors: vec![],
            conflict_labels: vec![],
            secure_sig: None,
            raw_git_commit: vec![],
            external_ids: Default::default(),
        };
        store.put_commit("agent-1", &cid, &commit).await.unwrap();
    }

    // Sibling repository `agent-2` in `shared-proj` reads and paginates all 5 commits,
    // while `other-agent` in `isolated-proj` sees zero commits.
    let (page1, tok1, sync1) = store
        .list_commits_with_sync("agent-2", &[], 2, &[])
        .await
        .unwrap();
    assert_eq!(page1.len(), 2);
    assert!(!tok1.is_empty());
    assert!(!sync1.is_empty());
    let (page2, tok2) = store.list_commits("agent-2", &tok1, 2).await.unwrap();
    assert_eq!(page2.len(), 2);
    assert!(!tok2.is_empty());
    let (page3, tok3) = store.list_commits("agent-2", &tok2, 2).await.unwrap();
    assert_eq!(page3.len(), 1);
    assert!(tok3.is_empty());

    // Incremental `since_token` sync returns 0 rows until a 6th commit is written.
    let (delta_empty, _, sync2) = store
        .list_commits_with_sync("agent-2", &[], 10, &sync1)
        .await
        .unwrap();
    assert!(delta_empty.is_empty());

    let mut c6_id = vec![0u8; 20];
    c6_id[19] = 6;
    store
        .put_commit(
            "agent-1",
            &c6_id,
            &Commit {
                commit_id: c6_id.clone(),
                change_id: vec![6; 16],
                parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
                description: "commit 6\n".to_string(),
                author: None,
                committer: None,
                predecessors: vec![],
                conflict_labels: vec![],
                secure_sig: None,
                raw_git_commit: vec![],
                external_ids: Default::default(),
            },
        )
        .await
        .unwrap();
    let (delta_new, delta_next, _) = store
        .list_commits_with_sync("agent-2", &[], 10, &sync2)
        .await
        .unwrap();
    assert_eq!(delta_new, vec![c6_id]);
    assert!(delta_next.is_empty());

    let (isolated_list, _) = store.list_commits("other-agent", &[], 10).await.unwrap();
    assert!(isolated_list.is_empty());

    // Verify interleaved `Workspaces` persistence on Spanner (including multi-tree conflict roots).
    let ws_op_id = vec![0x11; 16];
    store
        .put_operation(
            "agent-1",
            &ws_op_id,
            &Operation {
                view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
                parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    #[allow(deprecated)]
    let ws = cc_common::workspace::WorkspaceState {
        repo_id: "agent-1".to_string(),
        user: "bot@example.com".to_string(),
        workspace_name: "sandbox-01".to_string(),
        commit_id: vec![0x05; 20],
        operation_id: ws_op_id,
        tree_id: vec![0xaa; 20],
        sparse_patterns: vec!["crates/core".to_string(), "crates/api".to_string()],
        tree_ids: vec![vec![0xaa; 20], vec![0xbb; 20], vec![0xcc; 20]],
        conflict_labels: Vec::new(),
        sparse_patterns_set: true,
    };
    store.put_workspace(&ws).await.unwrap();
    let fetched_ws = store
        .get_workspace("agent-1", "bot@example.com", "sandbox-01")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched_ws, ws);
    assert_eq!(store.list_workspaces("agent-1").await.unwrap().len(), 1);
    assert!(
        store
            .delete_workspace("agent-1", "bot@example.com", "sandbox-01")
            .await
            .unwrap()
    );
    assert!(store.list_workspaces("agent-1").await.unwrap().is_empty());
}

#[tokio::test]
async fn test_spanner_large_tree_over_4000_entries_stages_and_preserves_ready_state() {
    let Some(emulator) = testutils::SpannerEmulatorGuard::start_or_skip(
        "test_spanner_large_tree_over_4000_entries_stages_and_preserves_ready_state",
        false,
    )
    .await
    else {
        return;
    };

    let cfg = SpannerConnectConfig {
        database: emulator.database_uri(&unique_db_id("cc_large_tree")),
        endpoint: Some(emulator.grpc_endpoint()),
        admin_endpoint: Some(emulator.rest_endpoint().to_string()),
        auto_migrate: true,
        emulator: true,
        ..Default::default()
    };
    let meta = Arc::new(SpannerMetadataStore::connect(&cfg).await.unwrap());
    let store = Arc::new(CompositeStore::new(
        meta.clone(),
        Arc::new(MemoryBlobStore::new()),
    ));

    store
        .register_repo_in_project("wide-repo", Some("wide-proj"), None)
        .await
        .unwrap();
    let file_id = vec![0x42; 20];
    let payload = Bytes::from_static(b"shared leaf content");
    store
        .put_file("wide-repo", &file_id, payload.clone())
        .await
        .unwrap();

    // 4,500 entries exceeds SpannerMetadataStore's 4,000-entry single-batch cap, exercising
    // multi-chunk staging and Ready=true finalization.
    let entry_count = 4_500usize;
    let entries: Vec<TreeEntry> = (0..entry_count)
        .map(|i| TreeEntry {
            name: format!("file_{i:05}.txt"),
            value: Some(TreeValue {
                value: Some(tree_value::Value::File(File {
                    id: file_id.clone(),
                    executable: false,
                    copy_id: vec![],
                    size_bytes: Some(payload.len() as u64),
                })),
            }),
        })
        .collect();

    let large_tree_id = vec![0x7e; 20];
    store
        .put_tree("wide-repo", &large_tree_id, &entries)
        .await
        .unwrap();

    assert!(
        meta.find_existing_trees("wide-proj", std::slice::from_ref(&large_tree_id))
            .await
            .unwrap()
            .contains(&large_tree_id)
    );
    let fetched = store.get_tree("wide-repo", &large_tree_id).await.unwrap();
    assert_eq!(fetched.len(), entry_count);
    assert_eq!(fetched[0].name, "file_00000.txt");
    assert_eq!(fetched[entry_count - 1].name, "file_04499.txt");

    let (page1, tok1) = store
        .get_tree_page("wide-repo", &large_tree_id, "", 2_500)
        .await
        .unwrap();
    assert_eq!(page1.len(), 2_500);
    assert!(!tok1.is_empty());
    let (page2, tok2) = store
        .get_tree_page("wide-repo", &large_tree_id, &tok1, 2_500)
        .await
        .unwrap();
    assert_eq!(page2.len(), 2_000);
    assert!(tok2.is_empty());

    // Re-uploading a ready >4,000-entry tree must skip without flipping Ready to false or
    // hiding the tree from concurrent readers.
    let store_reader = store.clone();
    let meta_reader = meta.clone();
    let tree_for_reader = large_tree_id.clone();
    let reader_task = tokio::spawn(async move {
        for _ in 0..10 {
            assert!(
                meta_reader
                    .find_existing_trees("wide-proj", std::slice::from_ref(&tree_for_reader))
                    .await
                    .unwrap()
                    .contains(&tree_for_reader)
            );
            let (page, _) = store_reader
                .get_tree_page("wide-repo", &tree_for_reader, "", 16)
                .await
                .unwrap();
            assert_eq!(page.len(), 16);
        }
    });

    for _ in 0..5 {
        store
            .put_tree("wide-repo", &large_tree_id, &entries)
            .await
            .unwrap();
    }
    reader_task.await.unwrap();

    // Direct MetadataStore re-upload with altered entries is skipped because Ready = TRUE.
    let mut altered = entries.clone();
    altered[0].name = "file_00000_mutated.txt".to_string();
    meta.put_tree("wide-proj", &large_tree_id, &altered)
        .await
        .unwrap();
    let after_reupload = store.get_tree("wide-repo", &large_tree_id).await.unwrap();
    assert_eq!(after_reupload[0].name, "file_00000.txt");

    // Shared project IDs resolve directly in CompositeStore even when no repository carries the
    // project's ID as its RepoId.
    assert!(meta.is_project_registered("wide-proj").await.unwrap());
    assert!(
        !meta
            .is_project_registered("nonexistent-proj")
            .await
            .unwrap()
    );
    let via_project = store.get_tree("wide-proj", &large_tree_id).await.unwrap();
    assert_eq!(via_project.len(), entry_count);
}

#[tokio::test]
async fn test_spanner_delete_repository_cascades_interleaved_children_and_preserves_shared_project()
{
    let Some(emulator) = testutils::SpannerEmulatorGuard::start_or_skip(
        "test_spanner_delete_repository_cascades_interleaved_children_and_preserves_shared_project",
        false,
    )
    .await
    else {
        return;
    };

    let cfg = SpannerConnectConfig {
        database: emulator.database_uri(&unique_db_id("cc_del_repo")),
        endpoint: Some(emulator.grpc_endpoint()),
        admin_endpoint: Some(emulator.rest_endpoint().to_string()),
        auto_migrate: true,
        emulator: true,
        ..Default::default()
    };
    let meta = Arc::new(SpannerMetadataStore::connect(&cfg).await.unwrap());
    let store = CompositeStore::new(meta.clone(), Arc::new(MemoryBlobStore::new()));

    store
        .register_project("shared-proj", Some("Spanner Shared"))
        .await
        .unwrap();
    store
        .register_repo_in_project("repo-del", Some("shared-proj"), None)
        .await
        .unwrap();
    store
        .register_repo_in_project("repo-keep", Some("shared-proj"), None)
        .await
        .unwrap();

    // Populate shared project objects (blob, symlink, tree, commit).
    let file_id = vec![0x11; 20];
    store
        .put_file("repo-del", &file_id, Bytes::from_static(b"spanner shared"))
        .await
        .unwrap();
    let sym_id = vec![0x22; 20];
    store
        .put_symlink("repo-del", &sym_id, "../shared-link")
        .await
        .unwrap();
    let tree_id = vec![0x33; 20];
    store
        .put_tree(
            "repo-del",
            &tree_id,
            &[
                TreeEntry {
                    name: "a.txt".to_string(),
                    value: Some(TreeValue {
                        value: Some(tree_value::Value::File(File {
                            id: file_id.clone(),
                            executable: false,
                            copy_id: Vec::new(),
                            size_bytes: None,
                        })),
                    }),
                },
                TreeEntry {
                    name: "b.link".to_string(),
                    value: Some(TreeValue {
                        value: Some(tree_value::Value::SymlinkId(sym_id.clone())),
                    }),
                },
            ],
        )
        .await
        .unwrap();
    let commit_id = vec![0x44; 20];
    store
        .put_commit(
            "repo-del",
            &commit_id,
            &Commit {
                commit_id: commit_id.clone(),
                change_id: vec![0x55; 16],
                parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: vec![tree_id.clone()],
                description: "spanner commit\n".to_string(),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Populate interleaved Views, Operations, OpHeads, and Workspaces on both repos.
    let view_id = vec![0x66; 16];
    let view = View {
        head_ids: vec![commit_id.clone()],
        ..Default::default()
    };
    let op_id = vec![0x77; 16];
    let op = Operation {
        view_id: view_id.clone(),
        parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
        ..Default::default()
    };
    for repo in ["repo-del", "repo-keep"] {
        store
            .publish_operation(
                repo,
                &view_id,
                &view,
                &op_id,
                &op,
                &[cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
            )
            .await
            .unwrap();
        #[allow(deprecated)]
        let ws = cc_common::workspace::WorkspaceState {
            repo_id: repo.to_string(),
            user: "alice@example.com".to_string(),
            workspace_name: "default".to_string(),
            commit_id: commit_id.clone(),
            operation_id: op_id.clone(),
            tree_id: tree_id.clone(),
            tree_ids: vec![tree_id.clone()],
            conflict_labels: Vec::new(),
            sparse_patterns: vec![String::new()],
            sparse_patterns_set: true,
        };
        store.put_workspace(&ws).await.unwrap();
    }

    // Absent repo returns Ok(false).
    assert!(
        !store
            .delete_repository("nonexistent-repo", None)
            .await
            .unwrap()
    );

    // Mismatched project_id fails with FailedPrecondition without deleting repo-del.
    let err = store
        .delete_repository("repo-del", Some("wrong-proj"))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::FailedPrecondition(_)), "{err:?}");
    assert_eq!(
        store.get_op_heads("repo-del").await.unwrap(),
        vec![op_id.clone()]
    );
    assert_eq!(store.list_workspaces("repo-del").await.unwrap().len(), 1);

    // Matching project_id deletes Repositories row and cascades interleaved Views/Operations/Workspaces.
    assert!(
        store
            .delete_repository("repo-del", Some("shared-proj"))
            .await
            .unwrap()
    );
    assert!(
        !store
            .delete_repository("repo-del", Some("shared-proj"))
            .await
            .unwrap()
    );

    assert!(matches!(
        store.get_op_heads("repo-del").await,
        Err(StoreError::RepoNotFound { .. })
    ));
    assert!(meta.get_view("repo-del", &view_id).await.unwrap().is_none());
    assert!(
        meta.get_operation("repo-del", &op_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(meta.list_workspaces("repo-del").await.unwrap().is_empty());

    // Direct MetadataStore writes targeting the deleted repository must all return RepoNotFound
    // (including non-empty expected_operation_id CAS workspace updates, not CasConflict).
    assert!(matches!(
        meta.put_view("repo-del", &view_id, &view).await,
        Err(StoreError::RepoNotFound { .. })
    ));
    assert!(matches!(
        meta.put_operation("repo-del", &op_id, &op).await,
        Err(StoreError::RepoNotFound { .. })
    ));
    assert!(matches!(
        meta.update_op_heads("repo-del", &[], &op_id).await,
        Err(StoreError::RepoNotFound { .. })
    ));
    assert!(matches!(
        meta.publish_operation_atomic("repo-del", &view_id, &view, &op_id, &op, &[])
            .await,
        Err(StoreError::RepoNotFound { .. })
    ));
    assert!(matches!(
        meta.compare_and_swap_op_heads("repo-del", &[], &op_id)
            .await,
        Err(StoreError::RepoNotFound { .. })
    ));
    #[allow(deprecated)]
    let del_ws = cc_common::workspace::WorkspaceState {
        repo_id: "repo-del".to_string(),
        user: "alice@example.com".to_string(),
        workspace_name: "default".to_string(),
        commit_id: commit_id.clone(),
        operation_id: op_id.clone(),
        tree_id: tree_id.clone(),
        tree_ids: vec![tree_id.clone()],
        conflict_labels: Vec::new(),
        sparse_patterns: vec![String::new()],
        sparse_patterns_set: true,
    };
    for expected in [None, Some([].as_slice()), Some(op_id.as_slice())] {
        let err = meta.put_workspace_cas(&del_ws, expected).await.unwrap_err();
        assert!(
            matches!(err, StoreError::RepoNotFound { .. }),
            "expected RepoNotFound for put_workspace_cas(expected={expected:?}), got {err:?}"
        );
    }

    // Shared project objects remain readable via sibling repo-keep and shared-proj.
    assert_eq!(
        store
            .get_file_range("repo-keep", &file_id, 0, None)
            .await
            .unwrap()
            .as_ref(),
        b"spanner shared"
    );
    assert_eq!(
        store.get_symlink("repo-keep", &sym_id).await.unwrap(),
        "../shared-link"
    );
    assert_eq!(
        store.get_tree("repo-keep", &tree_id).await.unwrap().len(),
        2
    );
    assert_eq!(
        store
            .get_commit("repo-keep", &commit_id)
            .await
            .unwrap()
            .description,
        "spanner commit\n"
    );
    assert_eq!(store.get_op_heads("repo-keep").await.unwrap(), vec![op_id]);
    assert_eq!(store.list_workspaces("repo-keep").await.unwrap().len(), 1);

    // Re-registering repo-del starts clean with no leftover Views/Operations/Workspaces.
    store
        .register_repo_in_project("repo-del", Some("shared-proj"), None)
        .await
        .unwrap();
    assert_eq!(
        store.get_op_heads("repo-del").await.unwrap(),
        vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()]
    );
    assert!(
        store
            .get_view("repo-del", &view_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(store.list_workspaces("repo-del").await.unwrap().is_empty());
}

#[tokio::test]
async fn test_spanner_delete_repository_multi_batch_and_project_checked_writes_prevent_toctou() {
    let Some(emulator) = testutils::SpannerEmulatorGuard::start_or_skip(
        "test_spanner_delete_repository_multi_batch_and_project_checked_writes_prevent_toctou",
        false,
    )
    .await
    else {
        return;
    };

    let cfg = SpannerConnectConfig {
        database: emulator.database_uri(&unique_db_id("cc_del_batch")),
        endpoint: Some(emulator.grpc_endpoint()),
        admin_endpoint: Some(emulator.rest_endpoint().to_string()),
        auto_migrate: true,
        emulator: true,
        ..Default::default()
    };
    // Configure a tiny batch size (2 rows/tx) so deleting 7 views, 7 operations, and 7
    // workspaces exercises multi-transaction batched deletion end to end.
    let meta = Arc::new(
        SpannerMetadataStore::connect(&cfg)
            .await
            .unwrap()
            .with_delete_batch_limit(2),
    );
    let store = CompositeStore::new(meta.clone(), Arc::new(MemoryBlobStore::new()));

    store
        .register_project("proj-a", Some("Batch Proj A"))
        .await
        .unwrap();
    store
        .register_project("proj-b", Some("Batch Proj B"))
        .await
        .unwrap();
    store
        .register_repo_in_project("repo-batch", Some("proj-a"), None)
        .await
        .unwrap();
    store
        .register_repo_in_project("repo-keep", Some("proj-a"), None)
        .await
        .unwrap();

    // Seed shared proj-a blob, tree, and commit.
    let file_id = vec![0x21; cc_common::FILE_ID_LENGTH];
    store
        .put_file(
            "repo-batch",
            &file_id,
            Bytes::from_static(b"spanner batched delete shared"),
        )
        .await
        .unwrap();
    let tree_id = vec![0x31; cc_common::TREE_ID_LENGTH];
    store
        .put_tree(
            "repo-batch",
            &tree_id,
            &[TreeEntry {
                name: "shared.txt".to_string(),
                value: Some(TreeValue {
                    value: Some(tree_value::Value::File(File {
                        id: file_id.clone(),
                        executable: false,
                        copy_id: Vec::new(),
                        size_bytes: None,
                    })),
                }),
            }],
        )
        .await
        .unwrap();
    let commit_id = vec![0x41; cc_common::COMMIT_ID_LENGTH];
    store
        .put_commit(
            "repo-batch",
            &commit_id,
            &Commit {
                commit_id: commit_id.clone(),
                change_id: vec![0x51; cc_common::CHANGE_ID_LENGTH],
                parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: vec![tree_id.clone()],
                description: "shared batch commit\n".to_string(),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Seed 7 Views, 7 Operations, and 7 Workspaces on repo-batch (each spans 4+ batches of 2)
    // plus 1 operation + 1 workspace on sibling repo-keep.
    let mut prev_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let mut view_ids = Vec::new();
    let mut op_ids = Vec::new();
    for idx in 1u8..=7u8 {
        let mut vid = vec![0x60; cc_common::VIEW_ID_LENGTH];
        vid[15] = idx;
        let mut oid = vec![0x70; cc_common::OPERATION_ID_LENGTH];
        oid[15] = idx;
        let view = View {
            head_ids: vec![commit_id.clone()],
            ..Default::default()
        };
        let op = Operation {
            view_id: vid.clone(),
            parents: vec![prev_op.clone()],
            ..Default::default()
        };
        store
            .publish_operation(
                "repo-batch",
                &vid,
                &view,
                &oid,
                &op,
                std::slice::from_ref(&prev_op),
            )
            .await
            .unwrap();
        #[allow(deprecated)]
        let ws = cc_common::workspace::WorkspaceState {
            repo_id: "repo-batch".to_string(),
            user: format!("user{idx}@example.com"),
            workspace_name: format!("ws-{idx}"),
            commit_id: commit_id.clone(),
            operation_id: oid.clone(),
            tree_id: tree_id.clone(),
            tree_ids: vec![tree_id.clone()],
            conflict_labels: Vec::new(),
            sparse_patterns: vec![String::new()],
            sparse_patterns_set: true,
        };
        store.put_workspace(&ws).await.unwrap();
        view_ids.push(vid);
        op_ids.push(oid.clone());
        prev_op = oid;
    }

    // Re-writing an already-published operation via put_operation_for_project must preserve
    // Published=true so retrying publish_operation_for_project does not resurrect it.
    let op1_dup = Operation {
        view_id: view_ids[0].clone(),
        parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
        ..Default::default()
    };
    meta.put_operation_for_project("repo-batch", "proj-a", &op_ids[0], &op1_dup, true)
        .await
        .unwrap();
    let heads_after_retry = meta
        .publish_operation_for_project(
            "repo-batch",
            "proj-a",
            &view_ids[0],
            &View {
                head_ids: vec![commit_id.clone()],
                ..Default::default()
            },
            &op_ids[0],
            &op1_dup,
            &[cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
            true,
        )
        .await
        .unwrap();
    assert_eq!(heads_after_retry, vec![prev_op.clone()]);

    // Transactional dependency checks reject missing non-root parent ops and external views
    // when `require_dependencies_exist = true`.
    let missing_parent_id = vec![0xee; cc_common::OPERATION_ID_LENGTH];
    let bad_parent_op_id = vec![0xef; cc_common::OPERATION_ID_LENGTH];
    let bad_parent_err = meta
        .publish_operation_for_project(
            "repo-batch",
            "proj-a",
            &view_ids[0],
            &View {
                head_ids: vec![commit_id.clone()],
                ..Default::default()
            },
            &bad_parent_op_id,
            &Operation {
                view_id: view_ids[0].clone(),
                parents: vec![missing_parent_id],
                ..Default::default()
            },
            std::slice::from_ref(&prev_op),
            true,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        bad_parent_err,
        StoreError::MissingDependency {
            parent_kind: "operation",
            missing_kind: "parent operation",
            ..
        }
    ));
    let missing_ext_view_id = vec![0xed; cc_common::VIEW_ID_LENGTH];
    let bad_view_err = meta
        .publish_operation_for_project(
            "repo-batch",
            "proj-a",
            &view_ids[0],
            &View {
                head_ids: vec![commit_id.clone()],
                ..Default::default()
            },
            &bad_parent_op_id,
            &Operation {
                view_id: missing_ext_view_id,
                parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                ..Default::default()
            },
            std::slice::from_ref(&prev_op),
            true,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        bad_view_err,
        StoreError::MissingDependency {
            parent_kind: "operation",
            missing_kind: "view",
            ..
        }
    ));

    // Populate sibling repo-keep.
    let keep_view_id = vec![0x81; cc_common::VIEW_ID_LENGTH];
    let keep_op_id = vec![0x82; cc_common::OPERATION_ID_LENGTH];
    store
        .publish_operation(
            "repo-keep",
            &keep_view_id,
            &View {
                head_ids: vec![commit_id.clone()],
                ..Default::default()
            },
            &keep_op_id,
            &Operation {
                view_id: keep_view_id.clone(),
                parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                ..Default::default()
            },
            &[cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
        )
        .await
        .unwrap();

    // Multi-batch delete_repository removes all 7 views, 7 operations, 7 workspaces, and the
    // parent row while leaving shared proj-a objects and sibling repo-keep intact.
    assert!(
        store
            .delete_repository("repo-batch", Some("proj-a"))
            .await
            .unwrap()
    );
    assert!(meta.get_op_heads("repo-batch").await.unwrap().is_none());
    assert!(meta.list_workspaces("repo-batch").await.unwrap().is_empty());
    for vid in &view_ids {
        assert!(meta.get_view("repo-batch", vid).await.unwrap().is_none());
    }
    for oid in &op_ids {
        assert!(
            meta.get_operation("repo-batch", oid)
                .await
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(
        store
            .get_file_range("repo-keep", &file_id, 0, None)
            .await
            .unwrap()
            .as_ref(),
        b"spanner batched delete shared"
    );
    assert_eq!(
        store.get_tree("repo-keep", &tree_id).await.unwrap().len(),
        1
    );
    assert_eq!(
        store
            .get_commit("repo-keep", &commit_id)
            .await
            .unwrap()
            .description,
        "shared batch commit\n"
    );
    assert_eq!(
        store.get_op_heads("repo-keep").await.unwrap(),
        vec![keep_op_id]
    );

    // Re-register repo-batch under proj-b and verify every *_for_project write with stale
    // expected_project_id = "proj-a" fails atomically with FailedPrecondition.
    store
        .register_repo_in_project("repo-batch", Some("proj-b"), None)
        .await
        .unwrap();
    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let stale_view = View {
        head_ids: vec![commit_id.clone()],
        ..Default::default()
    };
    let stale_op = Operation {
        view_id: view_ids[0].clone(),
        parents: vec![root_op.clone()],
        ..Default::default()
    };
    #[allow(deprecated)]
    let stale_ws = cc_common::workspace::WorkspaceState {
        repo_id: "repo-batch".to_string(),
        user: "alice@example.com".to_string(),
        workspace_name: "stale".to_string(),
        commit_id: commit_id.clone(),
        operation_id: root_op.clone(),
        tree_id: tree_id.clone(),
        tree_ids: vec![tree_id.clone()],
        conflict_labels: Vec::new(),
        sparse_patterns: vec![String::new()],
        sparse_patterns_set: true,
    };

    assert!(matches!(
        meta.put_view_for_project("repo-batch", "proj-a", &view_ids[0], &stale_view)
            .await,
        Err(StoreError::FailedPrecondition(_))
    ));
    assert!(matches!(
        meta.put_operation_for_project("repo-batch", "proj-a", &op_ids[0], &stale_op, true)
            .await,
        Err(StoreError::FailedPrecondition(_))
    ));
    assert!(matches!(
        meta.publish_operation_for_project(
            "repo-batch",
            "proj-a",
            &view_ids[0],
            &stale_view,
            &op_ids[0],
            &stale_op,
            std::slice::from_ref(&root_op),
            true,
        )
        .await,
        Err(StoreError::FailedPrecondition(_))
    ));
    assert!(matches!(
        meta.update_op_heads_for_project(
            "repo-batch",
            "proj-a",
            std::slice::from_ref(&root_op),
            &op_ids[0],
            true,
        )
        .await,
        Err(StoreError::FailedPrecondition(_))
    ));
    assert!(matches!(
        meta.update_op_heads_cas_for_project(
            "repo-batch",
            "proj-a",
            std::slice::from_ref(&root_op),
            &op_ids[0],
            true,
        )
        .await,
        Err(StoreError::FailedPrecondition(_))
    ));
    for expected in [None, Some([].as_slice()), Some(root_op.as_slice())] {
        assert!(matches!(
            meta.put_workspace_cas_for_project("proj-a", &stale_ws, expected, true)
                .await,
            Err(StoreError::FailedPrecondition(_))
        ));
    }

    // Even when expected_project_id matches "proj-b", requiring dependency existence inside
    // the serializable transaction rejects committing deleted/missing views, parent ops,
    // operation heads, and workspace operations.
    assert!(matches!(
        meta.put_operation_for_project("repo-batch", "proj-b", &op_ids[0], &stale_op, true)
            .await,
        Err(StoreError::MissingDependency {
            parent_kind: "operation",
            missing_kind: "view",
            ..
        })
    ));
    let missing_parent_op = Operation {
        view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
        parents: vec![op_ids[0].clone()],
        ..Default::default()
    };
    assert!(matches!(
        meta.put_operation_for_project(
            "repo-batch",
            "proj-b",
            &op_ids[1],
            &missing_parent_op,
            true,
        )
        .await,
        Err(StoreError::MissingDependency {
            parent_kind: "operation",
            missing_kind: "parent operation",
            ..
        })
    ));
    let mut stale_op_ws = stale_ws.clone();
    stale_op_ws.operation_id = op_ids[0].clone();
    assert!(matches!(
        meta.put_workspace_cas_for_project("proj-b", &stale_op_ws, None, true)
            .await,
        Err(StoreError::MissingDependency {
            parent_kind: "workspace",
            missing_kind: "operation",
            ..
        })
    ));
    assert!(matches!(
        meta.update_op_heads_for_project(
            "repo-batch",
            "proj-b",
            std::slice::from_ref(&root_op),
            &op_ids[0],
            true,
        )
        .await,
        Err(StoreError::MissingDependency {
            parent_kind: "op_heads",
            missing_kind: "operation",
            ..
        })
    ));
    assert!(matches!(
        meta.update_op_heads_cas_for_project(
            "repo-batch",
            "proj-b",
            std::slice::from_ref(&root_op),
            &op_ids[0],
            true,
        )
        .await,
        Err(StoreError::MissingDependency {
            parent_kind: "op_heads",
            missing_kind: "operation",
            ..
        })
    ));

    // Confirm nothing was persisted into repo-batch under proj-b.
    assert!(
        meta.get_view("repo-batch", &view_ids[0])
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        meta.get_operation("repo-batch", &op_ids[0])
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        meta.get_op_heads("repo-batch").await.unwrap(),
        Some(vec![root_op])
    );
    assert!(meta.list_workspaces("repo-batch").await.unwrap().is_empty());
}

#[tokio::test]
async fn test_spanner_delete_repository_atomic_fast_path_unconditional_replacement_and_batch_cap() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let Some(emulator) = testutils::SpannerEmulatorGuard::start_or_skip(
        "test_spanner_delete_repository_atomic_fast_path_unconditional_replacement_and_batch_cap",
        false,
    )
    .await
    else {
        return;
    };

    let cfg = SpannerConnectConfig {
        database: emulator.database_uri(&unique_db_id("cc_del_atom")),
        endpoint: Some(emulator.grpc_endpoint()),
        admin_endpoint: Some(emulator.rest_endpoint().to_string()),
        auto_migrate: true,
        emulator: true,
        ..Default::default()
    };
    let meta = Arc::new(
        SpannerMetadataStore::connect(&cfg)
            .await
            .unwrap()
            .with_delete_batch_limit(3),
    );
    let store = CompositeStore::new(meta.clone(), Arc::new(MemoryBlobStore::new()));
    store.register_project("proj-a", None).await.unwrap();
    store.register_project("proj-b", None).await.unwrap();

    // Exact boundary fast path (1 Workspace + 1 Operation + 1 View == batch_limit 3):
    // must delete everything in ONE Serializable transaction (0 drain-batch hook invocations).
    store
        .register_repo_in_project("repo-fast", Some("proj-a"), None)
        .await
        .unwrap();
    let fast_view_id = vec![0x11; cc_common::VIEW_ID_LENGTH];
    let fast_op_id = vec![0x12; cc_common::OPERATION_ID_LENGTH];
    store
        .publish_operation(
            "repo-fast",
            &fast_view_id,
            &View {
                head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                ..Default::default()
            },
            &fast_op_id,
            &Operation {
                view_id: fast_view_id.clone(),
                parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                ..Default::default()
            },
            &[cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
        )
        .await
        .unwrap();
    #[allow(deprecated)]
    let fast_ws = cc_common::workspace::WorkspaceState {
        repo_id: "repo-fast".to_string(),
        user: "alice@example.com".to_string(),
        workspace_name: "default".to_string(),
        commit_id: cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
        operation_id: fast_op_id.clone(),
        tree_id: cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
        tree_ids: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
        conflict_labels: Vec::new(),
        sparse_patterns: vec![String::new()],
        sparse_patterns_set: true,
    };
    store.put_workspace(&fast_ws).await.unwrap();

    let fast_batches = Arc::new(AtomicUsize::new(0));
    let fast_batches_hook = Arc::clone(&fast_batches);
    assert!(
        meta.delete_repository_with_after_batch_hook("repo-fast", Some("proj-a"), move |_| {
            let counter = Arc::clone(&fast_batches_hook);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
            }
        })
        .await
        .unwrap()
    );
    assert_eq!(
        fast_batches.load(Ordering::SeqCst),
        0,
        "child count == batch_limit must commit atomically in a single Serializable transaction"
    );
    assert!(meta.get_op_heads("repo-fast").await.unwrap().is_none());
    assert!(
        meta.get_view("repo-fast", &fast_view_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        meta.get_operation("repo-fast", &fast_op_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(meta.list_workspaces("repo-fast").await.unwrap().is_empty());

    // Unconditional delete (`expected_project_id == None`) encountering concurrent delete +
    // re-register under `proj-b` after batch 0 must return `Ok(false)` (not `FailedPrecondition`)
    // and leave `proj-b`'s new repository and child rows untouched.
    let small_meta = Arc::new(meta.as_ref().clone().with_delete_batch_limit(2));
    let seed_oversized = |repo_id: &'static str| {
        let store = store.clone();
        async move {
            store
                .register_repo_in_project(repo_id, Some("proj-a"), None)
                .await
                .unwrap();
            let mut prev_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
            for idx in 1u8..=3u8 {
                let mut vid = vec![0x30; cc_common::VIEW_ID_LENGTH];
                vid[15] = idx;
                let mut oid = vec![0x40; cc_common::OPERATION_ID_LENGTH];
                oid[15] = idx;
                store
                    .publish_operation(
                        repo_id,
                        &vid,
                        &View {
                            head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                            ..Default::default()
                        },
                        &oid,
                        &Operation {
                            view_id: vid.clone(),
                            parents: vec![prev_op.clone()],
                            ..Default::default()
                        },
                        std::slice::from_ref(&prev_op),
                    )
                    .await
                    .unwrap();
                prev_op = oid;
            }
        }
    };

    seed_oversized("repo-uncond").await;
    let new_proj_b_view = vec![0x91; cc_common::VIEW_ID_LENGTH];
    let new_proj_b_op = vec![0x92; cc_common::OPERATION_ID_LENGTH];
    let meta_for_replace = Arc::clone(&meta);
    let view_for_replace = new_proj_b_view.clone();
    let op_for_replace = new_proj_b_op.clone();
    let uncond_res = small_meta
        .delete_repository_with_after_batch_hook("repo-uncond", None, move |batch_idx| {
            let meta = Arc::clone(&meta_for_replace);
            let view_id = view_for_replace.clone();
            let op_id = op_for_replace.clone();
            async move {
                if batch_idx == 0 {
                    assert!(
                        meta.delete_repository("repo-uncond", Some("proj-a"))
                            .await
                            .unwrap()
                    );
                    meta.register_repo_in_project("repo-uncond", "proj-b", None)
                        .await
                        .unwrap();
                    meta.publish_operation_for_project(
                        "repo-uncond",
                        "proj-b",
                        &view_id,
                        &View {
                            head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                            ..Default::default()
                        },
                        &op_id,
                        &Operation {
                            view_id: view_id.clone(),
                            parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                            ..Default::default()
                        },
                        &[cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                        true,
                    )
                    .await
                    .unwrap();
                }
            }
        })
        .await
        .expect("unconditional delete after concurrent replacement must return Ok(false)");
    assert!(!uncond_res);
    assert_eq!(
        meta.get_repo_project_id("repo-uncond")
            .await
            .unwrap()
            .as_deref(),
        Some("proj-b")
    );
    assert_eq!(
        meta.get_op_heads("repo-uncond").await.unwrap(),
        Some(vec![new_proj_b_op.clone()])
    );
    assert!(
        meta.get_view("repo-uncond", &new_proj_b_view)
            .await
            .unwrap()
            .is_some()
    );

    // Conditional delete (`expected_project_id == Some("proj-a")`) encountering concurrent
    // replacement under `proj-b` returns `FailedPrecondition` and leaves `proj-b` intact.
    seed_oversized("repo-cond").await;
    let meta_for_cond = Arc::clone(&meta);
    let cond_err = small_meta
        .delete_repository_with_after_batch_hook("repo-cond", Some("proj-a"), move |batch_idx| {
            let meta = Arc::clone(&meta_for_cond);
            async move {
                if batch_idx == 0 {
                    assert!(
                        meta.delete_repository("repo-cond", Some("proj-a"))
                            .await
                            .unwrap()
                    );
                    meta.register_repo_in_project("repo-cond", "proj-b", None)
                        .await
                        .unwrap();
                }
            }
        })
        .await
        .unwrap_err();
    assert!(
        matches!(cond_err, StoreError::FailedPrecondition(_)),
        "expected FailedPrecondition, got {cond_err:?}"
    );
    assert_eq!(
        meta.get_repo_project_id("repo-cond")
            .await
            .unwrap()
            .as_deref(),
        Some("proj-b")
    );

    // Multi-batch drain with concurrent child write mid-drain leaves zero orphaned rows,
    // and hitting `max_delete_batch_iterations` returns `StoreError::Unavailable` for retry.
    seed_oversized("repo-cap").await;
    let capped_meta = meta
        .as_ref()
        .clone()
        .with_delete_batch_limit(2)
        .with_max_delete_batch_iterations(1);
    let cap_err = capped_meta
        .delete_repository("repo-cap", Some("proj-a"))
        .await
        .unwrap_err();
    assert!(
        matches!(cap_err, StoreError::Unavailable(_)),
        "exceeding max_delete_batch_iterations must return Unavailable, got {cap_err:?}"
    );
    // Retrying with normal iteration budget drains remaining rows + concurrent write.
    let late_view = vec![0xa1; cc_common::VIEW_ID_LENGTH];
    let late_op = vec![0xa2; cc_common::OPERATION_ID_LENGTH];
    let meta_for_late = Arc::clone(&meta);
    let late_view_hook = late_view.clone();
    let late_op_hook = late_op.clone();
    assert!(
        small_meta
            .delete_repository_with_after_batch_hook("repo-cap", Some("proj-a"), move |batch_idx| {
                let meta = Arc::clone(&meta_for_late);
                let vid = late_view_hook.clone();
                let oid = late_op_hook.clone();
                async move {
                    if batch_idx == 0 {
                        let _ = meta
                            .publish_operation_for_project(
                                "repo-cap",
                                "proj-a",
                                &vid,
                                &View {
                                    head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                                    ..Default::default()
                                },
                                &oid,
                                &Operation {
                                    view_id: vid.clone(),
                                    parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                                    ..Default::default()
                                },
                                &[cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                                true,
                            )
                            .await;
                    }
                }
            })
            .await
            .unwrap()
    );
    assert!(meta.get_op_heads("repo-cap").await.unwrap().is_none());
    assert!(
        meta.get_view("repo-cap", &late_view)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        meta.get_operation("repo-cap", &late_op)
            .await
            .unwrap()
            .is_none()
    );
}
