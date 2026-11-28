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
    #[allow(deprecated)]
    let ws = cc_common::workspace::WorkspaceState {
        repo_id: "agent-1".to_string(),
        user: "bot@example.com".to_string(),
        workspace_name: "sandbox-01".to_string(),
        commit_id: vec![0x05; 20],
        operation_id: vec![0x11; 16],
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
