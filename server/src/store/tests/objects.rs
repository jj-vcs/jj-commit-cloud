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

use super::super::*;

#[test]
fn test_hex_prefix_byte_bounds_even_and_odd_nibbles() {
    let (min_even, max_even) = hex_prefix_byte_bounds("4b82", 4).unwrap().unwrap();
    assert_eq!(min_even, vec![0x4b, 0x82, 0x00, 0x00]);
    assert_eq!(max_even, vec![0x4b, 0x82, 0xff, 0xff]);

    let (min_odd, max_odd) = hex_prefix_byte_bounds("4b8", 4).unwrap().unwrap();
    assert_eq!(min_odd, vec![0x4b, 0x80, 0x00, 0x00]);
    assert_eq!(max_odd, vec![0x4b, 0x8f, 0xff, 0xff]);
}

#[test]
fn test_hex_prefix_byte_bounds_rejects_invalid_input() {
    assert!(hex_prefix_byte_bounds("zz", 4).is_err());
    assert!(hex_prefix_byte_bounds("01020304zz", 4).is_err());
    assert_eq!(hex_prefix_byte_bounds("0102030405", 4).unwrap(), None);
}

#[test]
fn test_normalize_change_id_prefix_to_hex() {
    assert_eq!(normalize_change_id_prefix_to_hex("zyxk").unwrap(), "012f");
    assert_eq!(normalize_change_id_prefix_to_hex("01ab").unwrap(), "01ab");
    assert!(normalize_change_id_prefix_to_hex("01zk").is_err());
}

#[tokio::test]
async fn test_composite_store_enforces_dependency_validation_size_enrichment_and_sort_order() {
    let store = CompositeStore::in_memory();
    store.register_repo("repo-a", Some("alpha")).await.unwrap();
    store.register_repo("repo-b", Some("beta")).await.unwrap();

    let blob_id = vec![0x11; 20];
    let sub_tree_id = vec![0x33; 20];
    // Writing a tree referencing an unuploaded file fails with MissingDependency.
    let tree_entries = vec![TreeEntry {
        name: "readme.md".to_string(),
        value: Some(TreeValue {
            value: Some(tree_value::Value::File(File {
                id: blob_id.clone(),
                executable: false,
                copy_id: vec![],
                size_bytes: None,
            })),
        }),
    }];
    let err = store
        .put_tree("repo-a", &[0x22; 20], &tree_entries)
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::MissingDependency { .. }));

    // Upload the blob and a subtree into repo-a; write entries in Git trailing-slash order
    // (`"foo.rs"` before directory `"foo"`) and verify ReadTree returns UTF-8 byte order
    // (`"foo"` before `"foo.rs"`) with `size_bytes = Some(11)` populated.
    store
        .put_file("repo-a", &blob_id, Bytes::from_static(b"hello world"))
        .await
        .unwrap();
    store.put_tree("repo-a", &sub_tree_id, &[]).await.unwrap();

    let git_ordered = vec![
        TreeEntry {
            name: "foo.rs".to_string(),
            value: Some(TreeValue {
                value: Some(tree_value::Value::File(File {
                    id: blob_id.clone(),
                    executable: false,
                    copy_id: vec![],
                    size_bytes: None,
                })),
            }),
        },
        TreeEntry {
            name: "foo".to_string(),
            value: Some(TreeValue {
                value: Some(tree_value::Value::TreeId(sub_tree_id.clone())),
            }),
        },
    ];
    store
        .put_tree("repo-a", &[0x22; 20], &git_ordered)
        .await
        .unwrap();

    let fetched = store.get_tree("repo-a", &[0x22; 20]).await.unwrap();
    assert_eq!(fetched.len(), 2);
    assert_eq!(fetched[0].name, "foo");
    assert_eq!(fetched[1].name, "foo.rs");
    match fetched[1].value.as_ref().unwrap().value.as_ref().unwrap() {
        tree_value::Value::File(f) => assert_eq!(f.size_bytes, Some(11)),
        other => panic!("expected File, got {other:?}"),
    }

    let (page1, tok1) = store
        .get_tree_page("repo-a", &[0x22; 20], "", 1)
        .await
        .unwrap();
    assert_eq!(page1.len(), 1);
    assert_eq!(page1[0].name, "foo");
    assert_eq!(tok1, "foo");
    let (page2, tok2) = store
        .get_tree_page("repo-a", &[0x22; 20], &tok1, 1)
        .await
        .unwrap();
    assert_eq!(page2.len(), 1);
    assert_eq!(page2[0].name, "foo.rs");
    assert!(tok2.is_empty());

    // repo-b cannot probe repo-a's blob until repo-b links it.
    assert!(matches!(
        store.get_file_range("repo-b", &blob_id, 0, None).await,
        Err(StoreError::ObjectNotFound { .. })
    ));
}

#[tokio::test]
async fn test_sqlite_and_local_fs_blob_store_range_reads_and_durability() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("meta.db");
    let blob_dir = dir.path().join("blobs_dir");

    let meta = Arc::new(SqliteMetadataStore::open(&db_path).unwrap());
    let blobs = Arc::new(LocalFsBlobStore::open(&blob_dir).unwrap());
    let store = CompositeStore::new(meta, blobs);

    store
        .register_repo("repo-sql", Some("sql-alias"))
        .await
        .unwrap();
    let blob_id = vec![0x42; 20];
    let payload = Bytes::from_static(b"0123456789abcdef");
    store.put_file("repo-sql", &blob_id, payload).await.unwrap();

    let slice = store
        .get_file_range("repo-sql", &blob_id, 4, Some(6))
        .await
        .unwrap();
    assert_eq!(slice.as_ref(), b"456789");

    let eof_slice = store
        .get_file_range("repo-sql", &blob_id, 16, Some(1))
        .await
        .unwrap();
    assert!(eof_slice.is_empty());

    assert!(matches!(
        store
            .get_file_range("repo-sql", &blob_id, 17, Some(1))
            .await,
        Err(StoreError::OutOfRange(_))
    ));
}

#[tokio::test]
async fn test_put_tree_is_insert_only_per_project() {
    let store = CompositeStore::in_memory();
    store.register_repo("repo-p1", None).await.unwrap();
    store.register_repo("repo-p2", None).await.unwrap();

    let blob_id = vec![0x11; 20];
    let tree_id = vec![0x22; 20];
    store
        .put_file("repo-p1", &blob_id, Bytes::from_static(b"immutable"))
        .await
        .unwrap();

    let original_entry = TreeEntry {
        name: "a.txt".to_string(),
        value: Some(TreeValue {
            value: Some(tree_value::Value::File(File {
                id: blob_id.clone(),
                executable: false,
                copy_id: vec![],
                size_bytes: None,
            })),
        }),
    };
    store
        .put_tree("repo-p1", &tree_id, std::slice::from_ref(&original_entry))
        .await
        .unwrap();

    // Attempting to rewrite the same `tree_id` in the same project leaves the stored
    // entries unchanged.
    let injected_entry = TreeEntry {
        name: "injected_submodule".to_string(),
        value: Some(TreeValue {
            value: Some(tree_value::Value::GitSubmoduleId(vec![0x99; 20])),
        }),
    };
    store
        .put_tree(
            "repo-p1",
            &tree_id,
            &[original_entry.clone(), injected_entry],
        )
        .await
        .unwrap();

    let stored = store.get_tree("repo-p1", &tree_id).await.unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].name, "a.txt");

    // A separate project does not inherit `tree_id` and still enforces dependency checks.
    let err = store
        .put_tree("repo-p2", &tree_id, std::slice::from_ref(&original_entry))
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::MissingDependency {
            missing_kind: "file",
            ..
        }
    ));
}

#[tokio::test]
async fn test_put_commit_batches_parent_generations_and_overlong_prefixes_return_no_match() {
    let store = CompositeStore::in_memory();
    store.register_repo("repo-gen", None).await.unwrap();

    let make_commit = |commit_id: Vec<u8>, change_id: Vec<u8>, parents: Vec<Vec<u8>>| Commit {
        commit_id,
        change_id,
        parent_commit_ids: parents,
        root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
        description: "commit\n".to_string(),
        author: None,
        committer: None,
        predecessors: vec![],
        conflict_labels: vec![],
        secure_sig: None,
        raw_git_commit: vec![],
        external_ids: Default::default(),
    };

    // Build a chain of depth 4 (generations 1..=4) plus 18 sibling root-parented commits.
    let mut chain_prev = cc_common::ROOT_COMMIT_ID_BYTES.to_vec();
    for depth in 1..=4u8 {
        let cid = vec![depth; 20];
        let commit = make_commit(cid.clone(), vec![depth; 16], vec![chain_prev]);
        store.put_commit("repo-gen", &cid, &commit).await.unwrap();
        chain_prev = cid;
    }
    let mut merge_parents = vec![chain_prev];
    for idx in 10..28u8 {
        let cid = vec![idx; 20];
        let commit = make_commit(
            cid.clone(),
            vec![idx; 16],
            vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
        );
        store.put_commit("repo-gen", &cid, &commit).await.unwrap();
        merge_parents.push(cid);
    }

    let merge_id = vec![0xfe; 20];
    let merge_commit = make_commit(merge_id.clone(), vec![0xfe; 16], merge_parents.clone());
    store
        .put_commit("repo-gen", &merge_id, &merge_commit)
        .await
        .unwrap();
    assert_eq!(
        store
            .metadata
            .get_commit_generation("repo-gen", &merge_id)
            .await
            .unwrap(),
        Some(5)
    );

    let mut missing_parents = merge_parents;
    missing_parents.push(vec![0xee; 20]);
    let bad_commit = make_commit(vec![0xfd; 20], vec![0xfd; 16], missing_parents);
    let err = store
        .put_commit("repo-gen", &[0xfd; 20], &bad_commit)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::MissingDependency {
            missing_kind: "parent commit",
            ..
        }
    ));

    // Over-long valid prefixes resolve to NoMatch rather than InvalidArgument.
    assert_eq!(
        store
            .resolve_commit_prefix("repo-gen", &"fe".repeat(32))
            .await
            .unwrap(),
        IdPrefixResolution::NoMatch
    );
    assert_eq!(
        store
            .resolve_change_prefix("repo-gen", &"k".repeat(40), 10)
            .await
            .unwrap(),
        ChangePrefixResolution::NoMatch
    );
    assert_eq!(
        store
            .resolve_operation_prefix("repo-gen", &"00".repeat(64))
            .await
            .unwrap(),
        IdPrefixResolution::NoMatch
    );
}

#[tokio::test]
async fn test_list_commits_full_scan_carries_page1_watermark_across_pages() {
    let dir = tempfile::tempdir().unwrap();
    let sql_meta = Arc::new(SqliteMetadataStore::open(&dir.path().join("scan.db")).unwrap());
    let sql_blobs = Arc::new(LocalFsBlobStore::open(dir.path().join("blobs")).unwrap());
    let stores = [
        CompositeStore::in_memory(),
        CompositeStore::new(sql_meta, sql_blobs),
    ];

    for store in stores {
        store.register_repo("scan-repo", None).await.unwrap();

        let make_commit = |byte: u8| Commit {
            commit_id: vec![byte; 20],
            change_id: vec![byte; 16],
            parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
            root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
            description: format!("commit {byte:#04x}\n"),
            author: None,
            committer: None,
            predecessors: vec![],
            conflict_labels: vec![],
            secure_sig: None,
            raw_git_commit: vec![],
            external_ids: Default::default(),
        };

        // Seed 3 commits whose IDs sort above 0x05 (`0x10`, `0x20`, `0x30`).
        for b in [0x10u8, 0x20, 0x30] {
            let c = make_commit(b);
            store
                .put_commit("scan-repo", &c.commit_id, &c)
                .await
                .unwrap();
        }

        // Read page 1 of a full scan (`page_size = 2`): returns `[0x10, 0x20]` and W1.
        let (p1, tok1, sync1) = store
            .list_commits_with_sync("scan-repo", &[], 2, &[])
            .await
            .unwrap();
        assert_eq!(p1, vec![vec![0x10; 20], vec![0x20; 20]]);
        assert!(!tok1.is_empty());
        assert!(!sync1.is_empty());

        // Mid-scan concurrent writer inserts commit `0x05`, which sorts BEFORE page 1's
        // cursor (`0x20`) and therefore will not appear in page 2 of the key-ordered scan.
        let low_commit = make_commit(0x05);
        store
            .put_commit("scan-repo", &low_commit.commit_id, &low_commit)
            .await
            .unwrap();

        // Final page returns `[0x30]` and MUST preserve `sync1` rather than recomputing an
        // end-of-scan watermark past `0x05`.
        let (p2, tok2, sync2) = store
            .list_commits_with_sync("scan-repo", &tok1, 2, &[])
            .await
            .unwrap();
        assert_eq!(p2, vec![vec![0x30; 20]]);
        assert!(tok2.is_empty());
        assert_eq!(sync2, sync1);

        // Subsequent incremental sync from `sync2` discovers `0x05` instead of losing it.
        let (delta, delta_next, _) = store
            .list_commits_with_sync("scan-repo", &[], 10, &sync2)
            .await
            .unwrap();
        assert_eq!(delta, vec![vec![0x05; 20]]);
        assert!(delta_next.is_empty());
    }
}

#[tokio::test]
async fn test_get_tree_page_enforces_ceiling_when_page_size_is_zero() {
    let dir = tempfile::tempdir().unwrap();
    let sql_meta = Arc::new(SqliteMetadataStore::open(&dir.path().join("tree_cap.db")).unwrap());
    let sql_blobs = Arc::new(LocalFsBlobStore::open(dir.path().join("blobs")).unwrap());
    let stores = [
        CompositeStore::in_memory(),
        CompositeStore::new(sql_meta, sql_blobs),
    ];

    for store in stores {
        store.register_repo("tree-cap-repo", None).await.unwrap();
        let blob_id = vec![0x44; 20];
        store
            .put_file("tree-cap-repo", &blob_id, Bytes::from_static(b"data"))
            .await
            .unwrap();

        let entries: Vec<TreeEntry> = ["a", "b", "c", "d", "e"]
            .into_iter()
            .map(|name| TreeEntry {
                name: name.to_string(),
                value: Some(TreeValue {
                    value: Some(tree_value::Value::File(File {
                        id: blob_id.clone(),
                        executable: false,
                        copy_id: Vec::new(),
                        size_bytes: None,
                    })),
                }),
            })
            .collect();
        let tree_id = vec![0x55; 20];
        store
            .put_tree("tree-cap-repo", &tree_id, &entries)
            .await
            .unwrap();

        // `page_size = 0` must still honor the ceiling and return `next_page_token`.
        let (p1, tok1) = store
            .get_tree_page_with_cap("tree-cap-repo", &tree_id, "", 0, 2)
            .await
            .unwrap();
        assert_eq!(
            p1.iter().map(|e| e.name.as_str()).collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        assert_eq!(tok1, "b");

        // Requesting `page_size > cap` is also clamped to `cap`.
        let (p2, tok2) = store
            .get_tree_page_with_cap("tree-cap-repo", &tree_id, &tok1, 100, 2)
            .await
            .unwrap();
        assert_eq!(
            p2.iter().map(|e| e.name.as_str()).collect::<Vec<_>>(),
            vec!["c", "d"]
        );
        assert_eq!(tok2, "d");

        let (p3, tok3) = store
            .get_tree_page_with_cap("tree-cap-repo", &tree_id, &tok2, 0, 2)
            .await
            .unwrap();
        assert_eq!(
            p3.iter().map(|e| e.name.as_str()).collect::<Vec<_>>(),
            vec!["e"]
        );
        assert!(tok3.is_empty());

        // In-process `get_tree` still returns the complete tree.
        let full = store.get_tree("tree-cap-repo", &tree_id).await.unwrap();
        assert_eq!(full.len(), 5);

        // Batched `find_existing_trees` handles present, missing, and virtual empty trees.
        let missing_tree = vec![0x99; 20];
        let existing = store
            .find_existing_trees(
                "tree-cap-repo",
                &[
                    tree_id.clone(),
                    missing_tree.clone(),
                    cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                ],
            )
            .await
            .unwrap();
        assert!(existing.contains(&tree_id));
        assert!(existing.contains(cc_common::EMPTY_TREE_ID_BYTES.as_slice()));
        assert!(!existing.contains(&missing_tree));
    }
}

#[tokio::test]
async fn test_get_file_range_returns_corruption_when_blob_payload_missing_from_blob_store() {
    let meta = Arc::new(MemoryMetadataStore::new());
    let blobs = Arc::new(MemoryBlobStore::new());
    let store = CompositeStore::new(meta.clone(), blobs);
    store
        .register_repo("corrupt-blob-repo", None)
        .await
        .unwrap();

    let file_id = vec![0x77; 20];
    meta.put_blob_record(
        "corrupt-blob-repo",
        &BlobRecord {
            blob_id: file_id.clone(),
            content_size: 42,
            inline_content: None,
            primary_location: Some("memory://default".to_string()),
            replicated_locations: vec!["memory://default".to_string()],
        },
    )
    .await
    .unwrap();

    let err = store
        .get_file_range("corrupt-blob-repo", &file_id, 0, None)
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Corruption(_)), "{err:?}");
    assert_eq!(tonic::Status::from(err).code(), tonic::Code::DataLoss);
}

#[tokio::test]
async fn test_get_file_range_and_stream_detect_truncated_disk_blobs_and_inline_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let meta = Arc::new(SqliteMetadataStore::open(&dir.path().join("trunc.db")).unwrap());
    let blobs = Arc::new(LocalFsBlobStore::open(dir.path().join("blobs")).unwrap());
    let store = CompositeStore::new(meta.clone(), blobs);
    store.register_repo("trunc-repo", None).await.unwrap();

    // Store a 100-byte blob on LocalFsBlobStore, then truncate it on disk to 40 bytes.
    let file_id = vec![0x88; 20];
    let payload = Bytes::from(vec![0xab; 100]);
    store
        .put_file("trunc-repo", &file_id, payload)
        .await
        .unwrap();
    let hex_id = hex::encode(&file_id);
    let disk_path = dir
        .path()
        .join("blobs")
        .join("blobs")
        .join(&hex_id[0..2])
        .join(&hex_id[2..4])
        .join(&hex_id);
    std::fs::write(&disk_path, vec![0xab; 40]).unwrap();

    // Both offset=0 and offset between truncated disk size (40) and content_size (100)
    // must surface StoreError::Corruption (DATA_LOSS), never silent OK or OutOfRange.
    for (offset, max_bytes) in [(0, None), (0, Some(10)), (60, None), (100, None)] {
        let err = store
            .get_file_range("trunc-repo", &file_id, offset, max_bytes)
            .await
            .unwrap_err();
        assert!(
            matches!(err, StoreError::Corruption(_)),
            "get_file_range(offset={offset}, max_bytes={max_bytes:?}) expected Corruption, got {err:?}"
        );
        assert_eq!(tonic::Status::from(err).code(), tonic::Code::DataLoss);

        let stream_err = store
            .get_file_range_stream("trunc-repo", &file_id, offset, max_bytes)
            .await
            .unwrap_err();
        assert!(
            matches!(stream_err, StoreError::Corruption(_)),
            "get_file_range_stream(offset={offset}, max_bytes={max_bytes:?}) expected Corruption, got {stream_err:?}"
        );
        assert_eq!(
            tonic::Status::from(stream_err).code(),
            tonic::Code::DataLoss
        );
    }

    // Offset strictly past metadata content_size still returns OutOfRange.
    let oor = store
        .get_file_range("trunc-repo", &file_id, 101, None)
        .await
        .unwrap_err();
    assert!(matches!(oor, StoreError::OutOfRange(_)), "{oor:?}");

    // Oversized on-disk blob also surfaces Corruption.
    std::fs::write(&disk_path, vec![0xab; 120]).unwrap();
    let over_err = store
        .get_file_range("trunc-repo", &file_id, 0, None)
        .await
        .unwrap_err();
    assert!(
        matches!(over_err, StoreError::Corruption(_)),
        "{over_err:?}"
    );

    // Mismatched inline_content length vs content_size returns Corruption (DATA_LOSS).
    let inline_id = vec![0x89; 20];
    meta.put_blob_record(
        "trunc-repo",
        &BlobRecord {
            blob_id: inline_id.clone(),
            content_size: 50,
            inline_content: Some(Bytes::from_static(b"short")),
            primary_location: None,
            replicated_locations: Vec::new(),
        },
    )
    .await
    .unwrap();
    let inline_err = store
        .get_file_range("trunc-repo", &inline_id, 0, None)
        .await
        .unwrap_err();
    assert!(
        matches!(inline_err, StoreError::Corruption(_)),
        "{inline_err:?}"
    );
    assert_eq!(
        tonic::Status::from(inline_err).code(),
        tonic::Code::DataLoss
    );
    let inline_stream_err = store
        .get_file_range_stream("trunc-repo", &inline_id, 0, None)
        .await
        .unwrap_err();
    assert!(
        matches!(inline_stream_err, StoreError::Corruption(_)),
        "{inline_stream_err:?}"
    );
    assert_eq!(
        tonic::Status::from(inline_stream_err).code(),
        tonic::Code::DataLoss
    );

    // Default BlobStore::get_blob_range_stream also detects truncated backing blobs
    // (via MemoryBlobStore) for both offset=0 and offset between backing len and content_size.
    let mem_meta = Arc::new(MemoryMetadataStore::new());
    let mem_blobs = Arc::new(MemoryBlobStore::new());
    let mem_store = CompositeStore::new(mem_meta.clone(), mem_blobs.clone());
    mem_store
        .register_repo("mem-trunc-repo", None)
        .await
        .unwrap();
    let mem_file = vec![0x90; 20];
    mem_blobs
        .put_blob(&mem_file, Bytes::from(vec![0x11; 10]))
        .await
        .unwrap();
    mem_meta
        .put_blob_record(
            "mem-trunc-repo",
            &BlobRecord {
                blob_id: mem_file.clone(),
                content_size: 50,
                inline_content: None,
                primary_location: Some("memory://default".to_string()),
                replicated_locations: vec!["memory://default".to_string()],
            },
        )
        .await
        .unwrap();
    for offset in [0u64, 25, 50] {
        assert!(matches!(
            mem_store
                .get_file_range("mem-trunc-repo", &mem_file, offset, None)
                .await,
            Err(StoreError::Corruption(_))
        ));
        assert!(matches!(
            mem_store
                .get_file_range_stream("mem-trunc-repo", &mem_file, offset, None)
                .await,
            Err(StoreError::Corruption(_))
        ));
    }
}

#[tokio::test]
async fn test_list_commits_page_token_rejects_zero_oversized_and_bad_commit_id_len() {
    let store = CompositeStore::in_memory();
    store.register_repo("tok-repo", None).await.unwrap();

    let make_token = |watermark_len: u32, watermark_bytes: usize, after_len: usize| {
        let mut tok = Vec::new();
        tok.extend_from_slice(&watermark_len.to_be_bytes());
        tok.extend(std::iter::repeat_n(0x01u8, watermark_bytes));
        tok.extend(std::iter::repeat_n(0xaa, after_len));
        tok
    };

    let bad_tokens = [
        // Truncated length header
        vec![0, 1],
        // Zero-length watermark
        make_token(0, 0, cc_common::COMMIT_ID_LENGTH),
        // Oversized watermark (> 256 bytes)
        make_token(
            (MAX_WATERMARK_BYTES + 1) as u32,
            MAX_WATERMARK_BYTES + 1,
            cc_common::COMMIT_ID_LENGTH,
        ),
        // Bad after commit_id length (0, 19, 21)
        make_token(8, 8, 0),
        make_token(8, 8, cc_common::COMMIT_ID_LENGTH - 1),
        make_token(8, 8, cc_common::COMMIT_ID_LENGTH + 1),
    ];
    for (idx, tok) in bad_tokens.iter().enumerate() {
        let err = store
            .list_commits_with_sync("tok-repo", tok, 10, &[])
            .await
            .unwrap_err();
        assert!(
            matches!(err, StoreError::InvalidArgument(_)),
            "case {idx} expected InvalidArgument, got {err:?}"
        );
    }
}

#[tokio::test]
async fn test_composite_store_symlink_nul_validation() {
    let dir = tempfile::tempdir().unwrap();
    let stores = [
        CompositeStore::in_memory(),
        CompositeStore::new(
            Arc::new(SqliteMetadataStore::open(&dir.path().join("sym_nul.db")).unwrap()),
            Arc::new(MemoryBlobStore::new()),
        ),
    ];
    for store in stores {
        store.register_repo("sym-repo", None).await.unwrap();
        let sym_id = [0x51u8; cc_common::SYMLINK_ID_LENGTH];
        assert!(matches!(
            store.put_symlink("sym-repo", &sym_id, "bad\0target").await,
            Err(StoreError::InvalidArgument(_))
        ));

        // Seed corrupt NUL target directly in MetadataStore; CompositeStore::get_symlink
        // must surface StoreError::Corruption regardless of metadata backend.
        store
            .metadata()
            .put_symlink("sym-repo", &sym_id, "corrupt\0target")
            .await
            .unwrap();
        assert!(matches!(
            store.get_symlink("sym-repo", &sym_id).await,
            Err(StoreError::Corruption(_))
        ));
    }
}

#[tokio::test]
async fn test_get_commit_with_generation_memory_and_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let metas: [Arc<dyn MetadataStore>; 2] = [
        Arc::new(MemoryMetadataStore::new()),
        Arc::new(SqliteMetadataStore::open(&dir.path().join("commit_gen.db")).unwrap()),
    ];
    let cid = vec![0x61; cc_common::COMMIT_ID_LENGTH];
    let absent_cid = [0x62u8; cc_common::COMMIT_ID_LENGTH];
    let commit = Commit {
        commit_id: cid.clone(),
        change_id: vec![0x71; cc_common::CHANGE_ID_LENGTH],
        parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
        description: "with gen\n".to_string(),
        ..Default::default()
    };

    for meta in metas {
        meta.register_project("proj-cg", None).await.unwrap();
        assert!(
            meta.get_commit_with_generation("proj-cg", &absent_cid)
                .await
                .unwrap()
                .is_none()
        );
        meta.put_commit("proj-cg", &cid, &commit, 42).await.unwrap();
        let (loaded, gen_num) = meta
            .get_commit_with_generation("proj-cg", &cid)
            .await
            .unwrap()
            .expect("stored commit must be returned with generation");
        assert_eq!(loaded, commit);
        assert_eq!(gen_num, 42);
    }
}

#[tokio::test]
async fn test_put_file_from_temp_file_bounds_inline_reads_by_stream_memory_budget() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        CompositeStore::with_config(
            Arc::new(MemoryMetadataStore::new()),
            Arc::new(MemoryBlobStore::new()),
            CompositeStoreConfig {
                inline_blob_threshold: 1024,
                validate_dependencies: true,
            },
        )
        .with_stream_memory_budget(256),
    );
    store.register_project("proj-budget", None).await.unwrap();

    // Spilled inline-eligible file <= budget waits for stream_memory_budget permits,
    // then completes inline and releases permits.
    let small_path = dir.path().join("small.bin");
    let small_bytes = [0xabu8; 128];
    std::fs::write(&small_path, small_bytes).unwrap();
    let small_id = [0x11u8; cc_common::FILE_ID_LENGTH];

    let held = store
        .try_acquire_stream_memory_permit(200)
        .expect("200 of 256 permits should be available");
    let store_clone = Arc::clone(&store);
    let small_path_clone = small_path.clone();
    let task = tokio::spawn(async move {
        store_clone
            .put_file_from_temp_file(
                "proj-budget",
                &small_id,
                &small_path_clone,
                small_bytes.len() as u64,
            )
            .await
    });
    tokio::task::yield_now().await;
    assert!(
        !task.is_finished(),
        "inline finalize must wait when stream_memory_budget has insufficient permits"
    );
    drop(held);
    task.await.unwrap().unwrap();
    assert_eq!(store.stream_memory_budget().available_permits(), 256);
    let small_rec = store
        .metadata()
        .get_blob_record("proj-budget", &small_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        small_rec.inline_content.is_some(),
        "file within both inline threshold and budget should store inline"
    );

    // Spilled file larger than total stream_memory_budget (even if <= inline_blob_threshold)
    // must not deadlock and instead falls back to external BlobStore.
    let medium_path = dir.path().join("medium.bin");
    let medium_bytes = [0xcdu8; 512];
    std::fs::write(&medium_path, medium_bytes).unwrap();
    let medium_id = [0x22u8; cc_common::FILE_ID_LENGTH];
    store
        .put_file_from_temp_file(
            "proj-budget",
            &medium_id,
            &medium_path,
            medium_bytes.len() as u64,
        )
        .await
        .unwrap();
    let medium_rec = store
        .metadata()
        .get_blob_record("proj-budget", &medium_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        medium_rec.inline_content.is_none(),
        "file exceeding stream_memory_budget must fall back to external BlobStore"
    );
    assert_eq!(
        store
            .get_file_range("proj-budget", &medium_id, 0, None)
            .await
            .unwrap()
            .as_ref(),
        medium_bytes.as_slice()
    );
}
