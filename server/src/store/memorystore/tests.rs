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

use std::sync::{Arc, Barrier};

use super::*;
use crate::store::CompositeStore;
use crate::store::MetadataStore;
use bytes::Bytes;

#[test]
fn test_memory_store_concurrent_blob_writes_and_tree_reads_do_not_deadlock() {
    let store = Arc::new(CompositeStore::in_memory());
    let seed_blob = vec![0xaa; 20];
    futures::executor::block_on(async {
        store.register_repo("repo-stress", None).await.unwrap();
        store
            .put_file("repo-stress", &seed_blob, Bytes::from_static(b"seed"))
            .await
            .unwrap();
    });

    let num_threads = 6;
    let iterations = 400u32;
    let start = Barrier::new(num_threads);

    std::thread::scope(|s| {
        for worker in 0..num_threads {
            let store = Arc::clone(&store);
            let start = &start;
            let seed_blob = seed_blob.clone();
            s.spawn(move || {
                start.wait();
                for i in 0..iterations {
                    futures::executor::block_on(async {
                        match worker % 3 {
                            0 => {
                                // Acquires `project_blobs` then `blobs` in `put_blob_record`.
                                let mut blob_id = vec![0u8; 20];
                                blob_id[0] = worker as u8;
                                blob_id[1..5].copy_from_slice(&i.to_be_bytes());
                                store
                                    .put_file(
                                        "repo-stress",
                                        &blob_id,
                                        Bytes::from_static(b"payload"),
                                    )
                                    .await
                                    .unwrap();
                            }
                            1 => {
                                // Acquires `project_blobs` then `blobs` in `get_blob_sizes_batch`.
                                let mut tree_id = vec![0x10u8; 20];
                                tree_id[0] = worker as u8;
                                tree_id[1..5].copy_from_slice(&i.to_be_bytes());
                                let entry = TreeEntry {
                                    name: "seed.txt".to_string(),
                                    value: Some(TreeValue {
                                        value: Some(tree_value::Value::File(File {
                                            id: seed_blob.clone(),
                                            executable: false,
                                            copy_id: vec![],
                                            size_bytes: None,
                                        })),
                                    }),
                                };
                                store
                                    .put_tree("repo-stress", &tree_id, &[entry])
                                    .await
                                    .unwrap();
                            }
                            _ => {
                                // Acquires `project_blobs` then `blobs` in `get_blob_record`.
                                let bytes = store
                                    .get_file_range("repo-stress", &seed_blob, 0, None)
                                    .await
                                    .unwrap();
                                assert_eq!(bytes.as_ref(), b"seed");
                            }
                        }
                    });
                }
            });
        }
    });
}

fn test_view() -> View {
    View {
        head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
        wc_commit_ids: HashMap::new(),
        local_bookmarks: HashMap::new(),
        remote_bookmarks: HashMap::new(),
    }
}

fn test_op(view_id: &[u8], parents: Vec<Vec<u8>>, desc: &str) -> Operation {
    Operation {
        view_id: view_id.to_vec(),
        parents,
        metadata: Some(OperationMetadata {
            description: desc.to_string(),
            ..Default::default()
        }),
        commit_predecessors: Vec::new(),
        commit_predecessors_set: true,
    }
}

#[tokio::test]
async fn test_memory_publish_operation_tracks_published_flag_accurately() {
    let meta = MemoryMetadataStore::new();
    meta.register_repo("repo-pub", None).await.unwrap();
    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let view_id = vec![0xaa; 16];
    let view = test_view();
    meta.put_view("repo-pub", &view_id, &view).await.unwrap();

    // WriteOperation(op_a) -> sibling advances old_ids -> PublishOperation(op_a, [root])
    // must still add op_a to OpHeads instead of silently dropping it.
    let op_a_id = vec![0x11; 16];
    let op_a = test_op(&view_id, vec![root_op.clone()], "op_a");
    meta.put_operation("repo-pub", &op_a_id, &op_a)
        .await
        .unwrap();

    let op_sib_id = vec![0x22; 16];
    let op_sib = test_op(&view_id, vec![root_op.clone()], "op_sib");
    let heads = meta
        .publish_operation_atomic(
            "repo-pub",
            &view_id,
            &view,
            &op_sib_id,
            &op_sib,
            std::slice::from_ref(&root_op),
        )
        .await
        .unwrap();
    assert_eq!(heads, vec![op_sib_id.clone()]);

    let heads_after_a = meta
        .publish_operation_atomic(
            "repo-pub",
            &view_id,
            &view,
            &op_a_id,
            &op_a,
            std::slice::from_ref(&root_op),
        )
        .await
        .unwrap();
    assert_eq!(heads_after_a, vec![op_sib_id.clone(), op_a_id.clone()]);

    // WriteOperation(op_b) -> PublishOperation(op_b, old_ids=[]) adds op_b to OpHeads.
    let op_b_id = vec![0x33; 16];
    let op_b = test_op(&view_id, vec![root_op.clone()], "op_b");
    meta.put_operation("repo-pub", &op_b_id, &op_b)
        .await
        .unwrap();
    let heads_after_b = meta
        .publish_operation_atomic("repo-pub", &view_id, &view, &op_b_id, &op_b, &[])
        .await
        .unwrap();
    assert_eq!(
        heads_after_b,
        vec![op_sib_id.clone(), op_a_id.clone(), op_b_id.clone()]
    );

    // Supersede all three via compare_and_swap_op_heads -> retry of PublishOperation(op_a)
    // must NOT resurrect op_a.
    let op_merge_id = vec![0x44; 16];
    let op_merge = test_op(
        &view_id,
        vec![op_sib_id.clone(), op_a_id.clone(), op_b_id.clone()],
        "merge",
    );
    meta.put_operation("repo-pub", &op_merge_id, &op_merge)
        .await
        .unwrap();
    meta.compare_and_swap_op_heads(
        "repo-pub",
        &[op_sib_id.clone(), op_a_id.clone(), op_b_id.clone()],
        &op_merge_id,
    )
    .await
    .unwrap();

    let retry_a = meta
        .publish_operation_atomic(
            "repo-pub",
            &view_id,
            &view,
            &op_a_id,
            &op_a,
            std::slice::from_ref(&root_op),
        )
        .await
        .unwrap();
    assert_eq!(retry_a, vec![op_merge_id.clone()]);

    // Re-calling put_operation(op_a) and put_operation(op_merge) must NOT reset
    // published back to false.
    meta.put_operation("repo-pub", &op_a_id, &op_a)
        .await
        .unwrap();
    meta.put_operation("repo-pub", &op_merge_id, &op_merge)
        .await
        .unwrap();
    let retry_a_after_rewrite = meta
        .publish_operation_atomic(
            "repo-pub",
            &view_id,
            &view,
            &op_a_id,
            &op_a,
            std::slice::from_ref(&root_op),
        )
        .await
        .unwrap();
    assert_eq!(retry_a_after_rewrite, vec![op_merge_id.clone()]);

    // Also verify update_op_heads marks op_next as published so superseding + retry won't resurrect.
    let op_next_id = vec![0x55; 16];
    let op_next = test_op(&view_id, vec![op_merge_id.clone()], "next");
    meta.put_operation("repo-pub", &op_next_id, &op_next)
        .await
        .unwrap();
    meta.update_op_heads("repo-pub", std::slice::from_ref(&op_merge_id), &op_next_id)
        .await
        .unwrap();
    let retry_merge = meta
        .publish_operation_atomic("repo-pub", &view_id, &view, &op_merge_id, &op_merge, &[])
        .await
        .unwrap();
    assert_eq!(retry_merge, vec![op_next_id]);
}

#[tokio::test]
async fn test_memory_register_project_and_repo_prevent_namespace_shadowing() {
    let meta = MemoryMetadataStore::new();
    meta.register_repo_in_project("repo-x", "proj-a", None)
        .await
        .unwrap();

    // Creating a project with the ID of an existing repo bound to another project fails.
    let err = meta.register_project("repo-x", None).await.unwrap_err();
    assert_eq!(tonic::Status::from(err).code(), tonic::Code::AlreadyExists);
    assert!(!meta.is_project_registered("repo-x").await.unwrap());

    // Binding another repo to `project_id = "repo-x"` also fails.
    let err = meta
        .register_repo_in_project("repo-y", "repo-x", None)
        .await
        .unwrap_err();
    assert_eq!(tonic::Status::from(err).code(), tonic::Code::AlreadyExists);
    assert!(meta.get_repo_project_id("repo-y").await.unwrap().is_none());

    // Binding a repo whose ID collides with an existing different project fails.
    let err = meta
        .register_repo_in_project("proj-a", "proj-b", None)
        .await
        .unwrap_err();
    assert_eq!(tonic::Status::from(err).code(), tonic::Code::AlreadyExists);

    // Existing project re-registration and self-bound single-repo case continue to work.
    meta.register_project("proj-a", Some("Project A"))
        .await
        .unwrap();
    meta.register_repo_in_project("solo", "solo", None)
        .await
        .unwrap();
    meta.register_project("solo", Some("Solo Project"))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_memory_list_workspaces_page_orders_and_paginates_by_keyset() {
    let meta = MemoryMetadataStore::new();
    meta.register_repo("ws-repo", None).await.unwrap();
    // Distinct users do not affect keyset ordering (ordered strictly by `workspace_name`).
    let keys = [
        ("bob", "ws-2"),
        ("agent-42", "ws-b"),
        ("alice", "ws-a"),
        ("", "ws-1"),
    ];
    for (user, ws_name) in keys {
        #[allow(deprecated)]
        meta.put_workspace(&WorkspaceState {
            repo_id: "ws-repo".to_string(),
            user: user.to_string(),
            workspace_name: ws_name.to_string(),
            commit_id: cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
            operation_id: cc_common::ROOT_OPERATION_ID_BYTES.to_vec(),
            tree_id: cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
            tree_ids: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
            conflict_labels: Vec::new(),
            sparse_patterns: vec![String::new()],
            sparse_patterns_set: true,
        })
        .await
        .unwrap();
    }

    assert_eq!(
        meta.list_workspaces_page("ws-repo", None, 0, None)
            .await
            .unwrap(),
        (Vec::new(), false)
    );
    let (p1, has_more1) = meta
        .list_workspaces_page("ws-repo", None, 2, None)
        .await
        .unwrap();
    assert!(has_more1);
    assert_eq!(
        p1.iter()
            .map(|w| (w.user.as_str(), w.workspace_name.as_str()))
            .collect::<Vec<_>>(),
        vec![("", "ws-1"), ("bob", "ws-2")]
    );
    let (p2, has_more2) = meta
        .list_workspaces_page("ws-repo", Some("ws-2"), 2, None)
        .await
        .unwrap();
    assert!(!has_more2);
    assert_eq!(
        p2.iter()
            .map(|w| (w.user.as_str(), w.workspace_name.as_str()))
            .collect::<Vec<_>>(),
        vec![("alice", "ws-a"), ("agent-42", "ws-b")]
    );
    let (p3, has_more3) = meta
        .list_workspaces_page("ws-repo", Some("ws-b"), 2, None)
        .await
        .unwrap();
    assert!(p3.is_empty());
    assert!(!has_more3);
}

#[tokio::test]
async fn test_memory_register_repo_normalizes_name_and_preserves_existing_on_whitespace() {
    let meta = MemoryMetadataStore::new();
    meta.register_repo_in_project("repo-norm", "proj-norm", Some("  Main Repo  "))
        .await
        .unwrap();
    assert_eq!(
        meta.repos
            .lock()
            .unwrap()
            .get("repo-norm")
            .and_then(|m| m.name.as_deref()),
        Some("Main Repo")
    );

    // Re-registering with a whitespace-only name must preserve the existing non-empty name.
    meta.register_repo_in_project("repo-norm", "proj-norm", Some("   "))
        .await
        .unwrap();
    assert_eq!(
        meta.repos
            .lock()
            .unwrap()
            .get("repo-norm")
            .and_then(|m| m.name.as_deref()),
        Some("Main Repo")
    );

    // Initial whitespace-only name normalizes to None.
    meta.register_repo_in_project("repo-blank", "proj-norm", Some(" \t "))
        .await
        .unwrap();
    assert_eq!(
        meta.repos
            .lock()
            .unwrap()
            .get("repo-blank")
            .and_then(|m| m.name.as_deref()),
        None
    );
}
