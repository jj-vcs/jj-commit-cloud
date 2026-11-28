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

use prost::Message as _;
use tonic::Code;

use super::*;
use crate::store::{MemoryBlobStore, SqliteMetadataStore};

fn base_ws(workspace_name: &str) -> WorkspaceState {
    WorkspaceState {
        repo_id: "repo-1".to_string(),
        user: "alice".to_string(),
        workspace_name: workspace_name.to_string(),
        commit_id: cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
        operation_id: cc_common::ROOT_OPERATION_ID_BYTES.to_vec(),
        tree_id: cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
        tree_ids: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
        conflict_labels: Vec::new(),
        sparse_patterns: vec![String::new()],
        sparse_patterns_set: true,
    }
}

#[tokio::test]
async fn test_workspace_conflict_labels_sqlite_cas_cursor_and_advisory_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("ws.db");
    let sqlite_meta = Arc::new(SqliteMetadataStore::open(&db_path).unwrap());
    let sqlite_store = Arc::new(CompositeStore::new(
        sqlite_meta,
        Arc::new(MemoryBlobStore::new()),
    ));
    sqlite_store
        .register_repo_in_project("ws-repo", Some("ws-proj"), None)
        .await
        .unwrap();
    let sqlite_svc = CommitCloudWorkspaceService::new(sqlite_store);

    // Advisory DeleteWorkspace returns OK(false) on an unregistered repo.
    let del = sqlite_svc
        .delete_workspace(Request::new(DeleteWorkspaceRequest {
            repo_id: "missing-repo".to_string(),
            workspace_name: "default".to_string(),
        }))
        .await
        .expect("DeleteWorkspace on missing repo must be advisory OK(false)")
        .into_inner();
    assert!(!del.existed);

    // UpdateWorkspace persists conflict_labels on SQLite and round-trips via GetWorkspace and ListWorkspaces.
    let empty_tree = cc_common::EMPTY_TREE_ID_BYTES.to_vec();
    let ws = WorkspaceState {
        repo_id: "ws-repo".to_string(),
        user: "alice@example.com".to_string(),
        workspace_name: "default".to_string(),
        commit_id: cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
        operation_id: cc_common::ROOT_OPERATION_ID_BYTES.to_vec(),
        tree_id: empty_tree.clone(),
        tree_ids: vec![empty_tree.clone(), empty_tree.clone(), empty_tree.clone()],
        conflict_labels: vec!["left".to_string(), "base".to_string(), "right".to_string()],
        sparse_patterns: vec!["src".to_string()],
        sparse_patterns_set: true,
    };
    let updated = sqlite_svc
        .update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(ws.clone()),
            expected_operation_id: Some(Vec::new()),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(updated.success);
    assert_eq!(updated.workspace.as_ref(), Some(&ws));

    // Length-1 `[""]` on a 3-tree conflicted workspace is rejected (must be 0 or tree_ids.len()).
    let mut bad_len_labels = ws.clone();
    bad_len_labels.conflict_labels = vec![String::new()];
    let err = sqlite_svc
        .update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(bad_len_labels),
            expected_operation_id: None,
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    let fetched = sqlite_svc
        .get_workspace(Request::new(GetWorkspaceRequest {
            repo_id: "ws-repo".to_string(),
            workspace_name: "default".to_string(),
        }))
        .await
        .unwrap()
        .into_inner()
        .workspace
        .unwrap();
    assert_eq!(fetched.conflict_labels, ws.conflict_labels);
    assert_eq!(fetched.user, "alice@example.com");

    // MemoryMetadataStore CAS must reject stale expected_operation_id when conflict_labels differ.
    let mem_store = Arc::new(CompositeStore::in_memory());
    mem_store
        .register_repo_in_project("mem-repo", Some("mem-proj"), None)
        .await
        .unwrap();
    let mem_svc = CommitCloudWorkspaceService::new(mem_store);
    let base_ws = WorkspaceState {
        repo_id: "mem-repo".to_string(),
        user: "bob@example.com".to_string(),
        workspace_name: "default".to_string(),
        commit_id: cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
        operation_id: cc_common::ROOT_OPERATION_ID_BYTES.to_vec(),
        tree_id: empty_tree.clone(),
        tree_ids: vec![empty_tree.clone(), empty_tree.clone(), empty_tree.clone()],
        conflict_labels: Vec::new(),
        sparse_patterns: Vec::new(),
        sparse_patterns_set: true,
    };
    mem_svc
        .update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(base_ws.clone()),
            expected_operation_id: Some(Vec::new()),
        }))
        .await
        .unwrap();

    let mut labeled_ws = base_ws;
    labeled_ws.conflict_labels = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let stale_cas = mem_svc
        .update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(labeled_ws),
            expected_operation_id: Some(vec![0xFF; cc_common::OPERATION_ID_LENGTH]),
        }))
        .await
        .unwrap_err();
    assert_eq!(stale_cas.code(), Code::Aborted);

    // Cursor decoding accepts any non-empty NUL-free valid UTF-8 `workspace_name` (including
    // slashes and legacy persisted values) so pagination never dead-ends, while rejecting empty,
    // legacy v1, unprefixed hex, NUL-containing, malformed hex, oversized, and invalid UTF-8
    // tokens with `InvalidArgument`.
    for w in ["bad\x01ws", "../ws", "a/b", "default"] {
        let cursor = encode_workspace_cursor(w);
        assert!(cursor.starts_with(WORKSPACE_CURSOR_PREFIX));
        assert_eq!(decode_workspace_cursor(&cursor).unwrap(), w);
    }
    let oversized_unprefixed = "61".repeat((MAX_CURSOR_HEX_LEN / 2) + 1);
    let oversized_prefixed = format!("{WORKSPACE_CURSOR_PREFIX}{oversized_unprefixed}");
    for bad_cursor in [
        "",
        WORKSPACE_CURSOR_PREFIX,
        "not-hex",
        "v2:not-hex",
        "v2:abc",
        "v2:ff",
        "00000005616c6963657773",
        "0001",
        "64656661756c74",
        "v2:00",
        "v2:610062",
        oversized_unprefixed.as_str(),
        oversized_prefixed.as_str(),
    ] {
        let err = decode_workspace_cursor(bad_cursor).unwrap_err();
        assert_eq!(
            err.code(),
            Code::InvalidArgument,
            "expected InvalidArgument for cursor {bad_cursor:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_sqlite_store_cas_and_concurrent_race() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("cas_race.db");
    let store = Arc::new(CompositeStore::new(
        Arc::new(SqliteMetadataStore::open(&db_path).unwrap()),
        Arc::new(MemoryBlobStore::new()),
    ));
    store
        .register_repo_in_project("repo-1", Some("proj-1"), None)
        .await
        .unwrap();
    let svc = CommitCloudWorkspaceService::new(store.clone());

    let op_init = vec![0x01; cc_common::OPERATION_ID_LENGTH];
    let writer_ops: Vec<Vec<u8>> = (0..8u8)
        .map(|i| vec![0x10 + i; cc_common::OPERATION_ID_LENGTH])
        .collect();
    for op_id in std::iter::once(&op_init).chain(writer_ops.iter()) {
        store
            .put_operation(
                "repo-1",
                op_id,
                &cc_common::op_store::Operation {
                    view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
                    parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                    metadata: None,
                    commit_predecessors: Vec::new(),
                    commit_predecessors_set: true,
                },
            )
            .await
            .unwrap();
    }

    // Create-only CAS succeeds on insert and is idempotent on exact retry.
    let mut initial_ws = base_ws("ws");
    initial_ws.operation_id = op_init.clone();
    svc.update_workspace(Request::new(UpdateWorkspaceRequest {
        workspace: Some(initial_ws.clone()),
        expected_operation_id: Some(Vec::new()),
    }))
    .await
    .unwrap();
    svc.update_workspace(Request::new(UpdateWorkspaceRequest {
        workspace: Some(initial_ws.clone()),
        expected_operation_id: Some(Vec::new()),
    }))
    .await
    .unwrap();

    // Race 8 concurrent CAS writers from `op_init`: exactly 1 wins, 7 receive ABORTED.
    let handles: Vec<_> = writer_ops
        .iter()
        .cloned()
        .map(|op_id| {
            let svc = svc.clone();
            let mut next_ws = initial_ws.clone();
            next_ws.operation_id = op_id.clone();
            let expected = op_init.clone();
            tokio::spawn(async move {
                let res = svc
                    .update_workspace(Request::new(UpdateWorkspaceRequest {
                        workspace: Some(next_ws),
                        expected_operation_id: Some(expected),
                    }))
                    .await;
                (op_id, res)
            })
        })
        .collect();

    let results = futures::future::join_all(handles).await;
    let mut winners = Vec::new();
    let mut aborted_count = 0usize;
    for joined in results {
        let (op_id, res) = joined.unwrap();
        match res {
            Ok(_) => winners.push(op_id),
            Err(status) => {
                assert_eq!(status.code(), Code::Aborted);
                aborted_count += 1;
            }
        }
    }
    assert_eq!(winners.len(), 1, "exactly one racing CAS writer must win");
    assert_eq!(aborted_count, writer_ops.len() - 1);

    let persisted = svc
        .get_workspace(Request::new(GetWorkspaceRequest {
            repo_id: "repo-1".to_string(),
            workspace_name: "ws".to_string(),
        }))
        .await
        .unwrap()
        .into_inner()
        .workspace
        .unwrap();
    assert_eq!(persisted.operation_id, winners[0]);
}

#[tokio::test]
async fn test_check_working_copy_changes_conflicted_three_term_trees() {
    let store = Arc::new(CompositeStore::in_memory());
    store
        .register_repo_in_project("repo-1", Some("proj-1"), None)
        .await
        .unwrap();
    let svc = CommitCloudWorkspaceService::new(store.clone());
    let t1 = vec![0x11; cc_common::EMPTY_TREE_ID_BYTES.len()];
    let t2 = vec![0x22; cc_common::EMPTY_TREE_ID_BYTES.len()];
    let t3 = vec![0x33; cc_common::EMPTY_TREE_ID_BYTES.len()];
    let t4 = vec![0x44; cc_common::EMPTY_TREE_ID_BYTES.len()];
    for t in [&t1, &t2, &t3, &t4] {
        store.put_tree("proj-1", t, &[]).await.unwrap();
    }

    let commit_id = vec![0xaa; cc_common::COMMIT_ID_LENGTH];
    store
        .put_commit(
            "proj-1",
            &commit_id,
            &cc_common::backend::Commit {
                commit_id: commit_id.clone(),
                change_id: vec![0xbb; cc_common::CHANGE_ID_LENGTH],
                parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: vec![t1.clone(), t2.clone(), t3.clone()],
                description: "conflicted commit".to_string(),
                conflict_labels: vec!["side-a".into(), "base".into(), "side-b".into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Matching 3-term conflicted tree reports has_changes = false.
    let mut ws = base_ws("conflicted-ws");
    ws.commit_id = commit_id.clone();
    ws.tree_id = t1.clone();
    ws.tree_ids = vec![t1.clone(), t2.clone(), t3.clone()];
    ws.conflict_labels = vec!["side-a".into(), "base".into(), "side-b".into()];
    svc.update_workspace(Request::new(UpdateWorkspaceRequest {
        workspace: Some(ws.clone()),
        expected_operation_id: None,
    }))
    .await
    .unwrap();

    let matching = svc
        .check_working_copy_changes(Request::new(CheckWorkingCopyChangesRequest {
            repo_id: "repo-1".to_string(),
            workspace_name: "conflicted-ws".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!matching.has_changes);
    assert_eq!(matching.current_tree_id, t1);
    assert_eq!(matching.commit_tree_id, t1);
    assert_eq!(
        matching.current_tree_ids,
        vec![t1.clone(), t2.clone(), t3.clone()]
    );
    assert_eq!(
        matching.commit_tree_ids,
        vec![t1.clone(), t2.clone(), t3.clone()]
    );

    // Differing non-primary side (with identical primary tree_id == t1) reports has_changes = true.
    ws.tree_ids = vec![t1.clone(), t2.clone(), t4.clone()];
    svc.update_workspace(Request::new(UpdateWorkspaceRequest {
        workspace: Some(ws),
        expected_operation_id: None,
    }))
    .await
    .unwrap();

    let diverged = svc
        .check_working_copy_changes(Request::new(CheckWorkingCopyChangesRequest {
            repo_id: "repo-1".to_string(),
            workspace_name: "conflicted-ws".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(diverged.has_changes);
    assert_eq!(diverged.current_tree_id, diverged.commit_tree_id);
    assert_eq!(
        diverged.current_tree_ids,
        vec![t1.clone(), t2.clone(), t4.clone()]
    );
    assert_eq!(diverged.commit_tree_ids, vec![t1, t2, t3]);
}

#[tokio::test]
async fn test_update_workspace_normalizes_empty_operation_id_and_validates_commit_id_early() {
    let store = Arc::new(CompositeStore::in_memory());
    store.register_repo("repo-norm", None).await.unwrap();
    let svc = CommitCloudWorkspaceService::new(store.clone());

    // Malformed commit_id fails with INVALID_ARGUMENT BEFORE storage lookup (even on an
    // unregistered repo or with a non-existent operation_id).
    let mut bad_commit_unreg = base_ws("ws-bad");
    bad_commit_unreg.repo_id = "unregistered-repo".to_string();
    bad_commit_unreg.commit_id = vec![0xaa; 5];
    bad_commit_unreg.operation_id = vec![0xee; cc_common::OPERATION_ID_LENGTH];
    let err = svc
        .update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(bad_commit_unreg),
            expected_operation_id: None,
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    // Empty ws.operation_id is normalized to canonical ROOT_OPERATION_ID_BYTES in the
    // response and can be passed directly as expected_operation_id on the next CAS update.
    let mut ws = base_ws("ws-norm");
    ws.repo_id = "repo-norm".to_string();
    ws.operation_id = Vec::new();
    ws.commit_id = Vec::new();
    let created = svc
        .update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(ws),
            expected_operation_id: Some(Vec::new()),
        }))
        .await
        .unwrap()
        .into_inner();
    let returned_ws = created.workspace.expect("response workspace");
    assert_eq!(
        returned_ws.operation_id,
        cc_common::ROOT_OPERATION_ID_BYTES.to_vec()
    );
    assert_eq!(
        returned_ws.commit_id,
        cc_common::ROOT_COMMIT_ID_BYTES.to_vec()
    );

    let next_op = vec![0x42; cc_common::OPERATION_ID_LENGTH];
    store
        .put_operation(
            "repo-norm",
            &next_op,
            &cc_common::op_store::Operation {
                view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
                parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                metadata: None,
                commit_predecessors: Vec::new(),
                commit_predecessors_set: true,
            },
        )
        .await
        .unwrap();
    let mut next_ws = returned_ws.clone();
    next_ws.operation_id = next_op.clone();
    let updated = svc
        .update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(next_ws),
            expected_operation_id: Some(returned_ws.operation_id),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(updated.workspace.unwrap().operation_id, next_op);
}

#[tokio::test]
async fn test_memory_store_cas_and_concurrent_race() {
    let store = Arc::new(CompositeStore::in_memory());
    store
        .register_repo_in_project("repo-1", Some("proj-1"), None)
        .await
        .unwrap();
    let svc = CommitCloudWorkspaceService::new(store.clone());

    let op1 = vec![0x11; cc_common::OPERATION_ID_LENGTH];
    let op2 = vec![0x22; cc_common::OPERATION_ID_LENGTH];
    let op3 = vec![0x33; cc_common::OPERATION_ID_LENGTH];
    for op_id in [&op1, &op2, &op3] {
        store
            .put_operation(
                "repo-1",
                op_id,
                &cc_common::op_store::Operation {
                    view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
                    parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                    metadata: None,
                    commit_predecessors: Vec::new(),
                    commit_predecessors_set: true,
                },
            )
            .await
            .unwrap();
    }

    // Create-only CAS succeeds when absent, then succeeds idempotently on exact retry.
    let mut ws1 = base_ws("ws");
    ws1.operation_id = op1.clone();
    svc.update_workspace(Request::new(UpdateWorkspaceRequest {
        workspace: Some(ws1.clone()),
        expected_operation_id: Some(Vec::new()),
    }))
    .await
    .unwrap();
    svc.update_workspace(Request::new(UpdateWorkspaceRequest {
        workspace: Some(ws1.clone()),
        expected_operation_id: Some(Vec::new()),
    }))
    .await
    .unwrap();

    // Concurrent racing CAS writers from op1: exactly one wins, one gets ABORTED.
    let mut ws2 = ws1.clone();
    ws2.operation_id = op2;
    let mut ws3 = ws1;
    ws3.operation_id = op3;
    let (r1, r2) = tokio::join!(
        svc.update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(ws2),
            expected_operation_id: Some(op1.clone()),
        })),
        svc.update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(ws3),
            expected_operation_id: Some(op1),
        })),
    );
    let ok_count = [r1.is_ok(), r2.is_ok()].into_iter().filter(|b| *b).count();
    assert_eq!(ok_count, 1, "exactly one racing CAS writer must win");
    let err = r1.err().or_else(|| r2.err()).unwrap();
    assert_eq!(err.code(), Code::Aborted);
}

#[tokio::test]
async fn test_validation_matrix_conflict_labels_preconditions_and_byte_budget() {
    let store = Arc::new(CompositeStore::in_memory());
    store
        .register_repo_in_project("repo-1", Some("proj-1"), None)
        .await
        .unwrap();
    let svc = CommitCloudWorkspaceService::new(store.clone());

    let t1 = vec![0x11; 20];
    let t2 = vec![0x22; 20];
    let t3 = vec![0x33; 20];
    for t in [&t1, &t2, &t3] {
        store.put_tree("proj-1", t, &[]).await.unwrap();
    }

    // Round-trip conflicted workspace with labels via UpdateWorkspaceResponse and GetWorkspace.
    let mut conflicted = base_ws("conflicted");
    conflicted.tree_id = t1.clone();
    conflicted.tree_ids = vec![t1.clone(), t2.clone(), t3.clone()];
    conflicted.conflict_labels = vec!["side-a".into(), "base".into(), "side-b".into()];
    let updated = svc
        .update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(conflicted.clone()),
            expected_operation_id: None,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(updated.success);
    assert_eq!(updated.workspace.as_ref(), Some(&conflicted));

    // Oversized conflict_labels rejected.
    let mut huge_labels = conflicted.clone();
    huge_labels.conflict_labels = vec!["a".repeat(30_000), "b".repeat(30_000), "c".repeat(30_000)];
    assert_eq!(
        svc.update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(huge_labels),
            expected_operation_id: None,
        }))
        .await
        .unwrap_err()
        .code(),
        Code::InvalidArgument
    );

    // Unicode C1 control char in workspace_name rejected.
    let c1_ws = base_ws("bad\u{0085}name");
    assert_eq!(
        svc.update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(c1_ws),
            expected_operation_id: None,
        }))
        .await
        .unwrap_err()
        .code(),
        Code::InvalidArgument
    );

    // tree_id != tree_ids[0], even tree_ids, bad sparse pattern, bad expected_operation_id.
    let mut mismatch_tree = base_ws("m1");
    mismatch_tree.tree_id = t2.clone();
    mismatch_tree.tree_ids = vec![t1.clone()];
    assert_eq!(
        svc.update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(mismatch_tree),
            expected_operation_id: None,
        }))
        .await
        .unwrap_err()
        .code(),
        Code::InvalidArgument
    );

    let mut even_trees = base_ws("m2");
    even_trees.tree_id = t1.clone();
    even_trees.tree_ids = vec![t1, t2];
    assert_eq!(
        svc.update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(even_trees),
            expected_operation_id: None,
        }))
        .await
        .unwrap_err()
        .code(),
        Code::InvalidArgument
    );

    let mut bad_sparse = base_ws("m3");
    bad_sparse.sparse_patterns = vec!["../escape".to_string()];
    assert_eq!(
        svc.update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(bad_sparse),
            expected_operation_id: None,
        }))
        .await
        .unwrap_err()
        .code(),
        Code::InvalidArgument
    );

    assert_eq!(
        svc.update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(base_ws("m4")),
            expected_operation_id: Some(vec![0xaa; 3]),
        }))
        .await
        .unwrap_err()
        .code(),
        Code::InvalidArgument
    );

    // Missing non-root Commit -> FAILED_PRECONDITION.
    let mut missing_commit = base_ws("m5");
    missing_commit.commit_id = vec![0x99; cc_common::COMMIT_ID_LENGTH];
    assert_eq!(
        svc.update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(missing_commit),
            expected_operation_id: None,
        }))
        .await
        .unwrap_err()
        .code(),
        Code::FailedPrecondition
    );

    // Missing non-root View referenced by Operation -> FAILED_PRECONDITION.
    let op_missing_view = vec![0x44; cc_common::OPERATION_ID_LENGTH];
    store
        .metadata()
        .put_operation(
            "repo-1",
            &op_missing_view,
            &cc_common::op_store::Operation {
                view_id: vec![0x55; cc_common::VIEW_ID_LENGTH],
                parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                metadata: None,
                commit_predecessors: Vec::new(),
                commit_predecessors_set: true,
            },
        )
        .await
        .unwrap();
    let mut missing_view_ws = base_ws("m6");
    missing_view_ws.operation_id = op_missing_view;
    assert_eq!(
        svc.update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(missing_view_ws),
            expected_operation_id: None,
        }))
        .await
        .unwrap_err()
        .code(),
        Code::FailedPrecondition
    );

    // Empty commit_id with view lacking wc_commit_ids[workspace_name] -> INVALID_ARGUMENT.
    let valid_view = vec![0x66; cc_common::VIEW_ID_LENGTH];
    store
        .put_view(
            "repo-1",
            &valid_view,
            &cc_common::op_store::View {
                head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                wc_commit_ids: std::collections::HashMap::new(),
                local_bookmarks: std::collections::HashMap::new(),
                remote_bookmarks: std::collections::HashMap::new(),
            },
        )
        .await
        .unwrap();
    let op_empty_wc = vec![0x77; cc_common::OPERATION_ID_LENGTH];
    store
        .put_operation(
            "repo-1",
            &op_empty_wc,
            &cc_common::op_store::Operation {
                view_id: valid_view,
                parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                metadata: None,
                commit_predecessors: Vec::new(),
                commit_predecessors_set: true,
            },
        )
        .await
        .unwrap();
    let mut no_wc_ws = base_ws("missing-wc");
    no_wc_ws.commit_id.clear();
    no_wc_ws.operation_id = op_empty_wc;
    assert_eq!(
        svc.update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(no_wc_ws),
            expected_operation_id: None,
        }))
        .await
        .unwrap_err()
        .code(),
        Code::InvalidArgument
    );

    // ListWorkspaces enforces 2 MiB response byte budget even with page_size = 0.
    let big_patterns: Vec<String> = (0..1024)
        .map(|i| format!("dir{i:04}/{}", "x".repeat(200)))
        .collect();
    for i in 0..15 {
        let mut ws = base_ws(&format!("big-{i:02}"));
        ws.sparse_patterns = big_patterns.clone();
        svc.update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(ws),
            expected_operation_id: None,
        }))
        .await
        .unwrap();
    }
    let page1 = svc
        .list_workspaces(Request::new(ListWorkspacesRequest {
            repo_id: "repo-1".to_string(),
            page_size: 0,
            page_token: String::new(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(
        !page1.next_page_token.is_empty(),
        "expected byte-budget pagination"
    );
    assert!(page1.encoded_len() <= MAX_LIST_WORKSPACES_RESPONSE_BYTES + 1024);

    let mut total = page1.workspaces.len();
    let mut token = page1.next_page_token;
    while !token.is_empty() {
        let p = svc
            .list_workspaces(Request::new(ListWorkspacesRequest {
                repo_id: "repo-1".to_string(),
                page_size: 0,
                page_token: token,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!p.workspaces.is_empty());
        total += p.workspaces.len();
        token = p.next_page_token;
    }
    assert_eq!(total, 16);
}

#[tokio::test]
async fn test_list_workspaces_keyset_pagination_and_byte_budget() {
    let dir = tempfile::tempdir().unwrap();
    let sqlite_store = Arc::new(CompositeStore::new(
        Arc::new(SqliteMetadataStore::open(&dir.path().join("list_ws.db")).unwrap()),
        Arc::new(MemoryBlobStore::new()),
    ));
    let mem_store = Arc::new(CompositeStore::in_memory());

    let big_patterns: Vec<String> = (0..1024)
        .map(|i| format!("dir{i:04}/{}", "x".repeat(200)))
        .collect();

    for store in [mem_store, sqlite_store] {
        store.register_repo("repo-page", None).await.unwrap();
        let svc = CommitCloudWorkspaceService::new(store);

        // Empty repo yields empty page with no next_page_token.
        let empty = svc
            .list_workspaces(Request::new(ListWorkspacesRequest {
                repo_id: "repo-page".to_string(),
                page_size: 2,
                page_token: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(empty.workspaces.is_empty());
        assert!(empty.next_page_token.is_empty());

        let entries = [
            ("carol", "ws-c1"),
            ("alice", "ws-b"),
            ("bob", "ws-2"),
            ("alice", "ws-a"),
            ("agent-42", "ws-1"),
            ("", "ws-c2"),
        ];
        for (user, ws_name) in entries {
            let mut ws = base_ws(ws_name);
            ws.repo_id = "repo-page".to_string();
            ws.user = user.to_string();
            svc.update_workspace(Request::new(UpdateWorkspaceRequest {
                workspace: Some(ws),
                expected_operation_id: None,
            }))
            .await
            .unwrap();
        }

        let mut token = String::new();
        let mut collected = Vec::new();
        let mut pages = 0usize;
        loop {
            let resp = svc
                .list_workspaces(Request::new(ListWorkspacesRequest {
                    repo_id: "repo-page".to_string(),
                    page_size: 2,
                    page_token: token,
                }))
                .await
                .unwrap()
                .into_inner();
            pages += 1;
            assert_eq!(resp.workspaces.len(), 2);
            collected.extend(
                resp.workspaces
                    .into_iter()
                    .map(|w| (w.user, w.workspace_name)),
            );
            if resp.next_page_token.is_empty() {
                break;
            }
            token = resp.next_page_token;
        }
        assert_eq!(pages, 3);
        assert_eq!(
            collected,
            vec![
                ("agent-42".to_string(), "ws-1".to_string()),
                ("bob".to_string(), "ws-2".to_string()),
                ("alice".to_string(), "ws-a".to_string()),
                ("alice".to_string(), "ws-b".to_string()),
                ("carol".to_string(), "ws-c1".to_string()),
                ("".to_string(), "ws-c2".to_string()),
            ]
        );

        // Cursor past the last workspace returns an empty final page.
        let past_end = svc
            .list_workspaces(Request::new(ListWorkspacesRequest {
                repo_id: "repo-page".to_string(),
                page_size: 10,
                page_token: encode_workspace_cursor("ws-c2"),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(past_end.workspaces.is_empty());
        assert!(past_end.next_page_token.is_empty());

        // Large sparse_patterns workspaces trigger 2 MiB byte-budget splitting across keyset pages.
        for i in 0..12 {
            let mut ws = base_ws(&format!("big-{i:02}"));
            ws.repo_id = "repo-page".to_string();
            ws.user = "dave".to_string();
            ws.sparse_patterns = big_patterns.clone();
            svc.update_workspace(Request::new(UpdateWorkspaceRequest {
                workspace: Some(ws),
                expected_operation_id: None,
            }))
            .await
            .unwrap();
        }
        let mut all_keys = Vec::new();
        let mut byte_pages = 0usize;
        let mut token = String::new();
        loop {
            let resp = svc
                .list_workspaces(Request::new(ListWorkspacesRequest {
                    repo_id: "repo-page".to_string(),
                    page_size: 0,
                    page_token: token,
                }))
                .await
                .unwrap()
                .into_inner();
            byte_pages += 1;
            assert!(resp.encoded_len() <= MAX_LIST_WORKSPACES_RESPONSE_BYTES + 1024);
            all_keys.extend(
                resp.workspaces
                    .into_iter()
                    .map(|w| (w.user, w.workspace_name)),
            );
            if resp.next_page_token.is_empty() {
                break;
            }
            token = resp.next_page_token;
        }
        assert!(
            byte_pages >= 2,
            "expected >1 page under 2 MiB response budget"
        );
        assert_eq!(all_keys.len(), 18);
    }
}

#[tokio::test]
async fn test_legacy_cursor_pagination_and_slash_workspace_names() {
    async fn verify_backend(store: Arc<CompositeStore>) {
        store
            .register_repo_in_project("repo-1", Some("proj-1"), None)
            .await
            .unwrap();
        let svc = CommitCloudWorkspaceService::new(store.clone());

        // Insert legacy rows directly into MetadataStore that would fail current
        // UpdateWorkspace validation (> 256 bytes, whitespace-only, control chars) plus a valid
        // slash name.
        let oversized_ws = format!("b-legacy/{}", "x".repeat(300));
        let legacy_keys = [
            ("alice", "   ".to_string()),
            ("alice", "a-normal".to_string()),
            ("alice", oversized_ws.clone()),
            ("alice", "c-legacy\u{0007}ctrl".to_string()),
            ("bob", "feature/foo".to_string()),
        ];
        for (user, ws_name) in &legacy_keys {
            let mut ws = base_ws(ws_name);
            ws.user = (*user).to_string();
            store.metadata().put_workspace(&ws).await.unwrap();
        }

        // Paginate 1 row at a time; cursors emitted for legacy rows must decode so
        // rows after them remain reachable.
        let mut seen = Vec::new();
        let mut token = String::new();
        loop {
            let resp = svc
                .list_workspaces(Request::new(ListWorkspacesRequest {
                    repo_id: "repo-1".to_string(),
                    page_size: 1,
                    page_token: token,
                }))
                .await
                .expect("pagination past legacy workspace rows must succeed")
                .into_inner();
            for ws in resp.workspaces {
                seen.push((ws.user, ws.workspace_name));
            }
            if resp.next_page_token.is_empty() {
                break;
            }
            token = resp.next_page_token;
        }
        let expected: Vec<(String, String)> = legacy_keys
            .iter()
            .map(|(u, w)| ((*u).to_string(), w.clone()))
            .collect();
        assert_eq!(seen, expected);

        // Slash-containing workspace names (valid in upstream jj) work across all RPCs.
        let mut slash_ws = base_ws("team/feature/bar");
        slash_ws.user = "carol".to_string();
        let updated = svc
            .update_workspace(Request::new(UpdateWorkspaceRequest {
                workspace: Some(slash_ws.clone()),
                expected_operation_id: None,
            }))
            .await
            .expect("workspace_name containing '/' must be accepted")
            .into_inner();
        assert_eq!(updated.workspace.as_ref(), Some(&slash_ws));

        let fetched = svc
            .get_workspace(Request::new(GetWorkspaceRequest {
                repo_id: "repo-1".to_string(),
                workspace_name: "team/feature/bar".to_string(),
            }))
            .await
            .unwrap()
            .into_inner()
            .workspace;
        assert_eq!(fetched.as_ref(), Some(&slash_ws));

        let check = svc
            .check_working_copy_changes(Request::new(CheckWorkingCopyChangesRequest {
                repo_id: "repo-1".to_string(),
                workspace_name: "team/feature/bar".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!check.has_changes);

        let del = svc
            .delete_workspace(Request::new(DeleteWorkspaceRequest {
                repo_id: "repo-1".to_string(),
                workspace_name: "team/feature/bar".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(del.existed);
    }

    verify_backend(Arc::new(CompositeStore::in_memory())).await;
    let dir = tempfile::tempdir().unwrap();
    let sqlite_meta = Arc::new(SqliteMetadataStore::open(&dir.path().join("ws.db")).unwrap());
    verify_backend(Arc::new(CompositeStore::new(
        sqlite_meta,
        Arc::new(MemoryBlobStore::new()),
    )))
    .await;
}

#[tokio::test]
async fn test_conflict_labels_legacy_normalization_and_view_commit_derivation() {
    use std::collections::HashMap;

    let store = Arc::new(CompositeStore::in_memory());
    store
        .register_repo_in_project("repo-1", Some("proj-1"), None)
        .await
        .unwrap();
    let svc = CommitCloudWorkspaceService::new(store.clone());
    let t1 = vec![0x11; cc_common::EMPTY_TREE_ID_BYTES.len()];
    let t2 = vec![0x22; cc_common::EMPTY_TREE_ID_BYTES.len()];
    let t3 = vec![0x33; cc_common::EMPTY_TREE_ID_BYTES.len()];
    for t in [&t1, &t2, &t3] {
        store.put_tree("proj-1", t, &[]).await.unwrap();
    }

    // conflict_labels containing '\n' is rejected.
    let mut newline_labels = base_ws("ws-nl");
    newline_labels.tree_id = t1.clone();
    newline_labels.tree_ids = vec![t1.clone(), t2.clone(), t3.clone()];
    newline_labels.conflict_labels = vec!["bad\nlabel".into(), "base".into(), "side-b".into()];
    assert_eq!(
        svc.update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(newline_labels),
            expected_operation_id: None,
        }))
        .await
        .unwrap_err()
        .code(),
        Code::InvalidArgument
    );

    // Non-empty conflict_labels on a resolved 1-tree workspace is rejected.
    let mut resolved_labeled = base_ws("ws-resolved-label");
    resolved_labeled.tree_id = t1.clone();
    resolved_labeled.tree_ids = vec![t1.clone()];
    resolved_labeled.conflict_labels = vec!["unexpected".into()];
    assert_eq!(
        svc.update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(resolved_labeled),
            expected_operation_id: None,
        }))
        .await
        .unwrap_err()
        .code(),
        Code::InvalidArgument
    );

    // All-empty ["", "", ""] canonicalizes to [], while partially-empty
    // ["side-1", "", "side-2"] is preserved verbatim.
    let mut all_empty_labels = base_ws("ws-all-empty-labels");
    all_empty_labels.tree_id = t1.clone();
    all_empty_labels.tree_ids = vec![t1.clone(), t2.clone(), t3.clone()];
    all_empty_labels.conflict_labels = vec![String::new(), String::new(), String::new()];
    let resp = svc
        .update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(all_empty_labels),
            expected_operation_id: None,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.workspace.unwrap().conflict_labels.is_empty());
    let fetched_empty = svc
        .get_workspace(Request::new(GetWorkspaceRequest {
            repo_id: "repo-1".to_string(),
            workspace_name: "ws-all-empty-labels".to_string(),
        }))
        .await
        .unwrap()
        .into_inner()
        .workspace
        .unwrap();
    assert!(fetched_empty.conflict_labels.is_empty());

    let mut partial_labels = base_ws("ws-partial-labels");
    partial_labels.tree_id = t1.clone();
    partial_labels.tree_ids = vec![t1.clone(), t2.clone(), t3.clone()];
    partial_labels.conflict_labels = vec!["side-1".into(), String::new(), "side-2".into()];
    let resp = svc
        .update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(partial_labels.clone()),
            expected_operation_id: None,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        resp.workspace.unwrap().conflict_labels,
        partial_labels.conflict_labels
    );
    let fetched_partial = svc
        .get_workspace(Request::new(GetWorkspaceRequest {
            repo_id: "repo-1".to_string(),
            workspace_name: "ws-partial-labels".to_string(),
        }))
        .await
        .unwrap()
        .into_inner()
        .workspace
        .unwrap();
    assert_eq!(
        fetched_partial.conflict_labels,
        partial_labels.conflict_labels
    );

    // Legacy tree_id-only + sparse_patterns_set = false + empty operation_id + empty
    // commit_id are accepted and normalized.
    let legacy_req = WorkspaceState {
        repo_id: "repo-1".to_string(),
        user: "alice".to_string(),
        workspace_name: "ws-legacy-norm".to_string(),
        commit_id: Vec::new(),
        operation_id: Vec::new(),
        tree_id: t1.clone(),
        sparse_patterns: Vec::new(),
        tree_ids: Vec::new(),
        conflict_labels: Vec::new(),
        sparse_patterns_set: false,
    };
    let norm_resp = svc
        .update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(legacy_req),
            expected_operation_id: None,
        }))
        .await
        .unwrap()
        .into_inner()
        .workspace
        .unwrap();
    assert_eq!(norm_resp.commit_id, cc_common::ROOT_COMMIT_ID_BYTES);
    assert_eq!(norm_resp.operation_id, cc_common::ROOT_OPERATION_ID_BYTES);
    assert_eq!(norm_resp.tree_id, t1);
    assert_eq!(norm_resp.tree_ids, vec![t1.clone()]);
    assert_eq!(norm_resp.sparse_patterns, vec![String::new()]);
    assert!(norm_resp.sparse_patterns_set);

    // Server-side commit_id derivation from View.wc_commit_ids[workspace_name].
    let derived_commit = vec![0xcc; cc_common::COMMIT_ID_LENGTH];
    store
        .put_commit(
            "proj-1",
            &derived_commit,
            &cc_common::backend::Commit {
                commit_id: derived_commit.clone(),
                change_id: vec![0xdd; cc_common::CHANGE_ID_LENGTH],
                parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: vec![t1.clone()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let view_id = vec![0xee; cc_common::VIEW_ID_LENGTH];
    let mut wc_commit_ids = HashMap::new();
    wc_commit_ids.insert("derived-ws".to_string(), derived_commit.clone());
    store
        .put_view(
            "repo-1",
            &view_id,
            &cc_common::op_store::View {
                head_ids: vec![derived_commit.clone()],
                wc_commit_ids,
                local_bookmarks: HashMap::new(),
                remote_bookmarks: HashMap::new(),
            },
        )
        .await
        .unwrap();
    let op_id = vec![0xfe; cc_common::OPERATION_ID_LENGTH];
    store
        .put_operation(
            "repo-1",
            &op_id,
            &cc_common::op_store::Operation {
                view_id,
                parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                metadata: None,
                commit_predecessors: Vec::new(),
                commit_predecessors_set: true,
            },
        )
        .await
        .unwrap();

    let mut derived_ws = base_ws("derived-ws");
    derived_ws.commit_id.clear();
    derived_ws.operation_id = op_id.clone();
    derived_ws.tree_id = t1.clone();
    derived_ws.tree_ids = vec![t1.clone()];
    let derived_resp = svc
        .update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(derived_ws),
            expected_operation_id: None,
        }))
        .await
        .unwrap()
        .into_inner()
        .workspace
        .unwrap();
    assert_eq!(derived_resp.commit_id, derived_commit);
    let fetched_derived = svc
        .get_workspace(Request::new(GetWorkspaceRequest {
            repo_id: "repo-1".to_string(),
            workspace_name: "derived-ws".to_string(),
        }))
        .await
        .unwrap()
        .into_inner()
        .workspace
        .unwrap();
    assert_eq!(fetched_derived.commit_id, derived_commit);

    // Explicit commit_id with a non-root op exercises the lightweight has_view/has_commit path.
    let mut explicit_ws = base_ws("explicit-ws");
    explicit_ws.commit_id = derived_commit.clone();
    explicit_ws.operation_id = op_id;
    explicit_ws.tree_id = t1.clone();
    explicit_ws.tree_ids = vec![t1];
    let explicit_resp = svc
        .update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(explicit_ws),
            expected_operation_id: None,
        }))
        .await
        .unwrap()
        .into_inner()
        .workspace
        .unwrap();
    assert_eq!(explicit_resp.commit_id, derived_commit);
}

#[tokio::test]
async fn test_update_workspace_rejects_delete_and_reregister_between_op_lookup_and_commit() {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};

    use async_trait::async_trait;
    use cc_common::backend::{Commit, TreeEntry};
    use cc_common::op_store::{Operation, View};

    use crate::store::{
        BlobRecord, CommitId, MemoryMetadataStore, MetadataStore, OpId, StoreResult,
    };

    struct SwapAfterOpLookupStore {
        inner: Arc<MemoryMetadataStore>,
        swap_on_op: AtomicBool,
        swap_on_view: AtomicBool,
        swap_on_workspace: AtomicBool,
        swap_on_put_workspace: AtomicBool,
        repo_id: String,
        old_project_id: String,
        new_project_id: String,
    }

    impl SwapAfterOpLookupStore {
        async fn maybe_swap(&self, flag: &AtomicBool) -> StoreResult<()> {
            if flag.swap(false, Ordering::SeqCst) {
                let deleted = self
                    .inner
                    .delete_repository(&self.repo_id, Some(&self.old_project_id))
                    .await?;
                assert!(deleted);
                self.inner
                    .register_repo_in_project(&self.repo_id, &self.new_project_id, None)
                    .await?;
            }
            Ok(())
        }
    }

    #[async_trait]
    impl MetadataStore for SwapAfterOpLookupStore {
        fn backend_name(&self) -> &'static str {
            self.inner.backend_name()
        }
        async fn register_project(&self, project_id: &str, name: Option<&str>) -> StoreResult<()> {
            self.inner.register_project(project_id, name).await
        }
        async fn is_project_registered(&self, project_id: &str) -> StoreResult<bool> {
            self.inner.is_project_registered(project_id).await
        }
        async fn get_repo_project_id(&self, repo_id: &str) -> StoreResult<Option<String>> {
            self.inner.get_repo_project_id(repo_id).await
        }
        async fn register_repo_in_project(
            &self,
            repo_id: &str,
            project_id: &str,
            name: Option<&str>,
        ) -> StoreResult<String> {
            self.inner
                .register_repo_in_project(repo_id, project_id, name)
                .await
        }
        async fn delete_repository(
            &self,
            repo_id: &str,
            expected_project_id: Option<&str>,
        ) -> StoreResult<bool> {
            self.inner
                .delete_repository(repo_id, expected_project_id)
                .await
        }
        async fn get_commit(
            &self,
            project_id: &str,
            commit_id: &[u8],
        ) -> StoreResult<Option<Commit>> {
            self.inner.get_commit(project_id, commit_id).await
        }
        async fn get_commit_generation(
            &self,
            project_id: &str,
            commit_id: &[u8],
        ) -> StoreResult<Option<u64>> {
            self.inner
                .get_commit_generation(project_id, commit_id)
                .await
        }
        async fn put_commit(
            &self,
            project_id: &str,
            commit_id: &[u8],
            commit: &Commit,
            generation: u64,
        ) -> StoreResult<()> {
            self.inner
                .put_commit(project_id, commit_id, commit, generation)
                .await
        }
        async fn list_commit_ids(
            &self,
            project_id: &str,
            after_commit_id: Option<&[u8]>,
            limit: usize,
        ) -> StoreResult<Vec<CommitId>> {
            self.inner
                .list_commit_ids(project_id, after_commit_id, limit)
                .await
        }
        async fn current_commit_watermark(&self, project_id: &str) -> StoreResult<Vec<u8>> {
            self.inner.current_commit_watermark(project_id).await
        }
        async fn list_commit_ids_since(
            &self,
            project_id: &str,
            since_token: &[u8],
            limit: usize,
        ) -> StoreResult<(Vec<CommitId>, Vec<u8>, bool)> {
            self.inner
                .list_commit_ids_since(project_id, since_token, limit)
                .await
        }
        async fn resolve_commit_id_prefix(
            &self,
            project_id: &str,
            min_id: &[u8],
            max_id: &[u8],
        ) -> StoreResult<Vec<CommitId>> {
            self.inner
                .resolve_commit_id_prefix(project_id, min_id, max_id)
                .await
        }
        async fn resolve_change_id_prefix(
            &self,
            project_id: &str,
            min_id: &[u8],
            max_id: &[u8],
            limit: usize,
        ) -> StoreResult<Vec<(Vec<u8>, Vec<CommitId>)>> {
            self.inner
                .resolve_change_id_prefix(project_id, min_id, max_id, limit)
                .await
        }
        async fn get_tree(
            &self,
            project_id: &str,
            tree_id: &[u8],
        ) -> StoreResult<Option<Vec<TreeEntry>>> {
            self.inner.get_tree(project_id, tree_id).await
        }
        async fn put_tree(
            &self,
            project_id: &str,
            tree_id: &[u8],
            entries: &[TreeEntry],
        ) -> StoreResult<()> {
            self.inner.put_tree(project_id, tree_id, entries).await
        }
        async fn find_existing_trees(
            &self,
            project_id: &str,
            tree_ids: &[Vec<u8>],
        ) -> StoreResult<HashSet<Vec<u8>>> {
            self.inner.find_existing_trees(project_id, tree_ids).await
        }
        async fn get_symlink(
            &self,
            project_id: &str,
            symlink_id: &[u8],
        ) -> StoreResult<Option<String>> {
            self.inner.get_symlink(project_id, symlink_id).await
        }
        async fn put_symlink(
            &self,
            project_id: &str,
            symlink_id: &[u8],
            target: &str,
        ) -> StoreResult<()> {
            self.inner.put_symlink(project_id, symlink_id, target).await
        }
        async fn find_existing_symlinks(
            &self,
            project_id: &str,
            symlink_ids: &[Vec<u8>],
        ) -> StoreResult<HashSet<Vec<u8>>> {
            self.inner
                .find_existing_symlinks(project_id, symlink_ids)
                .await
        }
        async fn get_blob_record(
            &self,
            project_id: &str,
            blob_id: &[u8],
        ) -> StoreResult<Option<BlobRecord>> {
            self.inner.get_blob_record(project_id, blob_id).await
        }
        async fn get_blob_sizes_batch(
            &self,
            project_id: &str,
            blob_ids: &[Vec<u8>],
        ) -> StoreResult<HashMap<Vec<u8>, u64>> {
            self.inner.get_blob_sizes_batch(project_id, blob_ids).await
        }
        async fn get_global_blob_record(&self, blob_id: &[u8]) -> StoreResult<Option<BlobRecord>> {
            self.inner.get_global_blob_record(blob_id).await
        }
        async fn put_blob_record(&self, project_id: &str, record: &BlobRecord) -> StoreResult<()> {
            self.inner.put_blob_record(project_id, record).await
        }
        async fn add_blob_replica_location(
            &self,
            blob_id: &[u8],
            location: &str,
        ) -> StoreResult<()> {
            self.inner
                .add_blob_replica_location(blob_id, location)
                .await
        }
        async fn get_operation(
            &self,
            repo_id: &str,
            op_id: &[u8],
        ) -> StoreResult<Option<Operation>> {
            let res = self.inner.get_operation(repo_id, op_id).await?;
            if res.is_some() {
                self.maybe_swap(&self.swap_on_op).await?;
            }
            Ok(res)
        }
        async fn put_operation(
            &self,
            repo_id: &str,
            op_id: &[u8],
            op: &Operation,
        ) -> StoreResult<()> {
            self.inner.put_operation(repo_id, op_id, op).await
        }
        async fn put_operation_for_project(
            &self,
            repo_id: &str,
            expected_project_id: &str,
            op_id: &[u8],
            op: &Operation,
            require_dependencies_exist: bool,
        ) -> StoreResult<()> {
            self.inner
                .put_operation_for_project(
                    repo_id,
                    expected_project_id,
                    op_id,
                    op,
                    require_dependencies_exist,
                )
                .await
        }
        async fn resolve_operation_id_prefix(
            &self,
            repo_id: &str,
            min_id: &[u8],
            max_id: &[u8],
        ) -> StoreResult<Vec<OpId>> {
            self.inner
                .resolve_operation_id_prefix(repo_id, min_id, max_id)
                .await
        }
        async fn get_view(&self, repo_id: &str, view_id: &[u8]) -> StoreResult<Option<View>> {
            let res = self.inner.get_view(repo_id, view_id).await?;
            if res.is_some() {
                self.maybe_swap(&self.swap_on_view).await?;
            }
            Ok(res)
        }
        async fn put_view(&self, repo_id: &str, view_id: &[u8], view: &View) -> StoreResult<()> {
            self.inner.put_view(repo_id, view_id, view).await
        }
        async fn put_view_for_project(
            &self,
            repo_id: &str,
            expected_project_id: &str,
            view_id: &[u8],
            view: &View,
        ) -> StoreResult<()> {
            self.inner
                .put_view_for_project(repo_id, expected_project_id, view_id, view)
                .await
        }
        async fn get_op_heads(&self, repo_id: &str) -> StoreResult<Option<Vec<OpId>>> {
            self.inner.get_op_heads(repo_id).await
        }
        async fn update_op_heads(
            &self,
            repo_id: &str,
            old_ids: &[Vec<u8>],
            new_id: &[u8],
        ) -> StoreResult<Vec<OpId>> {
            self.inner.update_op_heads(repo_id, old_ids, new_id).await
        }
        async fn update_op_heads_for_project(
            &self,
            repo_id: &str,
            expected_project_id: &str,
            old_head_ids: &[Vec<u8>],
            new_head_id: &[u8],
            require_op_exists: bool,
        ) -> StoreResult<Vec<OpId>> {
            self.inner
                .update_op_heads_for_project(
                    repo_id,
                    expected_project_id,
                    old_head_ids,
                    new_head_id,
                    require_op_exists,
                )
                .await
        }
        async fn publish_operation_atomic(
            &self,
            repo_id: &str,
            view_id: &[u8],
            view: &View,
            op_id: &[u8],
            op: &Operation,
            old_ids: &[Vec<u8>],
        ) -> StoreResult<Vec<OpId>> {
            self.inner
                .publish_operation_atomic(repo_id, view_id, view, op_id, op, old_ids)
                .await
        }
        #[allow(clippy::too_many_arguments)]
        async fn publish_operation_for_project(
            &self,
            repo_id: &str,
            expected_project_id: &str,
            view_id: &[u8],
            view: &View,
            op_id: &[u8],
            op: &Operation,
            old_op_head_ids: &[Vec<u8>],
            require_dependencies_exist: bool,
        ) -> StoreResult<Vec<OpId>> {
            self.inner
                .publish_operation_for_project(
                    repo_id,
                    expected_project_id,
                    view_id,
                    view,
                    op_id,
                    op,
                    old_op_head_ids,
                    require_dependencies_exist,
                )
                .await
        }
        async fn compare_and_swap_op_heads(
            &self,
            repo_id: &str,
            expected_old_ids: &[Vec<u8>],
            new_id: &[u8],
        ) -> StoreResult<()> {
            self.inner
                .compare_and_swap_op_heads(repo_id, expected_old_ids, new_id)
                .await
        }
        async fn update_op_heads_cas_for_project(
            &self,
            repo_id: &str,
            expected_project_id: &str,
            expected_exact_heads: &[Vec<u8>],
            new_head_id: &[u8],
            require_op_exists: bool,
        ) -> StoreResult<()> {
            self.inner
                .update_op_heads_cas_for_project(
                    repo_id,
                    expected_project_id,
                    expected_exact_heads,
                    new_head_id,
                    require_op_exists,
                )
                .await
        }
        async fn get_workspace(
            &self,
            repo_id: &str,
            workspace_name: &str,
        ) -> StoreResult<Option<WorkspaceState>> {
            let res = self.inner.get_workspace(repo_id, workspace_name).await?;
            if res.is_some() {
                self.maybe_swap(&self.swap_on_workspace).await?;
            }
            Ok(res)
        }
        async fn put_workspace_cas(
            &self,
            workspace: &WorkspaceState,
            expected_operation_id: Option<&[u8]>,
        ) -> StoreResult<()> {
            self.inner
                .put_workspace_cas(workspace, expected_operation_id)
                .await
        }
        async fn put_workspace_cas_for_project(
            &self,
            expected_project_id: &str,
            workspace: &WorkspaceState,
            expected_operation_id: Option<&[u8]>,
            require_operation_exists: bool,
        ) -> StoreResult<()> {
            self.maybe_swap(&self.swap_on_put_workspace).await?;
            self.inner
                .put_workspace_cas_for_project(
                    expected_project_id,
                    workspace,
                    expected_operation_id,
                    require_operation_exists,
                )
                .await
        }
        async fn list_workspaces(&self, repo_id: &str) -> StoreResult<Vec<WorkspaceState>> {
            self.inner.list_workspaces(repo_id).await
        }
        async fn delete_workspace(&self, repo_id: &str, workspace_name: &str) -> StoreResult<bool> {
            self.inner.delete_workspace(repo_id, workspace_name).await
        }
    }

    // Delete + re-register under `proj-b` right after `get_operation` (before commit),
    // and right after `get_view` (while deriving commit_id).
    for swap_on_view in [false, true] {
        let inner = Arc::new(MemoryMetadataStore::new());
        let hooked = Arc::new(SwapAfterOpLookupStore {
            inner: Arc::clone(&inner),
            swap_on_op: AtomicBool::new(false),
            swap_on_view: AtomicBool::new(false),
            swap_on_workspace: AtomicBool::new(false),
            swap_on_put_workspace: AtomicBool::new(false),
            repo_id: "repo-race".to_string(),
            old_project_id: "proj-a".to_string(),
            new_project_id: "proj-b".to_string(),
        });
        let store = Arc::new(CompositeStore::new(
            hooked.clone(),
            Arc::new(MemoryBlobStore::new()),
        ));

        store.register_project("proj-a", None).await.unwrap();
        store.register_project("proj-b", None).await.unwrap();
        store
            .register_repo_in_project("repo-race", Some("proj-a"), None)
            .await
            .unwrap();

        // Place the same content-addressed commit in both projects so commit validation alone
        // cannot mask a late `resolve_repo_project` call.
        let commit_id = vec![0xab; cc_common::COMMIT_ID_LENGTH];
        let commit = Commit {
            commit_id: commit_id.clone(),
            change_id: vec![0xcd; cc_common::CHANGE_ID_LENGTH],
            parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
            root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
            description: "shared commit".to_string(),
            ..Default::default()
        };
        store
            .put_commit("proj-a", &commit_id, &commit)
            .await
            .unwrap();
        store
            .put_commit("proj-b", &commit_id, &commit)
            .await
            .unwrap();

        let op_id = vec![0x77; cc_common::OPERATION_ID_LENGTH];
        let view_id = if swap_on_view {
            let vid = vec![0x66; cc_common::VIEW_ID_LENGTH];
            let mut wc_commit_ids = HashMap::new();
            wc_commit_ids.insert("ws-race".to_string(), commit_id.clone());
            store
                .put_view(
                    "repo-race",
                    &vid,
                    &View {
                        head_ids: vec![commit_id.clone()],
                        wc_commit_ids,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            vid
        } else {
            cc_common::ROOT_VIEW_ID_BYTES.to_vec()
        };
        store
            .put_operation(
                "repo-race",
                &op_id,
                &Operation {
                    view_id,
                    parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        if swap_on_view {
            hooked.swap_on_view.store(true, Ordering::SeqCst);
        } else {
            hooked.swap_on_op.store(true, Ordering::SeqCst);
        }

        let svc = CommitCloudWorkspaceService::new(store.clone());
        let mut ws = base_ws("ws-race");
        ws.repo_id = "repo-race".to_string();
        ws.operation_id = op_id;
        ws.commit_id = if swap_on_view { Vec::new() } else { commit_id };

        let err = svc
            .update_workspace(Request::new(UpdateWorkspaceRequest {
                workspace: Some(ws),
                expected_operation_id: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            Code::FailedPrecondition,
            "swap_on_view={swap_on_view}: expected FAILED_PRECONDITION, got {err:?}"
        );
        assert!(
            inner
                .get_workspace("repo-race", "ws-race")
                .await
                .unwrap()
                .is_none(),
            "swap_on_view={swap_on_view}: stale workspace must not be written into proj-b"
        );
    }

    // `check_working_copy_changes` detects a concurrent delete + cross-project
    // re-bind between reading `WorkspaceState` and resolving the commit, returning
    // `FAILED_PRECONDITION` instead of looking up `proj-a`'s commit in `proj-b`.
    {
        let inner = Arc::new(MemoryMetadataStore::new());
        let hooked = Arc::new(SwapAfterOpLookupStore {
            inner,
            swap_on_op: AtomicBool::new(false),
            swap_on_view: AtomicBool::new(false),
            swap_on_workspace: AtomicBool::new(false),
            swap_on_put_workspace: AtomicBool::new(false),
            repo_id: "repo-check-race".to_string(),
            old_project_id: "proj-a".to_string(),
            new_project_id: "proj-b".to_string(),
        });
        let store = Arc::new(CompositeStore::new(
            hooked.clone(),
            Arc::new(MemoryBlobStore::new()),
        ));
        store.register_project("proj-a", None).await.unwrap();
        store.register_project("proj-b", None).await.unwrap();
        store
            .register_repo_in_project("repo-check-race", Some("proj-a"), None)
            .await
            .unwrap();

        let commit_id = vec![0xab; cc_common::COMMIT_ID_LENGTH];
        let commit = Commit {
            commit_id: commit_id.clone(),
            change_id: vec![0xcd; cc_common::CHANGE_ID_LENGTH],
            parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
            root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
            description: "proj-a commit".to_string(),
            ..Default::default()
        };
        store
            .put_commit("proj-a", &commit_id, &commit)
            .await
            .unwrap();
        let mut ws = base_ws("ws-check");
        ws.repo_id = "repo-check-race".to_string();
        ws.commit_id = commit_id;
        store.put_workspace(&ws).await.unwrap();

        hooked.swap_on_workspace.store(true, Ordering::SeqCst);
        let svc = CommitCloudWorkspaceService::new(store);
        let err = svc
            .check_working_copy_changes(Request::new(CheckWorkingCopyChangesRequest {
                repo_id: "repo-check-race".to_string(),
                workspace_name: "ws-check".to_string(),
            }))
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            Code::FailedPrecondition,
            "check_working_copy_changes must reject cross-project re-bind, got {err:?}"
        );
    }

    // Same-project delete + re-register (`proj-a -> proj-a`) right before
    // `put_workspace_cas_for_project` commits must reject the deleted `operation_id`
    // atomically with `FAILED_PRECONDITION` and leave no dangling workspace row.
    {
        let inner = Arc::new(MemoryMetadataStore::new());
        let hooked = Arc::new(SwapAfterOpLookupStore {
            inner: Arc::clone(&inner),
            swap_on_op: AtomicBool::new(false),
            swap_on_view: AtomicBool::new(false),
            swap_on_workspace: AtomicBool::new(false),
            swap_on_put_workspace: AtomicBool::new(false),
            repo_id: "repo-same-proj".to_string(),
            old_project_id: "proj-a".to_string(),
            new_project_id: "proj-a".to_string(),
        });
        let store = Arc::new(CompositeStore::new(
            hooked.clone(),
            Arc::new(MemoryBlobStore::new()),
        ));
        store.register_project("proj-a", None).await.unwrap();
        store
            .register_repo_in_project("repo-same-proj", Some("proj-a"), None)
            .await
            .unwrap();
        let view_id = vec![0x61; cc_common::VIEW_ID_LENGTH];
        let op_id = vec![0x71; cc_common::OPERATION_ID_LENGTH];
        store
            .put_view(
                "repo-same-proj",
                &view_id,
                &View {
                    head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        store
            .put_operation(
                "repo-same-proj",
                &op_id,
                &Operation {
                    view_id,
                    parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        hooked.swap_on_put_workspace.store(true, Ordering::SeqCst);
        let svc = CommitCloudWorkspaceService::new(store);
        let mut ws = base_ws("ws-same-proj");
        ws.repo_id = "repo-same-proj".to_string();
        ws.operation_id = op_id;
        let err = svc
            .update_workspace(Request::new(UpdateWorkspaceRequest {
                workspace: Some(ws),
                expected_operation_id: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            Code::FailedPrecondition,
            "same-project recreate race before put_workspace must return FAILED_PRECONDITION, got {err:?}"
        );
        assert!(
            inner
                .get_workspace("repo-same-proj", "ws-same-proj")
                .await
                .unwrap()
                .is_none(),
            "dangling workspace referencing deleted operation must not be persisted"
        );
    }
}

#[tokio::test]
async fn test_workspace_pk_repo_and_name_with_mutable_non_key_user() {
    let dir = tempfile::tempdir().unwrap();
    let sqlite_store = Arc::new(CompositeStore::new(
        Arc::new(SqliteMetadataStore::open(&dir.path().join("mutable_user.db")).unwrap()),
        Arc::new(MemoryBlobStore::new()),
    ));
    let mem_store = Arc::new(CompositeStore::in_memory());

    for store in [mem_store, sqlite_store] {
        store
            .register_repo_in_project("repo-1", Some("proj-1"), None)
            .await
            .unwrap();
        let op1 = vec![0x11; cc_common::OPERATION_ID_LENGTH];
        let op2 = vec![0x22; cc_common::OPERATION_ID_LENGTH];
        for op_id in [&op1, &op2] {
            store
                .put_operation(
                    "repo-1",
                    op_id,
                    &cc_common::op_store::Operation {
                        view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
                        parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                        metadata: None,
                        commit_predecessors: Vec::new(),
                        commit_predecessors_set: true,
                    },
                )
                .await
                .unwrap();
        }

        let metrics = ServerMetrics::new();
        let svc = CommitCloudWorkspaceService::with_metrics(store, metrics.clone());

        // Arbitrary non-email user identifiers (agent, service-account, empty, email)
        // are accepted, and overwriting the same `(repo_id, workspace_name)` with different
        // `user` values updates the single record in place without creating duplicates.
        for (idx, user_id) in ["alice@example.com", "agent-42", "sa-runner-01", ""]
            .iter()
            .enumerate()
        {
            let mut ws = base_ws("shared-ws");
            ws.user = (*user_id).to_string();
            let resp = svc
                .update_workspace(Request::new(UpdateWorkspaceRequest {
                    workspace: Some(ws.clone()),
                    expected_operation_id: None,
                }))
                .await
                .unwrap_or_else(|e| panic!("user {user_id:?} (idx {idx}) should succeed: {e}"))
                .into_inner();
            assert_eq!(resp.workspace.as_ref().unwrap().user, *user_id);

            let fetched = svc
                .get_workspace(Request::new(GetWorkspaceRequest {
                    repo_id: "repo-1".to_string(),
                    workspace_name: "shared-ws".to_string(),
                }))
                .await
                .unwrap()
                .into_inner()
                .workspace
                .unwrap();
            assert_eq!(fetched.user, *user_id);
            assert_eq!(metrics.snapshot().active_workspaces, 1);
        }
        let listed = svc
            .list_workspaces(Request::new(ListWorkspacesRequest {
                repo_id: "repo-1".to_string(),
                page_size: 10,
                page_token: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .workspaces;
        assert_eq!(
            listed.len(),
            1,
            "overwriting user must not create colliding records"
        );

        // CAS UpdateWorkspace respects `user`:
        // - Create-only CAS fails with ABORTED when workspace already exists under another user.
        let mut cas_ws = base_ws("cas-ws");
        cas_ws.user = "agent-42".to_string();
        cas_ws.operation_id = op1.clone();
        svc.update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(cas_ws.clone()),
            expected_operation_id: Some(Vec::new()),
        }))
        .await
        .unwrap();
        // Same-state retry (including same `user`) is idempotent.
        svc.update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(cas_ws.clone()),
            expected_operation_id: Some(Vec::new()),
        }))
        .await
        .unwrap();

        let mut different_user_same_op = cas_ws.clone();
        different_user_same_op.user = "sa-runner-01".to_string();
        // Create-only CAS with different `user` must ABORT (not treated as idempotent retry).
        assert_eq!(
            svc.update_workspace(Request::new(UpdateWorkspaceRequest {
                workspace: Some(different_user_same_op.clone()),
                expected_operation_id: Some(Vec::new()),
            }))
            .await
            .unwrap_err()
            .code(),
            Code::Aborted
        );
        // Stale expected_operation_id with different `user` must ABORT.
        assert_eq!(
            svc.update_workspace(Request::new(UpdateWorkspaceRequest {
                workspace: Some(different_user_same_op.clone()),
                expected_operation_id: Some(op2.clone()),
            }))
            .await
            .unwrap_err()
            .code(),
            Code::Aborted
        );
        // Valid CAS matching `op1` updates `user` (even on same `operation_id`).
        svc.update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(different_user_same_op.clone()),
            expected_operation_id: Some(op1.clone()),
        }))
        .await
        .unwrap();
        let after_user_cas = svc
            .get_workspace(Request::new(GetWorkspaceRequest {
                repo_id: "repo-1".to_string(),
                workspace_name: "cas-ws".to_string(),
            }))
            .await
            .unwrap()
            .into_inner()
            .workspace
            .unwrap();
        assert_eq!(after_user_cas.user, "sa-runner-01");
        assert_eq!(after_user_cas.operation_id, op1);

        // Advancing `operation_id` and `user` together via CAS persists both.
        let mut advanced = different_user_same_op;
        advanced.user = "alice@example.com".to_string();
        advanced.operation_id = op2.clone();
        svc.update_workspace(Request::new(UpdateWorkspaceRequest {
            workspace: Some(advanced),
            expected_operation_id: Some(op1),
        }))
        .await
        .unwrap();
        let after_advance = svc
            .get_workspace(Request::new(GetWorkspaceRequest {
                repo_id: "repo-1".to_string(),
                workspace_name: "cas-ws".to_string(),
            }))
            .await
            .unwrap()
            .into_inner()
            .workspace
            .unwrap();
        assert_eq!(after_advance.user, "alice@example.com");
        assert_eq!(after_advance.operation_id, op2);

        // CheckWorkingCopyChanges and DeleteWorkspace operate purely by `(repo_id, workspace_name)`.
        let ch = svc
            .check_working_copy_changes(Request::new(CheckWorkingCopyChangesRequest {
                repo_id: "repo-1".to_string(),
                workspace_name: "cas-ws".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!ch.has_changes);

        let del = svc
            .delete_workspace(Request::new(DeleteWorkspaceRequest {
                repo_id: "repo-1".to_string(),
                workspace_name: "cas-ws".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(del.existed);
        assert!(
            svc.get_workspace(Request::new(GetWorkspaceRequest {
                repo_id: "repo-1".to_string(),
                workspace_name: "cas-ws".to_string(),
            }))
            .await
            .unwrap()
            .into_inner()
            .workspace
            .is_none()
        );
        assert_eq!(metrics.snapshot().active_workspaces, 1);
    }
}
