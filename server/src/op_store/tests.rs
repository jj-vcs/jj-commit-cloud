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

use tonic::{Code, Request};

use super::*;

fn make_service() -> CommitCloudOpStoreService {
    CommitCloudOpStoreService::new(Arc::new(CompositeStore::in_memory()))
}

#[tokio::test]
async fn test_op_store_validates_identifiers_digests_views_and_operations() {
    let svc = make_service();
    svc.store
        .register_repo_in_project("op-repo", Some("op-proj"), None)
        .await
        .unwrap();

    // Malformed identifiers and digest lengths return INVALID_ARGUMENT.
    let bad_id = svc
        .get_op_heads(Request::new(GetOpHeadsRequest {
            repo_id: "../bad".to_string(),
        }))
        .await
        .unwrap_err();
    assert_eq!(bad_id.code(), Code::InvalidArgument);

    let bad_op_len = svc
        .read_operation(Request::new(ReadOperationRequest {
            repo_id: "op-repo".to_string(),
            operation_id: vec![0x01; 4],
        }))
        .await
        .unwrap_err();
    assert_eq!(bad_op_len.code(), Code::InvalidArgument);

    let bad_old_head = svc
        .update_op_heads(Request::new(UpdateOpHeadsRequest {
            repo_id: "op-repo".to_string(),
            old_op_head_ids: vec![vec![0x01; 3]],
            new_op_head_id: vec![0x11; cc_common::OPERATION_ID_LENGTH],
        }))
        .await
        .unwrap_err();
    assert_eq!(bad_old_head.code(), Code::InvalidArgument);

    // Absent local RefTarget is rejected, while tracked-absent remote bookmark is accepted
    // when the matching local bookmark is present.
    let mut absent_local = HashMap::new();
    absent_local.insert(
        "main".to_string(),
        RefTarget {
            adds: vec![],
            removes: vec![],
        },
    );
    let err = svc
        .write_view(Request::new(WriteViewRequest {
            repo_id: "op-repo".to_string(),
            view: Some(View {
                head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                wc_commit_ids: HashMap::new(),
                local_bookmarks: absent_local,
                remote_bookmarks: HashMap::new(),
            }),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    let mut present_local = HashMap::new();
    present_local.insert(
        "feat@v1".to_string(),
        RefTarget {
            adds: vec![RefTargetTerm {
                commit_id: cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
            }],
            removes: vec![],
        },
    );
    let mut tracked_absent_remote = HashMap::new();
    tracked_absent_remote.insert(
        "feat@v1@origin".to_string(),
        RemoteRef {
            target: Some(RefTarget {
                adds: vec![],
                removes: vec![],
            }),
            is_tracked: true,
        },
    );
    let view_id = svc
        .write_view(Request::new(WriteViewRequest {
            repo_id: "op-repo".to_string(),
            view: Some(View {
                head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                wc_commit_ids: HashMap::new(),
                local_bookmarks: present_local,
                remote_bookmarks: tracked_absent_remote,
            }),
        }))
        .await
        .expect("tracked-absent remote bookmark with matching local bookmark must succeed")
        .into_inner()
        .view_id;

    // Tracked-absent remote bookmark without a matching local bookmark succeeds
    // (for example, partial restores like `jj op restore --what=repo` or
    // `--what=remote-tracking`).
    let mut tracked_absent_without_local = HashMap::new();
    tracked_absent_without_local.insert(
        "orphan@origin".to_string(),
        RemoteRef {
            target: Some(RefTarget {
                adds: vec![],
                removes: vec![],
            }),
            is_tracked: true,
        },
    );
    svc.write_view(Request::new(WriteViewRequest {
        repo_id: "op-repo".to_string(),
        view: Some(View {
            head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: tracked_absent_without_local,
        }),
    }))
    .await
    .expect("tracked-absent remote bookmark without matching local bookmark must succeed");

    // Untracked-absent remote bookmark without a matching local bookmark is rejected.
    let mut untracked_absent_without_local = HashMap::new();
    untracked_absent_without_local.insert(
        "orphan@origin".to_string(),
        RemoteRef {
            target: Some(RefTarget {
                adds: vec![],
                removes: vec![],
            }),
            is_tracked: false,
        },
    );
    let err = svc
        .write_view(Request::new(WriteViewRequest {
            repo_id: "op-repo".to_string(),
            view: Some(View {
                head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                wc_commit_ids: HashMap::new(),
                local_bookmarks: HashMap::new(),
                remote_bookmarks: untracked_absent_without_local,
            }),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    // Empty commit_id term inside RefTarget is rejected.
    let empty_term_err = svc
        .write_view(Request::new(WriteViewRequest {
            repo_id: "op-repo".to_string(),
            view: Some(View {
                head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                wc_commit_ids: HashMap::new(),
                local_bookmarks: HashMap::from([(
                    "main".to_string(),
                    RefTarget {
                        removes: Vec::new(),
                        adds: vec![RefTargetTerm {
                            commit_id: Vec::new(),
                        }],
                    },
                )]),
                remote_bookmarks: HashMap::new(),
            }),
        }))
        .await
        .unwrap_err();
    assert_eq!(empty_term_err.code(), Code::InvalidArgument);

    // Malformed remote bookmark keys are rejected.
    for bad_key in ["no-at-sign", "@origin", "main@"] {
        let bad_remote_err = svc
            .write_view(Request::new(WriteViewRequest {
                repo_id: "op-repo".to_string(),
                view: Some(View {
                    head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                    wc_commit_ids: HashMap::new(),
                    local_bookmarks: HashMap::new(),
                    remote_bookmarks: HashMap::from([(
                        bad_key.to_string(),
                        RemoteRef {
                            target: Some(RefTarget {
                                removes: Vec::new(),
                                adds: vec![RefTargetTerm {
                                    commit_id: cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
                                }],
                            }),
                            is_tracked: true,
                        },
                    )]),
                }),
            }))
            .await
            .unwrap_err();
        assert_eq!(
            bad_remote_err.code(),
            Code::InvalidArgument,
            "bad remote bookmark key: {bad_key}"
        );
    }

    // Non-empty commit_predecessors with commit_predecessors_set = false is rejected.
    let unset_preds_err = svc
        .write_operation(Request::new(WriteOperationRequest {
            repo_id: "op-repo".to_string(),
            operation: Some(Operation {
                view_id: view_id.clone(),
                parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                metadata: Some(OperationMetadata::default()),
                commit_predecessors: vec![CommitPredecessors {
                    commit_id: vec![0xAA; 20],
                    predecessor_ids: vec![],
                }],
                commit_predecessors_set: false,
            }),
        }))
        .await
        .unwrap_err();
    assert_eq!(unset_preds_err.code(), Code::InvalidArgument);

    // Duplicate commit_predecessors.commit_id is rejected.
    let dup_err = svc
        .write_operation(Request::new(WriteOperationRequest {
            repo_id: "op-repo".to_string(),
            operation: Some(Operation {
                view_id,
                parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                metadata: Some(OperationMetadata::default()),
                commit_predecessors: vec![
                    CommitPredecessors {
                        commit_id: vec![0xAA; 20],
                        predecessor_ids: vec![],
                    },
                    CommitPredecessors {
                        commit_id: vec![0xAA; 20],
                        predecessor_ids: vec![],
                    },
                ],
                commit_predecessors_set: true,
            }),
        }))
        .await
        .unwrap_err();
    assert_eq!(dup_err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn test_op_store_dependency_checks_reject_missing_commits_parents_and_views() {
    let svc = make_service();
    svc.store
        .register_repo_in_project("dep-repo", Some("dep-proj"), None)
        .await
        .unwrap();

    let missing_commit = vec![0xab; cc_common::COMMIT_ID_LENGTH];
    let existing_commit = vec![0x11; cc_common::COMMIT_ID_LENGTH];
    svc.store
        .put_commit(
            "dep-proj",
            &existing_commit,
            &cc_common::backend::Commit {
                commit_id: existing_commit.clone(),
                change_id: vec![0x22; cc_common::CHANGE_ID_LENGTH],
                parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
                description: "existing\n".to_string(),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Missing commit in View.head_ids -> FAILED_PRECONDITION.
    let err = svc
        .write_view(Request::new(WriteViewRequest {
            repo_id: "dep-repo".to_string(),
            view: Some(View {
                head_ids: vec![missing_commit.clone()],
                wc_commit_ids: HashMap::new(),
                local_bookmarks: HashMap::new(),
                remote_bookmarks: HashMap::new(),
            }),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);

    // Missing commit in View.wc_commit_ids -> FAILED_PRECONDITION.
    let err = svc
        .write_view(Request::new(WriteViewRequest {
            repo_id: "dep-repo".to_string(),
            view: Some(View {
                head_ids: vec![existing_commit.clone()],
                wc_commit_ids: HashMap::from([("default".to_string(), missing_commit.clone())]),
                local_bookmarks: HashMap::new(),
                remote_bookmarks: HashMap::new(),
            }),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);

    // Missing commit in View.local_bookmarks -> FAILED_PRECONDITION.
    let err = svc
        .write_view(Request::new(WriteViewRequest {
            repo_id: "dep-repo".to_string(),
            view: Some(View {
                head_ids: vec![existing_commit.clone()],
                wc_commit_ids: HashMap::new(),
                local_bookmarks: HashMap::from([(
                    "main".to_string(),
                    RefTarget {
                        adds: vec![RefTargetTerm {
                            commit_id: missing_commit.clone(),
                        }],
                        removes: Vec::new(),
                    },
                )]),
                remote_bookmarks: HashMap::new(),
            }),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);

    // Missing commit in View.remote_bookmarks -> FAILED_PRECONDITION.
    let err = svc
        .write_view(Request::new(WriteViewRequest {
            repo_id: "dep-repo".to_string(),
            view: Some(View {
                head_ids: vec![existing_commit.clone()],
                wc_commit_ids: HashMap::new(),
                local_bookmarks: HashMap::new(),
                remote_bookmarks: HashMap::from([(
                    "main@origin".to_string(),
                    RemoteRef {
                        target: Some(RefTarget {
                            adds: vec![RefTargetTerm {
                                commit_id: missing_commit.clone(),
                            }],
                            removes: Vec::new(),
                        }),
                        is_tracked: true,
                    },
                )]),
            }),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);

    // Valid view with existing_commit succeeds.
    let valid_view = View {
        head_ids: vec![existing_commit.clone()],
        wc_commit_ids: HashMap::from([("default".to_string(), existing_commit.clone())]),
        local_bookmarks: HashMap::new(),
        remote_bookmarks: HashMap::new(),
    };
    let valid_view_id = svc
        .write_view(Request::new(WriteViewRequest {
            repo_id: "dep-repo".to_string(),
            view: Some(valid_view.clone()),
        }))
        .await
        .unwrap()
        .into_inner()
        .view_id;

    // WriteOperation with missing view_id -> FAILED_PRECONDITION.
    let err = svc
        .write_operation(Request::new(WriteOperationRequest {
            repo_id: "dep-repo".to_string(),
            operation: Some(Operation {
                view_id: vec![0x99; cc_common::VIEW_ID_LENGTH],
                parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                metadata: Some(OperationMetadata::default()),
                commit_predecessors: Vec::new(),
                commit_predecessors_set: true,
            }),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);

    // WriteOperation with missing parent operation -> FAILED_PRECONDITION.
    let err = svc
        .write_operation(Request::new(WriteOperationRequest {
            repo_id: "dep-repo".to_string(),
            operation: Some(Operation {
                view_id: valid_view_id.clone(),
                parents: vec![vec![0x88; cc_common::OPERATION_ID_LENGTH]],
                metadata: Some(OperationMetadata::default()),
                commit_predecessors: Vec::new(),
                commit_predecessors_set: true,
            }),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);

    // WriteOperation with missing commit in commit_predecessors (key or predecessor) -> FAILED_PRECONDITION.
    for bad_preds in [
        vec![CommitPredecessors {
            commit_id: missing_commit.clone(),
            predecessor_ids: Vec::new(),
        }],
        vec![CommitPredecessors {
            commit_id: existing_commit.clone(),
            predecessor_ids: vec![missing_commit.clone()],
        }],
    ] {
        let err = svc
            .write_operation(Request::new(WriteOperationRequest {
                repo_id: "dep-repo".to_string(),
                operation: Some(Operation {
                    view_id: valid_view_id.clone(),
                    parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                    metadata: Some(OperationMetadata::default()),
                    commit_predecessors: bad_preds,
                    commit_predecessors_set: true,
                }),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
    }

    // PublishOperation enforces both view commits and operation parents/predecessors.
    let err = svc
        .publish_operation(Request::new(PublishOperationRequest {
            repo_id: "dep-repo".to_string(),
            view: Some(View {
                head_ids: vec![missing_commit.clone()],
                wc_commit_ids: HashMap::new(),
                local_bookmarks: HashMap::new(),
                remote_bookmarks: HashMap::new(),
            }),
            operation: Some(Operation {
                view_id: Vec::new(),
                parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                metadata: Some(OperationMetadata::default()),
                commit_predecessors: Vec::new(),
                commit_predecessors_set: true,
            }),
            old_op_head_ids: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);

    let err = svc
        .publish_operation(Request::new(PublishOperationRequest {
            repo_id: "dep-repo".to_string(),
            view: Some(valid_view),
            operation: Some(Operation {
                view_id: Vec::new(),
                parents: vec![vec![0x77; cc_common::OPERATION_ID_LENGTH]],
                metadata: Some(OperationMetadata::default()),
                commit_predecessors: Vec::new(),
                commit_predecessors_set: true,
            }),
            old_op_head_ids: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn test_publish_operation_superseded_retry_does_not_resurrect_old_head() {
    let mem_store = Arc::new(CompositeStore::in_memory());
    let dir = tempfile::tempdir().unwrap();
    let sqlite_store = Arc::new(CompositeStore::new(
        Arc::new(crate::store::SqliteMetadataStore::open(&dir.path().join("pub.db")).unwrap()),
        Arc::new(crate::store::MemoryBlobStore::new()),
    ));

    for store in [mem_store, sqlite_store] {
        store
            .register_repo_in_project("pub-repo", Some("pub-proj"), None)
            .await
            .unwrap();
        let svc = CommitCloudOpStoreService::new(store);
        let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
        let root_view = View {
            head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: HashMap::new(),
        };

        let req1 = PublishOperationRequest {
            repo_id: "pub-repo".to_string(),
            view: Some(root_view.clone()),
            operation: Some(Operation {
                view_id: Vec::new(),
                parents: vec![root_op.clone()],
                metadata: Some(OperationMetadata {
                    description: "op1".to_string(),
                    ..Default::default()
                }),
                commit_predecessors: Vec::new(),
                commit_predecessors_set: true,
            }),
            old_op_head_ids: vec![root_op.clone()],
        };
        let resp1 = svc
            .publish_operation(Request::new(req1.clone()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp1.current_op_head_ids, vec![resp1.operation_id.clone()]);

        // Advance op1 -> op2.
        let req2 = PublishOperationRequest {
            repo_id: "pub-repo".to_string(),
            view: Some(root_view),
            operation: Some(Operation {
                view_id: Vec::new(),
                parents: vec![resp1.operation_id.clone()],
                metadata: Some(OperationMetadata {
                    description: "op2".to_string(),
                    ..Default::default()
                }),
                commit_predecessors: Vec::new(),
                commit_predecessors_set: true,
            }),
            old_op_head_ids: vec![resp1.operation_id.clone()],
        };
        let resp2 = svc
            .publish_operation(Request::new(req2.clone()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp2.current_op_head_ids, vec![resp2.operation_id.clone()]);

        // Retrying superseded PublishOperation(op1, old=[root]) must NOT resurrect op1.
        let retry1 = svc
            .publish_operation(Request::new(req1))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(retry1.operation_id, resp1.operation_id);
        assert_eq!(
            retry1.current_op_head_ids,
            vec![resp2.operation_id.clone()],
            "superseded PublishOperation retry must not resurrect op1"
        );

        // Retrying still-active PublishOperation(op2, old=[op1]) is also idempotent.
        let retry2 = svc
            .publish_operation(Request::new(req2))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(retry2.current_op_head_ids, vec![resp2.operation_id.clone()]);

        let live_heads = svc
            .get_op_heads(Request::new(GetOpHeadsRequest {
                repo_id: "pub-repo".to_string(),
            }))
            .await
            .unwrap()
            .into_inner()
            .op_head_ids;
        assert_eq!(live_heads, vec![resp2.operation_id]);
    }
}

#[tokio::test]
async fn test_resolve_operation_prefix_rpc_outcomes_and_validation() {
    let svc = make_service();
    svc.store
        .register_repo_in_project("op-prefix-repo", Some("op-prefix-proj"), None)
        .await
        .unwrap();

    let make_op = |desc: &str| Operation {
        view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
        parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
        metadata: Some(OperationMetadata {
            description: desc.to_string(),
            ..Default::default()
        }),
        commit_predecessors: Vec::new(),
        commit_predecessors_set: true,
    };
    let mut op_a = vec![0x00; cc_common::OPERATION_ID_LENGTH];
    op_a[0] = 0xab;
    op_a[1] = 0x10;
    let mut op_b = vec![0x00; cc_common::OPERATION_ID_LENGTH];
    op_b[0] = 0xab;
    op_b[1] = 0x20;
    svc.store
        .put_operation("op-prefix-repo", &op_a, &make_op("op-a"))
        .await
        .unwrap();
    svc.store
        .put_operation("op-prefix-repo", &op_b, &make_op("op-b"))
        .await
        .unwrap();

    // NO_MATCH
    let no_match = svc
        .resolve_operation_prefix(Request::new(ResolveOperationPrefixRequest {
            repo_id: "op-prefix-repo".to_string(),
            hex_prefix: "ff00".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        no_match.status,
        PrefixResolutionStatus::PrefixResolutionNoMatch as i32
    );
    assert!(no_match.matched_operation_id.is_empty());

    // SINGLE_MATCH (including odd-length uppercase prefix)
    let single = svc
        .resolve_operation_prefix(Request::new(ResolveOperationPrefixRequest {
            repo_id: "op-prefix-repo".to_string(),
            hex_prefix: "AB1".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        single.status,
        PrefixResolutionStatus::PrefixResolutionSingleMatch as i32
    );
    assert_eq!(single.matched_operation_id, op_a);

    // AMBIGUOUS_MATCH
    let ambiguous = svc
        .resolve_operation_prefix(Request::new(ResolveOperationPrefixRequest {
            repo_id: "op-prefix-repo".to_string(),
            hex_prefix: "ab".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        ambiguous.status,
        PrefixResolutionStatus::PrefixResolutionAmbiguousMatch as i32
    );
    assert!(ambiguous.matched_operation_id.is_empty());

    // Virtual root operation match without a persisted root row
    let root_match = svc
        .resolve_operation_prefix(Request::new(ResolveOperationPrefixRequest {
            repo_id: "op-prefix-repo".to_string(),
            hex_prefix: "0000".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        root_match.status,
        PrefixResolutionStatus::PrefixResolutionSingleMatch as i32
    );
    assert_eq!(
        root_match.matched_operation_id,
        cc_common::ROOT_OPERATION_ID_BYTES
    );

    // Malformed hex -> INVALID_ARGUMENT
    let bad_hex = svc
        .resolve_operation_prefix(Request::new(ResolveOperationPrefixRequest {
            repo_id: "op-prefix-repo".to_string(),
            hex_prefix: "not-hex!".to_string(),
        }))
        .await
        .unwrap_err();
    assert_eq!(bad_hex.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn test_update_op_heads_rejects_root_operation_id_as_new_head() {
    let svc = make_service();
    svc.store
        .register_repo_in_project("root-guard-repo", Some("root-guard-proj"), None)
        .await
        .unwrap();

    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let op_id = svc
        .write_operation(Request::new(WriteOperationRequest {
            repo_id: "root-guard-repo".to_string(),
            operation: Some(Operation {
                view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
                parents: vec![root_op.clone()],
                metadata: Some(OperationMetadata {
                    description: "initial op".to_string(),
                    ..Default::default()
                }),
                commit_predecessors: Vec::new(),
                commit_predecessors_set: true,
            }),
        }))
        .await
        .unwrap()
        .into_inner()
        .operation_id;
    let heads = svc
        .update_op_heads(Request::new(UpdateOpHeadsRequest {
            repo_id: "root-guard-repo".to_string(),
            old_op_head_ids: vec![root_op.clone()],
            new_op_head_id: op_id.clone(),
        }))
        .await
        .unwrap()
        .into_inner()
        .current_op_head_ids;
    assert_eq!(heads, vec![op_id.clone()]);

    // Rewinding to ROOT_OPERATION_ID or resurrecting it alongside existing heads must fail.
    for old_heads in [vec![op_id.clone()], Vec::new()] {
        let err = svc
            .update_op_heads(Request::new(UpdateOpHeadsRequest {
                repo_id: "root-guard-repo".to_string(),
                old_op_head_ids: old_heads,
                new_op_head_id: root_op.clone(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
    }

    let live_heads = svc
        .get_op_heads(Request::new(GetOpHeadsRequest {
            repo_id: "root-guard-repo".to_string(),
        }))
        .await
        .unwrap()
        .into_inner()
        .op_head_ids;
    assert_eq!(live_heads, vec![op_id]);
}

#[tokio::test]
async fn test_update_op_heads_rejects_root_operation_id() {
    let store = Arc::new(CompositeStore::in_memory());
    store
        .register_repo_in_project("repo-1", Some("proj-1"), None)
        .await
        .unwrap();
    let svc = CommitCloudOpStoreService::new(store.clone());
    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();

    // Reject virtual ROOT_OPERATION_ID as new_op_head_id on both empty and root-parented updates.
    for old_op_head_ids in [Vec::new(), vec![root_op.clone()]] {
        let err = svc
            .update_op_heads(tonic::Request::new(UpdateOpHeadsRequest {
                repo_id: "repo-1".to_string(),
                old_op_head_ids,
                new_op_head_id: root_op.clone(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
    }
    assert_eq!(
        store.get_op_heads("repo-1").await.unwrap(),
        vec![root_op.clone()]
    );

    // Also reject ROOT_OPERATION_ID when transitioning away from an active non-root head.
    let op1_id = vec![0x11; cc_common::OPERATION_ID_LENGTH];
    store
        .put_operation(
            "repo-1",
            &op1_id,
            &Operation {
                view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
                parents: vec![root_op.clone()],
                metadata: None,
                commit_predecessors: Vec::new(),
                commit_predecessors_set: true,
            },
        )
        .await
        .unwrap();
    svc.update_op_heads(tonic::Request::new(UpdateOpHeadsRequest {
        repo_id: "repo-1".to_string(),
        old_op_head_ids: vec![root_op.clone()],
        new_op_head_id: op1_id.clone(),
    }))
    .await
    .expect("valid non-root op head transition must succeed");

    let err = svc
        .update_op_heads(tonic::Request::new(UpdateOpHeadsRequest {
            repo_id: "repo-1".to_string(),
            old_op_head_ids: vec![op1_id.clone()],
            new_op_head_id: root_op,
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert_eq!(store.get_op_heads("repo-1").await.unwrap(), vec![op1_id]);
}
