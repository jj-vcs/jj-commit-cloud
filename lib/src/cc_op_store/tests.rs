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

use cc_common::op_store::op_store_service_server::{OpStoreService, OpStoreServiceServer};
use tonic::{Request, Response, Status};

use super::*;
use crate::util::run_async;

struct MockOpStoreServer {
    read_view_response_id: Option<Vec<u8>>,
    read_op_response: Option<cc_common::op_store::Operation>,
    prefix_match_id: Option<Vec<u8>>,
    publish_op_heads: Vec<Vec<u8>>,
}

#[tonic::async_trait]
impl OpStoreService for MockOpStoreServer {
    async fn read_operation(
        &self,
        _: Request<ReadOperationRequest>,
    ) -> Result<Response<ReadOperationResponse>, Status> {
        match &self.read_op_response {
            Some(op) => Ok(Response::new(ReadOperationResponse {
                operation: Some(op.clone()),
            })),
            None => Err(Status::not_found("missing op")),
        }
    }
    async fn write_operation(
        &self,
        _: Request<WriteOperationRequest>,
    ) -> Result<Response<WriteOperationResponse>, Status> {
        Ok(Response::new(WriteOperationResponse {
            operation_id: vec![0x11; 3],
        }))
    }
    async fn read_view(
        &self,
        _: Request<ReadViewRequest>,
    ) -> Result<Response<ReadViewResponse>, Status> {
        match &self.read_view_response_id {
            Some(view_id) => Ok(Response::new(ReadViewResponse {
                view_id: view_id.clone(),
                view: Some(cc_common::op_store::View {
                    head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                    wc_commit_ids: HashMap::new(),
                    local_bookmarks: HashMap::new(),
                    remote_bookmarks: HashMap::new(),
                }),
            })),
            None => Err(Status::not_found("missing view")),
        }
    }
    async fn write_view(
        &self,
        _: Request<WriteViewRequest>,
    ) -> Result<Response<WriteViewResponse>, Status> {
        Ok(Response::new(WriteViewResponse {
            view_id: vec![0x22; 4],
        }))
    }
    async fn get_op_heads(
        &self,
        _: Request<GetOpHeadsRequest>,
    ) -> Result<Response<GetOpHeadsResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn update_op_heads(
        &self,
        _: Request<UpdateOpHeadsRequest>,
    ) -> Result<Response<UpdateOpHeadsResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn reconcile_op_heads(
        &self,
        _: Request<ReconcileOpHeadsRequest>,
    ) -> Result<Response<ReconcileOpHeadsResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn resolve_operation_prefix(
        &self,
        _: Request<ResolveOperationPrefixRequest>,
    ) -> Result<Response<ResolveOperationPrefixResponse>, Status> {
        match &self.prefix_match_id {
            Some(id) => Ok(Response::new(ResolveOperationPrefixResponse {
                status: PrefixResolutionStatus::PrefixResolutionSingleMatch as i32,
                matched_operation_id: id.clone(),
            })),
            None => Err(Status::unimplemented("unused")),
        }
    }
    async fn publish_operation(
        &self,
        _: Request<PublishOperationRequest>,
    ) -> Result<Response<PublishOperationResponse>, Status> {
        Ok(Response::new(PublishOperationResponse {
            view_id: vec![0x33; cc_common::VIEW_ID_LENGTH],
            operation_id: vec![0x44; cc_common::OPERATION_ID_LENGTH],
            current_op_head_ids: self.publish_op_heads.clone(),
        }))
    }
}

fn spawn_mock_op_store(svc: MockOpStoreServer) -> String {
    run_async(|| async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let incoming = tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
            .map_err(|e| format!("incoming error: {e}"))?;
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(OpStoreServiceServer::new(svc))
                .serve_with_incoming(incoming),
        );
        Ok(format!("http://{addr}"))
    })
    .unwrap()
}

#[test]
fn operation_and_view_from_proto_reject_malformed_ids_and_empty_heads() {
    let valid_op = cc_common::op_store::Operation {
        view_id: vec![0xaa; cc_common::VIEW_ID_LENGTH],
        parents: vec![vec![0xbb; cc_common::OPERATION_ID_LENGTH]],
        metadata: None,
        commit_predecessors: vec![cc_common::op_store::CommitPredecessors {
            commit_id: vec![0xcc; cc_common::COMMIT_ID_LENGTH],
            predecessor_ids: vec![vec![0xdd; cc_common::COMMIT_ID_LENGTH]],
        }],
        commit_predecessors_set: true,
    };
    assert!(operation_from_proto(valid_op.clone()).is_ok());

    let mut bad_view_id = valid_op.clone();
    bad_view_id.view_id = vec![0xaa; 5];
    assert!(operation_from_proto(bad_view_id).is_err());

    let mut bad_parent = valid_op.clone();
    bad_parent.parents = vec![vec![0xbb; 15]];
    assert!(operation_from_proto(bad_parent).is_err());

    let mut empty_parents_non_root = valid_op.clone();
    empty_parents_non_root.parents.clear();
    assert!(operation_from_proto(empty_parents_non_root).is_err());

    let mut dup_parents = valid_op.clone();
    dup_parents
        .parents
        .push(vec![0xbb; cc_common::OPERATION_ID_LENGTH]);
    assert!(operation_from_proto(dup_parents).is_err());

    let mut mixed_root_parent = valid_op.clone();
    mixed_root_parent
        .parents
        .push(cc_common::ROOT_OPERATION_ID_BYTES.to_vec());
    assert!(operation_from_proto(mixed_root_parent).is_err());

    let mut dup_pred_keys = valid_op.clone();
    dup_pred_keys
        .commit_predecessors
        .push(cc_common::op_store::CommitPredecessors {
            commit_id: vec![0xcc; cc_common::COMMIT_ID_LENGTH],
            predecessor_ids: vec![],
        });
    assert!(operation_from_proto(dup_pred_keys).is_err());

    let mut unset_with_preds = valid_op.clone();
    unset_with_preds.commit_predecessors_set = false;
    assert!(operation_from_proto(unset_with_preds).is_err());

    let mut bad_pred = valid_op;
    bad_pred.commit_predecessors[0].predecessor_ids = vec![vec![0xdd; 19]];
    assert!(operation_from_proto(bad_pred).is_err());

    let valid_view = cc_common::op_store::View {
        head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
        wc_commit_ids: HashMap::from([(
            "default".to_string(),
            cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
        )]),
        local_bookmarks: HashMap::new(),
        remote_bookmarks: HashMap::new(),
    };
    assert!(view_from_proto(valid_view.clone()).is_ok());

    let mut empty_heads = valid_view.clone();
    empty_heads.head_ids.clear();
    assert!(view_from_proto(empty_heads).is_err());

    let mut bad_head = valid_view.clone();
    bad_head.head_ids = vec![vec![0x11; 10]];
    assert!(view_from_proto(bad_head).is_err());

    let mut bad_wc = valid_view.clone();
    bad_wc
        .wc_commit_ids
        .insert("default".to_string(), vec![0x22; 12]);
    assert!(view_from_proto(bad_wc).is_err());

    let mut empty_ws_name = valid_view.clone();
    empty_ws_name
        .wc_commit_ids
        .insert(String::new(), cc_common::ROOT_COMMIT_ID_BYTES.to_vec());
    assert!(view_from_proto(empty_ws_name).is_err());

    let mut empty_local_bm_name = valid_view.clone();
    empty_local_bm_name.local_bookmarks.insert(
        String::new(),
        cc_common::op_store::RefTarget {
            removes: vec![],
            adds: vec![cc_common::op_store::RefTargetTerm {
                commit_id: cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
            }],
        },
    );
    assert!(view_from_proto(empty_local_bm_name).is_err());

    let mut absent_local_bm = valid_view.clone();
    absent_local_bm.local_bookmarks.insert(
        "main".to_string(),
        cc_common::op_store::RefTarget {
            removes: vec![],
            adds: vec![],
        },
    );
    assert!(view_from_proto(absent_local_bm).is_err());

    let mut cancelling_local_bm = valid_view.clone();
    cancelling_local_bm.local_bookmarks.insert(
        "main".to_string(),
        cc_common::op_store::RefTarget {
            removes: vec![cc_common::op_store::RefTargetTerm {
                commit_id: cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
            }],
            adds: vec![cc_common::op_store::RefTargetTerm {
                commit_id: cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
            }],
        },
    );
    assert!(view_from_proto(cancelling_local_bm).is_err());

    for bad_remote_key in ["no-at-sign", "@origin", "main@", "@"] {
        let mut bad_remote = valid_view.clone();
        bad_remote.remote_bookmarks.insert(
            bad_remote_key.to_string(),
            cc_common::op_store::RemoteRef {
                target: Some(cc_common::op_store::RefTarget {
                    removes: vec![],
                    adds: vec![cc_common::op_store::RefTargetTerm {
                        commit_id: cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
                    }],
                }),
                is_tracked: true,
            },
        );
        assert!(
            view_from_proto(bad_remote).is_err(),
            "expected malformed remote_bookmarks key {bad_remote_key:?} to be rejected"
        );
    }

    // Valid tracked remote bookmark with absent target remains allowed.
    let mut tracked_absent_remote = valid_view.clone();
    tracked_absent_remote.remote_bookmarks.insert(
        "main@origin".to_string(),
        cc_common::op_store::RemoteRef {
            target: Some(cc_common::op_store::RefTarget {
                removes: vec![],
                adds: vec![],
            }),
            is_tracked: true,
        },
    );
    assert!(view_from_proto(tracked_absent_remote).is_ok());

    // Missing remote_ref_proto.target (`None`) must be rejected (both tracked and untracked).
    for is_tracked in [false, true] {
        let mut missing_target_remote = valid_view.clone();
        missing_target_remote.remote_bookmarks.insert(
            "main@origin".to_string(),
            cc_common::op_store::RemoteRef {
                target: None,
                is_tracked,
            },
        );
        assert!(
            view_from_proto(missing_target_remote).is_err(),
            "remote bookmark with target=None (is_tracked={is_tracked}) must be rejected"
        );
    }

    // Absent untracked remote bookmark (empty or cancelling terms) must be rejected.
    let mut absent_untracked_remote = valid_view.clone();
    absent_untracked_remote.remote_bookmarks.insert(
        "main@origin".to_string(),
        cc_common::op_store::RemoteRef {
            target: Some(cc_common::op_store::RefTarget {
                removes: vec![],
                adds: vec![],
            }),
            is_tracked: false,
        },
    );
    assert!(view_from_proto(absent_untracked_remote).is_err());

    let mut cancelling_untracked_remote = valid_view.clone();
    cancelling_untracked_remote.remote_bookmarks.insert(
        "main@origin".to_string(),
        cc_common::op_store::RemoteRef {
            target: Some(cc_common::op_store::RefTarget {
                removes: vec![cc_common::op_store::RefTargetTerm {
                    commit_id: cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
                }],
                adds: vec![cc_common::op_store::RefTargetTerm {
                    commit_id: cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
                }],
            }),
            is_tracked: false,
        },
    );
    assert!(view_from_proto(cancelling_untracked_remote).is_err());

    // Non-absent untracked remote bookmark remains allowed.
    let mut present_untracked_remote = valid_view.clone();
    present_untracked_remote.remote_bookmarks.insert(
        "main@origin".to_string(),
        cc_common::op_store::RemoteRef {
            target: Some(cc_common::op_store::RefTarget {
                removes: vec![],
                adds: vec![cc_common::op_store::RefTargetTerm {
                    commit_id: cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
                }],
            }),
            is_tracked: false,
        },
    );
    assert!(view_from_proto(present_untracked_remote).is_ok());

    let mut bad_bm = valid_view;
    bad_bm.local_bookmarks.insert(
        "main".to_string(),
        cc_common::op_store::RefTarget {
            removes: vec![],
            adds: vec![cc_common::op_store::RefTargetTerm {
                commit_id: vec![0x33; 8],
            }],
        },
    );
    assert!(view_from_proto(bad_bm).is_err());
}

#[test]
fn verified_operation_from_proto_rejects_malformed_root_operation_payloads() {
    let root_op_id = OperationId::from_bytes(&cc_common::ROOT_OPERATION_ID_BYTES);
    let valid_root = cc_common::op_store::Operation {
        view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
        parents: vec![],
        metadata: None,
        commit_predecessors: vec![],
        commit_predecessors_set: true,
    };
    assert!(verified_operation_from_proto(&root_op_id, valid_root.clone()).is_ok());

    // Root operation with parents must be rejected.
    let mut root_with_parents = valid_root.clone();
    root_with_parents.parents = vec![vec![0x11; cc_common::OPERATION_ID_LENGTH]];
    let err = verified_operation_from_proto(&root_op_id, root_with_parents)
        .expect_err("ROOT_OPERATION_ID with non-empty parents must be rejected");
    assert!(
        err.to_string().contains("must not have parents"),
        "unexpected error: {err}"
    );

    // Root operation pointing to a non-root view_id must be rejected.
    let mut root_with_non_root_view = valid_root;
    root_with_non_root_view.view_id = vec![0x22; cc_common::VIEW_ID_LENGTH];
    let err = verified_operation_from_proto(&root_op_id, root_with_non_root_view)
        .expect_err("ROOT_OPERATION_ID with non-ROOT_VIEW_ID must be rejected");
    assert!(
        err.to_string().contains("ROOT_VIEW_ID"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn read_maps_not_found_and_writes_validate_id_lengths() {
    let url = spawn_mock_op_store(MockOpStoreServer {
        read_view_response_id: None,
        read_op_response: None,
        prefix_match_id: None,
        publish_op_heads: vec![vec![0x55; 7]],
    });

    let store = CommitCloudOpStore::new("repo-1".to_string(), url);
    let missing_view = ViewId::from_bytes(&[0x11; cc_common::VIEW_ID_LENGTH]);
    assert!(matches!(
        store.read_view(&missing_view).await,
        Err(OpStoreError::ObjectNotFound { object_type, .. }) if object_type == "view"
    ));

    let missing_op = OperationId::from_bytes(&[0x22; cc_common::OPERATION_ID_LENGTH]);
    assert!(matches!(
        store.read_operation(&missing_op).await,
        Err(OpStoreError::ObjectNotFound { object_type, .. }) if object_type == "operation"
    ));

    let sample_view = View::make_root(CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES));
    let sample_op = Operation {
        view_id: ViewId::from_bytes(&cc_common::ROOT_VIEW_ID_BYTES),
        parents: vec![OperationId::from_bytes(&cc_common::ROOT_OPERATION_ID_BYTES)],
        metadata: OperationMetadata {
            time: TimestampRange {
                start: Timestamp {
                    timestamp: MillisSinceEpoch(0),
                    tz_offset: 0,
                },
                end: Timestamp {
                    timestamp: MillisSinceEpoch(0),
                    tz_offset: 0,
                },
            },
            description: "test".to_string(),
            hostname: String::new(),
            username: String::new(),
            is_snapshot: false,
            workspace_name: None,
            attributes: BTreeMap::new(),
        },
        commit_predecessors: None,
    };
    assert!(store.write_view(&sample_view).await.is_err());
    assert!(store.write_operation(&sample_op).await.is_err());
    assert!(
        store
            .publish_operation(&sample_view, &sample_op, &[])
            .await
            .is_err()
    );
}

#[tokio::test]
async fn read_view_and_operation_validate_ids_and_non_root_parents() {
    // Mismatched ReadViewResponse.view_id must be rejected.
    let mismatched_view_url = spawn_mock_op_store(MockOpStoreServer {
        read_view_response_id: Some(vec![0x99; cc_common::VIEW_ID_LENGTH]),
        read_op_response: Some(cc_common::op_store::Operation {
            view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
            parents: vec![],
            metadata: None,
            commit_predecessors: vec![],
            commit_predecessors_set: true,
        }),
        prefix_match_id: Some(vec![0x99; cc_common::OPERATION_ID_LENGTH]),
        publish_op_heads: Vec::new(),
    });
    let store = CommitCloudOpStore::new("repo-1".to_string(), mismatched_view_url);
    let requested_view = ViewId::from_bytes(&[0x11; cc_common::VIEW_ID_LENGTH]);
    let err = store
        .read_view(&requested_view)
        .await
        .expect_err("mismatched ReadViewResponse.view_id must be rejected");
    assert!(
        err.to_string().contains("did not match requested"),
        "unexpected error: {err}"
    );

    // Non-root operation with empty parents (even pointing to ROOT_VIEW_ID) must be rejected,
    // while the root operation itself succeeds with empty parents.
    let root_op_id = OperationId::from_bytes(&cc_common::ROOT_OPERATION_ID_BYTES);
    assert!(store.read_operation(&root_op_id).await.is_ok());

    let non_root_op_id = OperationId::from_bytes(&[0x22; cc_common::OPERATION_ID_LENGTH]);
    let err = store
        .read_operation(&non_root_op_id)
        .await
        .expect_err("non-root operation with empty parents must be rejected");
    assert!(
        err.to_string().contains("must have at least one parent"),
        "unexpected error: {err}"
    );

    // Mismatched ResolveOperationPrefix matched_operation_id must be rejected.
    let prefix = HexPrefix::try_from_hex("1122").unwrap();
    let err = store
        .resolve_operation_id_prefix(&prefix)
        .await
        .expect_err("mismatched matched_operation_id must be rejected");
    assert!(
        err.to_string().contains("did not match requested prefix"),
        "unexpected error: {err}"
    );

    // Empty ReadViewResponse.view_id remains accepted for compatibility.
    let empty_view_id_url = spawn_mock_op_store(MockOpStoreServer {
        read_view_response_id: Some(Vec::new()),
        read_op_response: None,
        prefix_match_id: None,
        publish_op_heads: Vec::new(),
    });
    let compat_store = CommitCloudOpStore::new("repo-1".to_string(), empty_view_id_url);
    assert!(compat_store.read_view(&requested_view).await.is_ok());
}

#[tokio::test]
async fn publish_operation_rejects_empty_current_op_head_ids() {
    let url = spawn_mock_op_store(MockOpStoreServer {
        read_view_response_id: None,
        read_op_response: None,
        prefix_match_id: None,
        publish_op_heads: Vec::new(),
    });
    let store = CommitCloudOpStore::new("repo-empty-heads".to_string(), url);
    let sample_view = View::make_root(CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES));
    let sample_op = Operation {
        view_id: ViewId::from_bytes(&cc_common::ROOT_VIEW_ID_BYTES),
        parents: vec![OperationId::from_bytes(&cc_common::ROOT_OPERATION_ID_BYTES)],
        metadata: OperationMetadata {
            time: TimestampRange {
                start: Timestamp {
                    timestamp: MillisSinceEpoch(0),
                    tz_offset: 0,
                },
                end: Timestamp {
                    timestamp: MillisSinceEpoch(0),
                    tz_offset: 0,
                },
            },
            description: "empty heads test".to_string(),
            hostname: String::new(),
            username: String::new(),
            is_snapshot: false,
            workspace_name: None,
            attributes: BTreeMap::new(),
        },
        commit_predecessors: None,
    };
    let err = store
        .publish_operation(&sample_view, &sample_op, &[])
        .await
        .expect_err("PublishOperation returning empty current_op_head_ids must fail");
    assert!(
        err.to_string()
            .contains("PublishOperation returned empty current_op_head_ids"),
        "unexpected error: {err}"
    );
}
