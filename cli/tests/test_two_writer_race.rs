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

//! Two writers that expect the same op head race to replace it.
//! Every round must give exactly one success and one CAS rejection.

use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::sync::Barrier;

use cc_common::backend::backend_service_client::BackendServiceClient;
use cc_common::backend::{HashAlgorithm, RegisterRepositoryRequest};
use cc_common::op_store::op_store_service_client::OpStoreServiceClient;
use cc_common::op_store::{
    GetOpHeadsRequest, Operation, OperationMetadata, UpdateOpHeadsRequest, WriteOperationRequest,
};
use testutils::spawn_sqlite_server;

const ROUNDS: usize = 50;

fn make_op(parent: Vec<u8>, desc: String, ts: i64) -> Operation {
    Operation {
        parents: vec![parent],
        view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
        metadata: Some(OperationMetadata {
            start_time_millis: ts,
            end_time_millis: ts,
            description: desc,
            hostname: "test".to_string(),
            username: "test".to_string(),
            is_snapshot: false,
            workspace_name: None,
            attributes: Default::default(),
        }),
        commit_predecessors: vec![],
        commit_predecessors_set: false,
    }
}

async fn run_two_writer_race(server_url: String) {
    let mut backend = BackendServiceClient::connect(server_url.clone())
        .await
        .unwrap();
    let repo_id = backend
        .register_repository(RegisterRepositoryRequest {
            project_id: "race-project".to_string(),
            repo_id: None,
            name: None,
            hash_algorithm: HashAlgorithm::Sha1 as i32,
        })
        .await
        .unwrap()
        .into_inner()
        .repo_id;

    // Move off the root op first: a root-op parent is allowed to fork (new workspace init).
    let mut setup = OpStoreServiceClient::connect(server_url.clone())
        .await
        .unwrap();
    let mut base = setup
        .write_operation(WriteOperationRequest {
            repo_id: repo_id.clone(),
            operation: Some(make_op(
                cc_common::ROOT_OPERATION_ID_BYTES.to_vec(),
                "base".to_string(),
                1,
            )),
        })
        .await
        .unwrap()
        .into_inner()
        .operation_id;
    setup
        .update_op_heads(UpdateOpHeadsRequest {
            repo_id: repo_id.clone(),
            old_op_head_ids: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
            new_op_head_id: base.clone(),
        })
        .await
        .unwrap();

    for round in 0..ROUNDS {
        let barrier = Arc::new(Barrier::new(2));
        let mut handles = Vec::new();
        for writer in 0..2u8 {
            let (url, repo_id, base, barrier) = (
                server_url.clone(),
                repo_id.clone(),
                base.clone(),
                barrier.clone(),
            );
            handles.push(tokio::spawn(async move {
                let mut client = OpStoreServiceClient::connect(url).await.unwrap();
                let new_id = client
                    .write_operation(WriteOperationRequest {
                        repo_id: repo_id.clone(),
                        operation: Some(make_op(
                            base.clone(),
                            format!("round {round} writer {writer}"),
                            (round as i64) * 10 + (writer as i64) + 10,
                        )),
                    })
                    .await
                    .unwrap()
                    .into_inner()
                    .operation_id;
                barrier.wait().await;
                let result = client
                    .update_op_heads(UpdateOpHeadsRequest {
                        repo_id,
                        old_op_head_ids: vec![base],
                        new_op_head_id: new_id.clone(),
                    })
                    .await;
                (new_id, result)
            }));
        }

        let mut winners = Vec::new();
        let mut rejections = 0;
        for handle in handles {
            let (new_id, result) = handle.await.unwrap();
            match result {
                Ok(_) => winners.push(new_id),
                Err(status) if status.code() == tonic::Code::Aborted => rejections += 1,
                Err(status) => panic!("round {round}: unexpected error {status:?}"),
            }
        }
        assert_eq!(
            (winners.len(), rejections),
            (1, 1),
            "round {round}: expected one success and one rejection"
        );

        let heads = setup
            .get_op_heads(GetOpHeadsRequest {
                repo_id: repo_id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .op_head_ids;
        assert_eq!(
            heads, winners,
            "round {round}: head must be the winner only"
        );
        base = winners.pop().unwrap();
    }
}

#[tokio::test]
async fn test_two_writer_race_sqlite() {
    let db_file = NamedTempFile::new().unwrap();
    let server = spawn_sqlite_server(db_file.path()).await;
    run_two_writer_race(server.url().to_string()).await;
}

/// Runs against an already-started server, e.g. one on the Spanner store or the emulator:
/// CC_RACE_SERVER_URL=http://127.0.0.1:8080 cargo test --test test_two_writer_race -- --ignored
#[tokio::test]
#[ignore]
async fn test_two_writer_race_external_server() {
    let url = std::env::var("CC_RACE_SERVER_URL").expect("set CC_RACE_SERVER_URL");
    run_two_writer_race(url).await;
}
