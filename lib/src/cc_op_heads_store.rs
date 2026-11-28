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

//! `jj_lib::op_heads_store::OpHeadsStore` adapter over `OpStoreService` gRPC RPCs.
//!
//! # Authority and Crash-Safety Ordering
//! `update_op_heads` is called only after the target `Operation` and its `View` have
//! been persisted via [`crate::cc_op_store::CommitCloudOpStore`].
//!
//! # Concurrency and Merge Invariants
//! `lock()` is an intentional client-side no-op. `get_op_heads` invokes `ReconcileOpHeads`
//! so jj-cc-server merges divergent operation heads without cross-client locks. When
//! divergent operations carry cross-branch commit rewrites that require descendant
//! rebasing, `ReconcileOpHeads` sets `client_resolution_required = true` with `current_op_head_ids` so `jj_lib` resolves the rewrites against `DefaultIndexStore`.
//! `UpdateOpHeads` removes `old_ids` and appends `new_id` while retaining concurrent
//! sibling heads.
//!
//! # Cross-Process and RPC Boundaries
//! Connects to `OpStoreService` using the `server_url` and `repo_id` stored in
//! `.jj/repo/store/config.toml`.

use std::fmt::Debug;
use std::path::Path;

use async_trait::async_trait;
use cc_common::op_store::*;
use jj_lib::object_id::ObjectId;
use jj_lib::op_heads_store::{OpHeadsStore, OpHeadsStoreError, OpHeadsStoreLock};
use jj_lib::op_store::OperationId;

use crate::util::{CommitCloudConfig, connect_op_store_client, run_on_tokio};

#[derive(Debug)]
pub struct CommitCloudOpHeadsStore {
    server_url: String,
    repo_id: String,
}

struct CommitCloudOpHeadsStoreNoLock;
impl OpHeadsStoreLock for CommitCloudOpHeadsStoreNoLock {}

impl CommitCloudOpHeadsStore {
    pub fn name() -> &'static str {
        "commit_cloud"
    }

    pub fn new(repo_id: String, server_url: String) -> Self {
        Self {
            server_url,
            repo_id,
        }
    }

    pub fn load(store_path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config = CommitCloudConfig::load_from_store(store_path)?;
        Ok(Self::new(config.repo_id, config.server_url))
    }

    pub async fn get_op_heads_without_reconciliation(
        &self,
    ) -> Result<Vec<OperationId>, OpHeadsStoreError> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();

        run_on_tokio(async move {
            let mut client = connect_op_store_client(server_url).await?;
            let response = client.get_op_heads(GetOpHeadsRequest { repo_id }).await?;
            let raw_heads = response.into_inner().op_head_ids;
            if raw_heads.is_empty() {
                return Err("GetOpHeadsResponse returned empty op_head_ids".into());
            }
            let mut head_ids = Vec::with_capacity(raw_heads.len());
            for b in raw_heads {
                if b.len() != cc_common::OPERATION_ID_LENGTH {
                    return Err(format!(
                        "invalid OperationId length in GetOpHeadsResponse: expected {} bytes, got {}",
                        cc_common::OPERATION_ID_LENGTH,
                        b.len()
                    )
                    .into());
                }
                head_ids.push(OperationId::from_bytes(&b));
            }
            Ok(head_ids)
        })
        .await
        .map_err(OpHeadsStoreError::Read)
    }

    pub async fn get_op_heads_with_reconciliation(
        &self,
    ) -> Result<Vec<OperationId>, OpHeadsStoreError> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();

        let reconciled = run_on_tokio(async move {
            let mut client = connect_op_store_client(server_url).await?;
            let inner = match client
                .reconcile_op_heads(ReconcileOpHeadsRequest {
                    repo_id: repo_id.clone(),
                })
                .await
            {
                Ok(resp) => resp.into_inner(),
                Err(status)
                    if matches!(
                        status.code(),
                        tonic::Code::Aborted | tonic::Code::Unimplemented
                    ) =>
                {
                    tracing::warn!(
                        repo_id = %repo_id,
                        code = ?status.code(),
                        error = %status.message(),
                        "ReconcileOpHeads unavailable or contended; falling back to GetOpHeads"
                    );
                    return Ok(None);
                }
                Err(status) => return Err(status.into()),
            };
            if inner.client_resolution_required {
                let raw_heads = if inner.current_op_head_ids.is_empty() {
                    client
                        .get_op_heads(GetOpHeadsRequest { repo_id })
                        .await?
                        .into_inner()
                        .op_head_ids
                } else {
                    inner.current_op_head_ids
                };
                if raw_heads.is_empty() {
                    return Err("ReconcileOpHeads returned empty current_op_head_ids".into());
                }
                let mut parsed = Vec::with_capacity(raw_heads.len());
                for b in raw_heads {
                    if b.len() != cc_common::OPERATION_ID_LENGTH {
                        return Err(format!(
                            "invalid OperationId length in ReconcileOpHeadsResponse: expected {} bytes, got {}",
                            cc_common::OPERATION_ID_LENGTH,
                            b.len()
                        )
                        .into());
                    }
                    parsed.push(OperationId::from_bytes(&b));
                }
                return Ok(Some(parsed));
            }
            if inner.op_head.len() != cc_common::OPERATION_ID_LENGTH {
                return Err(format!(
                    "invalid resolved op_head length in ReconcileOpHeadsResponse: expected {} bytes, got {}",
                    cc_common::OPERATION_ID_LENGTH,
                    inner.op_head.len()
                )
                .into());
            }
            Ok(Some(vec![OperationId::from_bytes(&inner.op_head)]))
        })
        .await
        .map_err(OpHeadsStoreError::Read)?;

        match reconciled {
            Some(heads) => Ok(heads),
            None => self.get_op_heads_without_reconciliation().await,
        }
    }
}

#[async_trait]
impl OpHeadsStore for CommitCloudOpHeadsStore {
    fn name(&self) -> &str {
        Self::name()
    }

    async fn update_op_heads(
        &self,
        old_ids: &[OperationId],
        new_id: &OperationId,
    ) -> Result<(), OpHeadsStoreError> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let old_op_head_ids = old_ids.iter().map(|id| id.as_bytes().to_vec()).collect();
        let new_op_head_id = new_id.as_bytes().to_vec();
        let target_new_id = new_id.clone();

        run_on_tokio(async move {
            let mut client = connect_op_store_client(server_url).await?;
            client
                .update_op_heads(UpdateOpHeadsRequest {
                    repo_id,
                    old_op_head_ids,
                    new_op_head_id,
                })
                .await?;
            Ok(())
        })
        .await
        .map_err(|e| OpHeadsStoreError::Write {
            new_op_id: target_new_id,
            source: e,
        })
    }

    async fn get_op_heads(&self) -> Result<Vec<OperationId>, OpHeadsStoreError> {
        self.get_op_heads_with_reconciliation().await
    }

    async fn lock(&self) -> Result<Box<dyn OpHeadsStoreLock + '_>, OpHeadsStoreError> {
        Ok(Box::new(CommitCloudOpHeadsStoreNoLock))
    }
}

#[cfg(test)]
mod tests {
    use cc_common::op_store::op_store_service_server::{OpStoreService, OpStoreServiceServer};
    use tonic::{Request, Response, Status};

    use super::*;
    use crate::util::run_async;

    struct ConfigurableOpHeadsServer {
        op_head_ids: Vec<Vec<u8>>,
        reconcile_status: tonic::Code,
    }

    #[tonic::async_trait]
    impl OpStoreService for ConfigurableOpHeadsServer {
        async fn read_operation(
            &self,
            _: Request<ReadOperationRequest>,
        ) -> Result<Response<ReadOperationResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }
        async fn write_operation(
            &self,
            _: Request<WriteOperationRequest>,
        ) -> Result<Response<WriteOperationResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }
        async fn read_view(
            &self,
            _: Request<ReadViewRequest>,
        ) -> Result<Response<ReadViewResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }
        async fn write_view(
            &self,
            _: Request<WriteViewRequest>,
        ) -> Result<Response<WriteViewResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }
        async fn get_op_heads(
            &self,
            _: Request<GetOpHeadsRequest>,
        ) -> Result<Response<GetOpHeadsResponse>, Status> {
            Ok(Response::new(GetOpHeadsResponse {
                op_head_ids: self.op_head_ids.clone(),
            }))
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
            Err(Status::new(self.reconcile_status, "reconcile status"))
        }
        async fn resolve_operation_prefix(
            &self,
            _: Request<ResolveOperationPrefixRequest>,
        ) -> Result<Response<ResolveOperationPrefixResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }
        async fn publish_operation(
            &self,
            _: Request<PublishOperationRequest>,
        ) -> Result<Response<PublishOperationResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }
    }

    async fn spawn_mock_op_heads_server(svc: ConfigurableOpHeadsServer) -> String {
        run_async(|| async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let addr = listener.local_addr()?;
            let incoming =
                tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
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

    #[tokio::test]
    async fn get_op_heads_without_reconciliation_rejects_invalid_id_length() {
        let url = spawn_mock_op_heads_server(ConfigurableOpHeadsServer {
            op_head_ids: vec![vec![0xaa; 5]],
            reconcile_status: tonic::Code::Unimplemented,
        })
        .await;

        let store = CommitCloudOpHeadsStore::new("repo-1".to_string(), url);
        let err = store
            .get_op_heads_without_reconciliation()
            .await
            .expect_err("malformed op_head_id length must fail cleanly");
        match err {
            OpHeadsStoreError::Read(source) => {
                assert!(
                    source.to_string().contains("invalid OperationId length"),
                    "unexpected error source: {source}"
                );
            }
            other => panic!("expected OpHeadsStoreError::Read, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn get_op_heads_with_reconciliation_falls_back_on_aborted_and_unimplemented() {
        let head_a = vec![0x11; cc_common::OPERATION_ID_LENGTH];
        let head_b = vec![0x22; cc_common::OPERATION_ID_LENGTH];
        let expected = vec![
            OperationId::from_bytes(&head_a),
            OperationId::from_bytes(&head_b),
        ];

        for code in [tonic::Code::Aborted, tonic::Code::Unimplemented] {
            let url = spawn_mock_op_heads_server(ConfigurableOpHeadsServer {
                op_head_ids: vec![head_a.clone(), head_b.clone()],
                reconcile_status: code,
            })
            .await;
            let store = CommitCloudOpHeadsStore::new("repo-1".to_string(), url);
            let heads = store
                .get_op_heads_with_reconciliation()
                .await
                .unwrap_or_else(|e| panic!("expected fallback on {code:?}, got error: {e}"));
            assert_eq!(heads, expected);
        }

        // Non-fallback errors such as PermissionDenied must still propagate.
        let denied_url = spawn_mock_op_heads_server(ConfigurableOpHeadsServer {
            op_head_ids: vec![head_a],
            reconcile_status: tonic::Code::PermissionDenied,
        })
        .await;
        let denied_store = CommitCloudOpHeadsStore::new("repo-1".to_string(), denied_url);
        assert!(
            denied_store
                .get_op_heads_with_reconciliation()
                .await
                .is_err(),
            "PermissionDenied from ReconcileOpHeads must not fall back silently"
        );
    }

    #[tokio::test]
    async fn get_op_heads_rejects_empty_response_directly_and_on_reconcile_fallback() {
        let direct_url = spawn_mock_op_heads_server(ConfigurableOpHeadsServer {
            op_head_ids: Vec::new(),
            reconcile_status: tonic::Code::Unimplemented,
        })
        .await;
        let direct_store = CommitCloudOpHeadsStore::new("repo-1".to_string(), direct_url);
        let direct_err = direct_store
            .get_op_heads_without_reconciliation()
            .await
            .expect_err("empty GetOpHeadsResponse must fail cleanly");
        match direct_err {
            OpHeadsStoreError::Read(source) => {
                assert!(
                    source.to_string().contains("empty op_head_ids"),
                    "unexpected error source: {source}"
                );
            }
            other => panic!("expected OpHeadsStoreError::Read, got {other:?}"),
        }

        for code in [tonic::Code::Aborted, tonic::Code::Unimplemented] {
            let fallback_url = spawn_mock_op_heads_server(ConfigurableOpHeadsServer {
                op_head_ids: Vec::new(),
                reconcile_status: code,
            })
            .await;
            let fallback_store = CommitCloudOpHeadsStore::new("repo-1".to_string(), fallback_url);
            let fallback_err = fallback_store
                .get_op_heads_with_reconciliation()
                .await
                .expect_err("empty GetOpHeadsResponse via fallback must fail cleanly");
            match fallback_err {
                OpHeadsStoreError::Read(source) => {
                    assert!(
                        source.to_string().contains("empty op_head_ids"),
                        "unexpected error source on {code:?} fallback: {source}"
                    );
                }
                other => {
                    panic!("expected OpHeadsStoreError::Read on {code:?} fallback, got {other:?}")
                }
            }
        }
    }
}
