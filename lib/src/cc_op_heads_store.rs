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

//! `jj_lib::op_heads_store::OpHeadsStore` client adapter over `OpStoreService` gRPC RPCs.
//!
//! # Concurrency and Merge Invariants
//! `lock()` is a client-side no-op; `UpdateOpHeads` executes atomically on the server,
//! removing superseded `old_op_head_ids` while retaining concurrent sibling operation
//! heads so `jj_lib` can reconcile divergent views without distributed locks.

use async_trait::async_trait;
use cc_common::op_store::op_store_service_client::OpStoreServiceClient;
use cc_common::op_store::*;
use jj_lib::object_id::ObjectId;
use jj_lib::op_heads_store::{OpHeadsStore, OpHeadsStoreError, OpHeadsStoreLock};
use jj_lib::op_store::OperationId;
use std::fmt::Debug;
use std::path::Path;

use crate::util::{CommitCloudConfig, run_async};

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

    pub fn load(store_path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config = CommitCloudConfig::load_from_store(store_path)?;
        Ok(Self {
            server_url: config.server_url,
            repo_id: config.repo_id,
        })
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

        run_async(move || async move {
            let mut client = OpStoreServiceClient::connect(server_url).await?;
            client
                .update_op_heads(UpdateOpHeadsRequest {
                    repo_id,
                    old_op_head_ids,
                    new_op_head_id,
                })
                .await?;
            Ok(())
        })
        .map_err(|e| OpHeadsStoreError::Write {
            new_op_id: target_new_id,
            source: e,
        })
    }

    async fn get_op_heads(&self) -> Result<Vec<OperationId>, OpHeadsStoreError> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();

        run_async(move || async move {
            let mut client = OpStoreServiceClient::connect(server_url).await?;
            let response = client.get_op_heads(GetOpHeadsRequest { repo_id }).await?;
            let raw_ids = response.into_inner().op_head_ids;
            if raw_ids.is_empty() {
                return Err("GetOpHeadsResponse returned empty op_head_ids".into());
            }
            let mut head_ids = Vec::with_capacity(raw_ids.len());
            for b in raw_ids {
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
        .map_err(|e| OpHeadsStoreError::Read(e))
    }

    async fn lock(&self) -> Result<Box<dyn OpHeadsStoreLock + '_>, OpHeadsStoreError> {
        Ok(Box::new(CommitCloudOpHeadsStoreNoLock))
    }
}
