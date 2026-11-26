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

use async_trait::async_trait;
use cc_common::op_store::*;
use jj_lib::object_id::ObjectId;
use jj_lib::op_heads_store::{OpHeadsStore, OpHeadsStoreError, OpHeadsStoreLock};
use jj_lib::op_store::OperationId;
use std::fmt::Debug;
use std::path::Path;

use crate::client::connect_op_store_client;
use crate::util::{CommitCloudConfig, run_async};

#[derive(Debug)]
pub struct CommitCloudOpHeadsStore {
    config: CommitCloudConfig,
}

struct CommitCloudOpHeadsStoreNoLock;
impl OpHeadsStoreLock for CommitCloudOpHeadsStoreNoLock {}

impl CommitCloudOpHeadsStore {
    pub fn name() -> &'static str {
        "commit_cloud"
    }

    pub fn new(server_url: String, repo_id: String) -> Self {
        Self {
            config: CommitCloudConfig {
                server_url,
                project_id: String::new(),
                repo_id,
                use_daemon: true,
                daemon_socket: None,
            },
        }
    }

    pub fn load(store_path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config = CommitCloudConfig::load_from_store(store_path)?;
        Ok(Self { config })
    }

    // Currently unused, the function is implemented here to make the distinction between the 2 functions (this and get_op_heads_with_reconciliation), for future use
    pub async fn get_op_heads_without_reconciliation(
        &self,
    ) -> Result<Vec<OperationId>, OpHeadsStoreError> {
        let config = self.config.clone();
        let repo_id = self.config.repo_id.clone();

        run_async(move || async move {
            let mut client = connect_op_store_client(&config).await?;
            let response = client.get_op_heads(GetOpHeadsRequest { repo_id }).await?;
            let raw_heads = response.into_inner().op_head_ids;
            let mut head_ids = Vec::with_capacity(raw_heads.len());
            for b in raw_heads {
                if b.len() != cc_common::OPERATION_ID_LENGTH {
                    return Err(format!(
                        "GetOpHeads returned invalid op_head_id length {}",
                        b.len()
                    )
                    .into());
                }
                head_ids.push(OperationId::from_bytes(&b));
            }
            Ok(head_ids)
        })
        .map_err(|e| OpHeadsStoreError::Read(e.into()))
    }

    pub async fn get_op_heads_with_reconciliation(
        &self,
    ) -> Result<Vec<OperationId>, OpHeadsStoreError> {
        let config = self.config.clone();
        let repo_id = self.config.repo_id.clone();

        run_async(move || async move {
            let mut client = connect_op_store_client(&config).await?;
            let resp = client
                .reconcile_op_heads(ReconcileOpHeadsRequest { repo_id })
                .await?
                .into_inner();
            if resp.client_resolution_required {
                if resp.current_op_head_ids.is_empty() {
                    return Err(
                        "ReconcileOpHeads set client_resolution_required with empty current_op_head_ids"
                            .into(),
                    );
                }
                let mut heads = Vec::with_capacity(resp.current_op_head_ids.len());
                for b in resp.current_op_head_ids {
                    if b.len() != cc_common::OPERATION_ID_LENGTH {
                        return Err(format!(
                            "ReconcileOpHeads returned invalid op_head_id length {}",
                            b.len()
                        )
                        .into());
                    }
                    heads.push(OperationId::from_bytes(&b));
                }
                Ok(heads)
            } else {
                if resp.op_head.len() != cc_common::OPERATION_ID_LENGTH {
                    return Err(format!(
                        "ReconcileOpHeads returned invalid op_head length {}",
                        resp.op_head.len()
                    )
                    .into());
                }
                Ok(vec![OperationId::from_bytes(&resp.op_head)])
            }
        })
        .map_err(|e| OpHeadsStoreError::Read(e.into()))
    }
}

#[async_trait]
impl OpHeadsStore for CommitCloudOpHeadsStore {
    fn name(&self) -> &str {
        Self::name()
    }

    // TODO: Add support for concurrency control with locking to prevent race conditions
    // during concurrent client operations from overwriting operation heads.
    async fn update_op_heads(
        &self,
        old_ids: &[OperationId],
        new_id: &OperationId,
    ) -> Result<(), OpHeadsStoreError> {
        let config = self.config.clone();
        let repo_id = self.config.repo_id.clone();
        let old_op_head_ids = old_ids.iter().map(|id| id.as_bytes().to_vec()).collect();
        let new_op_head_id = new_id.as_bytes().to_vec();
        let target_new_id = new_id.clone();

        run_async(move || async move {
            let mut client = connect_op_store_client(&config).await?;
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
            source: e.into(),
        })
    }

    async fn get_op_heads(&self) -> Result<Vec<OperationId>, OpHeadsStoreError> {
        self.get_op_heads_with_reconciliation().await
    }

    async fn lock(&self) -> Result<Box<dyn OpHeadsStoreLock + '_>, OpHeadsStoreError> {
        Ok(Box::new(CommitCloudOpHeadsStoreNoLock))
    }
}
