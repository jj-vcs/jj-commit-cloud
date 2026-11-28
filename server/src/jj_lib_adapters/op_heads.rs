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

//! Server-side `jj_lib::op_heads_store::OpHeadsStore` adapter backed by [`CompositeStore`].
//!
//! # Concurrency and Merge Invariants
//! Pinned instances constructed with `with_expected_heads` atomically swap the validated
//! operation-head snapshot via compare-and-swap; unpinned instances delegate to additive
//! `update_op_heads`.

use std::fmt::{self, Debug};
use std::sync::Arc;

use async_trait::async_trait;
use jj_lib::object_id::ObjectId;
use jj_lib::op_heads_store::{OpHeadsStore, OpHeadsStoreError, OpHeadsStoreLock};
use jj_lib::op_store::OperationId;

use crate::store::{CompositeStore, StoreError};

pub struct ServerOpHeadsStore {
    store: Arc<CompositeStore>,
    repo_id: String,
    pinned_project_id: Option<String>,
    expected_heads: Option<Vec<Vec<u8>>>,
}

impl Debug for ServerOpHeadsStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerOpHeadsStore")
            .field("repo_id", &self.repo_id)
            .field("pinned_project_id", &self.pinned_project_id)
            .field("pinned_heads", &self.expected_heads.as_ref().map(Vec::len))
            .finish()
    }
}

pub struct ServerOpHeadsStoreNoLock;
impl OpHeadsStoreLock for ServerOpHeadsStoreNoLock {}

impl ServerOpHeadsStore {
    pub fn new(store: Arc<CompositeStore>, repo_id: String) -> Self {
        Self {
            store,
            repo_id,
            pinned_project_id: None,
            expected_heads: None,
        }
    }

    pub fn with_project_id(
        store: Arc<CompositeStore>,
        repo_id: String,
        project_id: String,
    ) -> Self {
        Self {
            store,
            repo_id,
            pinned_project_id: Some(project_id),
            expected_heads: None,
        }
    }

    /// Creates an op-heads store pinned to `expected_heads`: `get_op_heads()` returns only that
    /// validated snapshot and `update_op_heads()` atomically swaps the full expected head set via
    /// [`CompositeStore::compare_and_swap_op_heads`].
    pub fn with_expected_heads(
        store: Arc<CompositeStore>,
        repo_id: String,
        expected_heads: Vec<Vec<u8>>,
    ) -> Self {
        Self {
            store,
            repo_id,
            pinned_project_id: None,
            expected_heads: Some(expected_heads),
        }
    }

    /// Creates an op-heads store pinned to both `project_id` and `expected_heads`:
    /// `update_op_heads()` atomically swaps the full expected head set via
    /// [`CompositeStore::compare_and_swap_op_heads_for_project`].
    pub fn with_expected_heads_for_project(
        store: Arc<CompositeStore>,
        repo_id: String,
        project_id: String,
        expected_heads: Vec<Vec<u8>>,
    ) -> Self {
        Self {
            store,
            repo_id,
            pinned_project_id: Some(project_id),
            expected_heads: Some(expected_heads),
        }
    }
}

#[async_trait]
impl OpHeadsStore for ServerOpHeadsStore {
    fn name(&self) -> &str {
        "server_op_heads_store"
    }

    async fn update_op_heads(
        &self,
        old_ids: &[OperationId],
        new_id: &OperationId,
    ) -> Result<(), OpHeadsStoreError> {
        let target_id = new_id.clone();

        let res = match (&self.expected_heads, &self.pinned_project_id) {
            (Some(expected), Some(project_id)) => {
                self.store
                    .compare_and_swap_op_heads_for_project(
                        &self.repo_id,
                        project_id,
                        expected,
                        new_id.as_bytes(),
                    )
                    .await
            }
            (Some(expected), None) => {
                self.store
                    .compare_and_swap_op_heads(&self.repo_id, expected, new_id.as_bytes())
                    .await
            }
            (None, Some(project_id)) => {
                let old_vec: Vec<Vec<u8>> =
                    old_ids.iter().map(|id| id.as_bytes().to_vec()).collect();
                self.store
                    .update_op_heads_for_project(
                        &self.repo_id,
                        project_id,
                        &old_vec,
                        new_id.as_bytes(),
                    )
                    .await
                    .map(|_| ())
            }
            (None, None) => {
                let old_vec: Vec<Vec<u8>> =
                    old_ids.iter().map(|id| id.as_bytes().to_vec()).collect();
                self.store
                    .update_op_heads(&self.repo_id, &old_vec, new_id.as_bytes())
                    .await
                    .map(|_| ())
            }
        };
        res.map_err(|e| OpHeadsStoreError::Write {
            new_op_id: target_id,
            source: e.into(),
        })
    }

    async fn get_op_heads(&self) -> Result<Vec<OperationId>, OpHeadsStoreError> {
        let heads = match (&self.expected_heads, &self.pinned_project_id) {
            (Some(expected), _) => expected.clone(),
            (None, Some(project_id)) => self
                .store
                .get_op_heads_for_project(&self.repo_id, project_id)
                .await
                .map_err(|e| OpHeadsStoreError::Read(e.into()))?,
            (None, None) => self
                .store
                .get_op_heads(&self.repo_id)
                .await
                .map_err(|e| OpHeadsStoreError::Read(e.into()))?,
        };
        let mut out = Vec::with_capacity(heads.len());
        for raw in heads {
            if raw.len() != cc_common::OPERATION_ID_LENGTH {
                return Err(OpHeadsStoreError::Read(
                    StoreError::Corruption(format!(
                        "stored op_head in repo '{}' must be {} bytes (got {} bytes)",
                        self.repo_id,
                        cc_common::OPERATION_ID_LENGTH,
                        raw.len()
                    ))
                    .into(),
                ));
            }
            out.push(OperationId::from_bytes(&raw));
        }
        Ok(out)
    }

    async fn lock(&self) -> Result<Box<dyn OpHeadsStoreLock + '_>, OpHeadsStoreError> {
        Ok(Box::new(ServerOpHeadsStoreNoLock))
    }
}
