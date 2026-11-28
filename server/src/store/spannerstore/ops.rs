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

//! Repository-scoped view, operation, and operation-head storage on Cloud Spanner.
//!
//! # Concurrency and Merge Invariants
//! Operation-head updates, atomic operation publication, and compare-and-swap
//! transitions execute in `Serializable` read-write transactions. Once an operation
//! is marked `Published`, retries do not resurrect it into `Repositories.OpHeads`.

use cc_common::op_store::{Operation, View};
use google_cloud_spanner::mutation::Mutation;
use google_cloud_spanner::statement::Statement;
use prost::Message;

use super::codec::{drain_first_row, is_already_exists, map_spanner_err};
use super::{SpannerMetadataStore, TxAction};
use crate::store::{OpId, StoreError, StoreResult, apply_op_heads_mutation, op_heads_sets_equal};

impl SpannerMetadataStore {
    pub(super) async fn get_operation_impl(
        &self,
        repo_id: &str,
        op_id: &[u8],
    ) -> StoreResult<Option<Operation>> {
        let stmt = Statement::builder(
            "SELECT ProtoBytes FROM Operations WHERE RepoId = @repo_id AND OperationId = @op_id",
        )
        .add_param("repo_id", repo_id)
        .add_param("op_id", op_id)
        .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx.execute_query(stmt).await.map_err(map_spanner_err)?;
        let Some(row) = rs.next().await.transpose().map_err(map_spanner_err)? else {
            return Ok(None);
        };
        let proto_bytes: Vec<u8> = row.get("ProtoBytes").map_err(map_spanner_err)?;
        let op = Operation::decode(proto_bytes.as_slice()).map_err(|e| {
            StoreError::Corruption(format!("failed to decode Spanner Operation: {e}"))
        })?;
        Ok(Some(op))
    }

    pub(super) async fn put_operation_impl(
        &self,
        repo_id: &str,
        op_id: &[u8],
        op: &Operation,
    ) -> StoreResult<()> {
        let proto_bytes = op.encode_to_vec();
        // Insert-only so re-writing an already-published operation never resets `Published`.
        let mutation = Mutation::new_insert_builder("Operations")
            .set("RepoId")
            .to(repo_id)
            .set("OperationId")
            .to(op_id)
            .set("ProtoBytes")
            .to(&proto_bytes)
            .set("Published")
            .to(false)
            .build();
        match self
            .db_client
            .write_only_transaction()
            .build()
            .write(vec![mutation])
            .await
        {
            Ok(_) => Ok(()),
            Err(err) if is_already_exists(&err) => Ok(()),
            Err(err) => Err(map_spanner_err(err)),
        }
    }

    pub(super) async fn resolve_operation_id_prefix_impl(
        &self,
        repo_id: &str,
        min_id: &[u8],
        max_id: &[u8],
    ) -> StoreResult<Vec<OpId>> {
        let stmt = Statement::builder(
            "SELECT OperationId FROM Operations
             WHERE RepoId = @repo_id AND OperationId >= @min_id AND OperationId <= @max_id
             ORDER BY OperationId ASC
             LIMIT 2",
        )
        .add_param("repo_id", repo_id)
        .add_param("min_id", min_id)
        .add_param("max_id", max_id)
        .build();

        let tx = self.db_client.single_use().build();
        let mut rs = tx.execute_query(stmt).await.map_err(map_spanner_err)?;
        let mut matches = Vec::new();
        while let Some(row) = rs.next().await.transpose().map_err(map_spanner_err)? {
            let op_id: Vec<u8> = row.get("OperationId").map_err(map_spanner_err)?;
            matches.push(op_id);
        }
        Ok(matches)
    }

    pub(super) async fn get_view_impl(
        &self,
        repo_id: &str,
        view_id: &[u8],
    ) -> StoreResult<Option<View>> {
        let stmt = Statement::builder(
            "SELECT ProtoBytes FROM Views WHERE RepoId = @repo_id AND ViewId = @view_id",
        )
        .add_param("repo_id", repo_id)
        .add_param("view_id", view_id)
        .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx.execute_query(stmt).await.map_err(map_spanner_err)?;
        let Some(row) = rs.next().await.transpose().map_err(map_spanner_err)? else {
            return Ok(None);
        };
        let proto_bytes: Vec<u8> = row.get("ProtoBytes").map_err(map_spanner_err)?;
        let view = View::decode(proto_bytes.as_slice())
            .map_err(|e| StoreError::Corruption(format!("failed to decode Spanner View: {e}")))?;
        Ok(Some(view))
    }

    pub(super) async fn has_view_impl(&self, repo_id: &str, view_id: &[u8]) -> StoreResult<bool> {
        let stmt = Statement::builder(
            "SELECT 1 FROM Views WHERE RepoId = @repo_id AND ViewId = @view_id LIMIT 1",
        )
        .add_param("repo_id", repo_id)
        .add_param("view_id", view_id)
        .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx.execute_query(stmt).await.map_err(map_spanner_err)?;
        Ok(rs
            .next()
            .await
            .transpose()
            .map_err(map_spanner_err)?
            .is_some())
    }

    pub(super) async fn put_view_impl(
        &self,
        repo_id: &str,
        view_id: &[u8],
        view: &View,
    ) -> StoreResult<()> {
        let proto_bytes = view.encode_to_vec();
        let mutation = Mutation::new_insert_or_update_builder("Views")
            .set("RepoId")
            .to(repo_id)
            .set("ViewId")
            .to(view_id)
            .set("ProtoBytes")
            .to(&proto_bytes)
            .build();
        self.db_client
            .write_only_transaction()
            .build()
            .write(vec![mutation])
            .await
            .map_err(map_spanner_err)?;
        Ok(())
    }

    pub(super) async fn get_op_heads_impl(&self, repo_id: &str) -> StoreResult<Option<Vec<OpId>>> {
        let stmt = Statement::builder("SELECT OpHeads FROM Repositories WHERE RepoId = @repo_id")
            .add_param("repo_id", repo_id)
            .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx.execute_query(stmt).await.map_err(map_spanner_err)?;
        let Some(row) = rs.next().await.transpose().map_err(map_spanner_err)? else {
            return Ok(None);
        };
        let heads: Vec<Vec<u8>> = row.get("OpHeads").map_err(map_spanner_err)?;
        Ok(Some(heads))
    }

    pub(super) async fn update_op_heads_impl(
        &self,
        repo_id: &str,
        old_ids: &[Vec<u8>],
        new_id: &[u8],
    ) -> StoreResult<Vec<OpId>> {
        let repo_id = repo_id.to_string();
        let old_ids = old_ids.to_vec();
        let new_id = new_id.to_vec();

        self.run_rw_tx(async move |tx| {
            let stmt =
                Statement::builder("SELECT OpHeads FROM Repositories WHERE RepoId = @repo_id")
                    .add_param("repo_id", &repo_id)
                    .build();
            let rs = tx.execute_query(stmt).await?;
            let Some(row) = drain_first_row(rs).await? else {
                return Ok(TxAction::Rollback(Err(StoreError::RepoNotFound {
                    repo_id: repo_id.clone(),
                    action: "updating op heads".to_string(),
                })));
            };

            let mut current_heads: Vec<Vec<u8>> = row.get("OpHeads")?;

            let op_stmt = Statement::builder(
                "SELECT Published FROM Operations
                 WHERE RepoId = @repo_id AND OperationId = @op_id",
            )
            .add_param("repo_id", &repo_id)
            .add_param("op_id", &new_id)
            .build();
            let op_rs = tx.execute_query(op_stmt).await?;
            let op_row = drain_first_row(op_rs).await?;
            let already_published = match &op_row {
                Some(op_row) => {
                    let published: Option<bool> = op_row.get("Published")?;
                    published == Some(true)
                }
                None => false,
            };
            if already_published
                && !current_heads
                    .iter()
                    .any(|h| h.as_slice() == new_id.as_slice())
            {
                return Ok(TxAction::Rollback(Ok(current_heads)));
            }

            apply_op_heads_mutation(&mut current_heads, &old_ids, &new_id);

            let mut mutations = Vec::with_capacity(2);
            mutations.push(
                Mutation::new_update_builder("Repositories")
                    .set("RepoId")
                    .to(&repo_id)
                    .set("OpHeads")
                    .to(&current_heads)
                    .build(),
            );
            if op_row.is_some() && !already_published {
                mutations.push(
                    Mutation::new_update_builder("Operations")
                        .set("RepoId")
                        .to(&repo_id)
                        .set("OperationId")
                        .to(&new_id)
                        .set("Published")
                        .to(true)
                        .build(),
                );
            }
            tx.buffer(mutations)?;
            Ok(TxAction::Commit(current_heads))
        })
        .await
        .map_err(map_spanner_err)?
    }

    pub(super) async fn publish_operation_atomic_impl(
        &self,
        repo_id: &str,
        view_id: &[u8],
        view: &View,
        op_id: &[u8],
        op: &Operation,
        old_ids: &[Vec<u8>],
    ) -> StoreResult<Vec<OpId>> {
        let repo_id = repo_id.to_string();
        let view_id = view_id.to_vec();
        let view_bytes = view.encode_to_vec();
        let op_id = op_id.to_vec();
        let op_bytes = op.encode_to_vec();
        let old_ids = old_ids.to_vec();

        self.run_rw_tx(async move |tx| {
            let stmt =
                Statement::builder("SELECT OpHeads FROM Repositories WHERE RepoId = @repo_id")
                    .add_param("repo_id", &repo_id)
                    .build();
            let rs = tx.execute_query(stmt).await?;
            let Some(row) = drain_first_row(rs).await? else {
                return Ok(TxAction::Rollback(Err(StoreError::RepoNotFound {
                    repo_id: repo_id.clone(),
                    action: "publishing operation".to_string(),
                })));
            };

            let op_check_stmt = Statement::builder(
                "SELECT Published FROM Operations
                 WHERE RepoId = @repo_id AND OperationId = @op_id",
            )
            .add_param("repo_id", &repo_id)
            .add_param("op_id", &op_id)
            .build();
            let op_rs = tx.execute_query(op_check_stmt).await?;
            let already_published = if let Some(op_row) = drain_first_row(op_rs).await? {
                let published: Option<bool> = op_row.get("Published")?;
                published.unwrap_or(false)
            } else {
                false
            };

            let mut current_heads: Vec<Vec<u8>> = row.get("OpHeads")?;
            if already_published {
                return Ok(TxAction::Rollback(Ok(current_heads)));
            }

            apply_op_heads_mutation(&mut current_heads, &old_ids, &op_id);

            let view_mut = Mutation::new_insert_or_update_builder("Views")
                .set("RepoId")
                .to(&repo_id)
                .set("ViewId")
                .to(&view_id)
                .set("ProtoBytes")
                .to(&view_bytes)
                .build();
            let op_mut = Mutation::new_insert_or_update_builder("Operations")
                .set("RepoId")
                .to(&repo_id)
                .set("OperationId")
                .to(&op_id)
                .set("ProtoBytes")
                .to(&op_bytes)
                .set("Published")
                .to(true)
                .build();
            let heads_mut = Mutation::new_update_builder("Repositories")
                .set("RepoId")
                .to(&repo_id)
                .set("OpHeads")
                .to(&current_heads)
                .build();
            tx.buffer([view_mut, op_mut, heads_mut])?;
            Ok(TxAction::Commit(current_heads))
        })
        .await
        .map_err(map_spanner_err)?
    }

    pub(super) async fn compare_and_swap_op_heads_impl(
        &self,
        repo_id: &str,
        expected_old_ids: &[Vec<u8>],
        new_id: &[u8],
    ) -> StoreResult<()> {
        let repo_id = repo_id.to_string();
        let expected_old_ids = expected_old_ids.to_vec();
        let new_id = new_id.to_vec();

        self.run_rw_tx(async move |tx| {
            let stmt =
                Statement::builder("SELECT OpHeads FROM Repositories WHERE RepoId = @repo_id")
                    .add_param("repo_id", &repo_id)
                    .build();
            let rs = tx.execute_query(stmt).await?;
            let Some(row) = drain_first_row(rs).await? else {
                return Ok(TxAction::Rollback(Err(StoreError::RepoNotFound {
                    repo_id: repo_id.clone(),
                    action: "comparing and swapping op heads".to_string(),
                })));
            };

            let current_heads: Vec<Vec<u8>> = row.get("OpHeads")?;
            if !op_heads_sets_equal(&current_heads, &expected_old_ids) {
                return Ok(TxAction::Rollback(Err(StoreError::CasConflict(format!(
                    "op heads changed concurrently in repo '{repo_id}'"
                )))));
            }

            let updated = vec![new_id.clone()];
            let mut mutations = Vec::with_capacity(2);
            mutations.push(
                Mutation::new_update_builder("Repositories")
                    .set("RepoId")
                    .to(&repo_id)
                    .set("OpHeads")
                    .to(&updated)
                    .build(),
            );
            let op_stmt = Statement::builder(
                "SELECT Published FROM Operations
                 WHERE RepoId = @repo_id AND OperationId = @op_id",
            )
            .add_param("repo_id", &repo_id)
            .add_param("op_id", &new_id)
            .build();
            let op_rs = tx.execute_query(op_stmt).await?;
            if let Some(op_row) = drain_first_row(op_rs).await? {
                let published: Option<bool> = op_row.get("Published")?;
                if published != Some(true) {
                    mutations.push(
                        Mutation::new_update_builder("Operations")
                            .set("RepoId")
                            .to(&repo_id)
                            .set("OperationId")
                            .to(&new_id)
                            .set("Published")
                            .to(true)
                            .build(),
                    );
                }
            }
            tx.buffer(mutations)?;
            Ok(TxAction::Commit(()))
        })
        .await
        .map_err(map_spanner_err)?
    }
}
