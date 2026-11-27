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

//! PostgreSQL views, operations, and atomic operation-head transitions.
//!
//! # Concurrency and Merge Invariants
//! Locks the repository row (`FOR KEY SHARE` on view/operation writes and `FOR NO KEY UPDATE`
//! on head updates) to enforce project bindings and preserve concurrent sibling heads lock-free.
//! Idempotent `operations.published` tracking prevents duplicate retries from re-inserting
//! superseded heads.

use std::collections::HashSet;

use cc_common::op_store::{Operation, View};
use prost::Message as _;
use tokio_postgres::Transaction;
use tokio_postgres::error::SqlState;

use super::codec::map_pg_err;
use super::{PostgresMetadataStore, UNNEST_BATCH_SIZE};
use crate::store::{OpId, StoreError, StoreResult, apply_op_heads_mutation, op_heads_sets_equal};

async fn load_op_heads_in_tx(
    tx: &Transaction<'_>,
    repo_id: &str,
) -> Result<Vec<OpId>, tokio_postgres::Error> {
    let rows = tx
        .query(
            "SELECT op_id FROM op_heads WHERE repo_id = $1 ORDER BY added_seq ASC, op_id ASC",
            &[&repo_id],
        )
        .await?;
    Ok(rows.into_iter().map(|r| r.get::<_, Vec<u8>>(0)).collect())
}

async fn apply_op_heads_tables_in_tx(
    tx: &Transaction<'_>,
    repo_id: &str,
    old_ids: &[Vec<u8>],
    new_id: &[u8],
) -> Result<(), tokio_postgres::Error> {
    if !old_ids.is_empty() {
        tx.execute(
            "DELETE FROM op_heads WHERE repo_id = $1 AND op_id = ANY($2)",
            &[&repo_id, &old_ids],
        )
        .await?;
    }
    tx.execute(
        "INSERT INTO op_heads (repo_id, op_id) VALUES ($1, $2)
         ON CONFLICT (repo_id, op_id) DO NOTHING",
        &[&repo_id, &new_id],
    )
    .await?;
    Ok(())
}

impl PostgresMetadataStore {
    pub(super) async fn put_operation_inner(
        &self,
        repo_id: &str,
        expected_project_id: Option<&str>,
        op_id: &[u8],
        op: &Operation,
        require_dependencies_exist: bool,
    ) -> StoreResult<()> {
        let proto_bytes = op.encode_to_vec();
        let Some(expected_project_id) = expected_project_id else {
            let client = self.get_client().await?;
            // Insert-only so re-writing an already-published operation never resets `published`.
            if let Err(err) = client
                .execute(
                    "INSERT INTO operations (repo_id, op_id, published, proto_bytes)
                     VALUES ($1, $2, FALSE, $3)
                     ON CONFLICT (repo_id, op_id) DO NOTHING",
                    &[&repo_id, &op_id, &proto_bytes],
                )
                .await
            {
                if err.code() == Some(&SqlState::FOREIGN_KEY_VIOLATION) {
                    return Err(StoreError::RepoNotFound {
                        repo_id: repo_id.to_string(),
                        action: "writing operation".to_string(),
                    });
                }
                return Err(map_pg_err(err));
            }
            return Ok(());
        };

        let repo_id = repo_id.to_string();
        let expected_project_id = expected_project_id.to_string();
        let op_id = op_id.to_vec();
        let required_view_id = (require_dependencies_exist
            && !op.view_id.is_empty()
            && op.view_id.as_slice() != cc_common::ROOT_VIEW_ID_BYTES)
            .then(|| op.view_id.clone());
        let required_parents: Vec<Vec<u8>> = if require_dependencies_exist {
            let mut seen = HashSet::new();
            op.parents
                .iter()
                .filter(|p| p.as_slice() != cc_common::ROOT_OPERATION_ID_BYTES)
                .filter(|p| seen.insert(p.as_slice()))
                .cloned()
                .collect()
        } else {
            Vec::new()
        };
        self.run_tx_with_retry(|tx| {
            let repo_id = repo_id.clone();
            let expected_project_id = expected_project_id.clone();
            let op_id = op_id.clone();
            let proto_bytes = proto_bytes.clone();
            let required_view_id = required_view_id.clone();
            let required_parents = required_parents.clone();
            Box::pin(async move {
                let row = tx
                    .query_opt(
                        "SELECT project_id FROM repositories WHERE repo_id = $1 FOR KEY SHARE",
                        &[&repo_id],
                    )
                    .await?;
                let actual: Option<String> = row.map(|r| r.get(0));
                if let Err(e) = crate::store::verify_optional_repo_project(
                    &repo_id,
                    actual.as_deref(),
                    Some(expected_project_id.as_str()),
                    "writing operations",
                ) {
                    return Ok(Err(e));
                }
                if let Some(req_view_id) = &required_view_id {
                    let exists = tx
                        .query_opt(
                            "SELECT 1 FROM views WHERE repo_id = $1 AND view_id = $2",
                            &[&repo_id, req_view_id],
                        )
                        .await?;
                    if exists.is_none() {
                        return Ok(Err(StoreError::MissingDependency {
                            parent_kind: "operation",
                            missing_kind: "view",
                            id_hex: hex::encode(req_view_id),
                        }));
                    }
                }
                if !required_parents.is_empty() {
                    let mut existing_parents: HashSet<Vec<u8>> =
                        HashSet::with_capacity(required_parents.len());
                    for chunk in required_parents.chunks(UNNEST_BATCH_SIZE) {
                        let rows = tx
                            .query(
                                "SELECT op_id FROM operations
                                 WHERE repo_id = $1 AND op_id IN (SELECT UNNEST($2::BYTEA[]))",
                                &[&repo_id, &chunk],
                            )
                            .await?;
                        for row in rows {
                            existing_parents.insert(row.get::<_, Vec<u8>>(0));
                        }
                    }
                    for parent_id in &required_parents {
                        if !existing_parents.contains(parent_id) {
                            return Ok(Err(StoreError::MissingDependency {
                                parent_kind: "operation",
                                missing_kind: "parent operation",
                                id_hex: hex::encode(parent_id),
                            }));
                        }
                    }
                }
                tx.execute(
                    "INSERT INTO operations (repo_id, op_id, published, proto_bytes)
                     VALUES ($1, $2, FALSE, $3)
                     ON CONFLICT (repo_id, op_id) DO NOTHING",
                    &[&repo_id, &op_id, &proto_bytes],
                )
                .await?;
                Ok(Ok(()))
            })
        })
        .await
    }

    pub(super) async fn put_view_inner(
        &self,
        repo_id: &str,
        expected_project_id: Option<&str>,
        view_id: &[u8],
        view: &View,
    ) -> StoreResult<()> {
        let proto_bytes = view.encode_to_vec();
        let Some(expected_project_id) = expected_project_id else {
            let client = self.get_client().await?;
            if let Err(err) = client
                .execute(
                    "INSERT INTO views (repo_id, view_id, proto_bytes)
                     VALUES ($1, $2, $3)
                     ON CONFLICT (repo_id, view_id) DO NOTHING",
                    &[&repo_id, &view_id, &proto_bytes],
                )
                .await
            {
                if err.code() == Some(&SqlState::FOREIGN_KEY_VIOLATION) {
                    return Err(StoreError::RepoNotFound {
                        repo_id: repo_id.to_string(),
                        action: "writing view".to_string(),
                    });
                }
                return Err(map_pg_err(err));
            }
            return Ok(());
        };

        let repo_id = repo_id.to_string();
        let expected_project_id = expected_project_id.to_string();
        let view_id = view_id.to_vec();
        self.run_tx_with_retry(|tx| {
            let repo_id = repo_id.clone();
            let expected_project_id = expected_project_id.clone();
            let view_id = view_id.clone();
            let proto_bytes = proto_bytes.clone();
            Box::pin(async move {
                let row = tx
                    .query_opt(
                        "SELECT project_id FROM repositories WHERE repo_id = $1 FOR KEY SHARE",
                        &[&repo_id],
                    )
                    .await?;
                let actual: Option<String> = row.map(|r| r.get(0));
                if let Err(e) = crate::store::verify_optional_repo_project(
                    &repo_id,
                    actual.as_deref(),
                    Some(expected_project_id.as_str()),
                    "writing views",
                ) {
                    return Ok(Err(e));
                }
                tx.execute(
                    "INSERT INTO views (repo_id, view_id, proto_bytes)
                     VALUES ($1, $2, $3)
                     ON CONFLICT (repo_id, view_id) DO NOTHING",
                    &[&repo_id, &view_id, &proto_bytes],
                )
                .await?;
                Ok(Ok(()))
            })
        })
        .await
    }

    pub(super) async fn update_op_heads_inner(
        &self,
        repo_id: &str,
        expected_project_id: Option<&str>,
        old_ids: &[Vec<u8>],
        new_id: &[u8],
        require_op_exists: bool,
    ) -> StoreResult<Vec<OpId>> {
        let repo_id = repo_id.to_string();
        let expected_project_id = expected_project_id.map(str::to_string);
        let old_ids = old_ids.to_vec();
        let new_id = new_id.to_vec();

        self.run_tx_with_retry(|tx| {
            let repo_id = repo_id.clone();
            let expected_project_id = expected_project_id.clone();
            let old_ids = old_ids.clone();
            let new_id = new_id.clone();
            Box::pin(async move {
                let locked = tx
                    .query_opt(
                        "SELECT project_id FROM repositories WHERE repo_id = $1 FOR NO KEY UPDATE",
                        &[&repo_id],
                    )
                    .await?;
                let actual: Option<String> = locked.map(|r| r.get(0));
                if let Err(e) = crate::store::verify_optional_repo_project(
                    &repo_id,
                    actual.as_deref(),
                    expected_project_id.as_deref(),
                    "updating op heads",
                ) {
                    return Ok(Err(e));
                }

                let mut current_heads = load_op_heads_in_tx(tx, &repo_id).await?;
                let published_row = tx
                    .query_opt(
                        "SELECT published FROM operations WHERE repo_id = $1 AND op_id = $2",
                        &[&repo_id, &new_id],
                    )
                    .await?;
                if require_op_exists && published_row.is_none() {
                    return Ok(Err(StoreError::MissingDependency {
                        parent_kind: "op_heads",
                        missing_kind: "operation",
                        id_hex: hex::encode(&new_id),
                    }));
                }
                let already_published = published_row.as_ref().is_some_and(|r| r.get::<_, bool>(0));
                if already_published
                    && !current_heads
                        .iter()
                        .any(|h| h.as_slice() == new_id.as_slice())
                {
                    return Ok(Ok(current_heads));
                }

                apply_op_heads_mutation(&mut current_heads, &old_ids, &new_id);
                apply_op_heads_tables_in_tx(tx, &repo_id, &old_ids, &new_id).await?;

                if published_row.is_some() && !already_published {
                    tx.execute(
                        "UPDATE operations SET published = TRUE WHERE repo_id = $1 AND op_id = $2",
                        &[&repo_id, &new_id],
                    )
                    .await?;
                }

                Ok(Ok(current_heads))
            })
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn publish_operation_atomic_inner(
        &self,
        repo_id: &str,
        expected_project_id: Option<&str>,
        view_id: &[u8],
        view: &View,
        op_id: &[u8],
        op: &Operation,
        old_ids: &[Vec<u8>],
        require_dependencies_exist: bool,
    ) -> StoreResult<Vec<OpId>> {
        let repo_id = repo_id.to_string();
        let expected_project_id = expected_project_id.map(str::to_string);
        let view_id = view_id.to_vec();
        let view_bytes = view.encode_to_vec();
        let op_id = op_id.to_vec();
        let op_bytes = op.encode_to_vec();
        let old_ids = old_ids.to_vec();
        let external_view_id = (require_dependencies_exist
            && op.view_id.as_slice() != view_id.as_slice()
            && op.view_id.as_slice() != cc_common::ROOT_VIEW_ID_BYTES)
            .then(|| op.view_id.clone());
        let required_parents = if require_dependencies_exist {
            crate::store::collect_non_root_op_parents(op)
        } else {
            Vec::new()
        };

        self.run_tx_with_retry(|tx| {
            let repo_id = repo_id.clone();
            let expected_project_id = expected_project_id.clone();
            let view_id = view_id.clone();
            let view_bytes = view_bytes.clone();
            let op_id = op_id.clone();
            let op_bytes = op_bytes.clone();
            let old_ids = old_ids.clone();
            let external_view_id = external_view_id.clone();
            let required_parents = required_parents.clone();
            Box::pin(async move {
                let locked = tx
                    .query_opt(
                        "SELECT project_id FROM repositories WHERE repo_id = $1 FOR NO KEY UPDATE",
                        &[&repo_id],
                    )
                    .await?;
                let actual: Option<String> = locked.map(|r| r.get(0));
                if let Err(e) = crate::store::verify_optional_repo_project(
                    &repo_id,
                    actual.as_deref(),
                    expected_project_id.as_deref(),
                    "publishing operation",
                ) {
                    return Ok(Err(e));
                }

                let mut current_heads = load_op_heads_in_tx(tx, &repo_id).await?;
                let published_row = tx
                    .query_opt(
                        "SELECT published FROM operations WHERE repo_id = $1 AND op_id = $2",
                        &[&repo_id, &op_id],
                    )
                    .await?;
                if published_row.is_some_and(|r| r.get::<_, bool>(0)) {
                    return Ok(Ok(current_heads));
                }

                if let Some(ext_view_id) = &external_view_id {
                    let exists = tx
                        .query_opt(
                            "SELECT 1 FROM views WHERE repo_id = $1 AND view_id = $2",
                            &[&repo_id, ext_view_id],
                        )
                        .await?;
                    if exists.is_none() {
                        return Ok(Err(StoreError::MissingDependency {
                            parent_kind: "operation",
                            missing_kind: "view",
                            id_hex: hex::encode(ext_view_id),
                        }));
                    }
                }

                if !required_parents.is_empty() {
                    let mut existing_parents: HashSet<Vec<u8>> =
                        HashSet::with_capacity(required_parents.len());
                    for chunk in required_parents.chunks(UNNEST_BATCH_SIZE) {
                        let rows = tx
                            .query(
                                "SELECT op_id FROM operations
                                 WHERE repo_id = $1 AND op_id IN (SELECT UNNEST($2::BYTEA[]))",
                                &[&repo_id, &chunk],
                            )
                            .await?;
                        for row in rows {
                            existing_parents.insert(row.get::<_, Vec<u8>>(0));
                        }
                    }
                    for parent_id in &required_parents {
                        if !existing_parents.contains(parent_id) {
                            return Ok(Err(StoreError::MissingDependency {
                                parent_kind: "operation",
                                missing_kind: "parent operation",
                                id_hex: hex::encode(parent_id),
                            }));
                        }
                    }
                }

                tx.execute(
                    "INSERT INTO views (repo_id, view_id, proto_bytes)
                     VALUES ($1, $2, $3)
                     ON CONFLICT (repo_id, view_id) DO NOTHING",
                    &[&repo_id, &view_id, &view_bytes],
                )
                .await?;

                tx.execute(
                    "INSERT INTO operations (repo_id, op_id, published, proto_bytes)
                     VALUES ($1, $2, TRUE, $3)
                     ON CONFLICT (repo_id, op_id) DO UPDATE SET published = TRUE",
                    &[&repo_id, &op_id, &op_bytes],
                )
                .await?;

                apply_op_heads_mutation(&mut current_heads, &old_ids, &op_id);
                apply_op_heads_tables_in_tx(tx, &repo_id, &old_ids, &op_id).await?;

                Ok(Ok(current_heads))
            })
        })
        .await
    }

    pub(super) async fn compare_and_swap_op_heads_inner(
        &self,
        repo_id: &str,
        expected_project_id: Option<&str>,
        expected_old_ids: &[Vec<u8>],
        new_id: &[u8],
        require_op_exists: bool,
    ) -> StoreResult<()> {
        let repo_id = repo_id.to_string();
        let expected_project_id = expected_project_id.map(str::to_string);
        let expected_old_ids = expected_old_ids.to_vec();
        let new_id = new_id.to_vec();

        self.run_tx_with_retry(|tx| {
            let repo_id = repo_id.clone();
            let expected_project_id = expected_project_id.clone();
            let expected_old_ids = expected_old_ids.clone();
            let new_id = new_id.clone();
            Box::pin(async move {
                let locked = tx
                    .query_opt(
                        "SELECT project_id FROM repositories WHERE repo_id = $1 FOR NO KEY UPDATE",
                        &[&repo_id],
                    )
                    .await?;
                let actual: Option<String> = locked.map(|r| r.get(0));
                if let Err(e) = crate::store::verify_optional_repo_project(
                    &repo_id,
                    actual.as_deref(),
                    expected_project_id.as_deref(),
                    "comparing and swapping op heads",
                ) {
                    return Ok(Err(e));
                }

                let published_row = tx
                    .query_opt(
                        "SELECT published FROM operations WHERE repo_id = $1 AND op_id = $2",
                        &[&repo_id, &new_id],
                    )
                    .await?;
                if require_op_exists && published_row.is_none() {
                    return Ok(Err(StoreError::MissingDependency {
                        parent_kind: "op_heads",
                        missing_kind: "operation",
                        id_hex: hex::encode(&new_id),
                    }));
                }

                let current_heads = load_op_heads_in_tx(tx, &repo_id).await?;
                if !op_heads_sets_equal(&current_heads, &expected_old_ids) {
                    return Ok(Err(StoreError::CasConflict(format!(
                        "op heads changed concurrently in repo '{repo_id}'"
                    ))));
                }

                tx.execute("DELETE FROM op_heads WHERE repo_id = $1", &[&repo_id])
                    .await?;
                tx.execute(
                    "INSERT INTO op_heads (repo_id, op_id) VALUES ($1, $2)",
                    &[&repo_id, &new_id],
                )
                .await?;
                if published_row.is_some_and(|r| !r.get::<_, bool>(0)) {
                    tx.execute(
                        "UPDATE operations SET published = TRUE WHERE repo_id = $1 AND op_id = $2",
                        &[&repo_id, &new_id],
                    )
                    .await?;
                }

                Ok(Ok(()))
            })
        })
        .await
    }

    pub(super) async fn get_operation_impl(
        &self,
        repo_id: &str,
        op_id: &[u8],
    ) -> StoreResult<Option<Operation>> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(
                "SELECT proto_bytes FROM operations WHERE repo_id = $1 AND op_id = $2",
                &[&repo_id, &op_id],
            )
            .await
            .map_err(map_pg_err)?;
        row.map(|r| {
            let bytes: Vec<u8> = r.get(0);
            Operation::decode(bytes.as_slice()).map_err(|e| {
                StoreError::Corruption(format!(
                    "failed to decode PostgreSQL Operation protobuf: {e}"
                ))
            })
        })
        .transpose()
    }

    pub(super) async fn resolve_operation_id_prefix_impl(
        &self,
        repo_id: &str,
        min_id: &[u8],
        max_id: &[u8],
    ) -> StoreResult<Vec<OpId>> {
        let client = self.get_client().await?;
        let rows = client
            .query(
                "SELECT op_id FROM operations
                 WHERE repo_id = $1 AND op_id >= $2 AND op_id <= $3
                 ORDER BY op_id ASC
                 LIMIT 2",
                &[&repo_id, &min_id, &max_id],
            )
            .await
            .map_err(map_pg_err)?;
        Ok(rows.into_iter().map(|r| r.get::<_, Vec<u8>>(0)).collect())
    }

    pub(super) async fn has_view_impl(&self, repo_id: &str, view_id: &[u8]) -> StoreResult<bool> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(
                "SELECT 1 FROM views WHERE repo_id = $1 AND view_id = $2 LIMIT 1",
                &[&repo_id, &view_id],
            )
            .await
            .map_err(map_pg_err)?;
        Ok(row.is_some())
    }

    pub(super) async fn get_view_impl(
        &self,
        repo_id: &str,
        view_id: &[u8],
    ) -> StoreResult<Option<View>> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(
                "SELECT proto_bytes FROM views WHERE repo_id = $1 AND view_id = $2",
                &[&repo_id, &view_id],
            )
            .await
            .map_err(map_pg_err)?;
        row.map(|r| {
            let bytes: Vec<u8> = r.get(0);
            View::decode(bytes.as_slice()).map_err(|e| {
                StoreError::Corruption(format!("failed to decode PostgreSQL View protobuf: {e}"))
            })
        })
        .transpose()
    }

    pub(super) async fn get_op_heads_impl(&self, repo_id: &str) -> StoreResult<Option<Vec<OpId>>> {
        let client = self.get_client().await?;
        let rows = client
            .query(
                "SELECT oh.op_id
                 FROM repositories r
                 LEFT JOIN op_heads oh ON oh.repo_id = r.repo_id
                 WHERE r.repo_id = $1
                 ORDER BY oh.added_seq ASC, oh.op_id ASC",
                &[&repo_id],
            )
            .await
            .map_err(map_pg_err)?;
        if rows.is_empty() {
            return Ok(None);
        }
        let heads = rows
            .into_iter()
            .filter_map(|r| r.get::<_, Option<Vec<u8>>>(0))
            .collect();
        Ok(Some(heads))
    }
}
