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

//! Operation, view, and atomic operation-head persistence for `SqliteMetadataStore`.
//!
//! # Authority and Crash-Safety Ordering
//! `publish_operation_atomic`, `update_op_heads`, and `compare_and_swap_op_heads` execute
//! inside `TransactionBehavior::Immediate` transactions, persisting views and operations
//! and marking published operations atomically with the active head set update.
//!
//! # Concurrency and Merge Invariants
//! Serialized writer transactions apply set-based operation-head mutations without
//! read-modify-write races, guard against resurrecting already-published superseded heads,
//! and enforce exact expected-set checks for compare-and-swap reconciliation.

use cc_common::op_store::{Operation, View};
use prost::Message;
use rusqlite::{OptionalExtension, TransactionBehavior, params};

use super::SqliteMetadataStore;
use crate::store::{
    OpId, StoreError, StoreResult, apply_op_heads_mutation, collect_non_root_op_parents,
    op_heads_sets_equal, verify_optional_repo_project,
};

pub(super) fn encode_hex_id_list_json(ids: &[Vec<u8>], field: &str) -> StoreResult<String> {
    let hex_ids: Vec<String> = ids.iter().map(hex::encode).collect();
    serde_json::to_string(&hex_ids)
        .map_err(|e| StoreError::Corruption(format!("failed to serialize {field} JSON: {e}")))
}

pub(super) fn decode_hex_id_list_json(raw: &str, field: &str) -> StoreResult<Vec<Vec<u8>>> {
    let hex_ids: Vec<String> = serde_json::from_str(raw)
        .map_err(|e| StoreError::Corruption(format!("failed to parse {field} JSON: {e}")))?;
    hex_ids
        .into_iter()
        .map(|h| {
            hex::decode(&h).map_err(|e| {
                StoreError::Corruption(format!("invalid hex in persisted {field} '{h}': {e}"))
            })
        })
        .collect()
}

impl SqliteMetadataStore {
    async fn put_operation_inner(
        &self,
        repo_id: &str,
        expected_project_id: Option<&str>,
        op_id: &[u8],
        op: &Operation,
        require_dependencies_exist: bool,
    ) -> StoreResult<()> {
        let repo_id = repo_id.to_string();
        let expected_project_id = expected_project_id.map(ToOwned::to_owned);
        let op_id = op_id.to_vec();
        let proto_bytes = op.encode_to_vec();
        let required_view_id = (require_dependencies_exist
            && op.view_id.as_slice() != cc_common::ROOT_VIEW_ID_BYTES)
            .then(|| op.view_id.clone());
        let required_parents = if require_dependencies_exist {
            collect_non_root_op_parents(op)
        } else {
            Vec::new()
        };
        self.with_write_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite begin put_operation tx failed: {e}"))
                })?;
            let actual_project: Option<String> = tx
                .query_row(
                    "SELECT project_id FROM repositories WHERE repo_id = ?1",
                    params![repo_id],
                    |r| r.get(0),
                )
                .optional()
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite put_operation repo lookup failed: {e}"))
                })?;
            verify_optional_repo_project(
                &repo_id,
                actual_project.as_deref(),
                expected_project_id.as_deref(),
                "writing operations",
            )?;
            let op_exists: Option<i64> = tx
                .query_row(
                    "SELECT 1 FROM operations WHERE repo_id = ?1 AND op_id = ?2",
                    params![repo_id, op_id],
                    |r| r.get(0),
                )
                .optional()
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite check existing operation failed: {e}"))
                })?;
            if op_exists.is_some() {
                return Ok(());
            }
            if let Some(req_view_id) = &required_view_id {
                let exists: Option<i64> = tx
                    .query_row(
                        "SELECT 1 FROM views WHERE repo_id = ?1 AND view_id = ?2",
                        params![repo_id, req_view_id],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(|e| {
                        StoreError::Backend(format!("SQLite check operation view failed: {e}"))
                    })?;
                if exists.is_none() {
                    return Err(StoreError::MissingDependency {
                        parent_kind: "operation",
                        missing_kind: "view",
                        id_hex: hex::encode(req_view_id),
                    });
                }
            }
            for parent_id in &required_parents {
                let exists: Option<i64> = tx
                    .query_row(
                        "SELECT 1 FROM operations WHERE repo_id = ?1 AND op_id = ?2",
                        params![repo_id, parent_id],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(|e| {
                        StoreError::Backend(format!("SQLite check parent operation failed: {e}"))
                    })?;
                if exists.is_none() {
                    return Err(StoreError::MissingDependency {
                        parent_kind: "operation",
                        missing_kind: "parent operation",
                        id_hex: hex::encode(parent_id),
                    });
                }
            }
            tx.execute(
                "INSERT OR IGNORE INTO operations (repo_id, op_id, proto_bytes, published)
                 VALUES (?1, ?2, ?3, 0)",
                params![repo_id, op_id, proto_bytes],
            )
            .map_err(|e| StoreError::Backend(format!("SQLite put_operation failed: {e}")))?;
            tx.commit().map_err(|e| {
                StoreError::Backend(format!("SQLite commit put_operation failed: {e}"))
            })
        })
        .await
    }

    async fn put_view_inner(
        &self,
        repo_id: &str,
        expected_project_id: Option<&str>,
        view_id: &[u8],
        view: &View,
    ) -> StoreResult<()> {
        let repo_id = repo_id.to_string();
        let expected_project_id = expected_project_id.map(ToOwned::to_owned);
        let view_id = view_id.to_vec();
        let proto_bytes = view.encode_to_vec();
        self.with_write_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite begin put_view tx failed: {e}"))
                })?;
            let actual_project: Option<String> = tx
                .query_row(
                    "SELECT project_id FROM repositories WHERE repo_id = ?1",
                    params![repo_id],
                    |r| r.get(0),
                )
                .optional()
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite put_view repo lookup failed: {e}"))
                })?;
            verify_optional_repo_project(
                &repo_id,
                actual_project.as_deref(),
                expected_project_id.as_deref(),
                "writing views",
            )?;
            tx.execute(
                "INSERT OR IGNORE INTO views (repo_id, view_id, proto_bytes)
                 VALUES (?1, ?2, ?3)",
                params![repo_id, view_id, proto_bytes],
            )
            .map_err(|e| StoreError::Backend(format!("SQLite put_view failed: {e}")))?;
            tx.commit()
                .map_err(|e| StoreError::Backend(format!("SQLite commit put_view failed: {e}")))
        })
        .await
    }

    async fn update_op_heads_inner(
        &self,
        repo_id: &str,
        expected_project_id: Option<&str>,
        old_ids: &[Vec<u8>],
        new_id: &[u8],
        require_op_exists: bool,
    ) -> StoreResult<Vec<OpId>> {
        let repo_id = repo_id.to_string();
        let expected_project_id = expected_project_id.map(ToOwned::to_owned);
        let old_ids = old_ids.to_vec();
        let new_id = new_id.to_vec();

        self.with_write_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite begin update_op_heads tx failed: {e}"))
                })?;

            let repo_row: Option<(String, String)> = tx
                .query_row(
                    "SELECT project_id, op_heads_json FROM repositories WHERE repo_id = ?1",
                    params![repo_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(|e| StoreError::Backend(format!("SQLite select op_heads failed: {e}")))?;
            verify_optional_repo_project(
                &repo_id,
                repo_row.as_ref().map(|(p, _)| p.as_str()),
                expected_project_id.as_deref(),
                "updating op heads",
            )?;
            let (_, raw) = repo_row.expect("verified repo row exists");

            let mut current_heads = decode_hex_id_list_json(&raw, "op_heads")?;
            let published: Option<i64> = tx
                .query_row(
                    "SELECT published FROM operations WHERE repo_id = ?1 AND op_id = ?2",
                    params![repo_id, new_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite select operation published failed: {e}"))
                })?;
            if require_op_exists && published.is_none() {
                return Err(StoreError::MissingDependency {
                    parent_kind: "op_heads",
                    missing_kind: "operation",
                    id_hex: hex::encode(&new_id),
                });
            }
            let already_published = published.is_some_and(|p| p != 0);
            if already_published
                && !current_heads
                    .iter()
                    .any(|h| h.as_slice() == new_id.as_slice())
            {
                return Ok(current_heads);
            }
            apply_op_heads_mutation(&mut current_heads, &old_ids, &new_id);
            let encoded = encode_hex_id_list_json(&current_heads, "op_heads")?;

            tx.execute(
                "UPDATE repositories SET op_heads_json = ?1 WHERE repo_id = ?2",
                params![encoded, repo_id],
            )
            .map_err(|e| StoreError::Backend(format!("SQLite update op_heads failed: {e}")))?;

            if !already_published {
                tx.execute(
                    "UPDATE operations SET published = 1 WHERE repo_id = ?1 AND op_id = ?2",
                    params![repo_id, new_id],
                )
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite mark operation published failed: {e}"))
                })?;
            }

            tx.commit().map_err(|e| {
                StoreError::Backend(format!("SQLite commit update_op_heads failed: {e}"))
            })?;

            Ok(current_heads)
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn publish_operation_inner(
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
        let expected_project_id = expected_project_id.map(ToOwned::to_owned);
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
            collect_non_root_op_parents(op)
        } else {
            Vec::new()
        };

        self.with_write_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite begin publish_operation tx failed: {e}"))
                })?;

            let repo_row: Option<(String, String)> = tx
                .query_row(
                    "SELECT project_id, op_heads_json FROM repositories WHERE repo_id = ?1",
                    params![repo_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(|e| StoreError::Backend(format!("SQLite select op_heads failed: {e}")))?;
            verify_optional_repo_project(
                &repo_id,
                repo_row.as_ref().map(|(p, _)| p.as_str()),
                expected_project_id.as_deref(),
                "publishing operation",
            )?;
            let (_, raw) = repo_row.expect("verified repo row exists");
            let mut current_heads = decode_hex_id_list_json(&raw, "op_heads")?;

            let published: Option<i64> = tx
                .query_row(
                    "SELECT published FROM operations WHERE repo_id = ?1 AND op_id = ?2",
                    params![repo_id, op_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite select operation published failed: {e}"))
                })?;
            if published.is_some_and(|p| p != 0) {
                return Ok(current_heads);
            }

            if let Some(ext_view_id) = &external_view_id {
                let exists: Option<i64> = tx
                    .query_row(
                        "SELECT 1 FROM views WHERE repo_id = ?1 AND view_id = ?2",
                        params![repo_id, ext_view_id],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(|e| {
                        StoreError::Backend(format!("SQLite check external view failed: {e}"))
                    })?;
                if exists.is_none() {
                    return Err(StoreError::MissingDependency {
                        parent_kind: "operation",
                        missing_kind: "view",
                        id_hex: hex::encode(ext_view_id),
                    });
                }
            }

            for parent_id in &required_parents {
                let exists: Option<i64> = tx
                    .query_row(
                        "SELECT 1 FROM operations WHERE repo_id = ?1 AND op_id = ?2",
                        params![repo_id, parent_id],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(|e| {
                        StoreError::Backend(format!("SQLite check parent operation failed: {e}"))
                    })?;
                if exists.is_none() {
                    return Err(StoreError::MissingDependency {
                        parent_kind: "operation",
                        missing_kind: "parent operation",
                        id_hex: hex::encode(parent_id),
                    });
                }
            }

            tx.execute(
                "INSERT OR IGNORE INTO views (repo_id, view_id, proto_bytes) VALUES (?1, ?2, ?3)",
                params![repo_id, view_id, view_bytes],
            )
            .map_err(|e| StoreError::Backend(format!("SQLite publish view failed: {e}")))?;

            tx.execute(
                "INSERT INTO operations (repo_id, op_id, proto_bytes, published)
                 VALUES (?1, ?2, ?3, 1)
                 ON CONFLICT(repo_id, op_id) DO UPDATE SET published = 1",
                params![repo_id, op_id, op_bytes],
            )
            .map_err(|e| StoreError::Backend(format!("SQLite publish operation failed: {e}")))?;

            apply_op_heads_mutation(&mut current_heads, &old_ids, &op_id);
            let encoded = encode_hex_id_list_json(&current_heads, "op_heads")?;

            tx.execute(
                "UPDATE repositories SET op_heads_json = ?1 WHERE repo_id = ?2",
                params![encoded, repo_id],
            )
            .map_err(|e| StoreError::Backend(format!("SQLite update op_heads failed: {e}")))?;

            tx.commit().map_err(|e| {
                StoreError::Backend(format!("SQLite commit publish_operation failed: {e}"))
            })?;

            Ok(current_heads)
        })
        .await
    }

    async fn compare_and_swap_op_heads_inner(
        &self,
        repo_id: &str,
        expected_project_id: Option<&str>,
        expected_old_ids: &[Vec<u8>],
        new_id: &[u8],
        require_op_exists: bool,
    ) -> StoreResult<()> {
        let repo_id = repo_id.to_string();
        let expected_project_id = expected_project_id.map(ToOwned::to_owned);
        let expected_old_ids = expected_old_ids.to_vec();
        let new_id = new_id.to_vec();

        self.with_write_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite begin cas_op_heads tx failed: {e}"))
                })?;

            let repo_row: Option<(String, String)> = tx
                .query_row(
                    "SELECT project_id, op_heads_json FROM repositories WHERE repo_id = ?1",
                    params![repo_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(|e| StoreError::Backend(format!("SQLite select op_heads failed: {e}")))?;
            verify_optional_repo_project(
                &repo_id,
                repo_row.as_ref().map(|(p, _)| p.as_str()),
                expected_project_id.as_deref(),
                "comparing and swapping op heads",
            )?;
            let (_, raw) = repo_row.expect("verified repo row exists");

            let published: Option<i64> = tx
                .query_row(
                    "SELECT published FROM operations WHERE repo_id = ?1 AND op_id = ?2",
                    params![repo_id, new_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite select operation published failed: {e}"))
                })?;
            if require_op_exists && published.is_none() {
                return Err(StoreError::MissingDependency {
                    parent_kind: "op_heads",
                    missing_kind: "operation",
                    id_hex: hex::encode(&new_id),
                });
            }

            let current_heads = decode_hex_id_list_json(&raw, "op_heads")?;
            if !op_heads_sets_equal(&current_heads, &expected_old_ids) {
                return Err(StoreError::CasConflict(format!(
                    "op heads changed concurrently in repo '{repo_id}'"
                )));
            }

            let encoded = encode_hex_id_list_json(std::slice::from_ref(&new_id), "op_heads")?;
            tx.execute(
                "UPDATE repositories SET op_heads_json = ?1 WHERE repo_id = ?2",
                params![encoded, repo_id],
            )
            .map_err(|e| StoreError::Backend(format!("SQLite cas op_heads failed: {e}")))?;

            if published == Some(0) {
                tx.execute(
                    "UPDATE operations SET published = 1 WHERE repo_id = ?1 AND op_id = ?2",
                    params![repo_id, new_id],
                )
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite mark operation published failed: {e}"))
                })?;
            }

            tx.commit()
                .map_err(|e| StoreError::Backend(format!("SQLite commit cas_op_heads failed: {e}")))
        })
        .await
    }

    pub(super) async fn get_operation_impl(
        &self,
        repo_id: &str,
        op_id: &[u8],
    ) -> StoreResult<Option<Operation>> {
        let repo_id = repo_id.to_string();
        let op_id = op_id.to_vec();
        self.with_read_conn(move |conn| {
            let raw: Option<Vec<u8>> = conn
                .query_row(
                    "SELECT proto_bytes FROM operations WHERE repo_id = ?1 AND op_id = ?2",
                    params![repo_id, op_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| StoreError::Backend(format!("SQLite get_operation failed: {e}")))?;

            raw.map(|bytes| {
                Operation::decode(bytes.as_slice()).map_err(|e| {
                    StoreError::Corruption(format!("failed to decode Operation protobuf: {e}"))
                })
            })
            .transpose()
        })
        .await
    }

    pub(super) async fn put_operation_impl(
        &self,
        repo_id: &str,
        op_id: &[u8],
        op: &Operation,
    ) -> StoreResult<()> {
        self.put_operation_inner(repo_id, None, op_id, op, false)
            .await
    }

    pub(super) async fn put_operation_for_project_impl(
        &self,
        repo_id: &str,
        expected_project_id: &str,
        op_id: &[u8],
        op: &Operation,
        require_dependencies_exist: bool,
    ) -> StoreResult<()> {
        self.put_operation_inner(
            repo_id,
            Some(expected_project_id),
            op_id,
            op,
            require_dependencies_exist,
        )
        .await
    }

    pub(super) async fn resolve_operation_id_prefix_impl(
        &self,
        repo_id: &str,
        min_id: &[u8],
        max_id: &[u8],
    ) -> StoreResult<Vec<OpId>> {
        let repo_id = repo_id.to_string();
        let min_id = min_id.to_vec();
        let max_id = max_id.to_vec();
        self.with_read_conn(move |conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT op_id FROM operations
                     WHERE repo_id = ?1 AND op_id >= ?2 AND op_id <= ?3
                     ORDER BY op_id ASC
                     LIMIT 2",
                )
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite prepare prefix query failed: {e}"))
                })?;

            let rows = stmt
                .query_map(params![repo_id, min_id, max_id], |row| {
                    row.get::<_, Vec<u8>>(0)
                })
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite execute prefix query failed: {e}"))
                })?;

            let mut matches = Vec::new();
            for row in rows {
                matches.push(row.map_err(|e| {
                    StoreError::Backend(format!("SQLite read prefix match failed: {e}"))
                })?);
            }
            Ok(matches)
        })
        .await
    }

    pub(super) async fn get_view_impl(
        &self,
        repo_id: &str,
        view_id: &[u8],
    ) -> StoreResult<Option<View>> {
        let repo_id = repo_id.to_string();
        let view_id = view_id.to_vec();
        self.with_read_conn(move |conn| {
            let raw: Option<Vec<u8>> = conn
                .query_row(
                    "SELECT proto_bytes FROM views WHERE repo_id = ?1 AND view_id = ?2",
                    params![repo_id, view_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| StoreError::Backend(format!("SQLite get_view failed: {e}")))?;

            raw.map(|bytes| {
                View::decode(bytes.as_slice()).map_err(|e| {
                    StoreError::Corruption(format!("failed to decode View protobuf: {e}"))
                })
            })
            .transpose()
        })
        .await
    }

    pub(super) async fn has_view_impl(&self, repo_id: &str, view_id: &[u8]) -> StoreResult<bool> {
        let repo_id = repo_id.to_string();
        let view_id = view_id.to_vec();
        self.with_read_conn(move |conn| {
            let exists: Option<i64> = conn
                .query_row(
                    "SELECT 1 FROM views WHERE repo_id = ?1 AND view_id = ?2 LIMIT 1",
                    params![repo_id, view_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| StoreError::Backend(format!("SQLite has_view failed: {e}")))?;
            Ok(exists.is_some())
        })
        .await
    }

    pub(super) async fn put_view_impl(
        &self,
        repo_id: &str,
        view_id: &[u8],
        view: &View,
    ) -> StoreResult<()> {
        self.put_view_inner(repo_id, None, view_id, view).await
    }

    pub(super) async fn put_view_for_project_impl(
        &self,
        repo_id: &str,
        expected_project_id: &str,
        view_id: &[u8],
        view: &View,
    ) -> StoreResult<()> {
        self.put_view_inner(repo_id, Some(expected_project_id), view_id, view)
            .await
    }

    pub(super) async fn get_op_heads_impl(&self, repo_id: &str) -> StoreResult<Option<Vec<OpId>>> {
        let repo_id = repo_id.to_string();
        self.with_read_conn(move |conn| {
            let raw: Option<String> = conn
                .query_row(
                    "SELECT op_heads_json FROM repositories WHERE repo_id = ?1",
                    params![repo_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| StoreError::Backend(format!("SQLite get_op_heads failed: {e}")))?;

            raw.map(|json| decode_hex_id_list_json(&json, "op_heads"))
                .transpose()
        })
        .await
    }

    pub(super) async fn update_op_heads_impl(
        &self,
        repo_id: &str,
        old_ids: &[Vec<u8>],
        new_id: &[u8],
    ) -> StoreResult<Vec<OpId>> {
        self.update_op_heads_inner(repo_id, None, old_ids, new_id, false)
            .await
    }

    pub(super) async fn update_op_heads_for_project_impl(
        &self,
        repo_id: &str,
        expected_project_id: &str,
        old_head_ids: &[Vec<u8>],
        new_head_id: &[u8],
        require_op_exists: bool,
    ) -> StoreResult<Vec<OpId>> {
        self.update_op_heads_inner(
            repo_id,
            Some(expected_project_id),
            old_head_ids,
            new_head_id,
            require_op_exists,
        )
        .await
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
        self.publish_operation_inner(repo_id, None, view_id, view, op_id, op, old_ids, false)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn publish_operation_for_project_impl(
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
        self.publish_operation_inner(
            repo_id,
            Some(expected_project_id),
            view_id,
            view,
            op_id,
            op,
            old_op_head_ids,
            require_dependencies_exist,
        )
        .await
    }

    pub(super) async fn compare_and_swap_op_heads_impl(
        &self,
        repo_id: &str,
        expected_old_ids: &[Vec<u8>],
        new_id: &[u8],
    ) -> StoreResult<()> {
        self.compare_and_swap_op_heads_inner(repo_id, None, expected_old_ids, new_id, false)
            .await
    }

    pub(super) async fn update_op_heads_cas_for_project_impl(
        &self,
        repo_id: &str,
        expected_project_id: &str,
        expected_exact_heads: &[Vec<u8>],
        new_head_id: &[u8],
        require_op_exists: bool,
    ) -> StoreResult<()> {
        self.compare_and_swap_op_heads_inner(
            repo_id,
            Some(expected_project_id),
            expected_exact_heads,
            new_head_id,
            require_op_exists,
        )
        .await
    }
}
