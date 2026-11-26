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

//! Workspace state storage, keyset pagination, and CAS updates for `SqliteMetadataStore`.
//!
//! # Authority and Crash-Safety Ordering
//! `put_workspace_cas` evaluates optional expected operation preconditions inside a
//! `TransactionBehavior::Immediate` transaction before inserting or replacing workspace
//! metadata, while `CompositeStore` validates repository registration and dependencies.
//!
//! # Concurrency and Merge Invariants
//! Workspace compare-and-swap updates serialize under the SQLite writer lock without
//! read-modify-write races, and paginated listings return deterministic keyset order.

use cc_common::workspace::WorkspaceState;
use rusqlite::{OptionalExtension, TransactionBehavior, params};

use super::SqliteMetadataStore;
use super::ops::{decode_hex_id_list_json, encode_hex_id_list_json};
use crate::store::{StoreError, StoreResult};

type SqliteWorkspaceTuple = (
    String,
    String,
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    String,
    String,
    String,
);

#[allow(deprecated)]
fn decode_sqlite_workspace_row(
    repo_id: &str,
    row: rusqlite::Result<SqliteWorkspaceTuple>,
) -> StoreResult<WorkspaceState> {
    let (
        user,
        workspace_name,
        commit_id,
        operation_id,
        tree_id,
        tree_ids_json,
        conflict_labels_json,
        sparse_json,
    ) = row.map_err(|e| StoreError::Backend(format!("SQLite read workspace row failed: {e}")))?;
    let mut tree_ids = decode_hex_id_list_json(&tree_ids_json, "tree_ids_json")?;
    if tree_ids.is_empty() && !tree_id.is_empty() {
        tree_ids.push(tree_id.clone());
    }
    let conflict_labels: Vec<String> = serde_json::from_str(&conflict_labels_json)
        .map_err(|e| StoreError::Corruption(format!("invalid conflict_labels_json: {e}")))?;
    let sparse_patterns: Vec<String> = serde_json::from_str(&sparse_json)
        .map_err(|e| StoreError::Corruption(format!("invalid sparse_patterns_json: {e}")))?;
    #[allow(deprecated)]
    Ok(WorkspaceState {
        repo_id: repo_id.to_string(),
        user,
        workspace_name,
        commit_id,
        operation_id,
        tree_id,
        sparse_patterns,
        tree_ids,
        conflict_labels,
        sparse_patterns_set: true,
    })
}

fn decode_sqlite_workspace_rows<I>(repo_id: &str, rows: I) -> StoreResult<Vec<WorkspaceState>>
where
    I: IntoIterator<Item = rusqlite::Result<SqliteWorkspaceTuple>>,
{
    let mut out = Vec::new();
    for row in rows {
        out.push(decode_sqlite_workspace_row(repo_id, row)?);
    }
    Ok(out)
}

fn decode_sqlite_workspace_rows_page<I>(
    repo_id: &str,
    rows: I,
    limit: usize,
    max_bytes: Option<usize>,
) -> StoreResult<(Vec<WorkspaceState>, bool)>
where
    I: IntoIterator<Item = rusqlite::Result<SqliteWorkspaceTuple>>,
{
    let mut out = Vec::new();
    let mut response_bytes = 0usize;
    let mut has_more = false;
    for row in rows {
        let ws = decode_sqlite_workspace_row(repo_id, row)?;
        let entry_bytes = crate::store::workspace_list_entry_bytes(&ws);
        if out.len() >= limit
            || max_bytes.is_some_and(|cap| {
                !out.is_empty() && response_bytes.saturating_add(entry_bytes) > cap
            })
        {
            has_more = true;
            break;
        }
        response_bytes = response_bytes.saturating_add(entry_bytes);
        out.push(ws);
    }
    Ok((out, has_more))
}

impl SqliteMetadataStore {
    pub(super) async fn get_workspace_impl(
        &self,
        repo_id: &str,
        user: &str,
        workspace_name: &str,
    ) -> StoreResult<Option<WorkspaceState>> {
        let repo_id = repo_id.to_string();
        let user = user.to_string();
        let workspace_name = workspace_name.to_string();
        self.with_read_conn(move |conn| {
            let row = conn
                .query_row(
                    "SELECT commit_id, operation_id, tree_id, tree_ids_json, conflict_labels_json, sparse_patterns_json
                     FROM workspaces
                     WHERE repo_id = ?1 AND user_email = ?2 AND workspace_name = ?3",
                    params![repo_id, user, workspace_name],
                    |r| {
                        Ok((
                            r.get::<_, Vec<u8>>(0)?,
                            r.get::<_, Vec<u8>>(1)?,
                            r.get::<_, Vec<u8>>(2)?,
                            r.get::<_, String>(3)?,
                            r.get::<_, String>(4)?,
                            r.get::<_, String>(5)?,
                        ))
                    },
                )
                .optional()
                .map_err(|e| StoreError::Backend(format!("SQLite get_workspace failed: {e}")))?;

            row.map(
                |(
                    commit_id,
                    operation_id,
                    tree_id,
                    tree_ids_json,
                    conflict_labels_json,
                    sparse_json,
                )| {
                    let mut tree_ids = decode_hex_id_list_json(&tree_ids_json, "tree_ids_json")?;
                    if tree_ids.is_empty() && !tree_id.is_empty() {
                        tree_ids.push(tree_id.clone());
                    }
                    let conflict_labels: Vec<String> = serde_json::from_str(&conflict_labels_json)
                        .map_err(|e| {
                            StoreError::Corruption(format!("invalid conflict_labels_json: {e}"))
                        })?;
                    let sparse_patterns: Vec<String> =
                        serde_json::from_str(&sparse_json).map_err(|e| {
                            StoreError::Corruption(format!("invalid sparse_patterns_json: {e}"))
                        })?;
                    #[allow(deprecated)]
                    Ok(WorkspaceState {
                        repo_id,
                        user,
                        workspace_name,
                        commit_id,
                        operation_id,
                        tree_id,
                        sparse_patterns,
                        tree_ids,
                        conflict_labels,
                        sparse_patterns_set: true,
                    })
                },
            )
            .transpose()
        })
        .await
    }

    #[allow(deprecated)]
    pub(super) async fn put_workspace_cas_impl(
        &self,
        workspace: &WorkspaceState,
        expected_operation_id: Option<&[u8]>,
    ) -> StoreResult<()> {
        let ws = workspace.clone();
        let expected_op = expected_operation_id.map(|b| b.to_vec());
        self.with_write_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite begin put_workspace_cas failed: {e}"))
                })?;
            if expected_op.is_some() {
                let row = tx
                    .query_row(
                        "SELECT commit_id, operation_id, tree_id, tree_ids_json, conflict_labels_json, sparse_patterns_json
                         FROM workspaces
                         WHERE repo_id = ?1 AND user_email = ?2 AND workspace_name = ?3",
                        params![ws.repo_id, ws.user, ws.workspace_name],
                        |r| {
                            Ok((
                                r.get::<_, Vec<u8>>(0)?,
                                r.get::<_, Vec<u8>>(1)?,
                                r.get::<_, Vec<u8>>(2)?,
                                r.get::<_, String>(3)?,
                                r.get::<_, String>(4)?,
                                r.get::<_, String>(5)?,
                            ))
                        },
                    )
                    .optional()
                    .map_err(|e| {
                        StoreError::Backend(format!("SQLite query workspace CAS failed: {e}"))
                    })?;
                let existing = match row {
                    Some((
                        commit_id,
                        operation_id,
                        tree_id,
                        tree_ids_json,
                        conflict_labels_json,
                        sparse_json,
                    )) => {
                        let mut tree_ids = decode_hex_id_list_json(&tree_ids_json, "tree_ids_json")?;
                        if tree_ids.is_empty() && !tree_id.is_empty() {
                            tree_ids.push(tree_id.clone());
                        }
                        let conflict_labels: Vec<String> =
                            serde_json::from_str(&conflict_labels_json).map_err(|e| {
                                StoreError::Corruption(format!(
                                    "corrupt conflict_labels_json in SQLite: {e}"
                                ))
                            })?;
                        let sparse_patterns: Vec<String> =
                            serde_json::from_str(&sparse_json).map_err(|e| {
                                StoreError::Corruption(format!(
                                    "corrupt sparse_patterns_json in SQLite: {e}"
                                ))
                            })?;
                        Some(WorkspaceState {
                            repo_id: ws.repo_id.clone(),
                            user: ws.user.clone(),
                            workspace_name: ws.workspace_name.clone(),
                            commit_id,
                            operation_id,
                            tree_id,
                            sparse_patterns,
                            tree_ids,
                            conflict_labels,
                            sparse_patterns_set: true,
                        })
                    }
                    None => None,
                };
                if crate::store::check_workspace_cas_precondition(
                    existing.as_ref(),
                    &ws,
                    expected_op.as_deref(),
                )? {
                    return Ok(());
                }
            }
            let tree_ids_json = encode_hex_id_list_json(&ws.tree_ids, "tree_ids_json")?;
            let conflict_labels_json = serde_json::to_string(&ws.conflict_labels).map_err(|e| {
                StoreError::Corruption(format!("failed to serialize conflict_labels: {e}"))
            })?;
            let sparse_json = serde_json::to_string(&ws.sparse_patterns).map_err(|e| {
                StoreError::Corruption(format!("failed to serialize sparse_patterns: {e}"))
            })?;
            tx.execute(
                "INSERT OR REPLACE INTO workspaces
                 (repo_id, user_email, workspace_name, commit_id, operation_id, tree_id, tree_ids_json, conflict_labels_json, sparse_patterns_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    ws.repo_id,
                    ws.user,
                    ws.workspace_name,
                    ws.commit_id,
                    ws.operation_id,
                    ws.tree_id,
                    tree_ids_json,
                    conflict_labels_json,
                    sparse_json
                ],
            )
            .map_err(|e| StoreError::Backend(format!("SQLite put_workspace failed: {e}")))?;
            tx.commit()
                .map_err(|e| StoreError::Backend(format!("SQLite commit put_workspace failed: {e}")))
        })
        .await
    }

    #[allow(deprecated)]
    pub(super) async fn list_workspaces_impl(
        &self,
        repo_id: &str,
    ) -> StoreResult<Vec<WorkspaceState>> {
        let repo_id = repo_id.to_string();
        self.with_read_conn(move |conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT user_email, workspace_name, commit_id, operation_id, tree_id, tree_ids_json, conflict_labels_json, sparse_patterns_json
                     FROM workspaces
                     WHERE repo_id = ?1
                     ORDER BY user_email ASC, workspace_name ASC",
                )
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite prepare list_workspaces failed: {e}"))
                })?;

            let rows = stmt
                .query_map(params![repo_id], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, Vec<u8>>(2)?,
                        r.get::<_, Vec<u8>>(3)?,
                        r.get::<_, Vec<u8>>(4)?,
                        r.get::<_, String>(5)?,
                        r.get::<_, String>(6)?,
                        r.get::<_, String>(7)?,
                    ))
                })
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite query list_workspaces failed: {e}"))
                })?;

            decode_sqlite_workspace_rows(&repo_id, rows)
        })
        .await
    }

    #[allow(deprecated)]
    pub(super) async fn list_workspaces_page_impl(
        &self,
        repo_id: &str,
        after: Option<(&str, &str)>,
        limit: usize,
        max_bytes: Option<usize>,
    ) -> StoreResult<(Vec<WorkspaceState>, bool)> {
        if limit == 0 {
            return Ok((Vec::new(), false));
        }
        let repo_id = repo_id.to_string();
        let after = after.map(|(u, w)| (u.to_string(), w.to_string()));
        let limit_i64 = i64::try_from(limit.saturating_add(1)).unwrap_or(i64::MAX);
        self.with_read_conn(move |conn| match after {
            Some((after_user, after_ws)) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT user_email, workspace_name, commit_id, operation_id, tree_id, tree_ids_json, conflict_labels_json, sparse_patterns_json
                         FROM workspaces
                         WHERE repo_id = ?1
                           AND (user_email, workspace_name) > (?2, ?3)
                         ORDER BY user_email ASC, workspace_name ASC
                         LIMIT ?4",
                    )
                    .map_err(|e| {
                        StoreError::Backend(format!(
                            "SQLite prepare list_workspaces_page failed: {e}"
                        ))
                    })?;
                let rows = stmt
                    .query_map(params![repo_id, after_user, after_ws, limit_i64], |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, Vec<u8>>(2)?,
                            r.get::<_, Vec<u8>>(3)?,
                            r.get::<_, Vec<u8>>(4)?,
                            r.get::<_, String>(5)?,
                            r.get::<_, String>(6)?,
                            r.get::<_, String>(7)?,
                        ))
                    })
                    .map_err(|e| {
                        StoreError::Backend(format!(
                            "SQLite query list_workspaces_page failed: {e}"
                        ))
                    })?;
                decode_sqlite_workspace_rows_page(&repo_id, rows, limit, max_bytes)
            }
            None => {
                let mut stmt = conn
                    .prepare(
                        "SELECT user_email, workspace_name, commit_id, operation_id, tree_id, tree_ids_json, conflict_labels_json, sparse_patterns_json
                         FROM workspaces
                         WHERE repo_id = ?1
                         ORDER BY user_email ASC, workspace_name ASC
                         LIMIT ?2",
                    )
                    .map_err(|e| {
                        StoreError::Backend(format!(
                            "SQLite prepare list_workspaces_page failed: {e}"
                        ))
                    })?;
                let rows = stmt
                    .query_map(params![repo_id, limit_i64], |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, Vec<u8>>(2)?,
                            r.get::<_, Vec<u8>>(3)?,
                            r.get::<_, Vec<u8>>(4)?,
                            r.get::<_, String>(5)?,
                            r.get::<_, String>(6)?,
                            r.get::<_, String>(7)?,
                        ))
                    })
                    .map_err(|e| {
                        StoreError::Backend(format!(
                            "SQLite query list_workspaces_page failed: {e}"
                        ))
                    })?;
                decode_sqlite_workspace_rows_page(&repo_id, rows, limit, max_bytes)
            }
        })
        .await
    }

    pub(super) async fn delete_workspace_impl(
        &self,
        repo_id: &str,
        user: &str,
        workspace_name: &str,
    ) -> StoreResult<bool> {
        let repo_id = repo_id.to_string();
        let user = user.to_string();
        let workspace_name = workspace_name.to_string();
        self.with_write_conn(move |conn| {
            let removed = conn
                .execute(
                    "DELETE FROM workspaces WHERE repo_id = ?1 AND user_email = ?2 AND workspace_name = ?3",
                    params![repo_id, user, workspace_name],
                )
                .map_err(|e| StoreError::Backend(format!("SQLite delete_workspace failed: {e}")))?;
            Ok(removed > 0)
        })
        .await
    }
}
