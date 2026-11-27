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

//! PostgreSQL workspace state persistence, compare-and-swap updates, and keyset pagination.
//!
//! # Concurrency and Merge Invariants
//! Serializes compare-and-swap updates with `FOR UPDATE` row locks and uses `ON CONFLICT DO
//! NOTHING` create-only races to distinguish idempotent retries from conflicting creations
//! without requiring `SERIALIZABLE` isolation. Keyset pagination orders by `COLLATE "C"`.

use cc_common::workspace::WorkspaceState;
use prost::Message as _;
use tokio_postgres::error::SqlState;

use super::PostgresMetadataStore;
use super::codec::{current_unix_millis, decode_workspace_row, map_pg_err};
use crate::store::{StoreError, StoreResult};

impl PostgresMetadataStore {
    #[allow(deprecated)]
    pub(super) async fn put_workspace_cas_inner(
        &self,
        expected_project_id: Option<&str>,
        workspace: &WorkspaceState,
        expected_operation_id: Option<&[u8]>,
        require_operation_exists: bool,
    ) -> StoreResult<()> {
        let mut normalized = workspace.clone();
        if normalized.tree_ids.is_empty() && !normalized.tree_id.is_empty() {
            normalized.tree_ids = vec![normalized.tree_id.clone()];
        } else if normalized.tree_id.is_empty()
            && let Some(first) = normalized.tree_ids.first()
        {
            normalized.tree_id = first.clone();
        }
        let expected_project_id = expected_project_id.map(str::to_string);
        let expected_op = expected_operation_id.map(|b| b.to_vec());
        let required_op_id = (require_operation_exists
            && !normalized.operation_id.is_empty()
            && normalized.operation_id.as_slice() != cc_common::ROOT_OPERATION_ID_BYTES)
            .then(|| normalized.operation_id.clone());
        let updated_at = current_unix_millis();

        self.run_tx_with_retry(|tx| {
            let ws = normalized.clone();
            let expected_project_id = expected_project_id.clone();
            let expected_op = expected_op.clone();
            let required_op_id = required_op_id.clone();
            Box::pin(async move {
                if let Some(exp_proj) = expected_project_id.as_deref() {
                    let row = tx
                        .query_opt(
                            "SELECT project_id FROM repositories WHERE repo_id = $1 FOR KEY SHARE",
                            &[&ws.repo_id],
                        )
                        .await?;
                    let actual: Option<String> = row.map(|r| r.get(0));
                    if let Err(e) = crate::store::verify_optional_repo_project(
                        &ws.repo_id,
                        actual.as_deref(),
                        Some(exp_proj),
                        "updating workspaces",
                    ) {
                        return Ok(Err(e));
                    }
                }

                if let Some(op_id) = &required_op_id {
                    let exists = tx
                        .query_opt(
                            "SELECT 1 FROM operations WHERE repo_id = $1 AND op_id = $2",
                            &[&ws.repo_id, op_id],
                        )
                        .await?;
                    if exists.is_none() {
                        return Ok(Err(StoreError::MissingDependency {
                            parent_kind: "workspace",
                            missing_kind: "operation",
                            id_hex: hex::encode(op_id),
                        }));
                    }
                }

                let Some(exp) = &expected_op else {
                    match tx
                        .execute(
                            "INSERT INTO workspaces
                             (repo_id, user_email, workspace_name, operation_id, commit_id,
                              tree_ids, conflict_labels, sparse_patterns, sparse_patterns_set, updated_at)
                             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                             ON CONFLICT (repo_id, user_email, workspace_name) DO UPDATE SET
                               operation_id = EXCLUDED.operation_id,
                               commit_id = EXCLUDED.commit_id,
                               tree_ids = EXCLUDED.tree_ids,
                               conflict_labels = EXCLUDED.conflict_labels,
                               sparse_patterns = EXCLUDED.sparse_patterns,
                               sparse_patterns_set = EXCLUDED.sparse_patterns_set,
                               updated_at = EXCLUDED.updated_at",
                            &[
                                &ws.repo_id,
                                &ws.user,
                                &ws.workspace_name,
                                &ws.operation_id,
                                &ws.commit_id,
                                &ws.tree_ids,
                                &ws.conflict_labels,
                                &ws.sparse_patterns,
                                &ws.sparse_patterns_set,
                                &updated_at,
                            ],
                        )
                        .await
                    {
                        Ok(_) => return Ok(Ok(())),
                        Err(err) if err.code() == Some(&SqlState::FOREIGN_KEY_VIOLATION) => {
                            return Ok(Err(StoreError::RepoNotFound {
                                repo_id: ws.repo_id,
                                action: "updating workspaces".to_string(),
                            }));
                        }
                        Err(err) => return Err(err),
                    }
                };

                let row = tx
                    .query_opt(
                        "SELECT operation_id, commit_id, tree_ids, conflict_labels, sparse_patterns, sparse_patterns_set
                         FROM workspaces
                         WHERE repo_id = $1 AND user_email = $2 AND workspace_name = $3
                         FOR UPDATE",
                        &[&ws.repo_id, &ws.user, &ws.workspace_name],
                    )
                    .await?;

                let existing = row.map(|r| {
                    decode_workspace_row(
                        ws.repo_id.clone(),
                        ws.user.clone(),
                        ws.workspace_name.clone(),
                        &r,
                        0,
                    )
                });

                if existing.is_none() && expected_project_id.is_none() {
                    let repo_exists = tx
                        .query_opt(
                            "SELECT 1 FROM repositories WHERE repo_id = $1",
                            &[&ws.repo_id],
                        )
                        .await?;
                    if repo_exists.is_none() {
                        return Ok(Err(StoreError::RepoNotFound {
                            repo_id: ws.repo_id,
                            action: "updating workspaces".to_string(),
                        }));
                    }
                }

                match crate::store::check_workspace_cas_precondition(
                    existing.as_ref(),
                    &ws,
                    Some(exp.as_slice()),
                ) {
                    Ok(true) => return Ok(Ok(())),
                    Ok(false) => {}
                    Err(err) => return Ok(Err(err)),
                }

                if existing.is_some() {
                    tx.execute(
                        "UPDATE workspaces
                         SET operation_id = $4, commit_id = $5, tree_ids = $6,
                             conflict_labels = $7, sparse_patterns = $8,
                             sparse_patterns_set = $9, updated_at = $10
                         WHERE repo_id = $1 AND user_email = $2 AND workspace_name = $3",
                        &[
                            &ws.repo_id,
                            &ws.user,
                            &ws.workspace_name,
                            &ws.operation_id,
                            &ws.commit_id,
                            &ws.tree_ids,
                            &ws.conflict_labels,
                            &ws.sparse_patterns,
                            &ws.sparse_patterns_set,
                            &updated_at,
                        ],
                    )
                    .await?;
                    return Ok(Ok(()));
                }

                // Create-only CAS: DO NOTHING lets us distinguish concurrent identical retry
                // from a conflicting create without requiring SERIALIZABLE isolation.
                let inserted = match tx
                    .execute(
                        "INSERT INTO workspaces
                         (repo_id, user_email, workspace_name, operation_id, commit_id,
                          tree_ids, conflict_labels, sparse_patterns, sparse_patterns_set, updated_at)
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                         ON CONFLICT (repo_id, user_email, workspace_name) DO NOTHING",
                        &[
                            &ws.repo_id,
                            &ws.user,
                            &ws.workspace_name,
                            &ws.operation_id,
                            &ws.commit_id,
                            &ws.tree_ids,
                            &ws.conflict_labels,
                            &ws.sparse_patterns,
                            &ws.sparse_patterns_set,
                            &updated_at,
                        ],
                    )
                    .await
                {
                    Ok(n) => n,
                    Err(err) if err.code() == Some(&SqlState::FOREIGN_KEY_VIOLATION) => {
                        return Ok(Err(StoreError::RepoNotFound {
                            repo_id: ws.repo_id,
                            action: "updating workspaces".to_string(),
                        }));
                    }
                    Err(err) => return Err(err),
                };

                if inserted == 0 {
                    let raced_row = tx
                        .query_opt(
                            "SELECT operation_id, commit_id, tree_ids, conflict_labels, sparse_patterns, sparse_patterns_set
                             FROM workspaces
                             WHERE repo_id = $1 AND user_email = $2 AND workspace_name = $3
                             FOR UPDATE",
                            &[&ws.repo_id, &ws.user, &ws.workspace_name],
                        )
                        .await?;
                    let raced = raced_row.map(|r| {
                        decode_workspace_row(
                            ws.repo_id.clone(),
                            ws.user.clone(),
                            ws.workspace_name.clone(),
                            &r,
                            0,
                        )
                    });
                    match crate::store::check_workspace_cas_precondition(
                        raced.as_ref(),
                        &ws,
                        Some(exp.as_slice()),
                    ) {
                        Ok(true) => return Ok(Ok(())),
                        Ok(false) => {
                            return Ok(Err(StoreError::CasConflict(format!(
                                "workspace '{}' creation raced in repo '{}'",
                                ws.workspace_name, ws.repo_id
                            ))));
                        }
                        Err(err) => return Ok(Err(err)),
                    }
                }
                Ok(Ok(()))
            })
        })
        .await
    }

    pub(super) async fn get_workspace_impl(
        &self,
        repo_id: &str,
        user: &str,
        workspace_name: &str,
    ) -> StoreResult<Option<WorkspaceState>> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(
                "SELECT operation_id, commit_id, tree_ids, conflict_labels, sparse_patterns, sparse_patterns_set
                 FROM workspaces
                 WHERE repo_id = $1 AND user_email = $2 AND workspace_name = $3",
                &[&repo_id, &user, &workspace_name],
            )
            .await
            .map_err(map_pg_err)?;
        Ok(row.map(|r| {
            decode_workspace_row(
                repo_id.to_string(),
                user.to_string(),
                workspace_name.to_string(),
                &r,
                0,
            )
        }))
    }

    pub(super) async fn list_workspaces_impl(
        &self,
        repo_id: &str,
    ) -> StoreResult<Vec<WorkspaceState>> {
        let client = self.get_client().await?;
        let rows = client
            .query(
                "SELECT user_email, workspace_name, operation_id, commit_id, tree_ids,
                        conflict_labels, sparse_patterns, sparse_patterns_set
                 FROM workspaces
                 WHERE repo_id = $1
                 ORDER BY user_email COLLATE \"C\" ASC, workspace_name COLLATE \"C\" ASC",
                &[&repo_id],
            )
            .await
            .map_err(map_pg_err)?;
        Ok(rows
            .into_iter()
            .map(|r| decode_workspace_row(repo_id.to_string(), r.get(0), r.get(1), &r, 2))
            .collect())
    }

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
        let fetch_limit = i64::try_from(limit.saturating_add(1)).unwrap_or(i64::MAX);
        let client = self.get_client().await?;
        let rows = match after {
            Some((after_user, after_ws)) => client
                .query(
                    "SELECT user_email, workspace_name, operation_id, commit_id, tree_ids,
                            conflict_labels, sparse_patterns, sparse_patterns_set
                     FROM workspaces
                     WHERE repo_id = $1
                       AND (user_email COLLATE \"C\", workspace_name COLLATE \"C\")
                           > ($2::TEXT COLLATE \"C\", $3::TEXT COLLATE \"C\")
                     ORDER BY user_email COLLATE \"C\" ASC, workspace_name COLLATE \"C\" ASC
                     LIMIT $4",
                    &[&repo_id, &after_user, &after_ws, &fetch_limit],
                )
                .await
                .map_err(map_pg_err)?,
            None => client
                .query(
                    "SELECT user_email, workspace_name, operation_id, commit_id, tree_ids,
                            conflict_labels, sparse_patterns, sparse_patterns_set
                     FROM workspaces
                     WHERE repo_id = $1
                     ORDER BY user_email COLLATE \"C\" ASC, workspace_name COLLATE \"C\" ASC
                     LIMIT $2",
                    &[&repo_id, &fetch_limit],
                )
                .await
                .map_err(map_pg_err)?,
        };
        let mut out = Vec::new();
        let mut response_bytes = 0usize;
        let mut has_more = false;
        for row in rows {
            let ws = decode_workspace_row(repo_id.to_string(), row.get(0), row.get(1), &row, 2);
            let entry_bytes = ws.encoded_len().saturating_add(8);
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

    pub(super) async fn delete_workspace_impl(
        &self,
        repo_id: &str,
        user: &str,
        workspace_name: &str,
    ) -> StoreResult<bool> {
        let client = self.get_client().await?;
        let removed = client
            .execute(
                "DELETE FROM workspaces
                 WHERE repo_id = $1 AND user_email = $2 AND workspace_name = $3",
                &[&repo_id, &user, &workspace_name],
            )
            .await
            .map_err(map_pg_err)?;
        Ok(removed > 0)
    }
}
