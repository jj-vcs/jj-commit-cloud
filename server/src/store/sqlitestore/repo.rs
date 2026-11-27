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

//! Project and repository registration and lookup methods for `SqliteMetadataStore`.
//!
//! # Authority and Crash-Safety Ordering
//! Project and repository registrations enforce unique alias bindings, parent-project
//! ownership, and initial root op-head creation inside `IMMEDIATE` transactions.
//!
//! # Concurrency and Merge Invariants
//! Serialized writer transactions prevent repository/project identifier shadowing and
//! preserve existing names on idempotent re-registration.

use rusqlite::{OptionalExtension, TransactionBehavior, params};

use super::SqliteMetadataStore;
use super::ops::encode_hex_id_list_json;
use crate::store::{StoreError, StoreResult};

impl SqliteMetadataStore {
    pub(super) async fn register_project_impl(
        &self,
        project_id: &str,
        name: Option<&str>,
    ) -> StoreResult<()> {
        let project_id = project_id.to_string();
        let name = name
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned);
        self.with_write_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|e| StoreError::Backend(format!("SQLite begin project tx failed: {e}")))?;

            let shadowed_repo: Option<String> = tx
                .query_row(
                    "SELECT project_id FROM repositories WHERE repo_id = ?1",
                    params![project_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite project shadow lookup failed: {e}"))
                })?;
            if let Some(bound_proj) = shadowed_repo
                && bound_proj != project_id
            {
                return Err(StoreError::ProjectAlreadyExists(format!(
                    "project_id '{project_id}' conflicts with existing repository '{project_id}' bound to project '{bound_proj}'"
                )));
            }

            if let Some(alias) = &name {
                let existing: Option<String> = tx
                    .query_row(
                        "SELECT project_id FROM projects WHERE name = ?1",
                        params![alias],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(|e| {
                        StoreError::Backend(format!("SQLite project alias lookup failed: {e}"))
                    })?;
                if let Some(other_id) = existing
                    && other_id != project_id
                {
                    return Err(StoreError::ProjectAlreadyExists(alias.clone()));
                }
            }

            tx.execute(
                "INSERT INTO projects (project_id, name)
                 VALUES (?1, ?2)
                 ON CONFLICT(project_id) DO UPDATE SET name = COALESCE(excluded.name, projects.name)",
                params![project_id, name],
            )
            .map_err(|e| StoreError::Backend(format!("SQLite register_project failed: {e}")))?;

            tx.commit().map_err(|e| {
                StoreError::Backend(format!("SQLite commit register_project failed: {e}"))
            })
        })
        .await
    }

    pub(super) async fn is_project_registered_impl(&self, project_id: &str) -> StoreResult<bool> {
        let project_id = project_id.to_string();
        self.with_read_conn(move |conn| {
            let mut stmt = conn
                .prepare("SELECT 1 FROM projects WHERE project_id = ?1")
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite is_project_registered prepare failed: {e}"))
                })?;
            stmt.exists(params![project_id]).map_err(|e| {
                StoreError::Backend(format!("SQLite is_project_registered failed: {e}"))
            })
        })
        .await
    }

    pub(super) async fn get_repo_project_id_impl(
        &self,
        repo_id: &str,
    ) -> StoreResult<Option<String>> {
        let repo_id = repo_id.to_string();
        self.with_read_conn(move |conn| {
            conn.query_row(
                "SELECT project_id FROM repositories WHERE repo_id = ?1",
                params![repo_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| StoreError::Backend(format!("SQLite get_repo_project_id failed: {e}")))
        })
        .await
    }

    pub(super) async fn register_repo_in_project_impl(
        &self,
        repo_id: &str,
        project_id: &str,
        name: Option<&str>,
    ) -> StoreResult<String> {
        let repo_id = repo_id.to_string();
        let project_id = project_id.to_string();
        let name = name
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned);
        self.with_write_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|e| StoreError::Backend(format!("SQLite begin tx failed: {e}")))?;

            let existing_project: Option<String> = tx
                .query_row(
                    "SELECT project_id FROM repositories WHERE repo_id = ?1",
                    params![repo_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite existing repo lookup failed: {e}"))
                })?;
            if let Some(bound_proj) = &existing_project
                && bound_proj != &project_id
            {
                return Err(StoreError::RepoAlreadyExists(format!(
                    "repository '{repo_id}' is already bound to project '{bound_proj}', cannot rebind to '{project_id}'"
                )));
            }

            if repo_id != project_id {
                let shadowed_repo: Option<String> = tx
                    .query_row(
                        "SELECT project_id FROM repositories WHERE repo_id = ?1",
                        params![project_id],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(|e| {
                        StoreError::Backend(format!("SQLite project shadow lookup failed: {e}"))
                    })?;
                if let Some(bound_proj) = shadowed_repo
                    && bound_proj != project_id
                {
                    return Err(StoreError::RepoAlreadyExists(format!(
                        "project_id '{project_id}' conflicts with existing repository '{project_id}' bound to project '{bound_proj}'"
                    )));
                }
                let conflicting_proj: Option<i64> = tx
                    .query_row(
                        "SELECT 1 FROM projects WHERE project_id = ?1",
                        params![repo_id],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(|e| {
                        StoreError::Backend(format!("SQLite repo shadow lookup failed: {e}"))
                    })?;
                if conflicting_proj.is_some() {
                    return Err(StoreError::RepoAlreadyExists(format!(
                        "repository '{repo_id}' conflicts with existing project '{repo_id}'"
                    )));
                }
            }

            tx.execute(
                "INSERT OR IGNORE INTO projects (project_id, name) VALUES (?1, NULL)",
                params![project_id],
            )
            .map_err(|e| StoreError::Backend(format!("SQLite ensure project failed: {e}")))?;

            let initial_heads = encode_hex_id_list_json(
                &[cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                "op_heads",
            )?;
            tx.execute(
                "INSERT INTO repositories (repo_id, project_id, name, op_heads_json)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(repo_id) DO UPDATE SET name = COALESCE(excluded.name, repositories.name)",
                params![repo_id, project_id, name, initial_heads],
            )
            .map_err(|e| StoreError::Backend(format!("SQLite register_repo failed: {e}")))?;

            tx.commit().map_err(|e| {
                StoreError::Backend(format!("SQLite commit register_repo failed: {e}"))
            })?;
            Ok(project_id)
        })
        .await
    }

    pub(super) async fn delete_repository_impl(
        &self,
        repo_id: &str,
        expected_project_id: Option<&str>,
    ) -> StoreResult<bool> {
        let repo_id = repo_id.to_string();
        let expected_project_id = expected_project_id.map(ToOwned::to_owned);
        self.with_write_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite begin delete_repository tx failed: {e}"))
                })?;

            let existing_project: Option<String> = tx
                .query_row(
                    "SELECT project_id FROM repositories WHERE repo_id = ?1",
                    params![repo_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite delete_repository lookup failed: {e}"))
                })?;

            let Some(actual_project_id) = existing_project else {
                return Ok(false);
            };
            if let Some(expected) = &expected_project_id
                && actual_project_id != *expected
            {
                return Err(StoreError::FailedPrecondition(format!(
                    "repository '{repo_id}' belongs to project '{actual_project_id}', not '{expected}'"
                )));
            }

            for sql in [
                "DELETE FROM workspaces WHERE repo_id = ?1",
                "DELETE FROM operations WHERE repo_id = ?1",
                "DELETE FROM views WHERE repo_id = ?1",
                "DELETE FROM repositories WHERE repo_id = ?1",
            ] {
                tx.execute(sql, params![repo_id]).map_err(|e| {
                    StoreError::Backend(format!("SQLite delete_repository ({sql}) failed: {e}"))
                })?;
            }

            tx.commit().map_err(|e| {
                StoreError::Backend(format!("SQLite commit delete_repository failed: {e}"))
            })?;
            Ok(true)
        })
        .await
    }
}
