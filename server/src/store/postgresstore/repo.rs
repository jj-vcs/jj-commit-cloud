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

//! PostgreSQL project and repository registration and cascade deletion.
//!
//! # Concurrency and Merge Invariants
//! Serializes project and repository namespace transitions with sorted `pg_advisory_xact_lock`
//! keys to prevent cross-table shadowing races, locks `repositories` rows for update on delete,
//! and relies on `ON DELETE CASCADE` to clean up repository-scoped rows while keeping project
//! objects intact.

use tokio_postgres::error::SqlState;

use super::PostgresMetadataStore;
use super::codec::{map_pg_err, namespace_lock_key};
use crate::store::{StoreError, StoreResult};

impl PostgresMetadataStore {
    /// Atomically deletes `repo_id` and its repository-scoped state (`op_heads`, `views`,
    /// `operations`, `workspaces`) via `ON DELETE CASCADE`, while preserving shared project
    /// objects (`projects`, `commits`, `trees`, `symlinks`, `blobs`, `project_blobs`).
    ///
    /// Returns `Ok(false)` if `repo_id` does not exist. If `expected_project_id` is `Some`
    /// and does not match the repository's bound project, returns an error without mutating
    /// state.
    pub(super) async fn delete_repository_impl(
        &self,
        repo_id: &str,
        expected_project_id: Option<&str>,
    ) -> StoreResult<bool> {
        let repo_id = repo_id.to_string();
        let expected_project_id = expected_project_id.map(ToOwned::to_owned);
        let lock_key = namespace_lock_key(&repo_id);

        self.run_tx_with_retry(|tx| {
            let repo_id = repo_id.clone();
            let expected_project_id = expected_project_id.clone();
            Box::pin(async move {
                tx.execute("SELECT pg_advisory_xact_lock($1)", &[&lock_key])
                    .await?;
                let row = tx
                    .query_opt(
                        "SELECT project_id FROM repositories WHERE repo_id = $1 FOR UPDATE",
                        &[&repo_id],
                    )
                    .await?;
                let Some(row) = row else {
                    return Ok(Ok(false));
                };
                let bound_project: String = row.get(0);
                if let Some(expected) = &expected_project_id
                    && bound_project != *expected
                {
                    return Ok(Err(StoreError::FailedPrecondition(format!(
                        "repository '{repo_id}' belongs to project '{bound_project}', expected '{expected}'"
                    ))));
                }
                let deleted = tx
                    .execute("DELETE FROM repositories WHERE repo_id = $1", &[&repo_id])
                    .await?;
                Ok(Ok(deleted > 0))
            })
        })
        .await
    }

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
        let lock_key = namespace_lock_key(&project_id);

        self.run_tx_with_retry(|tx| {
            let project_id = project_id.clone();
            let name = name.clone();
            Box::pin(async move {
                tx.execute("SELECT pg_advisory_xact_lock($1)", &[&lock_key])
                    .await?;

                let shadowed = tx
                    .query_opt(
                        "SELECT project_id FROM repositories WHERE repo_id = $1",
                        &[&project_id],
                    )
                    .await?;
                if let Some(row) = shadowed {
                    let bound_proj: String = row.get(0);
                    if bound_proj != project_id {
                        return Ok(Err(StoreError::ProjectAlreadyExists(format!(
                            "project_id '{project_id}' conflicts with existing repository '{project_id}' bound to project '{bound_proj}'"
                        ))));
                    }
                }

                if let Some(alias) = &name {
                    let existing = tx
                        .query_opt("SELECT project_id FROM projects WHERE name = $1", &[alias])
                        .await?;
                    if let Some(row) = existing {
                        let other_id: String = row.get(0);
                        if other_id != project_id {
                            return Ok(Err(StoreError::ProjectAlreadyExists(alias.clone())));
                        }
                    }
                }

                match tx
                    .execute(
                        "INSERT INTO projects (project_id, name)
                         VALUES ($1, $2)
                         ON CONFLICT (project_id) DO UPDATE
                           SET name = COALESCE(EXCLUDED.name, projects.name)",
                        &[&project_id, &name],
                    )
                    .await
                {
                    Ok(_) => Ok(Ok(())),
                    Err(err) if err.code() == Some(&SqlState::UNIQUE_VIOLATION) => {
                        Ok(Err(StoreError::ProjectAlreadyExists(
                            name.unwrap_or_else(|| project_id.clone()),
                        )))
                    }
                    Err(err) => Err(err),
                }
            })
        })
        .await
    }

    pub(super) async fn is_project_registered_impl(&self, project_id: &str) -> StoreResult<bool> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(
                "SELECT 1 FROM projects WHERE project_id = $1",
                &[&project_id],
            )
            .await
            .map_err(map_pg_err)?;
        Ok(row.is_some())
    }

    pub(super) async fn get_repo_project_id_impl(
        &self,
        repo_id: &str,
    ) -> StoreResult<Option<String>> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(
                "SELECT project_id FROM repositories WHERE repo_id = $1",
                &[&repo_id],
            )
            .await
            .map_err(map_pg_err)?;
        Ok(row.map(|r| r.get(0)))
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

        let key_a = namespace_lock_key(&repo_id);
        let key_b = namespace_lock_key(&project_id);
        let (first_key, second_key) = if key_a <= key_b {
            (key_a, (key_a != key_b).then_some(key_b))
        } else {
            (key_b, Some(key_a))
        };

        self.run_tx_with_retry(|tx| {
            let repo_id = repo_id.clone();
            let project_id = project_id.clone();
            let name = name.clone();
            Box::pin(async move {
                tx.execute("SELECT pg_advisory_xact_lock($1)", &[&first_key])
                    .await?;
                if let Some(second) = second_key {
                    tx.execute("SELECT pg_advisory_xact_lock($1)", &[&second])
                        .await?;
                }

                let existing_repo = tx
                    .query_opt(
                        "SELECT project_id FROM repositories WHERE repo_id = $1 FOR NO KEY UPDATE",
                        &[&repo_id],
                    )
                    .await?;
                if let Some(row) = &existing_repo {
                    let bound_proj: String = row.get(0);
                    if bound_proj != project_id {
                        return Ok(Err(StoreError::RepoAlreadyExists(format!(
                            "repository '{repo_id}' is already bound to project '{bound_proj}', cannot rebind to '{project_id}'"
                        ))));
                    }
                }

                if repo_id != project_id {
                    let shadowed_repo = tx
                        .query_opt(
                            "SELECT project_id FROM repositories WHERE repo_id = $1",
                            &[&project_id],
                        )
                        .await?;
                    if let Some(row) = shadowed_repo {
                        let bound_proj: String = row.get(0);
                        if bound_proj != project_id {
                            return Ok(Err(StoreError::RepoAlreadyExists(format!(
                                "project_id '{project_id}' conflicts with existing repository '{project_id}' bound to project '{bound_proj}'"
                            ))));
                        }
                    }

                    let conflicting_proj = tx
                        .query_opt("SELECT 1 FROM projects WHERE project_id = $1", &[&repo_id])
                        .await?;
                    if conflicting_proj.is_some() {
                        return Ok(Err(StoreError::RepoAlreadyExists(format!(
                            "repository '{repo_id}' conflicts with existing project '{repo_id}'"
                        ))));
                    }
                }

                tx.execute(
                    "INSERT INTO projects (project_id, name) VALUES ($1, NULL)
                     ON CONFLICT (project_id) DO NOTHING",
                    &[&project_id],
                )
                .await?;

                tx.execute(
                    "INSERT INTO repositories (repo_id, project_id, name)
                     VALUES ($1, $2, $3)
                     ON CONFLICT (repo_id) DO UPDATE
                       SET name = COALESCE(EXCLUDED.name, repositories.name)",
                    &[&repo_id, &project_id, &name],
                )
                .await?;

                if existing_repo.is_none() {
                    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.as_slice();
                    tx.execute(
                        "INSERT INTO op_heads (repo_id, op_id) VALUES ($1, $2)
                         ON CONFLICT (repo_id, op_id) DO NOTHING",
                        &[&repo_id, &root_op],
                    )
                    .await?;
                }

                Ok(Ok(project_id))
            })
        })
        .await
    }
}
