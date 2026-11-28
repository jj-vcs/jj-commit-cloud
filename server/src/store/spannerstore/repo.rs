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

//! Project and repository registration, lookup, and deletion on Cloud Spanner.
//!
//! # Concurrency and Merge Invariants
//! Project and repository registration run in `Serializable` read-write
//! transactions that prevent repo/project identifier shadowing and retry races on
//! initial parent `Projects` creation without clobbering concurrent aliases.
//! Repository deletion removes `Workspaces`, `Operations`, `Views`, and
//! `Repositories` atomically when within the mutation budget and drains in bounded
//! `Serializable` batches while verifying `Repositories.ProjectId` on each batch.

use google_cloud_spanner::key;
use google_cloud_spanner::key::KeySet;
use google_cloud_spanner::mutation::Mutation;
use google_cloud_spanner::statement::Statement;

use super::codec::{
    build_binary_child_delete_mutation, build_workspace_delete_mutation, drain_first_row,
    is_already_exists, map_spanner_err,
};
use super::{SpannerMetadataStore, TxAction};
use crate::store::{StoreError, StoreResult};

/// Maximum child rows deleted per Spanner read-write transaction in
/// [`SpannerMetadataStore::delete_repository`], keeping each transaction well below
/// Cloud Spanner's 80,000-mutation limit.
pub const SPANNER_DELETE_BATCH_LIMIT: i64 = 5_000;

/// Defensive upper bound on read-write delete batch transactions issued by a single
/// [`SpannerMetadataStore::delete_repository`] call so sustained concurrent writes cannot
/// prevent termination.
pub const MAX_DELETE_BATCH_ITERATIONS: usize = 1_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DeleteRepoProjectCheck<'a> {
    RepoGone,
    Proceed(&'a str),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum DeleteBatchStep {
    RepoGone,
    Deleted,
    MoreBatches { pinned_project_id: String },
}

pub(super) fn clamp_delete_batch_limit(limit: i64) -> i64 {
    limit.clamp(1, SPANNER_DELETE_BATCH_LIMIT)
}

pub(super) fn clamp_max_delete_batch_iterations(limit: usize) -> usize {
    limit.clamp(1, MAX_DELETE_BATCH_ITERATIONS)
}

/// Validates the repository's current `ProjectId` during a `delete_repository` transaction.
///
/// - Absent repository -> `RepoGone` (idempotent `Ok(false)`).
/// - Explicit `expected_project_id` mismatch -> `FailedPrecondition`.
/// - Unconditional delete (`expected_project_id == None`) that observes a different `ProjectId`
///   than the one pinned on batch 0 -> `RepoGone` (`Ok(false)`), because the original repository
///   instance was already deleted and replaced under another project.
pub(super) fn evaluate_delete_repo_project<'a>(
    repo_id: &str,
    actual_project_id: Option<&'a str>,
    expected_project_id: Option<&str>,
    pinned_project_id: Option<&str>,
) -> StoreResult<DeleteRepoProjectCheck<'a>> {
    let Some(actual) = actual_project_id else {
        return Ok(DeleteRepoProjectCheck::RepoGone);
    };
    if let Some(expected) = expected_project_id {
        if actual != expected {
            return Err(StoreError::FailedPrecondition(format!(
                "repository '{repo_id}' belongs to project '{actual}', not '{expected}'"
            )));
        }
        return Ok(DeleteRepoProjectCheck::Proceed(actual));
    }
    if let Some(pinned) = pinned_project_id
        && actual != pinned
    {
        return Ok(DeleteRepoProjectCheck::RepoGone);
    }
    Ok(DeleteRepoProjectCheck::Proceed(actual))
}

impl SpannerMetadataStore {
    /// Deletes `repo_id` and all repository-scoped rows (`Workspaces`, `Operations`, `Views`)
    /// in a single `Serializable` transaction when total child rows fit within
    /// `self.delete_batch_limit` (`5,000` by default).
    ///
    /// For very large repositories exceeding `self.delete_batch_limit`, drains child rows across
    /// bounded `Serializable` batches while verifying `Repositories.ProjectId` on every batch.
    /// Multi-batch caveats (tracked for a follow-up schema migration adding a `Deleting` tombstone
    /// state or monotonic repository generation column):
    /// 1. Concurrent readers between intermediate batches can observe partially deleted child rows
    ///    while `Repositories` still exists.
    /// 2. If another caller completes deletion between batches and a client immediately
    ///    re-registers the same `repo_id` under the *same* `project_id`, `pinned_project_id` still
    ///    matches and subsequent batches continue deleting the newly registered repository
    ///    (cross-project re-registration is detected and aborted).
    #[doc(hidden)]
    pub async fn delete_repository_with_after_batch_hook<F, Fut>(
        &self,
        repo_id: &str,
        expected_project_id: Option<&str>,
        mut after_batch: F,
    ) -> StoreResult<bool>
    where
        F: FnMut(usize) -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let batch_limit = clamp_delete_batch_limit(self.delete_batch_limit);
        let batch_limit_usize = usize::try_from(batch_limit).unwrap_or(1);
        let max_iterations = clamp_max_delete_batch_iterations(self.max_delete_batch_iterations);
        let mut pinned_project_id: Option<String> = None;

        for batch_idx in 0..max_iterations {
            let repo_id_owned = repo_id.to_string();
            let expected_proj_owned = expected_project_id.map(ToOwned::to_owned);
            let pinned_proj_owned = pinned_project_id.clone();

            let step = self
                .run_rw_tx(async move |tx| {
                    let check_stmt = Statement::builder(
                        "SELECT ProjectId FROM Repositories WHERE RepoId = @repo_id",
                    )
                    .add_param("repo_id", &repo_id_owned)
                    .build();
                    let check_rs = tx.execute_query(check_stmt).await?;
                    let actual_project_id: Option<String> =
                        if let Some(row) = drain_first_row(check_rs).await? {
                            Some(row.get("ProjectId")?)
                        } else {
                            None
                        };

                    let active_project_id = match evaluate_delete_repo_project(
                        &repo_id_owned,
                        actual_project_id.as_deref(),
                        expected_proj_owned.as_deref(),
                        pinned_proj_owned.as_deref(),
                    ) {
                        Ok(DeleteRepoProjectCheck::RepoGone) => {
                            return Ok(TxAction::Rollback(Ok(DeleteBatchStep::RepoGone)));
                        }
                        Ok(DeleteRepoProjectCheck::Proceed(proj)) => proj.to_string(),
                        Err(err) => return Ok(TxAction::Rollback(Err(err))),
                    };

                    // Probe `Workspaces`, `Operations`, and `Views` up to `batch_limit + 1`
                    // total rows. When all three tables fit within `batch_limit`, delete all
                    // children and `Repositories` atomically in this single transaction;
                    // otherwise delete `batch_limit` child rows and continue draining.
                    let ws_stmt = Statement::builder(
                        "SELECT WorkspaceName FROM Workspaces
                         WHERE RepoId = @repo_id
                         LIMIT @limit",
                    )
                    .add_param("repo_id", &repo_id_owned)
                    .add_param("limit", batch_limit + 1)
                    .build();
                    let mut ws_rs = tx.execute_query(ws_stmt).await?;
                    let mut ws_keys = Vec::new();
                    while let Some(row) = ws_rs.next().await.transpose()? {
                        let ws_name: String = row.get("WorkspaceName")?;
                        ws_keys.push(ws_name);
                    }
                    if ws_keys.len() > batch_limit_usize {
                        ws_keys.truncate(batch_limit_usize);
                        tx.buffer([build_workspace_delete_mutation(&repo_id_owned, &ws_keys)])?;
                        return Ok(TxAction::Commit(DeleteBatchStep::MoreBatches {
                            pinned_project_id: active_project_id,
                        }));
                    }

                    let rem_after_ws = batch_limit_usize - ws_keys.len();
                    let op_stmt = Statement::builder(
                        "SELECT OperationId FROM Operations
                         WHERE RepoId = @repo_id
                         LIMIT @limit",
                    )
                    .add_param("repo_id", &repo_id_owned)
                    .add_param("limit", (rem_after_ws as i64) + 1)
                    .build();
                    let mut op_rs = tx.execute_query(op_stmt).await?;
                    let mut op_keys = Vec::new();
                    while let Some(row) = op_rs.next().await.transpose()? {
                        let op_id: Vec<u8> = row.get("OperationId")?;
                        op_keys.push(op_id);
                    }
                    if op_keys.len() > rem_after_ws {
                        op_keys.truncate(rem_after_ws);
                        let mut mutations = Vec::with_capacity(2);
                        if !ws_keys.is_empty() {
                            mutations
                                .push(build_workspace_delete_mutation(&repo_id_owned, &ws_keys));
                        }
                        if !op_keys.is_empty() {
                            mutations.push(build_binary_child_delete_mutation(
                                "Operations",
                                &repo_id_owned,
                                &op_keys,
                            ));
                        }
                        tx.buffer(mutations)?;
                        return Ok(TxAction::Commit(DeleteBatchStep::MoreBatches {
                            pinned_project_id: active_project_id,
                        }));
                    }

                    let rem_after_ops = rem_after_ws - op_keys.len();
                    let view_stmt = Statement::builder(
                        "SELECT ViewId FROM Views
                         WHERE RepoId = @repo_id
                         LIMIT @limit",
                    )
                    .add_param("repo_id", &repo_id_owned)
                    .add_param("limit", (rem_after_ops as i64) + 1)
                    .build();
                    let mut view_rs = tx.execute_query(view_stmt).await?;
                    let mut view_keys = Vec::new();
                    while let Some(row) = view_rs.next().await.transpose()? {
                        let view_id: Vec<u8> = row.get("ViewId")?;
                        view_keys.push(view_id);
                    }
                    if view_keys.len() > rem_after_ops {
                        view_keys.truncate(rem_after_ops);
                        let mut mutations = Vec::with_capacity(3);
                        if !ws_keys.is_empty() {
                            mutations
                                .push(build_workspace_delete_mutation(&repo_id_owned, &ws_keys));
                        }
                        if !op_keys.is_empty() {
                            mutations.push(build_binary_child_delete_mutation(
                                "Operations",
                                &repo_id_owned,
                                &op_keys,
                            ));
                        }
                        if !view_keys.is_empty() {
                            mutations.push(build_binary_child_delete_mutation(
                                "Views",
                                &repo_id_owned,
                                &view_keys,
                            ));
                        }
                        tx.buffer(mutations)?;
                        return Ok(TxAction::Commit(DeleteBatchStep::MoreBatches {
                            pinned_project_id: active_project_id,
                        }));
                    }

                    // All child rows fit within `batch_limit`: delete top-level `Operations`
                    // and `Views` alongside `Repositories` (whose `ON DELETE CASCADE` removes
                    // interleaved `Workspaces`) in this single Serializable transaction.
                    let mut mutations = Vec::with_capacity(3);
                    if !op_keys.is_empty() {
                        mutations.push(build_binary_child_delete_mutation(
                            "Operations",
                            &repo_id_owned,
                            &op_keys,
                        ));
                    }
                    if !view_keys.is_empty() {
                        mutations.push(build_binary_child_delete_mutation(
                            "Views",
                            &repo_id_owned,
                            &view_keys,
                        ));
                    }
                    let repo_keys = KeySet::builder()
                        .add_key(key![repo_id_owned.as_str()])
                        .build();
                    mutations.push(Mutation::delete("Repositories", repo_keys));
                    tx.buffer(mutations)?;
                    Ok(TxAction::Commit(DeleteBatchStep::Deleted))
                })
                .await
                .map_err(map_spanner_err)??;

            match step {
                DeleteBatchStep::RepoGone => return Ok(false),
                DeleteBatchStep::Deleted => return Ok(true),
                DeleteBatchStep::MoreBatches {
                    pinned_project_id: observed,
                } => {
                    pinned_project_id.get_or_insert(observed);
                    after_batch(batch_idx).await;
                }
            }
        }

        Err(StoreError::Unavailable(format!(
            "deleting repository '{repo_id}' exceeded maximum of {max_iterations} batches; retry the idempotent delete"
        )))
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
        let project_id_for_err = project_id.clone();
        let alias_for_err = name.clone();

        self.run_rw_tx(async move |tx| {
            let repo_shadow_stmt = Statement::builder(
                "SELECT ProjectId FROM Repositories WHERE RepoId = @project_id",
            )
            .add_param("project_id", &project_id)
            .build();
            let repo_shadow_rs = tx.execute_query(repo_shadow_stmt).await?;
            if let Some(row) = drain_first_row(repo_shadow_rs).await? {
                let bound_project: String = row.get("ProjectId")?;
                if bound_project != project_id {
                    return Ok(TxAction::Rollback(Err(StoreError::ProjectAlreadyExists(
                        format!(
                            "project_id '{project_id}' conflicts with existing repository '{project_id}' bound to project '{bound_project}'"
                        ),
                    ))));
                }
            }

            if let Some(alias) = &name {
                let alias_stmt =
                    Statement::builder("SELECT ProjectId FROM Projects WHERE Name = @name")
                        .add_param("name", alias)
                        .build();
                let alias_rs = tx.execute_query(alias_stmt).await?;
                if let Some(row) = drain_first_row(alias_rs).await? {
                    let existing_id: String = row.get("ProjectId")?;
                    if existing_id != project_id {
                        return Ok(TxAction::Rollback(Err(StoreError::ProjectAlreadyExists(
                            alias.clone(),
                        ))));
                    }
                }
            }

            let check_stmt =
                Statement::builder("SELECT Name FROM Projects WHERE ProjectId = @project_id")
                    .add_param("project_id", &project_id)
                    .build();
            let check_rs = tx.execute_query(check_stmt).await?;
            let effective_name: Option<String> =
                if let Some(row) = drain_first_row(check_rs).await? {
                    let prev_name: Option<String> = row.get("Name")?;
                    name.clone().or(prev_name)
                } else {
                    name.clone()
                };

            let mutation = Mutation::new_insert_or_update_builder("Projects")
                .set("ProjectId")
                .to(&project_id)
                .set("Name")
                .to(&effective_name)
                .build();
            tx.buffer([mutation])?;
            Ok(TxAction::Commit(()))
        })
        .await
        .map_err(|err| {
            if is_already_exists(&err) {
                StoreError::ProjectAlreadyExists(alias_for_err.unwrap_or(project_id_for_err))
            } else {
                map_spanner_err(err)
            }
        })?
    }

    pub(super) async fn is_project_registered_impl(&self, project_id: &str) -> StoreResult<bool> {
        let stmt =
            Statement::builder("SELECT 1 FROM Projects WHERE ProjectId = @project_id LIMIT 1")
                .add_param("project_id", project_id)
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

    pub(super) async fn get_repo_project_id_impl(
        &self,
        repo_id: &str,
    ) -> StoreResult<Option<String>> {
        let stmt = Statement::builder("SELECT ProjectId FROM Repositories WHERE RepoId = @repo_id")
            .add_param("repo_id", repo_id)
            .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx.execute_query(stmt).await.map_err(map_spanner_err)?;
        let Some(row) = rs.next().await.transpose().map_err(map_spanner_err)? else {
            return Ok(None);
        };
        let project_id: String = row.get("ProjectId").map_err(map_spanner_err)?;
        Ok(Some(project_id))
    }

    pub(super) async fn register_repo_in_project_impl(
        &self,
        repo_id: &str,
        project_id: &str,
        name: Option<&str>,
    ) -> StoreResult<String> {
        const MAX_PROJECT_INSERT_RACE_RETRIES: usize = 3;

        for attempt in 0..MAX_PROJECT_INSERT_RACE_RETRIES {
            let repo_id = repo_id.to_string();
            let project_id = project_id.to_string();
            let name = name
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToOwned::to_owned);

            let res = self
                .run_rw_tx(async move |tx| {
                    let check_stmt = Statement::builder(
                        "SELECT ProjectId, Name, OpHeads FROM Repositories WHERE RepoId = @repo_id",
                    )
                    .add_param("repo_id", &repo_id)
                    .build();
                    let check_rs = tx.execute_query(check_stmt).await?;

                    let (effective_name, op_heads): (Option<String>, Vec<Vec<u8>>) =
                        if let Some(row) = drain_first_row(check_rs).await? {
                            let bound_project: String = row.get("ProjectId")?;
                            if bound_project != project_id {
                                return Ok(TxAction::Rollback(Err(StoreError::RepoAlreadyExists(
                                    format!(
                                        "repository '{repo_id}' is already bound to project '{bound_project}', cannot rebind to '{project_id}'"
                                    ),
                                ))));
                            }
                            let prev_name: Option<String> = row.get("Name")?;
                            let prev_heads: Vec<Vec<u8>> = row.get("OpHeads")?;
                            (name.clone().or(prev_name), prev_heads)
                        } else {
                            (
                                name.clone(),
                                vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                            )
                        };

                    if repo_id != project_id {
                        let proj_shadow_stmt = Statement::builder(
                            "SELECT ProjectId FROM Repositories WHERE RepoId = @project_id",
                        )
                        .add_param("project_id", &project_id)
                        .build();
                        let proj_shadow_rs = tx.execute_query(proj_shadow_stmt).await?;
                        if let Some(row) = drain_first_row(proj_shadow_rs).await? {
                            let bound_project: String = row.get("ProjectId")?;
                            if bound_project != project_id {
                                return Ok(TxAction::Rollback(Err(StoreError::RepoAlreadyExists(
                                    format!(
                                        "project_id '{project_id}' conflicts with existing repository '{project_id}' bound to project '{bound_project}'"
                                    ),
                                ))));
                            }
                        }

                        let repo_shadow_stmt = Statement::builder(
                            "SELECT 1 FROM Projects WHERE ProjectId = @repo_id",
                        )
                        .add_param("repo_id", &repo_id)
                        .build();
                        let repo_shadow_rs = tx.execute_query(repo_shadow_stmt).await?;
                        if drain_first_row(repo_shadow_rs).await?.is_some() {
                            return Ok(TxAction::Rollback(Err(StoreError::RepoAlreadyExists(
                                format!(
                                    "repository '{repo_id}' conflicts with existing project '{repo_id}'"
                                ),
                            ))));
                        }
                    }

                    let proj_stmt = Statement::builder(
                        "SELECT 1 FROM Projects WHERE ProjectId = @project_id",
                    )
                    .add_param("project_id", &project_id)
                    .build();
                    let proj_rs = tx.execute_query(proj_stmt).await?;
                    let mut mutations = Vec::with_capacity(2);
                    if drain_first_row(proj_rs).await?.is_none() {
                        let none_name: Option<String> = None;
                        mutations.push(
                            Mutation::new_insert_builder("Projects")
                                .set("ProjectId")
                                .to(&project_id)
                                .set("Name")
                                .to(&none_name)
                                .build(),
                        );
                    }

                    mutations.push(
                        Mutation::new_insert_or_update_builder("Repositories")
                            .set("RepoId")
                            .to(&repo_id)
                            .set("ProjectId")
                            .to(&project_id)
                            .set("Name")
                            .to(&effective_name)
                            .set("OpHeads")
                            .to(&op_heads)
                            .build(),
                    );
                    tx.buffer(mutations)?;
                    Ok(TxAction::Commit(project_id.clone()))
                })
                .await;

            match res {
                Ok(outcome) => return outcome,
                // Two concurrent registrations into a brand-new `project_id` can race on
                // inserting the parent `Projects` row; retrying observes the committed row
                // without overwriting any concurrent `Projects.Name`.
                Err(err)
                    if is_already_exists(&err) && attempt + 1 < MAX_PROJECT_INSERT_RACE_RETRIES =>
                {
                    continue;
                }
                Err(err) => return Err(map_spanner_err(err)),
            }
        }
        unreachable!("loop returns on final attempt")
    }
}
