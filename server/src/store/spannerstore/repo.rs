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

//! Project and repository registration and lookup on Cloud Spanner.
//!
//! # Concurrency and Merge Invariants
//! Project and repository registration run in `Serializable` read-write
//! transactions that prevent repo/project identifier shadowing and retry races on
//! initial parent `Projects` creation without clobbering concurrent aliases.

use google_cloud_spanner::mutation::Mutation;
use google_cloud_spanner::statement::Statement;

use super::codec::{drain_first_row, is_already_exists, map_spanner_err};
use super::{SpannerMetadataStore, TxAction};
use crate::store::{StoreError, StoreResult};

impl SpannerMetadataStore {
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
