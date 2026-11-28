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

//! Repository-scoped workspace state CRUD, CAS updates, and pagination on Spanner.
//!
//! # Concurrency and Merge Invariants
//! Compare-and-swap workspace updates run in `Serializable` read-write transactions
//! using insert-only mutations for new rows and transactional DML deletes so
//! concurrent creates and updates surface as `StoreError::CasConflict`.

use cc_common::workspace::WorkspaceState;
use google_cloud_gax::error::rpc::Code as RpcCode;
use google_cloud_spanner::mutation::Mutation;
use google_cloud_spanner::statement::Statement;

use super::codec::{decode_workspace_row, drain_first_row, is_already_exists, map_spanner_err};
use super::{SpannerMetadataStore, TxAction};
use crate::store::{StoreError, StoreResult, verify_optional_repo_project};

impl SpannerMetadataStore {
    #[allow(deprecated)]
    pub(super) async fn put_workspace_cas_inner(
        &self,
        expected_project_id: Option<&str>,
        workspace: &WorkspaceState,
        expected_operation_id: Option<&[u8]>,
        require_operation_exists: bool,
    ) -> StoreResult<()> {
        let tree_ids = if workspace.tree_ids.is_empty() && !workspace.tree_id.is_empty() {
            vec![workspace.tree_id.clone()]
        } else {
            workspace.tree_ids.clone()
        };
        let build_workspace_mutation = |is_insert: bool| {
            let builder = if is_insert {
                Mutation::new_insert_builder("Workspaces")
            } else {
                Mutation::new_insert_or_update_builder("Workspaces")
            };
            builder
                .set("RepoId")
                .to(&workspace.repo_id)
                .set("UserEmail")
                .to(&workspace.user)
                .set("WorkspaceName")
                .to(&workspace.workspace_name)
                .set("CommitId")
                .to(&workspace.commit_id)
                .set("OperationId")
                .to(&workspace.operation_id)
                .set("TreeId")
                .to(&workspace.tree_id)
                .set("TreeIds")
                .to(&tree_ids)
                .set("ConflictLabels")
                .to(&workspace.conflict_labels)
                .set("SparsePatterns")
                .to(&workspace.sparse_patterns)
                .build()
        };

        let insert_mutation = build_workspace_mutation(true);
        let upsert_mutation = build_workspace_mutation(false);
        let ws = workspace.clone();
        let expected_proj = expected_project_id.map(ToOwned::to_owned);
        let exp_owned = expected_operation_id.map(|b| b.to_vec());
        let is_cas = exp_owned.is_some();
        let required_op_id = (require_operation_exists
            && !ws.operation_id.is_empty()
            && ws.operation_id.as_slice() != cc_common::ROOT_OPERATION_ID_BYTES)
            .then(|| ws.operation_id.clone());

        self.run_rw_tx(async move |tx| {
            let repo_stmt =
                Statement::builder("SELECT ProjectId FROM Repositories WHERE RepoId = @repo_id")
                    .add_param("repo_id", &ws.repo_id)
                    .build();
            let repo_rs = tx.execute_query(repo_stmt).await?;
            let actual_project_id: Option<String> =
                if let Some(row) = drain_first_row(repo_rs).await? {
                    Some(row.get("ProjectId")?)
                } else {
                    None
                };
            if let Err(err) = verify_optional_repo_project(
                &ws.repo_id,
                actual_project_id.as_deref(),
                expected_proj.as_deref(),
                "updating workspace",
            ) {
                return Ok(TxAction::Rollback(Err(err)));
            }

            if let Some(op_id) = &required_op_id {
                let op_stmt = Statement::builder(
                    "SELECT 1 FROM Operations
                     WHERE RepoId = @repo_id AND OperationId = @op_id",
                )
                .add_param("repo_id", &ws.repo_id)
                .add_param("op_id", op_id)
                .build();
                let op_rs = tx.execute_query(op_stmt).await?;
                if drain_first_row(op_rs).await?.is_none() {
                    return Ok(TxAction::Rollback(Err(StoreError::MissingDependency {
                        parent_kind: "workspace",
                        missing_kind: "operation",
                        id_hex: hex::encode(op_id),
                    })));
                }
            }

            let Some(exp_slice) = exp_owned.as_deref() else {
                tx.buffer([upsert_mutation.clone()])?;
                return Ok(TxAction::Commit(()));
            };

            let stmt = Statement::builder(
                "SELECT CommitId, OperationId, TreeId, TreeIds, ConflictLabels, SparsePatterns
                 FROM Workspaces
                 WHERE RepoId = @repo_id AND UserEmail = @user AND WorkspaceName = @ws_name",
            )
            .add_param("repo_id", &ws.repo_id)
            .add_param("user", &ws.user)
            .add_param("ws_name", &ws.workspace_name)
            .build();
            let rs = tx.execute_query(stmt).await?;
            let existing = match drain_first_row(rs).await? {
                Some(row) => {
                    let commit_id: Vec<u8> = row.get("CommitId")?;
                    let operation_id: Vec<u8> = row.get("OperationId")?;
                    let tree_id: Vec<u8> = row.get("TreeId")?;
                    let mut row_tree_ids: Vec<Vec<u8>> = row.get("TreeIds")?;
                    if row_tree_ids.is_empty() && !tree_id.is_empty() {
                        row_tree_ids.push(tree_id.clone());
                    }
                    let conflict_labels: Vec<String> = row.get("ConflictLabels")?;
                    let sparse_patterns: Vec<String> = row.get("SparsePatterns")?;
                    Some(WorkspaceState {
                        repo_id: ws.repo_id.clone(),
                        user: ws.user.clone(),
                        workspace_name: ws.workspace_name.clone(),
                        commit_id,
                        operation_id,
                        tree_id,
                        sparse_patterns,
                        tree_ids: row_tree_ids,
                        conflict_labels,
                        sparse_patterns_set: true,
                    })
                }
                None => None,
            };
            match crate::store::check_workspace_cas_precondition(
                existing.as_ref(),
                &ws,
                Some(exp_slice),
            ) {
                Ok(true) => return Ok(TxAction::Rollback(Ok(()))),
                Ok(false) => {}
                Err(err) => return Ok(TxAction::Rollback(Err(err))),
            }
            let mutation = if existing.is_none() {
                insert_mutation.clone()
            } else {
                upsert_mutation.clone()
            };
            tx.buffer([mutation])?;
            Ok(TxAction::Commit(()))
        })
        .await
        .map_err(|err| {
            if is_cas
                && (is_already_exists(&err)
                    || err.status().is_some_and(|s| s.code == RpcCode::Aborted))
            {
                StoreError::CasConflict(err.to_string())
            } else {
                map_spanner_err(err)
            }
        })?
    }

    pub(super) async fn get_workspace_impl(
        &self,
        repo_id: &str,
        user: &str,
        workspace_name: &str,
    ) -> StoreResult<Option<WorkspaceState>> {
        let stmt = Statement::builder(
            "SELECT CommitId, OperationId, TreeId, TreeIds, ConflictLabels, SparsePatterns
             FROM Workspaces
             WHERE RepoId = @repo_id AND UserEmail = @user AND WorkspaceName = @ws",
        )
        .add_param("repo_id", repo_id)
        .add_param("user", user)
        .add_param("ws", workspace_name)
        .build();

        let tx = self.db_client.single_use().build();
        let mut rs = tx.execute_query(stmt).await.map_err(map_spanner_err)?;
        let Some(row) = rs.next().await.transpose().map_err(map_spanner_err)? else {
            return Ok(None);
        };
        let commit_id: Vec<u8> = row.get("CommitId").map_err(map_spanner_err)?;
        let operation_id: Vec<u8> = row.get("OperationId").map_err(map_spanner_err)?;
        let tree_id: Vec<u8> = row.get("TreeId").map_err(map_spanner_err)?;
        let mut tree_ids: Vec<Vec<u8>> = row.get("TreeIds").map_err(map_spanner_err)?;
        if tree_ids.is_empty() && !tree_id.is_empty() {
            tree_ids.push(tree_id.clone());
        }
        let conflict_labels: Vec<String> = row.get("ConflictLabels").map_err(map_spanner_err)?;
        let sparse_patterns: Vec<String> = row.get("SparsePatterns").map_err(map_spanner_err)?;

        #[allow(deprecated)]
        let ws = WorkspaceState {
            repo_id: repo_id.to_string(),
            user: user.to_string(),
            workspace_name: workspace_name.to_string(),
            commit_id,
            operation_id,
            tree_id,
            sparse_patterns,
            tree_ids,
            conflict_labels,
            sparse_patterns_set: true,
        };
        Ok(Some(ws))
    }

    pub(super) async fn list_workspaces_impl(
        &self,
        repo_id: &str,
    ) -> StoreResult<Vec<WorkspaceState>> {
        let stmt = Statement::builder(
            "SELECT UserEmail, WorkspaceName, CommitId, OperationId, TreeId, TreeIds, ConflictLabels, SparsePatterns
             FROM Workspaces
             WHERE RepoId = @repo_id
             ORDER BY UserEmail ASC, WorkspaceName ASC",
        )
        .add_param("repo_id", repo_id)
        .build();

        let tx = self.db_client.single_use().build();
        let mut rs = tx.execute_query(stmt).await.map_err(map_spanner_err)?;
        let mut out = Vec::new();
        while let Some(row) = rs.next().await.transpose().map_err(map_spanner_err)? {
            out.push(decode_workspace_row(repo_id, &row)?);
        }
        Ok(out)
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
        let stmt = match after {
            Some((after_user, after_ws)) => Statement::builder(
                "SELECT UserEmail, WorkspaceName, CommitId, OperationId, TreeId, TreeIds, ConflictLabels, SparsePatterns
                 FROM Workspaces
                 WHERE RepoId = @repo_id
                   AND (
                     UserEmail > @after_user
                     OR (UserEmail = @after_user AND WorkspaceName > @after_ws)
                   )
                 ORDER BY UserEmail ASC, WorkspaceName ASC
                 LIMIT @limit",
            )
            .add_param("repo_id", repo_id)
            .add_param("after_user", after_user)
            .add_param("after_ws", after_ws)
            .add_param("limit", fetch_limit)
            .build(),
            None => Statement::builder(
                "SELECT UserEmail, WorkspaceName, CommitId, OperationId, TreeId, TreeIds, ConflictLabels, SparsePatterns
                 FROM Workspaces
                 WHERE RepoId = @repo_id
                 ORDER BY UserEmail ASC, WorkspaceName ASC
                 LIMIT @limit",
            )
            .add_param("repo_id", repo_id)
            .add_param("limit", fetch_limit)
            .build(),
        };

        let tx = self.db_client.single_use().build();
        let mut rs = tx.execute_query(stmt).await.map_err(map_spanner_err)?;
        let mut out = Vec::new();
        let mut response_bytes = 0usize;
        let mut has_more = false;
        while let Some(row) = rs.next().await.transpose().map_err(map_spanner_err)? {
            let ws = decode_workspace_row(repo_id, &row)?;
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

    pub(super) async fn delete_workspace_impl(
        &self,
        repo_id: &str,
        user: &str,
        workspace_name: &str,
    ) -> StoreResult<bool> {
        let repo_id = repo_id.to_string();
        let user = user.to_string();
        let workspace_name = workspace_name.to_string();

        self.run_rw_tx(async move |tx| {
            let stmt = Statement::builder(
                "DELETE FROM Workspaces
                 WHERE RepoId = @repo_id AND UserEmail = @user AND WorkspaceName = @ws",
            )
            .add_param("repo_id", &repo_id)
            .add_param("user", &user)
            .add_param("ws", &workspace_name)
            .build();
            let rows_deleted = tx.execute_update(stmt).await?;
            if rows_deleted > 0 {
                Ok(TxAction::Commit(true))
            } else {
                Ok(TxAction::Rollback(Ok(false)))
            }
        })
        .await
        .map_err(map_spanner_err)?
    }
}
