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

//! Cloud Spanner [`Store`] backend.
//!
//! # Authority and Crash-Safety Ordering
//! `connect()` applies `db/schema_spanner.sql` DDL and polls the resulting LRO before opening
//! `DatabaseClient` queries. Workspace writes encode the full `WorkspaceState` in `proto_data`
//! alongside scalar columns and enforce `expected_operation_id` CAS inside a `Serializable`
//! Spanner `ReadWriteTransaction` (using `Insert` for new rows and `InsertOrUpdate` for updates),
//! rolling back on application errors or mapping `ABORTED`/`ALREADY_EXISTS` to
//! `StoreError::CasConflict`. `delete_workspace` runs transactional DML so `existed` is atomic
//! and corrupt rows remain deletable.

use async_trait::async_trait;
use cc_common::backend::*;
use cc_common::op_store::*;
use cc_common::workspace::WorkspaceState;
use google_cloud_spanner::client::{DatabaseClient, Spanner};
use google_cloud_spanner::key::{Key, KeySet};
use google_cloud_spanner::mutation::Mutation;
use google_cloud_spanner::statement::Statement;
use prost::Message;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{info, warn};

use super::{CommitId, Store, StoreError, StoreResult};

#[derive(Clone)]
pub struct SpannerStore {
    db_client: Arc<DatabaseClient>,
}

#[derive(prost::Message)]
struct TreeEntryList {
    #[prost(message, repeated, tag = "1")]
    pub entries: Vec<TreeEntry>,
}

impl SpannerStore {
    pub async fn connect(database_name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let spanner = Spanner::builder()
            .build()
            .await
            .map_err(|e| format!("Failed to build Spanner client: {e}"))?;

        if let Ok(admin_client) = spanner.database_admin_builder().build().await {
            let schema_str = include_str!("../../db/schema_spanner.sql");
            let ddl_statements: Vec<String> = schema_str
                .split(';')
                .map(|s| {
                    s.lines()
                        .filter(|line| !line.trim().starts_with("--"))
                        .collect::<Vec<_>>()
                        .join("\n")
                })
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();

            if !ddl_statements.is_empty() {
                info!(
                    "Ensuring Spanner database schema tables exist for '{}'...",
                    database_name
                );
                let res = admin_client
                    .update_database_ddl()
                    .set_database(database_name)
                    .set_statements(ddl_statements)
                    .send()
                    .await;
                match res {
                    Ok(mut op) => {
                        let op_name = op.name.clone();
                        let deadline =
                            tokio::time::Instant::now() + std::time::Duration::from_secs(60);
                        while !op.done && tokio::time::Instant::now() < deadline {
                            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                            match admin_client.get_operation().set_name(&op_name).send().await {
                                Ok(updated) => op = updated,
                                Err(e) => {
                                    warn!("Spanner DDL LRO poll notice: {e}");
                                    break;
                                }
                            }
                        }
                        if !op.done {
                            warn!("Spanner DDL LRO '{}' did not complete within 60s", op_name);
                        } else if let Some(err) = op.error() {
                            warn!(
                                "Spanner DDL LRO '{}' completed with error: {:?}",
                                op_name, err
                            );
                        }
                    }
                    Err(e) => {
                        warn!("Spanner auto-schema initialization notice: {e}");
                    }
                }
            }
        }

        let db_client = spanner
            .database_client(database_name)
            .build()
            .await
            .map_err(|e| format!("Failed to connect to Spanner database: {e}"))?;

        Ok(Self {
            db_client: Arc::new(db_client),
        })
    }
}

#[async_trait]
impl Store for SpannerStore {
    async fn is_project_registered(&self, project_id: &str) -> StoreResult<bool> {
        let stmt = Statement::builder("SELECT 1 FROM projects WHERE project_id = @project_id")
            .add_param("project_id", project_id)
            .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        Ok(rs.next().await.is_some())
    }

    async fn register_project(&self, project_id: String, name: Option<String>) -> StoreResult<()> {
        let mut builder = Mutation::new_insert_or_update_builder("projects")
            .set("project_id")
            .to(&project_id);
        if let Some(n) = name {
            builder = builder.set("name").to(&n);
        }
        let mutation = builder.build();
        self.db_client
            .write_only_transaction()
            .build()
            .write(vec![mutation])
            .await
            .map_err(|e| StoreError::Write(e.to_string()))?;
        Ok(())
    }

    async fn is_repo_registered(&self, repo_id: &str) -> StoreResult<bool> {
        let stmt = Statement::builder("SELECT 1 FROM repos WHERE repo_id = @repo_id")
            .add_param("repo_id", repo_id)
            .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        Ok(rs.next().await.is_some())
    }

    async fn register_repo(
        &self,
        repo_id: String,
        project_id: String,
        name: Option<String>,
    ) -> StoreResult<()> {
        let runner = self
            .db_client
            .read_write_transaction()
            .with_transaction_timeout(std::time::Duration::from_secs(30))
            .build()
            .await
            .map_err(|e| {
                if e.status().is_some_and(|s| s.code.name() == "ABORTED") {
                    StoreError::CasConflict(e.to_string())
                } else {
                    StoreError::Write(e.to_string())
                }
            })?;
        let app_err: Arc<std::sync::Mutex<Option<StoreError>>> =
            Arc::new(std::sync::Mutex::new(None));
        let app_err_tx = Arc::clone(&app_err);
        let tx_res = runner
            .run(async move |tx| {
                let repo_stmt =
                    Statement::builder("SELECT project_id FROM repos WHERE repo_id = @repo_id")
                        .add_param("repo_id", &repo_id)
                        .build();
                let mut rs = tx.execute_query(repo_stmt).await?;
                if let Some(row_res) = rs.next().await {
                    let row = row_res?;
                    let existing_project_id: String = row.try_get("project_id")?;
                    if existing_project_id != project_id {
                        *app_err_tx.lock().unwrap() = Some(StoreError::AlreadyExists(format!(
                            "repository '{repo_id}' is already registered under project '{existing_project_id}', not '{project_id}'"
                        )));
                        return Err(google_cloud_spanner::Error::deser(std::io::Error::other(
                            "repository project conflict",
                        )));
                    }
                    return Ok(());
                }

                let proj_stmt = Statement::builder(
                    "SELECT 1 FROM projects WHERE project_id = @project_id",
                )
                .add_param("project_id", &project_id)
                .build();
                let mut proj_rs = tx.execute_query(proj_stmt).await?;
                let mut mutations = Vec::new();
                if proj_rs.next().await.transpose()?.is_none() {
                    mutations.push(
                        Mutation::new_insert_builder("projects")
                            .set("project_id")
                            .to(&project_id)
                            .build(),
                    );
                }

                let mut repo_builder = Mutation::new_insert_builder("repos")
                    .set("repo_id")
                    .to(&repo_id)
                    .set("project_id")
                    .to(&project_id);
                if let Some(n) = &name {
                    repo_builder = repo_builder.set("name").to(n);
                }
                mutations.push(repo_builder.build());
                tx.buffer(mutations)?;
                Ok(())
            })
            .await;

        if let Some(err) = app_err.lock().unwrap().take() {
            return Err(err);
        }
        tx_res.map_err(|e| {
            if e.status().is_some_and(|s| s.code.name() == "ABORTED") {
                StoreError::CasConflict(e.to_string())
            } else {
                StoreError::Write(e.to_string())
            }
        })?;
        Ok(())
    }

    async fn get_repo_project_id(&self, repo_id: &str) -> StoreResult<Option<String>> {
        let stmt = Statement::builder("SELECT project_id FROM repos WHERE repo_id = @repo_id")
            .add_param("repo_id", repo_id)
            .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        if let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| StoreError::Read(e.to_string()))?;
            let project_id: String = row.get("project_id");
            Ok(Some(project_id))
        } else {
            Ok(None)
        }
    }

    async fn get_commit(&self, project_id: &str, commit_id: &[u8]) -> StoreResult<Option<Commit>> {
        let stmt = Statement::builder(
            "SELECT data FROM commits WHERE project_id = @project_id AND commit_id = @commit_id",
        )
        .add_param("project_id", project_id)
        .add_param("commit_id", commit_id.to_vec())
        .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        if let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| StoreError::Read(e.to_string()))?;
            let data: Vec<u8> = row.get("data");
            let commit =
                Commit::decode(&data[..]).map_err(|e| StoreError::Decode(e.to_string()))?;
            Ok(Some(commit))
        } else {
            Ok(None)
        }
    }

    async fn list_project_commit_ids(&self, project_id: &str) -> StoreResult<Vec<CommitId>> {
        let stmt =
            Statement::builder("SELECT commit_id FROM commits WHERE project_id = @project_id")
                .add_param("project_id", project_id)
                .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        let mut result = Vec::new();
        while let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| StoreError::Read(e.to_string()))?;
            let commit_id: Vec<u8> = row.get("commit_id");
            result.push(commit_id);
        }
        Ok(result)
    }

    async fn put_commit(
        &self,
        project_id: String,
        commit_id: Vec<u8>,
        commit: Commit,
    ) -> StoreResult<()> {
        let mut encoded = Vec::new();
        commit
            .encode(&mut encoded)
            .map_err(|e| StoreError::Encode(e.to_string()))?;
        let proj_mutation = Mutation::new_insert_or_update_builder("projects")
            .set("project_id")
            .to(&project_id)
            .build();
        let commit_mutation = Mutation::new_insert_or_update_builder("commits")
            .set("project_id")
            .to(&project_id)
            .set("commit_id")
            .to(&commit_id)
            .set("data")
            .to(&encoded)
            .build();
        self.db_client
            .write_only_transaction()
            .build()
            .write(vec![proj_mutation, commit_mutation])
            .await
            .map_err(|e| StoreError::Write(e.to_string()))?;
        Ok(())
    }

    async fn get_tree(
        &self,
        project_id: &str,
        tree_id: &[u8],
    ) -> StoreResult<Option<Vec<TreeEntry>>> {
        let stmt = Statement::builder(
            "SELECT data FROM trees WHERE project_id = @project_id AND tree_id = @tree_id",
        )
        .add_param("project_id", project_id)
        .add_param("tree_id", tree_id.to_vec())
        .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        if let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| StoreError::Read(e.to_string()))?;
            let data: Vec<u8> = row.get("data");
            let list =
                TreeEntryList::decode(&data[..]).map_err(|e| StoreError::Decode(e.to_string()))?;
            Ok(Some(list.entries))
        } else {
            Ok(None)
        }
    }

    async fn put_tree(
        &self,
        project_id: String,
        tree_id: Vec<u8>,
        entries: Vec<TreeEntry>,
    ) -> StoreResult<()> {
        let list = TreeEntryList { entries };
        let mut encoded = Vec::new();
        list.encode(&mut encoded)
            .map_err(|e| StoreError::Encode(e.to_string()))?;
        let proj_mutation = Mutation::new_insert_or_update_builder("projects")
            .set("project_id")
            .to(&project_id)
            .build();
        let tree_mutation = Mutation::new_insert_or_update_builder("trees")
            .set("project_id")
            .to(&project_id)
            .set("tree_id")
            .to(&tree_id)
            .set("data")
            .to(&encoded)
            .build();
        self.db_client
            .write_only_transaction()
            .build()
            .write(vec![proj_mutation, tree_mutation])
            .await
            .map_err(|e| StoreError::Write(e.to_string()))?;
        Ok(())
    }

    async fn get_file(&self, project_id: &str, file_id: &[u8]) -> StoreResult<Option<Vec<u8>>> {
        let stmt = Statement::builder(
            "SELECT data FROM files WHERE project_id = @project_id AND file_id = @file_id",
        )
        .add_param("project_id", project_id)
        .add_param("file_id", file_id.to_vec())
        .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        if let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| StoreError::Read(e.to_string()))?;
            let data: Vec<u8> = row.get("data");
            Ok(Some(data))
        } else {
            Ok(None)
        }
    }

    async fn put_file(
        &self,
        project_id: String,
        file_id: Vec<u8>,
        content: Vec<u8>,
    ) -> StoreResult<()> {
        let proj_mutation = Mutation::new_insert_or_update_builder("projects")
            .set("project_id")
            .to(&project_id)
            .build();
        let file_mutation = Mutation::new_insert_or_update_builder("files")
            .set("project_id")
            .to(&project_id)
            .set("file_id")
            .to(&file_id)
            .set("data")
            .to(&content)
            .build();
        self.db_client
            .write_only_transaction()
            .build()
            .write(vec![proj_mutation, file_mutation])
            .await
            .map_err(|e| StoreError::Write(e.to_string()))?;
        Ok(())
    }

    async fn get_operation(&self, repo_id: &str, op_id: &[u8]) -> StoreResult<Option<Operation>> {
        let stmt = Statement::builder(
            "SELECT data FROM operations WHERE repo_id = @repo_id AND op_id = @op_id",
        )
        .add_param("repo_id", repo_id)
        .add_param("op_id", op_id.to_vec())
        .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        if let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| StoreError::Read(e.to_string()))?;
            let data: Vec<u8> = row.get("data");
            let op = Operation::decode(&data[..]).map_err(|e| StoreError::Decode(e.to_string()))?;
            Ok(Some(op))
        } else {
            Ok(None)
        }
    }

    async fn put_operation(
        &self,
        repo_id: String,
        op_id: Vec<u8>,
        op: Operation,
    ) -> StoreResult<()> {
        let mut encoded = Vec::new();
        op.encode(&mut encoded)
            .map_err(|e| StoreError::Encode(e.to_string()))?;
        let mutation = Mutation::new_insert_or_update_builder("operations")
            .set("repo_id")
            .to(&repo_id)
            .set("op_id")
            .to(&op_id)
            .set("data")
            .to(&encoded)
            .build();
        self.db_client
            .write_only_transaction()
            .build()
            .write(vec![mutation])
            .await
            .map_err(|e| StoreError::Write(e.to_string()))?;
        Ok(())
    }

    async fn get_view(&self, repo_id: &str, view_id: &[u8]) -> StoreResult<Option<View>> {
        let stmt = Statement::builder(
            "SELECT data FROM views WHERE repo_id = @repo_id AND view_id = @view_id",
        )
        .add_param("repo_id", repo_id)
        .add_param("view_id", view_id.to_vec())
        .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        if let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| StoreError::Read(e.to_string()))?;
            let data: Vec<u8> = row.get("data");
            let view = View::decode(&data[..]).map_err(|e| StoreError::Decode(e.to_string()))?;
            Ok(Some(view))
        } else {
            Ok(None)
        }
    }

    async fn put_view(&self, repo_id: String, view_id: Vec<u8>, view: View) -> StoreResult<()> {
        let mut encoded = Vec::new();
        view.encode(&mut encoded)
            .map_err(|e| StoreError::Encode(e.to_string()))?;
        let mutation = Mutation::new_insert_or_update_builder("views")
            .set("repo_id")
            .to(&repo_id)
            .set("view_id")
            .to(&view_id)
            .set("data")
            .to(&encoded)
            .build();
        self.db_client
            .write_only_transaction()
            .build()
            .write(vec![mutation])
            .await
            .map_err(|e| StoreError::Write(e.to_string()))?;
        Ok(())
    }

    async fn get_op_heads(&self, repo_id: &str) -> StoreResult<Option<Vec<Vec<u8>>>> {
        let stmt = Statement::builder("SELECT op_id FROM op_heads WHERE repo_id = @repo_id")
            .add_param("repo_id", repo_id)
            .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        let mut result = Vec::new();
        while let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| StoreError::Read(e.to_string()))?;
            let op_id: Vec<u8> = row.get("op_id");
            result.push(op_id);
        }
        if result.is_empty() {
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }

    async fn update_op_heads_append_new_remove_old(
        &self,
        repo_id: String,
        ids_to_remove: &[Vec<u8>],
        id_to_add: Vec<u8>,
    ) -> StoreResult<Vec<Vec<u8>>> {
        let stmt = Statement::builder("SELECT op_id FROM op_heads WHERE repo_id = @repo_id")
            .add_param("repo_id", &repo_id)
            .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        let mut current_heads = Vec::new();
        while let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| StoreError::Read(e.to_string()))?;
            let op_id: Vec<u8> = row.get("op_id");
            current_heads.push(op_id);
        }
        if current_heads.is_empty() {
            current_heads.push(cc_common::ROOT_OPERATION_ID_BYTES.to_vec());
        }

        if !ids_to_remove.is_empty() {
            for old in ids_to_remove {
                if !current_heads.contains(old) {
                    return Err(StoreError::CasConflict(format!(
                        "write race detected on stale old_op_head_ids: head {} no longer active in repo {}",
                        hex::encode(old),
                        repo_id
                    )));
                }
            }
        }

        let mut mutations = Vec::new();
        for old in ids_to_remove {
            let key = Key::new(vec![repo_id.clone().into(), old.clone().into()]);
            let key_set = KeySet::builder().add_key(key).build();
            mutations.push(Mutation::delete("op_heads", key_set));
        }
        mutations.push(
            Mutation::new_insert_or_update_builder("op_heads")
                .set("repo_id")
                .to(&repo_id)
                .set("op_id")
                .to(&id_to_add)
                .build(),
        );

        self.db_client
            .write_only_transaction()
            .build()
            .write(mutations)
            .await
            .map_err(|e| StoreError::Write(e.to_string()))?;

        let stmt = Statement::builder("SELECT op_id FROM op_heads WHERE repo_id = @repo_id")
            .add_param("repo_id", &repo_id)
            .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        let mut result = Vec::new();
        while let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| StoreError::Read(e.to_string()))?;
            let op_id: Vec<u8> = row.get("op_id");
            result.push(op_id);
        }
        Ok(result)
    }

    async fn update_op_heads_compare_and_swap(
        &self,
        repo_id: String,
        expected_exact_heads: &[Vec<u8>],
        new_id: Vec<u8>,
    ) -> StoreResult<Vec<Vec<u8>>> {
        let stmt = Statement::builder("SELECT op_id FROM op_heads WHERE repo_id = @repo_id")
            .add_param("repo_id", &repo_id)
            .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        let mut current_heads = Vec::new();
        while let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| StoreError::Read(e.to_string()))?;
            let op_id: Vec<u8> = row.get("op_id");
            current_heads.push(op_id);
        }
        if current_heads.is_empty() {
            current_heads.push(cc_common::ROOT_OPERATION_ID_BYTES.to_vec());
        }

        let current_set: HashSet<Vec<u8>> = current_heads.into_iter().collect();
        let expected_set: HashSet<Vec<u8>> = expected_exact_heads.iter().cloned().collect();

        if current_set != expected_set {
            return Err(StoreError::CasConflict(format!(
                "reconciliation CAS conflict for repo {repo_id}: expected heads {expected_set:?}, found {current_set:?}"
            )));
        }

        let mut mutations = Vec::new();
        for old in &expected_set {
            if old == &new_id {
                continue;
            }
            let key = Key::new(vec![repo_id.clone().into(), old.clone().into()]);
            let key_set = KeySet::builder().add_key(key).build();
            mutations.push(Mutation::delete("op_heads", key_set));
        }
        mutations.push(
            Mutation::new_insert_or_update_builder("op_heads")
                .set("repo_id")
                .to(&repo_id)
                .set("op_id")
                .to(&new_id)
                .build(),
        );

        self.db_client
            .write_only_transaction()
            .build()
            .write(mutations)
            .await
            .map_err(|e| StoreError::Write(e.to_string()))?;

        Ok(vec![new_id])
    }

    async fn get_workspace(
        &self,
        repo_id: &str,
        user: &str,
        workspace_name: &str,
    ) -> StoreResult<Option<WorkspaceState>> {
        let stmt = Statement::builder(
            "SELECT commit_id, operation_id, tree_id, proto_data FROM workspaces WHERE repo_id = @repo_id AND user = @user AND workspace_name = @workspace_name",
        )
        .add_param("repo_id", repo_id)
        .add_param("user", user)
        .add_param("workspace_name", workspace_name)
        .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        if let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| StoreError::Read(e.to_string()))?;
            let commit_id: Vec<u8> = row
                .try_get("commit_id")
                .map_err(|e| StoreError::Read(e.to_string()))?;
            let operation_id: Vec<u8> = row
                .try_get("operation_id")
                .map_err(|e| StoreError::Read(e.to_string()))?;
            let tree_id: Vec<u8> = row
                .try_get("tree_id")
                .map_err(|e| StoreError::Read(e.to_string()))?;
            let proto_data: Option<Vec<u8>> = row
                .try_get("proto_data")
                .map_err(|e| StoreError::Read(e.to_string()))?;
            let ws = super::decode_workspace_row(
                repo_id.to_string(),
                user.to_string(),
                workspace_name.to_string(),
                commit_id,
                operation_id,
                tree_id,
                proto_data,
            )?;
            Ok(Some(ws))
        } else {
            Ok(None)
        }
    }

    #[allow(deprecated)]
    async fn put_workspace_cas(
        &self,
        workspace: WorkspaceState,
        expected_operation_id: Option<&[u8]>,
    ) -> StoreResult<()> {
        let normalized = super::normalize_workspace_record(workspace);
        let proto_data = normalized.encode_to_vec();
        let build_mutation = |ws: &WorkspaceState, data: &Vec<u8>| {
            Mutation::new_insert_or_update_builder("workspaces")
                .set("repo_id")
                .to(&ws.repo_id)
                .set("user")
                .to(&ws.user)
                .set("workspace_name")
                .to(&ws.workspace_name)
                .set("commit_id")
                .to(&ws.commit_id)
                .set("operation_id")
                .to(&ws.operation_id)
                .set("tree_id")
                .to(&ws.tree_id)
                .set("proto_data")
                .to(data)
                .build()
        };

        let Some(expected) = expected_operation_id else {
            let mutation = build_mutation(&normalized, &proto_data);
            self.db_client
                .write_only_transaction()
                .build()
                .write(vec![mutation])
                .await
                .map_err(|e| StoreError::Write(e.to_string()))?;
            return Ok(());
        };

        let exp_owned = expected.to_vec();
        let ws = normalized;
        let data = proto_data;
        let runner = self
            .db_client
            .read_write_transaction()
            .set_isolation_level(
                google_cloud_spanner::model::transaction_options::IsolationLevel::Serializable,
            )
            .with_transaction_timeout(std::time::Duration::from_secs(30))
            .build()
            .await
            .map_err(|e| {
                if e.status().is_some_and(|s| s.code.name() == "ABORTED") {
                    StoreError::CasConflict(e.to_string())
                } else {
                    StoreError::Write(e.to_string())
                }
            })?;
        let app_err: Arc<std::sync::Mutex<Option<StoreError>>> =
            Arc::new(std::sync::Mutex::new(None));
        let app_err_tx = Arc::clone(&app_err);
        let outcome = runner
            .run(async move |tx| -> Result<(), google_cloud_spanner::Error> {
                *app_err_tx.lock().unwrap() = None;
                let abort_with = |err: StoreError| -> google_cloud_spanner::Error {
                    *app_err_tx.lock().unwrap() = Some(err);
                    google_cloud_spanner::Error::deser(std::io::Error::other(
                        "workspace CAS transaction aborted",
                    ))
                };
                let stmt = Statement::builder(
                    "SELECT commit_id, operation_id, tree_id, proto_data FROM workspaces WHERE repo_id = @repo_id AND user = @user AND workspace_name = @workspace_name",
                )
                .add_param("repo_id", &ws.repo_id)
                .add_param("user", &ws.user)
                .add_param("workspace_name", &ws.workspace_name)
                .build();
                let mut rs = tx.execute_query(stmt).await?;
                // Drain the stream completely so trailing precommit tokens and gRPC errors
                // (including `ABORTED`) are never dropped before evaluating the first row.
                let mut first_row = None;
                while let Some(row_res) = rs.next().await {
                    let row = row_res?;
                    if first_row.is_none() {
                        first_row = Some(row);
                    }
                }
                let existing = match first_row {
                    Some(row) => {
                        let commit_id: Vec<u8> = match row.try_get("commit_id") {
                            Ok(v) => v,
                            Err(e) => return Err(abort_with(StoreError::Read(e.to_string()))),
                        };
                        let operation_id: Vec<u8> = match row.try_get("operation_id") {
                            Ok(v) => v,
                            Err(e) => return Err(abort_with(StoreError::Read(e.to_string()))),
                        };
                        let tree_id: Vec<u8> = match row.try_get("tree_id") {
                            Ok(v) => v,
                            Err(e) => return Err(abort_with(StoreError::Read(e.to_string()))),
                        };
                        let proto_bytes: Option<Vec<u8>> = match row.try_get("proto_data") {
                            Ok(v) => v,
                            Err(e) => return Err(abort_with(StoreError::Read(e.to_string()))),
                        };
                        match super::decode_workspace_row(
                            ws.repo_id.clone(),
                            ws.user.clone(),
                            ws.workspace_name.clone(),
                            commit_id,
                            operation_id,
                            tree_id,
                            proto_bytes,
                        ) {
                            Ok(decoded) => Some(decoded),
                            Err(err) => return Err(abort_with(err)),
                        }
                    }
                    None => None,
                };
                if let Err(err) =
                    super::verify_workspace_cas(existing.as_ref(), &ws, Some(&exp_owned))
                {
                    return Err(abort_with(err));
                }
                if existing.as_ref() == Some(&ws) {
                    return Ok(());
                }
                let mutation_builder = if existing.is_none() {
                    Mutation::new_insert_builder("workspaces")
                } else {
                    Mutation::new_insert_or_update_builder("workspaces")
                };
                tx.buffer(vec![
                    mutation_builder
                        .set("repo_id")
                        .to(&ws.repo_id)
                        .set("user")
                        .to(&ws.user)
                        .set("workspace_name")
                        .to(&ws.workspace_name)
                        .set("commit_id")
                        .to(&ws.commit_id)
                        .set("operation_id")
                        .to(&ws.operation_id)
                        .set("tree_id")
                        .to(&ws.tree_id)
                        .set("proto_data")
                        .to(&data)
                        .build(),
                ])?;
                Ok(())
            })
            .await;

        if let Some(err) = app_err.lock().unwrap().take() {
            return Err(err);
        }
        outcome.map_err(|e| {
            if e.status()
                .is_some_and(|s| matches!(s.code.name(), "ABORTED" | "ALREADY_EXISTS"))
            {
                StoreError::CasConflict(e.to_string())
            } else {
                StoreError::Write(e.to_string())
            }
        })?;
        Ok(())
    }

    async fn delete_workspace(
        &self,
        repo_id: &str,
        user: &str,
        workspace_name: &str,
    ) -> StoreResult<bool> {
        let repo_id = repo_id.to_string();
        let user = user.to_string();
        let workspace_name = workspace_name.to_string();
        let runner = self
            .db_client
            .read_write_transaction()
            .with_transaction_timeout(std::time::Duration::from_secs(30))
            .build()
            .await
            .map_err(|e| StoreError::Write(e.to_string()))?;
        let tx_res = runner
            .run(async move |tx| {
                let stmt = Statement::builder(
                    "DELETE FROM workspaces WHERE repo_id = @repo_id AND user = @user AND workspace_name = @workspace_name",
                )
                .add_param("repo_id", &repo_id)
                .add_param("user", &user)
                .add_param("workspace_name", &workspace_name)
                .build();
                tx.execute_update(stmt).await
            })
            .await
            .map_err(|e| StoreError::Write(e.to_string()))?;
        Ok(tx_res.result > 0)
    }

    async fn list_workspaces(&self, repo_id: &str) -> StoreResult<Vec<WorkspaceState>> {
        let stmt = Statement::builder(
            "SELECT user, workspace_name, commit_id, operation_id, tree_id, proto_data FROM workspaces WHERE repo_id = @repo_id ORDER BY user, workspace_name",
        )
        .add_param("repo_id", repo_id)
        .build();
        self.query_workspaces(repo_id, stmt).await
    }

    async fn list_workspaces_page(
        &self,
        repo_id: &str,
        after: Option<(&str, &str)>,
        limit: usize,
    ) -> StoreResult<Vec<WorkspaceState>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let limit_i64 = i64::try_from(limit).unwrap_or(i64::MAX);
        let stmt = match after {
            Some((after_user, after_ws)) => Statement::builder(
                "SELECT user, workspace_name, commit_id, operation_id, tree_id, proto_data \
                 FROM workspaces \
                 WHERE repo_id = @repo_id AND (user > @after_user OR (user = @after_user AND workspace_name > @after_ws)) \
                 ORDER BY user ASC, workspace_name ASC \
                 LIMIT @limit",
            )
            .add_param("repo_id", repo_id)
            .add_param("after_user", after_user)
            .add_param("after_ws", after_ws)
            .add_param("limit", limit_i64)
            .build(),
            None => Statement::builder(
                "SELECT user, workspace_name, commit_id, operation_id, tree_id, proto_data \
                 FROM workspaces \
                 WHERE repo_id = @repo_id \
                 ORDER BY user ASC, workspace_name ASC \
                 LIMIT @limit",
            )
            .add_param("repo_id", repo_id)
            .add_param("limit", limit_i64)
            .build(),
        };
        self.query_workspaces(repo_id, stmt).await
    }
}

impl SpannerStore {
    async fn query_workspaces(
        &self,
        repo_id: &str,
        stmt: Statement,
    ) -> StoreResult<Vec<WorkspaceState>> {
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        let mut result = Vec::new();
        while let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| StoreError::Read(e.to_string()))?;
            let user: String = row
                .try_get("user")
                .map_err(|e| StoreError::Read(e.to_string()))?;
            let workspace_name: String = row
                .try_get("workspace_name")
                .map_err(|e| StoreError::Read(e.to_string()))?;
            let commit_id: Vec<u8> = row
                .try_get("commit_id")
                .map_err(|e| StoreError::Read(e.to_string()))?;
            let operation_id: Vec<u8> = row
                .try_get("operation_id")
                .map_err(|e| StoreError::Read(e.to_string()))?;
            let tree_id: Vec<u8> = row
                .try_get("tree_id")
                .map_err(|e| StoreError::Read(e.to_string()))?;
            let proto_data: Option<Vec<u8>> = row
                .try_get("proto_data")
                .map_err(|e| StoreError::Read(e.to_string()))?;
            result.push(super::decode_workspace_row(
                repo_id.to_string(),
                user,
                workspace_name,
                commit_id,
                operation_id,
                tree_id,
                proto_data,
            )?);
        }
        Ok(result)
    }
}
