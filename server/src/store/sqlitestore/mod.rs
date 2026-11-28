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

//! Local SQLite `MetadataStore` implementation running in `WAL` journal mode with a
//! concurrent read-connection pool.
//!
//! # Authority and Crash-Safety Ordering
//! `SqliteMetadataStore` persists relational commit, tree, symlink, view, operation, and
//! blob-location rows after `CompositeStore` has completed any out-of-line `BlobStore`
//! upload. Multi-row writes (`put_tree`, `put_blob_record`, `update_op_heads`, and
//! `publish_operation_atomic`) execute inside explicit SQLite `IMMEDIATE` transactions so
//! partial tree entry lists or orphaned `project_blobs` links never appear after a crash.
//!
//! # Concurrency and Merge Invariants
//! File-backed databases open a dedicated writer connection alongside a pool of read-only
//! `WAL` snapshot connections so concurrent agent reads never contend on the writer mutex.
//! `TransactionBehavior::Immediate` transactions serialize `update_op_heads`,
//! `publish_operation_atomic`, atomic `*_for_project` project-binding checks, and
//! `delete_repository` (cascading repo-scoped tables while preserving shared project data).
//!
//! # Cross-Process and RPC Boundaries
//! Blocking `rusqlite` operations run on Tokio's `spawn_blocking` pool and map SQLite
//! constraint and I/O failures into [`StoreError`].

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cc_common::backend::*;
use cc_common::op_store::*;
use cc_common::workspace::WorkspaceState;
use rusqlite::{Connection, OpenFlags};

use super::{BlobRecord, CommitId, MetadataStore, OpId, StoreError, StoreResult};
use schema::{
    configure_reader_connection, configure_writer_connection, create_dir_all_private,
    precreate_private_file, restrict_sqlite_sidecar_permissions,
};

mod commits;
mod objects;
mod ops;
mod repo;
mod schema;
#[cfg(test)]
mod tests;
mod workspaces;

const WAL_READER_POOL_SIZE: usize = 8;

#[derive(Clone)]
pub struct SqliteMetadataStore {
    writer: Arc<Mutex<Connection>>,
    readers: Arc<Vec<Arc<Mutex<Connection>>>>,
    next_reader: Arc<AtomicUsize>,
}

impl SqliteMetadataStore {
    pub fn open(db_path: &Path) -> StoreResult<Self> {
        if db_path.as_os_str().is_empty() || db_path.to_str().is_some_and(|s| s.trim().is_empty()) {
            return Err(StoreError::InvalidArgument(
                "SQLite database path must not be empty".to_string(),
            ));
        }
        if db_path == Path::new(":memory:") {
            return Self::in_memory();
        }
        if let Some(parent) = db_path.parent()
            && !parent.as_os_str().is_empty()
        {
            create_dir_all_private(parent).map_err(|e| {
                StoreError::Backend(format!(
                    "failed to create SQLite parent directory {}: {e}",
                    parent.display()
                ))
            })?;
        }
        let created_db = precreate_private_file(db_path).map_err(|e| {
            StoreError::Backend(format!(
                "failed to create SQLite database file {}: {e}",
                db_path.display()
            ))
        })?;
        let mut writer_conn = Connection::open(db_path).map_err(|e| {
            StoreError::Backend(format!(
                "failed to open SQLite database {}: {e}",
                db_path.display()
            ))
        })?;
        configure_writer_connection(&mut writer_conn)?;
        if created_db {
            restrict_sqlite_sidecar_permissions(db_path);
        }

        let mut reader_conns = Vec::with_capacity(WAL_READER_POOL_SIZE);
        let read_flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        for _ in 0..WAL_READER_POOL_SIZE {
            let rconn = Connection::open_with_flags(db_path, read_flags).map_err(|e| {
                StoreError::Backend(format!(
                    "failed to open SQLite WAL reader {}: {e}",
                    db_path.display()
                ))
            })?;
            configure_reader_connection(&rconn)?;
            reader_conns.push(Arc::new(Mutex::new(rconn)));
        }

        Ok(Self {
            writer: Arc::new(Mutex::new(writer_conn)),
            readers: Arc::new(reader_conns),
            next_reader: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn in_memory() -> StoreResult<Self> {
        let mut conn = Connection::open_in_memory()
            .map_err(|e| StoreError::Backend(format!("failed to open in-memory SQLite: {e}")))?;
        configure_writer_connection(&mut conn)?;
        let shared = Arc::new(Mutex::new(conn));
        Ok(Self {
            writer: Arc::clone(&shared),
            readers: Arc::new(vec![shared]),
            next_reader: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub(super) async fn with_write_conn<F, T>(&self, f: F) -> StoreResult<T>
    where
        F: FnOnce(&mut Connection) -> StoreResult<T> + Send + 'static,
        T: Send + 'static,
    {
        let conn = Arc::clone(&self.writer);
        tokio::task::spawn_blocking(move || {
            let mut guard = conn
                .lock()
                .map_err(|_| StoreError::Backend("SQLite writer mutex poisoned".to_string()))?;
            f(&mut guard)
        })
        .await
        .map_err(|e| StoreError::Backend(format!("SQLite blocking task panicked: {e}")))?
    }

    pub(super) async fn with_read_conn<F, T>(&self, f: F) -> StoreResult<T>
    where
        F: FnOnce(&mut Connection) -> StoreResult<T> + Send + 'static,
        T: Send + 'static,
    {
        let idx = self.next_reader.fetch_add(1, Ordering::Relaxed) % self.readers.len();
        let conn = Arc::clone(&self.readers[idx]);
        tokio::task::spawn_blocking(move || {
            let mut guard = conn
                .lock()
                .map_err(|_| StoreError::Backend("SQLite reader mutex poisoned".to_string()))?;
            f(&mut guard)
        })
        .await
        .map_err(|e| StoreError::Backend(format!("SQLite blocking task panicked: {e}")))?
    }
}

#[async_trait]
impl MetadataStore for SqliteMetadataStore {
    fn backend_name(&self) -> &'static str {
        "sqlite"
    }

    async fn register_project(&self, project_id: &str, name: Option<&str>) -> StoreResult<()> {
        self.register_project_impl(project_id, name).await
    }

    async fn is_project_registered(&self, project_id: &str) -> StoreResult<bool> {
        self.is_project_registered_impl(project_id).await
    }

    async fn get_repo_project_id(&self, repo_id: &str) -> StoreResult<Option<String>> {
        self.get_repo_project_id_impl(repo_id).await
    }

    async fn register_repo_in_project(
        &self,
        repo_id: &str,
        project_id: &str,
        name: Option<&str>,
    ) -> StoreResult<String> {
        self.register_repo_in_project_impl(repo_id, project_id, name)
            .await
    }

    async fn delete_repository(
        &self,
        repo_id: &str,
        expected_project_id: Option<&str>,
    ) -> StoreResult<bool> {
        self.delete_repository_impl(repo_id, expected_project_id)
            .await
    }

    async fn get_commit(&self, project_id: &str, commit_id: &[u8]) -> StoreResult<Option<Commit>> {
        self.get_commit_impl(project_id, commit_id).await
    }

    async fn has_commit(&self, project_id: &str, commit_id: &[u8]) -> StoreResult<bool> {
        self.has_commit_impl(project_id, commit_id).await
    }

    async fn get_commit_generation(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> StoreResult<Option<u64>> {
        self.get_commit_generation_impl(project_id, commit_id).await
    }

    async fn get_commit_with_generation(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> StoreResult<Option<(Commit, u64)>> {
        self.get_commit_with_generation_impl(project_id, commit_id)
            .await
    }

    async fn get_commit_generations(
        &self,
        project_id: &str,
        commit_ids: &[Vec<u8>],
    ) -> StoreResult<HashMap<Vec<u8>, u64>> {
        self.get_commit_generations_impl(project_id, commit_ids)
            .await
    }

    async fn put_commit(
        &self,
        project_id: &str,
        commit_id: &[u8],
        commit: &Commit,
        generation: u64,
    ) -> StoreResult<()> {
        self.put_commit_impl(project_id, commit_id, commit, generation)
            .await
    }

    async fn list_commit_ids(
        &self,
        project_id: &str,
        after_commit_id: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<Vec<CommitId>> {
        self.list_commit_ids_impl(project_id, after_commit_id, limit)
            .await
    }

    async fn current_commit_watermark(&self, project_id: &str) -> StoreResult<Vec<u8>> {
        self.current_commit_watermark_impl(project_id).await
    }

    async fn list_commit_ids_since(
        &self,
        project_id: &str,
        since_token: &[u8],
        limit: usize,
    ) -> StoreResult<(Vec<CommitId>, Vec<u8>, bool)> {
        self.list_commit_ids_since_impl(project_id, since_token, limit)
            .await
    }

    async fn resolve_commit_id_prefix(
        &self,
        project_id: &str,
        min_id: &[u8],
        max_id: &[u8],
    ) -> StoreResult<Vec<CommitId>> {
        self.resolve_commit_id_prefix_impl(project_id, min_id, max_id)
            .await
    }

    async fn resolve_change_id_prefix(
        &self,
        project_id: &str,
        min_id: &[u8],
        max_id: &[u8],
        limit: usize,
    ) -> StoreResult<Vec<(Vec<u8>, Vec<CommitId>)>> {
        self.resolve_change_id_prefix_impl(project_id, min_id, max_id, limit)
            .await
    }

    async fn get_tree(
        &self,
        project_id: &str,
        tree_id: &[u8],
    ) -> StoreResult<Option<Vec<TreeEntry>>> {
        self.get_tree_impl(project_id, tree_id).await
    }

    async fn get_tree_page(
        &self,
        project_id: &str,
        tree_id: &[u8],
        after_name: Option<&str>,
        limit: usize,
    ) -> StoreResult<Option<Vec<TreeEntry>>> {
        self.get_tree_page_impl(project_id, tree_id, after_name, limit)
            .await
    }

    async fn put_tree(
        &self,
        project_id: &str,
        tree_id: &[u8],
        entries: &[TreeEntry],
    ) -> StoreResult<()> {
        self.put_tree_impl(project_id, tree_id, entries).await
    }

    async fn find_existing_trees(
        &self,
        project_id: &str,
        tree_ids: &[Vec<u8>],
    ) -> StoreResult<HashSet<Vec<u8>>> {
        self.find_existing_trees_impl(project_id, tree_ids).await
    }

    async fn get_symlink(
        &self,
        project_id: &str,
        symlink_id: &[u8],
    ) -> StoreResult<Option<String>> {
        self.get_symlink_impl(project_id, symlink_id).await
    }

    async fn put_symlink(
        &self,
        project_id: &str,
        symlink_id: &[u8],
        target: &str,
    ) -> StoreResult<()> {
        self.put_symlink_impl(project_id, symlink_id, target).await
    }

    async fn find_existing_symlinks(
        &self,
        project_id: &str,
        symlink_ids: &[Vec<u8>],
    ) -> StoreResult<HashSet<Vec<u8>>> {
        self.find_existing_symlinks_impl(project_id, symlink_ids)
            .await
    }

    async fn get_blob_record(
        &self,
        project_id: &str,
        blob_id: &[u8],
    ) -> StoreResult<Option<BlobRecord>> {
        self.get_blob_record_impl(project_id, blob_id).await
    }

    async fn get_blob_sizes_batch(
        &self,
        project_id: &str,
        blob_ids: &[Vec<u8>],
    ) -> StoreResult<HashMap<Vec<u8>, u64>> {
        self.get_blob_sizes_batch_impl(project_id, blob_ids).await
    }

    async fn get_global_blob_record(&self, blob_id: &[u8]) -> StoreResult<Option<BlobRecord>> {
        self.get_global_blob_record_impl(blob_id).await
    }

    async fn put_blob_record(&self, project_id: &str, record: &BlobRecord) -> StoreResult<()> {
        self.put_blob_record_impl(project_id, record).await
    }

    async fn add_blob_replica_location(&self, blob_id: &[u8], location: &str) -> StoreResult<()> {
        self.add_blob_replica_location_impl(blob_id, location).await
    }

    async fn get_operation(&self, repo_id: &str, op_id: &[u8]) -> StoreResult<Option<Operation>> {
        self.get_operation_impl(repo_id, op_id).await
    }

    async fn put_operation(&self, repo_id: &str, op_id: &[u8], op: &Operation) -> StoreResult<()> {
        self.put_operation_impl(repo_id, op_id, op).await
    }

    async fn put_operation_for_project(
        &self,
        repo_id: &str,
        expected_project_id: &str,
        op_id: &[u8],
        op: &Operation,
        require_dependencies_exist: bool,
    ) -> StoreResult<()> {
        self.put_operation_for_project_impl(
            repo_id,
            expected_project_id,
            op_id,
            op,
            require_dependencies_exist,
        )
        .await
    }

    async fn resolve_operation_id_prefix(
        &self,
        repo_id: &str,
        min_id: &[u8],
        max_id: &[u8],
    ) -> StoreResult<Vec<OpId>> {
        self.resolve_operation_id_prefix_impl(repo_id, min_id, max_id)
            .await
    }

    async fn get_view(&self, repo_id: &str, view_id: &[u8]) -> StoreResult<Option<View>> {
        self.get_view_impl(repo_id, view_id).await
    }

    async fn has_view(&self, repo_id: &str, view_id: &[u8]) -> StoreResult<bool> {
        self.has_view_impl(repo_id, view_id).await
    }

    async fn put_view(&self, repo_id: &str, view_id: &[u8], view: &View) -> StoreResult<()> {
        self.put_view_impl(repo_id, view_id, view).await
    }

    async fn put_view_for_project(
        &self,
        repo_id: &str,
        expected_project_id: &str,
        view_id: &[u8],
        view: &View,
    ) -> StoreResult<()> {
        self.put_view_for_project_impl(repo_id, expected_project_id, view_id, view)
            .await
    }

    async fn get_op_heads(&self, repo_id: &str) -> StoreResult<Option<Vec<OpId>>> {
        self.get_op_heads_impl(repo_id).await
    }

    async fn update_op_heads(
        &self,
        repo_id: &str,
        old_ids: &[Vec<u8>],
        new_id: &[u8],
    ) -> StoreResult<Vec<OpId>> {
        self.update_op_heads_impl(repo_id, old_ids, new_id).await
    }

    async fn update_op_heads_for_project(
        &self,
        repo_id: &str,
        expected_project_id: &str,
        old_head_ids: &[Vec<u8>],
        new_head_id: &[u8],
        require_op_exists: bool,
    ) -> StoreResult<Vec<OpId>> {
        self.update_op_heads_for_project_impl(
            repo_id,
            expected_project_id,
            old_head_ids,
            new_head_id,
            require_op_exists,
        )
        .await
    }

    async fn publish_operation_atomic(
        &self,
        repo_id: &str,
        view_id: &[u8],
        view: &View,
        op_id: &[u8],
        op: &Operation,
        old_ids: &[Vec<u8>],
    ) -> StoreResult<Vec<OpId>> {
        self.publish_operation_atomic_impl(repo_id, view_id, view, op_id, op, old_ids)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn publish_operation_for_project(
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
        self.publish_operation_for_project_impl(
            repo_id,
            expected_project_id,
            view_id,
            view,
            op_id,
            op,
            old_op_head_ids,
            require_dependencies_exist,
        )
        .await
    }

    async fn compare_and_swap_op_heads(
        &self,
        repo_id: &str,
        expected_old_ids: &[Vec<u8>],
        new_id: &[u8],
    ) -> StoreResult<()> {
        self.compare_and_swap_op_heads_impl(repo_id, expected_old_ids, new_id)
            .await
    }

    async fn update_op_heads_cas_for_project(
        &self,
        repo_id: &str,
        expected_project_id: &str,
        expected_exact_heads: &[Vec<u8>],
        new_head_id: &[u8],
        require_op_exists: bool,
    ) -> StoreResult<()> {
        self.update_op_heads_cas_for_project_impl(
            repo_id,
            expected_project_id,
            expected_exact_heads,
            new_head_id,
            require_op_exists,
        )
        .await
    }

    async fn get_workspace(
        &self,
        repo_id: &str,
        user: &str,
        workspace_name: &str,
    ) -> StoreResult<Option<WorkspaceState>> {
        self.get_workspace_impl(repo_id, user, workspace_name).await
    }

    async fn put_workspace_cas(
        &self,
        workspace: &WorkspaceState,
        expected_operation_id: Option<&[u8]>,
    ) -> StoreResult<()> {
        self.put_workspace_cas_impl(workspace, expected_operation_id)
            .await
    }

    async fn put_workspace_cas_for_project(
        &self,
        expected_project_id: &str,
        workspace: &WorkspaceState,
        expected_operation_id: Option<&[u8]>,
        require_operation_exists: bool,
    ) -> StoreResult<()> {
        self.put_workspace_cas_for_project_impl(
            expected_project_id,
            workspace,
            expected_operation_id,
            require_operation_exists,
        )
        .await
    }

    #[allow(deprecated)]
    async fn list_workspaces(&self, repo_id: &str) -> StoreResult<Vec<WorkspaceState>> {
        self.list_workspaces_impl(repo_id).await
    }

    #[allow(deprecated)]
    async fn list_workspaces_page(
        &self,
        repo_id: &str,
        after: Option<(&str, &str)>,
        limit: usize,
        max_bytes: Option<usize>,
    ) -> StoreResult<(Vec<WorkspaceState>, bool)> {
        self.list_workspaces_page_impl(repo_id, after, limit, max_bytes)
            .await
    }

    async fn delete_workspace(
        &self,
        repo_id: &str,
        user: &str,
        workspace_name: &str,
    ) -> StoreResult<bool> {
        self.delete_workspace_impl(repo_id, user, workspace_name)
            .await
    }
}
