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

//! Server persistence traits (`MetadataStore` and `BlobStore`) and `CompositeStore`
//! coordinator for jj-commit-cloud.
//!
//! # Authority and Crash-Safety Ordering
//! `CompositeStore` enforces write-before-publish ordering across metadata and object
//! stores:
//! 1. `put_file` writes bytes to `BlobStore` (unless smaller than `inline_blob_threshold`) before
//!    recording `BlobRecord` and `(project_id, blob_id)` ownership in `MetadataStore`.
//! 2. `put_tree`, `put_commit`, `put_operation`, and `update_op_heads` validate that referenced
//!    child blobs, symlinks, subtrees, parent commits, views, and operations already exist in
//!    `MetadataStore` before publishing parent records.
//!
//! # Concurrency and Merge Invariants
//! Content-addressed writes (`Commit`, `Tree`, `Symlink`, `Blob`, `View`, `Operation`) are
//! immutable and idempotent. `CompositeStore` resolves `repo_id -> project_id` live from
//! `MetadataStore` and passes the validated `project_id` into atomic `*_for_project` writes
//! so delete + re-register across replicas never leaks data or persists cross-project refs.
//! `update_op_heads` and `publish_operation_atomic` retain concurrent sibling heads lock-free.
//!
//! # Cross-Process and RPC Boundaries
//! Errors propagate as typed [`StoreError`] variants that convert deterministically into
//! gRPC `tonic::Status` codes at the `BackendService`, `OpStoreService`, and
//! `WorkspaceService` boundaries.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use bytes::Bytes;
use cc_common::backend::*;
use cc_common::op_store::*;
use cc_common::workspace::WorkspaceState;
use prost::Message as _;
use tokio::io::AsyncReadExt as _;
use tokio::sync::{Semaphore, mpsc};

mod composite;
pub mod error;
pub mod gcsblob;
pub mod localblob;
pub mod memorystore;
pub mod postgresstore;
pub mod replicatedblob;
pub mod s3blob;
pub mod spannerstore;
pub mod sqlitestore;
#[cfg(test)]
mod tests;

#[cfg(test)]
use composite::BoundedNamespaceCache;
pub use composite::*;
pub(crate) use composite::{
    check_workspace_cas_precondition, collect_non_root_op_parents, verify_optional_repo_project,
};
pub use error::{StoreError, StoreResult};
pub use gcsblob::{GcsBlobStore, GcsConnectConfig, IMMUTABLE_CACHE_CONTROL};
pub use localblob::LocalFsBlobStore;
pub use memorystore::{
    CommitId, FileId, MemoryBlobStore, MemoryMetadataStore, OpId, ProjectId, RepoId, SymlinkId,
    TreeId, ViewId,
};
pub use postgresstore::{PostgresConnectConfig, PostgresMetadataStore};
pub use replicatedblob::{ReplicatedBlobStore, ReplicatedBlobStoreConfig, ReplicationStats};
pub use s3blob::{S3BlobStore, S3ConnectConfig};
pub use spannerstore::{SpannerConnectConfig, SpannerMetadataStore};
pub use sqlitestore::SqliteMetadataStore;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobRecord {
    pub blob_id: Vec<u8>,
    pub content_size: u64,
    pub inline_content: Option<Bytes>,
    pub primary_location: Option<String>,
    pub replicated_locations: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdPrefixResolution {
    NoMatch,
    SingleMatch(Vec<u8>),
    AmbiguousMatch,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChangePrefixResolution {
    NoMatch,
    SingleMatch {
        change_id: Vec<u8>,
        commit_ids: Vec<CommitId>,
    },
    AmbiguousMatch,
}

/// Estimated serialized size of a `WorkspaceState` entry inside `ListWorkspacesResponse`
/// (protobuf length-delimited field tag + varint length overhead plus payload).
pub(crate) fn workspace_list_entry_bytes(ws: &WorkspaceState) -> usize {
    ws.encoded_len().saturating_add(8)
}

#[async_trait]
pub trait MetadataStore: Send + Sync {
    fn backend_name(&self) -> &'static str;

    async fn register_project(&self, project_id: &str, name: Option<&str>) -> StoreResult<()>;
    async fn is_project_registered(&self, project_id: &str) -> StoreResult<bool>;
    async fn get_repo_project_id(&self, repo_id: &str) -> StoreResult<Option<String>>;
    async fn register_repo_in_project(
        &self,
        repo_id: &str,
        project_id: &str,
        name: Option<&str>,
    ) -> StoreResult<String>;
    async fn delete_repository(
        &self,
        repo_id: &str,
        expected_project_id: Option<&str>,
    ) -> StoreResult<bool>;

    async fn is_repo_registered(&self, repo_id: &str) -> StoreResult<bool> {
        Ok(self.get_repo_project_id(repo_id).await?.is_some())
    }

    async fn register_repo(&self, repo_id: &str, name: Option<&str>) -> StoreResult<()> {
        self.register_repo_in_project(repo_id, repo_id, name)
            .await
            .map(|_| ())
    }

    async fn get_commit(&self, project_id: &str, commit_id: &[u8]) -> StoreResult<Option<Commit>>;
    async fn has_commit(&self, project_id: &str, commit_id: &[u8]) -> StoreResult<bool> {
        Ok(self.get_commit(project_id, commit_id).await?.is_some())
    }
    async fn get_commit_generation(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> StoreResult<Option<u64>>;
    /// Returns both the stored [`Commit`] and its generation number in a single backend lookup
    /// when overridden. The default implementation joins [`MetadataStore::get_commit`] and
    /// [`MetadataStore::get_commit_generation`] and treats partial presence as
    /// [`StoreError::Corruption`].
    async fn get_commit_with_generation(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> StoreResult<Option<(Commit, u64)>> {
        let (commit_res, gen_res) = futures::future::join(
            self.get_commit(project_id, commit_id),
            self.get_commit_generation(project_id, commit_id),
        )
        .await;
        match (commit_res?, gen_res?) {
            (Some(commit), Some(generation)) => Ok(Some((commit, generation))),
            (None, None) => Ok(None),
            (Some(_), None) => Err(StoreError::Corruption(format!(
                "corrupt commit graph: commit {} exists without generation in project {project_id}",
                hex::encode(commit_id)
            ))),
            (None, Some(_)) => Err(StoreError::Corruption(format!(
                "corrupt commit graph: generation exists without commit {} in project {project_id}",
                hex::encode(commit_id)
            ))),
        }
    }
    /// Returns the generation numbers of the commits in `commit_ids` that exist in
    /// `project_id`; missing commits are omitted. The default implementation runs up to 16
    /// `get_commit_generation` reads concurrently, so backends with a batched read should
    /// override it.
    async fn get_commit_generations(
        &self,
        project_id: &str,
        commit_ids: &[Vec<u8>],
    ) -> StoreResult<HashMap<Vec<u8>, u64>> {
        use futures::{StreamExt as _, TryStreamExt as _};

        let mut lookups = Vec::with_capacity(commit_ids.len());
        for commit_id in commit_ids {
            lookups.push(async move {
                let generation = self.get_commit_generation(project_id, commit_id).await?;
                Ok::<_, StoreError>(generation.map(|g| (commit_id.clone(), g)))
            });
        }
        let found: Vec<_> = futures::stream::iter(lookups)
            .buffer_unordered(16)
            .try_collect()
            .await?;
        Ok(found.into_iter().flatten().collect())
    }
    async fn put_commit(
        &self,
        project_id: &str,
        commit_id: &[u8],
        commit: &Commit,
        generation: u64,
    ) -> StoreResult<()>;
    async fn list_commit_ids(
        &self,
        project_id: &str,
        after_commit_id: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<Vec<CommitId>>;
    async fn current_commit_watermark(&self, project_id: &str) -> StoreResult<Vec<u8>>;
    async fn list_commit_ids_since(
        &self,
        project_id: &str,
        since_token: &[u8],
        limit: usize,
    ) -> StoreResult<(Vec<CommitId>, Vec<u8>, bool)>;
    async fn resolve_commit_id_prefix(
        &self,
        project_id: &str,
        min_id: &[u8],
        max_id: &[u8],
    ) -> StoreResult<Vec<CommitId>>;
    async fn resolve_change_id_prefix(
        &self,
        project_id: &str,
        min_id: &[u8],
        max_id: &[u8],
        limit: usize,
    ) -> StoreResult<Vec<(Vec<u8>, Vec<CommitId>)>>;

    async fn get_tree(
        &self,
        project_id: &str,
        tree_id: &[u8],
    ) -> StoreResult<Option<Vec<TreeEntry>>>;
    async fn get_tree_page(
        &self,
        project_id: &str,
        tree_id: &[u8],
        after_name: Option<&str>,
        limit: usize,
    ) -> StoreResult<Option<Vec<TreeEntry>>> {
        let Some(entries) = self.get_tree(project_id, tree_id).await? else {
            return Ok(None);
        };
        let iter = entries.into_iter().filter(|e| {
            after_name
                .filter(|a| !a.is_empty())
                .is_none_or(|after| e.name.as_bytes() > after.as_bytes())
        });
        Ok(Some(iter.take(limit).collect()))
    }
    async fn put_tree(
        &self,
        project_id: &str,
        tree_id: &[u8],
        entries: &[TreeEntry],
    ) -> StoreResult<()>;
    async fn find_existing_trees(
        &self,
        project_id: &str,
        tree_ids: &[Vec<u8>],
    ) -> StoreResult<HashSet<Vec<u8>>>;

    async fn get_symlink(&self, project_id: &str, symlink_id: &[u8])
    -> StoreResult<Option<String>>;
    async fn put_symlink(
        &self,
        project_id: &str,
        symlink_id: &[u8],
        target: &str,
    ) -> StoreResult<()>;
    async fn find_existing_symlinks(
        &self,
        project_id: &str,
        symlink_ids: &[Vec<u8>],
    ) -> StoreResult<HashSet<Vec<u8>>>;

    async fn get_blob_record(
        &self,
        project_id: &str,
        blob_id: &[u8],
    ) -> StoreResult<Option<BlobRecord>>;
    async fn get_blob_sizes_batch(
        &self,
        project_id: &str,
        blob_ids: &[Vec<u8>],
    ) -> StoreResult<HashMap<Vec<u8>, u64>>;
    async fn get_global_blob_record(&self, blob_id: &[u8]) -> StoreResult<Option<BlobRecord>>;
    /// Persists `record` in the global blob table and links `(project_id, record.blob_id)`
    /// atomically inside the backend's lock or transaction.
    ///
    /// When a global row for `record.blob_id` already exists, implementations must preserve
    /// existing `inline_content` and `primary_location` values (populating them from `record`
    /// only when absent) and union `record.replicated_locations` without dropping concurrent
    /// locations.
    async fn put_blob_record(&self, project_id: &str, record: &BlobRecord) -> StoreResult<()>;
    async fn add_blob_replica_location(&self, blob_id: &[u8], location: &str) -> StoreResult<()>;

    async fn get_operation(&self, repo_id: &str, op_id: &[u8]) -> StoreResult<Option<Operation>>;
    async fn put_operation(&self, repo_id: &str, op_id: &[u8], op: &Operation) -> StoreResult<()>;
    /// Persists an operation atomically after verifying `repo_id` is still bound to
    /// `expected_project_id` inside the same lock or transaction. When
    /// `require_dependencies_exist` is true and `op_id` is not already stored, also verifies
    /// inside the same lock or transaction that any non-root `op.view_id` exists in `Views`
    /// and every non-root `parent_id` in `op.parents` exists in `Operations`.
    async fn put_operation_for_project(
        &self,
        repo_id: &str,
        expected_project_id: &str,
        op_id: &[u8],
        op: &Operation,
        require_dependencies_exist: bool,
    ) -> StoreResult<()>;
    async fn resolve_operation_id_prefix(
        &self,
        repo_id: &str,
        min_id: &[u8],
        max_id: &[u8],
    ) -> StoreResult<Vec<OpId>>;

    async fn get_view(&self, repo_id: &str, view_id: &[u8]) -> StoreResult<Option<View>>;
    async fn has_view(&self, repo_id: &str, view_id: &[u8]) -> StoreResult<bool> {
        Ok(self.get_view(repo_id, view_id).await?.is_some())
    }
    async fn put_view(&self, repo_id: &str, view_id: &[u8], view: &View) -> StoreResult<()>;
    /// Persists a view atomically after verifying `repo_id` is still bound to
    /// `expected_project_id` inside the same lock or transaction.
    async fn put_view_for_project(
        &self,
        repo_id: &str,
        expected_project_id: &str,
        view_id: &[u8],
        view: &View,
    ) -> StoreResult<()>;

    async fn get_op_heads(&self, repo_id: &str) -> StoreResult<Option<Vec<OpId>>>;
    async fn update_op_heads(
        &self,
        repo_id: &str,
        old_ids: &[Vec<u8>],
        new_id: &[u8],
    ) -> StoreResult<Vec<OpId>>;
    /// Updates operation heads atomically after verifying `repo_id` is still bound to
    /// `expected_project_id` (and, when `require_op_exists` is true, that `new_head_id` exists
    /// in `Operations`) inside the same lock or transaction.
    async fn update_op_heads_for_project(
        &self,
        repo_id: &str,
        expected_project_id: &str,
        old_head_ids: &[Vec<u8>],
        new_head_id: &[u8],
        require_op_exists: bool,
    ) -> StoreResult<Vec<OpId>>;
    async fn publish_operation_atomic(
        &self,
        repo_id: &str,
        view_id: &[u8],
        view: &View,
        op_id: &[u8],
        op: &Operation,
        old_ids: &[Vec<u8>],
    ) -> StoreResult<Vec<OpId>>;
    /// Atomically persists `view`, `op`, and updated `OpHeads` after verifying `repo_id` is still
    /// bound to `expected_project_id` inside the same lock or transaction. When
    /// `require_dependencies_exist` is true and `op_id` is not already published, also verifies
    /// inside the same transaction that every non-root `parent_id` in `op.parents` exists in
    /// `Operations` and any external non-root `op.view_id != view_id` exists in `Views`.
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
    ) -> StoreResult<Vec<OpId>>;
    async fn compare_and_swap_op_heads(
        &self,
        repo_id: &str,
        expected_old_ids: &[Vec<u8>],
        new_id: &[u8],
    ) -> StoreResult<()>;
    /// Atomically compares and swaps `OpHeads` after verifying `repo_id` is still bound to
    /// `expected_project_id` (and, when `require_op_exists` is true, that `new_head_id` exists
    /// in `Operations`) inside the same lock or transaction.
    async fn update_op_heads_cas_for_project(
        &self,
        repo_id: &str,
        expected_project_id: &str,
        expected_exact_heads: &[Vec<u8>],
        new_head_id: &[u8],
        require_op_exists: bool,
    ) -> StoreResult<()>;

    async fn get_workspace(
        &self,
        repo_id: &str,
        workspace_name: &str,
    ) -> StoreResult<Option<WorkspaceState>>;
    async fn put_workspace(&self, workspace: &WorkspaceState) -> StoreResult<()> {
        self.put_workspace_cas(workspace, None).await
    }
    async fn put_workspace_cas(
        &self,
        workspace: &WorkspaceState,
        expected_operation_id: Option<&[u8]>,
    ) -> StoreResult<()>;
    /// Atomically updates a workspace with optional CAS after verifying `workspace.repo_id` is
    /// still bound to `expected_project_id` inside the same lock or transaction. When
    /// `require_operation_exists` is true and `workspace.operation_id` is non-empty and
    /// non-root, also verifies inside the same lock or transaction that `workspace.operation_id`
    /// exists in `Operations`.
    async fn put_workspace_cas_for_project(
        &self,
        expected_project_id: &str,
        workspace: &WorkspaceState,
        expected_operation_id: Option<&[u8]>,
        require_operation_exists: bool,
    ) -> StoreResult<()>;
    async fn list_workspaces(&self, repo_id: &str) -> StoreResult<Vec<WorkspaceState>>;
    /// Returns up to `limit` workspaces strictly after `after_workspace_name` in
    /// `workspace_name` UTF-8 byte order, stopping early once adding another record would exceed
    /// `max_bytes` (always returning at least one record when one exists) alongside a `has_more`
    /// flag.
    async fn list_workspaces_page(
        &self,
        repo_id: &str,
        after_workspace_name: Option<&str>,
        limit: usize,
        max_bytes: Option<usize>,
    ) -> StoreResult<(Vec<WorkspaceState>, bool)> {
        use prost::Message as _;

        if limit == 0 {
            return Ok((Vec::new(), false));
        }
        let mut all = self.list_workspaces(repo_id).await?;
        all.sort_by(|a, b| a.workspace_name.cmp(&b.workspace_name));
        let iter = all.into_iter().filter(|w| match after_workspace_name {
            Some(after_ws) => w.workspace_name.as_str() > after_ws,
            None => true,
        });
        let mut page = Vec::new();
        let mut response_bytes = 0usize;
        let mut has_more = false;
        for ws in iter {
            let entry_bytes = ws.encoded_len().saturating_add(8);
            if page.len() >= limit
                || (max_bytes.is_some_and(|cap| {
                    !page.is_empty() && response_bytes.saturating_add(entry_bytes) > cap
                }))
            {
                has_more = true;
                break;
            }
            response_bytes = response_bytes.saturating_add(entry_bytes);
            page.push(ws);
        }
        Ok((page, has_more))
    }
    async fn delete_workspace(&self, repo_id: &str, workspace_name: &str) -> StoreResult<bool>;
}

/// Bounded read window (4 MiB) used by the default [`BlobStore::get_blob_range_stream`]
/// implementation when streaming blobs from backends without a native streaming reader.
pub const BLOB_READ_WINDOW_BYTES: u64 = 4 * 1024 * 1024;

/// Default server-wide memory budget (256 MiB) for in-memory streaming upload staging buffers
/// and fallback blob reads.
pub const DEFAULT_STREAM_MEMORY_BUDGET_BYTES: usize = 256 * 1024 * 1024;

const MAX_WATERMARK_BYTES: usize = 256;

static FALLBACK_BLOB_UPLOAD_BUDGET: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(DEFAULT_STREAM_MEMORY_BUDGET_BYTES)));

pub(crate) fn expected_range_len(content_size: u64, offset: u64, length: Option<u64>) -> u64 {
    let remaining = content_size.saturating_sub(offset);
    match length {
        Some(0) | None => remaining,
        Some(n) => remaining.min(n),
    }
}

pub(crate) fn stream_inline_blob(
    blob_id: &[u8],
    content_size: u64,
    inline: Bytes,
    offset: u64,
    length: Option<u64>,
) -> StoreResult<Option<mpsc::Receiver<StoreResult<Bytes>>>> {
    if inline.len() as u64 != content_size {
        return Err(StoreError::Corruption(format!(
            "inline blob payload length {} does not match metadata content_size {} for file {}",
            inline.len(),
            content_size,
            hex::encode(blob_id)
        )));
    }
    let slice = slice_blob_range(&inline, offset, length)?;
    let (tx, rx) = mpsc::channel(8);
    if !slice.is_empty() {
        crate::resilience::spawn_blob_stream_producer(
            "inline blob stream",
            tx,
            move |tx| async move {
                let mut pos = 0usize;
                while pos < slice.len() {
                    let end = (pos + cc_common::FILE_STREAM_CHUNK_SIZE).min(slice.len());
                    if tx.send(Ok(slice.slice(pos..end))).await.is_err() {
                        return;
                    }
                    pos = end;
                }
            },
        );
    }
    Ok(Some(rx))
}

async fn read_bounded_file(path: &Path, expected_size: u64) -> StoreResult<Bytes> {
    let cap = usize::try_from(expected_size).map_err(|_| {
        StoreError::ResourceExhausted(format!(
            "staged blob size ({expected_size} bytes) exceeds addressable memory"
        ))
    })?;
    let file = tokio::fs::File::open(path).await.map_err(|e| {
        StoreError::Backend(format!(
            "failed to open staged blob {}: {e}",
            path.display()
        ))
    })?;
    let mut buf = Vec::with_capacity(cap);
    file.take(expected_size.saturating_add(1))
        .read_to_end(&mut buf)
        .await
        .map_err(|e| {
            StoreError::Backend(format!(
                "failed to read staged blob {}: {e}",
                path.display()
            ))
        })?;
    if buf.len() as u64 != expected_size {
        return Err(StoreError::Backend(format!(
            "staged blob {} has {} bytes, expected {expected_size}",
            path.display(),
            buf.len()
        )));
    }
    Ok(Bytes::from(buf))
}

#[async_trait]
pub trait BlobStore: Send + Sync + 'static {
    fn location_id(&self) -> &str;

    async fn put_blob(&self, blob_id: &[u8], data: Bytes) -> StoreResult<()>;

    /// Persists a blob staged in a local temporary file of exact byte length `size`.
    ///
    /// The default implementation guards reading into memory with a bounded permit budget
    /// before delegating to [`BlobStore::put_blob`]. Backends that can stream or persist
    /// directly from disk should override this method to avoid full-blob heap allocation.
    async fn put_blob_from_file(
        &self,
        blob_id: &[u8],
        temp_path: &Path,
        size: u64,
    ) -> StoreResult<()> {
        if size > DEFAULT_STREAM_MEMORY_BUDGET_BYTES as u64 {
            return Err(StoreError::ResourceExhausted(format!(
                "blob size {size} exceeds in-memory fallback upload budget ({DEFAULT_STREAM_MEMORY_BUDGET_BYTES} bytes)"
            )));
        }
        let permits = u32::try_from(size).map_err(|_| {
            StoreError::ResourceExhausted(format!(
                "blob size {size} exceeds permit budget capacity"
            ))
        })?;
        let _permit =
            crate::resilience::run_with_rpc_deadline("blob upload staging permit", async move {
                FALLBACK_BLOB_UPLOAD_BUDGET
                    .clone()
                    .acquire_many_owned(permits)
                    .await
                    .map_err(|_| StoreError::ResourceExhausted("upload budget closed".to_string()))
            })
            .await?;
        let data = crate::resilience::run_with_rpc_deadline(
            "blob upload staging read",
            read_bounded_file(temp_path, size),
        )
        .await?;
        self.put_blob(blob_id, data).await
    }

    /// Optional post-publish notification called by [`CompositeStore::put_file`] after
    /// `MetadataStore::put_blob_record` succeeds, so replication wrappers can fan out peer
    /// writes without racing ahead of the metadata row.
    async fn notify_blob_published(&self, _blob_id: &[u8], _data: Bytes) {}

    /// Optional post-publish notification called by [`CompositeStore::put_file_from_temp_file`]
    /// after `MetadataStore::put_blob_record` succeeds, avoiding full-blob memory buffering.
    async fn notify_blob_file_published(&self, _blob_id: &[u8], _size: u64) {}

    async fn get_blob(&self, blob_id: &[u8]) -> StoreResult<Option<Bytes>>;

    async fn get_blob_range(
        &self,
        blob_id: &[u8],
        offset: u64,
        length: Option<u64>,
    ) -> StoreResult<Option<Bytes>> {
        let Some(full) = self.get_blob(blob_id).await? else {
            return Ok(None);
        };
        slice_blob_range(&full, offset, length).map(Some)
    }

    /// Reads a blob slice using an already-fetched [`BlobRecord`] so replication-aware
    /// coordinators can route origin fallback without a second metadata lookup.
    async fn get_blob_range_with_record(
        &self,
        record: &BlobRecord,
        offset: u64,
        length: Option<u64>,
    ) -> StoreResult<Option<Bytes>> {
        self.get_blob_range(&record.blob_id, offset, length).await
    }

    /// Streams a byte range of `record` in bounded 64 KiB frames into a bounded channel.
    ///
    /// The default implementation fetches at most 4 MiB per window via
    /// [`BlobStore::get_blob_range_with_record`], validates returned byte counts against
    /// `record.content_size` to detect truncation or oversized payloads as
    /// [`StoreError::Corruption`], and terminates immediately if the receiver drops.
    async fn get_blob_range_stream(
        self: Arc<Self>,
        record: BlobRecord,
        offset: u64,
        length: Option<u64>,
    ) -> StoreResult<Option<mpsc::Receiver<StoreResult<Bytes>>>> {
        if offset > record.content_size {
            return Err(StoreError::OutOfRange(format!(
                "requested read at offset {offset}, file size is {}",
                record.content_size
            )));
        }
        if let Some(inline) = record.inline_content {
            return stream_inline_blob(
                &record.blob_id,
                record.content_size,
                inline,
                offset,
                length,
            );
        }

        let expected_len = expected_range_len(record.content_size, offset, length);
        let reading_to_eof = offset.saturating_add(expected_len) == record.content_size
            && matches!(length, None | Some(0));

        // Fetch the first bounded window up front so missing or immediately-truncated blobs
        // fail synchronously before the caller commits streaming response headers.
        let first_window_expected = expected_len.min(BLOB_READ_WINDOW_BYTES);
        let first_req_len = if first_window_expected == expected_len && reading_to_eof {
            Some(first_window_expected.saturating_add(1))
        } else if first_window_expected == 0 {
            Some(1)
        } else {
            Some(first_window_expected)
        };
        let first_bytes = match self
            .get_blob_range_with_record(&record, offset, first_req_len)
            .await
        {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return Ok(None),
            Err(StoreError::OutOfRange(msg)) => {
                return Err(StoreError::Corruption(format!(
                    "blob payload truncated in object store for file {}: {msg}",
                    hex::encode(&record.blob_id)
                )));
            }
            Err(err) => return Err(err),
        };
        if first_bytes.len() as u64 != first_window_expected {
            return Err(StoreError::Corruption(format!(
                "blob payload length mismatch for file {}: expected {first_window_expected} bytes at offset {offset}, got {} bytes",
                hex::encode(&record.blob_id),
                first_bytes.len()
            )));
        }

        let (tx, rx) = mpsc::channel(8);
        if expected_len == 0 {
            return Ok(Some(rx));
        }

        crate::resilience::spawn_blob_stream_producer(
            "blob range stream",
            tx,
            move |tx| async move {
                let mut pos = 0usize;
                while pos < first_bytes.len() {
                    let end = (pos + cc_common::FILE_STREAM_CHUNK_SIZE).min(first_bytes.len());
                    if tx.send(Ok(first_bytes.slice(pos..end))).await.is_err() {
                        return;
                    }
                    pos = end;
                }

                let mut cur_offset = offset + first_window_expected;
                let mut remaining = expected_len - first_window_expected;
                while remaining > 0 {
                    let win_expected = remaining.min(BLOB_READ_WINDOW_BYTES);
                    let is_final_eof_window = win_expected == remaining && reading_to_eof;
                    let req_len = if is_final_eof_window {
                        Some(win_expected.saturating_add(1))
                    } else {
                        Some(win_expected)
                    };
                    let window = match self
                        .get_blob_range_with_record(&record, cur_offset, req_len)
                        .await
                    {
                        Ok(Some(bytes)) => bytes,
                        Ok(None) => {
                            let _ = tx
                                .send(Err(StoreError::Corruption(format!(
                                    "blob payload missing in object store mid-stream for file {}",
                                    hex::encode(&record.blob_id)
                                ))))
                                .await;
                            return;
                        }
                        Err(StoreError::OutOfRange(msg)) => {
                            let _ = tx
                                .send(Err(StoreError::Corruption(format!(
                                    "blob payload truncated in object store mid-stream for file {}: {msg}",
                                    hex::encode(&record.blob_id)
                                ))))
                                .await;
                            return;
                        }
                        Err(err) => {
                            let _ = tx.send(Err(err)).await;
                            return;
                        }
                    };
                    if window.len() as u64 != win_expected {
                        let _ = tx
                            .send(Err(StoreError::Corruption(format!(
                                "blob payload length mismatch mid-stream for file {}: expected {win_expected} bytes at offset {cur_offset}, got {} bytes",
                                hex::encode(&record.blob_id),
                                window.len()
                            ))))
                            .await;
                        return;
                    }
                    let mut wpos = 0usize;
                    while wpos < window.len() {
                        let end = (wpos + cc_common::FILE_STREAM_CHUNK_SIZE).min(window.len());
                        if tx.send(Ok(window.slice(wpos..end))).await.is_err() {
                            return;
                        }
                        wpos = end;
                    }
                    cur_offset += win_expected;
                    remaining -= win_expected;
                }
            },
        );

        Ok(Some(rx))
    }

    async fn has_blob(&self, blob_id: &[u8]) -> StoreResult<bool> {
        Ok(self.get_blob(blob_id).await?.is_some())
    }
}

pub fn slice_blob_range(data: &Bytes, offset: u64, length: Option<u64>) -> StoreResult<Bytes> {
    let total_len = data.len() as u64;
    if offset > total_len {
        return Err(StoreError::OutOfRange(format!(
            "requested read at offset {offset}, file size is {total_len}"
        )));
    }
    if offset == total_len {
        return Ok(Bytes::new());
    }
    let end = match length {
        Some(0) | None => total_len,
        Some(max_len) => offset.saturating_add(max_len).min(total_len),
    };
    Ok(data.slice((offset as usize)..(end as usize)))
}

/// Computes inclusive `[min_bytes, max_bytes]` lexicographical bounds of length `id_len`
/// matching `hex_prefix` (supports both even and odd nibble lengths). Returns `None` for a
/// valid hex prefix longer than an ID, which matches no ID.
pub fn hex_prefix_byte_bounds(
    hex_prefix: &str,
    id_len: usize,
) -> StoreResult<Option<(Vec<u8>, Vec<u8>)>> {
    if hex_prefix.len() > id_len * 2 {
        for b in hex_prefix.bytes() {
            decode_hex_nibble(b)?;
        }
        return Ok(None);
    }
    let mut min_bytes = vec![0u8; id_len];
    let mut max_bytes = vec![0xffu8; id_len];

    let bytes = hex_prefix.as_bytes();
    let full_bytes = bytes.len() / 2;
    for i in 0..full_bytes {
        let hi = decode_hex_nibble(bytes[2 * i])?;
        let lo = decode_hex_nibble(bytes[2 * i + 1])?;
        let b = (hi << 4) | lo;
        min_bytes[i] = b;
        max_bytes[i] = b;
    }
    if bytes.len() % 2 == 1 {
        let hi = decode_hex_nibble(bytes[bytes.len() - 1])?;
        min_bytes[full_bytes] = hi << 4;
        max_bytes[full_bytes] = (hi << 4) | 0x0f;
    }
    Ok(Some((min_bytes, max_bytes)))
}

/// Normalizes a `ChangeId` prefix given in either jj `z-k` reverse-hex or standard
/// `0-9a-f` hex into lowercase forward hexadecimal digits.
pub fn normalize_change_id_prefix_to_hex(prefix: &str) -> StoreResult<String> {
    if prefix.is_empty() {
        return Ok(String::new());
    }
    let all_rev_hex = prefix
        .bytes()
        .all(|b| matches!(b, b'k'..=b'z' | b'K'..=b'Z'));
    if all_rev_hex {
        let mut out = String::with_capacity(prefix.len());
        for b in prefix.bytes() {
            let lower = b.to_ascii_lowercase();
            let nibble = b'z' - lower;
            out.push(char::from_digit(u32::from(nibble), 16).unwrap());
        }
        return Ok(out);
    }
    let all_std_hex = prefix.bytes().all(|b| b.is_ascii_hexdigit());
    if all_std_hex {
        return Ok(prefix.to_ascii_lowercase());
    }
    Err(StoreError::InvalidArgument(format!(
        "invalid change ID prefix '{prefix}' (expected z-k reverse-hex or 0-9a-f hex)"
    )))
}

fn decode_hex_nibble(b: u8) -> StoreResult<u8> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(StoreError::InvalidArgument(format!(
            "invalid hex character '{}'",
            b as char
        ))),
    }
}
