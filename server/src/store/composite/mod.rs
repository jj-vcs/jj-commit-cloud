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

//! `CompositeStore` coordinator, namespace cache, and project/repository registration.
//!
//! # Authority and Crash-Safety Ordering
//! `CompositeStore` persists project and repository bindings in `MetadataStore` before
//! caching them or permitting object, operation, or workspace mutations.
//!
//! # Concurrency and Merge Invariants
//! Bounded FIFO in-memory caches (`registered_projects`, `repo_projects`) avoid repeated
//! `MetadataStore` lookups for immutable project bindings, and a shared semaphore bounds
//! concurrent streaming memory usage.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::{
    BlobStore, DEFAULT_STREAM_MEMORY_BUDGET_BYTES, MemoryBlobStore, MemoryMetadataStore,
    MetadataStore, StoreError, StoreResult,
};

mod objects;
mod ops;
mod workspace;

pub use ops::{apply_op_heads_mutation, op_heads_sets_equal};
pub(crate) use ops::{collect_non_root_op_parents, verify_optional_repo_project};
pub(crate) use workspace::check_workspace_cas_precondition;

#[derive(Clone, Debug)]
pub struct CompositeStoreConfig {
    /// Blobs with byte length `<= inline_blob_threshold` are stored inline in `MetadataStore`
    /// without an object-store roundtrip. `0` sends all non-empty blobs to `BlobStore`.
    pub inline_blob_threshold: usize,
    /// Enforce bottom-up dependency existence checks on tree, commit, operation, and op-head
    /// writes.
    pub validate_dependencies: bool,
}

impl Default for CompositeStoreConfig {
    fn default() -> Self {
        Self {
            inline_blob_threshold: 0,
            validate_dependencies: true,
        }
    }
}

const MAX_NAMESPACE_CACHE_ENTRIES: usize = 10_000;

const MAX_TREE_PAGE_SIZE: usize = 20_000;

pub(super) struct BoundedNamespaceCache<V> {
    map: HashMap<String, V>,
    order: VecDeque<String>,
    capacity: usize,
}

impl<V> BoundedNamespaceCache<V> {
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self {
            map: HashMap::new(),
            order: VecDeque::new(),
            capacity: capacity.max(1),
        }
    }

    pub(super) fn contains(&self, key: &str) -> bool {
        self.map.contains_key(key)
    }

    pub(super) fn insert(&mut self, key: String, value: V) {
        if let Some(existing) = self.map.get_mut(&key) {
            *existing = value;
            return;
        }
        if self.map.len() >= self.capacity
            && let Some(oldest) = self.order.pop_front()
        {
            self.map.remove(&oldest);
        }
        self.order.push_back(key.clone());
        self.map.insert(key, value);
    }
}

/// Coordinates `MetadataStore` and `BlobStore` operations with write-before-publish
/// crash safety and bottom-up dependency validation.
#[derive(Clone)]
pub struct CompositeStore {
    pub(super) metadata: Arc<dyn MetadataStore>,
    blobs: Arc<dyn BlobStore>,
    config: CompositeStoreConfig,
    registered_projects: Arc<RwLock<BoundedNamespaceCache<()>>>,
    stream_memory_budget: Arc<Semaphore>,
    stream_memory_budget_bytes: usize,
}

impl CompositeStore {
    pub fn new(metadata: Arc<dyn MetadataStore>, blobs: Arc<dyn BlobStore>) -> Self {
        Self::with_config(metadata, blobs, CompositeStoreConfig::default())
    }

    pub fn with_config(
        metadata: Arc<dyn MetadataStore>,
        blobs: Arc<dyn BlobStore>,
        config: CompositeStoreConfig,
    ) -> Self {
        Self {
            metadata,
            blobs,
            config,
            registered_projects: Arc::new(RwLock::new(BoundedNamespaceCache::with_capacity(
                MAX_NAMESPACE_CACHE_ENTRIES,
            ))),
            stream_memory_budget: Arc::new(Semaphore::new(DEFAULT_STREAM_MEMORY_BUDGET_BYTES)),
            stream_memory_budget_bytes: DEFAULT_STREAM_MEMORY_BUDGET_BYTES,
        }
    }

    pub fn with_stream_memory_budget(mut self, budget_bytes: usize) -> Self {
        self.stream_memory_budget = Arc::new(Semaphore::new(budget_bytes));
        self.stream_memory_budget_bytes = budget_bytes;
        self
    }

    pub fn stream_memory_budget(&self) -> &Arc<Semaphore> {
        &self.stream_memory_budget
    }

    pub fn try_acquire_stream_memory_permit(&self, bytes: usize) -> Option<OwnedSemaphorePermit> {
        let permits = u32::try_from(bytes).ok()?;
        self.stream_memory_budget
            .clone()
            .try_acquire_many_owned(permits)
            .ok()
    }

    pub fn in_memory() -> Self {
        Self::new(
            Arc::new(MemoryMetadataStore::new()),
            Arc::new(MemoryBlobStore::new()),
        )
    }

    pub fn metadata(&self) -> &Arc<dyn MetadataStore> {
        &self.metadata
    }

    pub fn blobs(&self) -> &Arc<dyn BlobStore> {
        &self.blobs
    }

    /// Resolves the `project_id` bound to `repo_id` directly from `MetadataStore`, returning
    /// `StoreError::RepoNotFound` when `repo_id` is not registered.
    pub async fn resolve_repo_project(&self, repo_id: &str, action: &str) -> StoreResult<String> {
        self.metadata
            .get_repo_project_id(repo_id)
            .await?
            .ok_or_else(|| StoreError::RepoNotFound {
                repo_id: repo_id.to_string(),
                action: action.to_string(),
            })
    }

    /// Resolves `namespace_id` for `BackendService` object operations: checks `Projects`
    /// first (`project_id`), then falls back to `Repositories(namespace_id).project_id` so
    /// repository-scoped callers (`jj-vfs`) can resolve shared project objects.
    pub async fn resolve_project_namespace(
        &self,
        namespace_id: &str,
        action: &str,
    ) -> StoreResult<String> {
        if self
            .registered_projects
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .contains(namespace_id)
        {
            return Ok(namespace_id.to_string());
        }
        if self.metadata.is_project_registered(namespace_id).await? {
            self.registered_projects
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .insert(namespace_id.to_string(), ());
            return Ok(namespace_id.to_string());
        }
        self.resolve_repo_project(namespace_id, action).await
    }

    pub async fn ensure_repo_registered(&self, repo_id: &str, action: &str) -> StoreResult<()> {
        let _ = self.resolve_repo_project(repo_id, action).await?;
        Ok(())
    }

    pub async fn verify_repo_project(
        &self,
        repo_id: &str,
        expected_project_id: &str,
        action: &str,
    ) -> StoreResult<()> {
        let actual = self.metadata.get_repo_project_id(repo_id).await?;
        verify_optional_repo_project(
            repo_id,
            actual.as_deref(),
            Some(expected_project_id),
            action,
        )
    }

    pub async fn register_project(&self, project_id: &str, name: Option<&str>) -> StoreResult<()> {
        self.metadata.register_project(project_id, name).await?;
        self.registered_projects
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(project_id.to_string(), ());
        Ok(())
    }

    pub async fn register_repo_in_project(
        &self,
        repo_id: &str,
        project_id: Option<&str>,
        name: Option<&str>,
    ) -> StoreResult<String> {
        let target_project = project_id
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or(repo_id);
        let bound_project = self
            .metadata
            .register_repo_in_project(repo_id, target_project, name)
            .await?;
        self.registered_projects
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(bound_project.clone(), ());
        Ok(bound_project)
    }

    pub async fn register_repo(&self, repo_id: &str, name: Option<&str>) -> StoreResult<()> {
        self.register_repo_in_project(repo_id, None, name)
            .await
            .map(|_| ())
    }

    pub async fn delete_repository(
        &self,
        repo_id: &str,
        expected_project_id: Option<&str>,
    ) -> StoreResult<bool> {
        let res = self
            .metadata
            .delete_repository(repo_id, expected_project_id)
            .await;
        if !matches!(
            res,
            Err(StoreError::FailedPrecondition(_) | StoreError::InvalidArgument(_))
        ) {
            crate::jj_lib_adapters::invalidate_repo_operation_cache(self, repo_id);
        }
        res
    }
}
