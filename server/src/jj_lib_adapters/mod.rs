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

//! Server-side `jj_lib` storage adapters (`ServerBackend`, `ServerOpStore`,
//! `ServerOpHeadsStore`, and `ServerIndexStore`) backed by [`CompositeStore`].
//!
//! # Authority and Crash-Safety Ordering
//! When `jj_lib` resolves divergent operation heads on jj-cc-server, any synthesized
//! merged trees, commits, views, and operations are persisted through [`CompositeStore`]
//! (preserving write-before-publish ordering and bottom-up dependency validation) before
//! [`ServerOpHeadsStore::update_op_heads`] advances the operation head set.
//!
//! # Concurrency and Merge Invariants
//! [`ServerOpHeadsStore::with_expected_heads_for_project`] pins a validated head snapshot
//! and `project_id` and swaps it atomically via [`CompositeStore::compare_and_swap_op_heads_for_project`].
//! [`ServerIndex`] prunes `is_ancestor`, `heads`, and `common_ancestors` walks using commit
//! generation numbers, enforces per-walk and aggregate visit budgets, caches immutable
//! `(project_id, commit_id)` graph nodes across reconciles, and surfaces missing or malformed
//! rows as [`StoreError::Corruption`]. [`ServerOpStore`] validates persisted digests, pins
//! writes to `project_id`, and caches operations scoped by `(store, repo_id, project_id, op_id)`
//! with invalidation on repo delete.
//!
//! # Cross-Process and RPC Boundaries
//! Executes in-process inside jj-cc-server with zero loopback gRPC serialization.

#![allow(elided_lifetimes_in_paths)]

mod backend;
mod index;
mod op_heads;
mod op_store;
#[cfg(test)]
mod tests;

pub use backend::ServerBackend;
pub use index::{
    EmptyRevset, IndexWalkBudget, IndexWalkBudgetExceeded, IndexWalkLimits,
    MAX_ANCESTRY_WALK_COMMITS, MAX_RECONCILE_TOTAL_COMMIT_VISITS, ServerChangeIdIndex, ServerIndex,
    ServerIndexStore,
};
pub use op_heads::{ServerOpHeadsStore, ServerOpHeadsStoreNoLock};
pub use op_store::ServerOpStore;
pub(crate) use op_store::invalidate_repo_operation_cache;

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::{Arc, Weak};

use jj_lib::ref_name::WorkspaceName;
use jj_lib::submodule_store::SubmoduleStore;
use jj_lib::workspace_store::{WorkspaceStore, WorkspaceStoreError};

use crate::store::{CompositeStore, MetadataStore};

struct BoundedCache<K, V> {
    map: HashMap<K, V>,
    order: VecDeque<K>,
    capacity: usize,
}

impl<K: Eq + Hash + Clone, V> BoundedCache<K, V> {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            map: HashMap::new(),
            order: VecDeque::new(),
            capacity,
        }
    }

    fn get(&self, key: &K) -> Option<&V> {
        self.map.get(key)
    }

    fn insert(&mut self, key: K, value: V) {
        if let std::collections::hash_map::Entry::Occupied(mut occupied) =
            self.map.entry(key.clone())
        {
            occupied.insert(value);
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

    fn retain<F>(&mut self, mut predicate: F)
    where
        F: FnMut(&K, &V) -> bool,
    {
        self.map.retain(|k, v| predicate(k, v));
        self.order.retain(|k| self.map.contains_key(k));
    }
}

#[derive(Clone)]
struct StoreScopedEntry<V> {
    metadata_ref: Weak<dyn MetadataStore>,
    value: V,
}

fn store_cache_key(store: &CompositeStore) -> usize {
    Arc::as_ptr(store.metadata()).cast::<()>() as usize
}

fn store_entry_matches<V>(entry: &StoreScopedEntry<V>, store: &CompositeStore) -> bool {
    entry
        .metadata_ref
        .upgrade()
        .is_some_and(|arc| std::ptr::addr_eq(Arc::as_ptr(&arc), Arc::as_ptr(store.metadata())))
}

#[derive(Debug, Default)]
pub struct ServerSubmoduleStore;

impl SubmoduleStore for ServerSubmoduleStore {
    fn name(&self) -> &str {
        "server"
    }
}

/// No-op [`WorkspaceStore`] for server-side operations. The server never checks
/// out workspaces on disk, so there are no workspace paths to record.
#[derive(Debug, Default)]
pub struct ServerWorkspaceStore;

impl WorkspaceStore for ServerWorkspaceStore {
    fn name(&self) -> &str {
        "server"
    }

    fn add(
        &self,
        _workspace_name: &WorkspaceName,
        _path: &std::path::Path,
    ) -> Result<(), WorkspaceStoreError> {
        Ok(())
    }

    fn forget(&self, _workspace_names: &[&WorkspaceName]) -> Result<(), WorkspaceStoreError> {
        Ok(())
    }

    fn rename(
        &self,
        _old_name: &WorkspaceName,
        _new_name: &WorkspaceName,
    ) -> Result<(), WorkspaceStoreError> {
        Ok(())
    }

    fn get_workspace_path(
        &self,
        _workspace_name: &WorkspaceName,
    ) -> Result<Option<std::path::PathBuf>, WorkspaceStoreError> {
        Ok(None)
    }
}
