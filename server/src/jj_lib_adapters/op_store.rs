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

//! Server-side `jj_lib::op_store::OpStore` adapter backed by [`CompositeStore`].
//!
//! # Authority and Crash-Safety Ordering
//! Validates persisted operation and view digests on read, persists new views and
//! operations through [`CompositeStore`], and caches immutable decoded operations bounded
//! by store identity.

use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::SystemTime;

use async_trait::async_trait;
use cc_lib::cc_op_store::{
    operation_from_proto, operation_to_proto, view_from_proto, view_to_proto,
};
use jj_lib::object_id::{HexPrefix, ObjectId, PrefixResolution};
use jj_lib::op_store::{
    OpStore, OpStoreError, OpStoreResult, Operation, OperationId, View, ViewId,
};

use super::{BoundedCache, StoreScopedEntry, store_cache_key, store_entry_matches};
use crate::hash_utils::{hash_operation, hash_view};
use crate::store::{CompositeStore, IdPrefixResolution, StoreError};

const OPERATION_CACHE_CAPACITY: usize = 16_384;

pub(super) struct OperationCacheState {
    pub(super) entries:
        BoundedCache<(usize, String, String, OperationId), StoreScopedEntry<Operation>>,
    delete_generation: u64,
}

impl OperationCacheState {
    fn new(capacity: usize) -> Self {
        Self {
            entries: BoundedCache::with_capacity(capacity),
            delete_generation: 0,
        }
    }
}

/// Process-local bounded cache of immutable `Operation` payloads keyed by
/// `(store_identity, repo_id, project_id, op_id)`.
///
/// Multi-replica semantics:
/// - Single-replica `DeleteRepository` immediately evicts all entries for `(store, repo_id)` and
///   bumps `delete_generation` via [`invalidate_repo_operation_cache`].
/// - Across replicas, unpinned `ServerOpStore::new` resolves `project_id` on every read, so a
///   remote delete + cross-project re-registration changes `project_id` and misses immediately.
/// - Two residual multi-replica staleness cases remain until a follow-up schema migration adds a
///   monotonic repository generation column (or cache TTL): (a) `ServerOpStore::with_project_id`
///   (used during `reconcile_op_heads` for a single reconciliation pass) skips
///   `resolve_repo_project` on cache hits, though any resulting writes are still blocked by
///   transactional `*_for_project` checks; and (b) a remote delete + same-project re-registration
///   can still match an old `(store, repo_id, project_id, op_id)` entry on another replica until
///   eviction or process restart (writes remain protected by transactional `require_op_exists` /
///   `require_dependencies_exist` checks).
pub(super) static OPERATION_CACHE: LazyLock<Mutex<OperationCacheState>> =
    LazyLock::new(|| Mutex::new(OperationCacheState::new(OPERATION_CACHE_CAPACITY)));

/// Evicts cached operations for `(store, repo_id)` and advances the deletion generation so
/// concurrent reads started before `DeleteRepository` cannot re-populate deleted entries.
pub(crate) fn invalidate_repo_operation_cache(store: &CompositeStore, repo_id: &str) {
    let key = store_cache_key(store);
    let mut cache = OPERATION_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    cache.delete_generation = cache.delete_generation.wrapping_add(1);
    cache.entries.retain(|(s_key, r_id, _, _), entry| {
        (*s_key != key || r_id != repo_id) && entry.metadata_ref.strong_count() > 0
    });
}

fn validate_stored_operation(
    id: &OperationId,
    op: &cc_common::op_store::Operation,
) -> Result<(), StoreError> {
    if op.view_id.len() != cc_common::VIEW_ID_LENGTH {
        return Err(StoreError::Corruption(format!(
            "stored operation {} view_id must be {} bytes (got {} bytes)",
            id.hex(),
            cc_common::VIEW_ID_LENGTH,
            op.view_id.len()
        )));
    }
    for parent in &op.parents {
        if parent.len() != cc_common::OPERATION_ID_LENGTH {
            return Err(StoreError::Corruption(format!(
                "stored operation {} parent must be {} bytes (got {} bytes)",
                id.hex(),
                cc_common::OPERATION_ID_LENGTH,
                parent.len()
            )));
        }
    }
    for entry in &op.commit_predecessors {
        if entry.commit_id.len() != cc_common::COMMIT_ID_LENGTH {
            return Err(StoreError::Corruption(format!(
                "stored operation {} commit_predecessors.commit_id must be {} bytes (got {} bytes)",
                id.hex(),
                cc_common::COMMIT_ID_LENGTH,
                entry.commit_id.len()
            )));
        }
        for pred in &entry.predecessor_ids {
            if pred.len() != cc_common::COMMIT_ID_LENGTH {
                return Err(StoreError::Corruption(format!(
                    "stored operation {} commit_predecessors.predecessor_ids entry must be {} bytes (got {} bytes)",
                    id.hex(),
                    cc_common::COMMIT_ID_LENGTH,
                    pred.len()
                )));
            }
        }
    }
    Ok(())
}

fn validate_stored_ref_target(
    view_id: &ViewId,
    field: &str,
    target: &cc_common::op_store::RefTarget,
) -> Result<(), StoreError> {
    for term in target.adds.iter().chain(target.removes.iter()) {
        if term.commit_id.len() != cc_common::COMMIT_ID_LENGTH {
            return Err(StoreError::Corruption(format!(
                "stored view {} {field} commit_id must be {} bytes (got {} bytes)",
                view_id.hex(),
                cc_common::COMMIT_ID_LENGTH,
                term.commit_id.len()
            )));
        }
    }
    Ok(())
}

fn validate_stored_view(id: &ViewId, view: &cc_common::op_store::View) -> Result<(), StoreError> {
    for head in &view.head_ids {
        if head.len() != cc_common::COMMIT_ID_LENGTH {
            return Err(StoreError::Corruption(format!(
                "stored view {} head_ids entry must be {} bytes (got {} bytes)",
                id.hex(),
                cc_common::COMMIT_ID_LENGTH,
                head.len()
            )));
        }
    }
    for cid in view.wc_commit_ids.values() {
        if cid.len() != cc_common::COMMIT_ID_LENGTH {
            return Err(StoreError::Corruption(format!(
                "stored view {} wc_commit_ids entry must be {} bytes (got {} bytes)",
                id.hex(),
                cc_common::COMMIT_ID_LENGTH,
                cid.len()
            )));
        }
    }
    for target in view.local_bookmarks.values() {
        validate_stored_ref_target(id, "local_bookmarks", target)?;
    }
    for remote in view.remote_bookmarks.values() {
        if let Some(target) = &remote.target {
            validate_stored_ref_target(id, "remote_bookmarks", target)?;
        }
    }
    Ok(())
}

pub struct ServerOpStore {
    store: Arc<CompositeStore>,
    repo_id: String,
    pinned_project_id: Option<String>,
    root_operation_id: OperationId,
}

impl Debug for ServerOpStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerOpStore")
            .field("repo_id", &self.repo_id)
            .field("pinned_project_id", &self.pinned_project_id)
            .finish()
    }
}

impl ServerOpStore {
    pub fn new(store: Arc<CompositeStore>, repo_id: String) -> Self {
        Self {
            store,
            repo_id,
            pinned_project_id: None,
            root_operation_id: OperationId::from_bytes(&cc_common::ROOT_OPERATION_ID_BYTES),
        }
    }

    pub fn with_project_id(
        store: Arc<CompositeStore>,
        repo_id: String,
        project_id: String,
    ) -> Self {
        Self {
            store,
            repo_id,
            pinned_project_id: Some(project_id),
            root_operation_id: OperationId::from_bytes(&cc_common::ROOT_OPERATION_ID_BYTES),
        }
    }
}

#[async_trait]
impl OpStore for ServerOpStore {
    fn name(&self) -> &str {
        "server_op_store"
    }

    fn root_operation_id(&self) -> &OperationId {
        &self.root_operation_id
    }

    async fn read_operation(&self, id: &OperationId) -> OpStoreResult<Operation> {
        let project_id = match &self.pinned_project_id {
            Some(pid) => pid.clone(),
            None => self
                .store
                .resolve_repo_project(&self.repo_id, "reading operations")
                .await
                .map_err(|e| OpStoreError::Other(e.into()))?,
        };
        let cache_key = (
            store_cache_key(&self.store),
            self.repo_id.clone(),
            project_id.clone(),
            id.clone(),
        );
        let gen_before = {
            let cache = OPERATION_CACHE.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(entry) = cache.entries.get(&cache_key)
                && store_entry_matches(entry, &self.store)
            {
                return Ok(entry.value.clone());
            }
            cache.delete_generation
        };

        if let Some(op) = self
            .store
            .get_operation_for_project(&self.repo_id, &project_id, id.as_bytes())
            .await
            .map_err(|e| OpStoreError::Other(e.into()))?
        {
            validate_stored_operation(id, &op).map_err(|e| OpStoreError::Other(e.into()))?;
            let converted = operation_from_proto(op)?;
            let mut cache = OPERATION_CACHE.lock().unwrap_or_else(|e| e.into_inner());
            if cache.delete_generation == gen_before {
                cache.entries.insert(
                    cache_key,
                    StoreScopedEntry {
                        metadata_ref: Arc::downgrade(self.store.metadata()),
                        value: converted.clone(),
                    },
                );
            }
            return Ok(converted);
        }

        if id.as_bytes() == cc_common::ROOT_OPERATION_ID_BYTES {
            let root_op = cc_common::op_store::Operation {
                view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
                parents: vec![],
                metadata: Some(cc_common::op_store::OperationMetadata {
                    start_time_millis: 0,
                    end_time_millis: 0,
                    description: "root()".to_string(),
                    hostname: String::new(),
                    username: String::new(),
                    is_snapshot: false,
                    workspace_name: None,
                    attributes: HashMap::new(),
                }),
                commit_predecessors: vec![],
                commit_predecessors_set: true,
            };
            let converted = operation_from_proto(root_op)?;
            let mut cache = OPERATION_CACHE.lock().unwrap_or_else(|e| e.into_inner());
            if cache.delete_generation == gen_before {
                cache.entries.insert(
                    cache_key,
                    StoreScopedEntry {
                        metadata_ref: Arc::downgrade(self.store.metadata()),
                        value: converted.clone(),
                    },
                );
            }
            return Ok(converted);
        }

        Err(OpStoreError::ObjectNotFound {
            object_type: "operation".into(),
            hash: id.hex(),
            source: Some("operation not found in server store".into()),
        })
    }

    async fn write_operation(&self, operation: &Operation) -> OpStoreResult<OperationId> {
        let op_proto = operation_to_proto(operation);
        let op_id_bytes = hash_operation(&op_proto).map_err(|e| OpStoreError::Other(e.into()))?;
        let gen_before = OPERATION_CACHE
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .delete_generation;
        let project_id = match &self.pinned_project_id {
            Some(pid) => pid.clone(),
            None => self
                .store
                .resolve_repo_project(&self.repo_id, "writing operations")
                .await
                .map_err(|e| OpStoreError::Other(e.into()))?,
        };
        self.store
            .put_operation_for_project(&self.repo_id, &project_id, &op_id_bytes, &op_proto)
            .await
            .map_err(|e| OpStoreError::Other(e.into()))?;
        let op_id = OperationId::from_bytes(&op_id_bytes);
        let mut cache = OPERATION_CACHE.lock().unwrap_or_else(|e| e.into_inner());
        if cache.delete_generation == gen_before {
            cache.entries.insert(
                (
                    store_cache_key(&self.store),
                    self.repo_id.clone(),
                    project_id,
                    op_id.clone(),
                ),
                StoreScopedEntry {
                    metadata_ref: Arc::downgrade(self.store.metadata()),
                    value: operation.clone(),
                },
            );
        }
        Ok(op_id)
    }

    async fn read_view(&self, id: &ViewId) -> OpStoreResult<View> {
        let fetched = match &self.pinned_project_id {
            Some(pid) => self
                .store
                .get_view_for_project(&self.repo_id, pid, id.as_bytes())
                .await
                .map_err(|e| OpStoreError::Other(e.into()))?,
            None => self
                .store
                .get_view(&self.repo_id, id.as_bytes())
                .await
                .map_err(|e| OpStoreError::Other(e.into()))?,
        };
        if let Some(view) = fetched {
            validate_stored_view(id, &view).map_err(|e| OpStoreError::Other(e.into()))?;
            return view_from_proto(view);
        }

        if id.as_bytes() == cc_common::ROOT_VIEW_ID_BYTES {
            let root_view = cc_common::op_store::View {
                head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                wc_commit_ids: HashMap::new(),
                local_bookmarks: HashMap::new(),
                remote_bookmarks: HashMap::new(),
            };
            return view_from_proto(root_view);
        }

        Err(OpStoreError::ObjectNotFound {
            object_type: "view".into(),
            hash: id.hex(),
            source: Some("view not found in server store".into()),
        })
    }

    async fn write_view(&self, view: &View) -> OpStoreResult<ViewId> {
        let view_proto = view_to_proto(view);
        let view_id_bytes = hash_view(&view_proto).map_err(|e| OpStoreError::Other(e.into()))?;
        match &self.pinned_project_id {
            Some(pid) => {
                self.store
                    .put_view_for_project(&self.repo_id, pid, &view_id_bytes, &view_proto)
                    .await
                    .map_err(|e| OpStoreError::Other(e.into()))?;
            }
            None => {
                self.store
                    .put_view(&self.repo_id, &view_id_bytes, &view_proto)
                    .await
                    .map_err(|e| OpStoreError::Other(e.into()))?;
            }
        }
        Ok(ViewId::from_bytes(&view_id_bytes))
    }

    async fn resolve_operation_id_prefix(
        &self,
        prefix: &HexPrefix,
    ) -> OpStoreResult<PrefixResolution<OperationId>> {
        let hex_str = prefix.hex();
        let res = match &self.pinned_project_id {
            Some(pid) => self
                .store
                .resolve_operation_prefix_for_project(&self.repo_id, pid, &hex_str)
                .await
                .map_err(|e| OpStoreError::Other(e.into()))?,
            None => self
                .store
                .resolve_operation_prefix(&self.repo_id, &hex_str)
                .await
                .map_err(|e| OpStoreError::Other(e.into()))?,
        };
        Ok(match res {
            IdPrefixResolution::NoMatch => PrefixResolution::NoMatch,
            IdPrefixResolution::SingleMatch(id) => {
                if id.len() != cc_common::OPERATION_ID_LENGTH {
                    return Err(OpStoreError::Other(
                        StoreError::Corruption(format!(
                            "stored operation prefix match has invalid length {} (expected {})",
                            id.len(),
                            cc_common::OPERATION_ID_LENGTH
                        ))
                        .into(),
                    ));
                }
                PrefixResolution::SingleMatch(OperationId::from_bytes(&id))
            }
            IdPrefixResolution::AmbiguousMatch => PrefixResolution::AmbiguousMatch,
        })
    }

    async fn gc(
        &self,
        _target_op_ids: &[OperationId],
        _keep_newer: SystemTime,
    ) -> OpStoreResult<()> {
        Ok(())
    }
}
