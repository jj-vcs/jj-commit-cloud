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

//! Byte-bounded L1 memory LRU, singleflight coalescing, and scope authorization state.
//!
//! # Authority and Crash-Safety Ordering
//! `ByteBoundedLru` caps memory by payload plus key overhead, skips objects larger than
//! `MAX_L1_OBJECT_BYTES`, and never serves shared CAS entries without a verified scope
//! authorization entry. `insert_verified` authorizes a scope only after at least one tier
//! persists a validated payload.
//!
//! # Concurrency and Merge Invariants
//! The `l1`, `authorized`, and `inflight` mutexes are never nested or held across blocking
//! disk I/O. Concurrent cold misses coalesce via [`SingleflightGuard`] keyed by
//! `(scope_hex, kind, id)` over a `watch` channel; dropping an unpublished leader removes
//! the entry so surviving waiters elect a new leader.

use std::collections::HashMap;
use std::fs;
use std::io::Read as _;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use cc_common::MAX_GRPC_MESSAGE_BYTES;
use lru::LruCache;
use tokio::sync::watch;

use super::l2::{
    discard_invalid_l2_entry, ensure_private_dir_within, is_regular_file, is_valid_object_id,
    l2_payload_is_valid, write_marker_private,
};
use super::{CacheInner, CacheScope, MAX_L1_OBJECT_BYTES, ObjectKind};

/// Fixed per-entry map/node overhead charged against the L1 byte budget.
pub(super) const L1_ENTRY_OVERHEAD_BYTES: usize = 64;

pub(super) const MAX_AUTHORIZED_CACHE_ENTRIES: usize = 65_536;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) enum L1Partition {
    Shared {
        upstream_hex: String,
    },
    Scoped {
        namespace_hex: String,
        scope_hex: String,
    },
}

impl L1Partition {
    fn key_bytes(&self) -> usize {
        match self {
            Self::Shared { upstream_hex } => upstream_hex.len(),
            Self::Scoped {
                namespace_hex,
                scope_hex,
            } => namespace_hex.len().saturating_add(scope_hex.len()),
        }
    }
}

/// `(partition, kind, id)`: scoped by `(namespace_hex, scope_hex)` for scoped kinds
/// (`Commit`, `View`, `Operation`), or by `upstream_hex` otherwise (`Tree`, `File`, `Symlink`).
pub(super) type L1Key = (L1Partition, ObjectKind, Vec<u8>);

pub(super) fn l1_key_overhead(key: &L1Key) -> usize {
    key.0
        .key_bytes()
        .saturating_add(key.2.len())
        .saturating_add(L1_ENTRY_OVERHEAD_BYTES)
}

pub(super) fn l1_entry_weight(key: &L1Key, val: &Bytes) -> usize {
    val.len().saturating_add(l1_key_overhead(key))
}

pub(super) struct ByteBoundedLru {
    pub(super) map: LruCache<L1Key, Bytes>,
    pub(super) current_bytes: usize,
    pub(super) max_bytes: usize,
}

impl ByteBoundedLru {
    pub(super) fn new(max_bytes: usize) -> Self {
        Self {
            map: LruCache::unbounded(),
            current_bytes: 0,
            max_bytes,
        }
    }

    pub(super) fn get(&mut self, key: &L1Key) -> Option<Bytes> {
        self.map.get(key).cloned()
    }

    pub(super) fn peek_len(&self, key: &L1Key) -> Option<u64> {
        self.map.peek(key).map(|b| b.len() as u64)
    }

    pub(super) fn put(&mut self, key: L1Key, val: Bytes) -> bool {
        let key_overhead = l1_key_overhead(&key);
        let weight = val.len().saturating_add(key_overhead);
        if self.max_bytes == 0 || val.len() > MAX_L1_OBJECT_BYTES || weight > self.max_bytes {
            if let Some(prev) = self.map.pop(&key) {
                let prev_weight = l1_entry_weight(&key, &prev);
                self.current_bytes = self.current_bytes.saturating_sub(prev_weight);
            }
            return false;
        }
        if let Some(prev) = self.map.put(key, val) {
            let prev_weight = prev.len().saturating_add(key_overhead);
            self.current_bytes = self.current_bytes.saturating_sub(prev_weight);
        }
        self.current_bytes = self.current_bytes.saturating_add(weight);
        while self.current_bytes > self.max_bytes {
            let Some((evicted_key, evicted_val)) = self.map.pop_lru() else {
                self.current_bytes = 0;
                break;
            };
            let evicted_weight = l1_entry_weight(&evicted_key, &evicted_val);
            self.current_bytes = self.current_bytes.saturating_sub(evicted_weight);
        }
        true
    }

    pub(super) fn evict_namespace(&mut self, namespace_hex: &str) {
        let doomed: Vec<L1Key> = self
            .map
            .iter()
            .filter_map(|(k, _)| match &k.0 {
                L1Partition::Scoped {
                    namespace_hex: ns, ..
                } if ns == namespace_hex => Some(k.clone()),
                _ => None,
            })
            .collect();
        for key in doomed {
            if let Some(val) = self.map.pop(&key) {
                let weight = l1_entry_weight(&key, &val);
                self.current_bytes = self.current_bytes.saturating_sub(weight);
            }
        }
    }
}

pub(super) type InflightKey = (u64, String, ObjectKind, Vec<u8>);

#[derive(Clone)]
pub(super) enum FlightOutcome {
    Ok(Bytes),
    Err(Arc<tonic::Status>),
}

/// Clones a [`tonic::Status`] preserving code, message, binary details, and trailing metadata.
pub(crate) fn clone_status(status: &tonic::Status) -> tonic::Status {
    tonic::Status::with_details_and_metadata(
        status.code(),
        status.message(),
        Bytes::copy_from_slice(status.details()),
        status.metadata().clone(),
    )
}

/// The leader publishes its fetched bytes or upstream error through the `watch` channel. A `watch`
/// keeps the last value, so a waiter that joined just before the leader finished still observes
/// the outcome. If the leader is cancelled/dropped before publishing, the channel closes with
/// `None` so surviving waiters can elect a new leader.
pub(super) type InflightMap = Mutex<HashMap<InflightKey, watch::Receiver<Option<FlightOutcome>>>>;

pub(super) struct SingleflightGuard<'a> {
    pub(super) inflight: &'a InflightMap,
    pub(super) key: InflightKey,
    pub(super) result: watch::Sender<Option<FlightOutcome>>,
}

impl SingleflightGuard<'_> {
    pub(super) fn publish(&self, fetched: &Bytes) {
        self.result
            .send_replace(Some(FlightOutcome::Ok(fetched.clone())));
    }

    pub(super) fn publish_err(&self, err: &tonic::Status) {
        self.result
            .send_replace(Some(FlightOutcome::Err(Arc::new(clone_status(err)))));
    }
}

impl Drop for SingleflightGuard<'_> {
    fn drop(&mut self) {
        // The sender drops after this, which closes the channel. Waiters that joined before
        // then see either the published outcome or, if the leader was cancelled before
        // publishing, a closed channel, and retry.
        self.inflight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.key);
    }
}

pub(super) enum Flight<'a> {
    Leader(SingleflightGuard<'a>),
    Waiter(watch::Receiver<Option<FlightOutcome>>),
}

/// `(namespace_hex, scope_hex, kind, id_bytes)` authorization key.
pub(super) type AuthKey = (String, String, ObjectKind, Vec<u8>);

impl CacheInner {
    pub(super) fn l1_key(scope: &CacheScope, kind: ObjectKind, id_bytes: &[u8]) -> L1Key {
        if kind.is_scoped() {
            (
                L1Partition::Scoped {
                    namespace_hex: scope.namespace_hex.clone(),
                    scope_hex: scope.scope_hex.clone(),
                },
                kind,
                id_bytes.to_vec(),
            )
        } else {
            (
                L1Partition::Shared {
                    upstream_hex: scope.upstream_hex.clone(),
                },
                kind,
                id_bytes.to_vec(),
            )
        }
    }

    pub(super) fn evict_namespace_memory(&self, namespace_hex: &str) {
        self.l1
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .evict_namespace(namespace_hex);
        let mut auth = self.authorized.lock().unwrap_or_else(|e| e.into_inner());
        let doomed: Vec<AuthKey> = auth
            .iter()
            .filter(|(k, _)| k.0 == namespace_hex)
            .map(|(k, _)| k.clone())
            .collect();
        for key in doomed {
            auth.pop(&key);
        }
    }

    pub(super) fn authorize_scope(&self, scope: &CacheScope, kind: ObjectKind, id_bytes: &[u8]) {
        if !is_valid_object_id(id_bytes)
            || kind.is_scoped()
            || self.is_pending_purge(&scope.namespace_hex)
        {
            return;
        }
        let guard = self.scope_io_lock.read().unwrap_or_else(|e| e.into_inner());
        self.authorize_scope_locked(scope, kind, id_bytes, &guard);
    }

    pub(super) fn authorize_scope_locked(
        &self,
        scope: &CacheScope,
        kind: ObjectKind,
        id_bytes: &[u8],
        _guard: &std::sync::RwLockReadGuard<'_, ()>,
    ) {
        if !is_valid_object_id(id_bytes)
            || kind.is_scoped()
            || self.is_pending_purge(&scope.namespace_hex)
        {
            return;
        }
        let key = (
            scope.namespace_hex.clone(),
            scope.scope_hex.clone(),
            kind,
            id_bytes.to_vec(),
        );
        let is_new = self
            .authorized
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .put(key, ())
            .is_none();
        let marker = self.scoped_path(scope, kind, id_bytes);
        if is_new
            && !is_regular_file(&marker)
            && let Some(parent) = marker.parent()
            && ensure_private_dir_within(&self.cache_dir, parent).is_ok()
        {
            let _ = write_marker_private(&marker);
        }
    }

    pub(super) fn is_authorized_in_memory(
        &self,
        scope: &CacheScope,
        kind: ObjectKind,
        id_bytes: &[u8],
    ) -> bool {
        if !is_valid_object_id(id_bytes) || self.is_pending_purge(&scope.namespace_hex) {
            return false;
        }
        if kind.is_scoped() {
            return true;
        }
        let key = (
            scope.namespace_hex.clone(),
            scope.scope_hex.clone(),
            kind,
            id_bytes.to_vec(),
        );
        self.authorized
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&key)
            .is_some()
    }

    pub(super) fn is_authorized(
        &self,
        scope: &CacheScope,
        kind: ObjectKind,
        id_bytes: &[u8],
    ) -> bool {
        if !is_valid_object_id(id_bytes) || self.is_pending_purge(&scope.namespace_hex) {
            return false;
        }
        if kind.is_scoped() {
            return true;
        }
        if self.is_authorized_in_memory(scope, kind, id_bytes) {
            return true;
        }
        let guard = self.scope_io_lock.read().unwrap_or_else(|e| e.into_inner());
        self.is_authorized_locked(scope, kind, id_bytes, &guard)
    }

    pub(super) fn is_authorized_locked(
        &self,
        scope: &CacheScope,
        kind: ObjectKind,
        id_bytes: &[u8],
        _guard: &std::sync::RwLockReadGuard<'_, ()>,
    ) -> bool {
        if !is_valid_object_id(id_bytes) || self.is_pending_purge(&scope.namespace_hex) {
            return false;
        }
        if kind.is_scoped() {
            return true;
        }
        let key = (
            scope.namespace_hex.clone(),
            scope.scope_hex.clone(),
            kind,
            id_bytes.to_vec(),
        );
        if self
            .authorized
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&key)
            .is_some()
        {
            return true;
        }
        let marker = self.scoped_path(scope, kind, id_bytes);
        if is_regular_file(&marker) {
            self.authorized
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .put(key, ());
            return true;
        }
        false
    }

    pub(super) fn put_write_through_inner(
        &self,
        scope: &CacheScope,
        kind: ObjectKind,
        id_bytes: &[u8],
        payload: &Bytes,
        expected_epoch: Option<u64>,
    ) -> bool {
        if !is_valid_object_id(id_bytes) {
            return false;
        }
        let inserted =
            self.insert_verified_with_epoch(scope, kind, id_bytes, payload, expected_epoch);
        if inserted {
            self.write_through_objects.fetch_add(1, Ordering::Relaxed);
        }
        inserted
    }

    /// Validates `payload`, persists to L2 and/or L1, and authorizes `scope` only if at least
    /// one cache tier retained the object. Returns `false` if validation fails, `purge_epoch`
    /// advanced past `expected_epoch`, or neither tier stored the payload.
    pub(super) fn insert_verified_with_epoch(
        &self,
        scope: &CacheScope,
        kind: ObjectKind,
        id_bytes: &[u8],
        payload: &Bytes,
        expected_epoch: Option<u64>,
    ) -> bool {
        if !is_valid_object_id(id_bytes) {
            return false;
        }
        if !l2_payload_is_valid(kind, id_bytes, payload) {
            tracing::warn!("not caching invalid {kind:?} {}", hex::encode(id_bytes));
            return false;
        }
        let guard = self.scope_io_lock.read().unwrap_or_else(|e| e.into_inner());
        if self.is_pending_purge(&scope.namespace_hex)
            || expected_epoch.is_some_and(|e| self.purge_epoch.load(Ordering::Acquire) != e)
        {
            return false;
        }
        let dst = self.object_path(scope, kind, id_bytes);
        let l2_written =
            if self.max_l2_object_bytes > 0 && (payload.len() as u64) <= self.max_l2_object_bytes {
                match self.write_l2_atomic(&dst, id_bytes, payload) {
                    Ok(()) => true,
                    Err(err) => {
                        self.l2_write_errors.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!("failed to write L2 cache entry {}: {err}", dst.display());
                        false
                    }
                }
            } else {
                false
            };
        let l1_inserted = self
            .l1
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .put(Self::l1_key(scope, kind, id_bytes), payload.clone());
        if !l2_written && !l1_inserted {
            return false;
        }
        self.authorize_scope_locked(scope, kind, id_bytes, &guard);
        true
    }

    pub(super) fn lookup_l1_if_authorized_in_memory(
        &self,
        scope: &CacheScope,
        kind: ObjectKind,
        id_bytes: &[u8],
    ) -> Option<Bytes> {
        if !self.is_authorized_in_memory(scope, kind, id_bytes) {
            return None;
        }
        let key = Self::l1_key(scope, kind, id_bytes);
        let hit = self
            .l1
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&key)?;
        self.l1_hits.fetch_add(1, Ordering::Relaxed);
        Some(hit)
    }

    pub(super) fn lookup_cached(
        &self,
        scope: &CacheScope,
        kind: ObjectKind,
        id_bytes: &[u8],
    ) -> Option<Bytes> {
        if !is_valid_object_id(id_bytes) {
            return None;
        }
        if let Some(hit) = self.lookup_l1_if_authorized_in_memory(scope, kind, id_bytes) {
            return Some(hit);
        }
        let guard = self.scope_io_lock.read().unwrap_or_else(|e| e.into_inner());
        if !self.is_authorized_locked(scope, kind, id_bytes, &guard) {
            return None;
        }
        let key = Self::l1_key(scope, kind, id_bytes);
        if let Some(hit) = self.l1.lock().unwrap_or_else(|e| e.into_inner()).get(&key) {
            self.l1_hits.fetch_add(1, Ordering::Relaxed);
            return Some(hit);
        }
        let path = self.object_path(scope, kind, id_bytes);
        let Ok(mut file) = fs::File::open(&path) else {
            return None;
        };
        let Ok(meta) = file.metadata() else {
            return None;
        };
        if !meta.is_file() {
            return None;
        }
        // Large `File` blobs (> `MAX_L1_OBJECT_BYTES`) must never be loaded into RAM; callers
        // stream them directly from L2 disk CAS via `read_cached_file_range`. Non-file unary
        // messages are still bounded by `MAX_GRPC_MESSAGE_BYTES`.
        let max_in_memory_len = if kind == ObjectKind::File {
            MAX_L1_OBJECT_BYTES as u64
        } else {
            MAX_GRPC_MESSAGE_BYTES as u64
        };
        let total_len = meta.len();
        if total_len > max_in_memory_len {
            return None;
        }
        let mut bytes = Vec::with_capacity(total_len as usize);
        if (&mut file)
            .take(max_in_memory_len.saturating_add(1))
            .read_to_end(&mut bytes)
            .is_err()
        {
            return None;
        }
        if (bytes.len() as u64) > max_in_memory_len {
            return None;
        }
        if !l2_payload_is_valid(kind, id_bytes, &bytes) {
            discard_invalid_l2_entry(&path);
            return None;
        }
        if self.is_pending_purge(&scope.namespace_hex) {
            return None;
        }
        self.l2_hits.fetch_add(1, Ordering::Relaxed);
        let b = Bytes::from(bytes);
        self.l1
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .put(key, b.clone());
        Some(b)
    }
}
