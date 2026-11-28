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

//! Two-tier (L1 byte-bounded memory LRU + L2 local disk CAS) immutable object cache
//! with `(upstream_url, namespace_id, caller_auth_fingerprint)` scoping and singleflight.
//!
//! # Authority and Crash-Safety Ordering
//! Shared `Tree`, `Symlink`, and `File` payloads (`objects/<upstream_hex>/...`) and scoped
//! `Commit`, `View`, and `Operation` files (`scopes/<scope_hex>/...`) validate before
//! caching, write to a process-unique temp file (`0600` on Unix), call `sync_all`, and
//! link or rename into place. Authorization markers (`0600` under `0700` dirs) are created
//! only after L1 or L2 succeeds. Reads discard empty, torn, or invalid L2 entries.
//!
//! # Concurrency and Merge Invariants
//! `Commit`, `View`, and `Operation` objects are scope-partitioned (`Commit` payloads carry
//! unhashed `predecessors` and `external_ids` and are cached only from authoritative reads).
//! Concurrent misses within a [`CacheScope`] coalesce via [`SingleflightGuard`] over a `watch`
//! channel; callers with distinct credentials never coalesce or share authorization. Async
//! helpers offload blocking disk I/O onto `tokio::task::spawn_blocking`.
//!
//! # Cross-Process and RPC Boundaries
//! File blobs up to [`MAX_L1_OBJECT_BYTES`] (`4 MiB`) stay in L1 RAM as zero-copy [`Bytes`]
//! and are SHA-1 verified on every L2 read. Larger blobs never load into RAM via
//! `lookup_cached`: they spool through `<cache_dir>/staging` up to `max_l2_object_bytes` and
//! stream via `read_cached_file_range` in 64 KiB chunks over a bounded `mpsc` channel.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use lru::LruCache;
use serde::{Deserialize, Serialize};
use sha1::Digest;
use sha2::Sha256;
use tokio::sync::watch;

mod eviction;
mod l1;
mod l2;
mod spool;
#[cfg(test)]
mod tests;

pub(crate) use l1::clone_status;
pub use spool::BlobSpooler;

use eviction::cleanup_stale_staging_files;
use l1::{
    AuthKey, ByteBoundedLru, Flight, FlightOutcome, InflightKey, InflightMap,
    MAX_AUTHORIZED_CACHE_ENTRIES, SingleflightGuard,
};
use l2::{create_dir_all_private, ensure_private_dir_within, is_valid_object_id, set_dir_private};

/// Maximum single-object payload promoted into L1 RAM (`4 MiB`). Larger blobs live in L2 disk CAS.
pub const MAX_L1_OBJECT_BYTES: usize = 4 * 1024 * 1024;

/// Default maximum single-object payload spooled into L2 disk CAS (`512 MiB`). Larger blobs
/// stream directly between client and upstream without caching.
pub const DEFAULT_MAX_L2_OBJECT_BYTES: u64 = 512 * 1024 * 1024;

/// Streaming frame size (`64 KiB`) when serving `ReadFile` hits from L1/L2 cache.
pub const CACHE_STREAM_CHUNK_BYTES: usize = 64 * 1024;

/// Bounded per-stream channel depth (`8 * 64 KiB = 512 KiB` max buffered chunks).
const CACHE_STREAM_CHANNEL_CAPACITY: usize = 8;

const MAX_OBJECT_ID_BYTES: usize = 64;

/// Process-wide sequence counter for L2 staging temp files so multiple [`TwoTierCache`]
/// instances in the same process never collide on `.tmp.<pid>.<seq>.<hex_id>`.
static TEMP_SEQ: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjectKind {
    Commit,
    Tree,
    Symlink,
    File,
    View,
    Operation,
}

impl ObjectKind {
    pub fn dir_name(self) -> &'static str {
        match self {
            Self::Commit => "commits",
            Self::Tree => "trees",
            Self::Symlink => "symlinks",
            Self::File => "blobs",
            Self::View => "views",
            Self::Operation => "ops",
        }
    }

    /// Returns `true` for kinds stored under `scopes/<scope_hex>/...` and keyed by `scope_hex`
    /// in L1 rather than shared across scopes under `objects/<upstream_hex>/...`.
    ///
    /// `Commit` payloads include unhashed per-project metadata (`predecessors` and
    /// `external_ids`), while `View` and `Operation` are repository-scoped.
    pub fn is_scoped(self) -> bool {
        matches!(self, Self::Commit | Self::View | Self::Operation)
    }
}

/// Errors returned by [`TwoTierCache::read_cached_file_range`].
#[derive(Debug)]
pub enum CacheRangeError {
    OffsetOutOfRange { offset: u64, total_len: u64 },
    Io(std::io::Error),
}

impl fmt::Display for CacheRangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OffsetOutOfRange { offset, total_len } => {
                write!(f, "offset {offset} exceeds file length {total_len}")
            }
            Self::Io(err) => write!(f, "cache I/O error: {err}"),
        }
    }
}

impl std::error::Error for CacheRangeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::OffsetOutOfRange { .. } => None,
            Self::Io(err) => Some(err),
        }
    }
}

impl From<std::io::Error> for CacheRangeError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

/// Logical `(upstream_url, namespace_id, caller_auth_fingerprint)` isolation boundary
/// for cached lookups.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheScope {
    upstream_url: String,
    namespace_id: String,
    scope_hex: String,
    /// Partition of the shared CAS for `upstream_url`, so bytes cached for one server are
    /// never served to scopes of another server.
    upstream_hex: String,
}

impl CacheScope {
    pub fn new(upstream_url: impl AsRef<str>, namespace_id: impl AsRef<str>) -> Self {
        Self::new_with_auth(upstream_url, namespace_id, None)
    }

    pub fn new_with_auth(
        upstream_url: impl AsRef<str>,
        namespace_id: impl AsRef<str>,
        auth_header: Option<&str>,
    ) -> Self {
        let normalized_url = crate::upstream::normalize_origin(upstream_url.as_ref())
            .unwrap_or_else(|_| {
                upstream_url
                    .as_ref()
                    .trim()
                    .trim_end_matches('/')
                    .to_ascii_lowercase()
            });
        let namespace = namespace_id.as_ref().to_string();
        let auth_fingerprint = fingerprint_auth_header(auth_header);

        let mut scope_hasher = Sha256::new();
        scope_hasher.update((normalized_url.len() as u64).to_be_bytes());
        scope_hasher.update(normalized_url.as_bytes());
        scope_hasher.update((namespace.len() as u64).to_be_bytes());
        scope_hasher.update(namespace.as_bytes());
        scope_hasher.update(auth_fingerprint);
        let scope_hex = hex::encode(scope_hasher.finalize());
        let upstream_hex = hex::encode(sha256_bytes(normalized_url.as_bytes()));
        Self {
            upstream_url: normalized_url,
            namespace_id: namespace,
            scope_hex,
            upstream_hex,
        }
    }

    pub fn upstream_url(&self) -> &str {
        &self.upstream_url
    }

    pub fn namespace_id(&self) -> &str {
        &self.namespace_id
    }

    pub fn scope_hex(&self) -> &str {
        &self.scope_hex
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CacheStatsSnapshot {
    pub l1_hits: u64,
    pub l2_hits: u64,
    pub misses: u64,
    pub singleflight_coalesced: u64,
    pub write_through_objects: u64,
    pub upstream_rpcs: u64,
    pub l1_bytes: usize,
    pub l1_entries: usize,
    pub l2_write_errors: u64,
}

impl CacheStatsSnapshot {
    pub fn hit_ratio(&self) -> f64 {
        let hits = self.l1_hits.saturating_add(self.l2_hits);
        let total = hits.saturating_add(self.misses);
        if total == 0 {
            0.0
        } else {
            (hits as f64) / (total as f64)
        }
    }
}

struct CacheInner {
    cache_dir: PathBuf,
    max_l2_object_bytes: u64,
    l1: Mutex<ByteBoundedLru>,
    authorized: Mutex<LruCache<AuthKey, ()>>,
    inflight: InflightMap,
    l1_hits: AtomicU64,
    l2_hits: AtomicU64,
    misses: AtomicU64,
    singleflight_coalesced: AtomicU64,
    write_through_objects: AtomicU64,
    upstream_rpcs: AtomicU64,
    l2_write_errors: AtomicU64,
    started_at: Instant,
    last_activity_elapsed_ms: AtomicU64,
}

#[derive(Clone)]
pub struct TwoTierCache {
    inner: Arc<CacheInner>,
}

fn fingerprint_auth_header(auth_header: Option<&str>) -> [u8; 32] {
    match auth_header.map(str::trim).filter(|s| !s.is_empty()) {
        Some(trimmed) => {
            // Match `server/src/auth.rs`: split strictly on ASCII space `' '` and normalize
            // scheme casing and extra spaces only for the `Bearer` scheme so rejected headers
            // such as `"Bearer\t<token>"` never collide with `"Bearer <token>"`.
            let normalized = match trimmed.split_once(' ') {
                Some((scheme, creds)) if scheme.eq_ignore_ascii_case("bearer") => {
                    format!("bearer {}", creds.trim_start_matches(' '))
                }
                _ => trimmed.to_string(),
            };
            let mut hasher = Sha256::new();
            hasher.update(b"auth:");
            hasher.update(normalized.as_bytes());
            hasher.finalize().into()
        }
        None => sha256_bytes(b"unauthenticated"),
    }
}

/// SHA-256 digest used to derive collision-resistant 256-bit [`CacheScope`] directory keys.
fn sha256_bytes(input: &[u8]) -> [u8; 32] {
    Sha256::digest(input).into()
}

impl TwoTierCache {
    pub fn new(cache_dir: PathBuf, memory_cache_bytes: usize) -> std::io::Result<Self> {
        Self::new_with_l2_object_cap(cache_dir, memory_cache_bytes, DEFAULT_MAX_L2_OBJECT_BYTES)
    }

    pub fn new_with_l2_object_cap(
        cache_dir: PathBuf,
        memory_cache_bytes: usize,
        max_l2_object_bytes: u64,
    ) -> std::io::Result<Self> {
        create_dir_all_private(&cache_dir)?;
        set_dir_private(&cache_dir)?;
        let objects_dir = cache_dir.join("objects");
        let scopes_dir = cache_dir.join("scopes");
        let staging_dir = cache_dir.join("staging");
        ensure_private_dir_within(&cache_dir, &objects_dir)?;
        ensure_private_dir_within(&cache_dir, &scopes_dir)?;
        ensure_private_dir_within(&cache_dir, &staging_dir)?;
        cleanup_stale_staging_files(&staging_dir);
        Ok(Self {
            inner: Arc::new(CacheInner {
                cache_dir,
                max_l2_object_bytes,
                l1: Mutex::new(ByteBoundedLru::new(memory_cache_bytes)),
                authorized: Mutex::new(LruCache::new(
                    NonZeroUsize::new(MAX_AUTHORIZED_CACHE_ENTRIES).unwrap(),
                )),
                inflight: Mutex::new(HashMap::new()),
                l1_hits: AtomicU64::new(0),
                l2_hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
                singleflight_coalesced: AtomicU64::new(0),
                write_through_objects: AtomicU64::new(0),
                upstream_rpcs: AtomicU64::new(0),
                l2_write_errors: AtomicU64::new(0),
                started_at: Instant::now(),
                last_activity_elapsed_ms: AtomicU64::new(0),
            }),
        })
    }

    pub fn cache_dir(&self) -> &Path {
        &self.inner.cache_dir
    }

    pub fn staging_dir(&self) -> PathBuf {
        self.inner.cache_dir.join("staging")
    }

    pub fn max_l2_object_bytes(&self) -> u64 {
        self.inner.max_l2_object_bytes
    }

    pub fn touch_activity(&self) {
        self.inner.touch_activity();
    }

    pub fn idle_millis(&self) -> u64 {
        self.inner.idle_millis()
    }

    pub fn record_upstream_rpc(&self) {
        self.inner.upstream_rpcs.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_miss(&self) {
        self.inner.misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_l1_hit(&self) {
        self.inner.l1_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Creates a streaming blob spooler for a cold full `ReadFile`: tees into RAM up to
    /// [`MAX_L1_OBJECT_BYTES`], then spills to a staging file under `<cache_dir>/staging` up to
    /// `max_l2_object_bytes`, and disables spooling above the L2 cap.
    pub fn new_read_blob_spooler(&self) -> BlobSpooler {
        BlobSpooler::for_read(Arc::clone(&self.inner))
    }

    /// Creates a streaming blob spooler for `WriteFileStream`: spools chunks directly to a disk
    /// staging file under `<cache_dir>/staging` (with optional incremental SHA-1 hashing when
    /// `declared_total_size` is known) up to `max_l2_object_bytes`, stopping above the L2 cap
    /// without buffering the file in RAM.
    pub fn new_write_blob_spooler(&self, declared_total_size: Option<u64>) -> BlobSpooler {
        BlobSpooler::for_write(Arc::clone(&self.inner), declared_total_size)
    }

    pub fn stats(&self) -> CacheStatsSnapshot {
        let l1 = self.inner.l1.lock().unwrap_or_else(|e| e.into_inner());
        CacheStatsSnapshot {
            l1_hits: self.inner.l1_hits.load(Ordering::Relaxed),
            l2_hits: self.inner.l2_hits.load(Ordering::Relaxed),
            misses: self.inner.misses.load(Ordering::Relaxed),
            singleflight_coalesced: self.inner.singleflight_coalesced.load(Ordering::Relaxed),
            write_through_objects: self.inner.write_through_objects.load(Ordering::Relaxed),
            upstream_rpcs: self.inner.upstream_rpcs.load(Ordering::Relaxed),
            l1_bytes: l1.current_bytes,
            l1_entries: l1.map.len(),
            l2_write_errors: self.inner.l2_write_errors.load(Ordering::Relaxed),
        }
    }

    pub fn object_path(&self, scope: &CacheScope, kind: ObjectKind, id_bytes: &[u8]) -> PathBuf {
        self.inner.object_path(scope, kind, id_bytes)
    }

    /// Records that `scope` is authorized to read `(kind, id_bytes)` from the shared CAS,
    /// persisting a lightweight marker so warm L2 hits survive daemon restarts.
    pub fn authorize_scope(&self, scope: &CacheScope, kind: ObjectKind, id_bytes: &[u8]) {
        self.inner.authorize_scope(scope, kind, id_bytes);
    }

    pub fn is_authorized(&self, scope: &CacheScope, kind: ObjectKind, id_bytes: &[u8]) -> bool {
        self.inner.is_authorized(scope, kind, id_bytes)
    }

    /// Returns the byte length of `file_id` if already cached and authorized for `scope`
    /// without mutating hit/miss statistics.
    pub fn blob_size(&self, scope: &CacheScope, file_id: &[u8]) -> Option<u64> {
        self.inner.blob_size(scope, file_id)
    }

    /// Returns cached, authorized byte lengths for each `file_id` in `ids` inside a single
    /// blocking task without mutating hit/miss statistics.
    pub async fn blob_sizes_async(&self, scope: CacheScope, ids: Vec<Vec<u8>>) -> Vec<Option<u64>> {
        if ids.is_empty() {
            return Vec::new();
        }
        let len = ids.len();
        let inner = Arc::clone(&self.inner);
        match tokio::task::spawn_blocking(move || {
            ids.iter()
                .map(|id| inner.blob_size(&scope, id))
                .collect::<Vec<_>>()
        })
        .await
        {
            Ok(sizes) => sizes,
            Err(err) => {
                tracing::warn!("cache blob_sizes_async task failed: {err}");
                vec![None; len]
            }
        }
    }

    /// Validates `payload` and writes it through to L2 and L1, overwriting any existing entry
    /// for `(scope, kind, id_bytes)` (and evicting any stale L1 entry if `payload` exceeds the
    /// L1 budget).
    ///
    /// Use this for authoritative server responses and strictly content-addressed immutable
    /// objects (`File`, `Symlink`, `Tree`, `View`, `Operation`) whose id uniquely determines the
    /// payload bytes. Do not write-through `Commit` payloads on `WriteCommit`, because
    /// `predecessors` and `external_ids` are excluded from `commit_id` under first-writer-wins
    /// server semantics.
    pub fn put_write_through(
        &self,
        scope: &CacheScope,
        kind: ObjectKind,
        id_bytes: &[u8],
        payload: Bytes,
    ) {
        self.inner
            .put_write_through_inner(scope, kind, id_bytes, &payload);
    }

    /// Async wrapper around [`Self::put_write_through`] that runs blocking disk I/O inside a
    /// single `spawn_blocking` task, returning `true` if the payload was stored in L1 or L2.
    pub async fn put_write_through_async(
        &self,
        scope: CacheScope,
        kind: ObjectKind,
        id_bytes: Vec<u8>,
        payload: Bytes,
    ) -> bool {
        self.touch_activity();
        if !is_valid_object_id(&id_bytes) {
            return false;
        }
        let inner = Arc::clone(&self.inner);
        match tokio::task::spawn_blocking(move || {
            inner.put_write_through_inner(&scope, kind, &id_bytes, &payload)
        })
        .await
        {
            Ok(inserted) => inserted,
            Err(err) => {
                tracing::warn!("cache write-through task failed: {err}");
                false
            }
        }
    }

    pub fn lookup_cached(
        &self,
        scope: &CacheScope,
        kind: ObjectKind,
        id_bytes: &[u8],
    ) -> Option<Bytes> {
        self.inner.lookup_cached(scope, kind, id_bytes)
    }

    /// Joins the in-flight fetch for `key`, or registers the caller as its leader. The lookup
    /// and the registration happen under one lock, so exactly one caller leads each fetch.
    fn join_flight(&self, key: &InflightKey) -> Flight<'_> {
        let mut map = self
            .inner
            .inflight
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(existing) = map.get(key) {
            return Flight::Waiter(existing.clone());
        }
        let (result, receiver) = watch::channel(None);
        map.insert(key.clone(), receiver);
        Flight::Leader(SingleflightGuard {
            inflight: &self.inner.inflight,
            key: key.clone(),
            result,
        })
    }

    async fn lookup_cached_async(
        &self,
        scope: &CacheScope,
        kind: ObjectKind,
        id_bytes: &[u8],
    ) -> Result<Option<Bytes>, tonic::Status> {
        if !is_valid_object_id(id_bytes) {
            return Ok(None);
        }
        if let Some(hit) = self
            .inner
            .lookup_l1_if_authorized_in_memory(scope, kind, id_bytes)
        {
            return Ok(Some(hit));
        }
        let inner = Arc::clone(&self.inner);
        let scope_cloned = scope.clone();
        let id_vec = id_bytes.to_vec();
        tokio::task::spawn_blocking(move || inner.lookup_cached(&scope_cloned, kind, &id_vec))
            .await
            .map_err(|err| tonic::Status::internal(format!("cache lookup task failed: {err}")))
    }

    pub async fn get_or_fetch<F, Fut>(
        &self,
        scope: &CacheScope,
        kind: ObjectKind,
        id_bytes: &[u8],
        fetch_fn: F,
    ) -> Result<Bytes, tonic::Status>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Bytes, tonic::Status>>,
    {
        self.touch_activity();
        if !is_valid_object_id(id_bytes) {
            return Err(tonic::Status::invalid_argument(format!(
                "object id length {} must be between 1 and {MAX_OBJECT_ID_BYTES} bytes",
                id_bytes.len()
            )));
        }
        if let Some(hit) = self.lookup_cached_async(scope, kind, id_bytes).await? {
            return Ok(hit);
        }

        let inflight_key = (scope.scope_hex.clone(), kind, id_bytes.to_vec());
        let guard = loop {
            let mut waiter = match self.join_flight(&inflight_key) {
                Flight::Leader(guard) => break guard,
                Flight::Waiter(receiver) => receiver,
            };
            self.inner
                .singleflight_coalesced
                .fetch_add(1, Ordering::Relaxed);
            match waiter.wait_for(Option::is_some).await.map(|v| (*v).clone()) {
                Ok(Some(FlightOutcome::Ok(fetched))) => return Ok(fetched),
                Ok(Some(FlightOutcome::Err(err))) => return Err(clone_status(&err)),
                _ => {
                    // The leader was dropped or cancelled before publishing an outcome; retry,
                    // leading with this caller's own request if nobody else has taken over.
                }
            }
        };
        // A fetch that completed between the lookup above and `join_flight` already filled
        // the cache.
        match self.lookup_cached_async(scope, kind, id_bytes).await {
            Ok(Some(hit)) => {
                guard.publish(&hit);
                return Ok(hit);
            }
            Ok(None) => {}
            Err(err) => {
                guard.publish_err(&err);
                return Err(err);
            }
        }

        self.inner.misses.fetch_add(1, Ordering::Relaxed);
        let fetched = match fetch_fn().await {
            Ok(fetched) => fetched,
            Err(err) => {
                guard.publish_err(&err);
                return Err(err);
            }
        };
        let inner = Arc::clone(&self.inner);
        let scope_cloned = scope.clone();
        let id_vec = id_bytes.to_vec();
        let payload_cloned = fetched.clone();
        if let Err(err) = tokio::task::spawn_blocking(move || {
            inner.insert_verified(&scope_cloned, kind, &id_vec, &payload_cloned);
        })
        .await
        {
            tracing::warn!("cache write task failed: {err}");
        }
        guard.publish(&fetched);
        Ok(fetched)
    }
}

impl CacheInner {
    fn elapsed_ms(&self) -> u64 {
        u64::try_from(self.started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    fn touch_activity(&self) {
        self.last_activity_elapsed_ms
            .store(self.elapsed_ms(), Ordering::Relaxed);
    }

    fn idle_millis(&self) -> u64 {
        self.elapsed_ms()
            .saturating_sub(self.last_activity_elapsed_ms.load(Ordering::Relaxed))
    }
}
