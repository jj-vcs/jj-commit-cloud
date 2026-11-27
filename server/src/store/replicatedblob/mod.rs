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

//! Replication-aware `BlobStore` coordinator resolving Cloud Storage replication skew.
//!
//! # Authority and Crash-Safety Ordering
//! `ReplicatedBlobStore` writes to the local `BlobStore` before `CompositeStore` publishes
//! `BlobRecord` (`primary_location`, `replicated_locations`) in `MetadataStore`, and triggers
//! optional background peer fan-out only after `notify_blob_published` or
//! `notify_blob_file_published`. Cross-region read skew is bridged via: (1) an in-memory
//! byte-bounded LRU cache for blobs up to [`SMALL_BLOB_MAX_BYTES`] (larger ranges and streams
//! forward directly); (2) peer fallback on local misses, transient errors, or corruption, with
//! deduplicated background read-repair on clean local `404` misses; and (3) optional bounded
//! retry (off by default).
//!
//! # Concurrency and Merge Invariants
//! Concurrent misses coalesce via a detached per-digest singleflight `watch` channel so one
//! cancelled caller does not abort other waiters. Background read-repair and write fan-out
//! tasks are bounded by a shared semaphore (`DEFAULT_MAX_BACKGROUND_TASKS`).
//!
//! # Cross-Process and RPC Boundaries
//! Background read-repair and write fan-out run asynchronously without blocking foreground
//! `ReadFile` or `WriteFileStream` RPC latency.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{Semaphore, watch};
use tokio::time::Instant;
use tracing::warn;

use super::{
    BlobRecord, BlobStore, MetadataStore, StoreError, StoreResult, expected_range_len,
    slice_blob_range,
};

mod repair;
mod streaming;
#[cfg(test)]
mod tests;

/// Ranged reads of blobs up to this size fetch and cache the whole object. Ranged reads of
/// larger blobs are forwarded by range and bypass the cache.
pub const SMALL_BLOB_MAX_BYTES: u64 = 1024 * 1024;
/// Maximum concurrent background read-repair and peer write fan-out tasks.
pub const DEFAULT_MAX_BACKGROUND_TASKS: usize = 64;
/// Upper bound on per-read visibility-lag retry windows so extreme configs cannot overflow `Instant`.
const MAX_REPLICATION_RETRY_BUDGET: Duration = Duration::from_secs(3600);
/// Maximum exponential backoff step between visibility-lag retries.
const MAX_RETRY_BACKOFF: Duration = Duration::from_millis(250);

#[derive(Clone, Debug)]
pub struct ReplicatedBlobStoreConfig {
    /// Maximum total bytes retained in the in-memory read-through LRU cache.
    pub cache_capacity_bytes: usize,
    /// Total retry window for a metadata-confirmed blob that no store returns. Zero disables
    /// the retry: Cloud Storage buckets have read-after-write consistency and lag between
    /// buckets is handled by peer fallback.
    pub replication_retry_budget: Duration,
    /// Initial retry delay before exponential backoff.
    pub initial_retry_backoff: Duration,
    /// Replicate uploaded blobs to configured peer stores in the background.
    pub async_write_fanout: bool,
}

impl Default for ReplicatedBlobStoreConfig {
    fn default() -> Self {
        Self {
            cache_capacity_bytes: 64 * 1024 * 1024,
            replication_retry_budget: Duration::ZERO,
            initial_retry_backoff: Duration::from_millis(20),
            async_write_fanout: false,
        }
    }
}

#[derive(Debug, Default)]
pub struct ReplicationStats {
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub local_hits: AtomicU64,
    pub origin_fallbacks: AtomicU64,
    pub read_repairs_completed: AtomicU64,
    pub read_repairs_deduplicated: AtomicU64,
    pub visibility_lag_retries: AtomicU64,
    pub singleflight_coalesced: AtomicU64,
}

/// Fixed per-entry metadata overhead covering `HashMap` and `VecDeque` slots so tiny blobs
/// cannot grow the LRU tables without bound.
const LRU_ENTRY_OVERHEAD_BYTES: usize =
    std::mem::size_of::<(Vec<u8>, (Bytes, u64))>() + std::mem::size_of::<(Vec<u8>, u64)>();

fn entry_cost_bytes_for_len(blob_id_len: usize, data_len: usize) -> usize {
    data_len
        .saturating_add(blob_id_len.saturating_mul(2))
        .saturating_add(LRU_ENTRY_OVERHEAD_BYTES)
}

fn entry_cost_bytes(blob_id: &[u8], data: &Bytes) -> usize {
    entry_cost_bytes_for_len(blob_id.len(), data.len())
}

#[derive(Debug)]
struct ByteBoundedLruCache {
    capacity_bytes: usize,
    used_bytes: usize,
    entries: HashMap<Vec<u8>, (Bytes, u64)>,
    order: VecDeque<(Vec<u8>, u64)>,
    tick: u64,
}

impl ByteBoundedLruCache {
    fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes,
            used_bytes: 0,
            entries: HashMap::new(),
            order: VecDeque::new(),
            tick: 0,
        }
    }

    fn contains_key(&self, blob_id: &[u8]) -> bool {
        self.capacity_bytes > 0 && self.entries.contains_key(blob_id)
    }

    fn cached_len(&self, blob_id: &[u8]) -> Option<u64> {
        if self.capacity_bytes == 0 {
            return None;
        }
        self.entries.get(blob_id).map(|(data, _)| data.len() as u64)
    }

    fn get(&mut self, blob_id: &[u8]) -> Option<Bytes> {
        if self.capacity_bytes == 0 {
            return None;
        }
        let data = self.entries.get(blob_id)?.0.clone();
        self.tick = self.tick.wrapping_add(1);
        let gen_id = self.tick;
        if let Some(entry) = self.entries.get_mut(blob_id) {
            entry.1 = gen_id;
        }
        self.order.push_back((blob_id.to_vec(), gen_id));
        self.maybe_compact_order();
        Some(data)
    }

    fn insert(&mut self, blob_id: &[u8], data: Bytes) {
        let entry_cost = entry_cost_bytes(blob_id, &data);
        if self.capacity_bytes == 0 || entry_cost > self.capacity_bytes {
            return;
        }
        if let Some((prev, _)) = self.entries.remove(blob_id) {
            self.used_bytes = self
                .used_bytes
                .saturating_sub(entry_cost_bytes(blob_id, &prev));
        }
        while self.used_bytes.saturating_add(entry_cost) > self.capacity_bytes {
            let Some((evict_key, evict_gen)) = self.order.pop_front() else {
                break;
            };
            if let Some((_, current_gen)) = self.entries.get(&evict_key)
                && *current_gen == evict_gen
                && let Some((removed, _)) = self.entries.remove(&evict_key)
            {
                self.used_bytes = self
                    .used_bytes
                    .saturating_sub(entry_cost_bytes(&evict_key, &removed));
            }
        }
        self.tick = self.tick.wrapping_add(1);
        let gen_id = self.tick;
        self.used_bytes = self.used_bytes.saturating_add(entry_cost);
        self.entries.insert(blob_id.to_vec(), (data, gen_id));
        self.order.push_back((blob_id.to_vec(), gen_id));
        self.maybe_compact_order();
    }

    fn maybe_compact_order(&mut self) {
        if self.order.len() > self.entries.len().saturating_mul(4).max(64) {
            self.compact_order();
        }
    }

    fn compact_order(&mut self) {
        self.order.retain(|(k, g)| {
            self.entries
                .get(k)
                .is_some_and(|(_, active_gen)| active_gen == g)
        });
    }
}

type InflightResult = StoreResult<Option<Bytes>>;
type InflightMap = HashMap<Vec<u8>, watch::Receiver<Option<InflightResult>>>;

struct InflightGuard {
    inner: Arc<ReplicatedInner>,
    blob_id: Vec<u8>,
    tx: Option<watch::Sender<Option<InflightResult>>>,
}

impl InflightGuard {
    fn complete(mut self, outcome: InflightResult) {
        self.inner
            .inflight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.blob_id);
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(Some(outcome));
        }
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            self.inner
                .inflight
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&self.blob_id);
            let _ = tx.send(Some(Err(StoreError::Backend(
                "singleflight blob fetch task terminated before completion".to_string(),
            ))));
        }
    }
}

fn validate_full_blob_size(blob_id: &[u8], bytes: &Bytes, expected_size: u64) -> StoreResult<()> {
    if bytes.len() as u64 != expected_size {
        return Err(StoreError::Corruption(format!(
            "blob {} has {} bytes, recorded content_size is {expected_size}",
            hex::encode(blob_id),
            bytes.len()
        )));
    }
    Ok(())
}

/// Bytes of a blob that a fetch returns.
#[derive(Clone, Copy, Debug)]
enum ReadSpan {
    /// The whole object. Small results are cached, and peer hits start a read-repair.
    Full,
    /// `length` bytes from `offset`, clamped to the object. Results are neither cached nor
    /// repaired.
    Range { offset: u64, length: u64 },
}

impl ReadSpan {
    async fn read_from(
        self,
        store: &dyn BlobStore,
        blob_id: &[u8],
        known_record: Option<&BlobRecord>,
    ) -> StoreResult<Option<Bytes>> {
        match self {
            ReadSpan::Full => store.get_blob(blob_id).await,
            ReadSpan::Range { offset, length } => match known_record {
                Some(record) => {
                    store
                        .get_blob_range_with_record(record, offset, Some(length))
                        .await
                }
                None => store.get_blob_range(blob_id, offset, Some(length)).await,
            },
        }
    }

    fn slice(self, full: &Bytes) -> StoreResult<Bytes> {
        match self {
            ReadSpan::Full => Ok(full.clone()),
            ReadSpan::Range { offset, length } => slice_blob_range(full, offset, Some(length)),
        }
    }

    fn validate_size(self, blob_id: &[u8], bytes: &Bytes, record: &BlobRecord) -> StoreResult<()> {
        match self {
            ReadSpan::Full => validate_full_blob_size(blob_id, bytes, record.content_size),
            ReadSpan::Range { offset, length } => {
                let expected_len = expected_range_len(record.content_size, offset, Some(length));
                if bytes.len() as u64 != expected_len {
                    return Err(StoreError::Corruption(format!(
                        "blob {} length mismatch at offset {offset}: expected {expected_len} bytes, got {} bytes",
                        hex::encode(blob_id),
                        bytes.len()
                    )));
                }
                Ok(())
            }
        }
    }
}

/// Errors after which another attempt or another store may still succeed.
fn is_transient(err: &StoreError) -> bool {
    matches!(
        err,
        StoreError::Unavailable(_) | StoreError::ResourceExhausted(_)
    )
}

/// Errors observed across local and peer read attempts in one fetch round, keeping transient
/// failures distinct from permanent peer errors so retries only run when a store returned
/// not-found or hit a transient error.
#[derive(Default)]
struct AttemptErrors {
    transient: Option<StoreError>,
    non_transient: Option<StoreError>,
}

impl AttemptErrors {
    fn record(&mut self, err: StoreError) {
        if is_transient(&err) {
            self.transient = Some(err);
        } else {
            self.non_transient = Some(err);
        }
    }

    fn clear(&mut self) {
        self.transient = None;
        self.non_transient = None;
    }

    fn is_non_transient_only(&self) -> bool {
        self.non_transient.is_some() && self.transient.is_none()
    }

    fn into_error(self) -> Option<StoreError> {
        self.transient.or(self.non_transient)
    }
}

/// Bounded exponential backoff + deterministic jitter state shared by read and existence-check
/// visibility-lag retries.
struct VisibilityRetry {
    deadline: Instant,
    delay: Duration,
    jitter_seed: u64,
    attempt: u64,
}

impl VisibilityRetry {
    fn start(config: &ReplicatedBlobStoreConfig, blob_id: &[u8]) -> Option<Self> {
        if config.replication_retry_budget.is_zero() {
            return None;
        }
        let budget = config
            .replication_retry_budget
            .min(MAX_REPLICATION_RETRY_BUDGET);
        let deadline = Instant::now().checked_add(budget)?;
        let delay = config
            .initial_retry_backoff
            .clamp(Duration::from_millis(5), MAX_RETRY_BACKOFF);
        let jitter_seed = u64::from(blob_id.first().copied().unwrap_or(0));
        Some(Self {
            deadline,
            delay,
            jitter_seed,
            attempt: 0,
        })
    }

    async fn sleep_next(&mut self, stats: &ReplicationStats) -> bool {
        let now = Instant::now();
        if now >= self.deadline {
            return false;
        }
        let remaining = self.deadline.saturating_duration_since(now);
        // Deterministic per-blob + per-attempt jitter in [0ms, 7ms] avoids retry thundering herds.
        let jitter =
            Duration::from_millis(self.jitter_seed.wrapping_add(self.attempt.wrapping_mul(13)) % 8);
        let sleep_dur = self.delay.saturating_add(jitter).min(remaining);
        if sleep_dur.is_zero() {
            return false;
        }
        tokio::time::sleep(sleep_dur).await;
        self.attempt = self.attempt.wrapping_add(1);
        stats.visibility_lag_retries.fetch_add(1, Ordering::Relaxed);
        self.delay = self.delay.saturating_mul(2).min(MAX_RETRY_BACKOFF);
        true
    }
}

struct ReplicatedInner {
    local: Arc<dyn BlobStore>,
    peers: HashMap<String, Arc<dyn BlobStore>>,
    metadata: Arc<dyn MetadataStore>,
    cache: Mutex<ByteBoundedLruCache>,
    inflight: Mutex<InflightMap>,
    repairs_in_flight: Mutex<HashSet<Vec<u8>>>,
    fanouts_in_flight: Mutex<HashSet<(Vec<u8>, String)>>,
    bg_tasks: Arc<Semaphore>,
    config: ReplicatedBlobStoreConfig,
    stats: Arc<ReplicationStats>,
}

pub struct ReplicatedBlobStore {
    inner: Arc<ReplicatedInner>,
}

impl ReplicatedBlobStore {
    pub fn new(
        local: Arc<dyn BlobStore>,
        peers: Vec<Arc<dyn BlobStore>>,
        metadata: Arc<dyn MetadataStore>,
        config: ReplicatedBlobStoreConfig,
    ) -> Self {
        let local_loc = local.location_id().to_string();
        let mut peer_map = HashMap::new();
        for peer in peers {
            let loc = peer.location_id().to_string();
            if loc != local_loc {
                peer_map.insert(loc, peer);
            }
        }
        Self {
            inner: Arc::new(ReplicatedInner {
                local,
                peers: peer_map,
                metadata,
                cache: Mutex::new(ByteBoundedLruCache::new(config.cache_capacity_bytes)),
                inflight: Mutex::new(HashMap::new()),
                repairs_in_flight: Mutex::new(HashSet::new()),
                fanouts_in_flight: Mutex::new(HashSet::new()),
                bg_tasks: Arc::new(Semaphore::new(DEFAULT_MAX_BACKGROUND_TASKS)),
                config,
                stats: Arc::new(ReplicationStats::default()),
            }),
        }
    }

    pub fn stats(&self) -> &Arc<ReplicationStats> {
        &self.inner.stats
    }

    async fn fetch_full_coalesced(
        &self,
        blob_id: &[u8],
        known_record: Option<&BlobRecord>,
    ) -> StoreResult<Option<Bytes>> {
        let cacheable = match known_record {
            Some(record) => self.inner.can_cache_blob(blob_id, record.content_size),
            None => self.inner.config.cache_capacity_bytes > 0,
        };
        if cacheable
            && let Some(cached) = self
                .inner
                .cache
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(blob_id)
        {
            if let Some(record) = known_record {
                validate_full_blob_size(blob_id, &cached, record.content_size)?;
            }
            self.inner.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(cached));
        }

        let (mut rx, leader_task) = {
            let mut inflight = self
                .inner
                .inflight
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if cacheable
                && let Some(cached) = self
                    .inner
                    .cache
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(blob_id)
            {
                if let Some(record) = known_record {
                    validate_full_blob_size(blob_id, &cached, record.content_size)?;
                }
                self.inner.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(Some(cached));
            }
            if let Some(existing_rx) = inflight.get(blob_id) {
                if cacheable {
                    self.inner
                        .stats
                        .cache_misses
                        .fetch_add(1, Ordering::Relaxed);
                }
                self.inner
                    .stats
                    .singleflight_coalesced
                    .fetch_add(1, Ordering::Relaxed);
                (existing_rx.clone(), None)
            } else {
                if cacheable {
                    self.inner
                        .stats
                        .cache_misses
                        .fetch_add(1, Ordering::Relaxed);
                }
                let (tx, rx) = watch::channel(None);
                let blob_id_owned = blob_id.to_vec();
                inflight.insert(blob_id_owned.clone(), rx.clone());
                (rx, Some((tx, blob_id_owned)))
            }
        };

        if let Some((tx, blob_id_owned)) = leader_task {
            let inner = self.inner.clone();
            let record_owned = known_record.cloned();
            // Construct the guard after releasing `inflight` and before spawning so dropping
            // an unpolled task (or a spawn failure) still clears the inflight entry safely.
            let guard = InflightGuard {
                inner: inner.clone(),
                blob_id: blob_id_owned.clone(),
                tx: Some(tx),
            };
            // Run the leader fetch in a detached task so cancelling one caller's RPC
            // future does not abort the download for coalesced waiters.
            tokio::spawn(async move {
                let res = inner
                    .fetch_uncached(&blob_id_owned, record_owned.as_ref(), ReadSpan::Full)
                    .await;
                guard.complete(res);
            });
        }

        let _ = rx.wait_for(Option::is_some).await;
        let outcome = rx.borrow().clone();
        outcome.unwrap_or_else(|| {
            Err(StoreError::Unavailable(
                "singleflight leader channel closed without publishing blob state".to_string(),
            ))
        })
    }
}

enum LocalRead {
    Hit(Bytes),
    CleanMiss,
    Failed,
}

impl ReplicatedInner {
    fn can_cache_blob(&self, blob_id: &[u8], content_size: u64) -> bool {
        if self.config.cache_capacity_bytes == 0 || content_size > SMALL_BLOB_MAX_BYTES {
            return false;
        }
        usize::try_from(content_size).is_ok_and(|len| {
            entry_cost_bytes_for_len(blob_id.len(), len) <= self.config.cache_capacity_bytes
        })
    }

    fn insert_cache_if_small(&self, blob_id: &[u8], bytes: &Bytes) {
        if bytes.len() as u64 <= SMALL_BLOB_MAX_BYTES {
            self.cache
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(blob_id, bytes.clone());
        }
    }

    async fn fetch_uncached(
        self: &Arc<Self>,
        blob_id: &[u8],
        known_record: Option<&BlobRecord>,
        span: ReadSpan,
    ) -> StoreResult<Option<Bytes>> {
        // Check the local regional store first. Transient or local-corruption failures fall back
        // to peers without triggering read-repair onto a degraded or corrupt local object.
        let mut errors = AttemptErrors::default();
        let allow_read_repair = match self
            .read_local(blob_id, known_record, span, &mut errors)
            .await?
        {
            LocalRead::Hit(bytes) => return Ok(Some(bytes)),
            LocalRead::CleanMiss => true,
            LocalRead::Failed => false,
        };

        // Local returned 404 (or a transient/corruption error); consult MetadataStore to
        // distinguish genuine missing hashes from replication lag.
        let owned_record;
        let record = match known_record {
            Some(r) => r,
            None => {
                owned_record = self.metadata.get_global_blob_record(blob_id).await?;
                match &owned_record {
                    Some(r) => r,
                    // Unconfirmed hash: return None with zero retries.
                    None => return errors.into_error().map_or(Ok(None), Err),
                }
            }
        };

        if let Some(inline) = &record.inline_content {
            validate_full_blob_size(blob_id, inline, record.content_size)?;
            if matches!(span, ReadSpan::Full) {
                self.insert_cache_if_small(blob_id, inline);
            }
            return span.slice(inline).map(Some);
        }

        if let Some(bytes) = self
            .try_peer_fallback(blob_id, record, span, allow_read_repair, &mut errors)
            .await?
        {
            return Ok(Some(bytes));
        }

        // Optional bounded retry with backoff + jitter for object stores without
        // read-after-write consistency. Cloud Storage buckets need none, and lag between
        // buckets is handled by peer fallback above, so the default budget is zero. Skip retrying
        // when every failing attempt hit a non-transient error.
        if errors.is_non_transient_only() {
            return errors.into_error().map_or(Ok(None), Err);
        }
        let Some(mut retry) = VisibilityRetry::start(&self.config, blob_id) else {
            return errors.into_error().map_or(Ok(None), Err);
        };
        while retry.sleep_next(&self.stats).await {
            errors.clear();
            let allow_read_repair = match self
                .read_local(blob_id, Some(record), span, &mut errors)
                .await?
            {
                LocalRead::Hit(bytes) => return Ok(Some(bytes)),
                LocalRead::CleanMiss => true,
                LocalRead::Failed => false,
            };
            if let Some(bytes) = self
                .try_peer_fallback(blob_id, record, span, allow_read_repair, &mut errors)
                .await?
            {
                return Ok(Some(bytes));
            }
            if errors.is_non_transient_only() {
                return errors.into_error().map_or(Ok(None), Err);
            }
        }

        errors.into_error().map_or(Ok(None), Err)
    }

    /// Reads `span` from the local store. Transient errors, and local corruption when peers are
    /// configured, are recorded in `errors` and reported as [`LocalRead::Failed`] so the caller
    /// can try peers without triggering read-repair onto a degraded or corrupt local object.
    async fn read_local(
        &self,
        blob_id: &[u8],
        known_record: Option<&BlobRecord>,
        span: ReadSpan,
        errors: &mut AttemptErrors,
    ) -> StoreResult<LocalRead> {
        match span
            .read_from(self.local.as_ref(), blob_id, known_record)
            .await
        {
            Ok(Some(bytes)) => {
                if let Some(record) = known_record
                    && let Err(err) = span.validate_size(blob_id, &bytes, record)
                {
                    if !self.peers.is_empty() {
                        warn!(
                            "Local blob read returned corrupt size for {}, trying peers: {}",
                            hex::encode(blob_id),
                            err
                        );
                        errors.record(err);
                        return Ok(LocalRead::Failed);
                    }
                    return Err(err);
                }
                self.stats.local_hits.fetch_add(1, Ordering::Relaxed);
                if matches!(span, ReadSpan::Full) {
                    self.insert_cache_if_small(blob_id, &bytes);
                }
                Ok(LocalRead::Hit(bytes))
            }
            Ok(None) => Ok(LocalRead::CleanMiss),
            Err(err) if is_transient(&err) => {
                warn!(
                    "Local blob read failed for {}, trying peers: {}",
                    hex::encode(blob_id),
                    err
                );
                errors.record(err);
                Ok(LocalRead::Failed)
            }
            Err(StoreError::OutOfRange(msg)) => {
                let Some(record) = known_record else {
                    return Err(StoreError::OutOfRange(msg));
                };
                let err = StoreError::Corruption(format!(
                    "blob {} truncated in {} before expected size {}: {msg}",
                    hex::encode(blob_id),
                    self.local.location_id(),
                    record.content_size
                ));
                if !self.peers.is_empty() {
                    warn!(
                        "Local blob read detected truncation for {}, trying peers: {}",
                        hex::encode(blob_id),
                        err
                    );
                    errors.record(err);
                    return Ok(LocalRead::Failed);
                }
                Err(err)
            }
            Err(err @ StoreError::Corruption(_)) if !self.peers.is_empty() => {
                warn!(
                    "Local blob read detected corruption for {}, trying peers: {}",
                    hex::encode(blob_id),
                    err
                );
                errors.record(err);
                Ok(LocalRead::Failed)
            }
            Err(err) => Err(err),
        }
    }

    fn peer_candidate_locations<'a>(&self, record: &'a BlobRecord) -> Vec<&'a str> {
        let local_loc = self.local.location_id();
        let mut candidate_locations: Vec<&str> = Vec::new();
        if let Some(primary) = record.primary_location.as_deref()
            && primary != local_loc
        {
            candidate_locations.push(primary);
        }
        for loc in &record.replicated_locations {
            if loc != local_loc && !candidate_locations.contains(&loc.as_str()) {
                candidate_locations.push(loc.as_str());
            }
        }
        candidate_locations
    }

    async fn try_peer_fallback(
        self: &Arc<Self>,
        blob_id: &[u8],
        record: &BlobRecord,
        span: ReadSpan,
        allow_read_repair: bool,
        errors: &mut AttemptErrors,
    ) -> StoreResult<Option<Bytes>> {
        if self.peers.is_empty() {
            return Ok(None);
        }

        for loc in self.peer_candidate_locations(record) {
            let Some(peer_store) = self.peers.get(loc) else {
                warn!(
                    "BlobRecord for {} lists peer location '{}' not configured in ReplicatedBlobStore",
                    hex::encode(blob_id),
                    loc
                );
                continue;
            };
            match span
                .read_from(peer_store.as_ref(), blob_id, Some(record))
                .await
            {
                Ok(Some(bytes)) => {
                    if let Err(err) = span.validate_size(blob_id, &bytes, record) {
                        warn!(
                            "Peer blob fallback from {} returned corrupt size for {}: {}",
                            loc,
                            hex::encode(blob_id),
                            err
                        );
                        errors.record(err);
                        continue;
                    }
                    self.stats.origin_fallbacks.fetch_add(1, Ordering::Relaxed);
                    if matches!(span, ReadSpan::Full) {
                        self.insert_cache_if_small(blob_id, &bytes);
                        if allow_read_repair {
                            self.spawn_read_repair(blob_id.to_vec(), bytes.clone());
                        }
                    }
                    return Ok(Some(bytes));
                }
                Ok(None) => continue,
                Err(StoreError::OutOfRange(msg)) => {
                    let err = StoreError::Corruption(format!(
                        "blob {} truncated in {} before expected size {}: {msg}",
                        hex::encode(blob_id),
                        loc,
                        record.content_size
                    ));
                    warn!(
                        "Peer blob fallback from {} detected truncation for {}: {}",
                        loc,
                        hex::encode(blob_id),
                        err
                    );
                    errors.record(err);
                }
                Err(err) => {
                    warn!(
                        "Peer blob fallback from {} failed for {}: {}",
                        loc,
                        hex::encode(blob_id),
                        err
                    );
                    errors.record(err);
                }
            }
        }

        Ok(None)
    }
}
