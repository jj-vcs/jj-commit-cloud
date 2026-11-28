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

//! Protocol-transparent gRPC handlers for `BackendService`, `OpStoreService`, and
//! `WorkspaceService` backed by [`TwoTierCache`] and [`UpstreamPool`].
//!
//! # Authority and Crash-Safety Ordering
//! Write RPCs for strictly content-addressed payloads (`WriteFile`, `WriteFileStream`,
//! `WriteSymlink`, `WriteTree`, `WriteView`, `WriteOperation`, and `PublishOperation`) succeed
//! on jj-cc-server before populating local L1/L2 cache; `WriteCommit` forwards without
//! write-through so `ReadCommit` always caches the server's authoritative first-writer commit.
//!
//! # Concurrency and Merge Invariants
//! Shared CAS objects (`Tree`, `File`, `Symlink`) authorize a [`CacheScope`] only on direct
//! upstream reads or direct uploads (never transitively from parent commits or trees). Cached
//! `TreeEntry` lists persist in UTF-8 byte order (`"foo" < "foo.rs"`); mutable head and
//! workspace RPCs forward straight through to jj-cc-server.
//!
//! # Cross-Process and RPC Boundaries
//! Outbound upstream clients match `cc_common::MAX_GRPC_MESSAGE_BYTES` (`64 MiB`). `ReadFile`
//! and `WriteFileStream` stream directly with bounded RAM (`<= 4 MiB`), spilling to disk
//! staging up to `max_l2_object_bytes` with no hard size cap; ranged cold `ReadFile` misses
//! and giant paginated `ReadTree` requests forward directly to upstream.

use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cc_common::MAX_GRPC_MESSAGE_BYTES;
use cc_common::backend::backend_service_client::BackendServiceClient;
use lru::LruCache;
use tonic::Status;
use tonic::metadata::MetadataMap;
use tonic::transport::Channel;

use crate::cache::{CacheScope, CacheStatsSnapshot, PendingPurgeGuard, TwoTierCache};
use crate::upstream::{UpstreamPool, extract_auth_header};

mod backend;
mod backend_file;
mod backend_tree;
mod op_store;
#[cfg(test)]
mod tests;
mod workspace;

/// Maximum entries returned in a single paginated `ReadTree` slice, matching `jj-cc-server`.
const MAX_TREE_PAGE_SIZE: usize = 20_000;

/// Maximum total entries buffered when assembling a paginated cold `ReadTree` response.
const MAX_CACHED_TREE_ENTRIES: usize = 500_000;

/// Bounded per-stream channel depth for streaming `ReadFile` and `WriteFileStream` forwarding.
const STREAM_CHANNEL_CAPACITY: usize = 8;

/// Maximum scope-isolated giant tree entries remembered for paginated pass-through.
const MAX_GIANT_TREES: usize = 1024;

/// Maximum normalized upstream origins remembered as rejecting paginated `ReadTree`.
const MAX_UNPAGINATED_TREE_ORIGINS: usize = 64;

/// Negative capability TTL for origins that rejected paginated `ReadTree` with `UNIMPLEMENTED`,
/// so upgraded servers automatically resume paginated fetching.
const UNPAGINATED_TREE_ORIGIN_TTL: Duration = Duration::from_secs(300);

/// Maximum length for `project_id` and `repo_id` identifiers, matching `jj-cc-server`.
const MAX_IDENTIFIER_LEN: usize = 128;

/// Process-wide sequence counter for stats temp filenames so multiple `DaemonProxy` instances
/// in the same process never collide on sibling temp paths.
static STATS_TMP_SEQ: AtomicU64 = AtomicU64::new(1);

/// Key for [`DaemonProxyInner::giant_trees`]: `(namespace_hex, scope_hex, tree_id)`.
type GiantTreeKey = (String, String, Vec<u8>);

#[derive(Clone)]
pub struct DaemonProxy {
    inner: Arc<DaemonProxyInner>,
}

struct DaemonProxyInner {
    cache: Arc<TwoTierCache>,
    upstream: Arc<UpstreamPool>,
    stats_path: Option<PathBuf>,
    started_at: Instant,
    /// Monotonic elapsed milliseconds since `started_at` of the last background stats flush
    /// (`u64::MAX` sentinel until the first flush).
    last_stats_flush_elapsed_ms: AtomicU64,
    stats_flush_in_flight: AtomicBool,
    /// Serializes `daemon.stats.json` writes and stores `closed` so late background flushes
    /// cannot race after shutdown.
    stats_io_lock: Mutex<bool>,
    active_rpcs: Arc<AtomicUsize>,
    max_cached_tree_entries: usize,
    max_cached_tree_bytes: usize,
    /// Scope-isolated cache of trees observed to exceed full-tree cache limits so follow-up
    /// paginated `ReadTree` calls forward straight to upstream. Keyed by
    /// `(namespace_hex, scope_hex, tree_id)` so namespace purges evict every caller variant.
    giant_trees: Mutex<LruCache<GiantTreeKey, ()>>,
    /// Per-normalized-origin negative capability cache of servers that rejected `page_size > 0`
    /// `ReadTree` with `UNIMPLEMENTED`, avoiding two sequential RPCs per cold unpaginated tree.
    unpaginated_tree_origins: Mutex<LruCache<String, Instant>>,
}

struct ActiveRpcGuard {
    active_rpcs: Arc<AtomicUsize>,
    cache: Arc<TwoTierCache>,
}

impl Drop for ActiveRpcGuard {
    fn drop(&mut self) {
        // Refresh idle timestamp before decrementing so the idle watchdog never fires
        // immediately when a long-running RPC finishes.
        self.cache.touch_activity();
        self.active_rpcs.fetch_sub(1, Ordering::AcqRel);
    }
}

fn write_stats_file(path: &Path, stats: &CacheStatsSnapshot) {
    let Ok(json) = serde_json::to_vec_pretty(stats) else {
        return;
    };
    let seq = STATS_TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let tmp = path.with_extension(format!("json.tmp.{}.{seq}", std::process::id()));
    let _ = std::fs::remove_file(&tmp);
    let res = (|| -> std::io::Result<()> {
        use std::io::Write as _;
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        {
            let mut file = opts.open(&tmp)?;
            file.write_all(&json)?;
        }
        std::fs::rename(&tmp, path)?;
        Ok(())
    })();
    if res.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
}

fn validate_identifier(id: &str, field: &str) -> Result<(), Status> {
    if id.is_empty() || id.len() > MAX_IDENTIFIER_LEN || id == "." || id == ".." {
        return Err(Status::invalid_argument(format!(
            "{field} must be 1..={MAX_IDENTIFIER_LEN} ASCII identifier characters (excluding '.' and '..')"
        )));
    }
    if !id
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    {
        return Err(Status::invalid_argument(format!(
            "{field} contains disallowed characters (allowed: ASCII alphanumeric, '-', '_', '.')"
        )));
    }
    Ok(())
}

fn backend_client(channel: Channel) -> BackendServiceClient<Channel> {
    BackendServiceClient::new(channel)
        .max_decoding_message_size(MAX_GRPC_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_GRPC_MESSAGE_BYTES)
}

impl DaemonProxy {
    pub fn new(
        cache: Arc<TwoTierCache>,
        upstream: Arc<UpstreamPool>,
        stats_path: Option<PathBuf>,
    ) -> Self {
        Self::new_with_tree_limits(
            cache,
            upstream,
            stats_path,
            MAX_CACHED_TREE_ENTRIES,
            MAX_GRPC_MESSAGE_BYTES,
        )
    }

    fn new_with_tree_limits(
        cache: Arc<TwoTierCache>,
        upstream: Arc<UpstreamPool>,
        stats_path: Option<PathBuf>,
        max_cached_tree_entries: usize,
        max_cached_tree_bytes: usize,
    ) -> Self {
        Self {
            inner: Arc::new(DaemonProxyInner {
                cache,
                upstream,
                stats_path,
                started_at: Instant::now(),
                last_stats_flush_elapsed_ms: AtomicU64::new(u64::MAX),
                stats_flush_in_flight: AtomicBool::new(false),
                stats_io_lock: Mutex::new(false),
                active_rpcs: Arc::new(AtomicUsize::new(0)),
                max_cached_tree_entries,
                max_cached_tree_bytes,
                giant_trees: Mutex::new(LruCache::new(NonZeroUsize::new(MAX_GIANT_TREES).unwrap())),
                unpaginated_tree_origins: Mutex::new(LruCache::new(
                    NonZeroUsize::new(MAX_UNPAGINATED_TREE_ORIGINS).unwrap(),
                )),
            }),
        }
    }

    fn track_active_rpc(&self) -> ActiveRpcGuard {
        self.inner.active_rpcs.fetch_add(1, Ordering::AcqRel);
        self.inner.cache.touch_activity();
        ActiveRpcGuard {
            active_rpcs: Arc::clone(&self.inner.active_rpcs),
            cache: Arc::clone(&self.inner.cache),
        }
    }

    pub fn active_rpcs(&self) -> usize {
        self.inner.active_rpcs.load(Ordering::Acquire)
    }

    pub fn flush_stats_now(&self) {
        let Some(path) = &self.inner.stats_path else {
            return;
        };
        let closed = self
            .inner
            .stats_io_lock
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if *closed {
            return;
        }
        let stats = self.inner.cache.stats();
        write_stats_file(path, &stats);
    }

    /// Stops further background stats flushes, waits for any in-flight flush to finish,
    /// and optionally writes one final snapshot before daemon exit.
    pub(crate) async fn shutdown_stats(&self, write_final: bool) {
        let proxy = self.clone();
        let _ = tokio::task::spawn_blocking(move || {
            let mut closed = proxy
                .inner
                .stats_io_lock
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *closed = true;
            if write_final && let Some(path) = &proxy.inner.stats_path {
                let stats = proxy.inner.cache.stats();
                write_stats_file(path, &stats);
            }
        })
        .await;
    }

    fn elapsed_ms(&self) -> u64 {
        // Reserve `u64::MAX` as the "never flushed" sentinel.
        u64::try_from(self.inner.started_at.elapsed().as_millis())
            .unwrap_or(u64::MAX - 1)
            .min(u64::MAX - 1)
    }

    fn maybe_flush_stats(&self) {
        let Some(path) = self.inner.stats_path.clone() else {
            return;
        };
        let now = self.elapsed_ms();
        let prev = self
            .inner
            .last_stats_flush_elapsed_ms
            .load(Ordering::Relaxed);
        if prev != u64::MAX && now.saturating_sub(prev) < 25 {
            return;
        }
        if self
            .inner
            .stats_flush_in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        if self
            .inner
            .last_stats_flush_elapsed_ms
            .compare_exchange(prev, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            self.inner
                .stats_flush_in_flight
                .store(false, Ordering::Release);
            return;
        }
        let proxy = self.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn_blocking(move || {
                struct ResetInFlight(Arc<DaemonProxyInner>);
                impl Drop for ResetInFlight {
                    fn drop(&mut self) {
                        self.0.stats_flush_in_flight.store(false, Ordering::Release);
                    }
                }
                let _reset = ResetInFlight(Arc::clone(&proxy.inner));
                let closed = proxy
                    .inner
                    .stats_io_lock
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                if *closed {
                    return;
                }
                let stats = proxy.inner.cache.stats();
                write_stats_file(&path, &stats);
            });
        } else {
            let closed = self
                .inner
                .stats_io_lock
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if !*closed {
                let stats = self.inner.cache.stats();
                write_stats_file(&path, &stats);
            }
            self.inner
                .stats_flush_in_flight
                .store(false, Ordering::Release);
        }
    }

    fn scope_for(&self, meta: &MetadataMap, repo_id: &str) -> Result<CacheScope, Status> {
        let target_url = self.inner.upstream.resolve_target_url(meta)?;
        let auth_header = extract_auth_header(meta)?;
        Ok(CacheScope::new_with_auth(
            target_url,
            repo_id,
            auth_header.as_deref(),
        ))
    }

    fn is_giant_tree(&self, key: &GiantTreeKey) -> bool {
        self.inner
            .giant_trees
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
            .is_some()
    }

    fn record_giant_tree(&self, key: GiantTreeKey, expected_epoch: u64) {
        let mut cache = self
            .inner
            .giant_trees
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if !self.inner.cache.is_pending_purge_hex(&key.0)
            && self.inner.cache.purge_epoch() == expected_epoch
        {
            cache.put(key, ());
        }
    }

    pub(super) fn purge_giant_trees_for_namespace(&self, upstream_url: &str, namespace_id: &str) {
        let namespace_hex = CacheScope::namespace_hex_for(upstream_url, namespace_id);
        let mut cache = self
            .inner
            .giant_trees
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let doomed: Vec<GiantTreeKey> = cache
            .iter()
            .filter(|(k, _)| k.0 == namespace_hex)
            .map(|(k, _)| k.clone())
            .collect();
        for key in doomed {
            cache.pop(&key);
        }
    }

    /// Purges L1/L2 namespace entries and authorization markers in `TwoTierCache` (advancing
    /// `purge_epoch`), then removes all matching `giant_trees` negative-cache entries so an
    /// in-flight `ReadTree` racing a delete or re-register cannot repopulate them.
    pub(super) async fn purge_namespace_state(&self, upstream_url: &str, namespace_id: &str) {
        self.inner
            .cache
            .purge_namespace(upstream_url, namespace_id)
            .await;
        self.purge_giant_trees_for_namespace(upstream_url, namespace_id);
    }

    pub(super) async fn purge_namespace_state_with_guard(
        &self,
        upstream_url: &str,
        namespace_id: &str,
        pending_guard: PendingPurgeGuard,
    ) {
        self.inner
            .cache
            .purge_namespace_with_guard(upstream_url, namespace_id, pending_guard)
            .await;
        self.purge_giant_trees_for_namespace(upstream_url, namespace_id);
    }

    fn is_unpaginated_tree_origin(&self, origin: &str) -> bool {
        let mut cache = self
            .inner
            .unpaginated_tree_origins
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let Some(&recorded_at) = cache.get(origin) else {
            return false;
        };
        if recorded_at.elapsed() < UNPAGINATED_TREE_ORIGIN_TTL {
            true
        } else {
            cache.pop(origin);
            false
        }
    }

    fn record_unpaginated_tree_origin(&self, origin: String) {
        self.inner
            .unpaginated_tree_origins
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .put(origin, Instant::now());
    }

    fn clear_unpaginated_tree_origin(&self, origin: &str) {
        self.inner
            .unpaginated_tree_origins
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pop(origin);
    }
}
