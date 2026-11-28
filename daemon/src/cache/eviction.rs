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

//! Startup staging cleanup, namespace purging, and background L2 disk-quota LRU eviction.
//!
//! # Authority and Crash-Safety Ordering
//! On startup, `cleanup_stale_staging_files` unlinks `.tmp.<pid>.<seq>` files in
//! `<cache_dir>/staging` whose PID is provably dead (`ESRCH` on Unix), and
//! `prune_legacy_flat_scope_dirs` removes pre-v2 scope layouts. `evict_l2_to_budget` enforces
//! `max_disk_bytes` down to an 85% low watermark, evicting oversized files (`> low_watermark`)
//! first, then cold non-L1 files oldest-mtime-first, and L1-resident files last while skipping
//! active `.tmp*` files, `staging/`, and daemon state files.
//!
//! # Concurrency and Merge Invariants
//! `purge_namespace` holds `scope_io_lock.write()` and increments `purge_epoch` before clearing
//! on-disk and in-memory state. L2 eviction resets `l2_bytes_written_since_sweep` before walking,
//! never holds `l1` or `authorized` across filesystem I/O, preserves lock order
//! `scope_io_lock -> l1 -> authorized`, and prunes orphaned scope auth markers only after
//! re-checking L1 residency and on-disk shared object existence inside short
//! `scope_io_lock.write()` batches so concurrent writes and live L1 entries stay authorized.
//!
//! # Cross-Process and RPC Boundaries
//! Live PIDs' staging files stay untouched for shared `<cache_dir>` safety. Background sweeps
//! poll a cooperative shutdown flag across walks, deletions, and marker pruning so daemon exit
//! finishes before releasing `daemon.instance.lock`.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime};

use tokio::sync::watch;

use super::l1::{AuthKey, L1Partition};
use super::l2::is_regular_file;
use super::{CacheInner, CacheScope, ObjectKind, TwoTierCache};

/// Target usage percentage after an L2 disk-quota eviction sweep so sweeps do not thrash
/// on every subsequent write.
pub(super) const L2_EVICTION_LOW_WATERMARK_PERCENT: u64 = 85;

/// Computes the 85% low watermark target in bytes for `max_disk_bytes`.
pub(super) fn l2_low_watermark_bytes(max_disk_bytes: u64) -> u64 {
    (max_disk_bytes / 100)
        .saturating_mul(L2_EVICTION_LOW_WATERMARK_PERCENT)
        .saturating_add(
            ((max_disk_bytes % 100).saturating_mul(L2_EVICTION_LOW_WATERMARK_PERCENT)) / 100,
        )
}

/// Number of candidate orphaned scope auth markers verified and unlinked per
/// `scope_io_lock.write()` batch so concurrent readers and writers are never stalled for long.
const ORPHAN_PRUNE_BATCH_SIZE: usize = 32;

/// Interval between periodic background L2 disk-quota sweeps.
pub(super) const L2_EVICTION_PERIODIC_INTERVAL: Duration = Duration::from_secs(60);

/// Minimum cooldown between write-triggered background L2 sweeps so write bursts do not spin.
pub(super) const L2_EVICTION_MIN_COOLDOWN: Duration = Duration::from_secs(5);

/// Cap on the cumulative L2 write threshold that wakes the background eviction task early.
pub(super) const L2_EVICTION_MAX_WRITE_TRIGGER_BYTES: u64 = 64 * 1024 * 1024;

/// Summary returned by [`TwoTierCache::evict_l2_to_budget`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EvictionStats {
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub evicted_objects: u64,
    pub evicted_bytes: u64,
    pub orphaned_markers_removed: u64,
}

/// Best-effort startup sweep removing crash-orphaned `.tmp.<pid>.<seq>` staging files whose
/// owning PID is provably dead, while leaving files belonging to live daemons (which may share
/// the same `<cache_dir>`) untouched.
pub(super) fn cleanup_stale_staging_files(staging_dir: &Path) {
    #[cfg(unix)]
    {
        let Ok(entries) = fs::read_dir(staging_dir) else {
            return;
        };
        let current_pid = std::process::id();
        for entry in entries.flatten() {
            let Ok(ft) = entry.file_type() else {
                continue;
            };
            if !ft.is_file() {
                continue;
            }
            let file_name = entry.file_name();
            let Some(name) = file_name.to_str() else {
                continue;
            };
            let Some(pid) = parse_staging_temp_pid(name) else {
                continue;
            };
            if pid == current_pid || !is_pid_dead(pid) {
                continue;
            }
            let _ = fs::remove_file(entry.path());
        }
    }
    #[cfg(not(unix))]
    {
        let _ = staging_dir;
    }
}

#[cfg(unix)]
pub(super) fn parse_staging_temp_pid(name: &str) -> Option<u32> {
    let rest = name.strip_prefix(".tmp.")?;
    let (pid_str, seq_str) = rest.split_once('.')?;
    if seq_str.is_empty() || !seq_str.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let pid: u32 = pid_str.parse().ok()?;
    if pid == 0 {
        return None;
    }
    libc::pid_t::try_from(pid).ok().filter(|&p| p > 0)?;
    Some(pid)
}

#[cfg(unix)]
pub(super) fn is_pid_dead(pid: u32) -> bool {
    let Ok(pid_t) = libc::pid_t::try_from(pid) else {
        return false;
    };
    if pid_t <= 0 {
        return false;
    }
    // SAFETY: `kill(pid, 0)` with `pid > 0` delivers no signal and only probes process existence.
    let rc = unsafe { libc::kill(pid_t, 0) };
    rc == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
}

pub(super) const LEGACY_SCOPE_KIND_DIRS: [&str; 6] =
    ["commits", "trees", "symlinks", "blobs", "views", "ops"];

/// Removes pre-upgrade 1-level `scopes/<scope_hex>/<kind>/...` directories left behind before
/// the v2 `scopes/<namespace_hex>/<scope_hex>/<kind>/...` layout. New 2-level namespace
/// directories only contain 64-char hex `<scope_hex>` subdirectories, never direct
/// `ObjectKind` directory names.
pub(super) fn prune_legacy_flat_scope_dirs(scopes_dir: &Path) {
    let Ok(entries) = fs::read_dir(scopes_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(ft) = entry.file_type() else {
            continue;
        };
        if !ft.is_dir() {
            continue;
        }
        let path = entry.path();
        let is_legacy = LEGACY_SCOPE_KIND_DIRS
            .iter()
            .any(|kind_dir| fs::symlink_metadata(path.join(kind_dir)).is_ok());
        if !is_legacy {
            continue;
        }
        match fs::remove_dir_all(&path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                tracing::warn!(
                    "failed to prune legacy flat scope directory {}: {err}",
                    path.display()
                );
            }
        }
    }
}

/// RAII guard marking `(upstream_url, namespace_id)` as having an in-flight purge so L1/L2
/// reads, on-disk authorization lookups, and cache write-backs treat the namespace as uncached
/// until disk deletion finishes (or the guard drops).
pub struct PendingPurgeGuard {
    inner: Arc<CacheInner>,
    namespace_hex: String,
    armed: bool,
}

impl PendingPurgeGuard {
    fn release(&mut self) {
        if !self.armed {
            return;
        }
        self.armed = false;
        let mut pending = self
            .inner
            .pending_purges
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(count) = pending.get_mut(&self.namespace_hex) {
            if *count <= 1 {
                pending.remove(&self.namespace_hex);
            } else {
                *count -= 1;
            }
        }
    }
}

impl Drop for PendingPurgeGuard {
    fn drop(&mut self) {
        self.release();
    }
}

impl CacheInner {
    pub(super) fn purge_namespace_blocking(
        &self,
        namespace_hex: &str,
        mut pending_guard: Option<PendingPurgeGuard>,
    ) {
        let ns_dir = self.cache_dir.join("scopes").join(namespace_hex);
        let _write_guard = self
            .scope_io_lock
            .write()
            .unwrap_or_else(|e| e.into_inner());
        self.purge_epoch.fetch_add(1, Ordering::AcqRel);
        match fs::remove_dir_all(&ns_dir) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                tracing::warn!(
                    "failed to purge L2 cache scope directory {}: {err}",
                    ns_dir.display()
                );
            }
        }
        self.evict_namespace_memory(namespace_hex);
        if let Some(guard) = pending_guard.as_mut() {
            guard.release();
        }
    }
}

pub(super) struct EvictionCandidate {
    path: PathBuf,
    size_bytes: u64,
    modified: SystemTime,
    partition_idx: usize,
    kind: ObjectKind,
    hex_id: String,
    l1_resident: bool,
    evicted: bool,
}

pub(super) struct ScopeAuthMarker {
    path: PathBuf,
    namespace_hex: String,
    scope_hex: String,
    kind: ObjectKind,
    hex_id: String,
}

pub(super) fn collect_child_dirs(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter(|e| e.file_type().is_ok_and(|ft| ft.is_dir()))
        .map(|e| e.path())
        .collect()
}

pub(super) fn collect_sharded_files<F, C>(
    kind_dir: &Path,
    is_cancelled: &C,
    mut on_file: F,
) -> std::io::Result<()>
where
    F: FnMut(PathBuf, String, fs::Metadata),
    C: Fn() -> bool,
{
    if is_cancelled() {
        return Ok(());
    }
    let p1_entries = match fs::read_dir(kind_dir) {
        Ok(rd) => rd,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err),
    };
    for p1 in p1_entries.flatten() {
        if is_cancelled() {
            return Ok(());
        }
        if !p1.file_type().is_ok_and(|ft| ft.is_dir()) {
            continue;
        }
        let p2_entries = match fs::read_dir(p1.path()) {
            Ok(rd) => rd,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err),
        };
        for p2 in p2_entries.flatten() {
            if is_cancelled() {
                return Ok(());
            }
            if !p2.file_type().is_ok_and(|ft| ft.is_dir()) {
                continue;
            }
            let files = match fs::read_dir(p2.path()) {
                Ok(rd) => rd,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err),
            };
            for file in files.flatten() {
                if is_cancelled() {
                    return Ok(());
                }
                if !file.file_type().is_ok_and(|ft| ft.is_file()) {
                    continue;
                }
                let file_name = file.file_name();
                let Some(name) = file_name.to_str() else {
                    continue;
                };
                // Skip active in-flight `.tmp*` staging files and dotfiles.
                if name.starts_with('.') {
                    continue;
                }
                let Ok(meta) = file.metadata() else {
                    continue;
                };
                on_file(file.path(), name.to_string(), meta);
            }
        }
    }
    Ok(())
}

pub(super) fn collect_shared_object_candidates<C>(
    objects_dir: &Path,
    partitions: &mut Vec<L1Partition>,
    candidates: &mut Vec<EvictionCandidate>,
    is_cancelled: &C,
) -> std::io::Result<()>
where
    C: Fn() -> bool,
{
    if is_cancelled() {
        return Ok(());
    }
    let upstreams = match fs::read_dir(objects_dir) {
        Ok(rd) => rd,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err),
    };
    for upstream in upstreams.flatten() {
        if is_cancelled() {
            return Ok(());
        }
        if !upstream.file_type().is_ok_and(|ft| ft.is_dir()) {
            continue;
        }
        let Some(upstream_hex) = upstream.file_name().to_str().map(ToOwned::to_owned) else {
            continue;
        };
        let partition_idx = partitions.len();
        partitions.push(L1Partition::Shared { upstream_hex });
        let kind_entries = match fs::read_dir(upstream.path()) {
            Ok(rd) => rd,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err),
        };
        for kind_entry in kind_entries.flatten() {
            if is_cancelled() {
                return Ok(());
            }
            if !kind_entry.file_type().is_ok_and(|ft| ft.is_dir()) {
                continue;
            }
            let kind_name = kind_entry.file_name();
            let Some(kind) = kind_name
                .to_str()
                .and_then(ObjectKind::from_dir_name)
                .filter(|k| !k.is_scoped())
            else {
                continue;
            };
            collect_sharded_files(&kind_entry.path(), is_cancelled, |path, hex_id, meta| {
                candidates.push(EvictionCandidate {
                    path,
                    size_bytes: meta.len(),
                    modified: meta.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                    partition_idx,
                    kind,
                    hex_id,
                    l1_resident: false,
                    evicted: false,
                });
            })?;
        }
    }
    Ok(())
}

pub(super) fn collect_scoped_entries<C>(
    scopes_dir: &Path,
    partitions: &mut Vec<L1Partition>,
    candidates: &mut Vec<EvictionCandidate>,
    auth_markers: &mut Vec<ScopeAuthMarker>,
    is_cancelled: &C,
) -> std::io::Result<()>
where
    C: Fn() -> bool,
{
    if is_cancelled() {
        return Ok(());
    }
    let namespaces = match fs::read_dir(scopes_dir) {
        Ok(rd) => rd,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err),
    };
    for ns_entry in namespaces.flatten() {
        if is_cancelled() {
            return Ok(());
        }
        if !ns_entry.file_type().is_ok_and(|ft| ft.is_dir()) {
            continue;
        }
        let Some(namespace_hex) = ns_entry.file_name().to_str().map(ToOwned::to_owned) else {
            continue;
        };
        let scopes = match fs::read_dir(ns_entry.path()) {
            Ok(rd) => rd,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err),
        };
        for scope_entry in scopes.flatten() {
            if is_cancelled() {
                return Ok(());
            }
            if !scope_entry.file_type().is_ok_and(|ft| ft.is_dir()) {
                continue;
            }
            let Some(scope_hex) = scope_entry.file_name().to_str().map(ToOwned::to_owned) else {
                continue;
            };
            let partition_idx = partitions.len();
            partitions.push(L1Partition::Scoped {
                namespace_hex: namespace_hex.clone(),
                scope_hex: scope_hex.clone(),
            });
            let kinds = match fs::read_dir(scope_entry.path()) {
                Ok(rd) => rd,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err),
            };
            for kind_entry in kinds.flatten() {
                if is_cancelled() {
                    return Ok(());
                }
                if !kind_entry.file_type().is_ok_and(|ft| ft.is_dir()) {
                    continue;
                }
                let kind_name = kind_entry.file_name();
                let Some(kind) = kind_name.to_str().and_then(ObjectKind::from_dir_name) else {
                    continue;
                };
                if kind.is_scoped() {
                    collect_sharded_files(
                        &kind_entry.path(),
                        is_cancelled,
                        |path, hex_id, meta| {
                            candidates.push(EvictionCandidate {
                                path,
                                size_bytes: meta.len(),
                                modified: meta.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                                partition_idx,
                                kind,
                                hex_id,
                                l1_resident: false,
                                evicted: false,
                            });
                        },
                    )?;
                } else {
                    collect_sharded_files(
                        &kind_entry.path(),
                        is_cancelled,
                        |path, hex_id, _meta| {
                            auth_markers.push(ScopeAuthMarker {
                                path,
                                namespace_hex: namespace_hex.clone(),
                                scope_hex: scope_hex.clone(),
                                kind,
                                hex_id,
                            });
                        },
                    )?;
                }
            }
        }
    }
    Ok(())
}

pub(super) fn shared_object_exists_on_disk(
    upstream_dirs: &[PathBuf],
    kind: ObjectKind,
    hex_id: &str,
) -> bool {
    if !hex_id.is_ascii() || !hex_id.bytes().all(|b| b.is_ascii_hexdigit()) {
        return false;
    }
    let (p1, p2) = match (hex_id.get(0..2), hex_id.get(2..4)) {
        (Some(p1), Some(p2)) => (p1, p2),
        _ => ("00", "00"),
    };
    upstream_dirs
        .iter()
        .any(|u_dir| is_regular_file(&u_dir.join(kind.dir_name()).join(p1).join(p2).join(hex_id)))
}

impl TwoTierCache {
    /// Synchronously marks `(upstream_url, namespace_id)` as pending purge, advances
    /// `purge_epoch`, and clears in-memory L1 and authorization entries without blocking on disk
    /// I/O. Move the returned [`PendingPurgeGuard`] into [`Self::purge_namespace_with_guard`] or
    /// [`Self::purge_namespace_blocking_with_guard`] so L2 reads remain suppressed until disk
    /// deletion finishes.
    pub fn begin_pending_purge(&self, upstream_url: &str, namespace_id: &str) -> PendingPurgeGuard {
        let namespace_hex = CacheScope::namespace_hex_for(upstream_url, namespace_id);
        {
            let mut pending = self
                .inner
                .pending_purges
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *pending.entry(namespace_hex.clone()).or_insert(0) += 1;
        }
        self.inner.purge_epoch.fetch_add(1, Ordering::AcqRel);
        self.inner.evict_namespace_memory(&namespace_hex);
        PendingPurgeGuard {
            inner: Arc::clone(&self.inner),
            namespace_hex,
            armed: true,
        }
    }

    /// Synchronously executes full L2 disk and memory namespace cleanup while holding
    /// `pending_guard` until deletion completes.
    pub fn purge_namespace_blocking_with_guard(
        &self,
        upstream_url: &str,
        namespace_id: &str,
        pending_guard: PendingPurgeGuard,
    ) {
        self.touch_activity();
        let namespace_hex = CacheScope::namespace_hex_for(upstream_url, namespace_id);
        self.inner
            .purge_namespace_blocking(&namespace_hex, Some(pending_guard));
    }

    /// Completes an asynchronous namespace purge started with [`Self::begin_pending_purge`],
    /// keeping L2 reads suppressed until `scopes/<namespace_hex>/` has been removed.
    pub async fn purge_namespace_with_guard(
        &self,
        upstream_url: &str,
        namespace_id: &str,
        pending_guard: PendingPurgeGuard,
    ) {
        self.touch_activity();
        let namespace_hex = CacheScope::namespace_hex_for(upstream_url, namespace_id);
        let inner = Arc::clone(&self.inner);
        if let Err(err) = tokio::task::spawn_blocking(move || {
            inner.purge_namespace_blocking(&namespace_hex, Some(pending_guard));
        })
        .await
        {
            tracing::warn!("cache purge_namespace task failed: {err}");
        }
    }

    /// Evicts all scoped L1 entries (`Commit`, `View`, `Operation`), in-memory authorization
    /// markers, and on-disk `scopes/<namespace_hex>/` trees for `(upstream_url, namespace_id)`
    /// across all caller credentials while preserving shared content-addressed CAS payloads
    /// under `objects/<upstream_hex>/`.
    pub async fn purge_namespace(&self, upstream_url: &str, namespace_id: &str) {
        self.touch_activity();
        let namespace_hex = CacheScope::namespace_hex_for(upstream_url, namespace_id);
        let inner = Arc::clone(&self.inner);
        if let Err(err) = tokio::task::spawn_blocking(move || {
            inner.purge_namespace_blocking(&namespace_hex, None);
        })
        .await
        {
            tracing::warn!("cache purge_namespace task failed: {err}");
        }
    }

    /// Walks L2 object files under `objects/` and `scopes/` (skipping active `.tmp*` staging
    /// files, `staging/`, `run/`, and top-level daemon state files) and evicts files when total
    /// disk usage exceeds `max_disk_bytes`, prioritizing oversized files (`> 85%` low watermark),
    /// then cold non-L1 files oldest-mtime-first, then L1-resident files, stopping once usage
    /// drops to or below 85% of `max_disk_bytes`. Also removes orphaned scope authorization
    /// markers whose referenced shared object exists neither in `objects/` nor in L1.
    ///
    /// Passing `max_disk_bytes == 0` disables automatic L2 disk-quota eviction and is a no-op.
    /// Never holds L1 or `authorized` mutexes across filesystem walks or deletions.
    pub fn evict_l2_to_budget(&self, max_disk_bytes: u64) -> std::io::Result<EvictionStats> {
        self.evict_l2_to_budget_cancellable(max_disk_bytes, || false)
    }

    pub(crate) fn evict_l2_to_budget_cancellable(
        &self,
        max_disk_bytes: u64,
        is_cancelled: impl Fn() -> bool,
    ) -> std::io::Result<EvictionStats> {
        if max_disk_bytes == 0 || is_cancelled() {
            return Ok(EvictionStats::default());
        }
        // Reset before walking so concurrent writes arriving during this sweep accumulate toward
        // the next write-volume trigger instead of being dropped when the sweep finishes.
        self.inner
            .l2_bytes_written_since_sweep
            .store(0, Ordering::Relaxed);

        let objects_dir = self.inner.cache_dir.join("objects");
        let scopes_dir = self.inner.cache_dir.join("scopes");

        let mut partitions = Vec::new();
        let mut candidates = Vec::new();
        let mut auth_markers = Vec::new();
        collect_shared_object_candidates(
            &objects_dir,
            &mut partitions,
            &mut candidates,
            &is_cancelled,
        )?;
        collect_scoped_entries(
            &scopes_dir,
            &mut partitions,
            &mut candidates,
            &mut auth_markers,
            &is_cancelled,
        )?;
        if is_cancelled() {
            return Ok(EvictionStats::default());
        }

        let bytes_before = candidates
            .iter()
            .map(|c| c.size_bytes)
            .fold(0u64, u64::saturating_add);
        let mut remaining_bytes = bytes_before;
        let mut evicted_objects = 0u64;
        let mut evicted_bytes = 0u64;

        if bytes_before > max_disk_bytes {
            let low_watermark = l2_low_watermark_bytes(max_disk_bytes);
            let l1_keys = self
                .inner
                .l1
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .snapshot_keys();
            let mut l1_snapshot: HashMap<L1Partition, HashMap<ObjectKind, HashSet<String>>> =
                HashMap::new();
            for (partition, kind, id) in l1_keys {
                l1_snapshot
                    .entry(partition)
                    .or_default()
                    .entry(kind)
                    .or_default()
                    .insert(hex::encode(id));
            }
            for cand in &mut candidates {
                cand.l1_resident = l1_snapshot
                    .get(&partitions[cand.partition_idx])
                    .and_then(|by_kind| by_kind.get(&cand.kind))
                    .is_some_and(|ids| ids.contains(&cand.hex_id));
            }

            candidates.sort_by(|a, b| {
                let a_oversized = a.size_bytes > low_watermark;
                let b_oversized = b.size_bytes > low_watermark;
                b_oversized
                    .cmp(&a_oversized)
                    .then_with(|| a.l1_resident.cmp(&b.l1_resident))
                    .then_with(|| a.modified.cmp(&b.modified))
                    .then_with(|| a.path.cmp(&b.path))
            });
            for cand in &mut candidates {
                if remaining_bytes <= low_watermark || is_cancelled() {
                    break;
                }
                match fs::remove_file(&cand.path) {
                    Ok(()) => {
                        cand.evicted = true;
                        remaining_bytes = remaining_bytes.saturating_sub(cand.size_bytes);
                        evicted_objects = evicted_objects.saturating_add(1);
                        evicted_bytes = evicted_bytes.saturating_add(cand.size_bytes);
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                        cand.evicted = true;
                        remaining_bytes = remaining_bytes.saturating_sub(cand.size_bytes);
                    }
                    Err(err) => {
                        tracing::warn!(
                            "failed to evict L2 cache file {}: {err}",
                            cand.path.display()
                        );
                    }
                }
            }
        }

        let mut orphaned_markers_removed = 0u64;
        if !is_cancelled() {
            let surviving_shared: HashSet<(ObjectKind, &str)> = candidates
                .iter()
                .filter(|c| !c.evicted && !c.kind.is_scoped())
                .map(|c| (c.kind, c.hex_id.as_str()))
                .collect();

            let mut orphan_candidates: Vec<(ScopeAuthMarker, Option<Vec<u8>>)> = auth_markers
                .into_iter()
                .filter(|marker| !surviving_shared.contains(&(marker.kind, marker.hex_id.as_str())))
                .map(|marker| {
                    let decoded = hex::decode(&marker.hex_id).ok();
                    (marker, decoded)
                })
                .collect();

            if !orphan_candidates.is_empty() && !is_cancelled() {
                // Skip markers whose shared object is still live in L1 (for example L1-only
                // entries or L1-hot objects whose L2 file was evicted under a tight disk quota).
                let l1 = self.inner.l1.lock().unwrap_or_else(|e| e.into_inner());
                orphan_candidates.retain(|(marker, decoded_id)| {
                    !decoded_id
                        .as_deref()
                        .is_some_and(|id| l1.contains_shared(marker.kind, id))
                });
            }

            if !orphan_candidates.is_empty() && !is_cancelled() {
                // Pre-filter against disk outside `scope_io_lock` to avoid taking a write lock
                // for objects written concurrently after the initial walk.
                let upstream_dirs = collect_child_dirs(&objects_dir);
                orphan_candidates.retain(|(marker, _)| {
                    !shared_object_exists_on_disk(&upstream_dirs, marker.kind, &marker.hex_id)
                });
            }

            // Re-verify L1 residency and on-disk existence in short `scope_io_lock.write()`
            // batches so concurrent `insert_verified_with_epoch` / `commit_staged_disk_file`
            // writes cannot race with orphan pruning. Never hold `l1` or `authorized` across
            // filesystem I/O (lock order: `scope_io_lock -> l1 -> authorized`).
            for batch in orphan_candidates.chunks(ORPHAN_PRUNE_BATCH_SIZE) {
                if is_cancelled() {
                    break;
                }
                let _scope_guard = self
                    .inner
                    .scope_io_lock
                    .write()
                    .unwrap_or_else(|e| e.into_inner());
                let upstream_dirs = collect_child_dirs(&objects_dir);
                let l1_present: Vec<bool> = {
                    let l1 = self.inner.l1.lock().unwrap_or_else(|e| e.into_inner());
                    batch
                        .iter()
                        .map(|(marker, decoded_id)| {
                            decoded_id
                                .as_deref()
                                .is_some_and(|id| l1.contains_shared(marker.kind, id))
                        })
                        .collect()
                };
                let mut batch_pruned_auth_keys: Vec<AuthKey> = Vec::new();
                for ((marker, decoded_id), in_l1) in batch.iter().zip(l1_present) {
                    if is_cancelled() {
                        break;
                    }
                    if in_l1
                        || shared_object_exists_on_disk(&upstream_dirs, marker.kind, &marker.hex_id)
                    {
                        continue;
                    }
                    match fs::remove_file(&marker.path) {
                        Ok(()) => {
                            orphaned_markers_removed = orphaned_markers_removed.saturating_add(1);
                            if let Some(id_bytes) = decoded_id {
                                batch_pruned_auth_keys.push((
                                    marker.namespace_hex.clone(),
                                    marker.scope_hex.clone(),
                                    marker.kind,
                                    id_bytes.clone(),
                                ));
                            }
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                            if let Some(id_bytes) = decoded_id {
                                batch_pruned_auth_keys.push((
                                    marker.namespace_hex.clone(),
                                    marker.scope_hex.clone(),
                                    marker.kind,
                                    id_bytes.clone(),
                                ));
                            }
                        }
                        Err(err) => {
                            tracing::warn!(
                                "failed to remove orphaned L2 auth marker {}: {err}",
                                marker.path.display()
                            );
                        }
                    }
                }
                if !batch_pruned_auth_keys.is_empty() {
                    let mut auth = self
                        .inner
                        .authorized
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    for key in batch_pruned_auth_keys {
                        auth.pop(&key);
                    }
                }
            }
        }

        if evicted_objects > 0 {
            self.inner
                .l2_evicted_objects
                .fetch_add(evicted_objects, Ordering::Relaxed);
            self.inner
                .l2_evicted_bytes
                .fetch_add(evicted_bytes, Ordering::Relaxed);
        }

        Ok(EvictionStats {
            bytes_before,
            bytes_after: remaining_bytes,
            evicted_objects,
            evicted_bytes,
            orphaned_markers_removed,
        })
    }

    /// Spawns a background Tokio task that enforces `max_disk_bytes` periodically (every 60s)
    /// and wakes early once cumulative L2 writes reach `min(max_disk_bytes / 10, 64 MiB)` with
    /// a cooldown between sweeps. Returns `None` when `max_disk_bytes == 0`.
    pub fn spawn_l2_eviction_task<F>(
        &self,
        max_disk_bytes: u64,
        mut shutdown_rx: watch::Receiver<bool>,
        on_evicted: F,
    ) -> Option<tokio::task::JoinHandle<()>>
    where
        F: Fn() + Send + 'static,
    {
        if max_disk_bytes == 0 {
            self.inner
                .l2_sweep_trigger_bytes
                .store(0, Ordering::Relaxed);
            return None;
        }
        let trigger_bytes = (max_disk_bytes / 10).clamp(1, L2_EVICTION_MAX_WRITE_TRIGGER_BYTES);
        self.inner
            .l2_sweep_trigger_bytes
            .store(trigger_bytes, Ordering::Relaxed);
        let cache = self.clone();
        Some(tokio::spawn(async move {
            let mut last_sweep: Option<Instant> = None;
            loop {
                let woken_by_write = tokio::select! {
                    _ = shutdown_rx.wait_for(|&v| v) => break,
                    _ = tokio::time::sleep(L2_EVICTION_PERIODIC_INTERVAL) => false,
                    _ = cache.inner.l2_eviction_notify.notified() => true,
                };
                if woken_by_write && let Some(prev) = last_sweep {
                    let elapsed = prev.elapsed();
                    if elapsed < L2_EVICTION_MIN_COOLDOWN {
                        let remaining = L2_EVICTION_MIN_COOLDOWN - elapsed;
                        tokio::select! {
                            _ = shutdown_rx.wait_for(|&v| v) => break,
                            _ = tokio::time::sleep(remaining) => {}
                        }
                    }
                }
                if *shutdown_rx.borrow() {
                    break;
                }
                let cancelled = Arc::new(AtomicBool::new(false));
                let cancelled_bg = Arc::clone(&cancelled);
                let sweep_cache = cache.clone();
                let mut blocking_handle = tokio::task::spawn_blocking(move || {
                    sweep_cache.evict_l2_to_budget_cancellable(max_disk_bytes, || {
                        cancelled_bg.load(Ordering::Relaxed)
                    })
                });
                let sweep_res = tokio::select! {
                    res = &mut blocking_handle => Some(res),
                    _ = shutdown_rx.wait_for(|&v| v) => None,
                };
                let sweep_res = match sweep_res {
                    Some(res) => res,
                    None => {
                        cancelled.store(true, Ordering::Release);
                        // Wait for the blocking sweep thread to observe `cancelled` and exit so
                        // `run_daemon` does not drop `daemon.instance.lock` while files are still
                        // being unlinked.
                        blocking_handle.await
                    }
                };
                match sweep_res {
                    Ok(Ok(stats)) => {
                        if stats.evicted_objects > 0 || stats.orphaned_markers_removed > 0 {
                            tracing::info!(
                                "L2 disk cache eviction removed {} objects ({} bytes) and {} orphaned markers ({} -> {} bytes)",
                                stats.evicted_objects,
                                stats.evicted_bytes,
                                stats.orphaned_markers_removed,
                                stats.bytes_before,
                                stats.bytes_after,
                            );
                            on_evicted();
                        }
                    }
                    Ok(Err(err)) => {
                        tracing::warn!("L2 disk cache eviction sweep failed: {err}");
                    }
                    Err(err) => {
                        tracing::warn!("L2 disk cache eviction task failed: {err}");
                    }
                }
                if cancelled.load(Ordering::Acquire) || *shutdown_rx.borrow() {
                    break;
                }
                last_sweep = Some(Instant::now());
            }
        }))
    }
}
