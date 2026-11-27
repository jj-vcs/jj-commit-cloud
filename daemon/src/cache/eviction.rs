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

//! Startup cleanup of orphaned L2 staging temporary files.
//!
//! # Authority and Crash-Safety Ordering
//! On daemon startup, `cleanup_stale_staging_files` scans `<cache_dir>/staging` for regular
//! files matching `.tmp.<pid>.<seq>` whose owning PID is provably dead (`kill(pid, 0)` returns
//! `ESRCH` on Unix) and unlinks them so crashed runs do not leak staging files.
//!
//! # Cross-Process and RPC Boundaries
//! Staging files belonging to the current process or any live PID are left untouched so
//! multiple daemons sharing `<cache_dir>` never remove each other's in-flight uploads or reads.

use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::{CacheInner, CacheScope, TwoTierCache};

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
}
