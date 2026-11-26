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

//! Streaming L2 disk-staging spooler for large file blobs.
//!
//! # Authority and Crash-Safety Ordering
//! `BlobSpooler` buffers small payloads in RAM up to `MAX_L1_OBJECT_BYTES` and spills larger
//! payloads into process-unique temp files under `<cache_dir>/staging` (`0600`). Committing
//! verifies the streaming SHA-1 git blob digest, syncs, renames into L2 CAS, and only then
//! authorizes the caller's scope; dropping or discarding deletes unlinked temp files.

use std::fs;
use std::io::{Read as _, Seek as _, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use sha1::{Digest, Sha1};
use tokio::io::{AsyncWriteExt, BufWriter};

use super::l1::{l1_entry_weight, l1_key_overhead};
use super::l2::{ensure_private_dir_within, is_valid_object_id};
use super::{
    CACHE_STREAM_CHUNK_BYTES, CacheInner, CacheScope, MAX_L1_OBJECT_BYTES, ObjectKind, TEMP_SEQ,
};

struct StagingTempFile {
    path: Option<PathBuf>,
}

impl StagingTempFile {
    fn new(path: PathBuf) -> Self {
        Self { path: Some(path) }
    }

    fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    fn disarm(&mut self) {
        self.path = None;
    }

    fn discard(&mut self) {
        if let Some(p) = self.path.take() {
            let _ = fs::remove_file(&p);
        }
    }
}

impl Drop for StagingTempFile {
    fn drop(&mut self) {
        self.discard();
    }
}

struct StagedDiskFile {
    // Declared before `guard` so the file descriptor closes before the temp path is unlinked.
    writer: Option<BufWriter<tokio::fs::File>>,
    guard: StagingTempFile,
    bytes_written: u64,
    incremental_sha1: Option<(u64, Sha1)>,
}

enum SpoolState {
    InMemory(Vec<u8>),
    PendingDisk {
        incremental_sha1: Option<(u64, Sha1)>,
    },
    Disk(StagedDiskFile),
    Disabled,
}

/// Bounded-RAM streaming spooler that writes incoming `ReadFile` or `WriteFileStream` chunks into
/// L2 disk CAS via a private staging file under `<cache_dir>/staging`, stopping above
/// `max_l2_object_bytes` and always deleting uncommitted temp files on error or drop.
pub struct BlobSpooler {
    inner: Arc<CacheInner>,
    state: SpoolState,
}

impl BlobSpooler {
    pub(super) fn for_read(inner: Arc<CacheInner>) -> Self {
        let state = if inner.max_l2_object_bytes == 0 {
            SpoolState::Disabled
        } else {
            SpoolState::InMemory(Vec::new())
        };
        Self { inner, state }
    }

    pub(super) fn for_write(inner: Arc<CacheInner>, declared_total_size: Option<u64>) -> Self {
        if inner.max_l2_object_bytes == 0
            || declared_total_size.is_some_and(|total| total > inner.max_l2_object_bytes)
        {
            return Self {
                inner,
                state: SpoolState::Disabled,
            };
        }
        let incremental_sha1 = declared_total_size.map(|total| {
            let mut hasher = Sha1::new();
            hasher.update(format!("blob {total}\0").as_bytes());
            (total, hasher)
        });
        Self {
            inner,
            state: SpoolState::PendingDisk { incremental_sha1 },
        }
    }

    /// Feeds the next streamed chunk into the spooler. Never fails the caller's RPC: exceeding
    /// `max_l2_object_bytes` or encountering a local disk error cleans up staging state and
    /// switches to pass-through mode.
    pub async fn write_chunk(&mut self, chunk: &[u8]) {
        if chunk.is_empty() {
            return;
        }
        let max_l2 = self.inner.max_l2_object_bytes;
        match &mut self.state {
            SpoolState::Disabled => {}
            SpoolState::InMemory(buf) => {
                let next_total = (buf.len() as u64).saturating_add(chunk.len() as u64);
                if next_total > max_l2 {
                    self.state = SpoolState::Disabled;
                    return;
                }
                if next_total <= MAX_L1_OBJECT_BYTES as u64 {
                    buf.extend_from_slice(chunk);
                    return;
                }
                // Spill RAM buffer into a disk staging file so heap usage stays <= MAX_L1_OBJECT_BYTES.
                let existing = std::mem::take(buf);
                match open_staging_file(Arc::clone(&self.inner)).await {
                    Ok((file, guard)) => {
                        let mut staged = StagedDiskFile {
                            writer: Some(BufWriter::with_capacity(CACHE_STREAM_CHUNK_BYTES, file)),
                            guard,
                            bytes_written: 0,
                            incremental_sha1: None,
                        };
                        let write_res = async {
                            let w = staged.writer.as_mut().expect("staging writer present");
                            w.write_all(&existing).await?;
                            w.write_all(chunk).await
                        }
                        .await;
                        drop(existing);
                        match write_res {
                            Ok(()) => {
                                staged.bytes_written = next_total;
                                self.state = SpoolState::Disk(staged);
                            }
                            Err(err) => {
                                self.inner.l2_write_errors.fetch_add(1, Ordering::Relaxed);
                                tracing::warn!("failed to spill L2 staging file: {err}");
                                discard_staged_async(staged).await;
                                self.state = SpoolState::Disabled;
                            }
                        }
                    }
                    Err(err) => {
                        drop(existing);
                        self.inner.l2_write_errors.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!("failed to open L2 staging file: {err}");
                        self.state = SpoolState::Disabled;
                    }
                }
            }
            SpoolState::PendingDisk { incremental_sha1 } => {
                let next_total = chunk.len() as u64;
                if next_total > max_l2 {
                    self.state = SpoolState::Disabled;
                    return;
                }
                let mut hasher = incremental_sha1.take();
                if let Some((_, h)) = &mut hasher {
                    h.update(chunk);
                }
                match open_staging_file(Arc::clone(&self.inner)).await {
                    Ok((file, guard)) => {
                        let mut staged = StagedDiskFile {
                            writer: Some(BufWriter::with_capacity(CACHE_STREAM_CHUNK_BYTES, file)),
                            guard,
                            bytes_written: 0,
                            incremental_sha1: hasher,
                        };
                        let write_res = staged
                            .writer
                            .as_mut()
                            .expect("staging writer present")
                            .write_all(chunk)
                            .await;
                        match write_res {
                            Ok(()) => {
                                staged.bytes_written = next_total;
                                self.state = SpoolState::Disk(staged);
                            }
                            Err(err) => {
                                self.inner.l2_write_errors.fetch_add(1, Ordering::Relaxed);
                                tracing::warn!("failed to write L2 staging file: {err}");
                                discard_staged_async(staged).await;
                                self.state = SpoolState::Disabled;
                            }
                        }
                    }
                    Err(err) => {
                        self.inner.l2_write_errors.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!("failed to open L2 staging file: {err}");
                        self.state = SpoolState::Disabled;
                    }
                }
            }
            SpoolState::Disk(_) => {
                let SpoolState::Disk(mut staged) =
                    std::mem::replace(&mut self.state, SpoolState::Disabled)
                else {
                    unreachable!();
                };
                let next_total = staged.bytes_written.saturating_add(chunk.len() as u64);
                if next_total > max_l2 {
                    discard_staged_async(staged).await;
                    return;
                }
                if let Some((_, h)) = &mut staged.incremental_sha1 {
                    h.update(chunk);
                }
                let write_res = staged
                    .writer
                    .as_mut()
                    .expect("staging writer present")
                    .write_all(chunk)
                    .await;
                match write_res {
                    Ok(()) => {
                        staged.bytes_written = next_total;
                        self.state = SpoolState::Disk(staged);
                    }
                    Err(err) => {
                        self.inner.l2_write_errors.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!("failed to append L2 staging file: {err}");
                        discard_staged_async(staged).await;
                    }
                }
            }
        }
    }

    /// Discards any open staging temp file asynchronously.
    pub async fn discard(mut self) {
        if let SpoolState::Disk(staged) = std::mem::replace(&mut self.state, SpoolState::Disabled) {
            discard_staged_async(staged).await;
        }
    }

    /// Verifies the spooled bytes against `file_id`, atomically commits them into L2 (and L1 if
    /// `<= MAX_L1_OBJECT_BYTES`), and authorizes `scope`. Returns `true` if cached.
    pub async fn commit(
        mut self,
        scope: CacheScope,
        file_id: Vec<u8>,
        is_write_through: bool,
    ) -> bool {
        if !is_valid_object_id(&file_id) {
            self.discard().await;
            return false;
        }
        let state = std::mem::replace(&mut self.state, SpoolState::Disabled);
        match state {
            SpoolState::Disabled => false,
            SpoolState::InMemory(buf) => {
                let inner = Arc::clone(&self.inner);
                let payload = Bytes::from(buf);
                tokio::task::spawn_blocking(move || {
                    let inserted =
                        inner.insert_verified(&scope, ObjectKind::File, &file_id, &payload);
                    if inserted && is_write_through {
                        inner.write_through_objects.fetch_add(1, Ordering::Relaxed);
                    }
                    inserted
                })
                .await
                .unwrap_or(false)
            }
            SpoolState::PendingDisk { .. } => {
                let inner = Arc::clone(&self.inner);
                tokio::task::spawn_blocking(move || {
                    let inserted =
                        inner.insert_verified(&scope, ObjectKind::File, &file_id, &Bytes::new());
                    if inserted && is_write_through {
                        inner.write_through_objects.fetch_add(1, Ordering::Relaxed);
                    }
                    inserted
                })
                .await
                .unwrap_or(false)
            }
            SpoolState::Disk(mut staged) => {
                let mut writer = staged.writer.take().expect("staging writer present");
                if let Err(err) = writer.flush().await {
                    self.inner.l2_write_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!("failed to flush L2 staging file: {err}");
                    drop(writer);
                    discard_staged_async(staged).await;
                    return false;
                }
                let std_file = writer.into_inner().into_std().await;
                let inner = Arc::clone(&self.inner);
                let bytes_written = staged.bytes_written;
                let incremental_sha1 = staged.incremental_sha1.take();
                let mut guard = staged.guard;
                tokio::task::spawn_blocking(move || {
                    commit_staged_disk_file(
                        &inner,
                        &scope,
                        &file_id,
                        std_file,
                        &mut guard,
                        bytes_written,
                        incremental_sha1,
                        is_write_through,
                    )
                })
                .await
                .unwrap_or(false)
            }
        }
    }
}

async fn open_staging_file(
    inner: Arc<CacheInner>,
) -> std::io::Result<(tokio::fs::File, StagingTempFile)> {
    tokio::task::spawn_blocking(move || {
        let staging_dir = inner.cache_dir.join("staging");
        ensure_private_dir_within(&inner.cache_dir, &staging_dir)?;
        let seq = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let path = staging_dir.join(format!(".tmp.{}.{seq}", std::process::id()));
        let mut opts = fs::OpenOptions::new();
        opts.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let file = opts.open(&path)?;
        Ok((tokio::fs::File::from_std(file), StagingTempFile::new(path)))
    })
    .await
    .map_err(std::io::Error::other)?
}

async fn discard_staged_async(mut staged: StagedDiskFile) {
    let _ = staged.writer.take();
    if let Some(path) = staged.guard.path().map(Path::to_path_buf) {
        let _ = tokio::fs::remove_file(&path).await;
        staged.guard.disarm();
    }
}

#[allow(clippy::too_many_arguments)]
fn commit_staged_disk_file(
    inner: &CacheInner,
    scope: &CacheScope,
    file_id: &[u8],
    mut std_file: fs::File,
    guard: &mut StagingTempFile,
    bytes_written: u64,
    incremental_sha1: Option<(u64, Sha1)>,
    is_write_through: bool,
) -> bool {
    let l1_key = CacheInner::l1_key(scope, ObjectKind::File, file_id);
    let wants_l1 = bytes_written <= MAX_L1_OBJECT_BYTES as u64 && {
        let l1 = inner.l1.lock().unwrap_or_else(|e| e.into_inner());
        let weight = (bytes_written as usize).saturating_add(l1_key_overhead(&l1_key));
        l1.max_bytes > 0 && weight <= l1.max_bytes
    };

    let needs_sha1_scan = if file_id.len() != 20 {
        false
    } else if let Some((declared_total, hasher)) = incremental_sha1
        && declared_total == bytes_written
    {
        if hasher.finalize().as_slice() != file_id {
            tracing::warn!(
                "not caching staged File {} with mismatched SHA-1",
                hex::encode(file_id)
            );
            return false;
        }
        false
    } else {
        true
    };

    let mut l1_payload: Option<Bytes> = None;
    if needs_sha1_scan || wants_l1 {
        if let Err(err) = std_file.seek(SeekFrom::Start(0)) {
            inner.l2_write_errors.fetch_add(1, Ordering::Relaxed);
            tracing::warn!("failed to rewind L2 staging file: {err}");
            return false;
        }
        let mut hasher = needs_sha1_scan.then(|| {
            let mut h = Sha1::new();
            h.update(format!("blob {bytes_written}\0").as_bytes());
            h
        });
        let mut l1_buf = wants_l1.then(|| Vec::with_capacity(bytes_written as usize));
        let mut scratch = [0u8; CACHE_STREAM_CHUNK_BYTES];
        let mut total_read = 0u64;
        loop {
            match std_file.read(&mut scratch) {
                Ok(0) => break,
                Ok(n) => {
                    total_read = total_read.saturating_add(n as u64);
                    if total_read > bytes_written {
                        tracing::warn!(
                            "L2 staging file grew unexpectedly while committing {}",
                            hex::encode(file_id)
                        );
                        return false;
                    }
                    if let Some(h) = &mut hasher {
                        h.update(&scratch[..n]);
                    }
                    if let Some(buf) = &mut l1_buf {
                        buf.extend_from_slice(&scratch[..n]);
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(err) => {
                    inner.l2_write_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!("failed to verify L2 staging file: {err}");
                    return false;
                }
            }
        }
        if total_read != bytes_written {
            tracing::warn!(
                "L2 staging file truncated while committing {} ({total_read} != {bytes_written})",
                hex::encode(file_id)
            );
            return false;
        }
        if let Some(h) = hasher
            && h.finalize().as_slice() != file_id
        {
            tracing::warn!(
                "not caching staged File {} with mismatched SHA-1",
                hex::encode(file_id)
            );
            return false;
        }
        if let Some(buf) = l1_buf {
            l1_payload = Some(Bytes::from(buf));
        }
    }

    let l2_written = match std_file.sync_all() {
        Ok(()) => {
            drop(std_file);
            let dst = inner.object_path(scope, ObjectKind::File, file_id);
            let rename_res = (|| -> std::io::Result<()> {
                let Some(parent) = dst.parent() else {
                    return Ok(());
                };
                ensure_private_dir_within(&inner.cache_dir, parent)?;
                let staging_path = guard.path().ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::NotFound, "missing staging path")
                })?;
                fs::rename(staging_path, &dst)
            })();
            match rename_res {
                Ok(()) => {
                    guard.disarm();
                    true
                }
                Err(err) => {
                    inner.l2_write_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        "failed to commit staged L2 cache entry {}: {err}",
                        dst.display()
                    );
                    false
                }
            }
        }
        Err(err) => {
            drop(std_file);
            inner.l2_write_errors.fetch_add(1, Ordering::Relaxed);
            tracing::warn!("failed to sync L2 staging file: {err}");
            false
        }
    };

    let l1_inserted = if let Some(payload) = l1_payload {
        inner
            .l1
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .put(l1_key, payload)
    } else {
        // Evict any stale L1 entry if the object exceeds the L1 threshold.
        if bytes_written > MAX_L1_OBJECT_BYTES as u64 {
            let mut l1 = inner.l1.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(prev) = l1.map.pop(&l1_key) {
                let prev_weight = l1_entry_weight(&l1_key, &prev);
                l1.current_bytes = l1.current_bytes.saturating_sub(prev_weight);
            }
        }
        false
    };

    if !l2_written && !l1_inserted {
        return false;
    }
    inner.authorize_scope(scope, ObjectKind::File, file_id);
    if is_write_through {
        inner.write_through_objects.fetch_add(1, Ordering::Relaxed);
    }
    true
}
