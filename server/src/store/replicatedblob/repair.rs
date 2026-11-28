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

//! Background read-repair and asynchronous peer write fan-out for `ReplicatedBlobStore`.
//!
//! # Authority and Crash-Safety Ordering
//! Read-repair writes verified peer bytes into the local `BlobStore` before updating
//! `MetadataStore` replica locations. Asynchronous write fan-out runs only after metadata
//! publication and stages large blobs to a temporary file before uploading to peers.
//!
//! # Concurrency and Merge Invariants
//! Per-blob and per-`(blob_id, location)` RAII guards deduplicate concurrent background work
//! while a shared semaphore (`DEFAULT_MAX_BACKGROUND_TASKS`) bounds total background tasks.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use tokio::io::AsyncWriteExt as _;
use tokio::sync::OwnedSemaphorePermit;
use tracing::warn;

use super::{ReplicatedInner, SMALL_BLOB_MAX_BYTES};
use crate::store::{BlobRecord, BlobStore};

/// Removes a blob from the in-flight read-repair set when its repair task ends.
pub(super) struct RepairGuard {
    pub(super) inner: Arc<ReplicatedInner>,
    pub(super) blob_id: Vec<u8>,
}

impl Drop for RepairGuard {
    fn drop(&mut self) {
        self.inner
            .repairs_in_flight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.blob_id);
    }
}

/// Removes a `(blob_id, location)` pair from the in-flight write fan-out set when its task ends.
pub(super) struct FanoutGuard {
    pub(super) inner: Arc<ReplicatedInner>,
    pub(super) key: (Vec<u8>, String),
}

impl Drop for FanoutGuard {
    fn drop(&mut self) {
        self.inner
            .fanouts_in_flight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.key);
    }
}

struct FanoutTarget {
    loc: String,
    peer: Arc<dyn BlobStore>,
    in_flight: FanoutGuard,
    permit: OwnedSemaphorePermit,
}

async fn drop_temp_file_blocking(temp_file: tempfile::NamedTempFile) {
    let _ = tokio::task::spawn_blocking(move || drop(temp_file)).await;
}

async fn stage_local_blob_to_temp_file(
    local: Arc<dyn BlobStore>,
    record: BlobRecord,
    blob_id_hex: &str,
) -> Option<tempfile::NamedTempFile> {
    let size = record.content_size;
    let mut rx = match local.get_blob_range_stream(record, 0, None).await {
        Ok(Some(rx)) => rx,
        Ok(None) => {
            warn!(
                "Background file peer fan-out aborted for {}: blob missing from local store",
                blob_id_hex
            );
            return None;
        }
        Err(err) => {
            warn!(
                "Background file peer fan-out failed streaming local blob {}: {}",
                blob_id_hex, err
            );
            return None;
        }
    };

    let (temp_file, std_writer) = match tokio::task::spawn_blocking(|| {
        let tf = tempfile::NamedTempFile::new()?;
        let clone = tf.as_file().try_clone()?;
        Ok::<_, std::io::Error>((tf, clone))
    })
    .await
    {
        Ok(Ok(pair)) => pair,
        Ok(Err(err)) => {
            warn!(
                "Background file peer fan-out failed creating temp file for {}: {}",
                blob_id_hex, err
            );
            return None;
        }
        Err(err) => {
            warn!(
                "Background file peer fan-out temp file task panicked for {}: {}",
                blob_id_hex, err
            );
            return None;
        }
    };

    let copy_ok = async {
        let mut writer = tokio::fs::File::from_std(std_writer);
        let mut written = 0u64;
        while let Some(chunk_res) = rx.recv().await {
            let chunk = match chunk_res {
                Ok(c) => c,
                Err(err) => {
                    warn!(
                        "Background file peer fan-out failed mid-stream for {}: {}",
                        blob_id_hex, err
                    );
                    return false;
                }
            };
            written = written.saturating_add(chunk.len() as u64);
            if written > size {
                warn!(
                    "Background file peer fan-out aborted for {}: local stream exceeded {size} bytes",
                    blob_id_hex
                );
                return false;
            }
            if let Err(err) = writer.write_all(&chunk).await {
                warn!(
                    "Background file peer fan-out failed writing temp file for {}: {}",
                    blob_id_hex, err
                );
                return false;
            }
        }
        if written != size {
            warn!(
                "Background file peer fan-out aborted for {}: local stream ended at {written} bytes, expected {size}",
                blob_id_hex
            );
            return false;
        }
        if let Err(err) = writer.flush().await {
            warn!(
                "Background file peer fan-out failed flushing temp file for {}: {}",
                blob_id_hex, err
            );
            return false;
        }
        drop(writer);
        true
    }
    .await;

    if !copy_ok {
        drop_temp_file_blocking(temp_file).await;
        return None;
    }
    Some(temp_file)
}

impl ReplicatedInner {
    pub(super) fn spawn_read_repair(self: &Arc<Self>, blob_id: Vec<u8>, bytes: Bytes) {
        // Deduplicate before acquiring a semaphore permit so duplicate misses while a repair
        // is already running increment `read_repairs_deduplicated` even under saturation.
        if !self
            .repairs_in_flight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(blob_id.clone())
        {
            self.stats
                .read_repairs_deduplicated
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        let in_flight = RepairGuard {
            inner: self.clone(),
            blob_id: blob_id.clone(),
        };
        let Ok(permit) = self.bg_tasks.clone().try_acquire_owned() else {
            warn!(
                "Skipping background read-repair for {} (replication concurrency cap reached)",
                hex::encode(&blob_id)
            );
            drop(in_flight);
            return;
        };
        let local = self.local.clone();
        let metadata = self.metadata.clone();
        let stats = self.stats.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let _in_flight = in_flight;
            let local_loc = local.location_id().to_string();
            if let Err(err) = local.put_blob(&blob_id, bytes).await {
                warn!(
                    "Background read-repair write to {} failed for {}: {}",
                    local_loc,
                    hex::encode(&blob_id),
                    err
                );
                return;
            }
            if let Err(err) = metadata
                .add_blob_replica_location(&blob_id, &local_loc)
                .await
            {
                warn!(
                    "Background read-repair metadata update failed for {}: {}",
                    hex::encode(&blob_id),
                    err
                );
                return;
            }
            stats.read_repairs_completed.fetch_add(1, Ordering::Relaxed);
        });
    }

    pub(super) fn spawn_write_fanout(self: &Arc<Self>, blob_id: Vec<u8>, bytes: Bytes) {
        for (loc, peer) in &self.peers {
            let key = (blob_id.clone(), loc.clone());
            if !self
                .fanouts_in_flight
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(key.clone())
            {
                continue;
            }
            let in_flight = FanoutGuard {
                inner: self.clone(),
                key,
            };
            let Ok(permit) = self.bg_tasks.clone().try_acquire_owned() else {
                warn!(
                    "Skipping background peer fan-out to {} for {} (replication concurrency cap reached; read-repair will backfill on demand)",
                    loc,
                    hex::encode(&blob_id)
                );
                drop(in_flight);
                continue;
            };
            let peer = peer.clone();
            let loc = loc.clone();
            let metadata = self.metadata.clone();
            let blob_id = blob_id.clone();
            let bytes = bytes.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let _in_flight = in_flight;
                if let Err(err) = peer.put_blob(&blob_id, bytes).await {
                    warn!(
                        "Background peer fan-out to {} failed for {}: {}",
                        loc,
                        hex::encode(&blob_id),
                        err
                    );
                    return;
                }
                if let Err(err) = metadata.add_blob_replica_location(&blob_id, &loc).await {
                    warn!(
                        "Background peer fan-out metadata update to {} failed for {}: {}",
                        loc,
                        hex::encode(&blob_id),
                        err
                    );
                }
            });
        }
    }

    pub(super) fn spawn_file_write_fanout(self: &Arc<Self>, blob_id: Vec<u8>, size: u64) {
        let mut targets: Vec<FanoutTarget> = Vec::new();
        for (loc, peer) in &self.peers {
            let key = (blob_id.clone(), loc.clone());
            if !self
                .fanouts_in_flight
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(key.clone())
            {
                continue;
            }
            let in_flight = FanoutGuard {
                inner: self.clone(),
                key,
            };
            let Ok(permit) = self.bg_tasks.clone().try_acquire_owned() else {
                warn!(
                    "Skipping background file peer fan-out to {} for {} (replication concurrency cap reached; read-repair will backfill on demand)",
                    loc,
                    hex::encode(&blob_id)
                );
                drop(in_flight);
                continue;
            };
            targets.push(FanoutTarget {
                loc: loc.clone(),
                peer: peer.clone(),
                in_flight,
                permit,
            });
        }
        if targets.is_empty() {
            return;
        }

        let local = self.local.clone();
        let metadata = self.metadata.clone();
        tokio::spawn(async move {
            let blob_id_hex = hex::encode(&blob_id);
            let record = BlobRecord {
                blob_id: blob_id.clone(),
                content_size: size,
                inline_content: None,
                primary_location: Some(local.location_id().to_string()),
                replicated_locations: vec![local.location_id().to_string()],
            };

            if size <= SMALL_BLOB_MAX_BYTES {
                let bytes = match local.get_blob_range_with_record(&record, 0, None).await {
                    Ok(Some(b)) if b.len() as u64 == size => b,
                    Ok(Some(b)) => {
                        warn!(
                            "Background file peer fan-out aborted for {}: local blob has {} bytes, expected {size}",
                            blob_id_hex,
                            b.len()
                        );
                        return;
                    }
                    Ok(None) => {
                        warn!(
                            "Background file peer fan-out aborted for {}: blob missing from local store",
                            blob_id_hex
                        );
                        return;
                    }
                    Err(err) => {
                        warn!(
                            "Background file peer fan-out failed reading local blob {}: {}",
                            blob_id_hex, err
                        );
                        return;
                    }
                };
                futures::future::join_all(targets.into_iter().map(|target| {
                    let metadata = metadata.clone();
                    let blob_id = &blob_id;
                    let blob_id_hex = &blob_id_hex;
                    let bytes = bytes.clone();
                    async move {
                        let _permit = target.permit;
                        let _in_flight = target.in_flight;
                        if let Err(err) = target.peer.put_blob(blob_id, bytes).await {
                            warn!(
                                "Background file peer fan-out to {} failed for {}: {}",
                                target.loc, blob_id_hex, err
                            );
                            return;
                        }
                        if let Err(err) = metadata
                            .add_blob_replica_location(blob_id, &target.loc)
                            .await
                        {
                            warn!(
                                "Background file peer fan-out metadata update to {} failed for {}: {}",
                                target.loc, blob_id_hex, err
                            );
                        }
                    }
                }))
                .await;
                return;
            }

            let Some(temp_file) = stage_local_blob_to_temp_file(local, record, &blob_id_hex).await
            else {
                return;
            };
            let temp_path = temp_file.path();
            futures::future::join_all(targets.into_iter().map(|target| {
                let metadata = metadata.clone();
                let blob_id = &blob_id;
                let blob_id_hex = &blob_id_hex;
                async move {
                    let _permit = target.permit;
                    let _in_flight = target.in_flight;
                    if let Err(err) = target
                        .peer
                        .put_blob_from_file(blob_id, temp_path, size)
                        .await
                    {
                        warn!(
                            "Background file peer fan-out to {} failed uploading {}: {}",
                            target.loc, blob_id_hex, err
                        );
                        return;
                    }
                    if let Err(err) = metadata
                        .add_blob_replica_location(blob_id, &target.loc)
                        .await
                    {
                        warn!(
                            "Background file peer fan-out metadata update to {} failed for {}: {}",
                            target.loc, blob_id_hex, err
                        );
                    }
                }
            }))
            .await;
            drop_temp_file_blocking(temp_file).await;
        });
    }
}
