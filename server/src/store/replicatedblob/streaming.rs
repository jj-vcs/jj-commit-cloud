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

//! Streaming, ranged read, EOF verification, and `BlobStore` implementation for
//! `ReplicatedBlobStore`.
//!
//! # Authority and Crash-Safety Ordering
//! Writes target the local `BlobStore` first; background peer fan-out only starts once
//! `MetadataStore` publishes the `BlobRecord`. Small blobs and full reads coalesce through
//! singleflight, while larger ranged reads and streams forward directly with EOF size checks.
//!
//! # Cross-Process and RPC Boundaries
//! Streaming reads preserve `BlobStore::get_blob_range_stream` bounds and fail over from local
//! misses, transient errors, or corruption to configured peer replicas without buffering large
//! blobs in memory.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::mpsc;
use tracing::warn;

use super::{
    AttemptErrors, ReadSpan, ReplicatedBlobStore, ReplicatedInner, SMALL_BLOB_MAX_BYTES,
    VisibilityRetry, is_transient, validate_full_blob_size,
};
use crate::store::{
    BlobRecord, BlobStore, StoreError, StoreResult, expected_range_len, slice_blob_range,
    stream_inline_blob,
};

async fn verify_eof_on_store(store: &dyn BlobStore, record: &BlobRecord) -> StoreResult<bool> {
    match store
        .get_blob_range_with_record(record, record.content_size, Some(1))
        .await
    {
        Ok(Some(bytes)) if bytes.is_empty() => Ok(true),
        Ok(Some(_)) => Err(StoreError::Corruption(format!(
            "blob {} oversized in {}: found trailing bytes past expected EOF {}",
            hex::encode(&record.blob_id),
            store.location_id(),
            record.content_size
        ))),
        Ok(None) => Ok(false),
        Err(StoreError::OutOfRange(msg)) => Err(StoreError::Corruption(format!(
            "blob {} truncated in {} before expected EOF {}: {msg}",
            hex::encode(&record.blob_id),
            store.location_id(),
            record.content_size
        ))),
        Err(err) => Err(err),
    }
}

fn stream_bytes(bytes: Bytes) -> mpsc::Receiver<StoreResult<Bytes>> {
    let (tx, rx) = mpsc::channel(8);
    if !bytes.is_empty() {
        crate::resilience::spawn_blob_stream_producer(
            "replicated blob stream",
            tx,
            move |tx| async move {
                let mut pos = 0usize;
                while pos < bytes.len() {
                    let end = (pos + cc_common::FILE_STREAM_CHUNK_SIZE).min(bytes.len());
                    if tx.send(Ok(bytes.slice(pos..end))).await.is_err() {
                        return;
                    }
                    pos = end;
                }
            },
        );
    }
    rx
}

impl ReplicatedBlobStore {
    /// Verifies blob existence and exact EOF size using a pre-fetched [`BlobRecord`] across
    /// the in-memory cache, local store, and configured peer replicas without re-querying
    /// `MetadataStore`.
    pub(super) async fn verify_eof_with_record(&self, record: &BlobRecord) -> StoreResult<bool> {
        if let Some(inline) = &record.inline_content {
            validate_full_blob_size(&record.blob_id, inline, record.content_size)?;
            return Ok(true);
        }
        if let Some(cached_len) = self
            .inner
            .cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .cached_len(&record.blob_id)
        {
            if cached_len != record.content_size {
                return Err(StoreError::Corruption(format!(
                    "cached blob {} has {cached_len} bytes, recorded content_size is {}",
                    hex::encode(&record.blob_id),
                    record.content_size
                )));
            }
            return Ok(true);
        }

        let mut errors = AttemptErrors::default();
        if self.inner.verify_eof_local(record, &mut errors).await? {
            return Ok(true);
        }
        if self.inner.verify_eof_in_peers(record, &mut errors).await? {
            return Ok(true);
        }

        if errors.is_non_transient_only() {
            return errors.into_error().map_or(Ok(false), Err);
        }
        let Some(mut retry) = VisibilityRetry::start(&self.inner.config, &record.blob_id) else {
            return errors.into_error().map_or(Ok(false), Err);
        };
        while retry.sleep_next(&self.inner.stats).await? {
            errors.clear();
            if self.inner.verify_eof_local(record, &mut errors).await? {
                return Ok(true);
            }
            if self.inner.verify_eof_in_peers(record, &mut errors).await? {
                return Ok(true);
            }
            if errors.is_non_transient_only() {
                return errors.into_error().map_or(Ok(false), Err);
            }
        }

        errors.into_error().map_or(Ok(false), Err)
    }
}

impl ReplicatedInner {
    async fn open_local_stream(
        &self,
        record: &BlobRecord,
        offset: u64,
        length: Option<u64>,
        errors: &mut AttemptErrors,
    ) -> StoreResult<Option<mpsc::Receiver<StoreResult<Bytes>>>> {
        match self
            .local
            .clone()
            .get_blob_range_stream(record.clone(), offset, length)
            .await
        {
            Ok(Some(rx)) => {
                self.stats.local_hits.fetch_add(1, Ordering::Relaxed);
                Ok(Some(rx))
            }
            Ok(None) => Ok(None),
            Err(err) if is_transient(&err) => {
                warn!(
                    "Local blob stream failed for {}, trying peers: {}",
                    hex::encode(&record.blob_id),
                    err
                );
                errors.record(err);
                Ok(None)
            }
            Err(StoreError::OutOfRange(msg)) => {
                let err = StoreError::Corruption(format!(
                    "blob {} truncated in {} at offset {offset} (expected {} bytes): {msg}",
                    hex::encode(&record.blob_id),
                    self.local.location_id(),
                    record.content_size
                ));
                if !self.peers.is_empty() {
                    warn!(
                        "Local blob stream detected truncation for {}, trying peers: {}",
                        hex::encode(&record.blob_id),
                        err
                    );
                    errors.record(err);
                    return Ok(None);
                }
                Err(err)
            }
            Err(err @ StoreError::Corruption(_)) if !self.peers.is_empty() => {
                warn!(
                    "Local blob stream detected corruption for {}, trying peers: {}",
                    hex::encode(&record.blob_id),
                    err
                );
                errors.record(err);
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }

    async fn open_peer_stream(
        &self,
        record: &BlobRecord,
        offset: u64,
        length: Option<u64>,
        errors: &mut AttemptErrors,
    ) -> StoreResult<Option<mpsc::Receiver<StoreResult<Bytes>>>> {
        if self.peers.is_empty() {
            return Ok(None);
        }
        for loc in self.peer_candidate_locations(record) {
            if crate::resilience::remaining_rpc_deadline().is_some_and(|r| r.is_zero()) {
                return Err(StoreError::DeadlineExceeded(
                    "gRPC deadline expired during peer blob stream fallback".to_string(),
                ));
            }
            let Some(peer_store) = self.peers.get(loc) else {
                warn!(
                    "BlobRecord for {} lists peer location '{}' not configured in ReplicatedBlobStore",
                    hex::encode(&record.blob_id),
                    loc
                );
                continue;
            };
            match peer_store
                .clone()
                .get_blob_range_stream(record.clone(), offset, length)
                .await
            {
                Ok(Some(rx)) => {
                    self.stats.origin_fallbacks.fetch_add(1, Ordering::Relaxed);
                    return Ok(Some(rx));
                }
                Ok(None) => continue,
                Err(err @ StoreError::DeadlineExceeded(_)) => return Err(err),
                Err(StoreError::OutOfRange(msg)) => {
                    let err = StoreError::Corruption(format!(
                        "blob {} truncated in {} at offset {offset} (expected {} bytes): {msg}",
                        hex::encode(&record.blob_id),
                        loc,
                        record.content_size
                    ));
                    warn!(
                        "Peer blob stream from {} detected truncation for {}: {}",
                        loc,
                        hex::encode(&record.blob_id),
                        err
                    );
                    errors.record(err);
                }
                Err(err) => {
                    warn!(
                        "Peer blob stream from {} failed for {}: {}",
                        loc,
                        hex::encode(&record.blob_id),
                        err
                    );
                    errors.record(err);
                }
            }
        }
        Ok(None)
    }

    async fn verify_eof_local(
        &self,
        record: &BlobRecord,
        errors: &mut AttemptErrors,
    ) -> StoreResult<bool> {
        match verify_eof_on_store(self.local.as_ref(), record).await {
            Ok(present) => Ok(present),
            Err(err) if is_transient(&err) => {
                warn!(
                    "Local EOF check failed for {}, trying peers: {}",
                    hex::encode(&record.blob_id),
                    err
                );
                errors.record(err);
                Ok(false)
            }
            Err(err @ StoreError::Corruption(_)) if !self.peers.is_empty() => {
                warn!(
                    "Local EOF check detected corruption for {}, trying peers: {}",
                    hex::encode(&record.blob_id),
                    err
                );
                errors.record(err);
                Ok(false)
            }
            Err(err) => Err(err),
        }
    }

    async fn verify_eof_in_peers(
        &self,
        record: &BlobRecord,
        errors: &mut AttemptErrors,
    ) -> StoreResult<bool> {
        if self.peers.is_empty() {
            return Ok(false);
        }
        for loc in self.peer_candidate_locations(record) {
            if crate::resilience::remaining_rpc_deadline().is_some_and(|r| r.is_zero()) {
                return Err(StoreError::DeadlineExceeded(
                    "gRPC deadline expired during peer EOF check".to_string(),
                ));
            }
            let Some(peer_store) = self.peers.get(loc) else {
                warn!(
                    "BlobRecord for {} lists peer location '{}' not configured in ReplicatedBlobStore",
                    hex::encode(&record.blob_id),
                    loc
                );
                continue;
            };
            match verify_eof_on_store(peer_store.as_ref(), record).await {
                Ok(true) => return Ok(true),
                Ok(false) => continue,
                Err(err @ StoreError::DeadlineExceeded(_)) => return Err(err),
                Err(err) => {
                    warn!(
                        "Peer EOF check on {} failed for {}: {}",
                        loc,
                        hex::encode(&record.blob_id),
                        err
                    );
                    errors.record(err);
                }
            }
        }
        Ok(false)
    }

    /// Checks local blob presence. A transient error is recorded in `errors` and reported as
    /// `false` so the caller can try peers or retry.
    async fn has_blob_local(
        &self,
        blob_id: &[u8],
        errors: &mut AttemptErrors,
    ) -> StoreResult<bool> {
        match self.local.has_blob(blob_id).await {
            Ok(present) => Ok(present),
            Err(err) if is_transient(&err) => {
                warn!(
                    "Local has_blob failed for {}, trying peers: {}",
                    hex::encode(blob_id),
                    err
                );
                errors.record(err);
                Ok(false)
            }
            Err(err) => Err(err),
        }
    }

    async fn has_blob_in_peers(
        &self,
        blob_id: &[u8],
        record: &BlobRecord,
        errors: &mut AttemptErrors,
    ) -> StoreResult<bool> {
        if self.peers.is_empty() {
            return Ok(false);
        }
        for loc in self.peer_candidate_locations(record) {
            if crate::resilience::remaining_rpc_deadline().is_some_and(|r| r.is_zero()) {
                return Err(StoreError::DeadlineExceeded(
                    "gRPC deadline expired during peer has_blob check".to_string(),
                ));
            }
            let Some(peer_store) = self.peers.get(loc) else {
                warn!(
                    "BlobRecord for {} lists peer location '{}' not configured in ReplicatedBlobStore",
                    hex::encode(blob_id),
                    loc
                );
                continue;
            };
            match peer_store.has_blob(blob_id).await {
                Ok(true) => return Ok(true),
                Ok(false) => continue,
                Err(err @ StoreError::DeadlineExceeded(_)) => return Err(err),
                Err(err) => {
                    warn!(
                        "Peer has_blob on {} failed for {}: {}",
                        loc,
                        hex::encode(blob_id),
                        err
                    );
                    errors.record(err);
                }
            }
        }
        Ok(false)
    }
}

#[async_trait]
impl BlobStore for ReplicatedBlobStore {
    fn location_id(&self) -> &str {
        self.inner.local.location_id()
    }

    async fn put_blob(&self, blob_id: &[u8], data: Bytes) -> StoreResult<()> {
        // Uploads stay out of the read cache so agent writes do not evict entries readers use.
        self.inner.local.put_blob(blob_id, data).await
    }

    async fn put_blob_from_file(
        &self,
        blob_id: &[u8],
        temp_path: &Path,
        size: u64,
    ) -> StoreResult<()> {
        self.inner
            .local
            .put_blob_from_file(blob_id, temp_path, size)
            .await
    }

    async fn notify_blob_published(&self, blob_id: &[u8], data: Bytes) {
        if self.inner.config.async_write_fanout && !self.inner.peers.is_empty() {
            self.inner.spawn_write_fanout(blob_id.to_vec(), data);
        }
    }

    async fn notify_blob_file_published(&self, blob_id: &[u8], size: u64) {
        if self.inner.config.async_write_fanout && !self.inner.peers.is_empty() {
            self.inner.spawn_file_write_fanout(blob_id.to_vec(), size);
        }
    }

    async fn get_blob(&self, blob_id: &[u8]) -> StoreResult<Option<Bytes>> {
        self.fetch_full_coalesced(blob_id, None).await
    }

    async fn get_blob_range(
        &self,
        blob_id: &[u8],
        offset: u64,
        length: Option<u64>,
    ) -> StoreResult<Option<Bytes>> {
        if self.inner.config.cache_capacity_bytes > 0
            && let Some(cached) = self
                .inner
                .cache
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(blob_id)
        {
            self.inner.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            return slice_blob_range(&cached, offset, length).map(Some);
        }
        if offset == 0 && matches!(length, None | Some(0)) {
            return self.fetch_full_coalesced(blob_id, None).await;
        }
        // The content size decides between a whole-object read and a forwarded range.
        match self.inner.metadata.get_global_blob_record(blob_id).await? {
            Some(record) => {
                self.get_blob_range_with_record(&record, offset, length)
                    .await
            }
            None => {
                if self.inner.config.cache_capacity_bytes > 0 {
                    self.inner
                        .stats
                        .cache_misses
                        .fetch_add(1, Ordering::Relaxed);
                }
                self.inner
                    .local
                    .get_blob_range(blob_id, offset, length)
                    .await
            }
        }
    }

    async fn get_blob_range_with_record(
        &self,
        record: &BlobRecord,
        offset: u64,
        length: Option<u64>,
    ) -> StoreResult<Option<Bytes>> {
        if let Some(inline) = &record.inline_content {
            if inline.len() as u64 != record.content_size {
                return Err(StoreError::Corruption(format!(
                    "inline blob {} has {} bytes, recorded content_size is {}",
                    hex::encode(&record.blob_id),
                    inline.len(),
                    record.content_size
                )));
            }
            return slice_blob_range(inline, offset, length).map(Some);
        }

        if offset > record.content_size {
            return Err(StoreError::OutOfRange(format!(
                "requested read at offset {offset}, file size is {}",
                record.content_size
            )));
        }
        if record.content_size == 0 {
            // Non-inline 0-byte blobs at offset 0 still run through singleflight so existence
            // checks, peer fallback, read-repair, caching, and retries apply uniformly.
            let Some(full) = self
                .fetch_full_coalesced(&record.blob_id, Some(record))
                .await?
            else {
                return Ok(None);
            };
            validate_full_blob_size(&record.blob_id, &full, 0)?;
            return Ok(Some(full));
        }
        if offset == record.content_size {
            return if self.verify_eof_with_record(record).await? {
                Ok(Some(Bytes::new()))
            } else {
                Ok(None)
            };
        }

        let remaining = record.content_size - offset;
        let expected_len = expected_range_len(record.content_size, offset, length);

        // Whole-object reads and cacheable small blobs go through singleflight; partial reads
        // of uncacheable small blobs or large blobs forward only the requested byte range.
        if (offset == 0 && expected_len == record.content_size)
            || self
                .inner
                .can_cache_blob(&record.blob_id, record.content_size)
        {
            let Some(full) = self
                .fetch_full_coalesced(&record.blob_id, Some(record))
                .await?
            else {
                return Ok(None);
            };
            validate_full_blob_size(&record.blob_id, &full, record.content_size)?;
            return slice_blob_range(&full, offset, Some(expected_len)).map(Some);
        }

        // Ranged reads of blobs larger than `SMALL_BLOB_MAX_BYTES` are never inserted into the
        // in-memory small-blob cache; bypass the cache lock and miss counter and fetch only the
        // requested range from the local store first and then from peers.

        // Probe +1 byte whenever reading through EOF so oversized large blobs are caught by
        // `get_file_range` and direct `get_blob_range_with_record` callers (for unbounded,
        // `Some(remaining)`, and `Some(max_len > remaining)` tail reads) while keeping
        // `offset + request_len <= i64::MAX`.
        let probe_eof = expected_len == remaining;
        let request_len = if probe_eof && offset.saturating_add(remaining) < i64::MAX as u64 {
            remaining.saturating_add(1)
        } else {
            expected_len
        };
        let bytes = match self
            .inner
            .fetch_uncached(
                &record.blob_id,
                Some(record),
                ReadSpan::Range {
                    offset,
                    length: request_len,
                },
            )
            .await
        {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return Ok(None),
            Err(StoreError::OutOfRange(msg)) => {
                return Err(StoreError::Corruption(format!(
                    "blob {} truncated in object store at offset {offset} (expected {} bytes): {msg}",
                    hex::encode(&record.blob_id),
                    record.content_size
                )));
            }
            Err(err) => return Err(err),
        };
        if bytes.len() as u64 != expected_len {
            return Err(StoreError::Corruption(format!(
                "blob {} length mismatch at offset {offset}: expected {expected_len} bytes, got {} bytes",
                hex::encode(&record.blob_id),
                bytes.len()
            )));
        }
        Ok(Some(bytes))
    }

    async fn get_blob_range_stream(
        self: Arc<Self>,
        record: BlobRecord,
        offset: u64,
        length: Option<u64>,
    ) -> StoreResult<Option<mpsc::Receiver<StoreResult<Bytes>>>> {
        if offset > record.content_size {
            return Err(StoreError::OutOfRange(format!(
                "requested read at offset {offset}, file size is {}",
                record.content_size
            )));
        }
        if let Some(inline) = record.inline_content {
            return stream_inline_blob(
                &record.blob_id,
                record.content_size,
                inline,
                offset,
                length,
            );
        }

        let expected_len = expected_range_len(record.content_size, offset, length);
        let is_small_full_read = offset == 0
            && expected_len == record.content_size
            && record.content_size <= SMALL_BLOB_MAX_BYTES;
        if record.content_size == 0
            || offset == record.content_size
            || is_small_full_read
            || self
                .inner
                .can_cache_blob(&record.blob_id, record.content_size)
        {
            return match self
                .get_blob_range_with_record(&record, offset, length)
                .await?
            {
                Some(bytes) => Ok(Some(stream_bytes(bytes))),
                None => Ok(None),
            };
        }

        let mut errors = AttemptErrors::default();
        if let Some(rx) = self
            .inner
            .open_local_stream(&record, offset, length, &mut errors)
            .await?
        {
            return Ok(Some(rx));
        }
        if let Some(rx) = self
            .inner
            .open_peer_stream(&record, offset, length, &mut errors)
            .await?
        {
            return Ok(Some(rx));
        }

        if errors.is_non_transient_only() {
            return errors.into_error().map_or(Ok(None), Err);
        }
        let Some(mut retry) = VisibilityRetry::start(&self.inner.config, &record.blob_id) else {
            return errors.into_error().map_or(Ok(None), Err);
        };
        while retry.sleep_next(&self.inner.stats).await? {
            errors.clear();
            if let Some(rx) = self
                .inner
                .open_local_stream(&record, offset, length, &mut errors)
                .await?
            {
                return Ok(Some(rx));
            }
            if let Some(rx) = self
                .inner
                .open_peer_stream(&record, offset, length, &mut errors)
                .await?
            {
                return Ok(Some(rx));
            }
            if errors.is_non_transient_only() {
                return errors.into_error().map_or(Ok(None), Err);
            }
        }

        errors.into_error().map_or(Ok(None), Err)
    }

    async fn has_blob(&self, blob_id: &[u8]) -> StoreResult<bool> {
        if self
            .inner
            .cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(blob_id)
        {
            return Ok(true);
        }

        let mut errors = AttemptErrors::default();
        if self.inner.has_blob_local(blob_id, &mut errors).await? {
            return Ok(true);
        }

        let Some(record) = self.inner.metadata.get_global_blob_record(blob_id).await? else {
            return errors.into_error().map_or(Ok(false), Err);
        };
        if let Some(inline) = &record.inline_content {
            validate_full_blob_size(blob_id, inline, record.content_size)?;
            return Ok(true);
        }

        if self
            .inner
            .has_blob_in_peers(blob_id, &record, &mut errors)
            .await?
        {
            return Ok(true);
        }

        if errors.is_non_transient_only() {
            return errors.into_error().map_or(Ok(false), Err);
        }
        let Some(mut retry) = VisibilityRetry::start(&self.inner.config, blob_id) else {
            return errors.into_error().map_or(Ok(false), Err);
        };
        while retry.sleep_next(&self.inner.stats).await? {
            errors.clear();
            if self.inner.has_blob_local(blob_id, &mut errors).await? {
                return Ok(true);
            }
            if self
                .inner
                .has_blob_in_peers(blob_id, &record, &mut errors)
                .await?
            {
                return Ok(true);
            }
            if errors.is_non_transient_only() {
                return errors.into_error().map_or(Ok(false), Err);
            }
        }

        errors.into_error().map_or(Ok(false), Err)
    }
}
