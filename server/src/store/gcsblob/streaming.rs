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

//! Cloud Storage `BlobStore` data-plane operations, streaming reads, and EOF verification.
//!
//! # Authority and Crash-Safety Ordering
//! Full-object reads avoid `Range` headers so Cloud Storage returns HTTP 200 OK and validates
//! end-to-end CRC32C checksums. Ranged reads reaching EOF probe one extra byte so oversized
//! objects fail closed with `StoreError::Corruption` rather than silent truncation. Record-checked
//! ranged and streaming reads also validate reported object size and the first non-empty streaming
//! chunk synchronously before returning so `ReplicatedBlobStore` can fail over on local corruption
//! before gRPC response headers are committed.
//!
//! # Concurrency and Merge Invariants
//! Uploads set `if_generation_match(0)` and treat HTTP 412 (`FailedPrecondition`) as
//! idempotent success so concurrent uploads of identical digests avoid overwrite contention.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use google_cloud_storage::model_ext::ReadRange;
use google_cloud_storage::stub::Storage as StorageStub;
use tokio::sync::mpsc;

use super::{
    GcsBlobStore, IMMUTABLE_CACHE_CONTROL, is_not_found, is_out_of_range, is_precondition_failed,
    map_gcs_err, parse_content_range_total,
};
use crate::store::{
    BlobRecord, BlobStore, StoreError, StoreResult, expected_range_len, stream_inline_blob,
};

fn check_reported_object_size(
    blob_id: &[u8],
    reported_size: i64,
    expected_total_size: u64,
) -> StoreResult<()> {
    // `google-cloud-storage` defaults `ObjectHighlights.size` to 0 when
    // `x-goog-stored-content-length` is omitted, so only negative or non-zero mismatches fail
    // here; 0-byte truncated objects are caught by body length / first-chunk checks.
    if reported_size < 0 || (reported_size > 0 && reported_size as u64 != expected_total_size) {
        return Err(StoreError::Corruption(format!(
            "blob {} has reported size {reported_size} in Cloud Storage, expected {expected_total_size} bytes",
            hex::encode(blob_id)
        )));
    }
    Ok(())
}

async fn send_stream_chunk(tx: &mpsc::Sender<StoreResult<Bytes>>, chunk: Bytes) -> bool {
    let mut pos = 0usize;
    while pos < chunk.len() {
        let end = (pos + cc_common::FILE_STREAM_CHUNK_SIZE).min(chunk.len());
        if tx.send(Ok(chunk.slice(pos..end))).await.is_err() {
            return false;
        }
        pos = end;
    }
    true
}

impl<S> GcsBlobStore<S>
where
    S: StorageStub + 'static,
{
    async fn has_byte_at(&self, key: &str, byte_index: u64) -> StoreResult<Option<bool>> {
        let mut reader = match self
            .client
            .read_object(&self.bucket_resource, key)
            .set_read_range(ReadRange::segment(byte_index, 1))
            .send()
            .await
        {
            Ok(r) => r,
            Err(err) if is_not_found(&err) => return Ok(None),
            Err(err) if is_out_of_range(&err) => return Ok(Some(false)),
            Err(err) => return Err(map_gcs_err(err)),
        };
        let mut found = false;
        while let Some(chunk) = reader.next().await.transpose().map_err(map_gcs_err)? {
            if !chunk.is_empty() {
                found = true;
            }
        }
        Ok(Some(found))
    }

    async fn read_full_checked(
        &self,
        blob_id: &[u8],
        expected_total_size: Option<u64>,
    ) -> StoreResult<Option<Bytes>> {
        let key = Self::object_key(blob_id)?;
        let mut reader = match self
            .client
            .read_object(&self.bucket_resource, key)
            .send()
            .await
        {
            Ok(r) => r,
            Err(err) if is_not_found(&err) => return Ok(None),
            Err(err) => return Err(map_gcs_err(err)),
        };
        if let Some(expected_total) = expected_total_size {
            check_reported_object_size(blob_id, reader.object().size, expected_total)?;
        }

        let mut buf = BytesMut::new();
        while let Some(chunk) = reader.next().await.transpose().map_err(map_gcs_err)? {
            buf.extend_from_slice(&chunk);
        }
        Ok(Some(buf.freeze()))
    }

    async fn read_range_checked(
        &self,
        blob_id: &[u8],
        offset: u64,
        length: Option<u64>,
        expected_total_size: Option<u64>,
    ) -> StoreResult<Option<Bytes>> {
        if offset == 0 && matches!(length, None | Some(0)) {
            return self.read_full_checked(blob_id, expected_total_size).await;
        }

        let key = Self::object_key(blob_id)?;
        if offset > i64::MAX as u64 {
            return if self.has_blob(blob_id).await? {
                Err(StoreError::OutOfRange(format!(
                    "requested read at offset {offset} exceeds maximum Cloud Storage object offset"
                )))
            } else {
                Ok(None)
            };
        }
        // The client builds the inclusive header `bytes=offset-(offset+length-1)` in i64
        // arithmetic, so read to the end when the requested span reaches past i64::MAX.
        let range = match length {
            Some(max_len) if max_len > 0 && offset.saturating_add(max_len) <= i64::MAX as u64 => {
                ReadRange::segment(offset, max_len)
            }
            _ => ReadRange::offset(offset),
        };

        let mut reader = match self
            .client
            .read_object(&self.bucket_resource, &key)
            .set_read_range(range)
            .send()
            .await
        {
            Ok(r) => r,
            Err(err) if is_not_found(&err) => return Ok(None),
            // HTTP Range requests return 416 / OutOfRange whenever `offset >= total_len`,
            // including `offset == total_len` (which backend.proto defines as an empty read)
            // and 0-byte objects at `offset == 0`.
            Err(err) if is_out_of_range(&err) => {
                if let Some(expected_total) = expected_total_size
                    && offset < expected_total
                {
                    return Err(StoreError::Corruption(format!(
                        "blob {} truncated in Cloud Storage at offset {offset} (expected {expected_total} bytes): {err}",
                        hex::encode(blob_id)
                    )));
                }
                if offset == 0 {
                    return Ok(Some(Bytes::new()));
                }
                if let Some(total_len) = parse_content_range_total(&err) {
                    return if offset == total_len {
                        Ok(Some(Bytes::new()))
                    } else {
                        Err(StoreError::OutOfRange(format!(
                            "requested read at offset {offset}, file size is {total_len}"
                        )))
                    };
                }
                return match self.has_byte_at(&key, offset - 1).await? {
                    Some(true) => Ok(Some(Bytes::new())),
                    Some(false) => Err(map_gcs_err(err)),
                    None => Ok(None),
                };
            }
            Err(err) => return Err(map_gcs_err(err)),
        };

        if let Some(expected_total) = expected_total_size {
            check_reported_object_size(blob_id, reader.object().size, expected_total)?;
        }

        let mut buf = BytesMut::new();
        while let Some(chunk) = reader.next().await.transpose().map_err(map_gcs_err)? {
            buf.extend_from_slice(&chunk);
        }
        Ok(Some(buf.freeze()))
    }

    /// Verifies that the backing object exists and has exact length `content_size`.
    ///
    /// Returns `Ok(None)` when the object is missing, `Ok(Some(()))` when the object ends
    /// exactly at `content_size`, and `Err(StoreError::Corruption(_))` when the object is
    /// truncated or has extra trailing bytes.
    pub(super) async fn verify_exact_eof(
        &self,
        blob_id: &[u8],
        key: &str,
        content_size: u64,
    ) -> StoreResult<Option<()>> {
        if content_size == 0 {
            let mut reader = match self
                .client
                .read_object(&self.bucket_resource, key)
                .set_read_range(ReadRange::segment(0, 1))
                .send()
                .await
            {
                Ok(r) => r,
                Err(err) if is_not_found(&err) => return Ok(None),
                // A 0-byte object exists but has no byte at offset 0.
                Err(err) if is_out_of_range(&err) => return Ok(Some(())),
                Err(err) => return Err(map_gcs_err(err)),
            };
            check_reported_object_size(blob_id, reader.object().size, 0)?;
            while let Some(chunk) = reader.next().await.transpose().map_err(map_gcs_err)? {
                if !chunk.is_empty() {
                    return Err(StoreError::Corruption(format!(
                        "blob {} oversized in Cloud Storage: expected 0 bytes, found trailing bytes past EOF",
                        hex::encode(blob_id)
                    )));
                }
            }
            return Ok(Some(()));
        }

        // Probe the 2-byte window `[content_size - 1, content_size]`:
        // - 404 => missing (`Ok(None)`)
        // - 416 / 0 bytes => truncated (`Corruption`)
        // - 1 byte => exact EOF (`Ok(Some(()))`)
        // - 2+ bytes => oversized (`Corruption`)
        let probe_start = content_size - 1;
        let bounded = probe_start.saturating_add(2) <= i64::MAX as u64;
        let range = if bounded {
            ReadRange::segment(probe_start, 2)
        } else {
            ReadRange::offset(probe_start)
        };
        let mut reader = match self
            .client
            .read_object(&self.bucket_resource, key)
            .set_read_range(range)
            .send()
            .await
        {
            Ok(r) => r,
            Err(err) if is_not_found(&err) => return Ok(None),
            Err(err) if is_out_of_range(&err) => {
                return Err(StoreError::Corruption(format!(
                    "blob {} truncated in Cloud Storage before expected EOF {content_size}: {err}",
                    hex::encode(blob_id)
                )));
            }
            Err(err) => return Err(map_gcs_err(err)),
        };
        check_reported_object_size(blob_id, reader.object().size, content_size)?;

        let mut probed = 0usize;
        while let Some(chunk) = reader.next().await.transpose().map_err(map_gcs_err)? {
            probed = probed.saturating_add(chunk.len());
            if !bounded && probed > 1 {
                break;
            }
        }
        match probed {
            1 => Ok(Some(())),
            0 => Err(StoreError::Corruption(format!(
                "blob {} truncated in Cloud Storage before expected EOF {content_size}",
                hex::encode(blob_id)
            ))),
            _ => Err(StoreError::Corruption(format!(
                "blob {} oversized in Cloud Storage: expected {content_size} bytes, found trailing bytes past EOF",
                hex::encode(blob_id)
            ))),
        }
    }
}

#[async_trait]
impl<S> BlobStore for GcsBlobStore<S>
where
    S: StorageStub + 'static,
{
    fn location_id(&self) -> &str {
        &self.location_uri
    }

    async fn put_blob(&self, blob_id: &[u8], data: Bytes) -> StoreResult<()> {
        let key = Self::object_key(blob_id)?;
        let res = self
            .client
            .write_object(&self.bucket_resource, key, data)
            .set_if_generation_match(0)
            .set_cache_control(IMMUTABLE_CACHE_CONTROL)
            .set_content_type("application/octet-stream")
            .send_buffered()
            .await;

        match res {
            Ok(_) => Ok(()),
            // Object already exists with identical content-addressed digest.
            Err(err) if is_precondition_failed(&err) => Ok(()),
            Err(err) => Err(map_gcs_err(err)),
        }
    }

    async fn put_blob_from_file(
        &self,
        blob_id: &[u8],
        temp_path: &Path,
        size: u64,
    ) -> StoreResult<()> {
        let key = Self::object_key(blob_id)?;
        let file = tokio::fs::File::open(temp_path).await.map_err(|e| {
            StoreError::Backend(format!(
                "failed to open staged blob {}: {e}",
                temp_path.display()
            ))
        })?;
        let actual_size = file
            .metadata()
            .await
            .map_err(|e| {
                StoreError::Backend(format!(
                    "failed to stat staged blob {}: {e}",
                    temp_path.display()
                ))
            })?
            .len();
        if actual_size != size {
            return Err(StoreError::Backend(format!(
                "staged blob {} has {actual_size} bytes, expected {size}",
                temp_path.display()
            )));
        }

        let res = self
            .client
            .write_object(&self.bucket_resource, key, file)
            .set_if_generation_match(0)
            .set_cache_control(IMMUTABLE_CACHE_CONTROL)
            .set_content_type("application/octet-stream")
            .send_unbuffered()
            .await;

        match res {
            Ok(_) => Ok(()),
            // Object already exists with identical content-addressed digest.
            Err(err) if is_precondition_failed(&err) => Ok(()),
            Err(err) => Err(map_gcs_err(err)),
        }
    }

    async fn get_blob(&self, blob_id: &[u8]) -> StoreResult<Option<Bytes>> {
        self.read_full_checked(blob_id, None).await
    }

    async fn get_blob_range(
        &self,
        blob_id: &[u8],
        offset: u64,
        length: Option<u64>,
    ) -> StoreResult<Option<Bytes>> {
        self.read_range_checked(blob_id, offset, length, None).await
    }

    async fn get_blob_range_with_record(
        &self,
        record: &BlobRecord,
        offset: u64,
        length: Option<u64>,
    ) -> StoreResult<Option<Bytes>> {
        if offset > record.content_size {
            return Err(StoreError::OutOfRange(format!(
                "requested read at offset {offset}, file size is {}",
                record.content_size
            )));
        }
        let key = Self::object_key(&record.blob_id)?;
        if offset == record.content_size {
            return match self
                .verify_exact_eof(&record.blob_id, &key, record.content_size)
                .await?
            {
                Some(()) => Ok(Some(Bytes::new())),
                None => Ok(None),
            };
        }

        let remaining = record.content_size - offset;
        let expected_len = expected_range_len(record.content_size, offset, length);
        // Full-object reads avoid Range requests so Cloud Storage returns HTTP 200 OK and the
        // client validates the object's CRC32C checksum automatically.
        if offset == 0 && expected_len == record.content_size {
            let Some(bytes) = self
                .read_full_checked(&record.blob_id, Some(record.content_size))
                .await?
            else {
                return Ok(None);
            };
            if bytes.len() as u64 != record.content_size {
                return Err(StoreError::Corruption(format!(
                    "blob {} has {} bytes in Cloud Storage, expected {} bytes",
                    hex::encode(&record.blob_id),
                    bytes.len(),
                    record.content_size
                )));
            }
            return Ok(Some(bytes));
        }

        // Probe +1 byte whenever reading through EOF so oversized objects are detected rather
        // than silently truncated to `remaining`, adding zero extra wire bytes on healthy objects.
        let probe_eof = expected_len == remaining;
        let req_len = if probe_eof {
            if offset.saturating_add(remaining) < i64::MAX as u64 {
                Some(remaining + 1)
            } else {
                None
            }
        } else {
            Some(expected_len)
        };
        let bytes = match self
            .read_range_checked(&record.blob_id, offset, req_len, Some(record.content_size))
            .await
        {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return Ok(None),
            Err(StoreError::OutOfRange(msg)) => {
                return Err(StoreError::Corruption(format!(
                    "blob {} truncated in Cloud Storage at offset {offset} (expected {} bytes): {msg}",
                    hex::encode(&record.blob_id),
                    record.content_size
                )));
            }
            Err(err) => return Err(err),
        };
        if bytes.len() as u64 != expected_len {
            return Err(StoreError::Corruption(format!(
                "blob {} length mismatch in Cloud Storage at offset {offset}: expected {expected_len} bytes, got {} bytes",
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

        let key = Self::object_key(&record.blob_id)?;
        if offset == record.content_size {
            return match self
                .verify_exact_eof(&record.blob_id, &key, record.content_size)
                .await?
            {
                Some(()) => {
                    let (_tx, rx) = mpsc::channel(1);
                    Ok(Some(rx))
                }
                None => Ok(None),
            };
        }

        let remaining = record.content_size - offset;
        let expected_len = expected_range_len(record.content_size, offset, length);
        debug_assert!(expected_len > 0);
        let is_full_read = offset == 0 && expected_len == record.content_size;

        let mut req = self.client.read_object(&self.bucket_resource, &key);
        if !is_full_read {
            // Probe +1 byte whenever streaming through EOF so oversized objects surface as
            // Corruption without adding extra wire bytes on healthy objects.
            let probe_eof = offset.saturating_add(expected_len) == record.content_size;
            let range = if probe_eof {
                if offset.saturating_add(remaining) < i64::MAX as u64 {
                    ReadRange::segment(offset, remaining + 1)
                } else {
                    ReadRange::offset(offset)
                }
            } else if offset.saturating_add(expected_len) <= i64::MAX as u64 {
                ReadRange::segment(offset, expected_len)
            } else {
                ReadRange::offset(offset)
            };
            req = req.set_read_range(range);
        }

        let mut reader = match req.send().await {
            Ok(r) => r,
            Err(err) if is_not_found(&err) => return Ok(None),
            Err(err) if is_out_of_range(&err) => {
                return Err(StoreError::Corruption(format!(
                    "blob {} truncated in Cloud Storage at offset {offset} (expected {} bytes): {err}",
                    hex::encode(&record.blob_id),
                    record.content_size
                )));
            }
            Err(err) => return Err(map_gcs_err(err)),
        };

        check_reported_object_size(&record.blob_id, reader.object().size, record.content_size)?;

        // Validate the first non-empty chunk before returning `Ok(Some(rx))` so 0-byte truncated
        // objects, immediate stream errors, and single-chunk overflows fail synchronously and
        // allow `ReplicatedBlobStore` to fail over to peers before response headers are committed.
        let blob_id_hex = hex::encode(&record.blob_id);
        let first_chunk = loop {
            match reader.next().await {
                Some(Ok(chunk)) if chunk.is_empty() => continue,
                Some(Ok(chunk)) => break chunk,
                Some(Err(err)) => return Err(map_gcs_err(err)),
                None => {
                    return Err(StoreError::Corruption(format!(
                        "blob {blob_id_hex} truncated in Cloud Storage at offset {offset}: expected {expected_len} bytes, got 0 bytes"
                    )));
                }
            }
        };
        let first_len = first_chunk.len() as u64;
        if first_len > expected_len {
            return Err(StoreError::Corruption(format!(
                "blob {blob_id_hex} oversized in Cloud Storage at offset {offset}: expected {expected_len} bytes, received at least {first_len} bytes"
            )));
        }

        let (tx, rx) = mpsc::channel(8);
        tokio::spawn(async move {
            if !send_stream_chunk(&tx, first_chunk).await {
                return;
            }
            let mut received = first_len;
            // Always drain until `reader.next()` returns `None` so full HTTP 200 streams execute
            // end-of-stream CRC32C verification and trailing bytes past `expected_len` are caught.
            while let Some(item) = reader.next().await {
                let chunk = match item {
                    Ok(c) => c,
                    Err(err) => {
                        let _ = tx.send(Err(map_gcs_err(err))).await;
                        return;
                    }
                };
                if chunk.is_empty() {
                    continue;
                }
                let next_received = received.saturating_add(chunk.len() as u64);
                if next_received > expected_len {
                    let _ = tx
                        .send(Err(StoreError::Corruption(format!(
                            "blob {blob_id_hex} oversized in Cloud Storage at offset {offset}: expected {expected_len} bytes, received at least {next_received} bytes"
                        ))))
                        .await;
                    return;
                }
                if !send_stream_chunk(&tx, chunk).await {
                    return;
                }
                received = next_received;
            }
            if received != expected_len {
                let _ = tx
                    .send(Err(StoreError::Corruption(format!(
                        "blob {blob_id_hex} truncated in Cloud Storage at offset {offset}: expected {expected_len} bytes, got {received} bytes"
                    ))))
                    .await;
            }
        });

        Ok(Some(rx))
    }

    async fn has_blob(&self, blob_id: &[u8]) -> StoreResult<bool> {
        let key = Self::object_key(blob_id)?;
        match self
            .client
            .read_object(&self.bucket_resource, key)
            .set_read_range(ReadRange::segment(0, 1))
            .send()
            .await
        {
            Ok(mut reader) => {
                // Drain the 1-byte body so HTTP/1.1 keep-alive connections return cleanly to the
                // pool instead of being dropped mid-stream.
                while let Some(Ok(_)) = reader.next().await {}
                Ok(true)
            }
            Err(err) if is_not_found(&err) => Ok(false),
            // A 0-byte object exists but has no byte at offset 0.
            Err(err) if is_out_of_range(&err) => Ok(true),
            Err(err) => Err(map_gcs_err(err)),
        }
    }
}
