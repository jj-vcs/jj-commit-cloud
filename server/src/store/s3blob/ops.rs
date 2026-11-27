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

//! `BlobStore` trait implementation for `S3BlobStore` and staged-file streaming uploads.
//!
//! # Authority and Crash-Safety Ordering
//! Conditional `PUT` with `If-None-Match: *` ensures immutable content-addressed blobs are
//! written before metadata publication without overwriting existing objects. Range and
//! streaming reads validate `Content-Range` and `Content-Length` bounds against `BlobRecord`
//! metadata to surface truncation or oversized payloads as [`StoreError::Corruption`].

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use reqwest::{Body, Method, StatusCode};
use sha2::{Digest as _, Sha256};
use tokio::sync::mpsc;

use super::sigv4::{EMPTY_PAYLOAD_SHA256, sha256_hex};
use super::{
    MAX_ERROR_BODY_BYTES, MAX_RETRY_ATTEMPTS, S3BlobStore, is_retryable_body_err,
    map_http_status_err, map_reqwest_err, parse_content_length, parse_content_range_start,
    parse_content_range_total, read_bounded_body, sleep_retry_backoff,
};
use crate::store::gcsblob::IMMUTABLE_CACHE_CONTROL;
use crate::store::{
    BlobRecord, BlobStore, StoreError, StoreResult, expected_range_len, slice_blob_range,
    stream_inline_blob,
};

#[async_trait]
impl BlobStore for S3BlobStore {
    fn location_id(&self) -> &str {
        &self.inner.location_uri
    }

    async fn put_blob(&self, blob_id: &[u8], data: Bytes) -> StoreResult<()> {
        let key = Self::object_key(blob_id)?;
        let encoded_path = self.object_path(&key);
        let payload_hash = sha256_hex(&data);
        let headers = [
            ("cache-control", IMMUTABLE_CACHE_CONTROL),
            ("content-type", "application/octet-stream"),
            ("if-none-match", "*"),
        ];
        self.send_put_blob_with_retry(blob_id, &encoded_path, &headers, &payload_hash, |_| {
            let cloned = data.clone();
            async move { Ok(Some(Body::from(cloned))) }
        })
        .await
    }

    async fn put_blob_from_file(
        &self,
        blob_id: &[u8],
        temp_path: &Path,
        size: u64,
    ) -> StoreResult<()> {
        let key = Self::object_key(blob_id)?;
        let encoded_path = self.object_path(&key);
        let path_buf = temp_path.to_path_buf();
        let (std_file, payload_hash) =
            tokio::task::spawn_blocking(move || hash_and_rewind_staged_file(&path_buf, size))
                .await
                .map_err(|e| {
                    StoreError::Backend(format!("staged blob SHA-256 task failed: {e}"))
                })??;

        let content_len = size.to_string();
        let headers = [
            ("cache-control", IMMUTABLE_CACHE_CONTROL),
            ("content-length", content_len.as_str()),
            ("content-type", "application/octet-stream"),
            ("if-none-match", "*"),
        ];
        let mut initial_file = Some(std_file);
        self.send_put_blob_with_retry(blob_id, &encoded_path, &headers, &payload_hash, |_| {
            let staged_file = initial_file.take();
            async move {
                if size == 0 {
                    return Ok(Some(Body::from(Bytes::new())));
                }
                let async_file = match staged_file {
                    Some(file) => tokio::fs::File::from_std(file),
                    None => tokio::fs::File::open(temp_path).await.map_err(|e| {
                        StoreError::Backend(format!(
                            "failed to reopen staged blob {} for S3 retry: {e}",
                            temp_path.display()
                        ))
                    })?,
                };
                Ok(Some(staged_file_body(async_file, size)))
            }
        })
        .await
    }

    async fn get_blob(&self, blob_id: &[u8]) -> StoreResult<Option<Bytes>> {
        let key = Self::object_key(blob_id)?;
        let encoded_path = self.object_path(&key);
        let mut attempt = 0;
        loop {
            let retry_allowed = attempt + 1 < MAX_RETRY_ATTEMPTS;
            let Some(resp) = self
                .send_request_attempt(
                    "GET object",
                    Method::GET,
                    &encoded_path,
                    &[],
                    EMPTY_PAYLOAD_SHA256,
                    None,
                    retry_allowed,
                )
                .await?
            else {
                sleep_retry_backoff(attempt).await;
                attempt += 1;
                continue;
            };
            let status = resp.status();
            if status == StatusCode::NOT_FOUND {
                let body = read_bounded_body(resp, MAX_ERROR_BODY_BYTES).await;
                if String::from_utf8_lossy(&body).contains("<Code>NoSuchBucket</Code>") {
                    return Err(map_http_status_err(status, "GET object", &body));
                }
                return Ok(None);
            }
            if status == StatusCode::OK {
                match resp.bytes().await {
                    Ok(bytes) => return Ok(Some(bytes)),
                    Err(err) if is_retryable_body_err(&err) && retry_allowed => {
                        sleep_retry_backoff(attempt).await;
                        attempt += 1;
                        continue;
                    }
                    Err(err) => return Err(map_reqwest_err(err)),
                }
            }
            let body = read_bounded_body(resp, MAX_ERROR_BODY_BYTES).await;
            return Err(map_http_status_err(status, "GET object", &body));
        }
    }

    async fn get_blob_range(
        &self,
        blob_id: &[u8],
        offset: u64,
        length: Option<u64>,
    ) -> StoreResult<Option<Bytes>> {
        if offset == 0 && matches!(length, None | Some(0)) {
            return self.get_blob(blob_id).await;
        }

        let key = Self::object_key(blob_id)?;
        let encoded_path = self.object_path(&key);
        if offset > i64::MAX as u64 {
            return match self.head_content_length(&encoded_path).await? {
                Some(total_len) => Err(StoreError::OutOfRange(format!(
                    "requested read at offset {offset}, file size is {total_len}"
                ))),
                None => Ok(None),
            };
        }
        let range_value = match length {
            Some(max_len) if max_len > 0 => match offset.checked_add(max_len - 1) {
                Some(end) if end <= i64::MAX as u64 => format!("bytes={offset}-{end}"),
                _ => format!("bytes={offset}-"),
            },
            _ => format!("bytes={offset}-"),
        };

        let mut attempt = 0;
        loop {
            let retry_allowed = attempt + 1 < MAX_RETRY_ATTEMPTS;
            let Some(resp) = self
                .send_request_attempt(
                    "GET object range",
                    Method::GET,
                    &encoded_path,
                    &[("range", range_value.as_str())],
                    EMPTY_PAYLOAD_SHA256,
                    None,
                    retry_allowed,
                )
                .await?
            else {
                sleep_retry_backoff(attempt).await;
                attempt += 1;
                continue;
            };
            let status = resp.status();

            match status {
                StatusCode::PARTIAL_CONTENT => {
                    if let Some(start) = parse_content_range_start(resp.headers())
                        && start != offset
                    {
                        return Err(StoreError::Corruption(format!(
                            "blob {} Content-Range start mismatch in S3: expected offset {offset}, got {start}",
                            hex::encode(blob_id)
                        )));
                    }
                    let bytes = match resp.bytes().await {
                        Ok(bytes) => bytes,
                        Err(err) if is_retryable_body_err(&err) && retry_allowed => {
                            sleep_retry_backoff(attempt).await;
                            attempt += 1;
                            continue;
                        }
                        Err(err) => return Err(map_reqwest_err(err)),
                    };
                    if let Some(max_len) = length.filter(|&l| l > 0)
                        && (bytes.len() as u64) > max_len
                    {
                        return Ok(Some(bytes.slice(..(max_len as usize))));
                    }
                    return Ok(Some(bytes));
                }
                // Some S3-compatible implementations ignore Range headers and return the full body.
                StatusCode::OK => {
                    let full = match resp.bytes().await {
                        Ok(bytes) => bytes,
                        Err(err) if is_retryable_body_err(&err) && retry_allowed => {
                            sleep_retry_backoff(attempt).await;
                            attempt += 1;
                            continue;
                        }
                        Err(err) => return Err(map_reqwest_err(err)),
                    };
                    return slice_blob_range(&full, offset, length).map(Some);
                }
                StatusCode::NOT_FOUND => {
                    let body = read_bounded_body(resp, MAX_ERROR_BODY_BYTES).await;
                    if String::from_utf8_lossy(&body).contains("<Code>NoSuchBucket</Code>") {
                        return Err(map_http_status_err(status, "GET object range", &body));
                    } else {
                        return Ok(None);
                    }
                }
                // HTTP Range requests return 416 whenever `offset >= total_len`, including
                // `offset == total_len` (which backend.proto defines as an empty read) and 0-byte
                // objects at `offset == 0`.
                StatusCode::RANGE_NOT_SATISFIABLE => {
                    let header_total = parse_content_range_total(resp.headers());
                    let _ = read_bounded_body(resp, MAX_ERROR_BODY_BYTES).await;
                    if let Some(total_len) = header_total {
                        return if offset == total_len {
                            Ok(Some(Bytes::new()))
                        } else {
                            Err(StoreError::OutOfRange(format!(
                                "requested read at offset {offset}, file size is {total_len}"
                            )))
                        };
                    }
                    if offset == 0 {
                        return Ok(Some(Bytes::new()));
                    }
                    return match self.head_content_length(&encoded_path).await? {
                        Some(total_len) if offset == total_len => Ok(Some(Bytes::new())),
                        Some(total_len) => Err(StoreError::OutOfRange(format!(
                            "requested read at offset {offset}, file size is {total_len}"
                        ))),
                        None => Ok(None),
                    };
                }
                _ => {
                    let body = read_bounded_body(resp, MAX_ERROR_BODY_BYTES).await;
                    return Err(map_http_status_err(status, "GET object range", &body));
                }
            }
        }
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
        let encoded_path = self.object_path(&key);
        if offset == record.content_size {
            return match self.head_content_length(&encoded_path).await? {
                None => Ok(None),
                Some(actual) if actual == record.content_size => Ok(Some(Bytes::new())),
                Some(actual) => Err(StoreError::Corruption(format!(
                    "blob {} has {actual} bytes in S3, expected {} bytes",
                    hex::encode(&record.blob_id),
                    record.content_size
                ))),
            };
        }

        let remaining = record.content_size - offset;
        let expected_len = expected_range_len(record.content_size, offset, length);
        if offset == 0 && expected_len == record.content_size {
            let Some(bytes) = self.get_blob(&record.blob_id).await? else {
                return Ok(None);
            };
            if bytes.len() as u64 != record.content_size {
                return Err(StoreError::Corruption(format!(
                    "blob {} has {} bytes in S3, expected {} bytes",
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
        let bytes = match self.get_blob_range(&record.blob_id, offset, req_len).await {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return Ok(None),
            Err(StoreError::OutOfRange(msg)) => {
                return Err(StoreError::Corruption(format!(
                    "blob {} truncated in S3 at offset {offset} (expected {} bytes): {msg}",
                    hex::encode(&record.blob_id),
                    record.content_size
                )));
            }
            Err(err) => return Err(err),
        };
        if bytes.len() as u64 != expected_len {
            return Err(StoreError::Corruption(format!(
                "blob {} length mismatch in S3 at offset {offset}: expected {expected_len} bytes, got {} bytes",
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
        let encoded_path = self.object_path(&key);
        let blob_id_hex = hex::encode(&record.blob_id);
        if offset == record.content_size {
            return match self.head_content_length(&encoded_path).await? {
                None => Ok(None),
                Some(actual) if actual == record.content_size => {
                    let (_tx, rx) = mpsc::channel(1);
                    Ok(Some(rx))
                }
                Some(actual) => Err(StoreError::Corruption(format!(
                    "blob {blob_id_hex} has {actual} bytes in S3, expected {} bytes",
                    record.content_size
                ))),
            };
        }

        let remaining = record.content_size - offset;
        let expected_len = expected_range_len(record.content_size, offset, length);
        let is_full_read = offset == 0 && expected_len == record.content_size;
        let op_name = if is_full_read {
            "GET object"
        } else {
            "GET object range"
        };

        let resp = if is_full_read {
            self.send_request(op_name, Method::GET, &encoded_path, &[], None)
                .await?
        } else {
            // Probe +1 byte whenever streaming through EOF so oversized objects surface as
            // Corruption without adding extra wire bytes on healthy objects.
            let probe_eof = offset.saturating_add(expected_len) == record.content_size;
            let range_value = if probe_eof {
                if offset.saturating_add(remaining) < i64::MAX as u64 {
                    format!("bytes={offset}-{}", offset + remaining)
                } else {
                    format!("bytes={offset}-")
                }
            } else if let Some(end) = offset.checked_add(expected_len - 1)
                && end <= i64::MAX as u64
            {
                format!("bytes={offset}-{end}")
            } else {
                format!("bytes={offset}-")
            };
            self.send_request(
                op_name,
                Method::GET,
                &encoded_path,
                &[("range", range_value.as_str())],
                None,
            )
            .await?
        };

        let status = resp.status();

        match status {
            StatusCode::NOT_FOUND => {
                let body = read_bounded_body(resp, MAX_ERROR_BODY_BYTES).await;
                if String::from_utf8_lossy(&body).contains("<Code>NoSuchBucket</Code>") {
                    Err(map_http_status_err(status, op_name, &body))
                } else {
                    Ok(None)
                }
            }
            StatusCode::RANGE_NOT_SATISFIABLE => {
                let header_total = parse_content_range_total(resp.headers());
                let _ = read_bounded_body(resp, MAX_ERROR_BODY_BYTES).await;
                let detail = match header_total {
                    Some(total) => format!("S3 reported object size {total} bytes"),
                    None => "S3 returned HTTP 416 Range Not Satisfiable".to_string(),
                };
                Err(StoreError::Corruption(format!(
                    "blob {blob_id_hex} truncated in S3 at offset {offset} (expected {} bytes): {detail}",
                    record.content_size
                )))
            }
            StatusCode::PARTIAL_CONTENT => {
                if let Some(start) = parse_content_range_start(resp.headers())
                    && start != offset
                {
                    return Err(StoreError::Corruption(format!(
                        "blob {blob_id_hex} Content-Range start mismatch in S3: expected offset {offset}, got {start}"
                    )));
                }
                if let Some(total) = parse_content_range_total(resp.headers())
                    && total != record.content_size
                {
                    return Err(StoreError::Corruption(format!(
                        "blob {blob_id_hex} has {total} bytes in S3, expected {} bytes",
                        record.content_size
                    )));
                }
                if let Some(content_len) = parse_content_length(resp.headers())
                    && content_len != expected_len
                {
                    return Err(StoreError::Corruption(format!(
                        "blob {blob_id_hex} length mismatch in S3 at offset {offset}: expected {expected_len} bytes, got {content_len} bytes"
                    )));
                }

                let (tx, rx) = mpsc::channel(8);
                tokio::spawn(async move {
                    let mut resp = resp;
                    let mut received = 0u64;
                    loop {
                        let next_chunk = tokio::select! {
                            biased;
                            _ = tx.closed() => return,
                            res = resp.chunk() => res,
                        };
                        match next_chunk {
                            Ok(Some(chunk)) => {
                                if chunk.is_empty() {
                                    continue;
                                }
                                let next_received = received.saturating_add(chunk.len() as u64);
                                if next_received > expected_len {
                                    let _ = tx
                                        .send(Err(StoreError::Corruption(format!(
                                            "blob {blob_id_hex} oversized in S3 at offset {offset}: expected {expected_len} bytes, received at least {next_received} bytes"
                                        ))))
                                        .await;
                                    return;
                                }
                                let mut pos = 0usize;
                                while pos < chunk.len() {
                                    let end =
                                        (pos + cc_common::FILE_STREAM_CHUNK_SIZE).min(chunk.len());
                                    if tx.send(Ok(chunk.slice(pos..end))).await.is_err() {
                                        return;
                                    }
                                    pos = end;
                                }
                                received = next_received;
                            }
                            Ok(None) => break,
                            Err(err) => {
                                let _ = tx.send(Err(map_reqwest_err(err))).await;
                                return;
                            }
                        }
                    }
                    if received != expected_len {
                        let _ = tx
                            .send(Err(StoreError::Corruption(format!(
                                "blob {blob_id_hex} truncated in S3 at offset {offset}: expected {expected_len} bytes, got {received} bytes"
                            ))))
                            .await;
                    }
                });
                Ok(Some(rx))
            }
            StatusCode::OK => {
                let expected_total = record.content_size;
                if let Some(content_len) = parse_content_length(resp.headers())
                    && content_len != expected_total
                {
                    return Err(StoreError::Corruption(format!(
                        "blob {blob_id_hex} has {content_len} bytes in S3, expected {expected_total} bytes"
                    )));
                }

                let range_end = offset.saturating_add(expected_len);
                let (tx, rx) = mpsc::channel(8);
                tokio::spawn(async move {
                    let mut resp = resp;
                    let mut total_received = 0u64;
                    loop {
                        let next_chunk = tokio::select! {
                            biased;
                            _ = tx.closed() => return,
                            res = resp.chunk() => res,
                        };
                        match next_chunk {
                            Ok(Some(chunk)) => {
                                if chunk.is_empty() {
                                    continue;
                                }
                                let chunk_start = total_received;
                                let next_total = total_received.saturating_add(chunk.len() as u64);
                                if next_total > expected_total {
                                    let _ = tx
                                        .send(Err(StoreError::Corruption(format!(
                                            "blob {blob_id_hex} oversized in S3: expected {expected_total} bytes, received at least {next_total} bytes"
                                        ))))
                                        .await;
                                    return;
                                }
                                let emit_start = offset.max(chunk_start);
                                let emit_end = range_end.min(next_total);
                                if emit_start < emit_end {
                                    let rel_start = (emit_start - chunk_start) as usize;
                                    let rel_end = (emit_end - chunk_start) as usize;
                                    let slice = chunk.slice(rel_start..rel_end);
                                    let mut pos = 0usize;
                                    while pos < slice.len() {
                                        let end = (pos + cc_common::FILE_STREAM_CHUNK_SIZE)
                                            .min(slice.len());
                                        if tx.send(Ok(slice.slice(pos..end))).await.is_err() {
                                            return;
                                        }
                                        pos = end;
                                    }
                                }
                                total_received = next_total;
                            }
                            Ok(None) => break,
                            Err(err) => {
                                let _ = tx.send(Err(map_reqwest_err(err))).await;
                                return;
                            }
                        }
                    }
                    if total_received != expected_total {
                        let _ = tx
                            .send(Err(StoreError::Corruption(format!(
                                "blob {blob_id_hex} truncated in S3: expected {expected_total} bytes, got {total_received} bytes"
                            ))))
                            .await;
                    }
                });
                Ok(Some(rx))
            }
            _ => {
                let body = read_bounded_body(resp, MAX_ERROR_BODY_BYTES).await;
                Err(map_http_status_err(status, op_name, &body))
            }
        }
    }

    async fn has_blob(&self, blob_id: &[u8]) -> StoreResult<bool> {
        let key = Self::object_key(blob_id)?;
        let encoded_path = self.object_path(&key);
        let resp = self
            .send_request("HEAD object", Method::HEAD, &encoded_path, &[], None)
            .await?;
        let status = resp.status();
        let body = read_bounded_body(resp, MAX_ERROR_BODY_BYTES).await;
        if status.is_success() {
            Ok(true)
        } else if status == StatusCode::NOT_FOUND {
            Ok(false)
        } else {
            Err(map_http_status_err(status, "HEAD object", &body))
        }
    }
}

fn staged_file_body(async_file: tokio::fs::File, size: u64) -> Body {
    let stream =
        futures::stream::try_unfold((async_file, size), |(mut file, remaining)| async move {
            if remaining == 0 {
                return Ok(None);
            }
            let to_read = remaining.min(cc_common::FILE_STREAM_CHUNK_SIZE as u64) as usize;
            let mut buf = vec![0u8; to_read];
            let n = tokio::io::AsyncReadExt::read(&mut file, &mut buf).await?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!("staged blob truncated during S3 upload ({remaining} bytes remaining)"),
                ));
            }
            buf.truncate(n);
            Ok(Some((Bytes::from(buf), (file, remaining - n as u64))))
        });
    Body::wrap_stream(stream)
}

fn hash_and_rewind_staged_file(
    path: &Path,
    expected_size: u64,
) -> StoreResult<(std::fs::File, String)> {
    use std::io::{Read as _, Seek as _, SeekFrom};

    let mut file = std::fs::File::open(path).map_err(|e| {
        StoreError::Backend(format!(
            "failed to open staged blob {}: {e}",
            path.display()
        ))
    })?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; cc_common::FILE_STREAM_CHUNK_SIZE];
    let mut total = 0u64;
    loop {
        let n = file.read(&mut buf).map_err(|e| {
            StoreError::Backend(format!(
                "failed to read staged blob {}: {e}",
                path.display()
            ))
        })?;
        if n == 0 {
            break;
        }
        total = total.saturating_add(n as u64);
        if total > expected_size {
            return Err(StoreError::Backend(format!(
                "staged blob {} exceeded expected size {expected_size} bytes",
                path.display()
            )));
        }
        hasher.update(&buf[..n]);
    }
    if total != expected_size {
        return Err(StoreError::Backend(format!(
            "staged blob {} has {total} bytes, expected {expected_size}",
            path.display()
        )));
    }
    file.seek(SeekFrom::Start(0)).map_err(|e| {
        StoreError::Backend(format!(
            "failed to rewind staged blob {}: {e}",
            path.display()
        ))
    })?;
    let payload_hash = if total == 0 {
        EMPTY_PAYLOAD_SHA256.to_string()
    } else {
        hex::encode(hasher.finalize())
    };
    Ok((file, payload_hash))
}
