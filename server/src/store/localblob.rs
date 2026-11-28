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

//! Local content-addressed `BlobStore` backed by two-level sharded filesystem directories.
//!
//! # Authority and Crash-Safety Ordering
//! `put_blob` writes content to a unique temporary file inside the target shard directory
//! (`blobs/<aa>/<bb>/.tmp-<uuid>`) with blocking `std::fs` calls, checks the written length,
//! fsyncs the file, renames it onto `blobs/<aa>/<bb>/<full_hex>`, and fsyncs the shard
//! directory on unix. Readers see either no file or a complete blob, and failed writes
//! remove their temporary file.
//!
//! # Concurrency and Merge Invariants
//! Because keys are 20-byte Git blob SHA-1 digests, concurrent writers uploading the
//! same `blob_id` produce identical byte streams; an existing file with the expected length
//! short-circuits without rewriting disk blocks, a file with any other length is a damaged
//! earlier write and is replaced, and racing renames replace the target with identical
//! bytes.
//!
//! # Cross-Process and RPC Boundaries
//! `get_blob_range` seeks directly to `offset` and reads at most `length` bytes so partial
//! hydration does not read entire multi-gigabyte blobs into server RAM.

use std::io::{Read as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::mpsc;

use super::{
    BlobRecord, BlobStore, StoreError, StoreResult, expected_range_len, stream_inline_blob,
};

#[derive(Clone, Debug)]
pub struct LocalFsBlobStore {
    root: PathBuf,
    location_uri: String,
}

impl LocalFsBlobStore {
    pub fn open(root: impl AsRef<Path>) -> StoreResult<Self> {
        let root_path = root.as_ref().to_path_buf();
        if root_path.as_os_str().is_empty()
            || root_path.to_str().is_some_and(|s| s.trim().is_empty())
        {
            return Err(StoreError::InvalidArgument(
                "local blob store root path must not be empty".to_string(),
            ));
        }
        create_dir_all_private(&root_path.join("blobs")).map_err(|e| {
            StoreError::Backend(format!(
                "failed to create local blob root {}: {e}",
                root_path.display()
            ))
        })?;
        let canonical = std::fs::canonicalize(&root_path).unwrap_or_else(|_| root_path.clone());
        let location_uri = format!("file://{}", canonical.display());
        Ok(Self {
            root: root_path,
            location_uri,
        })
    }

    pub fn open_with_location_id(
        root: impl AsRef<Path>,
        location_uri: impl Into<String>,
    ) -> StoreResult<Self> {
        let location_uri = location_uri.into();
        if location_uri.trim().is_empty() {
            return Err(StoreError::InvalidArgument(
                "local blob store location_id must not be empty".to_string(),
            ));
        }
        let mut store = Self::open(root)?;
        store.location_uri = location_uri;
        Ok(store)
    }

    fn object_path(&self, blob_id: &[u8]) -> StoreResult<PathBuf> {
        if blob_id.is_empty() || blob_id.len() > 64 {
            return Err(StoreError::InvalidArgument(
                "blob_id must be between 1 and 64 bytes".to_string(),
            ));
        }
        let hex_id = hex::encode(blob_id);
        let prefix_a = hex_id.get(0..2).unwrap_or("00");
        let prefix_b = hex_id.get(2..4).unwrap_or("00");
        Ok(self
            .root
            .join("blobs")
            .join(prefix_a)
            .join(prefix_b)
            .join(hex_id))
    }
}

/// Stores `data` at `target` unless a file with the same length is already there. The blob id
/// is a hash over the length and the content, so a length mismatch means a damaged file.
fn write_blob_file(
    target: &Path,
    data: &[u8],
    deadline: Option<std::time::Instant>,
) -> StoreResult<()> {
    if deadline.is_some_and(|d| d <= std::time::Instant::now()) {
        return Err(StoreError::DeadlineExceeded(
            "gRPC deadline expired before writing local blob".to_string(),
        ));
    }
    match std::fs::metadata(target) {
        Ok(meta) if meta.is_file() && meta.len() == data.len() as u64 => return Ok(()),
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(StoreError::Backend(format!("blob stat failed: {e}"))),
    }

    let shard_dir = target.parent().expect("blob path must have shard parent");
    create_dir_all_private(shard_dir)
        .map_err(|e| StoreError::Backend(format!("failed to create blob shard dir: {e}")))?;

    let tmp_path = shard_dir.join(format!(".tmp-{}", uuid::Uuid::new_v4()));
    let result = write_temp_and_rename(&tmp_path, target, data);
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp_path);
    }
    result
}

fn write_temp_and_rename(tmp_path: &Path, target: &Path, data: &[u8]) -> StoreResult<()> {
    let mut tmp_file = create_new_private_file(tmp_path)
        .map_err(|e| StoreError::Backend(format!("failed to create temp blob file: {e}")))?;
    tmp_file
        .write_all(data)
        .and_then(|()| tmp_file.flush())
        .map_err(|e| StoreError::Backend(format!("failed to write temp blob file: {e}")))?;
    let written = tmp_file
        .metadata()
        .map_err(|e| StoreError::Backend(format!("failed to stat temp blob file: {e}")))?
        .len();
    if written != data.len() as u64 {
        return Err(StoreError::Backend(format!(
            "temp blob file has {written} bytes, expected {}",
            data.len()
        )));
    }
    tmp_file
        .sync_all()
        .map_err(|e| StoreError::Backend(format!("failed to sync temp blob file: {e}")))?;
    drop(tmp_file);

    std::fs::rename(tmp_path, target)
        .map_err(|e| StoreError::Backend(format!("failed to rename temp blob file: {e}")))?;
    sync_dir(target.parent().expect("blob path must have shard parent"))
        .map_err(|e| StoreError::Backend(format!("failed to sync blob shard dir: {e}")))
}

/// Streams `src_path` into a private temp file inside `target`'s shard directory using a
/// bounded 64 KiB buffer, fsyncs, and atomically renames onto `target`. Works identically
/// across filesystems without allocating the whole blob on the heap.
fn copy_file_to_blob_target(
    src_path: &Path,
    target: &Path,
    expected_size: u64,
    deadline: Option<std::time::Instant>,
) -> StoreResult<()> {
    if deadline.is_some_and(|d| d <= std::time::Instant::now()) {
        return Err(StoreError::DeadlineExceeded(
            "gRPC deadline expired before copying staged local blob".to_string(),
        ));
    }
    match std::fs::metadata(target) {
        Ok(meta) if meta.is_file() && meta.len() == expected_size => return Ok(()),
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(StoreError::Backend(format!("blob stat failed: {e}"))),
    }

    let shard_dir = target.parent().expect("blob path must have shard parent");
    create_dir_all_private(shard_dir)
        .map_err(|e| StoreError::Backend(format!("failed to create blob shard dir: {e}")))?;

    let tmp_path = shard_dir.join(format!(".tmp-{}", uuid::Uuid::new_v4()));
    let result = copy_temp_and_rename(src_path, &tmp_path, target, expected_size, deadline);
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp_path);
    }
    result
}

fn copy_temp_and_rename(
    src_path: &Path,
    tmp_path: &Path,
    target: &Path,
    expected_size: u64,
    deadline: Option<std::time::Instant>,
) -> StoreResult<()> {
    let mut src = std::fs::File::open(src_path).map_err(|e| {
        StoreError::Backend(format!(
            "failed to open staged blob {}: {e}",
            src_path.display()
        ))
    })?;
    let mut tmp_file = create_new_private_file(tmp_path)
        .map_err(|e| StoreError::Backend(format!("failed to create temp blob file: {e}")))?;

    let mut buf = vec![0u8; cc_common::FILE_STREAM_CHUNK_SIZE];
    let mut copied = 0u64;
    loop {
        if deadline.is_some_and(|d| d <= std::time::Instant::now()) {
            return Err(StoreError::DeadlineExceeded(
                "gRPC deadline exceeded while copying staged local blob".to_string(),
            ));
        }
        let n = src
            .read(&mut buf)
            .map_err(|e| StoreError::Backend(format!("failed to read staged blob: {e}")))?;
        if n == 0 {
            break;
        }
        copied = copied.saturating_add(n as u64);
        if copied > expected_size {
            return Err(StoreError::Backend(format!(
                "staged blob exceeded expected size {expected_size} bytes"
            )));
        }
        tmp_file
            .write_all(&buf[..n])
            .map_err(|e| StoreError::Backend(format!("failed to write temp blob file: {e}")))?;
    }
    if copied != expected_size {
        return Err(StoreError::Backend(format!(
            "staged blob has {copied} bytes, expected {expected_size}"
        )));
    }
    tmp_file
        .flush()
        .and_then(|()| tmp_file.sync_all())
        .map_err(|e| StoreError::Backend(format!("failed to sync temp blob file: {e}")))?;
    drop(tmp_file);

    std::fs::rename(tmp_path, target)
        .map_err(|e| StoreError::Backend(format!("failed to rename temp blob file: {e}")))?;
    sync_dir(target.parent().expect("blob path must have shard parent"))
        .map_err(|e| StoreError::Backend(format!("failed to sync blob shard dir: {e}")))
}

/// Makes a completed rename durable across power loss.
#[cfg(unix)]
fn sync_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}

#[cfg(not(unix))]
fn sync_dir(_dir: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn create_dir_all_private(dir: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt as _;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
}

#[cfg(not(unix))]
fn create_dir_all_private(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)
}

#[cfg(unix)]
fn create_new_private_file(path: &Path) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt as _;
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
}

#[cfg(not(unix))]
fn create_new_private_file(path: &Path) -> std::io::Result<std::fs::File> {
    std::fs::File::create_new(path)
}

#[async_trait]
impl BlobStore for LocalFsBlobStore {
    fn location_id(&self) -> &str {
        &self.location_uri
    }

    async fn put_blob(&self, blob_id: &[u8], data: Bytes) -> StoreResult<()> {
        let target = self.object_path(blob_id)?;
        let deadline = crate::resilience::current_rpc_deadline();
        let metrics = crate::resilience::current_rpc_metrics();
        crate::resilience::run_with_rpc_deadline("local blob write", async move {
            // tokio::fs::File reports some write errors only on flush, so write with blocking
            // std::fs calls, where every error comes back from the call that hit it.
            match tokio::task::spawn_blocking(move || write_blob_file(&target, &data, deadline))
                .await
            {
                Ok(res) => res,
                Err(join_err) => {
                    crate::resilience::handle_join_error(
                        "local blob write task",
                        join_err,
                        metrics.as_ref(),
                    );
                    Err(StoreError::Backend(
                        "local blob write task failed".to_string(),
                    ))
                }
            }
        })
        .await
    }

    async fn put_blob_from_file(
        &self,
        blob_id: &[u8],
        temp_path: &Path,
        size: u64,
    ) -> StoreResult<()> {
        let target = self.object_path(blob_id)?;
        let src = temp_path.to_path_buf();
        let deadline = crate::resilience::current_rpc_deadline();
        let metrics = crate::resilience::current_rpc_metrics();
        crate::resilience::run_with_rpc_deadline("local blob file copy", async move {
            match tokio::task::spawn_blocking(move || {
                copy_file_to_blob_target(&src, &target, size, deadline)
            })
            .await
            {
                Ok(res) => res,
                Err(join_err) => {
                    crate::resilience::handle_join_error(
                        "local blob file copy task",
                        join_err,
                        metrics.as_ref(),
                    );
                    Err(StoreError::Backend(
                        "local blob file copy task failed".to_string(),
                    ))
                }
            }
        })
        .await
    }

    async fn get_blob(&self, blob_id: &[u8]) -> StoreResult<Option<Bytes>> {
        let target = self.object_path(blob_id)?;
        crate::resilience::run_with_rpc_deadline("local blob read", async move {
            match tokio::fs::read(&target).await {
                Ok(buf) => Ok(Some(Bytes::from(buf))),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(StoreError::Backend(format!(
                    "failed to read local blob {}: {e}",
                    target.display()
                ))),
            }
        })
        .await
    }

    async fn get_blob_range(
        &self,
        blob_id: &[u8],
        offset: u64,
        length: Option<u64>,
    ) -> StoreResult<Option<Bytes>> {
        let target = self.object_path(blob_id)?;
        crate::resilience::run_with_rpc_deadline("local blob range read", async move {
            let mut file = match tokio::fs::File::open(&target).await {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(e) => {
                    return Err(StoreError::Backend(format!(
                        "failed to open local blob {}: {e}",
                        target.display()
                    )));
                }
            };

            let total_len = file
                .metadata()
                .await
                .map_err(|e| StoreError::Backend(format!("failed to stat local blob: {e}")))?
                .len();

            if offset > total_len {
                return Err(StoreError::OutOfRange(format!(
                    "requested read at offset {offset}, file size is {total_len}"
                )));
            }
            if offset == total_len {
                return Ok(Some(Bytes::new()));
            }

            file.seek(SeekFrom::Start(offset))
                .await
                .map_err(|e| StoreError::Backend(format!("failed to seek local blob: {e}")))?;

            let to_read = match length {
                Some(0) | None => total_len - offset,
                Some(max_len) => max_len.min(total_len - offset),
            };

            let cap = usize::try_from(to_read).map_err(|_| {
                StoreError::OutOfRange(format!(
                    "requested local blob range ({to_read} bytes) exceeds addressable memory"
                ))
            })?;
            let mut buf = Vec::with_capacity(cap);
            file.take(to_read)
                .read_to_end(&mut buf)
                .await
                .map_err(|e| {
                    StoreError::Backend(format!("failed to read local blob range: {e}"))
                })?;
            Ok(Some(Bytes::from(buf)))
        })
        .await
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
        let target = self.object_path(&record.blob_id)?;
        let blob_id = record.blob_id.clone();
        let content_size = record.content_size;
        crate::resilience::run_with_rpc_deadline("local blob range read", async move {
            let mut file = match tokio::fs::File::open(&target).await {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(e) => {
                    return Err(StoreError::Backend(format!(
                        "failed to open local blob {}: {e}",
                        target.display()
                    )));
                }
            };
            let total_len = file
                .metadata()
                .await
                .map_err(|e| StoreError::Backend(format!("failed to stat local blob: {e}")))?
                .len();
            if total_len != content_size {
                return Err(StoreError::Corruption(format!(
                    "local blob {} has {total_len} bytes on disk, expected {content_size} bytes",
                    hex::encode(&blob_id),
                )));
            }
            let to_read = expected_range_len(content_size, offset, length);
            if to_read == 0 {
                return Ok(Some(Bytes::new()));
            }
            file.seek(SeekFrom::Start(offset))
                .await
                .map_err(|e| StoreError::Backend(format!("failed to seek local blob: {e}")))?;
            let cap = usize::try_from(to_read).map_err(|_| {
                StoreError::OutOfRange(format!(
                    "requested local blob range ({to_read} bytes) exceeds addressable memory"
                ))
            })?;
            let mut buf = Vec::with_capacity(cap);
            file.take(to_read)
                .read_to_end(&mut buf)
                .await
                .map_err(|e| {
                    StoreError::Backend(format!("failed to read local blob range: {e}"))
                })?;
            if buf.len() as u64 != to_read {
                return Err(StoreError::Corruption(format!(
                    "local blob {} truncated while reading: expected {to_read} bytes, got {}",
                    hex::encode(&blob_id),
                    buf.len()
                )));
            }
            Ok(Some(Bytes::from(buf)))
        })
        .await
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

        let target = self.object_path(&record.blob_id)?;
        let expected_len = expected_range_len(record.content_size, offset, length);
        let blob_id = record.blob_id.clone();
        let content_size = record.content_size;

        let maybe_file = crate::resilience::run_with_rpc_deadline(
            "local blob stream open",
            async move {
                let mut file = match tokio::fs::File::open(&target).await {
                    Ok(f) => f,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                    Err(e) => {
                        return Err(StoreError::Backend(format!(
                            "failed to open local blob {}: {e}",
                            target.display()
                        )));
                    }
                };
                let total_len = file
                    .metadata()
                    .await
                    .map_err(|e| StoreError::Backend(format!("failed to stat local blob: {e}")))?
                    .len();
                if total_len != content_size {
                    return Err(StoreError::Corruption(format!(
                        "local blob {} has {total_len} bytes on disk, expected {content_size} bytes",
                        hex::encode(&blob_id),
                    )));
                }
                if expected_len > 0 {
                    file.seek(SeekFrom::Start(offset))
                        .await
                        .map_err(|e| {
                            StoreError::Backend(format!("failed to seek local blob: {e}"))
                        })?;
                }
                Ok(Some(file))
            },
        )
        .await?;
        let Some(mut file) = maybe_file else {
            return Ok(None);
        };

        let (tx, rx) = mpsc::channel(8);
        if expected_len == 0 {
            return Ok(Some(rx));
        }

        let blob_id_hex = hex::encode(&record.blob_id);
        crate::resilience::spawn_blob_stream_producer(
            "local blob stream",
            tx,
            move |tx| async move {
                let mut remaining = expected_len;
                while remaining > 0 {
                    let to_read = remaining.min(cc_common::FILE_STREAM_CHUNK_SIZE as u64) as usize;
                    let mut buf = vec![0u8; to_read];
                    match file.read(&mut buf).await {
                        Ok(0) => {
                            let _ = tx
                                .send(Err(StoreError::Corruption(format!(
                                    "local blob {blob_id_hex} truncated mid-stream ({remaining} bytes remaining)"
                                ))))
                                .await;
                            return;
                        }
                        Ok(n) => {
                            buf.truncate(n);
                            remaining -= n as u64;
                            if tx.send(Ok(Bytes::from(buf))).await.is_err() {
                                return;
                            }
                        }
                        Err(e) => {
                            let _ = tx
                                .send(Err(StoreError::Backend(format!(
                                    "failed to read local blob {blob_id_hex}: {e}"
                                ))))
                                .await;
                            return;
                        }
                    }
                }
            },
        );

        Ok(Some(rx))
    }

    async fn has_blob(&self, blob_id: &[u8]) -> StoreResult<bool> {
        let target = self.object_path(blob_id)?;
        crate::resilience::run_with_rpc_deadline("local blob stat", async move {
            match tokio::fs::metadata(&target).await {
                Ok(meta) => Ok(meta.is_file()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
                Err(e) => Err(StoreError::Backend(format!(
                    "failed to stat local blob: {e}"
                ))),
            }
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard_dir_entries(store: &LocalFsBlobStore, blob_id: &[u8]) -> Vec<String> {
        let shard_dir = store
            .object_path(blob_id)
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();
        let mut names: Vec<String> = std::fs::read_dir(shard_dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    #[tokio::test]
    async fn put_blob_replaces_existing_file_with_wrong_length() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsBlobStore::open(dir.path()).unwrap();
        let blob_id = [0x5e; 20];
        let payload = Bytes::from_static(b"complete blob payload");

        // Zero-length or truncated files from interrupted writes must be replaced on retry.
        let target = store.object_path(&blob_id).unwrap();
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        for damaged in [&[][..], &payload[..5]] {
            std::fs::write(&target, damaged).unwrap();
            store.put_blob(&blob_id, payload.clone()).await.unwrap();
            assert_eq!(
                store.get_blob(&blob_id).await.unwrap(),
                Some(payload.clone())
            );
            assert_eq!(
                shard_dir_entries(&store, &blob_id),
                vec![hex::encode(blob_id)]
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn put_blob_keeps_existing_file_with_expected_length() {
        use std::os::unix::fs::MetadataExt as _;

        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsBlobStore::open(dir.path()).unwrap();
        let blob_id = [0x6f; 20];
        let payload = Bytes::from_static(b"stored once");

        store.put_blob(&blob_id, payload.clone()).await.unwrap();
        let inode = std::fs::metadata(store.object_path(&blob_id).unwrap())
            .unwrap()
            .ino();
        store.put_blob(&blob_id, payload).await.unwrap();
        assert_eq!(
            std::fs::metadata(store.object_path(&blob_id).unwrap())
                .unwrap()
                .ino(),
            inode
        );
    }

    #[tokio::test]
    async fn put_blob_removes_temp_file_when_write_fails() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsBlobStore::open(dir.path()).unwrap();
        let blob_id = [0x7a; 20];

        // A directory at the target path makes the final rename fail.
        let target = store.object_path(&blob_id).unwrap();
        std::fs::create_dir_all(target.join("occupied")).unwrap();

        let err = store
            .put_blob(&blob_id, Bytes::from_static(b"payload"))
            .await
            .unwrap_err();
        assert!(matches!(err, StoreError::Backend(_)), "{err:?}");
        assert_eq!(
            shard_dir_entries(&store, &blob_id),
            vec![hex::encode(blob_id)]
        );
    }

    #[test]
    fn open_rejects_empty_root_or_location_id() {
        assert!(matches!(
            LocalFsBlobStore::open(""),
            Err(StoreError::InvalidArgument(_))
        ));
        assert!(matches!(
            LocalFsBlobStore::open("   "),
            Err(StoreError::InvalidArgument(_))
        ));
        let dir = tempfile::tempdir().unwrap();
        assert!(matches!(
            LocalFsBlobStore::open_with_location_id(dir.path(), "   "),
            Err(StoreError::InvalidArgument(_))
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn created_dirs_and_blobs_use_private_permissions() {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::tempdir().unwrap();
        let store_root = dir.path().join("private-blobs");
        let store = LocalFsBlobStore::open(&store_root).unwrap();
        let blob_id = [0xab; 20];
        store
            .put_blob(&blob_id, Bytes::from_static(b"secret blob"))
            .await
            .unwrap();

        let blob_path = store.object_path(&blob_id).unwrap();
        let shard_b = blob_path.parent().unwrap();
        let shard_a = shard_b.parent().unwrap();
        let blobs_dir = shard_a.parent().unwrap();

        for d in [store_root.as_path(), blobs_dir, shard_a, shard_b] {
            let mode = std::fs::metadata(d).unwrap().permissions().mode() & 0o777;
            assert_eq!(
                mode,
                0o700,
                "dir {} expected 0o700, got {mode:#o}",
                d.display()
            );
        }
        let file_mode = std::fs::metadata(&blob_path).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            file_mode,
            0o600,
            "blob {} expected 0o600, got {file_mode:#o}",
            blob_path.display()
        );
    }

    #[tokio::test]
    async fn put_blob_from_file_and_get_blob_range_stream_roundtrip_and_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFsBlobStore::open(dir.path()).unwrap());
        let blob_id = vec![0xbc; 20];

        // Stage a multi-chunk payload (~160 KiB) in a temp file and persist without buffering.
        let payload: Vec<u8> = (0..(160 * 1024 + 17)).map(|i| (i % 251) as u8).collect();
        let mut staged = tempfile::NamedTempFile::new().unwrap();
        staged.write_all(&payload).unwrap();
        staged.flush().unwrap();

        store
            .put_blob_from_file(&blob_id, staged.path(), payload.len() as u64)
            .await
            .unwrap();

        let record = BlobRecord {
            blob_id: blob_id.clone(),
            content_size: payload.len() as u64,
            inline_content: None,
            primary_location: Some(store.location_id().to_string()),
            replicated_locations: vec![store.location_id().to_string()],
        };

        // Full stream emits multiple 64 KiB chunks matching exact payload bytes.
        let mut rx = store
            .clone()
            .get_blob_range_stream(record.clone(), 0, None)
            .await
            .unwrap()
            .expect("blob must exist");
        let mut received = Vec::new();
        let mut chunk_count = 0usize;
        while let Some(chunk) = rx.recv().await {
            let chunk = chunk.unwrap();
            assert!(chunk.len() <= cc_common::FILE_STREAM_CHUNK_SIZE);
            received.extend_from_slice(&chunk);
            chunk_count += 1;
        }
        assert!(chunk_count >= 3);
        assert_eq!(received, payload);

        // Sub-range stream matches exact slice.
        let mut sub_rx = store
            .clone()
            .get_blob_range_stream(record.clone(), 1000, Some(70_000))
            .await
            .unwrap()
            .unwrap();
        let mut sub = Vec::new();
        while let Some(chunk) = sub_rx.recv().await {
            sub.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(sub, payload[1000..71_000]);

        // Truncated on-disk file surfaces StoreError::Corruption immediately.
        let target = store.object_path(&blob_id).unwrap();
        std::fs::write(&target, &payload[..128]).unwrap();
        let trunc_err = store
            .clone()
            .get_blob_range_stream(record, 0, None)
            .await
            .unwrap_err();
        assert!(
            matches!(trunc_err, StoreError::Corruption(_)),
            "{trunc_err:?}"
        );
    }

    #[tokio::test]
    async fn local_blob_ops_fail_fast_when_rpc_deadline_expired() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFsBlobStore::open(dir.path()).unwrap());
        let blob_id = vec![0xcd; 20];
        let payload = Bytes::from_static(b"hello local blob");
        store.put_blob(&blob_id, payload.clone()).await.unwrap();

        let expired = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_millis(10))
            .unwrap_or_else(std::time::Instant::now);

        assert!(matches!(
            crate::resilience::with_rpc_deadline(Some(expired), store.put_blob(&blob_id, payload),)
                .await,
            Err(StoreError::DeadlineExceeded(_))
        ));
        assert!(matches!(
            crate::resilience::with_rpc_deadline(Some(expired), store.get_blob(&blob_id)).await,
            Err(StoreError::DeadlineExceeded(_))
        ));
        assert!(matches!(
            crate::resilience::with_rpc_deadline(Some(expired), store.has_blob(&blob_id)).await,
            Err(StoreError::DeadlineExceeded(_))
        ));
    }
}
