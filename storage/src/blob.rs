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

//! Blob store contract (DEV-193).
//!
//! A blob is the bytes of one file today, and may be one chunk of a file
//! later (DEV-80). The store does not know which.
//!
//! Everything about physical layout (object names, buckets, prefixes,
//! directories, object metadata) stays inside each implementation. DEV-189 and
//! DEV-190 decide the GCS layout without changing this trait.

use crate::error::StoreResult;
use crate::ids::{Digest, ProjectId};
use async_trait::async_trait;
use bytes::Bytes;

/// Logical name of a blob: the project that owns it and the digest of its
/// bytes. Two projects storing the same bytes hold two separate blobs, so
/// projects stay separated whatever the layout.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BlobKey {
    /// The project that owns the blob.
    pub project: ProjectId,
    /// Digest of the blob's bytes.
    pub digest: Digest,
}

/// A byte range of a blob. `len: None` means "to the end".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    /// First byte to read.
    pub offset: u64,
    /// Bytes to read from `offset`; `None` reads to the end.
    pub len: Option<u64>,
}

impl ByteRange {
    /// The whole blob.
    pub const ALL: ByteRange = ByteRange {
        offset: 0,
        len: None,
    };
}

/// Result of [`BlobStore::read_range`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobRead {
    /// The requested bytes. Shorter than requested only when the range runs
    /// past the end of the blob.
    pub data: Bytes,
    /// Full size of the blob, so callers can page without a separate stat.
    pub total_len: u64,
}

/// Size of a stored blob.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobInfo {
    /// Size in bytes.
    pub len: u64,
}

/// Whether [`BlobStore::put`] wrote anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutOutcome {
    /// This call stored the blob.
    Created,
    /// The blob was already stored with the same bytes; nothing changed.
    AlreadyPresent,
}

/// Limits a client must respect. Reported through `GetServerInfo`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobLimits {
    /// Largest blob `put` accepts, in bytes.
    pub max_blob_bytes: u64,
    /// Largest range one `read_range` returns, in bytes. Callers page above it.
    pub max_read_bytes: u64,
    /// Most keys in one `stat` or `find_missing` call.
    pub max_batch: usize,
}

/// File contents, addressed by digest.
///
/// Must fit Google Cloud Storage, S3 and a local file system equally well: no
/// method relies on a feature only one of them has. All methods are safe to
/// call concurrently; all writes are idempotent.
#[async_trait]
pub trait BlobStore: Send + Sync {
    /// Limits of this implementation. Constant for the life of the store.
    fn limits(&self) -> BlobLimits;

    /// Store `data` under `key`.
    ///
    /// - **Verifies** that `data` hashes to `key.digest` with
    ///   `key.digest.algorithm` before making it visible; returns
    ///   `DigestMismatch` and stores nothing otherwise.
    /// - **All or nothing:** after any outcome, including a crash, a reader
    ///   sees either no blob or the complete blob. Never a partial one.
    ///   (Local disk: write a temporary file, fsync, rename. GCS and S3: a
    ///   single-request upload, which is atomic.)
    /// - **Durable:** when `Ok` returns, the blob survives a process or machine
    ///   crash.
    /// - **Idempotent:** if the blob is already present, returns
    ///   `AlreadyPresent` without rewriting it. Two concurrent puts of the same
    ///   key both succeed.
    ///
    /// A crash after `put` and before the matching metadata write leaves an
    /// unreferenced blob; that is expected and cleaned up by `delete`.
    ///
    /// Errors: `DigestMismatch`; `LimitExceeded` (above `max_blob_bytes`);
    /// `InvalidArgument` (digest of the wrong length for its algorithm);
    /// `Unsupported` (an algorithm this store cannot verify); `Unavailable`.
    async fn put(&self, key: &BlobKey, data: Bytes) -> StoreResult<PutOutcome>;

    /// Read a byte range of a blob.
    ///
    /// A range starting at or past the end returns empty `data` (not an
    /// error). Implementations may verify bytes on read and return `Corrupt`.
    /// Read only, so a retry is always safe and a crash leaves nothing
    /// behind; since `put` is all or nothing, a read never sees part of a
    /// blob.
    ///
    /// Errors: `NotFound`; `LimitExceeded` (the bytes to return, after
    /// clamping the range to the blob, exceed `max_read_bytes`);
    /// `InvalidArgument` (digest of the wrong length); `Corrupt`; `Unavailable`.
    async fn read_range(&self, key: &BlobKey, range: ByteRange) -> StoreResult<BlobRead>;

    /// Size of each blob, one entry per input in input order; `None` where the
    /// blob is not stored. Read only, so a retry is always safe.
    ///
    /// Errors: `LimitExceeded` (above `max_batch`); `InvalidArgument` (digest
    /// of the wrong length); `Unavailable`.
    async fn stat(&self, keys: &[BlobKey]) -> StoreResult<Vec<Option<BlobInfo>>>;

    /// The keys from `keys` that are not stored, in input order. Read only,
    /// so a retry is always safe.
    ///
    /// Default: derived from [`BlobStore::stat`]. Implementations override it
    /// when the backend has a cheaper presence check.
    ///
    /// Errors: as [`BlobStore::stat`].
    async fn find_missing(&self, keys: &[BlobKey]) -> StoreResult<Vec<BlobKey>> {
        let infos = self.stat(keys).await?;
        Ok(keys
            .iter()
            .zip(infos)
            .filter(|(_, info)| info.is_none())
            .map(|(k, _)| k.clone())
            .collect())
    }

    /// Delete a blob. Optional; for cleanup of blobs nothing references.
    ///
    /// Callers must only delete blobs that no stored metadata references; the
    /// store does not check. Idempotent: deleting a missing blob succeeds.
    /// Not safe to run concurrently with writers: a `put` that just returned
    /// `AlreadyPresent` relies on the blob staying. A crash leaves the blob
    /// either present or gone. v0 never deletes; cleanup is a later, offline
    /// job.
    ///
    /// Default: returns `Unsupported`. Stores that never delete (permanent
    /// retention) keep the default.
    ///
    /// Errors: `Unsupported`; `InvalidArgument` (digest of the wrong length);
    /// `Unavailable`.
    async fn delete(&self, key: &BlobKey) -> StoreResult<()> {
        let _ = key;
        Err(crate::StoreError::new(
            crate::ErrorKind::Unsupported,
            "this blob store does not delete",
        ))
    }
}
