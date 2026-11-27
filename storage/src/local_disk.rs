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

//! Local-disk [`BlobStore`] (DEV-194).
//!
//! Layout (private to this implementation; nothing outside depends on it):
//!
//! ```text
//! <root>/LOCK                      held by the process that has the store open
//! <root>/blobs/<project dir>/<algorithm>/<first 2 hex of digest>/<digest hex>
//! <root>/tmp/<unique name>          in-flight writes, same file system
//! ```
//!
//! `<project dir>` is the project id hex-encoded (so any id is a safe
//! directory name on any file system, including case-insensitive ones), or
//! `h-<sha256 of the id>` when the hex form would be longer than 200
//! characters (file names are limited to 255 bytes).
//!
//! **No partial blob is ever visible.** `put` writes to `tmp/`, fsyncs the
//! file, renames it into place (atomic on POSIX within one file system) and
//! fsyncs the target directory. A crash before the rename leaves only a file
//! in `tmp/`, which [`LocalDiskBlobStore::open`] removes. For that reason one
//! root belongs to one process at a time, enforced by an exclusive lock on
//! `<root>/LOCK`; a second `open` fails with `Unavailable`.

use crate::blob::{BlobInfo, BlobKey, BlobLimits, BlobRead, BlobStore, ByteRange, PutOutcome};
use crate::error::{ErrorKind, StoreError, StoreResult};
use async_trait::async_trait;
use bytes::Bytes;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Blob store on the local file system.
pub struct LocalDiskBlobStore {
    inner: Arc<Inner>,
}

struct Inner {
    root: PathBuf,
    limits: BlobLimits,
    counter: AtomicU64,
    /// Held for the life of the store; the OS releases the lock on drop or
    /// process exit.
    _lock: File,
}

impl LocalDiskBlobStore {
    /// Open (or create) a store at `root` with default limits.
    pub fn open(root: impl Into<PathBuf>) -> StoreResult<Self> {
        Self::open_with_limits(
            root,
            BlobLimits {
                max_blob_bytes: 256 * 1024 * 1024,
                max_read_bytes: 4 * 1024 * 1024,
                max_batch: 1_000,
            },
        )
    }

    /// Open (or create) a store at `root`. Removes leftovers of writes that
    /// were interrupted by a crash.
    pub fn open_with_limits(root: impl Into<PathBuf>, limits: BlobLimits) -> StoreResult<Self> {
        let root = root.into();
        fs::create_dir_all(root.join("blobs")).map_err(io_err)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(root.join("LOCK"))
            .map_err(io_err)?;
        if lock.try_lock().is_err() {
            return Err(StoreError::new(
                ErrorKind::Unavailable,
                format!("{} is in use by another process", root.display()),
            ));
        }
        let tmp = root.join("tmp");
        fs::create_dir_all(&tmp).map_err(io_err)?;
        for entry in fs::read_dir(&tmp).map_err(io_err)? {
            let path = entry.map_err(io_err)?.path();
            let _ = fs::remove_file(path);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                root,
                limits,
                counter: AtomicU64::new(0),
                _lock: lock,
            }),
        })
    }

    /// Where `key` lives on disk. Exposed for tests and debugging only.
    pub fn path_of(&self, key: &BlobKey) -> PathBuf {
        self.inner.path_of(key)
    }
}

impl Inner {
    fn path_of(&self, key: &BlobKey) -> PathBuf {
        let mut project: String = key.project.0.bytes().map(|b| format!("{b:02x}")).collect();
        if project.len() > 200 {
            let d = crate::digest::compute(crate::HashAlgorithm::Sha256, key.project.0.as_bytes());
            project = format!("h-{}", d.to_hex());
        }
        let hex = key.digest.to_hex();
        let fan = hex.get(..2).unwrap_or("00");
        self.root
            .join("blobs")
            .join(project)
            .join(key.digest.algorithm.name())
            .join(fan)
            .join(hex)
    }

    fn tmp_path(&self) -> PathBuf {
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        self.root
            .join("tmp")
            .join(format!("{}-{nanos}-{n}.part", std::process::id()))
    }

    fn put(&self, key: &BlobKey, data: &[u8]) -> StoreResult<PutOutcome> {
        // A digest of the wrong length must never map onto a directory.
        key.digest.check_len()?;
        if data.len() as u64 > self.limits.max_blob_bytes {
            return Err(StoreError::new(ErrorKind::LimitExceeded, "blob too large"));
        }
        if !crate::digest::verify(&key.digest, data) {
            return Err(StoreError::new(
                ErrorKind::DigestMismatch,
                "bytes do not match digest",
            ));
        }
        let target = self.path_of(key);
        let dir = target
            .parent()
            .ok_or_else(|| StoreError::new(ErrorKind::Internal, "blob path has no parent"))?;
        if target.exists() {
            // A concurrent put may have renamed the file into place without
            // syncing the directory yet, or failed while syncing it. Sync it
            // here, so `AlreadyPresent` is as durable as `Created`.
            sync_dir(dir).map_err(io_err)?;
            return Ok(PutOutcome::AlreadyPresent);
        }
        create_dirs_durably(&self.root.join("blobs"), dir).map_err(io_err)?;

        let tmp = self.tmp_path();
        let result = (|| -> io::Result<()> {
            let mut f = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
            f.write_all(data)?;
            f.sync_all()?;
            drop(f);
            // A concurrent put of the same key may win the rename; both hold
            // identical bytes, so either result is correct.
            fs::rename(&tmp, &target)?;
            sync_dir(dir)
        })();
        if let Err(e) = result {
            let _ = fs::remove_file(&tmp);
            return Err(io_err(e));
        }
        Ok(PutOutcome::Created)
    }

    fn read_range(&self, key: &BlobKey, range: ByteRange) -> StoreResult<BlobRead> {
        key.digest.check_len()?;
        let mut f = File::open(self.path_of(key)).map_err(io_err)?;
        let total = f.metadata().map_err(io_err)?.len();
        let start = range.offset.min(total);
        let wanted = range.len.unwrap_or(total - start);
        let end = start.saturating_add(wanted).min(total);
        let len = end - start;
        if len > self.limits.max_read_bytes {
            return Err(StoreError::new(
                ErrorKind::LimitExceeded,
                "range larger than max_read_bytes",
            ));
        }
        f.seek(SeekFrom::Start(start)).map_err(io_err)?;
        let mut buf = vec![0u8; len as usize];
        f.read_exact(&mut buf).map_err(io_err)?;
        Ok(BlobRead {
            data: Bytes::from(buf),
            total_len: total,
        })
    }

    fn stat_one(&self, key: &BlobKey) -> StoreResult<Option<BlobInfo>> {
        key.digest.check_len()?;
        match fs::metadata(self.path_of(key)) {
            Ok(m) if m.is_file() => Ok(Some(BlobInfo { len: m.len() })),
            Ok(_) => Ok(None),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(io_err(e)),
        }
    }

    fn delete(&self, key: &BlobKey) -> StoreResult<()> {
        key.digest.check_len()?;
        match fs::remove_file(self.path_of(key)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io_err(e)),
        }
    }
}

/// Create `dir` and any missing parents below `base`, fsyncing each parent
/// whose entries changed, so the new directories survive power loss.
fn create_dirs_durably(base: &Path, dir: &Path) -> io::Result<()> {
    if dir.is_dir() {
        return Ok(());
    }
    let mut missing = Vec::new();
    let mut d = dir;
    while !d.is_dir() && d != base {
        missing.push(d.to_path_buf());
        d = match d.parent() {
            Some(p) => p,
            None => break,
        };
    }
    fs::create_dir_all(dir)?;
    for m in missing.iter().rev() {
        if let Some(parent) = m.parent() {
            sync_dir(parent)?;
        }
    }
    Ok(())
}

#[cfg(unix)]
fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

#[cfg(not(unix))]
fn sync_dir(_dir: &Path) -> io::Result<()> {
    Ok(())
}

fn io_err(e: io::Error) -> StoreError {
    let kind = match e.kind() {
        io::ErrorKind::NotFound => ErrorKind::NotFound,
        io::ErrorKind::TimedOut | io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock => {
            ErrorKind::Unavailable
        }
        io::ErrorKind::UnexpectedEof => ErrorKind::Corrupt,
        _ => ErrorKind::Internal,
    };
    StoreError {
        kind,
        message: e.to_string(),
        source: Some(Box::new(e)),
    }
}

/// Run blocking file-system work off the async runtime.
async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> StoreResult<T> + Send + 'static,
) -> StoreResult<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| StoreError {
            kind: ErrorKind::Internal,
            message: format!("blocking task failed: {e}"),
            source: Some(Box::new(e)),
        })?
}

#[async_trait]
impl BlobStore for LocalDiskBlobStore {
    fn limits(&self) -> BlobLimits {
        self.inner.limits
    }

    async fn put(&self, key: &BlobKey, data: Bytes) -> StoreResult<PutOutcome> {
        let (inner, key) = (self.inner.clone(), key.clone());
        blocking(move || inner.put(&key, &data)).await
    }

    async fn read_range(&self, key: &BlobKey, range: ByteRange) -> StoreResult<BlobRead> {
        let (inner, key) = (self.inner.clone(), key.clone());
        blocking(move || inner.read_range(&key, range)).await
    }

    async fn stat(&self, keys: &[BlobKey]) -> StoreResult<Vec<Option<BlobInfo>>> {
        if keys.len() > self.inner.limits.max_batch {
            return Err(StoreError::new(ErrorKind::LimitExceeded, "batch too large"));
        }
        let (inner, keys) = (self.inner.clone(), keys.to_vec());
        blocking(move || keys.iter().map(|k| inner.stat_one(k)).collect()).await
    }

    async fn delete(&self, key: &BlobKey) -> StoreResult<()> {
        let (inner, key) = (self.inner.clone(), key.clone());
        blocking(move || inner.delete(&key)).await
    }
}
