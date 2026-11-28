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

//! Content-addressed L2 disk storage, private file permissions, validation, and ranged reads.
//!
//! # Authority and Crash-Safety Ordering
//! L2 objects write to process-unique temporary files created with `create_new` and mode
//! `0600` on Unix, call `sync_all`, and rename atomically into sharded directories under
//! `0700` cache roots. Scope authorization markers are created directly with `create_new`
//! and mode `0600`. Unary L2 reads and small (`<= MAX_L1_OBJECT_BYTES`) `File` reads
//! validate blob SHA-1 or protobuf structure and discard invalid entries.
//!
//! # Cross-Process and RPC Boundaries
//! `read_cached_file_range` streams L1 or L2 file slices in 64 KiB chunks over a bounded
//! `mpsc` channel without loading large (`> MAX_L1_OBJECT_BYTES`) blobs into RAM. Large L2
//! blobs rely on write-time SHA-1 verification and discard the cached file if a ranged read
//! hits `UnexpectedEof` truncation.

use std::fs;
use std::io::{Read as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use cc_common::backend::{Commit, ReadTreeResponse};
use cc_common::op_store::{Operation, View};
use prost::Message;
use sha1::{Digest, Sha1};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::mpsc;

use super::{
    CACHE_STREAM_CHANNEL_CAPACITY, CACHE_STREAM_CHUNK_BYTES, CacheInner, CacheRangeError,
    CacheScope, MAX_L1_OBJECT_BYTES, MAX_OBJECT_ID_BYTES, ObjectKind, TEMP_SEQ, TwoTierCache,
};

impl TwoTierCache {
    /// Streams a byte-range slice `[offset .. offset + max_bytes]` from L1 or L2 disk CAS in
    /// bounded `64 KiB` frames if the blob is cached and authorized for `scope`, returning `None`
    /// on a cold miss.
    pub async fn read_cached_file_range(
        &self,
        scope: &CacheScope,
        file_id: &[u8],
        offset: u64,
        max_bytes: u64,
    ) -> Result<Option<mpsc::Receiver<Result<Vec<u8>, std::io::Error>>>, CacheRangeError> {
        self.touch_activity();
        if !is_valid_object_id(file_id) || self.inner.is_pending_purge(&scope.namespace_hex) {
            return Ok(None);
        }

        let start_epoch = self.purge_epoch();
        let key = CacheInner::l1_key(scope, ObjectKind::File, file_id);
        let in_memory_authorized =
            self.inner
                .is_authorized_in_memory(scope, ObjectKind::File, file_id);
        if in_memory_authorized
            && let Some(hit) = self
                .inner
                .l1
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&key)
        {
            self.inner.l1_hits.fetch_add(1, Ordering::Relaxed);
            let total_len = hit.len() as u64;
            if offset > total_len {
                return Err(CacheRangeError::OffsetOutOfRange { offset, total_len });
            }
            return Ok(Some(stream_bytes_range(hit, offset, max_bytes)));
        }

        if !in_memory_authorized {
            let inner = Arc::clone(&self.inner);
            let scope_cloned = scope.clone();
            let id_vec = file_id.to_vec();
            let authorized = tokio::task::spawn_blocking(move || {
                inner.is_authorized(&scope_cloned, ObjectKind::File, &id_vec)
            })
            .await
            .map_err(|err| CacheRangeError::Io(std::io::Error::other(err)))?;
            if !authorized
                || self.inner.is_pending_purge(&scope.namespace_hex)
                || self.purge_epoch() != start_epoch
            {
                return Ok(None);
            }
            if let Some(hit) = self
                .inner
                .l1
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&key)
            {
                self.inner.l1_hits.fetch_add(1, Ordering::Relaxed);
                let total_len = hit.len() as u64;
                if offset > total_len {
                    return Err(CacheRangeError::OffsetOutOfRange { offset, total_len });
                }
                return Ok(Some(stream_bytes_range(hit, offset, max_bytes)));
            }
        }

        enum L2FileRead {
            Miss,
            Small(Bytes),
            Large {
                file: fs::File,
                path: PathBuf,
                total_len: u64,
            },
        }

        let path = self
            .inner
            .shared_object_path(scope, ObjectKind::File, file_id);
        let id_vec = file_id.to_vec();
        let l2_read = tokio::task::spawn_blocking(move || -> std::io::Result<L2FileRead> {
            let Ok(mut file) = fs::File::open(&path) else {
                return Ok(L2FileRead::Miss);
            };
            let meta = file.metadata()?;
            if !meta.is_file() {
                return Ok(L2FileRead::Miss);
            }
            let total_len = meta.len();
            if total_len <= MAX_L1_OBJECT_BYTES as u64 {
                let mut all = Vec::with_capacity(total_len as usize);
                (&mut file).take(total_len + 1).read_to_end(&mut all)?;
                if all.len() as u64 != total_len || !matches_git_blob_id(&id_vec, &all) {
                    discard_invalid_l2_entry(&path);
                    return Ok(L2FileRead::Miss);
                }
                return Ok(L2FileRead::Small(Bytes::from(all)));
            }
            Ok(L2FileRead::Large {
                file,
                path,
                total_len,
            })
        })
        .await
        .map_err(|err| CacheRangeError::Io(std::io::Error::other(err)))??;

        let (mut file, path, total_len) = match l2_read {
            L2FileRead::Miss => return Ok(None),
            L2FileRead::Small(bytes) => {
                if self.inner.is_pending_purge(&scope.namespace_hex)
                    || self.purge_epoch() != start_epoch
                {
                    return Ok(None);
                }
                self.inner.l2_hits.fetch_add(1, Ordering::Relaxed);
                let actual_len = bytes.len() as u64;
                self.inner
                    .l1
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .put(key, bytes.clone());
                if offset > actual_len {
                    return Err(CacheRangeError::OffsetOutOfRange {
                        offset,
                        total_len: actual_len,
                    });
                }
                return Ok(Some(stream_bytes_range(bytes, offset, max_bytes)));
            }
            L2FileRead::Large {
                file,
                path,
                total_len,
            } => {
                if self.inner.is_pending_purge(&scope.namespace_hex)
                    || self.purge_epoch() != start_epoch
                {
                    return Ok(None);
                }
                self.inner.l2_hits.fetch_add(1, Ordering::Relaxed);
                (tokio::fs::File::from_std(file), path, total_len)
            }
        };

        if offset > total_len {
            return Err(CacheRangeError::OffsetOutOfRange { offset, total_len });
        }
        if offset == total_len {
            let (_tx, rx) = mpsc::channel(1);
            return Ok(Some(rx));
        }
        file.seek(SeekFrom::Start(offset)).await?;
        let remaining = if max_bytes == 0 {
            total_len - offset
        } else {
            max_bytes.min(total_len - offset)
        };
        let (tx, rx) = mpsc::channel(CACHE_STREAM_CHANNEL_CAPACITY);
        tokio::spawn(async move {
            let mut remaining = remaining;
            while remaining > 0 {
                let to_read = remaining.min(CACHE_STREAM_CHUNK_BYTES as u64) as usize;
                let mut buf = vec![0u8; to_read];
                match file.read(&mut buf).await {
                    Ok(0) => {
                        // Unexpected EOF means the L2 file was truncated or torn; discard before
                        // surfacing the error so subsequent reads refetch.
                        discard_invalid_l2_entry_async(path).await;
                        let _ = tx
                            .send(Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                format!(
                                    "cached L2 blob truncated with {remaining} bytes remaining"
                                ),
                            )))
                            .await;
                        return;
                    }
                    Ok(n) => {
                        buf.truncate(n);
                        remaining -= n as u64;
                        if tx.send(Ok(buf)).await.is_err() {
                            return;
                        }
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(err) => {
                        let _ = tx.send(Err(err)).await;
                        return;
                    }
                }
            }
        });
        Ok(Some(rx))
    }
}

impl CacheInner {
    pub(super) fn object_path(
        &self,
        scope: &CacheScope,
        kind: ObjectKind,
        id_bytes: &[u8],
    ) -> PathBuf {
        if kind.is_scoped() {
            self.scoped_path(scope, kind, id_bytes)
        } else {
            self.shared_object_path(scope, kind, id_bytes)
        }
    }

    pub(super) fn shared_object_path(
        &self,
        scope: &CacheScope,
        kind: ObjectKind,
        id_bytes: &[u8],
    ) -> PathBuf {
        let (p1, p2, hex_id) = sharded_hex_parts(id_bytes);
        self.cache_dir
            .join("objects")
            .join(&scope.upstream_hex)
            .join(kind.dir_name())
            .join(p1)
            .join(p2)
            .join(hex_id)
    }

    pub(super) fn scoped_path(
        &self,
        scope: &CacheScope,
        kind: ObjectKind,
        id_bytes: &[u8],
    ) -> PathBuf {
        let (p1, p2, hex_id) = sharded_hex_parts(id_bytes);
        self.cache_dir
            .join("scopes")
            .join(&scope.namespace_hex)
            .join(&scope.scope_hex)
            .join(kind.dir_name())
            .join(p1)
            .join(p2)
            .join(hex_id)
    }

    pub(super) fn blob_size(&self, scope: &CacheScope, file_id: &[u8]) -> Option<u64> {
        if !is_valid_object_id(file_id) || !self.is_authorized(scope, ObjectKind::File, file_id) {
            return None;
        }
        let key = Self::l1_key(scope, ObjectKind::File, file_id);
        if let Some(len) = self
            .l1
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .peek_len(&key)
        {
            return Some(len);
        }
        let meta = fs::metadata(self.shared_object_path(scope, ObjectKind::File, file_id)).ok()?;
        if !meta.is_file() {
            return None;
        }
        let len = meta.len();
        // An empty entry for a non-empty blob is a torn write, not a size.
        (len > 0 || matches_git_blob_id(file_id, b"")).then_some(len)
    }

    pub(super) fn write_l2_atomic(
        &self,
        dst: &Path,
        id_bytes: &[u8],
        data: &[u8],
    ) -> std::io::Result<()> {
        if !is_valid_object_id(id_bytes) {
            return Ok(());
        }
        let Some(parent) = dst.parent() else {
            return Ok(());
        };
        ensure_private_dir_within(&self.cache_dir, parent)?;
        let seq = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let tmp_name = format!(
            ".tmp.{}.{}.{}",
            std::process::id(),
            seq,
            hex::encode(id_bytes)
        );
        let tmp_path = parent.join(tmp_name);
        // Flush the data before the rename so that an OS crash cannot leave an empty or
        // partial file under the final name. Always overwrite `dst` so a previously corrupted
        // file on disk is replaced atomically.
        let written = write_synced(&tmp_path, data).and_then(|()| fs::rename(&tmp_path, dst));
        if written.is_err() {
            let _ = fs::remove_file(&tmp_path);
        }
        written
    }
}

pub(super) fn is_valid_object_id(id_bytes: &[u8]) -> bool {
    !id_bytes.is_empty() && id_bytes.len() <= MAX_OBJECT_ID_BYTES
}

pub(super) fn is_regular_file(path: &Path) -> bool {
    fs::metadata(path).map(|m| m.is_file()).unwrap_or(false)
}

pub(super) fn create_dir_all_private(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(path)
    }
    #[cfg(not(unix))]
    {
        fs::create_dir_all(path)
    }
}

fn create_dir_single_private(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new().mode(0o700).create(path)
    }
    #[cfg(not(unix))]
    {
        fs::create_dir(path)
    }
}

pub(super) fn set_dir_private(path: &Path) -> std::io::Result<()> {
    // Strip any trailing slash so `O_NOFOLLOW` and `symlink_metadata` inspect the final
    // path component itself rather than resolving through a symlinked directory.
    let target: PathBuf = path.components().collect();
    let meta = fs::symlink_metadata(&target)?;
    if meta.file_type().is_symlink() || !meta.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "{} must be a real directory (symlinks and non-directories are not allowed)",
                target.display()
            ),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        if (meta.permissions().mode() & 0o777) == 0o700 {
            return Ok(());
        }
        let fd = fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(&target)
            .map_err(|err| {
                if matches!(err.raw_os_error(), Some(libc::ELOOP | libc::ENOTDIR)) {
                    std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        format!("{} must be a real directory: {err}", target.display()),
                    )
                } else {
                    err
                }
            })?;
        let fd_meta = fd.metadata()?;
        if !fd_meta.is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!("{} must be a real directory", target.display()),
            ));
        }
        if (fd_meta.permissions().mode() & 0o777) != 0o700 {
            fd.set_permissions(fs::Permissions::from_mode(0o700))?;
        }
    }
    Ok(())
}

pub(super) fn ensure_private_dir_within(root: &Path, dir: &Path) -> std::io::Result<()> {
    let root_norm: PathBuf = root.components().collect();
    let dir_norm: PathBuf = dir.components().collect();
    if let Ok(rel) = dir_norm.strip_prefix(&root_norm) {
        // Walk top-down from `root` so a symlink planted at any intermediate component is
        // rejected before creating subdirectories or modifying permissions through it.
        match fs::symlink_metadata(&root_norm) {
            Ok(_) => set_dir_private(&root_norm)?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                create_dir_all_private(&root_norm)?;
                set_dir_private(&root_norm)?;
            }
            Err(err) => return Err(err),
        }
        let mut cur = root_norm;
        for comp in rel.components() {
            cur.push(comp);
            match fs::symlink_metadata(&cur) {
                Ok(_) => set_dir_private(&cur)?,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    match create_dir_single_private(&cur) {
                        Ok(()) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                        Err(e) => return Err(e),
                    }
                    set_dir_private(&cur)?;
                }
                Err(err) => return Err(err),
            }
        }
        return Ok(());
    }
    if let Ok(meta) = fs::symlink_metadata(&dir_norm)
        && (meta.file_type().is_symlink() || !meta.is_dir())
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "{} must be a real directory (symlinks and non-directories are not allowed)",
                dir_norm.display()
            ),
        ));
    }
    create_dir_all_private(&dir_norm)?;
    set_dir_private(&dir_norm)
}

pub(super) fn write_marker_private(path: &Path) -> std::io::Result<()> {
    let mut opts = fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    match opts.open(path) {
        Ok(_) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(err) => Err(err),
    }
}

pub(super) fn write_synced(path: &Path, data: &[u8]) -> std::io::Result<()> {
    let mut opts = fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut file = opts.open(path)?;
    file.write_all(data)?;
    file.sync_all()
}

/// Returns `false` only when `id_bytes` is a 20-byte SHA-1 that differs from the Git blob hash
/// (`blob <len>\0<content>`) of `content`. jj-cc-server derives `File` and `Symlink` ids this
/// way. Ids of other lengths are not checked.
pub(super) fn matches_git_blob_id(id_bytes: &[u8], content: &[u8]) -> bool {
    if id_bytes.len() != 20 {
        return true;
    }
    let mut hasher = Sha1::new();
    hasher.update(format!("blob {}\0", content.len()).as_bytes());
    hasher.update(content);
    hasher.finalize().as_slice() == id_bytes
}

/// Rejects payloads that the daemon must never serve or store: empty payloads other than the
/// empty blob, invalid or NUL-containing symlinks, undecodable protobufs, commits missing
/// `change_id` or a valid odd-length `root_tree_id` or whose `commit_id` mismatches `id_bytes`,
/// trees missing or mismatching `tree_id` or carrying a `next_page_token`, `View` snapshots with
/// empty `head_ids`, and `Operation` records with an empty `view_id`.
pub(super) fn l2_payload_is_valid(kind: ObjectKind, id_bytes: &[u8], bytes: &[u8]) -> bool {
    if bytes.is_empty() {
        return matches!(kind, ObjectKind::File | ObjectKind::Symlink)
            && matches_git_blob_id(id_bytes, bytes);
    }
    match kind {
        ObjectKind::File => matches_git_blob_id(id_bytes, bytes),
        ObjectKind::Symlink => {
            !bytes.contains(&0)
                && std::str::from_utf8(bytes).is_ok()
                && matches_git_blob_id(id_bytes, bytes)
        }
        ObjectKind::Commit => Commit::decode(bytes)
            .map(|commit| {
                (commit.commit_id.is_empty() || commit.commit_id == id_bytes)
                    && !commit.change_id.is_empty()
                    && commit.root_tree_id.len() % 2 == 1
                    && commit.root_tree_id.iter().all(|t| !t.is_empty())
            })
            .unwrap_or(false),
        ObjectKind::Tree => ReadTreeResponse::decode(bytes)
            .map(|tree| {
                !tree.tree_id.is_empty()
                    && tree.tree_id == id_bytes
                    && tree.next_page_token.is_empty()
            })
            .unwrap_or(false),
        ObjectKind::View => View::decode(bytes)
            .map(|view| !view.head_ids.is_empty())
            .unwrap_or(false),
        ObjectKind::Operation => Operation::decode(bytes)
            .map(|op| !op.view_id.is_empty())
            .unwrap_or(false),
    }
}

pub(super) fn discard_invalid_l2_entry(path: &Path) {
    tracing::warn!("discarding invalid L2 cache entry {}", path.display());
    let _ = fs::remove_file(path);
}

pub(super) async fn discard_invalid_l2_entry_async(path: PathBuf) {
    let _ = tokio::task::spawn_blocking(move || discard_invalid_l2_entry(&path)).await;
}

pub(super) fn sharded_hex_parts(id_bytes: &[u8]) -> (String, String, String) {
    let hex_id = hex::encode(id_bytes);
    let (p1, p2) = if hex_id.len() >= 4 {
        (hex_id[0..2].to_string(), hex_id[2..4].to_string())
    } else {
        ("00".to_string(), "00".to_string())
    };
    (p1, p2, hex_id)
}

pub(super) fn stream_bytes_range(
    bytes: Bytes,
    offset: u64,
    max_bytes: u64,
) -> mpsc::Receiver<Result<Vec<u8>, std::io::Error>> {
    let total = bytes.len();
    let start = usize::try_from(offset).unwrap_or(total).min(total);
    let end = if max_bytes == 0 {
        total
    } else {
        start
            .saturating_add(usize::try_from(max_bytes).unwrap_or(usize::MAX))
            .min(total)
    };
    if start >= end {
        let (_tx, rx) = mpsc::channel(1);
        return rx;
    }
    // Copy sub-slices so small range frames never pin a larger backing `Bytes` allocation.
    let slice = if start > 0 || end < total {
        Bytes::copy_from_slice(&bytes[start..end])
    } else {
        bytes
    };
    let (tx, rx) = mpsc::channel(CACHE_STREAM_CHANNEL_CAPACITY);
    tokio::spawn(async move {
        for chunk in slice.chunks(CACHE_STREAM_CHUNK_BYTES) {
            if tx.send(Ok(chunk.to_vec())).await.is_err() {
                break;
            }
        }
    });
    rx
}
