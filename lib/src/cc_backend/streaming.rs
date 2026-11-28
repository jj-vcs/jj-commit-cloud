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

//! Streaming file I/O, paginated tree RPCs, symlink RPCs, and tree or blob digest
//! verification for [`super::CommitCloudBackend`].
//!
//! # Authority and Crash-Safety Ordering
//! File uploads stream in `FILE_STREAM_CHUNK_SIZE` chunks through `WriteFileStream`
//! with transparent fallback to unary `WriteFile` when an older server returns
//! `UNIMPLEMENTED`. Tree and blob reads verify canonical Git SHA-1 digests before
//! returning bytes or `Tree` values to `jj_lib`.
//!
//! # Cross-Process and RPC Boundaries
//! Caps buffered `ReadFile` and `WriteFile` streams at `MAX_STREAM_FILE_BYTES` (512 MiB),
//! enforces `MAX_TREE_ENTRIES` and strictly advancing pagination cursors on `ReadTree`, and
//! translates gRPC `Code::NotFound` statuses into `BackendError::ObjectNotFound`.

use std::pin::Pin;
use std::sync::atomic::Ordering;

use futures::AsyncReadExt as _;
use futures::StreamExt as _;
use futures::stream;
use jj_lib::backend::*;
use jj_lib::object_id::ObjectId;
use jj_lib::repo_path::{RepoPath, RepoPathComponentBuf};

use super::CommitCloudBackend;
use crate::util::run_on_tokio;

/// Maximum total entries accumulated when assembling a paginated `ReadTree` response,
/// matching `jj-cc-daemon`.
const MAX_TREE_ENTRIES: usize = 500_000;

/// Maximum total bytes buffered when assembling streamed `ReadFile` or `WriteFile`
/// payloads, matching the server's 512 MiB `MAX_STREAM_FILE_BYTES` limit.
const MAX_STREAM_FILE_BYTES: usize = 512 * 1024 * 1024;

pub fn tree_entry_to_proto(
    entry: &jj_lib::backend::TreeEntry,
) -> BackendResult<cc_common::backend::TreeEntry> {
    let value = match entry.value() {
        TreeValue::File {
            id,
            executable,
            copy_id,
        } => cc_common::backend::TreeValue {
            value: Some(cc_common::backend::tree_value::Value::File(
                cc_common::backend::File {
                    id: id.to_bytes().to_vec(),
                    executable: *executable,
                    copy_id: copy_id.to_bytes().to_vec(),
                    size_bytes: None,
                },
            )),
        },
        TreeValue::Symlink(id) => cc_common::backend::TreeValue {
            value: Some(cc_common::backend::tree_value::Value::SymlinkId(
                id.to_bytes().to_vec(),
            )),
        },
        TreeValue::Tree(id) => cc_common::backend::TreeValue {
            value: Some(cc_common::backend::tree_value::Value::TreeId(
                id.to_bytes().to_vec(),
            )),
        },
        TreeValue::GitSubmodule(id) => cc_common::backend::TreeValue {
            value: Some(cc_common::backend::tree_value::Value::GitSubmoduleId(
                id.to_bytes().to_vec(),
            )),
        },
    };

    Ok(cc_common::backend::TreeEntry {
        name: entry.name().as_internal_str().to_string(),
        value: Some(value),
    })
}

pub fn tree_entry_from_proto(
    proto_entry: cc_common::backend::TreeEntry,
) -> Result<(RepoPathComponentBuf, TreeValue), Box<dyn std::error::Error + Send + Sync>> {
    let name = &proto_entry.name;
    if name.is_empty() || name == "." || name == ".." || name.contains(['/', '\0']) {
        return Err(format!("invalid tree entry name {name:?}").into());
    }
    let component = RepoPathComponentBuf::new(proto_entry.name)?;
    let proto_val = proto_entry
        .value
        .ok_or("tree entry should have contained a TreeValue")?;
    let val = match proto_val
        .value
        .ok_or("TreeValue should have contained an inner value")?
    {
        cc_common::backend::tree_value::Value::File(f) => {
            if f.id.len() != cc_common::FILE_ID_LENGTH {
                return Err(format!(
                    "tree file entry id must be {} bytes (got {} bytes)",
                    cc_common::FILE_ID_LENGTH,
                    f.id.len()
                )
                .into());
            }
            if !f.copy_id.is_empty() {
                return Err(format!(
                    "tree file entry copy_id is unsupported and must be empty (got {} bytes)",
                    f.copy_id.len()
                )
                .into());
            }
            TreeValue::File {
                id: FileId::from_bytes(&f.id),
                executable: f.executable,
                copy_id: CopyId::placeholder(),
            }
        }
        cc_common::backend::tree_value::Value::SymlinkId(id) => {
            if id.len() != cc_common::SYMLINK_ID_LENGTH {
                return Err(format!(
                    "tree symlink entry id must be {} bytes (got {} bytes)",
                    cc_common::SYMLINK_ID_LENGTH,
                    id.len()
                )
                .into());
            }
            TreeValue::Symlink(SymlinkId::from_bytes(&id))
        }
        cc_common::backend::tree_value::Value::TreeId(id) => {
            if id.len() != cc_common::TREE_ID_LENGTH {
                return Err(format!(
                    "tree subdir entry id must be {} bytes (got {} bytes)",
                    cc_common::TREE_ID_LENGTH,
                    id.len()
                )
                .into());
            }
            TreeValue::Tree(TreeId::from_bytes(&id))
        }
        cc_common::backend::tree_value::Value::GitSubmoduleId(id) => {
            if id.len() != cc_common::COMMIT_ID_LENGTH {
                return Err(format!(
                    "tree submodule entry id must be {} bytes (got {} bytes)",
                    cc_common::COMMIT_ID_LENGTH,
                    id.len()
                )
                .into());
            }
            TreeValue::GitSubmodule(CommitId::from_bytes(&id))
        }
        cc_common::backend::tree_value::Value::ConflictId(_) => {
            return Err("tree entry should not have contained a ConflictId".into());
        }
    };
    Ok((component, val))
}

pub(super) fn tree_from_proto_entries(
    proto_entries: Vec<cc_common::backend::TreeEntry>,
) -> BackendResult<Tree> {
    let mut jj_entries = Vec::with_capacity(proto_entries.len());
    for entry in proto_entries {
        let (comp, val) = tree_entry_from_proto(entry).map_err(BackendError::Other)?;
        jj_entries.push((comp, val));
    }
    jj_entries.sort_by(|(a, _), (b, _)| a.cmp(b));
    for pair in jj_entries.windows(2) {
        if pair[0].0 == pair[1].0 {
            return Err(BackendError::Other(
                format!(
                    "ReadTree returned duplicate entry name {:?}",
                    pair[0].0.as_internal_str()
                )
                .into(),
            ));
        }
    }
    Ok(Tree::from_sorted_entries(jj_entries))
}

fn verify_git_blob_digest(
    content: &[u8],
    expected_id: &[u8],
    operation: &str,
) -> BackendResult<()> {
    let computed = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Blob, content)
        .map_err(|e| BackendError::Other(format!("SHA-1 digest error: {e}").into()))?;
    if computed.as_bytes() != expected_id {
        return Err(BackendError::Other(
            format!(
                "{operation} digest mismatch: computed {computed} did not match requested {}",
                jj_lib::hex_util::encode_hex(expected_id)
            )
            .into(),
        ));
    }
    Ok(())
}

/// Computes the canonical Git tree SHA-1 in Git's trailing-`/` directory sort order
/// (`gix::objs::tree::Entry`'s `Ord`), matching `server::hash_utils::compute_git_tree_hash`,
/// without disturbing `Tree`'s `RepoPathComponent` byte ordering.
fn compute_git_tree_hash(tree: &Tree) -> BackendResult<gix::hash::ObjectId> {
    use gix::objs::WriteTo as _;

    let mut gix_entries = Vec::new();
    for entry in tree.entries() {
        let (kind, id_bytes) = match entry.value() {
            TreeValue::File { id, executable, .. } => {
                let kind = if *executable {
                    gix::objs::tree::EntryKind::BlobExecutable
                } else {
                    gix::objs::tree::EntryKind::Blob
                };
                (kind, id.as_bytes())
            }
            TreeValue::Tree(id) => (gix::objs::tree::EntryKind::Tree, id.as_bytes()),
            TreeValue::Symlink(id) => (gix::objs::tree::EntryKind::Link, id.as_bytes()),
            TreeValue::GitSubmodule(id) => (gix::objs::tree::EntryKind::Commit, id.as_bytes()),
        };
        let oid = super::codec::sha1_object_id(id_bytes, "tree entry object ID")?;
        gix_entries.push(gix::objs::tree::Entry {
            mode: kind.into(),
            filename: entry.name().as_internal_str().as_bytes().into(),
            oid,
        });
    }
    gix_entries.sort_unstable();

    let gix_tree = gix::objs::Tree {
        entries: gix_entries,
    };
    let mut buf = Vec::new();
    gix_tree
        .write_to(&mut buf)
        .map_err(|e| BackendError::Other(format!("tree cannot be serialized: {e}").into()))?;
    gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Tree, &buf)
        .map_err(|e| BackendError::Other(format!("SHA-1 digest error: {e}").into()))
}

fn verify_git_tree_digest(tree: &Tree, expected_id: &TreeId) -> BackendResult<()> {
    let computed = compute_git_tree_hash(tree)?;
    if computed.as_bytes() != expected_id.as_bytes() {
        return Err(BackendError::Other(
            format!(
                "ReadTree digest mismatch: computed {computed} did not match requested {}",
                expected_id.hex()
            )
            .into(),
        ));
    }
    Ok(())
}

fn write_file_chunk_stream(
    raw_bytes: bytes::Bytes,
    project_id: String,
) -> impl futures::Stream<Item = cc_common::backend::WriteFileChunk> + Send + 'static {
    let total_len = raw_bytes.len() as u64;
    stream::unfold(
        (raw_bytes, Some(project_id)),
        move |(mut remaining, mut first_project_id)| async move {
            let first_pid = first_project_id.take();
            if first_pid.is_none() && remaining.is_empty() {
                return None;
            }
            let take = remaining.len().min(cc_common::FILE_STREAM_CHUNK_SIZE);
            let chunk = remaining.split_to(take).to_vec();
            let (chunk_project_id, total_size) = match first_pid {
                Some(pid) => (pid, Some(total_len)),
                None => (String::new(), None),
            };
            Some((
                cc_common::backend::WriteFileChunk {
                    project_id: chunk_project_id,
                    chunk,
                    total_size,
                    expected_file_id: None,
                },
                (remaining, None),
            ))
        },
    )
}

impl CommitCloudBackend {
    pub(super) async fn read_file_with_max_bytes(
        &self,
        id: &FileId,
        max_bytes: usize,
    ) -> BackendResult<Vec<u8>> {
        let server_url = self.server_url.clone();
        let project_id = self.project_id.clone();
        let file_id_bytes = id.to_bytes().to_vec();
        let file_id_hex = id.hex();

        let content = run_on_tokio(async move {
            let mut client = crate::util::connect_backend_client(server_url).await?;
            let res = client
                .read_file(tonic::Request::new(cc_common::backend::ReadFileRequest {
                    project_id,
                    file_id: file_id_bytes,
                    offset: 0,
                    max_bytes: 0,
                }))
                .await;

            let mut stream = match res {
                Ok(r) => r.into_inner(),
                Err(status) if status.code() == tonic::Code::NotFound => {
                    return Err(Box::new(BackendError::ObjectNotFound {
                        object_type: "file".into(),
                        hash: file_id_hex,
                        source: Some(status.into()),
                    })
                        as Box<dyn std::error::Error + Send + Sync>);
                }
                Err(status) => {
                    return Err(Box::new(status) as Box<dyn std::error::Error + Send + Sync>);
                }
            };

            let mut content = Vec::new();
            while let Some(chunk) = stream.next().await {
                let chunk = match chunk {
                    Ok(chunk) => chunk,
                    Err(status) if status.code() == tonic::Code::NotFound => {
                        return Err(Box::new(BackendError::ObjectNotFound {
                            object_type: "file".into(),
                            hash: file_id_hex.clone(),
                            source: Some(status.into()),
                        })
                            as Box<dyn std::error::Error + Send + Sync>);
                    }
                    Err(status) => {
                        return Err(Box::new(status) as Box<dyn std::error::Error + Send + Sync>);
                    }
                };
                if content
                    .len()
                    .checked_add(chunk.chunk.len())
                    .is_none_or(|n| n > max_bytes)
                {
                    return Err(Box::new(std::io::Error::other(format!(
                        "ReadFile exceeded maximum allowed size of {max_bytes} bytes for file {file_id_hex}"
                    )))
                        as Box<dyn std::error::Error + Send + Sync>);
                }
                content.extend_from_slice(&chunk.chunk);
            }
            Ok(content)
        })
        .await
        .map_err(|e| match e.downcast::<BackendError>() {
            Ok(err) => *err,
            Err(e) => BackendError::Other(e),
        })?;

        verify_git_blob_digest(&content, id.as_bytes(), "ReadFile")?;
        Ok(content)
    }

    pub(super) async fn read_file_streaming(
        &self,
        _path: &RepoPath,
        id: &FileId,
    ) -> BackendResult<Pin<Box<dyn futures::AsyncRead + Send>>> {
        let content = self
            .read_file_with_max_bytes(id, MAX_STREAM_FILE_BYTES)
            .await?;
        let cursor = futures::io::Cursor::new(content);
        Ok(Box::pin(cursor) as Pin<Box<dyn futures::AsyncRead + Send>>)
    }

    pub(super) async fn write_file_streaming_with_max_bytes(
        &self,
        _path: &RepoPath,
        contents: &mut (dyn futures::AsyncRead + Send + Unpin),
        max_bytes: usize,
    ) -> BackendResult<FileId> {
        let mut raw_bytes = Vec::new();
        let mut scratch = vec![0u8; cc_common::FILE_STREAM_CHUNK_SIZE];
        loop {
            let n = contents
                .read(&mut scratch)
                .await
                .map_err(|e| BackendError::Other(e.into()))?;
            if n == 0 {
                break;
            }
            if raw_bytes
                .len()
                .checked_add(n)
                .is_none_or(|total| total > max_bytes)
            {
                return Err(BackendError::Other(
                    format!("WriteFile exceeded maximum allowed size of {max_bytes} bytes").into(),
                ));
            }
            raw_bytes.extend_from_slice(&scratch[..n]);
        }
        let raw_bytes = bytes::Bytes::from(raw_bytes);

        let server_url = self.server_url.clone();
        let project_id = self.project_id.clone();
        let try_unary_first = self.prefer_unary_write_file.load(Ordering::Relaxed);

        let (file_id_bytes, switched_to_unary) = run_on_tokio(async move {
            let mut client = crate::util::connect_backend_client(server_url).await?;
            if !try_unary_first {
                let chunk_stream = write_file_chunk_stream(raw_bytes.clone(), project_id.clone());
                match client
                    .write_file_stream(tonic::Request::new(chunk_stream))
                    .await
                {
                    Ok(res) => return Ok((res.into_inner().file_id, false)),
                    Err(status) if status.code() == tonic::Code::Unimplemented => {}
                    Err(status) => return Err(status.into()),
                }
            }

            let res = client
                .write_file(tonic::Request::new(cc_common::backend::WriteFileRequest {
                    project_id,
                    content: Vec::from(raw_bytes),
                }))
                .await?;
            Ok((res.into_inner().file_id, true))
        })
        .await
        .map_err(BackendError::Other)?;

        if switched_to_unary && !try_unary_first {
            self.prefer_unary_write_file.store(true, Ordering::Relaxed);
        }

        super::validate_response_digest_len(
            &file_id_bytes,
            cc_common::FILE_ID_LENGTH,
            "WriteFile response file_id",
        )?;
        Ok(FileId::from_bytes(&file_id_bytes))
    }

    pub(super) async fn write_file_streaming(
        &self,
        path: &RepoPath,
        contents: &mut (dyn futures::AsyncRead + Send + Unpin),
    ) -> BackendResult<FileId> {
        self.write_file_streaming_with_max_bytes(path, contents, MAX_STREAM_FILE_BYTES)
            .await
    }

    pub(super) async fn read_symlink_verified(
        &self,
        _path: &RepoPath,
        id: &SymlinkId,
    ) -> BackendResult<String> {
        let server_url = self.server_url.clone();
        let project_id = self.project_id.clone();
        let symlink_id_bytes = id.to_bytes().to_vec();
        let symlink_id_hex = id.hex();

        let target = run_on_tokio(async move {
            let mut client = crate::util::connect_backend_client(server_url).await?;
            let res = client
                .read_symlink(tonic::Request::new(
                    cc_common::backend::ReadSymlinkRequest {
                        project_id,
                        symlink_id: symlink_id_bytes,
                    },
                ))
                .await;

            match res {
                Ok(r) => Ok(r.into_inner().target),
                Err(status) if status.code() == tonic::Code::NotFound => {
                    Err(Box::new(BackendError::ObjectNotFound {
                        object_type: "symlink".into(),
                        hash: symlink_id_hex,
                        source: Some(status.into()),
                    })
                        as Box<dyn std::error::Error + Send + Sync>)
                }
                Err(status) => Err(Box::new(status) as Box<dyn std::error::Error + Send + Sync>),
            }
        })
        .await
        .map_err(|e| match e.downcast::<BackendError>() {
            Ok(err) => *err,
            Err(e) => BackendError::Other(e),
        })?;

        if target.contains('\0') {
            return Err(BackendError::Other(
                format!(
                    "ReadSymlink target for {} must not contain NUL bytes",
                    id.hex()
                )
                .into(),
            ));
        }
        verify_git_blob_digest(target.as_bytes(), id.as_bytes(), "ReadSymlink")?;
        Ok(target)
    }

    pub(super) async fn write_symlink_rpc(
        &self,
        _path: &RepoPath,
        target: &str,
    ) -> BackendResult<SymlinkId> {
        let server_url = self.server_url.clone();
        let project_id = self.project_id.clone();
        let target_owned = target.to_string();

        let symlink_id_bytes = run_on_tokio(async move {
            let mut client = crate::util::connect_backend_client(server_url).await?;
            let res = client
                .write_symlink(tonic::Request::new(
                    cc_common::backend::WriteSymlinkRequest {
                        project_id,
                        target: target_owned,
                    },
                ))
                .await?;
            Ok(res.into_inner().symlink_id)
        })
        .await
        .map_err(BackendError::Other)?;

        super::validate_response_digest_len(
            &symlink_id_bytes,
            cc_common::SYMLINK_ID_LENGTH,
            "WriteSymlink response symlink_id",
        )?;
        Ok(SymlinkId::from_bytes(&symlink_id_bytes))
    }

    pub(super) async fn read_tree_paginated(
        &self,
        path: &RepoPath,
        id: &TreeId,
    ) -> BackendResult<Tree> {
        if *id == self.empty_tree_id {
            return Ok(Tree::from_sorted_entries(vec![]));
        }

        let server_url = self.server_url.clone();
        let project_id = self.project_id.clone();
        let tree_id_bytes = id.to_bytes().to_vec();
        let tree_id_hex = id.hex();
        let path_str = path.as_internal_file_string().to_string();

        let proto_entries = run_on_tokio(async move {
            let mut client = crate::util::connect_backend_client(server_url).await?;
            let mut all_entries = Vec::new();
            let mut page_token = String::new();
            loop {
                let res = client
                    .read_tree(tonic::Request::new(cc_common::backend::ReadTreeRequest {
                        project_id: project_id.clone(),
                        tree_id: tree_id_bytes.clone(),
                        path: path_str.clone(),
                        page_size: 0,
                        page_token: page_token.clone(),
                    }))
                    .await;

                let resp = match res {
                    Ok(r) => r.into_inner(),
                    Err(status) if status.code() == tonic::Code::NotFound => {
                        return Err(Box::new(BackendError::ObjectNotFound {
                            object_type: "tree".into(),
                            hash: tree_id_hex,
                            source: Some(status.into()),
                        })
                            as Box<dyn std::error::Error + Send + Sync>);
                    }
                    Err(status) => {
                        return Err(Box::new(status) as Box<dyn std::error::Error + Send + Sync>);
                    }
                };
                if !resp.tree_id.is_empty() && resp.tree_id != tree_id_bytes {
                    return Err(Box::new(std::io::Error::other(format!(
                        "ReadTree response tree_id {} did not match requested {tree_id_hex}",
                        jj_lib::hex_util::encode_hex(&resp.tree_id),
                    )))
                        as Box<dyn std::error::Error + Send + Sync>);
                }
                if resp.entries.is_empty() && !resp.next_page_token.is_empty() {
                    return Err(Box::new(std::io::Error::other(format!(
                        "ReadTree returned empty page with non-empty next_page_token for tree {tree_id_hex}"
                    )))
                        as Box<dyn std::error::Error + Send + Sync>);
                }
                if all_entries.len().saturating_add(resp.entries.len()) > MAX_TREE_ENTRIES {
                    return Err(Box::new(std::io::Error::other(format!(
                        "ReadTree exceeded maximum of {MAX_TREE_ENTRIES} entries for tree {tree_id_hex}"
                    )))
                        as Box<dyn std::error::Error + Send + Sync>);
                }
                all_entries.extend(resp.entries);
                if resp.next_page_token.is_empty() {
                    break;
                }
                if !page_token.is_empty()
                    && resp.next_page_token.as_bytes() <= page_token.as_bytes()
                {
                    return Err(Box::new(std::io::Error::other(format!(
                        "read_tree pagination cursor did not advance for tree {tree_id_hex}"
                    )))
                        as Box<dyn std::error::Error + Send + Sync>);
                }
                page_token = resp.next_page_token;
            }
            Ok(all_entries)
        })
        .await
        .map_err(|e| match e.downcast::<BackendError>() {
            Ok(err) => *err,
            Err(e) => BackendError::Other(e),
        })?;

        let tree = tree_from_proto_entries(proto_entries)?;
        verify_git_tree_digest(&tree, id)?;
        Ok(tree)
    }

    pub(super) async fn write_tree_rpc(
        &self,
        path: &RepoPath,
        tree: &Tree,
    ) -> BackendResult<TreeId> {
        if tree.entries().next().is_none() {
            return Ok(self.empty_tree_id.clone());
        }

        let proto_entries: Result<Vec<_>, _> =
            tree.entries().map(|e| tree_entry_to_proto(&e)).collect();
        let proto_entries = proto_entries?;

        let server_url = self.server_url.clone();
        let project_id = self.project_id.clone();
        let path_str = path.as_internal_file_string().to_string();

        let tree_id_bytes = run_on_tokio(async move {
            let mut client = crate::util::connect_backend_client(server_url).await?;
            let res = client
                .write_tree(tonic::Request::new(cc_common::backend::WriteTreeRequest {
                    project_id,
                    path: path_str,
                    entries: proto_entries,
                }))
                .await?;
            Ok(res.into_inner().tree_id)
        })
        .await
        .map_err(BackendError::Other)?;

        super::validate_response_digest_len(
            &tree_id_bytes,
            cc_common::TREE_ID_LENGTH,
            "WriteTree response tree_id",
        )?;
        Ok(TreeId::from_bytes(&tree_id_bytes))
    }
}
