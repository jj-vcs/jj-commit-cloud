// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! gRPC `BackendService` implementation backed by [`CompositeStore`].
//!
//! # Authority and Crash-Safety Ordering
//! `WriteFile` and `WriteFileStream` persist file payloads to `BlobStore` before
//! publishing `BlobRecord` rows in `MetadataStore`. `WriteTree` and `WriteCommit`
//! delegate batched dependency checks to `CompositeStore` before exposing parent
//! directory or commit graph records, and `WriteCommit` verifies canonical Git SHA-1
//! digest parity on any client-supplied `commit_id` or `raw_git_commit`.
//!
//! # Concurrency and Merge Invariants
//! All object mutations (`WriteFile`, `WriteFileStream`, `WriteSymlink`, `WriteTree`,
//! `WriteCommit`) are idempotent content-addressed writes keyed by 20-byte Git SHA-1
//! digests, allowing parallel client uploads without server-side locks.
//!
//! # Cross-Process and RPC Boundaries
//! `ReadFile` and `WriteFileStream` stream payloads in 64 KiB frames (`FILE_STREAM_CHUNK_SIZE`),
//! while `ReadTree` returns entries sorted in UTF-8 `RepoPathComponent` byte order with
//! `File.size_bytes` populated from `MetadataStore` and optional cursor pagination.

use std::path::PathBuf;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::io::AsyncWriteExt as _;
use tokio::sync::OwnedSemaphorePermit;
use tokio_stream::StreamExt;
use tracing::info;

use cc_common::backend::backend_service_server::BackendService;
use cc_common::backend::*;

use crate::hash_utils::{
    compute_git_tree_hash, try_compute_git_blob_hash, verify_or_compute_git_commit_hash,
};
use crate::metrics::ServerMetrics;
use crate::store::{ChangePrefixResolution, CompositeStore, IdPrefixResolution, StoreError};

const MAX_IDENTIFIER_LEN: usize = 128;
const MAX_DISPLAY_NAME_LEN: usize = 256;
/// Uploads exceeding 4 MiB (or arriving while the server-wide streaming memory budget is
/// saturated) spill automatically to a temporary file on disk to keep peak server RAM bounded.
pub const DEFAULT_WRITE_STREAM_SPILL_THRESHOLD_BYTES: usize = 4 * 1024 * 1024;

fn normalize_optional_name<'a>(
    raw: Option<&'a str>,
    field: &str,
) -> Result<Option<&'a str>, tonic::Status> {
    let Some(trimmed) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    if trimmed.chars().count() > MAX_DISPLAY_NAME_LEN {
        return Err(tonic::Status::invalid_argument(format!(
            "{field} must be at most {MAX_DISPLAY_NAME_LEN} characters"
        )));
    }
    if trimmed.chars().any(char::is_control) {
        return Err(tonic::Status::invalid_argument(format!(
            "{field} must not contain control characters"
        )));
    }
    Ok(Some(trimmed))
}

pub(crate) fn validate_identifier(id: &str, field: &str) -> Result<(), tonic::Status> {
    if id.is_empty() || id.len() > MAX_IDENTIFIER_LEN || id == "." || id == ".." {
        return Err(tonic::Status::invalid_argument(format!(
            "{field} must be 1..={MAX_IDENTIFIER_LEN} ASCII identifier characters (excluding '.' and '..')"
        )));
    }
    if !id
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    {
        return Err(tonic::Status::invalid_argument(format!(
            "{field} contains disallowed characters (allowed: ASCII alphanumeric, '-', '_', '.')"
        )));
    }
    Ok(())
}

pub(crate) fn validate_digest_len(
    bytes: &[u8],
    expected_len: usize,
    field: &str,
) -> Result<(), tonic::Status> {
    if bytes.len() != expected_len {
        return Err(tonic::Status::invalid_argument(format!(
            "{field} must be {expected_len} bytes (got {} bytes)",
            bytes.len()
        )));
    }
    Ok(())
}

#[derive(Clone)]
pub struct CommitCloudBackendService {
    store: Arc<CompositeStore>,
    write_stream_spill_threshold: usize,
    metrics: ServerMetrics,
}

impl CommitCloudBackendService {
    pub fn new(store: Arc<CompositeStore>) -> Self {
        Self::with_metrics(store, ServerMetrics::new())
    }

    pub fn with_metrics(store: Arc<CompositeStore>, metrics: ServerMetrics) -> Self {
        Self {
            store,
            write_stream_spill_threshold: DEFAULT_WRITE_STREAM_SPILL_THRESHOLD_BYTES,
            metrics,
        }
    }

    pub fn with_write_stream_spill_threshold(mut self, bytes: usize) -> Self {
        self.write_stream_spill_threshold = bytes;
        self
    }
}

enum StreamStaging {
    Memory {
        buf: BytesMut,
        permit: Option<OwnedSemaphorePermit>,
    },
    Disk {
        // Declared before `named` so Rust closes the cloned file handle before
        // `NamedTempFile` unlinks the file on early-return error paths.
        writer: tokio::fs::File,
        named: tempfile::NamedTempFile,
    },
}

async fn create_spill_file() -> Result<(tempfile::NamedTempFile, tokio::fs::File), tonic::Status> {
    tokio::task::spawn_blocking(|| {
        let named = tempfile::NamedTempFile::new().map_err(|e| {
            tonic::Status::internal(format!("failed to create upload spill file: {e}"))
        })?;
        let std_file = named.as_file().try_clone().map_err(|e| {
            tonic::Status::internal(format!("failed to clone upload spill file handle: {e}"))
        })?;
        Ok::<_, tonic::Status>((named, tokio::fs::File::from_std(std_file)))
    })
    .await
    .map_err(|e| tonic::Status::internal(format!("spill file creation task failed: {e}")))?
}

async fn compute_git_blob_hash_from_file(
    path: PathBuf,
    total_size: u64,
) -> Result<Vec<u8>, tonic::Status> {
    tokio::task::spawn_blocking(move || {
        use std::io::Read as _;

        let mut file = std::fs::File::open(&path).map_err(|e| {
            tonic::Status::internal(format!("failed to open spilled upload file: {e}"))
        })?;
        let mut hasher = gix::hash::hasher(gix::hash::Kind::Sha1);
        hasher.update(format!("blob {total_size}\0").as_bytes());
        let mut buf = vec![0u8; cc_common::FILE_STREAM_CHUNK_SIZE];
        let mut hashed = 0u64;
        loop {
            let n = file.read(&mut buf).map_err(|e| {
                tonic::Status::internal(format!("failed to read spilled upload file: {e}"))
            })?;
            if n == 0 {
                break;
            }
            hashed = hashed.saturating_add(n as u64);
            hasher.update(&buf[..n]);
        }
        if hashed != total_size {
            return Err(tonic::Status::internal(format!(
                "spilled upload file length mismatch: hashed {hashed} bytes, expected {total_size}"
            )));
        }
        hasher
            .try_finalize()
            .map(|id| id.as_bytes().to_vec())
            .map_err(|e| {
                tonic::Status::invalid_argument(format!("SHA-1 digest computation error: {e}"))
            })
    })
    .await
    .map_err(|e| tonic::Status::internal(format!("spilled upload hash task failed: {e}")))?
}

#[tonic::async_trait]
impl BackendService for CommitCloudBackendService {
    async fn register_project(
        &self,
        request: tonic::Request<RegisterProjectRequest>,
    ) -> Result<tonic::Response<RegisterProjectResponse>, tonic::Status> {
        let req = request.into_inner();
        let project_id = match req.project_id {
            Some(raw) => {
                validate_identifier(&raw, "project_id")?;
                raw
            }
            None => uuid::Uuid::new_v4().to_string(),
        };
        let clean_name = normalize_optional_name(req.name.as_deref(), "name")?;
        info!("Registering project: {}", project_id);
        self.store.register_project(&project_id, clean_name).await?;
        Ok(tonic::Response::new(RegisterProjectResponse { project_id }))
    }

    async fn register_repository(
        &self,
        request: tonic::Request<RegisterRepositoryRequest>,
    ) -> Result<tonic::Response<RegisterRepositoryResponse>, tonic::Status> {
        let req = request.into_inner();
        match HashAlgorithm::try_from(req.hash_algorithm) {
            Ok(HashAlgorithm::Unspecified | HashAlgorithm::Sha1) => {}
            Ok(HashAlgorithm::Sha256) => {
                return Err(tonic::Status::unimplemented(
                    "SHA-256 repositories are not yet implemented; only SHA-1 is supported",
                ));
            }
            Err(_) => {
                return Err(tonic::Status::invalid_argument(format!(
                    "unknown hash_algorithm value: {}",
                    req.hash_algorithm
                )));
            }
        }
        let repo_id = match req.repo_id {
            Some(raw) => {
                validate_identifier(&raw, "repo_id")?;
                raw
            }
            None => uuid::Uuid::new_v4().to_string(),
        };
        let requested_project = if req.project_id.is_empty() {
            None
        } else {
            validate_identifier(&req.project_id, "project_id")?;
            Some(req.project_id.as_str())
        };
        let clean_name = normalize_optional_name(req.name.as_deref(), "name")?;
        info!(
            "Registering repository: {} (requested project: {:?})",
            repo_id, requested_project
        );
        let project_id = self
            .store
            .register_repo_in_project(&repo_id, requested_project, clean_name)
            .await?;
        Ok(tonic::Response::new(RegisterRepositoryResponse {
            repo_id,
            project_id,
        }))
    }

    async fn delete_repository(
        &self,
        request: tonic::Request<DeleteRepositoryRequest>,
    ) -> Result<tonic::Response<DeleteRepositoryResponse>, tonic::Status> {
        let req = request.into_inner();
        validate_identifier(&req.repo_id, "repo_id")?;
        let expected_project_id = match req.project_id.as_deref() {
            Some(raw) => {
                validate_identifier(raw, "project_id")?;
                Some(raw)
            }
            None => None,
        };
        info!(
            "Deleting repository: {} (expected project: {:?})",
            req.repo_id, expected_project_id
        );
        let existed = self
            .store
            .delete_repository(&req.repo_id, expected_project_id)
            .await?;
        self.metrics.record_repository_deleted(&req.repo_id);
        Ok(tonic::Response::new(DeleteRepositoryResponse { existed }))
    }

    async fn read_commit(
        &self,
        request: tonic::Request<ReadCommitRequest>,
    ) -> Result<tonic::Response<ReadCommitResponse>, tonic::Status> {
        let req = request.into_inner();
        validate_identifier(&req.project_id, "project_id")?;
        validate_digest_len(&req.commit_id, cc_common::COMMIT_ID_LENGTH, "commit_id")?;
        let commit = match self.store.get_commit(&req.project_id, &req.commit_id).await {
            Ok(commit) => commit,
            Err(StoreError::ObjectNotFound { .. })
                if req.commit_id.as_slice() == cc_common::ROOT_COMMIT_ID_BYTES =>
            {
                Commit {
                    commit_id: cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
                    change_id: cc_common::ROOT_CHANGE_ID_BYTES.to_vec(),
                    parent_commit_ids: Vec::new(),
                    root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
                    description: String::new(),
                    author: Some(Signature {
                        name: String::new(),
                        email: String::new(),
                        timestamp: Some(Timestamp {
                            millis_since_epoch: 0,
                            tz_offset: 0,
                        }),
                    }),
                    committer: Some(Signature {
                        name: String::new(),
                        email: String::new(),
                        timestamp: Some(Timestamp {
                            millis_since_epoch: 0,
                            tz_offset: 0,
                        }),
                    }),
                    predecessors: Vec::new(),
                    conflict_labels: Vec::new(),
                    secure_sig: None,
                    raw_git_commit: Vec::new(),
                    external_ids: Default::default(),
                }
            }
            Err(e) => return Err(e.into()),
        };
        Ok(tonic::Response::new(ReadCommitResponse {
            commit: Some(commit),
        }))
    }

    async fn write_commit(
        &self,
        request: tonic::Request<WriteCommitRequest>,
    ) -> Result<tonic::Response<WriteCommitResponse>, tonic::Status> {
        let req = request.into_inner();
        validate_identifier(&req.project_id, "project_id")?;
        let mut commit = req.commit.ok_or_else(|| {
            tonic::Status::invalid_argument("request should have contained commit data")
        })?;
        if commit.parent_commit_ids.len() > 1
            && commit
                .parent_commit_ids
                .iter()
                .any(|p| p.as_slice() == cc_common::ROOT_COMMIT_ID_BYTES)
        {
            return Err(tonic::Status::invalid_argument(
                "The Commit Cloud backend does not support creating merge commits with the root commit as one of the parents.",
            ));
        }

        let commit_id = verify_or_compute_git_commit_hash(&mut commit)
            .map_err(tonic::Status::invalid_argument)?;
        commit.commit_id = commit_id.clone();
        info!(
            "Writing commit {} for project {}",
            hex::encode(&commit_id),
            req.project_id
        );

        self.store
            .put_commit(&req.project_id, &commit_id, &commit)
            .await?;
        self.metrics.record_commit();
        Ok(tonic::Response::new(WriteCommitResponse { commit_id }))
    }

    async fn list_commits(
        &self,
        request: tonic::Request<ListCommitsRequest>,
    ) -> Result<tonic::Response<ListCommitsResponse>, tonic::Status> {
        let req = request.into_inner();
        validate_identifier(&req.project_id, "project_id")?;
        let (commit_ids, next_page_token, sync_token) = self
            .store
            .list_commits_with_sync(
                &req.project_id,
                &req.page_token,
                req.page_size,
                &req.since_token,
            )
            .await?;
        Ok(tonic::Response::new(ListCommitsResponse {
            commit_ids,
            next_page_token,
            sync_token,
        }))
    }

    async fn resolve_commit_prefix(
        &self,
        request: tonic::Request<ResolveCommitPrefixRequest>,
    ) -> Result<tonic::Response<ResolveCommitPrefixResponse>, tonic::Status> {
        let req = request.into_inner();
        validate_identifier(&req.project_id, "project_id")?;
        let resolution = self
            .store
            .resolve_commit_prefix(&req.project_id, &req.hex_prefix)
            .await?;
        let (status, matched_commit_id) = match resolution {
            IdPrefixResolution::NoMatch => (ObjectPrefixStatus::ObjectPrefixNoMatch, Vec::new()),
            IdPrefixResolution::SingleMatch(id) => {
                (ObjectPrefixStatus::ObjectPrefixSingleMatch, id)
            }
            IdPrefixResolution::AmbiguousMatch => {
                (ObjectPrefixStatus::ObjectPrefixAmbiguousMatch, Vec::new())
            }
        };
        Ok(tonic::Response::new(ResolveCommitPrefixResponse {
            status: status as i32,
            matched_commit_id,
        }))
    }

    async fn resolve_change_prefix(
        &self,
        request: tonic::Request<ResolveChangePrefixRequest>,
    ) -> Result<tonic::Response<ResolveChangePrefixResponse>, tonic::Status> {
        const DEFAULT_CHANGE_PREFIX_CAP: usize = 256;
        const MAX_CHANGE_PREFIX_CAP: usize = 1024;

        let req = request.into_inner();
        validate_identifier(&req.project_id, "project_id")?;
        if req.repo_id.is_some() || !req.visible_head_ids.is_empty() {
            return Err(tonic::Status::unimplemented(
                "repo_id / visible_head_ids scoping on ResolveChangePrefix is not implemented yet",
            ));
        }
        let cap = if req.max_results == 0 {
            DEFAULT_CHANGE_PREFIX_CAP
        } else {
            (req.max_results as usize).min(MAX_CHANGE_PREFIX_CAP)
        };
        let resolution = self
            .store
            .resolve_change_prefix(&req.project_id, &req.prefix, cap.saturating_add(1))
            .await?;
        let (status, matched_change_id, commit_ids, truncated) = match resolution {
            ChangePrefixResolution::NoMatch => (
                ObjectPrefixStatus::ObjectPrefixNoMatch,
                Vec::new(),
                Vec::new(),
                false,
            ),
            ChangePrefixResolution::SingleMatch {
                change_id,
                mut commit_ids,
            } => {
                let was_truncated = commit_ids.len() > cap;
                commit_ids.truncate(cap);
                (
                    ObjectPrefixStatus::ObjectPrefixSingleMatch,
                    change_id,
                    commit_ids,
                    was_truncated,
                )
            }
            ChangePrefixResolution::AmbiguousMatch => (
                ObjectPrefixStatus::ObjectPrefixAmbiguousMatch,
                Vec::new(),
                Vec::new(),
                false,
            ),
        };
        Ok(tonic::Response::new(ResolveChangePrefixResponse {
            status: status as i32,
            matched_change_id,
            commit_ids,
            truncated,
        }))
    }

    async fn read_tree(
        &self,
        request: tonic::Request<ReadTreeRequest>,
    ) -> Result<tonic::Response<ReadTreeResponse>, tonic::Status> {
        let req = request.into_inner();
        validate_identifier(&req.project_id, "project_id")?;
        validate_digest_len(&req.tree_id, cc_common::TREE_ID_LENGTH, "tree_id")?;
        let (entries, next_page_token) = self
            .store
            .get_tree_page(
                &req.project_id,
                &req.tree_id,
                &req.page_token,
                req.page_size,
            )
            .await?;
        Ok(tonic::Response::new(ReadTreeResponse {
            tree_id: req.tree_id,
            entries,
            next_page_token,
        }))
    }

    async fn write_tree(
        &self,
        request: tonic::Request<WriteTreeRequest>,
    ) -> Result<tonic::Response<WriteTreeResponse>, tonic::Status> {
        let req = request.into_inner();
        validate_identifier(&req.project_id, "project_id")?;
        let tree_id =
            compute_git_tree_hash(&req.entries).map_err(tonic::Status::invalid_argument)?;
        self.store
            .put_tree(&req.project_id, &tree_id, &req.entries)
            .await?;
        Ok(tonic::Response::new(WriteTreeResponse { tree_id }))
    }

    type ReadFileStream =
        tokio_stream::wrappers::ReceiverStream<Result<ReadFileResponse, tonic::Status>>;

    async fn read_file(
        &self,
        request: tonic::Request<ReadFileRequest>,
    ) -> Result<tonic::Response<Self::ReadFileStream>, tonic::Status> {
        let req = request.into_inner();
        validate_identifier(&req.project_id, "project_id")?;
        validate_digest_len(&req.file_id, cc_common::FILE_ID_LENGTH, "file_id")?;
        let max_bytes = (req.max_bytes > 0).then_some(req.max_bytes);
        let mut chunk_rx = self
            .store
            .get_file_range_stream(&req.project_id, &req.file_id, req.offset, max_bytes)
            .await?;

        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move {
            while let Some(item) = chunk_rx.recv().await {
                let msg = match item {
                    Ok(chunk) => Ok(ReadFileResponse {
                        chunk: Vec::from(chunk),
                    }),
                    Err(err) => Err(tonic::Status::from(err)),
                };
                if tx.send(msg).await.is_err() {
                    break;
                }
            }
        });

        Ok(tonic::Response::new(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        ))
    }

    async fn write_file(
        &self,
        request: tonic::Request<WriteFileRequest>,
    ) -> Result<tonic::Response<WriteFileResponse>, tonic::Status> {
        let req = request.into_inner();
        validate_identifier(&req.project_id, "project_id")?;
        let file_id =
            try_compute_git_blob_hash(&req.content).map_err(tonic::Status::invalid_argument)?;
        self.store
            .put_file(&req.project_id, &file_id, Bytes::from(req.content))
            .await?;
        Ok(tonic::Response::new(WriteFileResponse { file_id }))
    }

    async fn write_file_stream(
        &self,
        request: tonic::Request<tonic::Streaming<WriteFileChunk>>,
    ) -> Result<tonic::Response<WriteFileResponse>, tonic::Status> {
        const MAX_STREAM_FILE_BYTES: u64 = 512 * 1024 * 1024;
        let mut stream = request.into_inner();
        let mut project_id: Option<String> = None;
        let mut declared_total_size: Option<u64> = None;
        let mut expected_file_id: Option<Vec<u8>> = None;
        let mut streaming_hasher: Option<gix::hash::Hasher> = None;
        let mut total_bytes: u64 = 0;
        let mut staging = StreamStaging::Memory {
            buf: BytesMut::new(),
            permit: None,
        };

        while let Some(frame) = stream.next().await {
            let frame = frame?;
            match &project_id {
                None => {
                    validate_identifier(&frame.project_id, "project_id")?;
                    if let Some(total) = frame.total_size {
                        if total > MAX_STREAM_FILE_BYTES {
                            return Err(tonic::Status::resource_exhausted(format!(
                                "WriteFileStream total_size {total} exceeds maximum allowed size of {MAX_STREAM_FILE_BYTES} bytes"
                            )));
                        }
                        let mut h = gix::hash::hasher(gix::hash::Kind::Sha1);
                        h.update(format!("blob {total}\0").as_bytes());
                        streaming_hasher = Some(h);
                        declared_total_size = Some(total);
                    }
                    if let Some(exp_id) = frame.expected_file_id {
                        if exp_id.len() != cc_common::FILE_ID_LENGTH {
                            return Err(tonic::Status::invalid_argument(format!(
                                "WriteFileChunk expected_file_id must be {} bytes (got {} bytes)",
                                cc_common::FILE_ID_LENGTH,
                                exp_id.len()
                            )));
                        }
                        expected_file_id = Some(exp_id);
                    }
                    self.store
                        .resolve_project_namespace(&frame.project_id, "writing files")
                        .await?;
                    project_id = Some(frame.project_id);
                    if declared_total_size
                        .is_some_and(|total| total > self.write_stream_spill_threshold as u64)
                    {
                        let (named, writer) = create_spill_file().await?;
                        staging = StreamStaging::Disk { named, writer };
                    }
                }
                Some(expected) => {
                    if !frame.project_id.is_empty() && &frame.project_id != expected {
                        return Err(tonic::Status::invalid_argument(
                            "WriteFileChunk project_id changed mid-stream",
                        ));
                    }
                    if frame.total_size.is_some() || frame.expected_file_id.is_some() {
                        return Err(tonic::Status::invalid_argument(
                            "WriteFileChunk total_size and expected_file_id may only be set on the first chunk",
                        ));
                    }
                    if frame.chunk.is_empty() {
                        return Err(tonic::Status::invalid_argument(
                            "WriteFileChunk chunk must not be empty after the first frame",
                        ));
                    }
                }
            }
            let next_len = total_bytes.saturating_add(frame.chunk.len() as u64);
            if next_len > MAX_STREAM_FILE_BYTES {
                return Err(tonic::Status::resource_exhausted(format!(
                    "WriteFileStream exceeds maximum allowed size of {MAX_STREAM_FILE_BYTES} bytes"
                )));
            }
            if let Some(total) = declared_total_size
                && next_len > total
            {
                return Err(tonic::Status::invalid_argument(format!(
                    "WriteFileStream received {next_len} bytes, exceeding declared total_size {total}"
                )));
            }
            if let Some(ref mut h) = streaming_hasher {
                h.update(&frame.chunk);
            }
            if !frame.chunk.is_empty() {
                match &mut staging {
                    StreamStaging::Memory { buf, permit } => {
                        let fits_threshold = buf.len().saturating_add(frame.chunk.len())
                            <= self.write_stream_spill_threshold;
                        let acquired = if fits_threshold {
                            self.store
                                .try_acquire_stream_memory_permit(frame.chunk.len())
                        } else {
                            None
                        };
                        if let Some(p) = acquired {
                            match permit {
                                Some(existing) => existing.merge(p),
                                None => *permit = Some(p),
                            }
                            buf.extend_from_slice(&frame.chunk);
                        } else {
                            let (named, mut writer) = create_spill_file().await?;
                            if !buf.is_empty() {
                                writer.write_all(buf).await.map_err(|e| {
                                    tonic::Status::internal(format!(
                                        "failed to spill upload buffer to disk: {e}"
                                    ))
                                })?;
                            }
                            writer.write_all(&frame.chunk).await.map_err(|e| {
                                tonic::Status::internal(format!(
                                    "failed to write upload chunk to spill file: {e}"
                                ))
                            })?;
                            // Replacing `staging` drops both `buf` and the held memory permit
                            // immediately so memory returns to the global budget.
                            staging = StreamStaging::Disk { writer, named };
                        }
                    }
                    StreamStaging::Disk { writer, .. } => {
                        writer.write_all(&frame.chunk).await.map_err(|e| {
                            tonic::Status::internal(format!(
                                "failed to write upload chunk to spill file: {e}"
                            ))
                        })?;
                    }
                }
            }
            total_bytes = next_len;
        }

        let project_id = project_id.ok_or_else(|| {
            tonic::Status::invalid_argument("WriteFileStream requires at least one chunk frame")
        })?;
        if let Some(total) = declared_total_size
            && total_bytes != total
        {
            return Err(tonic::Status::invalid_argument(format!(
                "WriteFileStream truncated: declared total_size {total} bytes, received {total_bytes} bytes"
            )));
        }

        match staging {
            StreamStaging::Memory { buf, permit } => {
                let content = buf.freeze();
                let file_id = match streaming_hasher {
                    Some(h) => h
                        .try_finalize()
                        .map_err(|e| {
                            tonic::Status::invalid_argument(format!(
                                "SHA-1 digest computation error: {e}"
                            ))
                        })?
                        .as_bytes()
                        .to_vec(),
                    None => try_compute_git_blob_hash(&content)
                        .map_err(tonic::Status::invalid_argument)?,
                };
                if let Some(exp_id) = expected_file_id
                    && exp_id != file_id
                {
                    return Err(tonic::Status::invalid_argument(format!(
                        "WriteFileStream expected_file_id {} did not match computed {}",
                        hex::encode(exp_id),
                        hex::encode(&file_id)
                    )));
                }
                self.store.put_file(&project_id, &file_id, content).await?;
                drop(permit);
                Ok(tonic::Response::new(WriteFileResponse { file_id }))
            }
            StreamStaging::Disk { mut writer, named } => {
                writer.flush().await.map_err(|e| {
                    tonic::Status::internal(format!("failed to flush upload spill file: {e}"))
                })?;
                drop(writer);
                let file_id = match streaming_hasher {
                    Some(h) => h
                        .try_finalize()
                        .map_err(|e| {
                            tonic::Status::invalid_argument(format!(
                                "SHA-1 digest computation error: {e}"
                            ))
                        })?
                        .as_bytes()
                        .to_vec(),
                    None => {
                        compute_git_blob_hash_from_file(named.path().to_path_buf(), total_bytes)
                            .await?
                    }
                };
                if let Some(exp_id) = expected_file_id
                    && exp_id != file_id
                {
                    return Err(tonic::Status::invalid_argument(format!(
                        "WriteFileStream expected_file_id {} did not match computed {}",
                        hex::encode(exp_id),
                        hex::encode(&file_id)
                    )));
                }
                self.store
                    .put_file_from_temp_file(&project_id, &file_id, named.path(), total_bytes)
                    .await?;
                Ok(tonic::Response::new(WriteFileResponse { file_id }))
            }
        }
    }

    async fn read_symlink(
        &self,
        request: tonic::Request<ReadSymlinkRequest>,
    ) -> Result<tonic::Response<ReadSymlinkResponse>, tonic::Status> {
        let req = request.into_inner();
        validate_identifier(&req.project_id, "project_id")?;
        validate_digest_len(&req.symlink_id, cc_common::SYMLINK_ID_LENGTH, "symlink_id")?;
        let target = self
            .store
            .get_symlink(&req.project_id, &req.symlink_id)
            .await?;
        Ok(tonic::Response::new(ReadSymlinkResponse { target }))
    }

    async fn write_symlink(
        &self,
        request: tonic::Request<WriteSymlinkRequest>,
    ) -> Result<tonic::Response<WriteSymlinkResponse>, tonic::Status> {
        let req = request.into_inner();
        validate_identifier(&req.project_id, "project_id")?;
        if req.target.as_bytes().contains(&0) {
            return Err(tonic::Status::invalid_argument(
                "symlink target must not contain NUL bytes",
            ));
        }
        let symlink_id = try_compute_git_blob_hash(req.target.as_bytes())
            .map_err(tonic::Status::invalid_argument)?;
        self.store
            .put_symlink(&req.project_id, &symlink_id, &req.target)
            .await?;
        Ok(tonic::Response::new(WriteSymlinkResponse { symlink_id }))
    }
}

#[cfg(test)]
mod tests;
