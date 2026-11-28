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

//! `BackendService` gRPC proxy implementation for projects, repositories, commits, symlinks,
//! trees, and files.
//!
//! # Authority and Crash-Safety Ordering
//! `WriteCommit` forwards without write-through so `ReadCommit` always caches the server's
//! authoritative commit record, while `WriteSymlink` writes through only after upstream
//! succeeds. Mutable and listing RPCs (`RegisterProject`, `RegisterRepository`, `ListCommits`,
//! `ResolveCommitPrefix`, and `ResolveChangePrefix`) forward directly to upstream without
//! local caching.
//!
//! # Cross-Process and RPC Boundaries
//! Paginated tree and streaming file handlers live in sibling modules `backend_tree` and
//! `backend_file` and execute through the same `DaemonProxy` instance.

use std::sync::Arc;

use bytes::Bytes;
use cc_common::backend::backend_service_server::BackendService;
use cc_common::backend::*;
use prost::Message;
use tonic::{Request, Response, Status, Streaming};

use super::{DaemonProxy, backend_client};
use crate::cache::ObjectKind;

pub(super) fn decode_symlink_target(raw: &[u8]) -> Result<String, Status> {
    if raw.contains(&0) {
        return Err(Status::data_loss("symlink target contains NUL byte"));
    }
    String::from_utf8(raw.to_vec())
        .map_err(|e| Status::data_loss(format!("symlink target is not valid UTF-8: {e}")))
}

#[tonic::async_trait]
impl BackendService for DaemonProxy {
    async fn register_project(
        &self,
        request: Request<RegisterProjectRequest>,
    ) -> Result<Response<RegisterProjectResponse>, Status> {
        let _active = self.track_active_rpc();
        let meta = request.metadata().clone();
        let out = self
            .inner
            .upstream
            .prepare_outbound(&meta, request.into_inner())?;
        let channel = self.inner.upstream.channel_for(&meta).await?;
        self.inner.cache.record_upstream_rpc();
        let mut client = backend_client(channel);
        let res = client.register_project(out).await?.into_inner();
        self.maybe_flush_stats();
        Ok(Response::new(res))
    }

    async fn register_repository(
        &self,
        request: Request<RegisterRepositoryRequest>,
    ) -> Result<Response<RegisterRepositoryResponse>, Status> {
        let _active = self.track_active_rpc();
        let meta = request.metadata().clone();
        let out = self
            .inner
            .upstream
            .prepare_outbound(&meta, request.into_inner())?;
        let channel = self.inner.upstream.channel_for(&meta).await?;
        self.inner.cache.record_upstream_rpc();
        let mut client = backend_client(channel);
        let res = client.register_repository(out).await?.into_inner();
        self.maybe_flush_stats();
        Ok(Response::new(res))
    }

    async fn read_commit(
        &self,
        request: Request<ReadCommitRequest>,
    ) -> Result<Response<ReadCommitResponse>, Status> {
        let _active = self.track_active_rpc();
        let meta = request.metadata().clone();
        let req = request.into_inner();
        let scope = self.scope_for(&meta, &req.project_id)?;
        let commit_id = req.commit_id.clone();
        let expected_commit_id = commit_id.clone();
        let upstream = Arc::clone(&self.inner.upstream);
        let cache_for_fetch = Arc::clone(&self.inner.cache);
        let raw = self
            .inner
            .cache
            .get_or_fetch(&scope, ObjectKind::Commit, &commit_id, || async move {
                let out = upstream.prepare_coalesced_outbound(&meta, req)?;
                let channel = upstream.channel_for(&meta).await?;
                cache_for_fetch.record_upstream_rpc();
                let mut client = backend_client(channel);
                let resp = client.read_commit(out).await?.into_inner();
                let commit = resp
                    .commit
                    .ok_or_else(|| Status::not_found("upstream returned empty commit"))?;
                if !commit.commit_id.is_empty() && commit.commit_id != expected_commit_id {
                    return Err(Status::internal(format!(
                        "upstream ReadCommit returned mismatched commit_id {} (expected {})",
                        hex::encode(&commit.commit_id),
                        hex::encode(&expected_commit_id),
                    )));
                }
                Ok(Bytes::from(commit.encode_to_vec()))
            })
            .await?;
        let decoded = Commit::decode(raw)
            .map_err(|e| Status::internal(format!("cached commit decode failed: {e}")))?;
        self.maybe_flush_stats();
        Ok(Response::new(ReadCommitResponse {
            commit: Some(decoded),
        }))
    }

    async fn write_commit(
        &self,
        request: Request<WriteCommitRequest>,
    ) -> Result<Response<WriteCommitResponse>, Status> {
        let _active = self.track_active_rpc();
        let meta = request.metadata().clone();
        let req = request.into_inner();
        let out = self.inner.upstream.prepare_outbound(&meta, req)?;
        let channel = self.inner.upstream.channel_for(&meta).await?;
        self.inner.cache.record_upstream_rpc();
        let mut client = backend_client(channel);
        // Do not write-through `Commit` payloads on `WriteCommit`: `predecessors` and
        // `external_ids` are excluded from `commit_id` under first-writer-wins server semantics
        // and `WriteCommitResponse` does not echo back the winning commit. Let the first
        // `ReadCommit` fetch and cache the server's authoritative payload.
        let res = client.write_commit(out).await?.into_inner();
        self.maybe_flush_stats();
        Ok(Response::new(res))
    }

    async fn list_commits(
        &self,
        request: Request<ListCommitsRequest>,
    ) -> Result<Response<ListCommitsResponse>, Status> {
        let _active = self.track_active_rpc();
        let meta = request.metadata().clone();
        let out = self
            .inner
            .upstream
            .prepare_outbound(&meta, request.into_inner())?;
        let channel = self.inner.upstream.channel_for(&meta).await?;
        self.inner.cache.record_upstream_rpc();
        let mut client = backend_client(channel);
        let res = client.list_commits(out).await?.into_inner();
        self.maybe_flush_stats();
        Ok(Response::new(res))
    }

    async fn resolve_commit_prefix(
        &self,
        request: Request<ResolveCommitPrefixRequest>,
    ) -> Result<Response<ResolveCommitPrefixResponse>, Status> {
        let _active = self.track_active_rpc();
        let meta = request.metadata().clone();
        let out = self
            .inner
            .upstream
            .prepare_outbound(&meta, request.into_inner())?;
        let channel = self.inner.upstream.channel_for(&meta).await?;
        self.inner.cache.record_upstream_rpc();
        let mut client = backend_client(channel);
        let res = client.resolve_commit_prefix(out).await?.into_inner();
        self.maybe_flush_stats();
        Ok(Response::new(res))
    }

    async fn resolve_change_prefix(
        &self,
        request: Request<ResolveChangePrefixRequest>,
    ) -> Result<Response<ResolveChangePrefixResponse>, Status> {
        let _active = self.track_active_rpc();
        let meta = request.metadata().clone();
        let out = self
            .inner
            .upstream
            .prepare_outbound(&meta, request.into_inner())?;
        let channel = self.inner.upstream.channel_for(&meta).await?;
        self.inner.cache.record_upstream_rpc();
        let mut client = backend_client(channel);
        let res = client.resolve_change_prefix(out).await?.into_inner();
        self.maybe_flush_stats();
        Ok(Response::new(res))
    }
    async fn read_tree(
        &self,
        request: Request<ReadTreeRequest>,
    ) -> Result<Response<ReadTreeResponse>, Status> {
        self.handle_read_tree(request).await
    }
    async fn write_tree(
        &self,
        request: Request<WriteTreeRequest>,
    ) -> Result<Response<WriteTreeResponse>, Status> {
        self.handle_write_tree(request).await
    }
    type ReadFileStream = super::backend_file::DaemonReadFileStream;
    async fn read_file(
        &self,
        request: Request<ReadFileRequest>,
    ) -> Result<Response<Self::ReadFileStream>, Status> {
        self.handle_read_file(request).await
    }
    async fn write_file(
        &self,
        request: Request<WriteFileRequest>,
    ) -> Result<Response<WriteFileResponse>, Status> {
        self.handle_write_file(request).await
    }
    async fn write_file_stream(
        &self,
        request: Request<Streaming<WriteFileChunk>>,
    ) -> Result<Response<WriteFileResponse>, Status> {
        self.handle_write_file_stream(request).await
    }

    async fn read_symlink(
        &self,
        request: Request<ReadSymlinkRequest>,
    ) -> Result<Response<ReadSymlinkResponse>, Status> {
        let _active = self.track_active_rpc();
        let meta = request.metadata().clone();
        let req = request.into_inner();
        let scope = self.scope_for(&meta, &req.project_id)?;
        let symlink_id = req.symlink_id.clone();
        let upstream = Arc::clone(&self.inner.upstream);
        let cache_for_fetch = Arc::clone(&self.inner.cache);
        let raw = self
            .inner
            .cache
            .get_or_fetch(&scope, ObjectKind::Symlink, &symlink_id, || async move {
                let out = upstream.prepare_coalesced_outbound(&meta, req)?;
                let channel = upstream.channel_for(&meta).await?;
                cache_for_fetch.record_upstream_rpc();
                let mut client = backend_client(channel);
                let resp = client.read_symlink(out).await?.into_inner();
                let bytes = resp.target.into_bytes();
                if bytes.contains(&0) {
                    return Err(Status::data_loss("symlink target contains NUL byte"));
                }
                Ok(Bytes::from(bytes))
            })
            .await?;
        let target = decode_symlink_target(&raw)?;
        self.maybe_flush_stats();
        Ok(Response::new(ReadSymlinkResponse { target }))
    }

    async fn write_symlink(
        &self,
        request: Request<WriteSymlinkRequest>,
    ) -> Result<Response<WriteSymlinkResponse>, Status> {
        let _active = self.track_active_rpc();
        let meta = request.metadata().clone();
        let req = request.into_inner();
        let scope = self.scope_for(&meta, &req.project_id)?;
        let target_bytes = Bytes::from(req.target.clone().into_bytes());
        let out = self.inner.upstream.prepare_outbound(&meta, req)?;
        let channel = self.inner.upstream.channel_for(&meta).await?;
        self.inner.cache.record_upstream_rpc();
        let mut client = backend_client(channel);
        let res = client.write_symlink(out).await?.into_inner();
        self.inner
            .cache
            .put_write_through_async(
                scope,
                ObjectKind::Symlink,
                res.symlink_id.clone(),
                target_bytes,
            )
            .await;
        self.maybe_flush_stats();
        Ok(Response::new(res))
    }
}
