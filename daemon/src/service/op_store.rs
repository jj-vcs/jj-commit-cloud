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

//! `OpStoreService` gRPC proxy implementation for views, operations, and operation heads.
//!
//! # Authority and Crash-Safety Ordering
//! Immutable `Operation` and `View` records cache per `(upstream, repo, auth_fingerprint)`
//! after protobuf structural validation (non-empty `view_id` / `head_ids` and matching
//! `view_id` on `ReadView`). `WriteOperation`, `WriteView`, and `PublishOperation` populate
//! the local cache only after jj-cc-server commits the write.
//!
//! # Concurrency and Merge Invariants
//! Mutable coordination and prefix-lookup RPCs (`GetOpHeads`, `UpdateOpHeads`,
//! `ReconcileOpHeads`, and `ResolveOperationPrefix`) forward directly to jj-cc-server
//! without caching.

use std::sync::Arc;

use bytes::Bytes;
use cc_common::op_store::op_store_service_client::OpStoreServiceClient;
use cc_common::op_store::op_store_service_server::OpStoreService;
use cc_common::op_store::*;
use cc_common::{
    MAX_GRPC_MESSAGE_BYTES, ROOT_COMMIT_ID_BYTES, ROOT_OPERATION_ID_BYTES, ROOT_VIEW_ID_BYTES,
};
use prost::Message;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use super::{DaemonProxy, validate_identifier};
use crate::cache::ObjectKind;

fn op_store_client(channel: Channel) -> OpStoreServiceClient<Channel> {
    OpStoreServiceClient::new(channel)
        .max_decoding_message_size(MAX_GRPC_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_GRPC_MESSAGE_BYTES)
}

#[tonic::async_trait]
impl OpStoreService for DaemonProxy {
    async fn read_operation(
        &self,
        request: Request<ReadOperationRequest>,
    ) -> Result<Response<ReadOperationResponse>, Status> {
        let _active = self.track_active_rpc();
        let meta = request.metadata().clone();
        let req = request.into_inner();
        let scope = self.scope_for(&meta, &req.repo_id)?;
        if req.operation_id.as_slice() == ROOT_OPERATION_ID_BYTES {
            validate_identifier(&req.repo_id, "repo_id")?;
            self.inner.cache.touch_activity();
            self.inner.cache.record_l1_hit();
            self.maybe_flush_stats();
            return Ok(Response::new(ReadOperationResponse {
                operation: Some(Operation {
                    view_id: ROOT_VIEW_ID_BYTES.to_vec(),
                    parents: Vec::new(),
                    metadata: Some(OperationMetadata {
                        description: "root()".to_string(),
                        ..Default::default()
                    }),
                    commit_predecessors: Vec::new(),
                    commit_predecessors_set: true,
                }),
            }));
        }
        let op_id = req.operation_id.clone();
        let upstream = Arc::clone(&self.inner.upstream);
        let cache_for_fetch = Arc::clone(&self.inner.cache);
        let raw = self
            .inner
            .cache
            .get_or_fetch(&scope, ObjectKind::Operation, &op_id, || async move {
                let out = upstream.prepare_coalesced_outbound(&meta, req)?;
                let channel = upstream.channel_for(&meta).await?;
                cache_for_fetch.record_upstream_rpc();
                let mut client = op_store_client(channel);
                let resp = client.read_operation(out).await?.into_inner();
                let op = resp
                    .operation
                    .ok_or_else(|| Status::not_found("upstream returned empty operation"))?;
                Ok(Bytes::from(op.encode_to_vec()))
            })
            .await?;
        let decoded = Operation::decode(raw)
            .map_err(|e| Status::internal(format!("cached operation decode failed: {e}")))?;
        self.maybe_flush_stats();
        Ok(Response::new(ReadOperationResponse {
            operation: Some(decoded),
        }))
    }

    async fn write_operation(
        &self,
        request: Request<WriteOperationRequest>,
    ) -> Result<Response<WriteOperationResponse>, Status> {
        let _active = self.track_active_rpc();
        let meta = request.metadata().clone();
        let req = request.into_inner();
        let scope = self.scope_for(&meta, &req.repo_id)?;
        let cache_epoch = self.inner.cache.purge_epoch();
        let op_clone = req.operation.clone();
        let out = self.inner.upstream.prepare_outbound(&meta, req)?;
        let channel = self.inner.upstream.channel_for(&meta).await?;
        self.inner.cache.record_upstream_rpc();
        let mut client = op_store_client(channel);
        let res = client.write_operation(out).await?.into_inner();
        if let Some(op) = op_clone {
            self.inner
                .cache
                .put_write_through_if_epoch_async(
                    scope,
                    ObjectKind::Operation,
                    res.operation_id.clone(),
                    Bytes::from(op.encode_to_vec()),
                    Some(cache_epoch),
                )
                .await;
        }
        self.maybe_flush_stats();
        Ok(Response::new(res))
    }

    async fn resolve_operation_prefix(
        &self,
        request: Request<ResolveOperationPrefixRequest>,
    ) -> Result<Response<ResolveOperationPrefixResponse>, Status> {
        let _active = self.track_active_rpc();
        let meta = request.metadata().clone();
        let out = self
            .inner
            .upstream
            .prepare_outbound(&meta, request.into_inner())?;
        let channel = self.inner.upstream.channel_for(&meta).await?;
        self.inner.cache.record_upstream_rpc();
        let mut client = op_store_client(channel);
        let res = client.resolve_operation_prefix(out).await?.into_inner();
        self.maybe_flush_stats();
        Ok(Response::new(res))
    }

    async fn read_view(
        &self,
        request: Request<ReadViewRequest>,
    ) -> Result<Response<ReadViewResponse>, Status> {
        let _active = self.track_active_rpc();
        let meta = request.metadata().clone();
        let req = request.into_inner();
        let scope = self.scope_for(&meta, &req.repo_id)?;
        if req.view_id.as_slice() == ROOT_VIEW_ID_BYTES {
            validate_identifier(&req.repo_id, "repo_id")?;
            self.inner.cache.touch_activity();
            self.inner.cache.record_l1_hit();
            self.maybe_flush_stats();
            return Ok(Response::new(ReadViewResponse {
                view_id: req.view_id,
                view: Some(View {
                    head_ids: vec![ROOT_COMMIT_ID_BYTES.to_vec()],
                    ..Default::default()
                }),
            }));
        }
        let view_id = req.view_id.clone();
        let expected_view_id = view_id.clone();
        let upstream = Arc::clone(&self.inner.upstream);
        let cache_for_fetch = Arc::clone(&self.inner.cache);
        let raw = self
            .inner
            .cache
            .get_or_fetch(&scope, ObjectKind::View, &view_id, || async move {
                let out = upstream.prepare_coalesced_outbound(&meta, req)?;
                let channel = upstream.channel_for(&meta).await?;
                cache_for_fetch.record_upstream_rpc();
                let mut client = op_store_client(channel);
                let resp = client.read_view(out).await?.into_inner();
                if !resp.view_id.is_empty() && resp.view_id != expected_view_id {
                    return Err(Status::internal(format!(
                        "upstream ReadView returned mismatched view_id {} (expected {})",
                        hex::encode(&resp.view_id),
                        hex::encode(&expected_view_id),
                    )));
                }
                let view = resp
                    .view
                    .ok_or_else(|| Status::not_found("upstream returned empty view"))?;
                Ok(Bytes::from(view.encode_to_vec()))
            })
            .await?;
        let decoded = View::decode(raw)
            .map_err(|e| Status::internal(format!("cached view decode failed: {e}")))?;
        self.maybe_flush_stats();
        Ok(Response::new(ReadViewResponse {
            view_id,
            view: Some(decoded),
        }))
    }

    async fn write_view(
        &self,
        request: Request<WriteViewRequest>,
    ) -> Result<Response<WriteViewResponse>, Status> {
        let _active = self.track_active_rpc();
        let meta = request.metadata().clone();
        let req = request.into_inner();
        let scope = self.scope_for(&meta, &req.repo_id)?;
        let cache_epoch = self.inner.cache.purge_epoch();
        let view_clone = req.view.clone();
        let out = self.inner.upstream.prepare_outbound(&meta, req)?;
        let channel = self.inner.upstream.channel_for(&meta).await?;
        self.inner.cache.record_upstream_rpc();
        let mut client = op_store_client(channel);
        let res = client.write_view(out).await?.into_inner();
        if let Some(v) = view_clone {
            self.inner
                .cache
                .put_write_through_if_epoch_async(
                    scope,
                    ObjectKind::View,
                    res.view_id.clone(),
                    Bytes::from(v.encode_to_vec()),
                    Some(cache_epoch),
                )
                .await;
        }
        self.maybe_flush_stats();
        Ok(Response::new(res))
    }

    async fn get_op_heads(
        &self,
        request: Request<GetOpHeadsRequest>,
    ) -> Result<Response<GetOpHeadsResponse>, Status> {
        let _active = self.track_active_rpc();
        let meta = request.metadata().clone();
        let out = self
            .inner
            .upstream
            .prepare_outbound(&meta, request.into_inner())?;
        let channel = self.inner.upstream.channel_for(&meta).await?;
        self.inner.cache.record_upstream_rpc();
        let mut client = op_store_client(channel);
        let res = client.get_op_heads(out).await?.into_inner();
        self.maybe_flush_stats();
        Ok(Response::new(res))
    }

    async fn update_op_heads(
        &self,
        request: Request<UpdateOpHeadsRequest>,
    ) -> Result<Response<UpdateOpHeadsResponse>, Status> {
        let _active = self.track_active_rpc();
        let meta = request.metadata().clone();
        let out = self
            .inner
            .upstream
            .prepare_outbound(&meta, request.into_inner())?;
        let channel = self.inner.upstream.channel_for(&meta).await?;
        self.inner.cache.record_upstream_rpc();
        let mut client = op_store_client(channel);
        let res = client.update_op_heads(out).await?.into_inner();
        self.maybe_flush_stats();
        Ok(Response::new(res))
    }

    async fn reconcile_op_heads(
        &self,
        request: Request<ReconcileOpHeadsRequest>,
    ) -> Result<Response<ReconcileOpHeadsResponse>, Status> {
        let _active = self.track_active_rpc();
        let meta = request.metadata().clone();
        let out = self
            .inner
            .upstream
            .prepare_outbound(&meta, request.into_inner())?;
        let channel = self.inner.upstream.channel_for(&meta).await?;
        self.inner.cache.record_upstream_rpc();
        let mut client = op_store_client(channel);
        let res = client.reconcile_op_heads(out).await?.into_inner();
        self.maybe_flush_stats();
        Ok(Response::new(res))
    }

    async fn publish_operation(
        &self,
        request: Request<PublishOperationRequest>,
    ) -> Result<Response<PublishOperationResponse>, Status> {
        let _active = self.track_active_rpc();
        let meta = request.metadata().clone();
        let req = request.into_inner();
        let scope = self.scope_for(&meta, &req.repo_id)?;
        let cache_epoch = self.inner.cache.purge_epoch();
        let view_clone = req.view.clone();
        let op_clone = req.operation.clone();
        let out = self.inner.upstream.prepare_outbound(&meta, req)?;
        let channel = self.inner.upstream.channel_for(&meta).await?;
        self.inner.cache.record_upstream_rpc();
        let mut client = op_store_client(channel);
        let res = client.publish_operation(out).await?.into_inner();
        if let Some(v) = view_clone {
            self.inner
                .cache
                .put_write_through_if_epoch_async(
                    scope.clone(),
                    ObjectKind::View,
                    res.view_id.clone(),
                    Bytes::from(v.encode_to_vec()),
                    Some(cache_epoch),
                )
                .await;
        }
        if let Some(mut op) = op_clone {
            op.view_id = res.view_id.clone();
            self.inner
                .cache
                .put_write_through_if_epoch_async(
                    scope,
                    ObjectKind::Operation,
                    res.operation_id.clone(),
                    Bytes::from(op.encode_to_vec()),
                    Some(cache_epoch),
                )
                .await;
        }
        self.maybe_flush_stats();
        Ok(Response::new(res))
    }
}
