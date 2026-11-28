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

//! `WorkspaceService` gRPC pass-through proxy implementation.
//!
//! # Concurrency and Merge Invariants
//! All workspace state is mutable and authoritative on jj-cc-server, so every
//! `WorkspaceService` RPC validates request metadata and forwards directly to upstream
//! without local caching.

use cc_common::MAX_GRPC_MESSAGE_BYTES;
use cc_common::workspace::workspace_service_client::WorkspaceServiceClient;
use cc_common::workspace::workspace_service_server::WorkspaceService;
use cc_common::workspace::*;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use super::DaemonProxy;

fn workspace_client(channel: Channel) -> WorkspaceServiceClient<Channel> {
    WorkspaceServiceClient::new(channel)
        .max_decoding_message_size(MAX_GRPC_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_GRPC_MESSAGE_BYTES)
}

#[tonic::async_trait]
impl WorkspaceService for DaemonProxy {
    async fn get_workspace(
        &self,
        request: Request<GetWorkspaceRequest>,
    ) -> Result<Response<GetWorkspaceResponse>, Status> {
        let _active = self.begin_rpc(request.metadata())?;
        let meta = request.metadata().clone();
        let out = self
            .inner
            .upstream
            .prepare_outbound(&meta, request.into_inner())?;
        let channel = self.inner.upstream.channel_for(&meta).await?;
        self.inner.cache.record_upstream_rpc();
        let mut client = workspace_client(channel);
        let res = client.get_workspace(out).await?.into_inner();
        self.maybe_flush_stats();
        Ok(Response::new(res))
    }

    async fn update_workspace(
        &self,
        request: Request<UpdateWorkspaceRequest>,
    ) -> Result<Response<UpdateWorkspaceResponse>, Status> {
        let _active = self.begin_rpc(request.metadata())?;
        let meta = request.metadata().clone();
        let out = self
            .inner
            .upstream
            .prepare_outbound(&meta, request.into_inner())?;
        let channel = self.inner.upstream.channel_for(&meta).await?;
        self.inner.cache.record_upstream_rpc();
        let mut client = workspace_client(channel);
        let res = client.update_workspace(out).await?.into_inner();
        self.maybe_flush_stats();
        Ok(Response::new(res))
    }

    async fn list_workspaces(
        &self,
        request: Request<ListWorkspacesRequest>,
    ) -> Result<Response<ListWorkspacesResponse>, Status> {
        let _active = self.begin_rpc(request.metadata())?;
        let meta = request.metadata().clone();
        let out = self
            .inner
            .upstream
            .prepare_outbound(&meta, request.into_inner())?;
        let channel = self.inner.upstream.channel_for(&meta).await?;
        self.inner.cache.record_upstream_rpc();
        let mut client = workspace_client(channel);
        let res = client.list_workspaces(out).await?.into_inner();
        self.maybe_flush_stats();
        Ok(Response::new(res))
    }

    async fn check_working_copy_changes(
        &self,
        request: Request<CheckWorkingCopyChangesRequest>,
    ) -> Result<Response<CheckWorkingCopyChangesResponse>, Status> {
        let _active = self.begin_rpc(request.metadata())?;
        let meta = request.metadata().clone();
        let out = self
            .inner
            .upstream
            .prepare_outbound(&meta, request.into_inner())?;
        let channel = self.inner.upstream.channel_for(&meta).await?;
        self.inner.cache.record_upstream_rpc();
        let mut client = workspace_client(channel);
        let res = client.check_working_copy_changes(out).await?.into_inner();
        self.maybe_flush_stats();
        Ok(Response::new(res))
    }

    async fn delete_workspace(
        &self,
        request: Request<DeleteWorkspaceRequest>,
    ) -> Result<Response<DeleteWorkspaceResponse>, Status> {
        let _active = self.begin_rpc(request.metadata())?;
        let meta = request.metadata().clone();
        let out = self
            .inner
            .upstream
            .prepare_outbound(&meta, request.into_inner())?;
        let channel = self.inner.upstream.channel_for(&meta).await?;
        self.inner.cache.record_upstream_rpc();
        let mut client = workspace_client(channel);
        let res = client.delete_workspace(out).await?.into_inner();
        self.maybe_flush_stats();
        Ok(Response::new(res))
    }
}
