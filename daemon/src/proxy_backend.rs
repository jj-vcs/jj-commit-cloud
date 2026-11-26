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

use crate::connection::RemoteConnectionManager;
use cc_common::backend::backend_service_server::BackendService;
use cc_common::backend::*;
use tonic::{Request, Response, Status, Streaming};

#[derive(Clone)]
pub struct DaemonBackendProxy {
    conn_manager: RemoteConnectionManager,
}

impl DaemonBackendProxy {
    pub fn new(conn_manager: RemoteConnectionManager) -> Self {
        Self { conn_manager }
    }
}

#[tonic::async_trait]
impl BackendService for DaemonBackendProxy {
    async fn register_repository(
        &self,
        request: Request<RegisterRepositoryRequest>,
    ) -> Result<Response<RegisterRepositoryResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.register_repository(request).await })
            .await
    }

    async fn read_commit(
        &self,
        request: Request<ReadCommitRequest>,
    ) -> Result<Response<ReadCommitResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.read_commit(request).await })
            .await
    }

    async fn write_commit(
        &self,
        request: Request<WriteCommitRequest>,
    ) -> Result<Response<WriteCommitResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.write_commit(request).await })
            .await
    }

    async fn read_tree(
        &self,
        request: Request<ReadTreeRequest>,
    ) -> Result<Response<ReadTreeResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.read_tree(request).await })
            .await
    }

    async fn write_tree(
        &self,
        request: Request<WriteTreeRequest>,
    ) -> Result<Response<WriteTreeResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.write_tree(request).await })
            .await
    }

    type ReadFileStream = Streaming<ReadFileResponse>;

    async fn read_file(
        &self,
        request: Request<ReadFileRequest>,
    ) -> Result<Response<Self::ReadFileStream>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.read_file(request).await })
            .await
    }

    async fn write_file(
        &self,
        request: Request<WriteFileRequest>,
    ) -> Result<Response<WriteFileResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.write_file(request).await })
            .await
    }

    async fn read_symlink(
        &self,
        request: Request<ReadSymlinkRequest>,
    ) -> Result<Response<ReadSymlinkResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.read_symlink(request).await })
            .await
    }

    async fn write_symlink(
        &self,
        request: Request<WriteSymlinkRequest>,
    ) -> Result<Response<WriteSymlinkResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.write_symlink(request).await })
            .await
    }

    async fn register_project(
        &self,
        request: Request<RegisterProjectRequest>,
    ) -> Result<Response<RegisterProjectResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.register_project(request).await })
            .await
    }

    async fn list_project_commits(
        &self,
        request: Request<ListProjectCommitsRequest>,
    ) -> Result<Response<ListProjectCommitsResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.list_project_commits(request).await })
            .await
    }
}
