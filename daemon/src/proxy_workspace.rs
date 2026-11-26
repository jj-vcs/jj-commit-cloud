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
use cc_common::workspace::workspace_service_server::WorkspaceService;
use cc_common::workspace::*;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct DaemonWorkspaceProxy {
    conn_manager: RemoteConnectionManager,
}

impl DaemonWorkspaceProxy {
    pub fn new(conn_manager: RemoteConnectionManager) -> Self {
        Self { conn_manager }
    }
}

#[tonic::async_trait]
impl WorkspaceService for DaemonWorkspaceProxy {
    async fn get_workspace(
        &self,
        request: Request<GetWorkspaceRequest>,
    ) -> Result<Response<GetWorkspaceResponse>, Status> {
        self.conn_manager
            .with_workspace(|mut c| async move { c.get_workspace(request).await })
            .await
    }

    async fn update_workspace(
        &self,
        request: Request<UpdateWorkspaceRequest>,
    ) -> Result<Response<UpdateWorkspaceResponse>, Status> {
        self.conn_manager
            .with_workspace(|mut c| async move { c.update_workspace(request).await })
            .await
    }

    async fn list_workspaces(
        &self,
        request: Request<ListWorkspacesRequest>,
    ) -> Result<Response<ListWorkspacesResponse>, Status> {
        self.conn_manager
            .with_workspace(|mut c| async move { c.list_workspaces(request).await })
            .await
    }

    async fn check_working_copy_changes(
        &self,
        request: Request<CheckWorkingCopyChangesRequest>,
    ) -> Result<Response<CheckWorkingCopyChangesResponse>, Status> {
        self.conn_manager
            .with_workspace(|mut c| async move { c.check_working_copy_changes(request).await })
            .await
    }

    async fn delete_workspace(
        &self,
        request: Request<DeleteWorkspaceRequest>,
    ) -> Result<Response<DeleteWorkspaceResponse>, Status> {
        self.conn_manager
            .with_workspace(|mut c| async move { c.delete_workspace(request).await })
            .await
    }
}
