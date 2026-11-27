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

use std::pin::Pin;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use cc_common::backend::backend_service_server::{BackendService, BackendServiceServer};
use cc_common::backend::*;
use cc_common::op_store::op_store_service_server::{OpStoreService, OpStoreServiceServer};
use cc_common::op_store::*;
use cc_common::workspace::workspace_service_server::{WorkspaceService, WorkspaceServiceServer};
use cc_common::workspace::*;
use futures::{Stream, StreamExt};
use prost::Message;
use sha1::{Digest, Sha1};
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status, Streaming};

use super::backend_tree::slice_tree_page;
use crate::upstream::UPSTREAM_HEADER;

mod backend;
mod files;
mod op_store_and_repo;
mod trees;

fn slice_file_range_frames(
    content: &Bytes,
    offset: u64,
    max_bytes: u64,
) -> Result<Vec<Result<ReadFileResponse, Status>>, Status> {
    let total = content.len() as u64;
    if offset > total {
        return Err(Status::out_of_range(format!(
            "offset {offset} exceeds file length {total}"
        )));
    }
    let start = offset as usize;
    let end = if max_bytes == 0 {
        content.len()
    } else {
        start
            .saturating_add(usize::try_from(max_bytes).unwrap_or(usize::MAX))
            .min(content.len())
    };
    Ok(content[start..end]
        .chunks(1024)
        .map(|chunk| {
            Ok(ReadFileResponse {
                chunk: chunk.to_vec(),
            })
        })
        .collect())
}

/// Upstream that serves one file and rejects every other RPC.
struct FileOnlyUpstream {
    content: Bytes,
}

fn unused<T>() -> Result<Response<T>, Status> {
    Err(Status::unimplemented("not used by this test"))
}

#[tonic::async_trait]
impl BackendService for FileOnlyUpstream {
    async fn register_project(
        &self,
        _: Request<RegisterProjectRequest>,
    ) -> Result<Response<RegisterProjectResponse>, Status> {
        unused()
    }
    async fn register_repository(
        &self,
        _: Request<RegisterRepositoryRequest>,
    ) -> Result<Response<RegisterRepositoryResponse>, Status> {
        unused()
    }
    async fn delete_repository(
        &self,
        _: Request<DeleteRepositoryRequest>,
    ) -> Result<Response<DeleteRepositoryResponse>, Status> {
        unused()
    }
    async fn read_commit(
        &self,
        _: Request<ReadCommitRequest>,
    ) -> Result<Response<ReadCommitResponse>, Status> {
        unused()
    }
    async fn write_commit(
        &self,
        _: Request<WriteCommitRequest>,
    ) -> Result<Response<WriteCommitResponse>, Status> {
        unused()
    }
    async fn list_commits(
        &self,
        _: Request<ListCommitsRequest>,
    ) -> Result<Response<ListCommitsResponse>, Status> {
        unused()
    }
    async fn resolve_commit_prefix(
        &self,
        _: Request<ResolveCommitPrefixRequest>,
    ) -> Result<Response<ResolveCommitPrefixResponse>, Status> {
        unused()
    }
    async fn resolve_change_prefix(
        &self,
        _: Request<ResolveChangePrefixRequest>,
    ) -> Result<Response<ResolveChangePrefixResponse>, Status> {
        unused()
    }
    async fn read_tree(
        &self,
        _: Request<ReadTreeRequest>,
    ) -> Result<Response<ReadTreeResponse>, Status> {
        unused()
    }
    async fn write_tree(
        &self,
        _: Request<WriteTreeRequest>,
    ) -> Result<Response<WriteTreeResponse>, Status> {
        unused()
    }

    type ReadFileStream =
        Pin<Box<dyn Stream<Item = Result<ReadFileResponse, Status>> + Send + 'static>>;

    async fn read_file(
        &self,
        request: Request<ReadFileRequest>,
    ) -> Result<Response<Self::ReadFileStream>, Status> {
        let req = request.into_inner();
        let frames = slice_file_range_frames(&self.content, req.offset, req.max_bytes)?;
        Ok(Response::new(Box::pin(futures::stream::iter(frames))))
    }
    async fn write_file(
        &self,
        _: Request<WriteFileRequest>,
    ) -> Result<Response<WriteFileResponse>, Status> {
        unused()
    }
    async fn write_file_stream(
        &self,
        _: Request<Streaming<WriteFileChunk>>,
    ) -> Result<Response<WriteFileResponse>, Status> {
        unused()
    }
    async fn read_symlink(
        &self,
        _: Request<ReadSymlinkRequest>,
    ) -> Result<Response<ReadSymlinkResponse>, Status> {
        unused()
    }
    async fn write_symlink(
        &self,
        _: Request<WriteSymlinkRequest>,
    ) -> Result<Response<WriteSymlinkResponse>, Status> {
        unused()
    }
}

#[derive(Default)]
struct MockUpstreamState {
    required_auth: Option<String>,
    read_commit_calls: usize,
    write_commit_calls: usize,
    read_tree_calls: usize,
    read_tree_page_sizes: Vec<u32>,
    read_tree_requests: Vec<(u32, String)>,
    reject_nonzero_tree_page_size: bool,
    read_file_calls: usize,
    read_file_requests: Vec<(u64, u64)>,
    read_symlink_calls: usize,
    read_view_calls: usize,
    read_op_calls: usize,
    write_file_stream_calls: usize,
    get_workspace_calls: usize,
    update_workspace_calls: usize,
    last_read_commit_timeout: Option<String>,
    last_get_workspace_timeout: Option<String>,
    last_write_file_stream_auth: Option<String>,
    last_write_file_stream_timeout: Option<String>,
    last_write_file_stream_had_upstream_header: bool,
    register_repository_calls: usize,
    delete_repository_calls: usize,
    delete_repository_error: Option<tonic::Code>,
    repos: std::collections::HashMap<String, String>,
    commits: std::collections::HashMap<(String, Vec<u8>), Commit>,
    trees: std::collections::HashMap<Vec<u8>, Vec<ReadTreeResponse>>,
    dynamic_trees: std::collections::HashMap<Vec<u8>, Vec<TreeEntry>>,
    files: std::collections::HashMap<Vec<u8>, Bytes>,
    symlinks: std::collections::HashMap<Vec<u8>, String>,
    views: std::collections::HashMap<Vec<u8>, View>,
    view_id_overrides: std::collections::HashMap<Vec<u8>, Vec<u8>>,
    ops: std::collections::HashMap<Vec<u8>, Operation>,
    /// Optional ACL map restricting which `(project_id, auth_header)` pairs may read an object.
    object_acls: std::collections::HashMap<Vec<u8>, (String, Option<String>)>,
}

#[derive(Clone, Default)]
struct MockUpstream {
    state: Arc<Mutex<MockUpstreamState>>,
}

impl MockUpstream {
    fn check_auth(&self, meta: &MetadataMap) -> Result<(), Status> {
        let required = self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .required_auth
            .clone();
        let Some(expected) = required else {
            return Ok(());
        };
        match meta.get("authorization").and_then(|v| v.to_str().ok()) {
            None => Err(Status::unauthenticated("missing authorization")),
            Some(got) if got == expected => Ok(()),
            Some(_) => Err(Status::permission_denied("wrong authorization")),
        }
    }

    fn check_object_acl(
        &self,
        meta: &MetadataMap,
        project_id: &str,
        object_id: &[u8],
    ) -> Result<(), Status> {
        let auth = meta
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(ToOwned::to_owned);
        let st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((allowed_proj, allowed_auth)) = st.object_acls.get(object_id)
            && (allowed_proj != project_id || allowed_auth != &auth)
        {
            return Err(Status::permission_denied(
                "caller is not authorized for object",
            ));
        }
        Ok(())
    }
}

fn git_blob_id(content: &[u8]) -> Vec<u8> {
    let mut hasher = Sha1::new();
    hasher.update(format!("blob {}\0", content.len()).as_bytes());
    hasher.update(content);
    hasher.finalize().to_vec()
}

#[tonic::async_trait]
impl BackendService for MockUpstream {
    async fn register_project(
        &self,
        _: Request<RegisterProjectRequest>,
    ) -> Result<Response<RegisterProjectResponse>, Status> {
        unused()
    }
    async fn register_repository(
        &self,
        request: Request<RegisterRepositoryRequest>,
    ) -> Result<Response<RegisterRepositoryResponse>, Status> {
        self.check_auth(request.metadata())?;
        let req = request.into_inner();
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        st.register_repository_calls += 1;
        let repo_id = req
            .repo_id
            .unwrap_or_else(|| format!("auto-repo-{}", st.register_repository_calls));
        let project_id = if req.project_id.is_empty() {
            repo_id.clone()
        } else {
            req.project_id
        };
        if let Some(existing) = st.repos.get(&repo_id)
            && existing != &project_id
        {
            return Err(Status::already_exists(format!(
                "repo '{repo_id}' already bound to '{existing}'"
            )));
        }
        st.repos.insert(repo_id.clone(), project_id.clone());
        Ok(Response::new(RegisterRepositoryResponse {
            repo_id,
            project_id,
        }))
    }
    async fn delete_repository(
        &self,
        request: Request<DeleteRepositoryRequest>,
    ) -> Result<Response<DeleteRepositoryResponse>, Status> {
        self.check_auth(request.metadata())?;
        let req = request.into_inner();
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        st.delete_repository_calls += 1;
        if let Some(code) = st.delete_repository_error.take() {
            return Err(Status::new(code, "injected delete_repository failure"));
        }
        let Some(actual_project) = st.repos.get(&req.repo_id).cloned() else {
            return Ok(Response::new(DeleteRepositoryResponse { existed: false }));
        };
        if let Some(expected) = req.project_id
            && actual_project != expected
        {
            return Err(Status::failed_precondition(format!(
                "repo '{}' belongs to '{}', not '{}'",
                req.repo_id, actual_project, expected
            )));
        }
        st.repos.remove(&req.repo_id);
        Ok(Response::new(DeleteRepositoryResponse { existed: true }))
    }
    async fn read_commit(
        &self,
        request: Request<ReadCommitRequest>,
    ) -> Result<Response<ReadCommitResponse>, Status> {
        self.check_auth(request.metadata())?;
        let timeout = request
            .metadata()
            .get("grpc-timeout")
            .and_then(|v| v.to_str().ok())
            .map(ToOwned::to_owned);
        let req = request.into_inner();
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        st.read_commit_calls += 1;
        st.last_read_commit_timeout = timeout;
        let commit = st
            .commits
            .get(&(req.project_id, req.commit_id))
            .cloned()
            .ok_or_else(|| Status::not_found("commit not found"))?;
        Ok(Response::new(ReadCommitResponse {
            commit: Some(commit),
        }))
    }
    async fn write_commit(
        &self,
        request: Request<WriteCommitRequest>,
    ) -> Result<Response<WriteCommitResponse>, Status> {
        self.check_auth(request.metadata())?;
        let req = request.into_inner();
        let mut commit = req
            .commit
            .ok_or_else(|| Status::invalid_argument("missing commit"))?;
        if commit.conflict_labels.iter().all(|l| l.is_empty()) {
            commit.conflict_labels.clear();
        }
        if let Some(sig) = &mut commit.secure_sig {
            if sig.sig.is_empty() {
                commit.secure_sig = None;
            } else if sig.data.is_empty() {
                sig.data = b"server-derived-signed-payload".to_vec();
            }
        }
        let id = if commit.commit_id.is_empty() {
            Sha1::digest(commit.encode_to_vec()).to_vec()
        } else {
            commit.commit_id.clone()
        };
        commit.commit_id = id.clone();
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        st.write_commit_calls += 1;
        st.commits
            .entry((req.project_id, id.clone()))
            .or_insert(commit);
        Ok(Response::new(WriteCommitResponse { commit_id: id }))
    }
    async fn list_commits(
        &self,
        _: Request<ListCommitsRequest>,
    ) -> Result<Response<ListCommitsResponse>, Status> {
        unused()
    }
    async fn resolve_commit_prefix(
        &self,
        _: Request<ResolveCommitPrefixRequest>,
    ) -> Result<Response<ResolveCommitPrefixResponse>, Status> {
        unused()
    }
    async fn resolve_change_prefix(
        &self,
        _: Request<ResolveChangePrefixRequest>,
    ) -> Result<Response<ResolveChangePrefixResponse>, Status> {
        unused()
    }
    async fn read_tree(
        &self,
        request: Request<ReadTreeRequest>,
    ) -> Result<Response<ReadTreeResponse>, Status> {
        self.check_auth(request.metadata())?;
        let meta = request.metadata().clone();
        let req = request.into_inner();
        self.check_object_acl(&meta, &req.project_id, &req.tree_id)?;
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        st.read_tree_calls += 1;
        st.read_tree_page_sizes.push(req.page_size);
        st.read_tree_requests
            .push((req.page_size, req.page_token.clone()));
        if st.reject_nonzero_tree_page_size && req.page_size != 0 {
            return Err(Status::unimplemented(
                "paginated ReadTree is not implemented yet",
            ));
        }
        if let Some(entries) = st.dynamic_trees.get(&req.tree_id).cloned() {
            let full = ReadTreeResponse {
                tree_id: req.tree_id,
                entries,
                next_page_token: String::new(),
            };
            return Ok(Response::new(slice_tree_page(
                full,
                req.page_size,
                &req.page_token,
            )));
        }
        let pages = st
            .trees
            .get(&req.tree_id)
            .ok_or_else(|| Status::not_found("tree not found"))?;
        let page_idx = if req.page_token.is_empty() {
            0
        } else {
            pages
                .iter()
                .position(|p| p.next_page_token == req.page_token)
                .map(|idx| idx + 1)
                .unwrap_or(pages.len())
        };
        let page = pages
            .get(page_idx)
            .cloned()
            .ok_or_else(|| Status::not_found("tree page not found"))?;
        Ok(Response::new(page))
    }
    async fn write_tree(
        &self,
        request: Request<WriteTreeRequest>,
    ) -> Result<Response<WriteTreeResponse>, Status> {
        self.check_auth(request.metadata())?;
        let req = request.into_inner();
        let mut entries = req.entries;
        entries.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        for entry in &mut entries {
            if let Some(tree_value::Value::File(f)) =
                entry.value.as_mut().and_then(|v| v.value.as_mut())
                && let Some(bytes) = st.files.get(&f.id)
            {
                f.size_bytes = Some(bytes.len() as u64);
            }
        }
        let tree_id = Sha1::digest(
            ReadTreeResponse {
                tree_id: Vec::new(),
                entries: entries.clone(),
                next_page_token: String::new(),
            }
            .encode_to_vec(),
        )
        .to_vec();
        st.trees.insert(
            tree_id.clone(),
            vec![ReadTreeResponse {
                tree_id: tree_id.clone(),
                entries,
                next_page_token: String::new(),
            }],
        );
        Ok(Response::new(WriteTreeResponse { tree_id }))
    }

    type ReadFileStream =
        Pin<Box<dyn Stream<Item = Result<ReadFileResponse, Status>> + Send + 'static>>;

    async fn read_file(
        &self,
        request: Request<ReadFileRequest>,
    ) -> Result<Response<Self::ReadFileStream>, Status> {
        self.check_auth(request.metadata())?;
        let meta = request.metadata().clone();
        let req = request.into_inner();
        self.check_object_acl(&meta, &req.project_id, &req.file_id)?;
        let content = {
            let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
            st.read_file_calls += 1;
            st.read_file_requests.push((req.offset, req.max_bytes));
            st.files
                .get(&req.file_id)
                .cloned()
                .ok_or_else(|| Status::not_found("file not found"))?
        };
        let frames = slice_file_range_frames(&content, req.offset, req.max_bytes)?;
        Ok(Response::new(Box::pin(futures::stream::iter(frames))))
    }
    async fn write_file(
        &self,
        request: Request<WriteFileRequest>,
    ) -> Result<Response<WriteFileResponse>, Status> {
        self.check_auth(request.metadata())?;
        let req = request.into_inner();
        let file_id = git_blob_id(&req.content);
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        st.files.insert(file_id.clone(), Bytes::from(req.content));
        Ok(Response::new(WriteFileResponse { file_id }))
    }
    async fn write_file_stream(
        &self,
        request: Request<Streaming<WriteFileChunk>>,
    ) -> Result<Response<WriteFileResponse>, Status> {
        self.check_auth(request.metadata())?;
        let auth = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(ToOwned::to_owned);
        let timeout = request
            .metadata()
            .get("grpc-timeout")
            .and_then(|v| v.to_str().ok())
            .map(ToOwned::to_owned);
        let had_upstream_header = request.metadata().get(UPSTREAM_HEADER).is_some();
        let mut stream = request.into_inner();
        let mut data = Vec::new();
        while let Some(chunk) = stream.next().await {
            data.extend_from_slice(&chunk?.chunk);
        }
        let file_id = git_blob_id(&data);
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        st.write_file_stream_calls += 1;
        st.last_write_file_stream_auth = auth;
        st.last_write_file_stream_timeout = timeout;
        st.last_write_file_stream_had_upstream_header = had_upstream_header;
        st.files.insert(file_id.clone(), Bytes::from(data));
        Ok(Response::new(WriteFileResponse { file_id }))
    }
    async fn read_symlink(
        &self,
        request: Request<ReadSymlinkRequest>,
    ) -> Result<Response<ReadSymlinkResponse>, Status> {
        self.check_auth(request.metadata())?;
        let meta = request.metadata().clone();
        let req = request.into_inner();
        self.check_object_acl(&meta, &req.project_id, &req.symlink_id)?;
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        st.read_symlink_calls += 1;
        let target = st
            .symlinks
            .get(&req.symlink_id)
            .cloned()
            .ok_or_else(|| Status::not_found("symlink not found"))?;
        Ok(Response::new(ReadSymlinkResponse { target }))
    }
    async fn write_symlink(
        &self,
        request: Request<WriteSymlinkRequest>,
    ) -> Result<Response<WriteSymlinkResponse>, Status> {
        self.check_auth(request.metadata())?;
        let req = request.into_inner();
        let symlink_id = git_blob_id(req.target.as_bytes());
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        st.symlinks.insert(symlink_id.clone(), req.target);
        Ok(Response::new(WriteSymlinkResponse { symlink_id }))
    }
}

#[tonic::async_trait]
impl OpStoreService for MockUpstream {
    async fn read_operation(
        &self,
        request: Request<ReadOperationRequest>,
    ) -> Result<Response<ReadOperationResponse>, Status> {
        self.check_auth(request.metadata())?;
        let req = request.into_inner();
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        st.read_op_calls += 1;
        let op = st
            .ops
            .get(&req.operation_id)
            .cloned()
            .ok_or_else(|| Status::not_found("op not found"))?;
        Ok(Response::new(ReadOperationResponse {
            operation: Some(op),
        }))
    }
    async fn write_operation(
        &self,
        _: Request<WriteOperationRequest>,
    ) -> Result<Response<WriteOperationResponse>, Status> {
        unused()
    }
    async fn resolve_operation_prefix(
        &self,
        _: Request<ResolveOperationPrefixRequest>,
    ) -> Result<Response<ResolveOperationPrefixResponse>, Status> {
        unused()
    }
    async fn read_view(
        &self,
        request: Request<ReadViewRequest>,
    ) -> Result<Response<ReadViewResponse>, Status> {
        self.check_auth(request.metadata())?;
        let req = request.into_inner();
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        st.read_view_calls += 1;
        let view = st
            .views
            .get(&req.view_id)
            .cloned()
            .ok_or_else(|| Status::not_found("view not found"))?;
        let returned_view_id = st
            .view_id_overrides
            .get(&req.view_id)
            .cloned()
            .unwrap_or_else(|| req.view_id.clone());
        Ok(Response::new(ReadViewResponse {
            view_id: returned_view_id,
            view: Some(view),
        }))
    }
    async fn write_view(
        &self,
        _: Request<WriteViewRequest>,
    ) -> Result<Response<WriteViewResponse>, Status> {
        unused()
    }
    async fn get_op_heads(
        &self,
        _: Request<GetOpHeadsRequest>,
    ) -> Result<Response<GetOpHeadsResponse>, Status> {
        unused()
    }
    async fn update_op_heads(
        &self,
        _: Request<UpdateOpHeadsRequest>,
    ) -> Result<Response<UpdateOpHeadsResponse>, Status> {
        unused()
    }
    async fn reconcile_op_heads(
        &self,
        _: Request<ReconcileOpHeadsRequest>,
    ) -> Result<Response<ReconcileOpHeadsResponse>, Status> {
        unused()
    }
    async fn publish_operation(
        &self,
        request: Request<PublishOperationRequest>,
    ) -> Result<Response<PublishOperationResponse>, Status> {
        self.check_auth(request.metadata())?;
        let req = request.into_inner();
        let view = req
            .view
            .ok_or_else(|| Status::invalid_argument("missing view"))?;
        let mut op = req
            .operation
            .ok_or_else(|| Status::invalid_argument("missing operation"))?;
        let view_id = Sha1::digest(view.encode_to_vec())[..16].to_vec();
        op.view_id = view_id.clone();
        let operation_id = Sha1::digest(op.encode_to_vec())[..16].to_vec();
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        st.views.insert(view_id.clone(), view);
        st.ops.insert(operation_id.clone(), op);
        Ok(Response::new(PublishOperationResponse {
            view_id,
            operation_id: operation_id.clone(),
            current_op_head_ids: vec![operation_id],
        }))
    }
}

#[tonic::async_trait]
impl WorkspaceService for MockUpstream {
    async fn get_workspace(
        &self,
        request: Request<GetWorkspaceRequest>,
    ) -> Result<Response<GetWorkspaceResponse>, Status> {
        self.check_auth(request.metadata())?;
        let timeout = request
            .metadata()
            .get("grpc-timeout")
            .and_then(|v| v.to_str().ok())
            .map(ToOwned::to_owned);
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        st.get_workspace_calls += 1;
        st.last_get_workspace_timeout = timeout;
        Ok(Response::new(GetWorkspaceResponse { workspace: None }))
    }
    async fn update_workspace(
        &self,
        request: Request<UpdateWorkspaceRequest>,
    ) -> Result<Response<UpdateWorkspaceResponse>, Status> {
        self.check_auth(request.metadata())?;
        let req = request.into_inner();
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        st.update_workspace_calls += 1;
        Ok(Response::new(UpdateWorkspaceResponse {
            success: true,
            workspace: req.workspace,
        }))
    }
    async fn list_workspaces(
        &self,
        _: Request<ListWorkspacesRequest>,
    ) -> Result<Response<ListWorkspacesResponse>, Status> {
        unused()
    }
    async fn check_working_copy_changes(
        &self,
        _: Request<CheckWorkingCopyChangesRequest>,
    ) -> Result<Response<CheckWorkingCopyChangesResponse>, Status> {
        unused()
    }
    async fn delete_workspace(
        &self,
        _: Request<DeleteWorkspaceRequest>,
    ) -> Result<Response<DeleteWorkspaceResponse>, Status> {
        unused()
    }
}

async fn spawn_mock_upstream(mock: MockUpstream) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let incoming =
        tonic::transport::server::TcpIncoming::from_listener(listener, true, None).unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(BackendServiceServer::new(mock.clone()))
            .add_service(OpStoreServiceServer::new(mock.clone()))
            .add_service(WorkspaceServiceServer::new(mock))
            .serve_with_incoming(incoming),
    );
    url
}

fn req_with_upstream<T>(payload: T, url: &str, auth: Option<&str>) -> Request<T> {
    let mut req = Request::new(payload);
    req.metadata_mut()
        .insert(UPSTREAM_HEADER, url.parse().unwrap());
    if let Some(auth) = auth {
        req.metadata_mut()
            .insert("authorization", auth.parse().unwrap());
    }
    req
}
