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

mod export;
mod import;
mod refs;

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use cc_common::backend::{
    Commit, ReadCommitRequest, ReadFileRequest, ReadSymlinkRequest, ReadTreeRequest,
    RegisterRepositoryRequest, TreeEntry, WriteCommitRequest, WriteFileChunk, WriteFileRequest,
    WriteSymlinkRequest, WriteTreeRequest,
};
use cc_common::op_store::op_store_service_server::OpStoreService;
use cc_common::op_store::{
    GetOpHeadsRequest, GetOpHeadsResponse, Operation, PublishOperationRequest,
    PublishOperationResponse, ReadOperationRequest, ReadOperationResponse, ReadViewRequest,
    ReadViewResponse, ReconcileOpHeadsRequest, ReconcileOpHeadsResponse,
    ResolveOperationPrefixRequest, ResolveOperationPrefixResponse, UpdateOpHeadsRequest,
    UpdateOpHeadsResponse, WriteOperationRequest, WriteOperationResponse, WriteViewRequest,
    WriteViewResponse,
};
use tonic::{Request, Response, Status};

use super::export::{
    ExportTreeState, export_tree_recursive, export_tree_recursive_with_max_file_bytes,
};
use super::git_repo::{
    ReftableObjectsDir, ensure_no_checked_out_branch_conflict, open_or_init_git_repo,
    resolve_dot_git_dir, resolve_git_common_dir,
};
use super::import::{collect_git_graph, import_tree_recursive};
use super::objects::{
    decode_change_id_header, export_single_commit, parse_jj_trees_header,
    serialize_proto_commit_to_git,
};
use super::publish::{
    CommitParentCache, merge_imported_local_bookmark, publish_imported_refs, resolve_single_op_head,
};
use super::refs::{
    decode_importable_git_ref_name, discover_git_refs, git_branch_ref_name, is_importable_git_ref,
    parse_reftable_for_each_ref_output, write_git_refs,
};
use super::{GitExportOptions, GitExporter, GitImportOptions, GitImporter};
use crate::util::run_on_tokio;

/// Op store whose `ReadOperation` or `ReadView` fails and that records publish attempts.
#[derive(Default)]
struct FailingReadOpStore {
    fail_read_op: bool,
    published: Arc<AtomicBool>,
}

#[tonic::async_trait]
impl OpStoreService for FailingReadOpStore {
    async fn read_operation(
        &self,
        _request: Request<ReadOperationRequest>,
    ) -> Result<Response<ReadOperationResponse>, Status> {
        if self.fail_read_op {
            return Err(Status::unavailable("injected ReadOperation failure"));
        }
        let operation = Operation {
            view_id: vec![1; cc_common::VIEW_ID_LENGTH],
            ..Default::default()
        };
        Ok(Response::new(ReadOperationResponse {
            operation: Some(operation),
        }))
    }

    async fn write_operation(
        &self,
        _request: Request<WriteOperationRequest>,
    ) -> Result<Response<WriteOperationResponse>, Status> {
        Err(Status::unimplemented("write_operation"))
    }

    async fn resolve_operation_prefix(
        &self,
        _request: Request<ResolveOperationPrefixRequest>,
    ) -> Result<Response<ResolveOperationPrefixResponse>, Status> {
        Err(Status::unimplemented("resolve_operation_prefix"))
    }

    async fn read_view(
        &self,
        _request: Request<ReadViewRequest>,
    ) -> Result<Response<ReadViewResponse>, Status> {
        Err(Status::unavailable("injected ReadView failure"))
    }

    async fn write_view(
        &self,
        _request: Request<WriteViewRequest>,
    ) -> Result<Response<WriteViewResponse>, Status> {
        Err(Status::unimplemented("write_view"))
    }

    async fn get_op_heads(
        &self,
        _request: Request<GetOpHeadsRequest>,
    ) -> Result<Response<GetOpHeadsResponse>, Status> {
        Err(Status::unimplemented("get_op_heads"))
    }

    async fn update_op_heads(
        &self,
        _request: Request<UpdateOpHeadsRequest>,
    ) -> Result<Response<UpdateOpHeadsResponse>, Status> {
        Err(Status::unimplemented("update_op_heads"))
    }

    async fn reconcile_op_heads(
        &self,
        _request: Request<ReconcileOpHeadsRequest>,
    ) -> Result<Response<ReconcileOpHeadsResponse>, Status> {
        Ok(Response::new(ReconcileOpHeadsResponse {
            op_head: vec![2; cc_common::OPERATION_ID_LENGTH],
            client_resolution_required: false,
            current_op_head_ids: vec![vec![2; cc_common::OPERATION_ID_LENGTH]],
        }))
    }

    async fn publish_operation(
        &self,
        _request: Request<PublishOperationRequest>,
    ) -> Result<Response<PublishOperationResponse>, Status> {
        self.published.store(true, Ordering::SeqCst);
        Ok(Response::new(PublishOperationResponse::default()))
    }
}

struct MockBackendService {
    commits: HashMap<Vec<u8>, Commit>,
    trees: HashMap<Vec<u8>, Vec<TreeEntry>>,
    files: HashMap<Vec<u8>, Vec<u8>>,
}

#[tonic::async_trait]
impl cc_common::backend::backend_service_server::BackendService for MockBackendService {
    type ReadFileStream = futures::stream::Iter<
        std::vec::IntoIter<Result<cc_common::backend::ReadFileResponse, Status>>,
    >;

    async fn register_project(
        &self,
        _request: Request<cc_common::backend::RegisterProjectRequest>,
    ) -> Result<Response<cc_common::backend::RegisterProjectResponse>, Status> {
        Err(Status::unimplemented("register_project"))
    }

    async fn register_repository(
        &self,
        _request: Request<RegisterRepositoryRequest>,
    ) -> Result<Response<cc_common::backend::RegisterRepositoryResponse>, Status> {
        Err(Status::unimplemented("register_repository"))
    }

    async fn delete_repository(
        &self,
        _request: Request<cc_common::backend::DeleteRepositoryRequest>,
    ) -> Result<Response<cc_common::backend::DeleteRepositoryResponse>, Status> {
        Err(Status::unimplemented("delete_repository"))
    }

    async fn read_commit(
        &self,
        request: Request<ReadCommitRequest>,
    ) -> Result<Response<cc_common::backend::ReadCommitResponse>, Status> {
        let cid = request.into_inner().commit_id;
        let commit =
            self.commits.get(&cid).cloned().ok_or_else(|| {
                Status::not_found(format!("missing commit {}", hex::encode(&cid)))
            })?;
        Ok(Response::new(cc_common::backend::ReadCommitResponse {
            commit: Some(commit),
        }))
    }

    async fn write_commit(
        &self,
        _request: Request<WriteCommitRequest>,
    ) -> Result<Response<cc_common::backend::WriteCommitResponse>, Status> {
        Err(Status::unimplemented("write_commit"))
    }

    async fn list_commits(
        &self,
        _request: Request<cc_common::backend::ListCommitsRequest>,
    ) -> Result<Response<cc_common::backend::ListCommitsResponse>, Status> {
        Err(Status::unimplemented("list_commits"))
    }

    async fn resolve_commit_prefix(
        &self,
        _request: Request<cc_common::backend::ResolveCommitPrefixRequest>,
    ) -> Result<Response<cc_common::backend::ResolveCommitPrefixResponse>, Status> {
        Err(Status::unimplemented("resolve_commit_prefix"))
    }

    async fn resolve_change_prefix(
        &self,
        _request: Request<cc_common::backend::ResolveChangePrefixRequest>,
    ) -> Result<Response<cc_common::backend::ResolveChangePrefixResponse>, Status> {
        Err(Status::unimplemented("resolve_change_prefix"))
    }

    async fn read_tree(
        &self,
        request: Request<ReadTreeRequest>,
    ) -> Result<Response<cc_common::backend::ReadTreeResponse>, Status> {
        let tid = request.into_inner().tree_id;
        let entries = self
            .trees
            .get(&tid)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("missing tree {}", hex::encode(&tid))))?;
        Ok(Response::new(cc_common::backend::ReadTreeResponse {
            tree_id: tid,
            entries,
            next_page_token: String::new(),
        }))
    }

    async fn write_tree(
        &self,
        _request: Request<WriteTreeRequest>,
    ) -> Result<Response<cc_common::backend::WriteTreeResponse>, Status> {
        Err(Status::unimplemented("write_tree"))
    }

    async fn read_file(
        &self,
        request: Request<ReadFileRequest>,
    ) -> Result<Response<Self::ReadFileStream>, Status> {
        let fid = request.into_inner().file_id;
        let bytes = self
            .files
            .get(&fid)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("missing file {}", hex::encode(&fid))))?;
        Ok(Response::new(futures::stream::iter(vec![Ok(
            cc_common::backend::ReadFileResponse { chunk: bytes },
        )])))
    }

    async fn write_file(
        &self,
        _request: Request<WriteFileRequest>,
    ) -> Result<Response<cc_common::backend::WriteFileResponse>, Status> {
        Err(Status::unimplemented("write_file"))
    }

    async fn write_file_stream(
        &self,
        _request: Request<tonic::Streaming<WriteFileChunk>>,
    ) -> Result<Response<cc_common::backend::WriteFileResponse>, Status> {
        Err(Status::unimplemented("write_file_stream"))
    }

    async fn read_symlink(
        &self,
        _request: Request<ReadSymlinkRequest>,
    ) -> Result<Response<cc_common::backend::ReadSymlinkResponse>, Status> {
        Err(Status::unimplemented("read_symlink"))
    }

    async fn write_symlink(
        &self,
        _request: Request<WriteSymlinkRequest>,
    ) -> Result<Response<cc_common::backend::WriteSymlinkResponse>, Status> {
        Err(Status::unimplemented("write_symlink"))
    }
}

struct MockOpStoreService {
    initial_view: cc_common::op_store::View,
    published_view: Arc<std::sync::Mutex<Option<cc_common::op_store::View>>>,
}

#[tonic::async_trait]
impl OpStoreService for MockOpStoreService {
    async fn read_operation(
        &self,
        _request: Request<ReadOperationRequest>,
    ) -> Result<Response<ReadOperationResponse>, Status> {
        Ok(Response::new(ReadOperationResponse {
            operation: Some(Operation {
                view_id: vec![1; cc_common::VIEW_ID_LENGTH],
                parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                ..Default::default()
            }),
        }))
    }

    async fn write_operation(
        &self,
        _request: Request<WriteOperationRequest>,
    ) -> Result<Response<WriteOperationResponse>, Status> {
        Err(Status::unimplemented("write_operation"))
    }

    async fn resolve_operation_prefix(
        &self,
        _request: Request<ResolveOperationPrefixRequest>,
    ) -> Result<Response<ResolveOperationPrefixResponse>, Status> {
        Err(Status::unimplemented("resolve_operation_prefix"))
    }

    async fn read_view(
        &self,
        _request: Request<ReadViewRequest>,
    ) -> Result<Response<ReadViewResponse>, Status> {
        Ok(Response::new(ReadViewResponse {
            view_id: vec![1; cc_common::VIEW_ID_LENGTH],
            view: Some(self.initial_view.clone()),
        }))
    }

    async fn write_view(
        &self,
        _request: Request<WriteViewRequest>,
    ) -> Result<Response<WriteViewResponse>, Status> {
        Err(Status::unimplemented("write_view"))
    }

    async fn get_op_heads(
        &self,
        _request: Request<GetOpHeadsRequest>,
    ) -> Result<Response<GetOpHeadsResponse>, Status> {
        Ok(Response::new(GetOpHeadsResponse {
            op_head_ids: vec![vec![2; cc_common::OPERATION_ID_LENGTH]],
        }))
    }

    async fn update_op_heads(
        &self,
        _request: Request<UpdateOpHeadsRequest>,
    ) -> Result<Response<UpdateOpHeadsResponse>, Status> {
        Err(Status::unimplemented("update_op_heads"))
    }

    async fn reconcile_op_heads(
        &self,
        _request: Request<ReconcileOpHeadsRequest>,
    ) -> Result<Response<ReconcileOpHeadsResponse>, Status> {
        Ok(Response::new(ReconcileOpHeadsResponse {
            op_head: vec![2; cc_common::OPERATION_ID_LENGTH],
            client_resolution_required: false,
            current_op_head_ids: vec![vec![2; cc_common::OPERATION_ID_LENGTH]],
        }))
    }

    async fn publish_operation(
        &self,
        request: Request<PublishOperationRequest>,
    ) -> Result<Response<PublishOperationResponse>, Status> {
        let req = request.into_inner();
        *self.published_view.lock().unwrap() = req.view;
        Ok(Response::new(PublishOperationResponse {
            view_id: vec![3; cc_common::VIEW_ID_LENGTH],
            operation_id: vec![4; cc_common::OPERATION_ID_LENGTH],
            current_op_head_ids: vec![vec![4; cc_common::OPERATION_ID_LENGTH]],
        }))
    }
}

/// Encodes `raw` as a valid zlib stream containing a single uncompressed RFC 1951 block so
/// tests can plant crafted loose Git objects under `.git/objects/xx/yyyy...`.
fn zlib_store_uncompressed(raw: &[u8]) -> Vec<u8> {
    assert!(raw.len() <= u16::MAX as usize);
    let len = raw.len() as u16;
    let nlen = !len;
    let mut out = Vec::with_capacity(raw.len() + 11);
    out.extend_from_slice(&[0x78, 0x01, 0x01]);
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&nlen.to_le_bytes());
    out.extend_from_slice(raw);
    let mut s1: u32 = 1;
    let mut s2: u32 = 0;
    for &b in raw {
        s1 = (s1 + u32::from(b)) % 65_521;
        s2 = (s2 + s1) % 65_521;
    }
    let adler32 = (s2 << 16) | s1;
    out.extend_from_slice(&adler32.to_be_bytes());
    out
}

fn write_crafted_loose_tree(
    repo_dir: &Path,
    tree_oid: gix::hash::ObjectId,
    child_name: &str,
    child_oid: gix::hash::ObjectId,
) {
    let mut body = Vec::new();
    body.extend_from_slice(b"40000 ");
    body.extend_from_slice(child_name.as_bytes());
    body.push(0);
    body.extend_from_slice(child_oid.as_bytes());
    let mut loose = format!("tree {}\0", body.len()).into_bytes();
    loose.extend_from_slice(&body);
    let compressed = zlib_store_uncompressed(&loose);
    let hex_id = tree_oid.to_string();
    let obj_dir = repo_dir.join("objects").join(&hex_id[..2]);
    fs::create_dir_all(&obj_dir).unwrap();
    fs::write(obj_dir.join(&hex_id[2..]), compressed).unwrap();
}

#[derive(Clone)]
enum MockTreeBehavior {
    Pages(Vec<cc_common::backend::ReadTreeResponse>),
    Oversized {
        page_entries: usize,
        total_pages: usize,
    },
}

struct ValidationMockBackendService {
    commits: HashMap<Vec<u8>, Commit>,
    trees: HashMap<Vec<u8>, MockTreeBehavior>,
    files: HashMap<Vec<u8>, Vec<u8>>,
    chunked_files: HashMap<Vec<u8>, Vec<Vec<u8>>>,
    symlinks: HashMap<Vec<u8>, String>,
    read_commit_calls: Arc<std::sync::Mutex<HashMap<Vec<u8>, usize>>>,
    read_tree_calls: Arc<std::sync::atomic::AtomicUsize>,
    read_file_calls: Arc<std::sync::atomic::AtomicUsize>,
}

#[tonic::async_trait]
impl cc_common::backend::backend_service_server::BackendService for ValidationMockBackendService {
    type ReadFileStream = futures::stream::Iter<
        std::vec::IntoIter<Result<cc_common::backend::ReadFileResponse, Status>>,
    >;

    async fn register_project(
        &self,
        _request: Request<cc_common::backend::RegisterProjectRequest>,
    ) -> Result<Response<cc_common::backend::RegisterProjectResponse>, Status> {
        Err(Status::unimplemented("register_project"))
    }

    async fn register_repository(
        &self,
        _request: Request<RegisterRepositoryRequest>,
    ) -> Result<Response<cc_common::backend::RegisterRepositoryResponse>, Status> {
        Err(Status::unimplemented("register_repository"))
    }

    async fn delete_repository(
        &self,
        _request: Request<cc_common::backend::DeleteRepositoryRequest>,
    ) -> Result<Response<cc_common::backend::DeleteRepositoryResponse>, Status> {
        Err(Status::unimplemented("delete_repository"))
    }

    async fn read_commit(
        &self,
        request: Request<ReadCommitRequest>,
    ) -> Result<Response<cc_common::backend::ReadCommitResponse>, Status> {
        let cid = request.into_inner().commit_id;
        *self
            .read_commit_calls
            .lock()
            .unwrap()
            .entry(cid.clone())
            .or_insert(0) += 1;
        let commit =
            self.commits.get(&cid).cloned().ok_or_else(|| {
                Status::not_found(format!("missing commit {}", hex::encode(&cid)))
            })?;
        Ok(Response::new(cc_common::backend::ReadCommitResponse {
            commit: Some(commit),
        }))
    }

    async fn write_commit(
        &self,
        _request: Request<WriteCommitRequest>,
    ) -> Result<Response<cc_common::backend::WriteCommitResponse>, Status> {
        Err(Status::unimplemented("write_commit"))
    }

    async fn list_commits(
        &self,
        _request: Request<cc_common::backend::ListCommitsRequest>,
    ) -> Result<Response<cc_common::backend::ListCommitsResponse>, Status> {
        Err(Status::unimplemented("list_commits"))
    }

    async fn resolve_commit_prefix(
        &self,
        _request: Request<cc_common::backend::ResolveCommitPrefixRequest>,
    ) -> Result<Response<cc_common::backend::ResolveCommitPrefixResponse>, Status> {
        Err(Status::unimplemented("resolve_commit_prefix"))
    }

    async fn resolve_change_prefix(
        &self,
        _request: Request<cc_common::backend::ResolveChangePrefixRequest>,
    ) -> Result<Response<cc_common::backend::ResolveChangePrefixResponse>, Status> {
        Err(Status::unimplemented("resolve_change_prefix"))
    }

    async fn read_tree(
        &self,
        request: Request<ReadTreeRequest>,
    ) -> Result<Response<cc_common::backend::ReadTreeResponse>, Status> {
        self.read_tree_calls.fetch_add(1, Ordering::SeqCst);
        let req = request.into_inner();
        let behavior = self.trees.get(&req.tree_id).ok_or_else(|| {
            Status::not_found(format!("missing tree {}", hex::encode(&req.tree_id)))
        })?;
        match behavior {
            MockTreeBehavior::Pages(pages) => {
                let idx: usize = if req.page_token.is_empty() {
                    0
                } else {
                    req.page_token.parse().unwrap_or(pages.len())
                };
                let page = pages
                    .get(idx)
                    .cloned()
                    .ok_or_else(|| Status::invalid_argument("invalid page token"))?;
                Ok(Response::new(page))
            }
            MockTreeBehavior::Oversized {
                page_entries,
                total_pages,
            } => {
                let idx: usize = if req.page_token.is_empty() {
                    0
                } else {
                    req.page_token.parse().unwrap_or(*total_pages)
                };
                let entries = vec![
                    TreeEntry {
                        name: "e".to_string(),
                        value: None,
                    };
                    *page_entries
                ];
                let next_page_token = if idx + 1 < *total_pages {
                    format!("{:04}", idx + 1)
                } else {
                    String::new()
                };
                Ok(Response::new(cc_common::backend::ReadTreeResponse {
                    tree_id: req.tree_id,
                    entries,
                    next_page_token,
                }))
            }
        }
    }

    async fn write_tree(
        &self,
        _request: Request<WriteTreeRequest>,
    ) -> Result<Response<cc_common::backend::WriteTreeResponse>, Status> {
        Err(Status::unimplemented("write_tree"))
    }

    async fn read_file(
        &self,
        request: Request<ReadFileRequest>,
    ) -> Result<Response<Self::ReadFileStream>, Status> {
        self.read_file_calls.fetch_add(1, Ordering::SeqCst);
        let fid = request.into_inner().file_id;
        if let Some(chunks) = self.chunked_files.get(&fid) {
            let items = chunks
                .iter()
                .cloned()
                .map(|chunk| Ok(cc_common::backend::ReadFileResponse { chunk }))
                .collect::<Vec<_>>();
            return Ok(Response::new(futures::stream::iter(items)));
        }
        let bytes = self
            .files
            .get(&fid)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("missing file {}", hex::encode(&fid))))?;
        Ok(Response::new(futures::stream::iter(vec![Ok(
            cc_common::backend::ReadFileResponse { chunk: bytes },
        )])))
    }

    async fn write_file(
        &self,
        _request: Request<WriteFileRequest>,
    ) -> Result<Response<cc_common::backend::WriteFileResponse>, Status> {
        Err(Status::unimplemented("write_file"))
    }

    async fn write_file_stream(
        &self,
        _request: Request<tonic::Streaming<WriteFileChunk>>,
    ) -> Result<Response<cc_common::backend::WriteFileResponse>, Status> {
        Err(Status::unimplemented("write_file_stream"))
    }

    async fn read_symlink(
        &self,
        request: Request<ReadSymlinkRequest>,
    ) -> Result<Response<cc_common::backend::ReadSymlinkResponse>, Status> {
        let sid = request.into_inner().symlink_id;
        let target =
            self.symlinks.get(&sid).cloned().ok_or_else(|| {
                Status::not_found(format!("missing symlink {}", hex::encode(&sid)))
            })?;
        Ok(Response::new(cc_common::backend::ReadSymlinkResponse {
            target,
        }))
    }

    async fn write_symlink(
        &self,
        _request: Request<WriteSymlinkRequest>,
    ) -> Result<Response<cc_common::backend::WriteSymlinkResponse>, Status> {
        Err(Status::unimplemented("write_symlink"))
    }
}

fn write_crafted_loose_object(repo_dir: &Path, oid: gix::hash::ObjectId, kind: &str, body: &[u8]) {
    let mut loose = format!("{kind} {}\0", body.len()).into_bytes();
    loose.extend_from_slice(body);
    let compressed = zlib_store_uncompressed(&loose);
    let hex_id = oid.to_string();
    let obj_dir = repo_dir.join("objects").join(&hex_id[..2]);
    fs::create_dir_all(&obj_dir).unwrap();
    fs::write(obj_dir.join(&hex_id[2..]), compressed).unwrap();
}

fn write_crafted_loose_commit(
    repo_dir: &Path,
    oid: gix::hash::ObjectId,
    tree_oid: gix::hash::ObjectId,
    parents: &[gix::hash::ObjectId],
) {
    let mut body = format!("tree {tree_oid}\n");
    for p in parents {
        body.push_str(&format!("parent {p}\n"));
    }
    body.push_str(
        "author Test <test@example.com> 1730000000 +0000\n\
         committer Test <test@example.com> 1730000000 +0000\n\n\
         crafted\n",
    );
    write_crafted_loose_object(repo_dir, oid, "commit", body.as_bytes());
}
