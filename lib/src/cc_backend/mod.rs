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

//! `jj_lib::backend::Backend` adapter over `BackendService` gRPC RPCs.
//!
//! # Authority and Crash-Safety Ordering
//! File payloads stream through `WriteFileStream` in 64 KiB chunks (`FILE_STREAM_CHUNK_SIZE`)
//! and symlink targets persist via `WriteSymlink` before referencing `Tree` and `Commit`
//! records are uploaded. Repository attachment (`init_in_project` with `create = false`)
//! validates remote `repo_id` registration (and optional `project_id` binding) before
//! writing `.jj/repo/store/config.toml`.
//!
//! # Concurrency and Merge Invariants
//! All `Backend` writes are immutable, content-addressed, and safe under concurrent
//! execution. Conflicted `Merge<TreeId>` root trees, `conflict_labels`, and `SecureSig`
//! payloads round-trip without loss, and `ReadCommit`, `ReadTree`, `ReadFile`, and
//! `ReadSymlink` validate sizes, NUL invariants, and canonical Git SHA-1 digests before
//! returning data to `jj_lib`.
//!
//! # Cross-Process and RPC Boundaries
//! Translates gRPC `Code::NotFound` responses into `BackendError::ObjectNotFound`, caps
//! buffered `ReadFile` and `WriteFile` streams at `MAX_STREAM_FILE_BYTES` (512 MiB), and
//! avoids unary 4 MiB frame limits on both `read_file` and `write_file`.

#![allow(elided_lifetimes_in_paths)]

use std::fmt::Debug;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::time::SystemTime;

use async_trait::async_trait;
use futures::StreamExt as _;
use futures::stream::{self, BoxStream};
use jj_lib::backend::*;
use jj_lib::object_id::ObjectId;
use jj_lib::repo_path::{RepoPath, RepoPathBuf};

use crate::util::{CommitCloudConfig, run_async, run_on_tokio};

mod codec;
mod streaming;

pub use codec::{apply_commit_signing, commit_from_proto, commit_to_proto};
pub use streaming::{tree_entry_from_proto, tree_entry_to_proto};

/// Default client concurrency reported to `jj_lib` so tree and file hydration
/// pipelines up to 16 concurrent RPCs over the pooled HTTP/2 channel.
pub const CLIENT_BACKEND_CONCURRENCY: usize = 16;

#[derive(Debug)]
pub struct CommitCloudBackend {
    server_url: String,
    repo_id: String,
    project_id: String,
    prefer_unary_write_file: AtomicBool,
    root_commit_id: CommitId,
    root_change_id: ChangeId,
    empty_tree_id: TreeId,
}

impl CommitCloudBackend {
    pub fn name() -> &'static str {
        "commit_cloud"
    }

    pub fn repo_id(&self) -> &str {
        &self.repo_id
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn init(
        store_path: &Path,
        server_url: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::init_with_options(store_path, server_url, None, None, true)
    }

    pub fn init_with_options(
        store_path: &Path,
        server_url: &str,
        name: Option<&str>,
        explicit_repo_id: Option<&str>,
        create: bool,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::init_in_project(store_path, server_url, name, None, explicit_repo_id, create)
    }

    pub fn init_in_project(
        store_path: &Path,
        server_url: &str,
        name: Option<&str>,
        explicit_project_id: Option<&str>,
        explicit_repo_id: Option<&str>,
        create: bool,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(raw_repo) = explicit_repo_id
            && raw_repo.trim().is_empty()
        {
            return Err("explicit repo_id must not be empty or whitespace".into());
        }
        if let Some(raw_proj) = explicit_project_id
            && raw_proj.trim().is_empty()
        {
            return Err("explicit project_id must not be empty or whitespace".into());
        }
        if !create && explicit_repo_id.is_none() {
            return Err("existing repo_id is required when create is false".into());
        }

        let root_commit_id = CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES);
        let root_change_id = ChangeId::from_bytes(&cc_common::ROOT_CHANGE_ID_BYTES);
        let empty_tree_id = TreeId::from_hex(cc_common::EMPTY_TREE_ID_HEX);

        let server_url_cloned = server_url.to_string();
        let name_opt = name.map(ToOwned::to_owned);
        let project_id_opt = explicit_project_id.map(ToOwned::to_owned);
        let repo_id_opt = explicit_repo_id.map(ToOwned::to_owned);

        let (repo_id, config_project_id) = match (create, repo_id_opt) {
            // TODO: Replace this attach path with a read-only `GetRepository` RPC in a follow-up
            // PR so `create == false` verifies existence, validates any explicit `--project-id`
            // without calling mutating `RegisterRepository`, and persists the authoritative
            // `project_id` even when `--project-id` is omitted (so local `jj-cc-daemon` L1/L2
            // caches are always shared across repos in the same project).
            (false, Some(existing_repo_id)) => run_async(move || async move {
                let mut op_client =
                    crate::util::connect_op_store_client(server_url_cloned.clone()).await?;
                op_client
                    .get_op_heads(tonic::Request::new(
                        cc_common::op_store::GetOpHeadsRequest {
                            repo_id: existing_repo_id.clone(),
                        },
                    ))
                    .await?;
                if let Some(proj) = project_id_opt {
                    let mut backend_client =
                        crate::util::connect_backend_client(server_url_cloned).await?;
                    let resp = backend_client
                        .register_repository(tonic::Request::new(
                            cc_common::backend::RegisterRepositoryRequest {
                                project_id: proj,
                                repo_id: Some(existing_repo_id.clone()),
                                name: None,
                                hash_algorithm: cc_common::backend::HashAlgorithm::Sha1 as i32,
                            },
                        ))
                        .await?
                        .into_inner();
                    Ok((existing_repo_id, Some(resp.project_id)))
                } else {
                    Ok((existing_repo_id, None))
                }
            })?,
            (true, maybe_repo_id) => run_async(move || async move {
                let mut client = crate::util::connect_backend_client(server_url_cloned).await?;
                let effective_repo_id =
                    maybe_repo_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                let effective_project_id =
                    project_id_opt.unwrap_or_else(|| effective_repo_id.clone());
                let register_repo_response = client
                    .register_repository(tonic::Request::new(
                        cc_common::backend::RegisterRepositoryRequest {
                            project_id: effective_project_id,
                            repo_id: Some(effective_repo_id),
                            name: name_opt,
                            hash_algorithm: cc_common::backend::HashAlgorithm::Sha1 as i32,
                        },
                    ))
                    .await?
                    .into_inner();
                Ok((
                    register_repo_response.repo_id,
                    Some(register_repo_response.project_id),
                ))
            })?,
            (false, None) => {
                unreachable!("create == false without repo_id rejected before RPC")
            }
        };

        let config = CommitCloudConfig {
            server_url: server_url.to_string(),
            repo_id: repo_id.clone(),
            project_id: config_project_id,
        };
        let project_id = config.effective_project_id().to_string();
        config.save_to_store(store_path)?;

        Ok(Self {
            server_url: server_url.to_string(),
            repo_id,
            project_id,
            prefer_unary_write_file: AtomicBool::new(false),
            root_commit_id,
            root_change_id,
            empty_tree_id,
        })
    }

    pub fn from_config(config: CommitCloudConfig) -> Self {
        let project_id = config.effective_project_id().to_string();
        let root_commit_id = CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES);
        let root_change_id = ChangeId::from_bytes(&cc_common::ROOT_CHANGE_ID_BYTES);
        let empty_tree_id = TreeId::from_hex(cc_common::EMPTY_TREE_ID_HEX);
        Self {
            server_url: config.server_url,
            repo_id: config.repo_id,
            project_id,
            prefer_unary_write_file: AtomicBool::new(false),
            root_commit_id,
            root_change_id,
            empty_tree_id,
        }
    }

    pub fn load(store_path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config = CommitCloudConfig::load_from_store(store_path)?;
        Ok(Self::from_config(config))
    }
}

fn validate_response_digest_len(
    bytes: &[u8],
    expected_len: usize,
    field: &str,
) -> BackendResult<()> {
    if bytes.len() != expected_len {
        return Err(BackendError::Other(
            format!(
                "{field} must be {expected_len} bytes (got {} bytes)",
                bytes.len()
            )
            .into(),
        ));
    }
    Ok(())
}

#[async_trait]
impl Backend for CommitCloudBackend {
    fn name(&self) -> &str {
        Self::name()
    }

    fn commit_id_length(&self) -> usize {
        cc_common::COMMIT_ID_LENGTH
    }

    fn change_id_length(&self) -> usize {
        cc_common::CHANGE_ID_LENGTH
    }

    fn root_commit_id(&self) -> &CommitId {
        &self.root_commit_id
    }

    fn root_change_id(&self) -> &ChangeId {
        &self.root_change_id
    }

    fn empty_tree_id(&self) -> &TreeId {
        &self.empty_tree_id
    }

    fn concurrency(&self) -> usize {
        CLIENT_BACKEND_CONCURRENCY
    }

    async fn read_file(
        &self,
        path: &RepoPath,
        id: &FileId,
    ) -> BackendResult<Pin<Box<dyn futures::AsyncRead + Send>>> {
        self.read_file_streaming(path, id).await
    }

    async fn write_file(
        &self,
        path: &RepoPath,
        contents: &mut (dyn futures::AsyncRead + Send + Unpin),
    ) -> BackendResult<FileId> {
        self.write_file_streaming(path, contents).await
    }

    async fn read_symlink(&self, path: &RepoPath, id: &SymlinkId) -> BackendResult<String> {
        self.read_symlink_verified(path, id).await
    }

    async fn write_symlink(&self, path: &RepoPath, target: &str) -> BackendResult<SymlinkId> {
        self.write_symlink_rpc(path, target).await
    }

    async fn read_copy(&self, _id: &CopyId) -> BackendResult<CopyHistory> {
        Err(BackendError::Unsupported(
            "copies not supported".to_string(),
        ))
    }

    async fn write_copy(&self, _contents: &CopyHistory) -> BackendResult<CopyId> {
        Err(BackendError::Unsupported(
            "copies not supported".to_string(),
        ))
    }

    async fn get_related_copies(&self, _copy_id: &CopyId) -> BackendResult<Vec<RelatedCopy>> {
        Err(BackendError::Unsupported(
            "copies not supported".to_string(),
        ))
    }

    async fn read_tree(&self, path: &RepoPath, id: &TreeId) -> BackendResult<Tree> {
        self.read_tree_paginated(path, id).await
    }

    async fn write_tree(&self, path: &RepoPath, tree: &Tree) -> BackendResult<TreeId> {
        self.write_tree_rpc(path, tree).await
    }

    async fn read_commit(&self, id: &CommitId) -> BackendResult<Commit> {
        if *id == self.root_commit_id {
            return Ok(make_root_commit(
                self.root_change_id().clone(),
                self.empty_tree_id.clone(),
            ));
        }

        let server_url = self.server_url.clone();
        let project_id = self.project_id.clone();
        let commit_id_bytes = id.to_bytes().to_vec();
        let commit_id_hex = id.hex();

        let proto_commit = run_on_tokio(async move {
            let mut client = crate::util::connect_backend_client(server_url).await?;
            let res = client
                .read_commit(tonic::Request::new(cc_common::backend::ReadCommitRequest {
                    project_id,
                    commit_id: commit_id_bytes,
                }))
                .await;

            match res {
                Ok(r) => r.into_inner().commit.ok_or_else(|| {
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "server response should have contained commit data",
                    )) as Box<dyn std::error::Error + Send + Sync>
                }),
                Err(status) if status.code() == tonic::Code::NotFound => {
                    Err(Box::new(BackendError::ObjectNotFound {
                        object_type: "commit".into(),
                        hash: commit_id_hex,
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

        codec::verified_commit_from_proto(id, proto_commit)
    }

    async fn write_commit(
        &self,
        mut commit: Commit,
        sign_with: Option<&mut SigningFn>,
    ) -> BackendResult<(CommitId, Commit)> {
        if commit.parents.contains(&self.root_commit_id) && commit.parents.len() > 1 {
            return Err(BackendError::Unsupported(
                "The Commit Cloud backend does not support creating merge commits with the root commit as one of the parents."
                    .to_owned(),
            ));
        }

        apply_commit_signing(&mut commit, sign_with)?;
        let proto_commit = commit_to_proto(&commit);
        let server_url = self.server_url.clone();
        let project_id = self.project_id.clone();

        let returned_id_bytes = run_on_tokio(async move {
            let mut client = crate::util::connect_backend_client(server_url).await?;
            let res = client
                .write_commit(tonic::Request::new(
                    cc_common::backend::WriteCommitRequest {
                        project_id,
                        commit: Some(proto_commit),
                    },
                ))
                .await?;
            Ok(res.into_inner().commit_id)
        })
        .await
        .map_err(BackendError::Other)?;

        validate_response_digest_len(
            &returned_id_bytes,
            cc_common::COMMIT_ID_LENGTH,
            "WriteCommit response commit_id",
        )?;
        let returned_id = CommitId::from_bytes(&returned_id_bytes);
        Ok((returned_id, commit))
    }

    fn get_copy_records(
        &self,
        _paths: Option<&[RepoPathBuf]>,
        _root: &CommitId,
        _head: &CommitId,
    ) -> BackendResult<BoxStream<'_, BackendResult<CopyRecord>>> {
        Ok(stream::empty().boxed())
    }

    async fn gc(&self, _heads: &[CommitId], _keep_newer: SystemTime) -> BackendResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
