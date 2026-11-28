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

//! `BackendService` gRPC handler validating object digests, dependencies, directory entry
//! ordering, and project/repository namespaces over [`Store`].
//!
//! # Authority and Crash-Safety Ordering
//! `WriteCommit`, `WriteTree`, `WriteFile`, and `WriteSymlink` compute canonical Git SHA-1
//! digests on the server (including `SecureSig.data` derivation and `raw_git_commit` cross-
//! verification) and verify child blob, subtree, symlink, and parent-commit dependencies
//! (`FAILED_PRECONDITION`) before persisting into [`Store`]. `WriteTree` populates
//! `File.size_bytes` from stored blob lengths and sorts entries in UTF-8 `RepoPathComponent`
//! byte order so `ReadTree` always satisfies `jj_lib::backend::Tree::from_sorted_entries`.
//!
//! # Concurrency and Merge Invariants
//! `RegisterProject` and `RegisterRepository` validate identifiers, remain non-destructive on
//! repeat calls, and reject conflicting `repo_id -> project_id` re-bindings with
//! `ALREADY_EXISTS`.
use std::sync::Arc;
use tracing::info;

use cc_common::backend::backend_service_server::BackendService;
use cc_common::backend::*;

use crate::error_util::ensure_project_registered_error;
use crate::hash_utils::{
    compute_git_tree_hash, try_compute_git_blob_hash, verify_or_compute_git_commit_hash,
};
use crate::store::Store;

const MAX_IDENTIFIER_LEN: usize = 128;
const DEFAULT_LIST_COMMITS_PAGE_SIZE: usize = 5000;
const MAX_LIST_COMMITS_PAGE_SIZE: usize = 5000;

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
    store: Arc<dyn Store>,
}

impl CommitCloudBackendService {
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl BackendService for CommitCloudBackendService {
    async fn register_project(
        &self,
        request: tonic::Request<RegisterProjectRequest>,
    ) -> Result<tonic::Response<RegisterProjectResponse>, tonic::Status> {
        let req = request.into_inner();
        let project_id = match req.project_id {
            None => uuid::Uuid::new_v4().to_string(),
            Some(raw) => {
                validate_identifier(&raw, "project_id")?;
                raw
            }
        };
        info!("Registering project: {} (name: {:?})", project_id, req.name);
        self.store
            .register_project(project_id.clone(), req.name)
            .await?;
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
            None => uuid::Uuid::new_v4().to_string(),
            Some(raw) => {
                validate_identifier(&raw, "repo_id")?;
                raw
            }
        };
        let project_id = if req.project_id.is_empty() {
            repo_id.clone()
        } else {
            validate_identifier(&req.project_id, "project_id")?;
            req.project_id
        };

        info!(
            "Registering repository: {} in project {} (name: {:?})",
            repo_id, project_id, req.name
        );
        self.store
            .register_repo(repo_id.clone(), project_id.clone(), req.name)
            .await?;
        Ok(tonic::Response::new(RegisterRepositoryResponse {
            repo_id,
            project_id,
        }))
    }

    async fn read_commit(
        &self,
        request: tonic::Request<ReadCommitRequest>,
    ) -> Result<tonic::Response<ReadCommitResponse>, tonic::Status> {
        let req = request.into_inner();
        validate_identifier(&req.project_id, "project_id")?;
        validate_digest_len(&req.commit_id, cc_common::COMMIT_ID_LENGTH, "commit_id")?;

        ensure_project_registered_error(self.store.as_ref(), &req.project_id, "requesting commits")
            .await?;

        if let Some(commit) = self
            .store
            .get_commit(&req.project_id, &req.commit_id)
            .await?
        {
            return Ok(tonic::Response::new(ReadCommitResponse {
                commit: Some(commit),
            }));
        }
        if req.commit_id.as_slice() == cc_common::ROOT_COMMIT_ID_BYTES {
            let root = Commit {
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
            };
            return Ok(tonic::Response::new(ReadCommitResponse {
                commit: Some(root),
            }));
        }
        Err(tonic::Status::not_found(
            "commit should have been present in cloud database",
        ))
    }

    async fn write_commit(
        &self,
        request: tonic::Request<WriteCommitRequest>,
    ) -> Result<tonic::Response<WriteCommitResponse>, tonic::Status> {
        let req = request.into_inner();
        validate_identifier(&req.project_id, "project_id")?;

        ensure_project_registered_error(self.store.as_ref(), &req.project_id, "writing commits")
            .await?;

        let mut commit = req.commit.ok_or_else(|| {
            tonic::Status::invalid_argument("request should have contained commit data")
        })?;
        if !commit.external_ids.is_empty() {
            return Err(tonic::Status::unimplemented(
                "Commit.external_ids is not implemented yet",
            ));
        }

        let commit_id = verify_or_compute_git_commit_hash(&mut commit)
            .map_err(tonic::Status::invalid_argument)?;

        for parent_id in &commit.parent_commit_ids {
            if parent_id.as_slice() != cc_common::ROOT_COMMIT_ID_BYTES
                && self
                    .store
                    .get_commit(&req.project_id, parent_id)
                    .await?
                    .is_none()
            {
                return Err(tonic::Status::failed_precondition(format!(
                    "parent commit {} does not exist in project '{}'; write parent commits before child commits",
                    hex::encode(parent_id),
                    req.project_id
                )));
            }
        }
        for tree_id in &commit.root_tree_id {
            if tree_id.as_slice() != cc_common::EMPTY_TREE_ID_BYTES
                && self
                    .store
                    .get_tree(&req.project_id, tree_id)
                    .await?
                    .is_none()
            {
                return Err(tonic::Status::failed_precondition(format!(
                    "root tree {} does not exist in project '{}'; write trees before commits",
                    hex::encode(tree_id),
                    req.project_id
                )));
            }
        }

        commit.commit_id = commit_id.clone();
        info!(
            "Writing commit {} for project {}",
            hex::encode(&commit_id),
            req.project_id
        );

        self.store
            .put_commit(req.project_id, commit_id.clone(), commit)
            .await?;

        Ok(tonic::Response::new(WriteCommitResponse { commit_id }))
    }

    async fn list_commits(
        &self,
        request: tonic::Request<ListCommitsRequest>,
    ) -> Result<tonic::Response<ListCommitsResponse>, tonic::Status> {
        let req = request.into_inner();
        validate_identifier(&req.project_id, "project_id")?;
        if !req.since_token.is_empty() {
            return Err(tonic::Status::unimplemented(
                "incremental ListCommits since_token watermarks are not implemented yet",
            ));
        }

        ensure_project_registered_error(self.store.as_ref(), &req.project_id, "listing commits")
            .await?;

        let cursor = if req.page_token.is_empty() {
            None
        } else {
            validate_digest_len(&req.page_token, cc_common::COMMIT_ID_LENGTH, "page_token")?;
            Some(req.page_token)
        };
        let limit = if req.page_size == 0 {
            DEFAULT_LIST_COMMITS_PAGE_SIZE
        } else {
            (req.page_size as usize).min(MAX_LIST_COMMITS_PAGE_SIZE)
        };

        let mut all_ids = self.store.list_project_commit_ids(&req.project_id).await?;
        all_ids.sort();
        all_ids.dedup();

        let filtered: Vec<Vec<u8>> = match &cursor {
            Some(c) => all_ids.into_iter().filter(|id| id > c).collect(),
            None => all_ids,
        };
        let has_more = filtered.len() > limit;
        let commit_ids: Vec<Vec<u8>> = filtered.into_iter().take(limit).collect();
        let next_page_token = if has_more {
            commit_ids.last().expect("limit >= 1").clone()
        } else {
            Vec::new()
        };

        Ok(tonic::Response::new(ListCommitsResponse {
            commit_ids,
            next_page_token,
            sync_token: Vec::new(),
        }))
    }

    async fn resolve_commit_prefix(
        &self,
        _request: tonic::Request<ResolveCommitPrefixRequest>,
    ) -> Result<tonic::Response<ResolveCommitPrefixResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "ResolveCommitPrefix is not implemented yet",
        ))
    }

    async fn resolve_change_prefix(
        &self,
        _request: tonic::Request<ResolveChangePrefixRequest>,
    ) -> Result<tonic::Response<ResolveChangePrefixResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "ResolveChangePrefix is not implemented yet",
        ))
    }

    async fn read_tree(
        &self,
        request: tonic::Request<ReadTreeRequest>,
    ) -> Result<tonic::Response<ReadTreeResponse>, tonic::Status> {
        let req = request.into_inner();
        validate_identifier(&req.project_id, "project_id")?;
        validate_digest_len(&req.tree_id, cc_common::TREE_ID_LENGTH, "tree_id")?;
        if req.page_size != 0 || !req.page_token.is_empty() {
            return Err(tonic::Status::unimplemented(
                "paginated ReadTree is not implemented yet",
            ));
        }

        ensure_project_registered_error(self.store.as_ref(), &req.project_id, "requesting trees")
            .await?;

        if req.tree_id == cc_common::EMPTY_TREE_ID_BYTES {
            return Ok(tonic::Response::new(ReadTreeResponse {
                tree_id: req.tree_id,
                entries: vec![],
                next_page_token: String::new(),
            }));
        }

        if let Some(mut entries) = self.store.get_tree(&req.project_id, &req.tree_id).await? {
            entries.sort_by(|a, b| a.name.cmp(&b.name));
            return Ok(tonic::Response::new(ReadTreeResponse {
                tree_id: req.tree_id,
                entries,
                next_page_token: String::new(),
            }));
        }
        Err(tonic::Status::not_found(
            "tree should have been present in cloud database",
        ))
    }

    async fn write_tree(
        &self,
        request: tonic::Request<WriteTreeRequest>,
    ) -> Result<tonic::Response<WriteTreeResponse>, tonic::Status> {
        let req = request.into_inner();
        validate_identifier(&req.project_id, "project_id")?;

        ensure_project_registered_error(self.store.as_ref(), &req.project_id, "writing trees")
            .await?;

        let tree_id =
            compute_git_tree_hash(&req.entries).map_err(tonic::Status::invalid_argument)?;

        let mut entries = req.entries;
        for entry in &mut entries {
            if let Some(tv) = entry.value.as_mut() {
                match tv.value.as_mut() {
                    Some(tree_value::Value::File(f)) => {
                        if !f.copy_id.is_empty() {
                            return Err(tonic::Status::invalid_argument(format!(
                                "file entry '{}' sets non-empty copy_id, which is not supported",
                                entry.name
                            )));
                        }
                        // TODO: Switch to a metadata-only blob size lookup once `Store` splits
                        // metadata and blob storage (avoiding a full blob read just for size).
                        let content = self
                            .store
                            .get_file(&req.project_id, &f.id)
                            .await?
                            .ok_or_else(|| {
                                tonic::Status::failed_precondition(format!(
                                    "file blob {} referenced by '{}' does not exist in project '{}'; write files before trees",
                                    hex::encode(&f.id),
                                    entry.name,
                                    req.project_id
                                ))
                            })?;
                        f.size_bytes = Some(content.len() as u64);
                    }
                    Some(tree_value::Value::TreeId(tid)) => {
                        if tid.as_slice() != cc_common::EMPTY_TREE_ID_BYTES
                            && self.store.get_tree(&req.project_id, tid).await?.is_none()
                        {
                            return Err(tonic::Status::failed_precondition(format!(
                                "child tree {} referenced by '{}' does not exist in project '{}'; write subtrees before parent trees",
                                hex::encode(tid),
                                entry.name,
                                req.project_id
                            )));
                        }
                    }
                    Some(tree_value::Value::SymlinkId(sid)) => {
                        let sid = sid.as_slice();
                        let raw_target = self
                            .store
                            .get_file(&req.project_id, sid)
                            .await?
                            .ok_or_else(|| {
                                tonic::Status::failed_precondition(format!(
                                    "symlink target {} referenced by '{}' does not exist in project '{}'; write symlinks before trees",
                                    hex::encode(sid),
                                    entry.name,
                                    req.project_id
                                ))
                            })?;
                        let target = std::str::from_utf8(&raw_target).map_err(|_| {
                            tonic::Status::invalid_argument(format!(
                                "symlink target {} referenced by '{}' is not valid UTF-8",
                                hex::encode(sid),
                                entry.name
                            ))
                        })?;
                        if target.contains('\0') {
                            return Err(tonic::Status::invalid_argument(format!(
                                "symlink target {} referenced by '{}' must not contain NUL bytes",
                                hex::encode(sid),
                                entry.name
                            )));
                        }
                    }
                    Some(tree_value::Value::GitSubmoduleId(_)) => {}
                    Some(tree_value::Value::ConflictId(_)) | None => {
                        return Err(tonic::Status::invalid_argument(format!(
                            "tree entry '{}' has missing or unsupported TreeValue",
                            entry.name
                        )));
                    }
                }
            }
        }
        // Persist in UTF-8 RepoPathComponent order so ReadTree matches Tree::from_sorted_entries.
        entries.sort_by(|a, b| a.name.cmp(&b.name));

        self.store
            .put_tree(req.project_id, tree_id.clone(), entries)
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
        if req.offset != 0 || req.max_bytes != 0 {
            return Err(tonic::Status::unimplemented(
                "ranged ReadFile(offset, max_bytes) is not implemented yet",
            ));
        }

        ensure_project_registered_error(self.store.as_ref(), &req.project_id, "requesting files")
            .await?;

        let content = self
            .store
            .get_file(&req.project_id, &req.file_id)
            .await?
            .ok_or_else(|| {
                tonic::Status::not_found("file should have been present in cloud database")
            })?;

        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            for chunk in content.chunks(64 * 1024) {
                if tx
                    .send(Ok(ReadFileResponse {
                        chunk: chunk.to_vec(),
                    }))
                    .await
                    .is_err()
                {
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

        ensure_project_registered_error(self.store.as_ref(), &req.project_id, "writing files")
            .await?;

        let file_id =
            try_compute_git_blob_hash(&req.content).map_err(tonic::Status::invalid_argument)?;

        self.store
            .put_file(req.project_id, file_id.clone(), req.content)
            .await?;

        Ok(tonic::Response::new(WriteFileResponse { file_id }))
    }

    async fn write_file_stream(
        &self,
        _request: tonic::Request<tonic::Streaming<WriteFileChunk>>,
    ) -> Result<tonic::Response<WriteFileResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "WriteFileStream is not implemented yet",
        ))
    }

    async fn read_symlink(
        &self,
        request: tonic::Request<ReadSymlinkRequest>,
    ) -> Result<tonic::Response<ReadSymlinkResponse>, tonic::Status> {
        let req = request.into_inner();
        validate_identifier(&req.project_id, "project_id")?;
        validate_digest_len(&req.symlink_id, cc_common::SYMLINK_ID_LENGTH, "symlink_id")?;

        ensure_project_registered_error(
            self.store.as_ref(),
            &req.project_id,
            "requesting symlinks",
        )
        .await?;

        let raw = self
            .store
            .get_file(&req.project_id, &req.symlink_id)
            .await?
            .ok_or_else(|| {
                tonic::Status::not_found("symlink should have been present in cloud database")
            })?;
        let target = String::from_utf8(raw).map_err(|_| {
            tonic::Status::data_loss("stored symlink target bytes are not valid UTF-8")
        })?;
        if target.contains('\0') {
            return Err(tonic::Status::data_loss(
                "stored symlink target contains NUL byte",
            ));
        }
        Ok(tonic::Response::new(ReadSymlinkResponse { target }))
    }

    async fn write_symlink(
        &self,
        request: tonic::Request<WriteSymlinkRequest>,
    ) -> Result<tonic::Response<WriteSymlinkResponse>, tonic::Status> {
        let req = request.into_inner();
        validate_identifier(&req.project_id, "project_id")?;
        if req.target.contains('\0') {
            return Err(tonic::Status::invalid_argument(
                "symlink target must not contain NUL bytes",
            ));
        }

        ensure_project_registered_error(self.store.as_ref(), &req.project_id, "writing symlinks")
            .await?;

        let raw = req.target.into_bytes();
        let symlink_id =
            try_compute_git_blob_hash(&raw).map_err(tonic::Status::invalid_argument)?;
        self.store
            .put_file(req.project_id, symlink_id.clone(), raw)
            .await?;

        Ok(tonic::Response::new(WriteSymlinkResponse { symlink_id }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryStore;
    use tonic::Code;

    fn test_service() -> CommitCloudBackendService {
        CommitCloudBackendService::new(Arc::new(MemoryStore::default()))
    }

    #[tokio::test]
    async fn test_register_repository_conflict_and_validation() {
        let svc = test_service();
        svc.register_repository(tonic::Request::new(RegisterRepositoryRequest {
            project_id: "proj-a".to_string(),
            repo_id: Some("repo-1".to_string()),
            name: None,
            hash_algorithm: HashAlgorithm::Sha1 as i32,
        }))
        .await
        .expect("initial registration should succeed");

        // Idempotent re-registration in the same project succeeds.
        svc.register_repository(tonic::Request::new(RegisterRepositoryRequest {
            project_id: "proj-a".to_string(),
            repo_id: Some("repo-1".to_string()),
            name: None,
            hash_algorithm: HashAlgorithm::Unspecified as i32,
        }))
        .await
        .expect("idempotent re-registration should succeed");

        // Conflicting re-binding to a different project fails with ALREADY_EXISTS.
        let conflict = svc
            .register_repository(tonic::Request::new(RegisterRepositoryRequest {
                project_id: "proj-b".to_string(),
                repo_id: Some("repo-1".to_string()),
                name: None,
                hash_algorithm: HashAlgorithm::Sha1 as i32,
            }))
            .await
            .unwrap_err();
        assert_eq!(conflict.code(), Code::AlreadyExists);

        // SHA-256 returns UNIMPLEMENTED and empty repo_id returns INVALID_ARGUMENT.
        let sha256_err = svc
            .register_repository(tonic::Request::new(RegisterRepositoryRequest {
                project_id: "proj-a".to_string(),
                repo_id: None,
                name: None,
                hash_algorithm: HashAlgorithm::Sha256 as i32,
            }))
            .await
            .unwrap_err();
        assert_eq!(sha256_err.code(), Code::Unimplemented);

        let empty_repo_err = svc
            .register_repository(tonic::Request::new(RegisterRepositoryRequest {
                project_id: "proj-a".to_string(),
                repo_id: Some(String::new()),
                name: None,
                hash_algorithm: HashAlgorithm::Sha1 as i32,
            }))
            .await
            .unwrap_err();
        assert_eq!(empty_repo_err.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_write_tree_utf8_order_size_bytes_and_duplicate_rejection() {
        let svc = test_service();
        svc.register_project(tonic::Request::new(RegisterProjectRequest {
            project_id: Some("proj-tree".to_string()),
            name: None,
        }))
        .await
        .unwrap();

        let file_a = svc
            .write_file(tonic::Request::new(WriteFileRequest {
                project_id: "proj-tree".to_string(),
                content: b"hello".to_vec(),
            }))
            .await
            .unwrap()
            .into_inner()
            .file_id;
        let file_b = svc
            .write_file(tonic::Request::new(WriteFileRequest {
                project_id: "proj-tree".to_string(),
                content: b"world!".to_vec(),
            }))
            .await
            .unwrap()
            .into_inner()
            .file_id;

        // Duplicate entry names across a file and a directory must be rejected.
        let dup_err = svc
            .write_tree(tonic::Request::new(WriteTreeRequest {
                project_id: "proj-tree".to_string(),
                path: String::new(),
                entries: vec![
                    TreeEntry {
                        name: "foo".to_string(),
                        value: Some(TreeValue {
                            value: Some(tree_value::Value::File(File {
                                id: file_a.clone(),
                                executable: false,
                                copy_id: Vec::new(),
                                size_bytes: None,
                            })),
                        }),
                    },
                    TreeEntry {
                        name: "foo-bar".to_string(),
                        value: Some(TreeValue {
                            value: Some(tree_value::Value::File(File {
                                id: file_b.clone(),
                                executable: false,
                                copy_id: Vec::new(),
                                size_bytes: None,
                            })),
                        }),
                    },
                    TreeEntry {
                        name: "foo".to_string(),
                        value: Some(TreeValue {
                            value: Some(tree_value::Value::TreeId(
                                cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                            )),
                        }),
                    },
                ],
            }))
            .await
            .unwrap_err();
        assert_eq!(dup_err.code(), Code::InvalidArgument);

        // Out-of-order entries are persisted and returned in UTF-8 RepoPathComponent order
        // (`"foo" < "foo.rs"`) with server-populated `File.size_bytes`.
        let tree_id = svc
            .write_tree(tonic::Request::new(WriteTreeRequest {
                project_id: "proj-tree".to_string(),
                path: String::new(),
                entries: vec![
                    TreeEntry {
                        name: "foo.rs".to_string(),
                        value: Some(TreeValue {
                            value: Some(tree_value::Value::File(File {
                                id: file_b,
                                executable: false,
                                copy_id: Vec::new(),
                                size_bytes: None,
                            })),
                        }),
                    },
                    TreeEntry {
                        name: "foo".to_string(),
                        value: Some(TreeValue {
                            value: Some(tree_value::Value::TreeId(
                                cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                            )),
                        }),
                    },
                    TreeEntry {
                        name: "sub".to_string(),
                        value: Some(TreeValue {
                            value: Some(tree_value::Value::GitSubmoduleId(vec![0xaa; 20])),
                        }),
                    },
                ],
            }))
            .await
            .unwrap()
            .into_inner()
            .tree_id;

        let read_resp = svc
            .read_tree(tonic::Request::new(ReadTreeRequest {
                project_id: "proj-tree".to_string(),
                path: String::new(),
                tree_id,
                page_size: 0,
                page_token: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        let names: Vec<&str> = read_resp.entries.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["foo", "foo.rs", "sub"]);
        match read_resp.entries[1]
            .value
            .as_ref()
            .and_then(|v| v.value.as_ref())
        {
            Some(tree_value::Value::File(f)) => assert_eq!(f.size_bytes, Some(6)),
            other => panic!("expected file entry with size_bytes=Some(6), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_list_commits_pagination_and_secure_sig_rejection() {
        let svc = test_service();
        svc.register_project(tonic::Request::new(RegisterProjectRequest {
            project_id: Some("proj-list".to_string()),
            name: None,
        }))
        .await
        .unwrap();

        let make_commit = |desc: &str, ch: u8| Commit {
            commit_id: Vec::new(),
            change_id: vec![ch; cc_common::CHANGE_ID_LENGTH],
            parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
            root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
            description: desc.to_string(),
            author: None,
            committer: None,
            predecessors: Vec::new(),
            conflict_labels: Vec::new(),
            secure_sig: None,
            raw_git_commit: Vec::new(),
            external_ids: Default::default(),
        };

        // Forged SecureSig.data that does not match canonical commit payload is rejected.
        let mut forged = make_commit("forged", 1);
        forged.secure_sig = Some(SecureSig {
            data: b"stolen payload from another commit".to_vec(),
            sig: b"-----BEGIN PGP SIGNATURE-----\nfake\n-----END PGP SIGNATURE-----\n".to_vec(),
        });
        let forged_err = svc
            .write_commit(tonic::Request::new(WriteCommitRequest {
                project_id: "proj-list".to_string(),
                commit: Some(forged),
            }))
            .await
            .unwrap_err();
        assert_eq!(forged_err.code(), Code::InvalidArgument);

        // Write 3 commits and page through them with page_size = 2.
        let mut expected = Vec::new();
        for (i, desc) in ["c1", "c2", "c3"].iter().enumerate() {
            let cid = svc
                .write_commit(tonic::Request::new(WriteCommitRequest {
                    project_id: "proj-list".to_string(),
                    commit: Some(make_commit(desc, (i + 2) as u8)),
                }))
                .await
                .unwrap()
                .into_inner()
                .commit_id;
            expected.push(cid);
        }
        expected.sort();

        let p1 = svc
            .list_commits(tonic::Request::new(ListCommitsRequest {
                project_id: "proj-list".to_string(),
                page_size: 2,
                page_token: Vec::new(),
                since_token: Vec::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(p1.commit_ids.len(), 2);
        assert!(!p1.next_page_token.is_empty());

        let p2 = svc
            .list_commits(tonic::Request::new(ListCommitsRequest {
                project_id: "proj-list".to_string(),
                page_size: 2,
                page_token: p1.next_page_token,
                since_token: Vec::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(p2.commit_ids.len(), 1);
        assert!(p2.next_page_token.is_empty());

        let mut actual = p1.commit_ids;
        actual.extend(p2.commit_ids);
        assert_eq!(actual, expected);

        // Unimplemented new RPCs and options fail with UNIMPLEMENTED.
        let since_err = svc
            .list_commits(tonic::Request::new(ListCommitsRequest {
                project_id: "proj-list".to_string(),
                page_size: 10,
                page_token: Vec::new(),
                since_token: b"wm-1".to_vec(),
            }))
            .await
            .unwrap_err();
        assert_eq!(since_err.code(), Code::Unimplemented);
        assert_eq!(
            svc.resolve_commit_prefix(tonic::Request::new(ResolveCommitPrefixRequest {
                project_id: "proj-list".to_string(),
                hex_prefix: "abcd".to_string(),
            }))
            .await
            .unwrap_err()
            .code(),
            Code::Unimplemented
        );
        assert_eq!(
            svc.resolve_change_prefix(tonic::Request::new(ResolveChangePrefixRequest {
                project_id: "proj-list".to_string(),
                prefix: "abcd".to_string(),
                repo_id: None,
                visible_head_ids: Vec::new(),
                max_results: 0,
            }))
            .await
            .unwrap_err()
            .code(),
            Code::Unimplemented
        );
    }

    #[tokio::test]
    async fn test_register_repository_empty_project_defaults_to_repo_id() {
        let svc = test_service();
        let resp = svc
            .register_repository(tonic::Request::new(RegisterRepositoryRequest {
                project_id: String::new(),
                repo_id: Some("solo-repo".to_string()),
                name: None,
                hash_algorithm: HashAlgorithm::Sha1 as i32,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.repo_id, "solo-repo");
        assert_eq!(resp.project_id, "solo-repo");

        // Auto-generated repo_id also becomes project_id when project_id is empty.
        let resp2 = svc
            .register_repository(tonic::Request::new(RegisterRepositoryRequest {
                project_id: String::new(),
                repo_id: None,
                name: None,
                hash_algorithm: HashAlgorithm::Unspecified as i32,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp2.repo_id.is_empty());
        assert_eq!(resp2.project_id, resp2.repo_id);
    }

    #[tokio::test]
    async fn test_write_commit_external_ids_and_write_tree_copy_id_and_symlink_nul_rejected() {
        let svc = test_service();
        svc.register_project(tonic::Request::new(RegisterProjectRequest {
            project_id: Some("proj-strict".to_string()),
            name: None,
        }))
        .await
        .unwrap();

        // Non-empty external_ids returns UNIMPLEMENTED.
        let mut external_ids = std::collections::HashMap::new();
        external_ids.insert("git-sha1".to_string(), vec![0x42; 20]);
        let ext_err = svc
            .write_commit(tonic::Request::new(WriteCommitRequest {
                project_id: "proj-strict".to_string(),
                commit: Some(Commit {
                    commit_id: Vec::new(),
                    change_id: vec![0x11; 16],
                    parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                    root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
                    description: "ext".to_string(),
                    author: None,
                    committer: None,
                    predecessors: Vec::new(),
                    conflict_labels: Vec::new(),
                    secure_sig: None,
                    raw_git_commit: Vec::new(),
                    external_ids,
                }),
            }))
            .await
            .unwrap_err();
        assert_eq!(ext_err.code(), Code::Unimplemented);

        // Non-empty File.copy_id rejected by WriteTree.
        let file_id = svc
            .write_file(tonic::Request::new(WriteFileRequest {
                project_id: "proj-strict".to_string(),
                content: b"hello".to_vec(),
            }))
            .await
            .unwrap()
            .into_inner()
            .file_id;
        let copy_err = svc
            .write_tree(tonic::Request::new(WriteTreeRequest {
                project_id: "proj-strict".to_string(),
                path: String::new(),
                entries: vec![TreeEntry {
                    name: "a.txt".to_string(),
                    value: Some(TreeValue {
                        value: Some(tree_value::Value::File(File {
                            id: file_id,
                            executable: false,
                            copy_id: vec![0x01; 20],
                            size_bytes: None,
                        })),
                    }),
                }],
            }))
            .await
            .unwrap_err();
        assert_eq!(copy_err.code(), Code::InvalidArgument);

        // Cross-type NUL-containing file blob cannot be referenced as a symlink in WriteTree.
        let nul_blob_id = svc
            .write_file(tonic::Request::new(WriteFileRequest {
                project_id: "proj-strict".to_string(),
                content: b"bad\0target".to_vec(),
            }))
            .await
            .unwrap()
            .into_inner()
            .file_id;
        let sym_err = svc
            .write_tree(tonic::Request::new(WriteTreeRequest {
                project_id: "proj-strict".to_string(),
                path: String::new(),
                entries: vec![TreeEntry {
                    name: "badlink".to_string(),
                    value: Some(TreeValue {
                        value: Some(tree_value::Value::SymlinkId(nul_blob_id)),
                    }),
                }],
            }))
            .await
            .unwrap_err();
        assert_eq!(sym_err.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_write_and_read_symlink_roundtrip_nul_rejection_and_not_found() {
        let store = Arc::new(MemoryStore::default());
        let svc = CommitCloudBackendService::new(store.clone());
        svc.register_project(tonic::Request::new(RegisterProjectRequest {
            project_id: Some("proj-sym".to_string()),
            name: None,
        }))
        .await
        .unwrap();

        let target = "../shared/target.txt";
        let symlink_id = svc
            .write_symlink(tonic::Request::new(WriteSymlinkRequest {
                project_id: "proj-sym".to_string(),
                target: target.to_string(),
            }))
            .await
            .unwrap()
            .into_inner()
            .symlink_id;
        assert_eq!(
            symlink_id,
            try_compute_git_blob_hash(target.as_bytes()).unwrap()
        );

        let read_resp = svc
            .read_symlink(tonic::Request::new(ReadSymlinkRequest {
                project_id: "proj-sym".to_string(),
                symlink_id,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(read_resp.target, target);

        // NUL bytes in target rejected by WriteSymlink.
        let nul_err = svc
            .write_symlink(tonic::Request::new(WriteSymlinkRequest {
                project_id: "proj-sym".to_string(),
                target: "bad\0target".to_string(),
            }))
            .await
            .unwrap_err();
        assert_eq!(nul_err.code(), Code::InvalidArgument);

        // Absent symlink_id returns NOT_FOUND.
        let missing_err = svc
            .read_symlink(tonic::Request::new(ReadSymlinkRequest {
                project_id: "proj-sym".to_string(),
                symlink_id: vec![0x99; cc_common::SYMLINK_ID_LENGTH],
            }))
            .await
            .unwrap_err();
        assert_eq!(missing_err.code(), Code::NotFound);

        // Corrupt non-UTF-8 or NUL-bearing blob returns DATA_LOSS.
        let bad_utf8_id = vec![0x9a; cc_common::SYMLINK_ID_LENGTH];
        store
            .put_file(
                "proj-sym".to_string(),
                bad_utf8_id.clone(),
                vec![0xff, 0xfe],
            )
            .await
            .unwrap();
        let data_loss_err = svc
            .read_symlink(tonic::Request::new(ReadSymlinkRequest {
                project_id: "proj-sym".to_string(),
                symlink_id: bad_utf8_id,
            }))
            .await
            .unwrap_err();
        assert_eq!(data_loss_err.code(), Code::DataLoss);
    }

    #[tokio::test]
    async fn test_unimplemented_guards_on_write_file_stream_ranged_read_file_and_paginated_read_tree()
     {
        let svc = test_service();
        svc.register_project(tonic::Request::new(RegisterProjectRequest {
            project_id: Some("proj-unimpl".to_string()),
            name: None,
        }))
        .await
        .unwrap();

        let file_id = svc
            .write_file(tonic::Request::new(WriteFileRequest {
                project_id: "proj-unimpl".to_string(),
                content: b"hello world".to_vec(),
            }))
            .await
            .unwrap()
            .into_inner()
            .file_id;
        let tree_id = svc
            .write_tree(tonic::Request::new(WriteTreeRequest {
                project_id: "proj-unimpl".to_string(),
                path: String::new(),
                entries: vec![TreeEntry {
                    name: "a.txt".to_string(),
                    value: Some(TreeValue {
                        value: Some(tree_value::Value::File(File {
                            id: file_id.clone(),
                            executable: false,
                            copy_id: Vec::new(),
                            size_bytes: None,
                        })),
                    }),
                }],
            }))
            .await
            .unwrap()
            .into_inner()
            .tree_id;

        // Ranged ReadFile(offset, max_bytes) returns UNIMPLEMENTED.
        for (offset, max_bytes) in [(1, 0), (0, 4), (2, 4)] {
            let err = svc
                .read_file(tonic::Request::new(ReadFileRequest {
                    project_id: "proj-unimpl".to_string(),
                    file_id: file_id.clone(),
                    offset,
                    max_bytes,
                }))
                .await
                .unwrap_err();
            assert_eq!(
                err.code(),
                Code::Unimplemented,
                "expected UNIMPLEMENTED for ReadFile(offset={offset}, max_bytes={max_bytes})"
            );
        }

        // Paginated ReadTree returns UNIMPLEMENTED.
        for (page_size, page_token) in [(1, ""), (0, "a"), (10, "a")] {
            let err = svc
                .read_tree(tonic::Request::new(ReadTreeRequest {
                    project_id: "proj-unimpl".to_string(),
                    path: String::new(),
                    tree_id: tree_id.clone(),
                    page_size,
                    page_token: page_token.to_string(),
                }))
                .await
                .unwrap_err();
            assert_eq!(
                err.code(),
                Code::Unimplemented,
                "expected UNIMPLEMENTED for ReadTree(page_size={page_size}, page_token={page_token:?})"
            );
        }

        // WriteFileStream returns UNIMPLEMENTED over gRPC.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let incoming = futures::stream::unfold(listener, |l| async move {
            Some((l.accept().await.map(|(stream, _)| stream), l))
        });
        let server_svc = svc.clone();
        let server_task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    cc_common::backend::backend_service_server::BackendServiceServer::new(
                        server_svc,
                    ),
                )
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .unwrap();
        });

        let mut client = cc_common::backend::backend_service_client::BackendServiceClient::connect(
            format!("http://{addr}"),
        )
        .await
        .unwrap();
        let stream_err = client
            .write_file_stream(tokio_stream::iter(vec![WriteFileChunk {
                project_id: "proj-unimpl".to_string(),
                chunk: b"streamed".to_vec(),
                total_size: Some(8),
                expected_file_id: None,
            }]))
            .await
            .unwrap_err();
        assert_eq!(stream_err.code(), Code::Unimplemented);

        let _ = shutdown_tx.send(());
        let _ = server_task.await;
    }
}
