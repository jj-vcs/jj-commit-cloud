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

//! `BackendService` gRPC handler validating object digests, dependencies, directory entry
//! ordering, and standalone repository namespaces over [`Store`].
//!
//! # Authority and Crash-Safety Ordering
//! `WriteCommit`, `WriteTree`, and `WriteFile` compute canonical Git SHA-1 digests on the
//! server and verify child blob, subtree, and parent-commit dependencies (`FAILED_PRECONDITION`)
//! before persisting into [`Store`]. `WriteTree` populates `File.size_bytes` from stored blob
//! lengths and sorts entries by UTF-8 `name` so `ReadTree` always satisfies
//! `jj_lib::backend::Tree::from_sorted_entries`.
//!
//! # Concurrency and Merge Invariants
//! `RegisterRepository` is non-destructive on repeat calls and rejects unimplemented
//! shared-project bindings (`project_id != repo_id`) or unimplemented symlink, copy, and commit
//! signature fields until full storage and verification are wired.

use std::sync::Arc;
use tracing::info;

use cc_common::backend::backend_service_server::BackendService;
use cc_common::backend::*;

use crate::error_util::ensure_repo_registered_error;
use crate::hash_utils::{compute_git_blob_hash, compute_git_commit_hash, compute_git_tree_hash};
use crate::store::Store;

const MAX_IDENTIFIER_LEN: usize = 128;

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
        _request: tonic::Request<RegisterProjectRequest>,
    ) -> Result<tonic::Response<RegisterProjectResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented yet"))
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
            if req.project_id != repo_id {
                return Err(tonic::Status::unimplemented(
                    "shared project_id bindings (project_id != repo_id) are not implemented yet",
                ));
            }
            req.project_id
        };
        info!("Registering repository: {}", repo_id);
        self.store.register_repo(repo_id.clone()).await;
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
        let repo_id = req.project_id;
        let commit_id = req.commit_id;
        validate_identifier(&repo_id, "project_id")?;
        validate_digest_len(&commit_id, cc_common::COMMIT_ID_LENGTH, "commit_id")?;

        ensure_repo_registered_error(self.store.as_ref(), &repo_id, "requesting commits").await?;

        if let Some(commit) = self.store.get_commit(&repo_id, &commit_id).await {
            return Ok(tonic::Response::new(ReadCommitResponse {
                commit: Some(commit),
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
        let repo_id = req.project_id;
        validate_identifier(&repo_id, "project_id")?;

        ensure_repo_registered_error(self.store.as_ref(), &repo_id, "requesting commits").await?;

        let mut commit = req.commit.ok_or_else(|| {
            tonic::Status::invalid_argument("request should have contained commit data")
        })?;
        if commit.secure_sig.is_some()
            || !commit.raw_git_commit.is_empty()
            || !commit.external_ids.is_empty()
        {
            return Err(tonic::Status::unimplemented(
                "commit signatures, raw_git_commit, and external_ids are not implemented yet",
            ));
        }
        let computed_id =
            compute_git_commit_hash(&commit).map_err(tonic::Status::invalid_argument)?;
        if !commit.commit_id.is_empty() && commit.commit_id != computed_id {
            return Err(tonic::Status::invalid_argument(
                "supplied commit_id does not match canonical commit digest",
            ));
        }
        for tree_id in &commit.root_tree_id {
            if tree_id.as_slice() != cc_common::EMPTY_TREE_ID_BYTES
                && self.store.get_tree(&repo_id, tree_id).await.is_none()
            {
                return Err(tonic::Status::failed_precondition(format!(
                    "root_tree_id {} must be persisted before WriteCommit",
                    hex::encode(tree_id)
                )));
            }
        }
        for parent_id in &commit.parent_commit_ids {
            if parent_id.as_slice() != cc_common::ROOT_COMMIT_ID_BYTES
                && self.store.get_commit(&repo_id, parent_id).await.is_none()
            {
                return Err(tonic::Status::failed_precondition(format!(
                    "parent commit {} must be persisted before WriteCommit",
                    hex::encode(parent_id)
                )));
            }
        }

        commit.commit_id = computed_id.clone();
        info!("Writing commit {:?} for repo {}", computed_id, repo_id);

        self.store
            .put_commit(repo_id, computed_id.clone(), commit)
            .await;

        Ok(tonic::Response::new(WriteCommitResponse {
            commit_id: computed_id,
        }))
    }

    async fn list_commits(
        &self,
        _request: tonic::Request<ListCommitsRequest>,
    ) -> Result<tonic::Response<ListCommitsResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented yet"))
    }

    async fn resolve_commit_prefix(
        &self,
        _request: tonic::Request<ResolveCommitPrefixRequest>,
    ) -> Result<tonic::Response<ResolveCommitPrefixResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented yet"))
    }

    async fn resolve_change_prefix(
        &self,
        _request: tonic::Request<ResolveChangePrefixRequest>,
    ) -> Result<tonic::Response<ResolveChangePrefixResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented yet"))
    }

    async fn read_tree(
        &self,
        request: tonic::Request<ReadTreeRequest>,
    ) -> Result<tonic::Response<ReadTreeResponse>, tonic::Status> {
        let req = request.into_inner();
        if req.page_size != 0 || !req.page_token.is_empty() {
            return Err(tonic::Status::unimplemented(
                "paginated ReadTree not implemented yet",
            ));
        }
        let repo_id = req.project_id;
        let tree_id = req.tree_id;
        validate_identifier(&repo_id, "project_id")?;
        validate_digest_len(&tree_id, cc_common::TREE_ID_LENGTH, "tree_id")?;

        ensure_repo_registered_error(self.store.as_ref(), &repo_id, "requesting trees").await?;

        if tree_id == cc_common::EMPTY_TREE_ID_BYTES {
            return Ok(tonic::Response::new(ReadTreeResponse {
                tree_id,
                entries: vec![],
                next_page_token: String::new(),
            }));
        }

        if let Some(entries) = self.store.get_tree(&repo_id, &tree_id).await {
            return Ok(tonic::Response::new(ReadTreeResponse {
                tree_id,
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
        let repo_id = req.project_id;
        validate_identifier(&repo_id, "project_id")?;

        ensure_repo_registered_error(self.store.as_ref(), &repo_id, "writing trees").await?;

        for entry in &req.entries {
            match entry.value.as_ref().and_then(|v| v.value.as_ref()) {
                Some(tree_value::Value::SymlinkId(_)) => {
                    return Err(tonic::Status::unimplemented(
                        "symlink tree entries are not implemented yet",
                    ));
                }
                Some(tree_value::Value::File(f)) if !f.copy_id.is_empty() => {
                    return Err(tonic::Status::unimplemented(
                        "file copy tracking is not implemented yet",
                    ));
                }
                _ => {}
            }
        }

        let tree_id =
            compute_git_tree_hash(&req.entries).map_err(tonic::Status::invalid_argument)?;

        let mut normalized_entries = req.entries;
        for entry in &mut normalized_entries {
            match entry.value.as_mut().and_then(|v| v.value.as_mut()) {
                Some(tree_value::Value::File(f)) => {
                    let blob = self.store.get_file(&repo_id, &f.id).await.ok_or_else(|| {
                        tonic::Status::failed_precondition(format!(
                            "file blob {} referenced by tree entry {:?} must be persisted before WriteTree",
                            hex::encode(&f.id),
                            entry.name
                        ))
                    })?;
                    f.size_bytes = Some(blob.len() as u64);
                }
                Some(tree_value::Value::TreeId(sub_id))
                    if sub_id.as_slice() != cc_common::EMPTY_TREE_ID_BYTES
                        && self.store.get_tree(&repo_id, sub_id).await.is_none() =>
                {
                    return Err(tonic::Status::failed_precondition(format!(
                        "subtree {} referenced by tree entry {:?} must be persisted before WriteTree",
                        hex::encode(sub_id),
                        entry.name
                    )));
                }
                _ => {}
            }
        }

        // Persist entries in UTF-8 `RepoPathComponent` byte order (`"foo"` < `"foo.rs"`) as
        // required by `ReadTree` and `jj_lib::backend::Tree::from_sorted_entries`.
        normalized_entries.sort_by(|a, b| a.name.cmp(&b.name));

        self.store
            .put_tree(repo_id, tree_id.clone(), normalized_entries)
            .await;

        Ok(tonic::Response::new(WriteTreeResponse { tree_id }))
    }

    type ReadFileStream =
        tokio_stream::wrappers::ReceiverStream<Result<ReadFileResponse, tonic::Status>>;

    async fn read_file(
        &self,
        request: tonic::Request<ReadFileRequest>,
    ) -> Result<tonic::Response<Self::ReadFileStream>, tonic::Status> {
        let req = request.into_inner();
        if req.offset != 0 || req.max_bytes != 0 {
            return Err(tonic::Status::unimplemented(
                "ranged ReadFile not implemented yet",
            ));
        }
        let repo_id = req.project_id;
        let file_id = req.file_id;
        validate_identifier(&repo_id, "project_id")?;
        validate_digest_len(&file_id, cc_common::FILE_ID_LENGTH, "file_id")?;

        ensure_repo_registered_error(self.store.as_ref(), &repo_id, "reading files").await?;

        let content = self
            .store
            .get_file(&repo_id, &file_id)
            .await
            .ok_or_else(|| {
                tonic::Status::not_found("file should have been present in cloud database")
            })?;

        let (tx, rx) = tokio::sync::mpsc::channel(2);
        tokio::spawn(async move {
            for slice in content.chunks(cc_common::FILE_STREAM_CHUNK_SIZE) {
                if tx
                    .send(Ok(ReadFileResponse {
                        chunk: slice.to_vec(),
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
        let repo_id = req.project_id;
        validate_identifier(&repo_id, "project_id")?;

        ensure_repo_registered_error(self.store.as_ref(), &repo_id, "writing files").await?;

        let file_id =
            compute_git_blob_hash(&req.content).map_err(tonic::Status::invalid_argument)?;

        self.store
            .put_file(repo_id, file_id.clone(), req.content)
            .await;

        Ok(tonic::Response::new(WriteFileResponse { file_id }))
    }

    async fn write_file_stream(
        &self,
        _request: tonic::Request<tonic::Streaming<WriteFileChunk>>,
    ) -> Result<tonic::Response<WriteFileResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented yet"))
    }

    async fn read_symlink(
        &self,
        _request: tonic::Request<ReadSymlinkRequest>,
    ) -> Result<tonic::Response<ReadSymlinkResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented yet"))
    }

    async fn write_symlink(
        &self,
        _request: tonic::Request<WriteSymlinkRequest>,
    ) -> Result<tonic::Response<WriteSymlinkResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented yet"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryStore;
    use tokio_stream::StreamExt as _;
    use tonic::{Code, Request};

    #[tokio::test]
    async fn register_repository_validates_identifiers_algorithms_and_is_non_destructive() {
        let store = Arc::new(MemoryStore::new());
        let svc = CommitCloudBackendService::new(store.clone());

        let resp = svc
            .register_repository(Request::new(RegisterRepositoryRequest {
                project_id: String::new(),
                repo_id: None,
                name: None,
                hash_algorithm: HashAlgorithm::Sha1 as i32,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.repo_id.is_empty());
        assert_eq!(resp.project_id, resp.repo_id);

        assert_eq!(
            svc.register_project(Request::new(RegisterProjectRequest {
                project_id: None,
                name: None,
            }))
            .await
            .unwrap_err()
            .code(),
            Code::Unimplemented
        );
        assert_eq!(
            svc.list_commits(Request::new(ListCommitsRequest {
                project_id: resp.project_id.clone(),
                page_size: 0,
                page_token: Vec::new(),
                since_token: Vec::new(),
            }))
            .await
            .unwrap_err()
            .code(),
            Code::Unimplemented
        );
        assert_eq!(
            svc.resolve_commit_prefix(Request::new(ResolveCommitPrefixRequest {
                project_id: resp.project_id.clone(),
                hex_prefix: "00".to_string(),
            }))
            .await
            .unwrap_err()
            .code(),
            Code::Unimplemented
        );
        assert_eq!(
            svc.resolve_change_prefix(Request::new(ResolveChangePrefixRequest {
                project_id: resp.project_id.clone(),
                prefix: "z".to_string(),
                repo_id: None,
                visible_head_ids: Vec::new(),
                max_results: 0,
            }))
            .await
            .unwrap_err()
            .code(),
            Code::Unimplemented
        );
        assert_eq!(
            svc.register_repository(Request::new(RegisterRepositoryRequest {
                project_id: String::new(),
                repo_id: None,
                name: None,
                hash_algorithm: HashAlgorithm::Sha256 as i32,
            }))
            .await
            .unwrap_err()
            .code(),
            Code::Unimplemented
        );
        assert_eq!(
            svc.register_repository(Request::new(RegisterRepositoryRequest {
                project_id: String::new(),
                repo_id: None,
                name: None,
                hash_algorithm: 99,
            }))
            .await
            .unwrap_err()
            .code(),
            Code::InvalidArgument
        );

        // Empty, padded, and path-traversal identifiers are rejected without silent trimming.
        for bad_id in ["", ".", "..", "   ", " repo-a ", "../escape", "bad\nlog"] {
            let err = svc
                .register_repository(Request::new(RegisterRepositoryRequest {
                    project_id: String::new(),
                    repo_id: Some(bad_id.to_string()),
                    name: None,
                    hash_algorithm: HashAlgorithm::Unspecified as i32,
                }))
                .await
                .unwrap_err();
            assert_eq!(err.code(), Code::InvalidArgument, "{bad_id:?}");
        }

        assert_eq!(
            svc.register_repository(Request::new(RegisterRepositoryRequest {
                project_id: "shared-proj".to_string(),
                repo_id: Some("repo-one".to_string()),
                name: None,
                hash_algorithm: HashAlgorithm::Sha1 as i32,
            }))
            .await
            .unwrap_err()
            .code(),
            Code::Unimplemented
        );

        let commit_resp = svc
            .write_commit(Request::new(WriteCommitRequest {
                project_id: resp.repo_id.clone(),
                commit: Some(Commit {
                    change_id: vec![0x22; 16],
                    parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                    root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
                    description: "keep me".to_string(),
                    ..Default::default()
                }),
            }))
            .await
            .unwrap()
            .into_inner();
        store
            .update_op_heads(
                resp.repo_id.clone(),
                &[cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                vec![0xaa; 16],
            )
            .await;

        svc.register_repository(Request::new(RegisterRepositoryRequest {
            project_id: resp.repo_id.clone(),
            repo_id: Some(resp.repo_id.clone()),
            name: None,
            hash_algorithm: HashAlgorithm::Sha1 as i32,
        }))
        .await
        .unwrap();

        let reread = svc
            .read_commit(Request::new(ReadCommitRequest {
                project_id: resp.repo_id.clone(),
                commit_id: commit_resp.commit_id,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reread.commit.unwrap().description, "keep me");
        assert_eq!(
            store.get_op_heads(&resp.repo_id).await.unwrap(),
            vec![vec![0xaa; 16]]
        );
    }

    #[tokio::test]
    async fn write_tree_enforces_deps_populates_sizes_sorts_utf8_and_streams_files() {
        let store = Arc::new(MemoryStore::new());
        let svc = CommitCloudBackendService::new(store);
        let repo_id = svc
            .register_repository(Request::new(RegisterRepositoryRequest {
                project_id: String::new(),
                repo_id: Some("tree-repo".to_string()),
                name: None,
                hash_algorithm: HashAlgorithm::Sha1 as i32,
            }))
            .await
            .unwrap()
            .into_inner()
            .repo_id;

        // Missing blob dependency returns FAILED_PRECONDITION.
        assert_eq!(
            svc.write_tree(Request::new(WriteTreeRequest {
                project_id: repo_id.clone(),
                path: String::new(),
                entries: vec![TreeEntry {
                    name: "missing.txt".to_string(),
                    value: Some(TreeValue {
                        value: Some(tree_value::Value::File(File {
                            id: vec![0x99; 20],
                            executable: false,
                            copy_id: Vec::new(),
                            size_bytes: None,
                        })),
                    }),
                }],
            }))
            .await
            .unwrap_err()
            .code(),
            Code::FailedPrecondition
        );

        // Symlink entries return UNIMPLEMENTED until symlink storage lands.
        assert_eq!(
            svc.write_tree(Request::new(WriteTreeRequest {
                project_id: repo_id.clone(),
                path: String::new(),
                entries: vec![TreeEntry {
                    name: "link".to_string(),
                    value: Some(TreeValue {
                        value: Some(tree_value::Value::SymlinkId(vec![0x88; 20])),
                    }),
                }],
            }))
            .await
            .unwrap_err()
            .code(),
            Code::Unimplemented
        );

        // Empty file streams 0 frames; >64 KiB file streams multiple 64 KiB frames.
        let empty_file_id = svc
            .write_file(Request::new(WriteFileRequest {
                project_id: repo_id.clone(),
                content: Vec::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .file_id;
        let mut empty_stream = svc
            .read_file(Request::new(ReadFileRequest {
                project_id: repo_id.clone(),
                file_id: empty_file_id.clone(),
                offset: 0,
                max_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(empty_stream.next().await.is_none());

        let big_payload = vec![b'x'; cc_common::FILE_STREAM_CHUNK_SIZE + 17];
        let big_file_id = svc
            .write_file(Request::new(WriteFileRequest {
                project_id: repo_id.clone(),
                content: big_payload.clone(),
            }))
            .await
            .unwrap()
            .into_inner()
            .file_id;
        let mut big_stream = svc
            .read_file(Request::new(ReadFileRequest {
                project_id: repo_id.clone(),
                file_id: big_file_id.clone(),
                offset: 0,
                max_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        let mut frames = Vec::new();
        while let Some(frame) = big_stream.next().await {
            frames.push(frame.unwrap().chunk);
        }
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].len(), cc_common::FILE_STREAM_CHUNK_SIZE);
        assert_eq!(frames[1].len(), 17);

        let sub_tree_id = svc
            .write_tree(Request::new(WriteTreeRequest {
                project_id: repo_id.clone(),
                path: "foo".to_string(),
                entries: vec![TreeEntry {
                    name: "empty".to_string(),
                    value: Some(TreeValue {
                        value: Some(tree_value::Value::File(File {
                            id: empty_file_id.clone(),
                            executable: false,
                            copy_id: Vec::new(),
                            size_bytes: Some(999_999),
                        })),
                    }),
                }],
            }))
            .await
            .unwrap()
            .into_inner()
            .tree_id;

        let entries = vec![
            TreeEntry {
                name: "z.txt".to_string(),
                value: Some(TreeValue {
                    value: Some(tree_value::Value::File(File {
                        id: empty_file_id,
                        executable: false,
                        copy_id: Vec::new(),
                        size_bytes: Some(12345),
                    })),
                }),
            },
            TreeEntry {
                name: "foo.rs".to_string(),
                value: Some(TreeValue {
                    value: Some(tree_value::Value::File(File {
                        id: big_file_id.clone(),
                        executable: false,
                        copy_id: Vec::new(),
                        size_bytes: None,
                    })),
                }),
            },
            TreeEntry {
                name: "foo".to_string(),
                value: Some(TreeValue {
                    value: Some(tree_value::Value::TreeId(sub_tree_id)),
                }),
            },
        ];
        let tree_id = svc
            .write_tree(Request::new(WriteTreeRequest {
                project_id: repo_id.clone(),
                path: String::new(),
                entries,
            }))
            .await
            .unwrap()
            .into_inner()
            .tree_id;

        let read_back = svc
            .read_tree(Request::new(ReadTreeRequest {
                project_id: repo_id.clone(),
                tree_id: tree_id.clone(),
                path: String::new(),
                page_size: 0,
                page_token: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        let names: Vec<_> = read_back.entries.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["foo", "foo.rs", "z.txt"]);
        match read_back.entries[1].value.as_ref().unwrap().value.as_ref() {
            Some(tree_value::Value::File(f)) => {
                assert_eq!(f.size_bytes, Some(big_payload.len() as u64));
            }
            other => panic!("unexpected entry: {other:?}"),
        }
        match read_back.entries[2].value.as_ref().unwrap().value.as_ref() {
            Some(tree_value::Value::File(f)) => assert_eq!(f.size_bytes, Some(0)),
            other => panic!("unexpected entry: {other:?}"),
        }

        // WriteCommit enforces root_tree_id and parent_commit_id FAILED_PRECONDITION checks.
        assert_eq!(
            svc.write_commit(Request::new(WriteCommitRequest {
                project_id: repo_id.clone(),
                commit: Some(Commit {
                    change_id: vec![0x44; 16],
                    parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                    root_tree_id: vec![vec![0x77; 20]],
                    ..Default::default()
                }),
            }))
            .await
            .unwrap_err()
            .code(),
            Code::FailedPrecondition
        );
        assert_eq!(
            svc.write_commit(Request::new(WriteCommitRequest {
                project_id: repo_id.clone(),
                commit: Some(Commit {
                    change_id: vec![0x44; 16],
                    parent_commit_ids: vec![vec![0x66; 20]],
                    root_tree_id: vec![tree_id.clone()],
                    ..Default::default()
                }),
            }))
            .await
            .unwrap_err()
            .code(),
            Code::FailedPrecondition
        );

        assert_eq!(
            svc.read_tree(Request::new(ReadTreeRequest {
                project_id: repo_id.clone(),
                tree_id,
                path: String::new(),
                page_size: 10,
                page_token: String::new(),
            }))
            .await
            .unwrap_err()
            .code(),
            Code::Unimplemented
        );
        assert_eq!(
            svc.read_file(Request::new(ReadFileRequest {
                project_id: repo_id.clone(),
                file_id: big_file_id,
                offset: 4,
                max_bytes: 0,
            }))
            .await
            .unwrap_err()
            .code(),
            Code::Unimplemented
        );
        assert_eq!(
            svc.write_commit(Request::new(WriteCommitRequest {
                project_id: repo_id.clone(),
                commit: Some(Commit {
                    change_id: vec![0x44; 16],
                    parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                    root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
                    secure_sig: Some(SecureSig {
                        data: b"forged".to_vec(),
                        sig: b"sig".to_vec(),
                    }),
                    ..Default::default()
                }),
            }))
            .await
            .unwrap_err()
            .code(),
            Code::Unimplemented
        );
        assert_eq!(
            svc.write_commit(Request::new(WriteCommitRequest {
                project_id: repo_id,
                commit: Some(Commit {
                    commit_id: vec![0xff; 20],
                    change_id: vec![0x44; 16],
                    parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                    root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
                    ..Default::default()
                }),
            }))
            .await
            .unwrap_err()
            .code(),
            Code::InvalidArgument
        );
    }

    #[tokio::test]
    async fn write_commit_is_insert_only_across_unhashed_metadata_collisions() {
        let store = Arc::new(MemoryStore::new());
        store.register_repo("repo-imm".to_string()).await;
        let svc = CommitCloudBackendService::new(store);

        let base_commit = Commit {
            commit_id: Vec::new(),
            change_id: vec![0x22; cc_common::CHANGE_ID_LENGTH],
            parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
            root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
            description: "immutable".to_string(),
            author: Some(Signature {
                name: "Alice".to_string(),
                email: "alice@example.com".to_string(),
                timestamp: Some(Timestamp {
                    millis_since_epoch: 1_700_000_000_100,
                    tz_offset: 0,
                }),
            }),
            committer: Some(Signature {
                name: "Alice".to_string(),
                email: "alice@example.com".to_string(),
                timestamp: Some(Timestamp {
                    millis_since_epoch: 1_700_000_000_100,
                    tz_offset: 0,
                }),
            }),
            predecessors: Vec::new(),
            conflict_labels: Vec::new(),
            secure_sig: None,
            raw_git_commit: Vec::new(),
            external_ids: Default::default(),
        };
        let commit_id = svc
            .write_commit(Request::new(WriteCommitRequest {
                project_id: "repo-imm".to_string(),
                commit: Some(base_commit.clone()),
            }))
            .await
            .unwrap()
            .into_inner()
            .commit_id;

        let mut colliding = base_commit;
        colliding.predecessors = vec![commit_id.clone()];
        let second_id = svc
            .write_commit(Request::new(WriteCommitRequest {
                project_id: "repo-imm".to_string(),
                commit: Some(colliding),
            }))
            .await
            .unwrap()
            .into_inner()
            .commit_id;
        assert_eq!(second_id, commit_id);

        let persisted = svc
            .read_commit(Request::new(ReadCommitRequest {
                project_id: "repo-imm".to_string(),
                commit_id,
            }))
            .await
            .unwrap()
            .into_inner()
            .commit
            .unwrap();
        assert!(persisted.predecessors.is_empty());
    }
}
