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

//! `jj_lib::backend::Backend` client implementation backed by `BackendService` over gRPC.
//!
//! # Authority and Crash-Safety Ordering
//! Validates object digest lengths (`CommitId`, `ChangeId`, `TreeId`, `FileId`, `SymlinkId`),
//! commit parent/conflict invariants, signature timezone bounds, and strict ascending
//! `RepoPathComponent` tree entry ordering on every response before constructing `jj_lib`
//! domain values, refusing unverified wire commit signatures or truncated paginated responses.

#![allow(elided_lifetimes_in_paths)]

use async_trait::async_trait;
use futures::StreamExt as _;
use futures::stream::{self, BoxStream};
use jj_lib::backend::*;
use jj_lib::index::Index;
use jj_lib::object_id::ObjectId;
use jj_lib::repo_path::{RepoPath, RepoPathBuf, RepoPathComponentBuf};
use std::fmt::Debug;
use std::fs;
use std::path::Path;
use std::pin::Pin;
use std::time::SystemTime;

use crate::util::{CommitCloudConfig, run_async};

const MAX_GIT_TZ_OFFSET_MINUTES: i32 = 99 * 60 + 59;

fn validate_object_bytes(bytes: &[u8], expected_len: usize, kind: &str) -> BackendResult<()> {
    if bytes.len() != expected_len {
        return Err(BackendError::Other(
            format!(
                "invalid {kind} byte length: expected {expected_len}, got {}",
                bytes.len()
            )
            .into(),
        ));
    }
    Ok(())
}

#[derive(Debug)]
pub struct CommitCloudBackend {
    server_url: String,
    repo_id: String,
    root_commit_id: CommitId,
    root_change_id: ChangeId,
    empty_tree_id: TreeId,
}

impl CommitCloudBackend {
    pub fn name() -> &'static str {
        "commit_cloud"
    }

    pub fn init(
        store_path: &Path,
        server_url: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let root_commit_id = CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES);
        let root_change_id = ChangeId::from_bytes(&cc_common::ROOT_CHANGE_ID_BYTES);
        let empty_tree_id = TreeId::from_hex(cc_common::EMPTY_TREE_ID_HEX);

        let server_url_cloned = server_url.to_string();
        let repo_id = run_async(move || async move {
            let mut client =
                cc_common::backend::backend_service_client::BackendServiceClient::connect(
                    server_url_cloned,
                )
                .await?;
            let register_repo_response = client
                .register_repository(tonic::Request::new(
                    cc_common::backend::RegisterRepositoryRequest {
                        project_id: String::new(),
                        repo_id: None,
                        name: None,
                        hash_algorithm: cc_common::backend::HashAlgorithm::Sha1 as i32,
                    },
                ))
                .await?
                .into_inner();
            if register_repo_response.repo_id.is_empty() {
                return Err("RegisterRepository returned an empty repo_id".into());
            }
            if !register_repo_response.project_id.is_empty()
                && register_repo_response.project_id != register_repo_response.repo_id
            {
                return Err(format!(
                    "RegisterRepository returned distinct project_id '{}' != repo_id '{}'",
                    register_repo_response.project_id, register_repo_response.repo_id
                )
                .into());
            }
            Ok(register_repo_response.repo_id)
        })?;

        let config_path = store_path.join("config.toml");
        let config = CommitCloudConfig {
            server_url: server_url.to_string(),
            repo_id: repo_id.clone(),
        };
        fs::write(&config_path, toml::to_string_pretty(&config)?)?;

        Ok(Self {
            server_url: server_url.to_string(),
            repo_id,
            root_commit_id,
            root_change_id,
            empty_tree_id,
        })
    }

    pub fn load(store_path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config = CommitCloudConfig::load_from_store(store_path)?;
        let root_commit_id = CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES);
        let root_change_id = ChangeId::from_bytes(&cc_common::ROOT_CHANGE_ID_BYTES);
        let empty_tree_id = TreeId::from_hex(cc_common::EMPTY_TREE_ID_HEX);
        Ok(Self {
            server_url: config.server_url,
            repo_id: config.repo_id,
            root_commit_id,
            root_change_id,
            empty_tree_id,
        })
    }
}

fn signature_to_proto(sig: &Signature) -> cc_common::backend::Signature {
    cc_common::backend::Signature {
        name: sig.name.clone(),
        email: sig.email.clone(),
        timestamp: Some(cc_common::backend::Timestamp {
            millis_since_epoch: sig.timestamp.timestamp.0,
            tz_offset: sig.timestamp.tz_offset,
        }),
    }
}

fn signature_from_proto(proto: cc_common::backend::Signature) -> BackendResult<Signature> {
    let ts = proto.timestamp.unwrap_or_default();
    if !(-MAX_GIT_TZ_OFFSET_MINUTES..=MAX_GIT_TZ_OFFSET_MINUTES).contains(&ts.tz_offset) {
        return Err(BackendError::Other(
            format!(
                "invalid signature tz_offset {} minutes (expected within +-{MAX_GIT_TZ_OFFSET_MINUTES})",
                ts.tz_offset
            )
            .into(),
        ));
    }
    Ok(Signature {
        name: proto.name,
        email: proto.email,
        timestamp: Timestamp {
            timestamp: MillisSinceEpoch(ts.millis_since_epoch),
            tz_offset: ts.tz_offset,
        },
    })
}

fn commit_to_proto(commit: &Commit) -> cc_common::backend::Commit {
    cc_common::backend::Commit {
        commit_id: vec![],
        change_id: commit.change_id.to_bytes().to_vec(),
        parent_commit_ids: commit
            .parents
            .iter()
            .map(|id| id.to_bytes().to_vec())
            .collect(),
        root_tree_id: commit
            .root_tree
            .iter()
            .map(|id| id.to_bytes().to_vec())
            .collect(),
        description: commit.description.clone(),
        author: Some(signature_to_proto(&commit.author)),
        committer: Some(signature_to_proto(&commit.committer)),
        predecessors: commit
            .predecessors
            .iter()
            .map(|id| id.to_bytes().to_vec())
            .collect(),
        conflict_labels: jj_lib::conflict_labels::ConflictLabels::from_merge(
            commit.conflict_labels.clone(),
        )
        .as_slice()
        .to_owned(),
        secure_sig: None,
        raw_git_commit: Vec::new(),
        external_ids: Default::default(),
    }
}

fn commit_from_proto(proto_commit: cc_common::backend::Commit) -> BackendResult<Commit> {
    if proto_commit.secure_sig.is_some() {
        return Err(BackendError::Other(
            "commit signature verification is not implemented yet".into(),
        ));
    }
    validate_object_bytes(
        &proto_commit.change_id,
        cc_common::CHANGE_ID_LENGTH,
        "change_id",
    )?;
    if proto_commit.root_tree_id.is_empty() || proto_commit.root_tree_id.len().is_multiple_of(2) {
        return Err(BackendError::Other(
            format!(
                "invalid root_tree_id term count: expected odd >= 1, got {}",
                proto_commit.root_tree_id.len()
            )
            .into(),
        ));
    }
    for b in &proto_commit.root_tree_id {
        validate_object_bytes(b, cc_common::TREE_ID_LENGTH, "root_tree_id")?;
    }
    if proto_commit.parent_commit_ids.is_empty() {
        return Err(BackendError::Other(
            "non-root commit parent_commit_ids must not be empty".into(),
        ));
    }
    for b in &proto_commit.parent_commit_ids {
        validate_object_bytes(b, cc_common::COMMIT_ID_LENGTH, "parent_commit_id")?;
    }
    let has_root_parent = proto_commit
        .parent_commit_ids
        .iter()
        .any(|b| b.as_slice() == cc_common::ROOT_COMMIT_ID_BYTES);
    if has_root_parent && proto_commit.parent_commit_ids.len() != 1 {
        return Err(BackendError::Other(
            "ROOT_COMMIT_ID may only appear as the sole entry in parent_commit_ids".into(),
        ));
    }
    for b in &proto_commit.predecessors {
        validate_object_bytes(b, cc_common::COMMIT_ID_LENGTH, "predecessor commit_id")?;
    }
    if proto_commit
        .conflict_labels
        .iter()
        .any(|l| l.contains('\n'))
    {
        return Err(BackendError::Other(
            "conflict_labels entries must not contain newline characters".into(),
        ));
    }
    let num_trees = proto_commit.root_tree_id.len();
    let unlabeled = proto_commit.conflict_labels.is_empty()
        || (proto_commit.conflict_labels.len() == 1 && proto_commit.conflict_labels[0].is_empty());
    let has_non_empty_label = proto_commit.conflict_labels.iter().any(|l| !l.is_empty());
    if !unlabeled
        && (num_trees == 1
            || proto_commit.conflict_labels.len() != num_trees
            || !has_non_empty_label)
    {
        return Err(BackendError::Other(
            format!(
                "invalid conflict_labels count {} for {num_trees} root_tree_id terms",
                proto_commit.conflict_labels.len()
            )
            .into(),
        ));
    }

    let author = signature_from_proto(proto_commit.author.unwrap_or_default())?;
    let committer = signature_from_proto(proto_commit.committer.unwrap_or_default())?;

    let merge_builder: jj_lib::merge::MergeBuilder<_> = proto_commit
        .root_tree_id
        .into_iter()
        .map(|b| TreeId::from_bytes(&b))
        .collect();

    let root_tree = merge_builder.build();
    let conflict_labels =
        jj_lib::conflict_labels::ConflictLabels::from_vec(proto_commit.conflict_labels)
            .into_merge();

    Ok(Commit {
        parents: proto_commit
            .parent_commit_ids
            .iter()
            .map(|b| CommitId::from_bytes(b))
            .collect(),
        predecessors: proto_commit
            .predecessors
            .iter()
            .map(|b| CommitId::from_bytes(b))
            .collect(),
        root_tree,
        change_id: ChangeId::from_bytes(&proto_commit.change_id),
        description: proto_commit.description,
        author,
        committer,
        conflict_labels,
        secure_sig: None,
    })
}

fn tree_entry_to_proto(
    entry: &jj_lib::backend::TreeEntry,
) -> BackendResult<cc_common::backend::TreeEntry> {
    let value = match entry.value() {
        TreeValue::File {
            id,
            executable,
            copy_id,
        } => {
            if !copy_id.as_bytes().is_empty() {
                return Err(BackendError::Unsupported(
                    "file copy tracking is not supported".to_string(),
                ));
            }
            cc_common::backend::TreeValue {
                value: Some(cc_common::backend::tree_value::Value::File(
                    cc_common::backend::File {
                        id: id.to_bytes().to_vec(),
                        executable: *executable,
                        copy_id: Vec::new(),
                        size_bytes: None,
                    },
                )),
            }
        }
        TreeValue::Symlink(id) => cc_common::backend::TreeValue {
            value: Some(cc_common::backend::tree_value::Value::SymlinkId(
                id.to_bytes().to_vec(),
            )),
        },
        TreeValue::Tree(id) => cc_common::backend::TreeValue {
            value: Some(cc_common::backend::tree_value::Value::TreeId(
                id.to_bytes().to_vec(),
            )),
        },
        TreeValue::GitSubmodule(id) => cc_common::backend::TreeValue {
            value: Some(cc_common::backend::tree_value::Value::GitSubmoduleId(
                id.to_bytes().to_vec(),
            )),
        },
    };

    Ok(cc_common::backend::TreeEntry {
        name: entry.name().as_internal_str().to_string(),
        value: Some(value),
    })
}

fn tree_entry_from_proto(
    proto_entry: cc_common::backend::TreeEntry,
) -> Result<(RepoPathComponentBuf, TreeValue), Box<dyn std::error::Error + Send + Sync>> {
    if proto_entry.name == "." || proto_entry.name == ".." || proto_entry.name.contains('\0') {
        return Err(format!("invalid tree entry name {:?}", proto_entry.name).into());
    }
    let component = RepoPathComponentBuf::new(proto_entry.name)?;
    let proto_val = proto_entry
        .value
        .ok_or("tree entry should have contained a TreeValue")?;
    let val = match proto_val
        .value
        .ok_or("TreeValue should have contained an inner value")?
    {
        cc_common::backend::tree_value::Value::File(f) => {
            if f.id.len() != cc_common::FILE_ID_LENGTH {
                return Err(format!(
                    "file id must be {} bytes (got {} bytes)",
                    cc_common::FILE_ID_LENGTH,
                    f.id.len()
                )
                .into());
            }
            if !f.copy_id.is_empty() {
                return Err("non-empty file copy_id is not supported".into());
            }
            TreeValue::File {
                id: FileId::from_bytes(&f.id),
                executable: f.executable,
                copy_id: CopyId::placeholder(),
            }
        }
        cc_common::backend::tree_value::Value::SymlinkId(id) => {
            if id.len() != cc_common::SYMLINK_ID_LENGTH {
                return Err(format!(
                    "symlink_id must be {} bytes (got {} bytes)",
                    cc_common::SYMLINK_ID_LENGTH,
                    id.len()
                )
                .into());
            }
            TreeValue::Symlink(SymlinkId::from_bytes(&id))
        }
        cc_common::backend::tree_value::Value::TreeId(id) => {
            if id.len() != cc_common::TREE_ID_LENGTH {
                return Err(format!(
                    "tree_id must be {} bytes (got {} bytes)",
                    cc_common::TREE_ID_LENGTH,
                    id.len()
                )
                .into());
            }
            TreeValue::Tree(TreeId::from_bytes(&id))
        }
        cc_common::backend::tree_value::Value::GitSubmoduleId(id) => {
            if id.len() != cc_common::COMMIT_ID_LENGTH {
                return Err(format!(
                    "git_submodule_id must be {} bytes (got {} bytes)",
                    cc_common::COMMIT_ID_LENGTH,
                    id.len()
                )
                .into());
            }
            TreeValue::GitSubmodule(CommitId::from_bytes(&id))
        }
        cc_common::backend::tree_value::Value::ConflictId(_) => {
            return Err("tree entry should not have contained a ConflictId".into());
        }
    };
    Ok((component, val))
}

fn build_validated_tree(proto_entries: Vec<cc_common::backend::TreeEntry>) -> BackendResult<Tree> {
    let mut jj_entries = Vec::with_capacity(proto_entries.len());
    for entry in proto_entries {
        let (comp, val) = tree_entry_from_proto(entry).map_err(BackendError::Other)?;
        jj_entries.push((comp, val));
    }
    if !jj_entries.windows(2).all(|w| w[0].0 < w[1].0) {
        return Err(BackendError::Other(
            "ReadTree response entries are not in strict ascending UTF-8 RepoPathComponent order"
                .into(),
        ));
    }
    Ok(Tree::from_sorted_entries(jj_entries))
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
        1
    }

    async fn read_file(
        &self,
        _path: &RepoPath,
        id: &FileId,
    ) -> BackendResult<Pin<Box<dyn futures::AsyncRead + Send>>> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let file_id_bytes = id.to_bytes().to_vec();
        let file_id_hex = id.hex();

        let content = run_async(move || async move {
            let mut client =
                cc_common::backend::backend_service_client::BackendServiceClient::connect(
                    server_url,
                )
                .await?;
            let res = client
                .read_file(tonic::Request::new(cc_common::backend::ReadFileRequest {
                    project_id: repo_id,
                    file_id: file_id_bytes,
                    offset: 0,
                    max_bytes: 0,
                }))
                .await;

            let mut stream = match res {
                Ok(r) => r.into_inner(),
                Err(status) if status.code() == tonic::Code::NotFound => {
                    return Err(Box::new(BackendError::ObjectNotFound {
                        object_type: "file".into(),
                        hash: file_id_hex,
                        source: status.into(),
                    })
                        as Box<dyn std::error::Error + Send + Sync>);
                }
                Err(status) => {
                    return Err(Box::new(status) as Box<dyn std::error::Error + Send + Sync>);
                }
            };

            let mut content = Vec::new();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|e| BackendError::Other(e.into()))?;
                content.extend_from_slice(&chunk.chunk);
            }
            Ok(content)
        })
        .map_err(|e| match e.downcast::<BackendError>() {
            Ok(err) => *err,
            Err(e) => BackendError::Other(e),
        })?;

        let cursor = futures::io::Cursor::new(content);
        Ok(Box::pin(cursor) as Pin<Box<dyn futures::AsyncRead + Send>>)
    }

    // TODO: Stream 64 KiB chunks via `WriteFileStream` once server streaming is wired.
    async fn write_file(
        &self,
        _path: &RepoPath,
        contents: &mut (dyn futures::AsyncRead + Send + Unpin),
    ) -> BackendResult<FileId> {
        let mut buffer = Vec::new();
        futures::AsyncReadExt::read_to_end(contents, &mut buffer)
            .await
            .map_err(|e| BackendError::Other(e.into()))?;

        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();

        let file_id_bytes = run_async(move || async move {
            let mut client =
                cc_common::backend::backend_service_client::BackendServiceClient::connect(
                    server_url,
                )
                .await?;
            let res = client
                .write_file(tonic::Request::new(cc_common::backend::WriteFileRequest {
                    project_id: repo_id,
                    content: buffer,
                }))
                .await?;
            Ok(res.into_inner().file_id)
        })
        .map_err(BackendError::Other)?;

        validate_object_bytes(&file_id_bytes, cc_common::FILE_ID_LENGTH, "file_id")?;
        Ok(FileId::from_bytes(&file_id_bytes))
    }

    async fn read_symlink(&self, _path: &RepoPath, _id: &SymlinkId) -> BackendResult<String> {
        Err(BackendError::Unsupported(
            "read_symlink not supported".to_string(),
        ))
    }

    async fn write_symlink(&self, _path: &RepoPath, _target: &str) -> BackendResult<SymlinkId> {
        Err(BackendError::Unsupported(
            "write_symlink not supported".to_string(),
        ))
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
        if *id == self.empty_tree_id {
            return Ok(Tree::from_sorted_entries(vec![]));
        }

        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let tree_id_bytes = id.to_bytes().to_vec();
        let tree_id_hex = id.hex();
        let path_str = path.as_internal_file_string().to_string();

        let proto_entries = run_async(move || async move {
            let mut client =
                cc_common::backend::backend_service_client::BackendServiceClient::connect(
                    server_url,
                )
                .await?;
            let res = client
                .read_tree(tonic::Request::new(cc_common::backend::ReadTreeRequest {
                    project_id: repo_id,
                    tree_id: tree_id_bytes.clone(),
                    path: path_str,
                    page_size: 0,
                    page_token: String::new(),
                }))
                .await;

            match res {
                Ok(r) => {
                    let inner = r.into_inner();
                    if !inner.tree_id.is_empty() && inner.tree_id != tree_id_bytes {
                        return Err(Box::new(BackendError::Other(
                            "ReadTreeResponse tree_id did not match requested tree_id".into(),
                        ))
                            as Box<dyn std::error::Error + Send + Sync>);
                    }
                    if !inner.next_page_token.is_empty() {
                        return Err(Box::new(BackendError::Other(
                            "unexpected paginated ReadTreeResponse for unpaginated request".into(),
                        ))
                            as Box<dyn std::error::Error + Send + Sync>);
                    }
                    Ok(inner.entries)
                }
                Err(status) if status.code() == tonic::Code::NotFound => {
                    Err(Box::new(BackendError::ObjectNotFound {
                        object_type: "tree".into(),
                        hash: tree_id_hex,
                        source: status.into(),
                    })
                        as Box<dyn std::error::Error + Send + Sync>)
                }
                Err(status) => Err(Box::new(status) as Box<dyn std::error::Error + Send + Sync>),
            }
        })
        .map_err(|e| match e.downcast::<BackendError>() {
            Ok(err) => *err,
            Err(e) => BackendError::Other(e),
        })?;

        build_validated_tree(proto_entries)
    }

    async fn write_tree(&self, path: &RepoPath, tree: &Tree) -> BackendResult<TreeId> {
        if tree.entries().next().is_none() {
            return Ok(self.empty_tree_id.clone());
        }

        let proto_entries: Result<Vec<_>, _> =
            tree.entries().map(|e| tree_entry_to_proto(&e)).collect();
        let proto_entries = proto_entries?;

        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let path_str = path.as_internal_file_string().to_string();

        let tree_id_bytes = run_async(move || async move {
            let mut client =
                cc_common::backend::backend_service_client::BackendServiceClient::connect(
                    server_url,
                )
                .await?;
            let res = client
                .write_tree(tonic::Request::new(cc_common::backend::WriteTreeRequest {
                    project_id: repo_id,
                    path: path_str,
                    entries: proto_entries,
                }))
                .await?;
            Ok(res.into_inner().tree_id)
        })
        .map_err(BackendError::Other)?;

        validate_object_bytes(&tree_id_bytes, cc_common::TREE_ID_LENGTH, "tree_id")?;
        Ok(TreeId::from_bytes(&tree_id_bytes))
    }

    async fn read_commit(&self, id: &CommitId) -> BackendResult<Commit> {
        if *id == self.root_commit_id {
            return Ok(make_root_commit(
                self.root_change_id().clone(),
                self.empty_tree_id.clone(),
            ));
        }

        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let commit_id_bytes = id.to_bytes().to_vec();
        let commit_id_hex = id.hex();

        let proto_commit = run_async(move || async move {
            let mut client =
                cc_common::backend::backend_service_client::BackendServiceClient::connect(
                    server_url,
                )
                .await?;
            let res = client
                .read_commit(tonic::Request::new(cc_common::backend::ReadCommitRequest {
                    project_id: repo_id,
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
                        source: status.into(),
                    })
                        as Box<dyn std::error::Error + Send + Sync>)
                }
                Err(status) => Err(Box::new(status) as Box<dyn std::error::Error + Send + Sync>),
            }
        })
        .map_err(|e| match e.downcast::<BackendError>() {
            Ok(err) => *err,
            Err(e) => BackendError::Other(e),
        })?;

        if !proto_commit.commit_id.is_empty() && proto_commit.commit_id.as_slice() != id.as_bytes()
        {
            return Err(BackendError::Other(
                format!(
                    "ReadCommit response commit_id {} did not match requested {}",
                    jj_lib::hex_util::encode_hex(&proto_commit.commit_id),
                    id.hex()
                )
                .into(),
            ));
        }

        commit_from_proto(proto_commit)
    }

    async fn write_commit(
        &self,
        commit: Commit,
        sign_with: Option<&mut SigningFn>,
    ) -> BackendResult<(CommitId, Commit)> {
        if commit.secure_sig.is_some() || sign_with.is_some() {
            return Err(BackendError::Unsupported(
                "commit signing is not implemented yet".to_string(),
            ));
        }
        let proto_commit = commit_to_proto(&commit);
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();

        let returned_id_bytes = run_async(move || async move {
            let mut client =
                cc_common::backend::backend_service_client::BackendServiceClient::connect(
                    server_url,
                )
                .await?;
            let res = client
                .write_commit(tonic::Request::new(
                    cc_common::backend::WriteCommitRequest {
                        project_id: repo_id,
                        commit: Some(proto_commit),
                    },
                ))
                .await?;
            Ok(res.into_inner().commit_id)
        })
        .map_err(BackendError::Other)?;

        validate_object_bytes(&returned_id_bytes, cc_common::COMMIT_ID_LENGTH, "commit_id")?;
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

    fn gc(&self, _index: &dyn Index, _keep_newer: SystemTime) -> BackendResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_and_submodule_tree_entries_preserve_placeholder_copy_id_and_validate_names() {
        let file_id = FileId::from_bytes(&[0xab; 20]);
        let sub_id = CommitId::from_bytes(&[0xcd; 20]);
        let input_tree = Tree::from_sorted_entries(vec![
            (
                RepoPathComponentBuf::new("readme.md").unwrap(),
                TreeValue::File {
                    id: file_id.clone(),
                    executable: true,
                    copy_id: CopyId::placeholder(),
                },
            ),
            (
                RepoPathComponentBuf::new("submod").unwrap(),
                TreeValue::GitSubmodule(sub_id.clone()),
            ),
        ]);
        let proto_entries: Vec<_> = input_tree
            .entries()
            .map(|e| tree_entry_to_proto(&e).unwrap())
            .collect();
        let reversed = vec![proto_entries[1].clone(), proto_entries[0].clone()];
        let duplicated = vec![proto_entries[0].clone(), proto_entries[0].clone()];
        let round_tripped = build_validated_tree(proto_entries).unwrap();
        assert_eq!(round_tripped, input_tree);
        assert!(build_validated_tree(reversed).is_err());
        assert!(build_validated_tree(duplicated).is_err());

        for bad_name in [".", "..", "bad\0name"] {
            assert!(
                tree_entry_from_proto(cc_common::backend::TreeEntry {
                    name: bad_name.to_string(),
                    value: Some(cc_common::backend::TreeValue {
                        value: Some(cc_common::backend::tree_value::Value::GitSubmoduleId(vec![
                            0xcd;
                            20
                        ])),
                    }),
                })
                .is_err(),
                "{bad_name:?}"
            );
        }

        // Short digest bytes and non-empty copy_id are rejected.
        assert!(
            tree_entry_from_proto(cc_common::backend::TreeEntry {
                name: "submod".to_string(),
                value: Some(cc_common::backend::TreeValue {
                    value: Some(cc_common::backend::tree_value::Value::GitSubmoduleId(vec![
                        0xcd;
                        19
                    ])),
                }),
            })
            .is_err()
        );
        assert!(
            tree_entry_from_proto(cc_common::backend::TreeEntry {
                name: "file.rs".to_string(),
                value: Some(cc_common::backend::TreeValue {
                    value: Some(cc_common::backend::tree_value::Value::File(
                        cc_common::backend::File {
                            id: vec![0xab; 20],
                            executable: false,
                            copy_id: vec![0x00; 20],
                            size_bytes: None,
                        },
                    )),
                }),
            })
            .is_err()
        );
    }

    #[test]
    fn commit_from_proto_validates_signatures_parents_labels_and_tz_offsets() {
        let valid_proto = cc_common::backend::Commit {
            change_id: vec![0x33; 16],
            parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
            root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
            description: "hello".to_string(),
            ..Default::default()
        };
        assert_eq!(
            commit_from_proto(valid_proto.clone()).unwrap().description,
            "hello"
        );

        let mut signed_proto = valid_proto.clone();
        signed_proto.secure_sig = Some(cc_common::backend::SecureSig {
            data: b"unverified".to_vec(),
            sig: b"sig".to_vec(),
        });
        assert!(commit_from_proto(signed_proto).is_err());

        let mut empty_parents = valid_proto.clone();
        empty_parents.parent_commit_ids.clear();
        assert!(commit_from_proto(empty_parents).is_err());

        let mut mixed_root = valid_proto.clone();
        mixed_root.parent_commit_ids =
            vec![vec![0x11; 20], cc_common::ROOT_COMMIT_ID_BYTES.to_vec()];
        assert!(commit_from_proto(mixed_root).is_err());

        let mut bad_tz = valid_proto.clone();
        bad_tz.author = Some(cc_common::backend::Signature {
            name: "Dev".to_string(),
            email: "dev@example.com".to_string(),
            timestamp: Some(cc_common::backend::Timestamp {
                millis_since_epoch: 0,
                tz_offset: 10_000,
            }),
        });
        assert!(commit_from_proto(bad_tz).is_err());

        let mut newline_label = valid_proto;
        newline_label.root_tree_id = vec![vec![0x11; 20], vec![0x22; 20], vec![0x33; 20]];
        newline_label.conflict_labels = vec!["a\nb".into(), "c".into(), "d".into()];
        assert!(commit_from_proto(newline_label).is_err());
    }
}
