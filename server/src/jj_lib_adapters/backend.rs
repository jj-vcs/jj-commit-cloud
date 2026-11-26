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

//! Server-side `jj_lib::backend::Backend` adapter backed by [`CompositeStore`].
//!
//! # Authority and Crash-Safety Ordering
//! Writes file, symlink, tree, and commit payloads directly through [`CompositeStore`]
//! before exposing dependent parents, and validates stored commit digests, tree entry
//! uniqueness, and metadata invariants on read.

use std::fmt::{self, Debug};
use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use cc_lib::cc_backend::{
    apply_commit_signing, commit_from_proto, commit_to_proto, tree_entry_from_proto,
    tree_entry_to_proto,
};
use futures::StreamExt as _;
use futures::stream::{self, BoxStream};
use jj_lib::backend::*;
use jj_lib::object_id::ObjectId;
use jj_lib::repo_path::{RepoPath, RepoPathBuf};

use crate::hash_utils::{
    compute_git_tree_hash, try_compute_git_blob_hash, verify_or_compute_git_commit_hash,
};
use crate::store::{CompositeStore, StoreError};

fn validate_stored_commit(
    id: &CommitId,
    proto_commit: &mut cc_common::backend::Commit,
) -> Result<(), StoreError> {
    if !proto_commit.commit_id.is_empty() && proto_commit.commit_id.as_slice() != id.as_bytes() {
        return Err(StoreError::Corruption(format!(
            "stored commit {} has mismatched commit_id {}",
            id.hex(),
            hex::encode(&proto_commit.commit_id)
        )));
    }
    if proto_commit.change_id.len() != cc_common::CHANGE_ID_LENGTH {
        return Err(StoreError::Corruption(format!(
            "stored commit {} change_id must be {} bytes (got {} bytes)",
            id.hex(),
            cc_common::CHANGE_ID_LENGTH,
            proto_commit.change_id.len()
        )));
    }
    if proto_commit.parent_commit_ids.is_empty() {
        return Err(StoreError::Corruption(format!(
            "stored commit {} parent_commit_ids must not be empty",
            id.hex()
        )));
    }
    for pid in &proto_commit.parent_commit_ids {
        if pid.len() != cc_common::COMMIT_ID_LENGTH {
            return Err(StoreError::Corruption(format!(
                "stored commit {} parent_commit_ids entry must be {} bytes (got {} bytes)",
                id.hex(),
                cc_common::COMMIT_ID_LENGTH,
                pid.len()
            )));
        }
    }
    if proto_commit.parent_commit_ids.len() > 1
        && proto_commit
            .parent_commit_ids
            .iter()
            .any(|p| p.as_slice() == cc_common::ROOT_COMMIT_ID_BYTES)
    {
        return Err(StoreError::Corruption(format!(
            "stored merge commit {} must not reference root commit as a parent",
            id.hex()
        )));
    }
    for pred in &proto_commit.predecessors {
        if pred.len() != cc_common::COMMIT_ID_LENGTH {
            return Err(StoreError::Corruption(format!(
                "stored commit {} predecessors entry must be {} bytes (got {} bytes)",
                id.hex(),
                cc_common::COMMIT_ID_LENGTH,
                pred.len()
            )));
        }
    }
    let num_trees = proto_commit.root_tree_id.len();
    if num_trees == 0 || num_trees.is_multiple_of(2) {
        return Err(StoreError::Corruption(format!(
            "stored commit {} root_tree_id must have odd non-zero length (got {num_trees})",
            id.hex()
        )));
    }
    for tid in &proto_commit.root_tree_id {
        if tid.len() != cc_common::TREE_ID_LENGTH {
            return Err(StoreError::Corruption(format!(
                "stored commit {} root_tree_id entry must be {} bytes (got {} bytes)",
                id.hex(),
                cc_common::TREE_ID_LENGTH,
                tid.len()
            )));
        }
    }
    let labels = &proto_commit.conflict_labels;
    let has_non_empty_label = labels.iter().any(|l| !l.is_empty());
    let unlabeled = labels.is_empty()
        || (labels.len() == 1 && labels[0].is_empty())
        || (labels.len() == num_trees && !has_non_empty_label);
    if !unlabeled && (num_trees == 1 || labels.len() != num_trees || !has_non_empty_label) {
        return Err(StoreError::Corruption(format!(
            "stored commit {} conflict_labels must be empty or match root_tree_id length (got {} labels for {num_trees} trees)",
            id.hex(),
            labels.len()
        )));
    }
    if labels.iter().any(|l| l.contains('\n')) {
        return Err(StoreError::Corruption(format!(
            "stored commit {} conflict_labels must not contain newlines",
            id.hex()
        )));
    }
    if unlabeled {
        proto_commit.conflict_labels.clear();
    }
    Ok(())
}

pub struct ServerBackend {
    store: Arc<CompositeStore>,
    repo_id: String,
    root_commit_id: CommitId,
    root_change_id: ChangeId,
    empty_tree_id: TreeId,
}

impl Debug for ServerBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerBackend")
            .field("repo_id", &self.repo_id)
            .finish()
    }
}

impl ServerBackend {
    pub fn new(store: Arc<CompositeStore>, repo_id: String) -> Self {
        Self {
            store,
            repo_id,
            root_commit_id: CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES),
            root_change_id: ChangeId::from_bytes(&cc_common::ROOT_CHANGE_ID_BYTES),
            empty_tree_id: TreeId::from_hex(cc_common::EMPTY_TREE_ID_HEX),
        }
    }

    async fn resolve_project_id(&self, action: &str) -> BackendResult<String> {
        self.store
            .resolve_repo_project(&self.repo_id, action)
            .await
            .map_err(|e| BackendError::Other(e.into()))
    }
}

#[async_trait]
impl Backend for ServerBackend {
    fn name(&self) -> &str {
        "server_backend"
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
        64
    }

    async fn read_file(
        &self,
        _path: &RepoPath,
        id: &FileId,
    ) -> BackendResult<Pin<Box<dyn futures::AsyncRead + Send>>> {
        let project_id = self.resolve_project_id("reading files").await?;
        let content = self
            .store
            .get_file_range(&project_id, id.as_bytes(), 0, None)
            .await
            .map_err(|e| match e {
                StoreError::ObjectNotFound { .. } => BackendError::ObjectNotFound {
                    object_type: "file".into(),
                    hash: id.hex(),
                    source: Some(e.into()),
                },
                other => BackendError::Other(other.into()),
            })?;
        Ok(Box::pin(futures::io::Cursor::new(content.to_vec())))
    }

    async fn write_file(
        &self,
        _path: &RepoPath,
        contents: &mut (dyn futures::AsyncRead + Send + Unpin),
    ) -> BackendResult<FileId> {
        let project_id = self.resolve_project_id("writing files").await?;
        let mut buffer = Vec::new();
        futures::AsyncReadExt::read_to_end(contents, &mut buffer)
            .await
            .map_err(|e| BackendError::Other(e.into()))?;
        let file_id_bytes =
            try_compute_git_blob_hash(&buffer).map_err(|e| BackendError::Other(e.into()))?;
        self.store
            .put_file(&project_id, &file_id_bytes, Bytes::from(buffer))
            .await
            .map_err(|e| BackendError::Other(e.into()))?;
        Ok(FileId::from_bytes(&file_id_bytes))
    }

    async fn read_symlink(&self, _path: &RepoPath, id: &SymlinkId) -> BackendResult<String> {
        let project_id = self.resolve_project_id("reading symlinks").await?;
        self.store
            .get_symlink(&project_id, id.as_bytes())
            .await
            .map_err(|e| match e {
                StoreError::ObjectNotFound { .. } => BackendError::ObjectNotFound {
                    object_type: "symlink".into(),
                    hash: id.hex(),
                    source: Some(e.into()),
                },
                other => BackendError::Other(other.into()),
            })
    }

    async fn write_symlink(&self, _path: &RepoPath, target: &str) -> BackendResult<SymlinkId> {
        let project_id = self.resolve_project_id("writing symlinks").await?;
        let symlink_id_bytes = try_compute_git_blob_hash(target.as_bytes())
            .map_err(|e| BackendError::Other(e.into()))?;
        self.store
            .put_symlink(&project_id, &symlink_id_bytes, target)
            .await
            .map_err(|e| BackendError::Other(e.into()))?;
        Ok(SymlinkId::from_bytes(&symlink_id_bytes))
    }

    async fn read_copy(&self, _id: &CopyId) -> BackendResult<CopyHistory> {
        Err(BackendError::Unsupported("copies not supported".into()))
    }

    async fn write_copy(&self, _contents: &CopyHistory) -> BackendResult<CopyId> {
        Err(BackendError::Unsupported("copies not supported".into()))
    }

    async fn get_related_copies(&self, _copy_id: &CopyId) -> BackendResult<Vec<RelatedCopy>> {
        Err(BackendError::Unsupported("copies not supported".into()))
    }

    async fn read_tree(&self, _path: &RepoPath, id: &TreeId) -> BackendResult<Tree> {
        if *id == self.empty_tree_id {
            return Ok(Tree::from_sorted_entries(vec![]));
        }

        let project_id = self.resolve_project_id("reading trees").await?;
        let proto_entries = self
            .store
            .get_tree(&project_id, id.as_bytes())
            .await
            .map_err(|e| match e {
                StoreError::ObjectNotFound { .. } => BackendError::ObjectNotFound {
                    object_type: "tree".into(),
                    hash: id.hex(),
                    source: Some(e.into()),
                },
                other => BackendError::Other(other.into()),
            })?;

        let mut jj_entries = Vec::with_capacity(proto_entries.len());
        for entry in proto_entries {
            let (comp, val) = tree_entry_from_proto(entry).map_err(BackendError::Other)?;
            jj_entries.push((comp, val));
        }
        jj_entries.sort_by(|(a, _), (b, _)| a.cmp(b));
        if let Some(dup) = jj_entries.windows(2).find(|w| w[0].0 == w[1].0) {
            return Err(BackendError::Other(
                StoreError::Corruption(format!(
                    "stored tree {} contains duplicate entry {:?}",
                    id.hex(),
                    dup[0].0.as_internal_str()
                ))
                .into(),
            ));
        }
        Ok(Tree::from_sorted_entries(jj_entries))
    }

    async fn write_tree(&self, _path: &RepoPath, tree: &Tree) -> BackendResult<TreeId> {
        if tree.entries().next().is_none() {
            return Ok(self.empty_tree_id.clone());
        }

        let project_id = self.resolve_project_id("writing trees").await?;
        let proto_entries: Result<Vec<_>, _> =
            tree.entries().map(|e| tree_entry_to_proto(&e)).collect();
        let proto_entries = proto_entries?;
        let tree_id_bytes =
            compute_git_tree_hash(&proto_entries).map_err(|e| BackendError::Other(e.into()))?;
        self.store
            .put_tree(&project_id, &tree_id_bytes, &proto_entries)
            .await
            .map_err(|e| BackendError::Other(e.into()))?;
        Ok(TreeId::from_bytes(&tree_id_bytes))
    }

    async fn read_commit(&self, id: &CommitId) -> BackendResult<Commit> {
        if *id == self.root_commit_id {
            return Ok(make_root_commit(
                self.root_change_id.clone(),
                self.empty_tree_id.clone(),
            ));
        }

        let project_id = self.resolve_project_id("reading commits").await?;
        let mut proto_commit = self
            .store
            .get_commit(&project_id, id.as_bytes())
            .await
            .map_err(|e| match e {
                StoreError::ObjectNotFound { .. } => BackendError::ObjectNotFound {
                    object_type: "commit".into(),
                    hash: id.hex(),
                    source: Some(e.into()),
                },
                other => BackendError::Other(other.into()),
            })?;
        validate_stored_commit(id, &mut proto_commit).map_err(|e| BackendError::Other(e.into()))?;
        Ok(commit_from_proto(proto_commit))
    }

    async fn write_commit(
        &self,
        mut commit: Commit,
        sign_with: Option<&mut SigningFn>,
    ) -> BackendResult<(CommitId, Commit)> {
        if commit.parents.len() > 1
            && commit
                .parents
                .iter()
                .any(|p| p.as_bytes() == cc_common::ROOT_COMMIT_ID_BYTES)
        {
            return Err(BackendError::Unsupported(
                "The root commit cannot be a parent of a merge commit".to_string(),
            ));
        }
        let project_id = self.resolve_project_id("writing commits").await?;
        apply_commit_signing(&mut commit, sign_with)?;
        let mut proto_commit = commit_to_proto(&commit);
        let commit_id_bytes = verify_or_compute_git_commit_hash(&mut proto_commit)
            .map_err(|e| BackendError::Other(e.into()))?;
        proto_commit.commit_id = commit_id_bytes.clone();
        self.store
            .put_commit(&project_id, &commit_id_bytes, &proto_commit)
            .await
            .map_err(|e| BackendError::Other(e.into()))?;
        Ok((CommitId::from_bytes(&commit_id_bytes), commit))
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
