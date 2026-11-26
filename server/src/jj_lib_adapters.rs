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

//! Server-side `jj_lib` storage adapters (`ServerBackend`, `ServerOpStore`,
//! `ServerOpHeadsStore`, and `ServerIndexStore`) over [`Store`].
//!
//! # Concurrency and Merge Invariants
//! [`ServerOpHeadsStore::with_expected_heads`] pins both `get_op_heads` and
//! `update_op_heads` CAS to a validated head snapshot so concurrent arrivals trigger a CAS
//! retry through safety checks and ancestor-only head pruning succeeds atomically.
//! `ServerBackend::read_tree` sorts and deduplicates entries before `Tree::from_sorted_entries`.

#![allow(elided_lifetimes_in_paths)]

use std::fmt::{self, Debug};
use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use futures::StreamExt as _;
use futures::stream::{self, BoxStream, LocalBoxStream};
use jj_lib::backend::*;
use jj_lib::commit::Commit as JjCommit;
use jj_lib::graph::GraphNode;
use jj_lib::index::{
    ChangeIdIndex, Index, IndexError, IndexResult, IndexStore, IndexStoreResult, MutableIndex,
    ReadonlyIndex, ResolvedChangeTargets,
};
use jj_lib::object_id::{HexPrefix, ObjectId, PrefixResolution};
use jj_lib::op_heads_store::{OpHeadsStore, OpHeadsStoreError, OpHeadsStoreLock};
use jj_lib::op_store::{
    OpStore, OpStoreError, OpStoreResult, Operation, OperationId, View, ViewId,
};
use jj_lib::operation::Operation as JjOperation;
use jj_lib::repo_path::{RepoPath, RepoPathBuf};
use jj_lib::revset::{ResolvedExpression, Revset, RevsetContainingFn, RevsetEvaluationError};
use jj_lib::store::Store as JjStore;
use jj_lib::submodule_store::SubmoduleStore;

use crate::hash_utils::{
    compute_git_commit_hash, compute_git_tree_hash, hash_operation, hash_view,
    try_compute_git_blob_hash,
};
use crate::store::Store;
use cc_common::conversions::backend::{
    commit_from_proto, commit_to_proto, tree_entry_from_proto, tree_entry_to_proto,
};
use cc_common::conversions::op_store::{
    operation_from_proto, operation_to_proto, view_from_proto, view_to_proto,
};

pub struct ServerBackend {
    store: Arc<dyn Store>,
    project_id: String,
    root_commit_id: CommitId,
    root_change_id: ChangeId,
    empty_tree_id: TreeId,
}

impl Debug for ServerBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerBackend")
            .field("project_id", &self.project_id)
            .finish()
    }
}

impl ServerBackend {
    pub fn new(store: Arc<dyn Store>, project_id: String) -> Self {
        let root_commit_id = CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES);
        let root_change_id = ChangeId::from_bytes(&cc_common::ROOT_CHANGE_ID_BYTES);
        let empty_tree_id = TreeId::from_hex(cc_common::EMPTY_TREE_ID_HEX);
        Self {
            store,
            project_id,
            root_commit_id,
            root_change_id,
            empty_tree_id,
        }
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

    // Use 100 as an arbitrary high value for concurrency. Google uses 100, this works since functions are async and can fetch objects in parallel.
    fn concurrency(&self) -> usize {
        100
    }

    async fn read_file(
        &self,
        _path: &RepoPath,
        id: &FileId,
    ) -> BackendResult<Pin<Box<dyn futures::AsyncRead + Send>>> {
        let file_id_bytes = id.to_bytes().to_vec();
        let content = self
            .store
            .get_file(&self.project_id, &file_id_bytes)
            .await
            .map_err(|e| BackendError::Other(e.into()))?
            .ok_or_else(|| BackendError::ObjectNotFound {
                object_type: "file".into(),
                hash: id.hex(),
                source: "file not found in server store".into(),
            })?;
        Ok(Box::pin(futures::io::Cursor::new(content)))
    }

    async fn write_file(
        &self,
        _path: &RepoPath,
        contents: &mut (dyn futures::AsyncRead + Send + Unpin),
    ) -> BackendResult<FileId> {
        let mut buffer = Vec::new();
        futures::AsyncReadExt::read_to_end(contents, &mut buffer)
            .await
            .map_err(|e| BackendError::Other(e.into()))?;
        let file_id_bytes =
            try_compute_git_blob_hash(&buffer).map_err(|e| BackendError::Other(e.into()))?;
        self.store
            .put_file(self.project_id.clone(), file_id_bytes.clone(), buffer)
            .await
            .map_err(|e| BackendError::Other(e.into()))?;
        Ok(FileId::from_bytes(&file_id_bytes))
    }

    async fn read_symlink(&self, _path: &RepoPath, id: &SymlinkId) -> BackendResult<String> {
        let raw = self
            .store
            .get_file(&self.project_id, id.as_bytes())
            .await
            .map_err(|e| BackendError::Other(e.into()))?
            .ok_or_else(|| BackendError::ObjectNotFound {
                object_type: "symlink".into(),
                hash: id.hex(),
                source: "symlink not found in server store".into(),
            })?;
        let target = String::from_utf8(raw).map_err(|e| {
            BackendError::Other(format!("invalid UTF-8 symlink target: {e}").into())
        })?;
        if target.contains('\0') {
            return Err(BackendError::Other(
                "stored symlink target contains NUL byte".into(),
            ));
        }
        Ok(target)
    }

    async fn write_symlink(&self, _path: &RepoPath, target: &str) -> BackendResult<SymlinkId> {
        if target.contains('\0') {
            return Err(BackendError::Other(
                "symlink target must not contain NUL bytes".into(),
            ));
        }
        let raw = target.as_bytes().to_vec();
        let symlink_id_bytes = crate::hash_utils::try_compute_git_blob_hash(&raw)
            .map_err(|e| BackendError::Other(e.into()))?;
        self.store
            .put_file(self.project_id.clone(), symlink_id_bytes.clone(), raw)
            .await
            .map_err(|e| BackendError::Other(e.into()))?;
        Ok(SymlinkId::from_bytes(&symlink_id_bytes))
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

    async fn read_tree(&self, _path: &RepoPath, id: &TreeId) -> BackendResult<Tree> {
        if *id == self.empty_tree_id {
            return Ok(Tree::from_sorted_entries(vec![]));
        }

        let proto_entries = self
            .store
            .get_tree(&self.project_id, id.as_bytes())
            .await
            .map_err(|e| BackendError::Other(e.into()))?
            .ok_or_else(|| BackendError::ObjectNotFound {
                object_type: "tree".into(),
                hash: id.hex(),
                source: "tree not found in server store".into(),
            })?;

        let mut jj_entries = Vec::with_capacity(proto_entries.len());
        for entry in proto_entries {
            let (comp, val) =
                tree_entry_from_proto(entry).map_err(|e| BackendError::Other(e.into()))?;
            jj_entries.push((comp, val));
        }
        jj_entries.sort_by(|(a, _), (b, _)| a.cmp(b));
        for pair in jj_entries.windows(2) {
            if pair[0].0 == pair[1].0 {
                return Err(BackendError::Other(
                    format!(
                        "stored tree {} contains duplicate entry {:?}",
                        id.hex(),
                        pair[0].0.as_internal_str()
                    )
                    .into(),
                ));
            }
        }

        Ok(Tree::from_sorted_entries(jj_entries))
    }

    async fn write_tree(&self, _path: &RepoPath, tree: &Tree) -> BackendResult<TreeId> {
        if tree.entries().next().is_none() {
            return Ok(self.empty_tree_id.clone());
        }

        let proto_entries: Result<Vec<_>, _> =
            tree.entries().map(|e| tree_entry_to_proto(&e)).collect();
        let mut proto_entries = proto_entries?;
        for entry in &mut proto_entries {
            if let Some(cc_common::backend::TreeValue {
                value: Some(cc_common::backend::tree_value::Value::File(file)),
            }) = entry.value.as_mut()
                && let Some(content) = self
                    .store
                    .get_file(&self.project_id, &file.id)
                    .await
                    .map_err(|e| BackendError::Other(e.into()))?
            {
                file.size_bytes = Some(content.len() as u64);
            }
        }
        proto_entries.sort_by(|a, b| a.name.cmp(&b.name));
        let tree_id_bytes =
            compute_git_tree_hash(&proto_entries).map_err(|e| BackendError::Other(e.into()))?;

        self.store
            .put_tree(
                self.project_id.clone(),
                tree_id_bytes.clone(),
                proto_entries,
            )
            .await
            .map_err(|e| BackendError::Other(e.into()))?;

        Ok(TreeId::from_bytes(&tree_id_bytes))
    }

    async fn read_commit(&self, id: &CommitId) -> BackendResult<Commit> {
        if *id == self.root_commit_id {
            return Ok(make_root_commit(
                self.root_change_id().clone(),
                self.empty_tree_id.clone(),
            ));
        }

        let mut proto_commit = self
            .store
            .get_commit(&self.project_id, id.as_bytes())
            .await
            .map_err(|e| BackendError::Other(e.into()))?
            .ok_or_else(|| BackendError::ObjectNotFound {
                object_type: "commit".into(),
                hash: id.hex(),
                source: "commit not found in server store".into(),
            })?;

        if proto_commit.change_id.len() != cc_common::CHANGE_ID_LENGTH {
            return Err(BackendError::Other(
                format!(
                    "stored commit {} change_id must be {} bytes (got {} bytes)",
                    id.hex(),
                    cc_common::CHANGE_ID_LENGTH,
                    proto_commit.change_id.len()
                )
                .into(),
            ));
        }
        let num_trees = proto_commit.root_tree_id.len();
        if num_trees == 0 || num_trees.is_multiple_of(2) {
            return Err(BackendError::Other(
                format!(
                    "stored commit {} root_tree_id must have odd non-zero length (got {num_trees})",
                    id.hex()
                )
                .into(),
            ));
        }
        for tid in &proto_commit.root_tree_id {
            if tid.len() != cc_common::TREE_ID_LENGTH {
                return Err(BackendError::Other(
                    format!(
                        "stored commit {} root_tree_id entry must be {} bytes (got {} bytes)",
                        id.hex(),
                        cc_common::TREE_ID_LENGTH,
                        tid.len()
                    )
                    .into(),
                ));
            }
        }
        let labels = &proto_commit.conflict_labels;
        let has_non_empty_label = labels.iter().any(|l| !l.is_empty());
        let unlabeled = labels.is_empty()
            || (labels.len() == 1 && labels[0].is_empty())
            || (labels.len() == num_trees && !has_non_empty_label);
        if !unlabeled && (num_trees == 1 || labels.len() != num_trees || !has_non_empty_label) {
            return Err(BackendError::Other(
                format!(
                    "stored commit {} conflict_labels must be empty or match root_tree_id length (got {} labels for {num_trees} trees)",
                    id.hex(),
                    labels.len()
                )
                .into(),
            ));
        }
        if labels.iter().any(|l| l.contains('\n')) {
            return Err(BackendError::Other(
                format!(
                    "stored commit {} conflict_labels must not contain newlines",
                    id.hex()
                )
                .into(),
            ));
        }
        if unlabeled {
            proto_commit.conflict_labels.clear();
        }
        if proto_commit.parent_commit_ids.is_empty() {
            return Err(BackendError::Other(
                format!("stored commit {} must have at least one parent", id.hex()).into(),
            ));
        }
        let mut has_root_parent = false;
        for pid in &proto_commit.parent_commit_ids {
            if pid.len() != cc_common::COMMIT_ID_LENGTH {
                return Err(BackendError::Other(
                    format!(
                        "stored commit {} parent_commit_id must be {} bytes (got {} bytes)",
                        id.hex(),
                        cc_common::COMMIT_ID_LENGTH,
                        pid.len()
                    )
                    .into(),
                ));
            }
            if pid.as_slice() == cc_common::ROOT_COMMIT_ID_BYTES {
                has_root_parent = true;
            }
        }
        if has_root_parent && proto_commit.parent_commit_ids.len() > 1 {
            return Err(BackendError::Other(
                format!(
                    "stored commit {} ROOT_COMMIT_ID may only appear as the sole parent",
                    id.hex()
                )
                .into(),
            ));
        }
        for pred in &proto_commit.predecessors {
            if pred.len() != cc_common::COMMIT_ID_LENGTH {
                return Err(BackendError::Other(
                    format!(
                        "stored commit {} predecessor must be {} bytes (got {} bytes)",
                        id.hex(),
                        cc_common::COMMIT_ID_LENGTH,
                        pred.len()
                    )
                    .into(),
                ));
            }
        }

        Ok(commit_from_proto(proto_commit))
    }

    async fn write_commit(
        &self,
        commit: Commit,
        _sign_with: Option<&mut SigningFn>,
    ) -> BackendResult<(CommitId, Commit)> {
        let mut proto_commit = commit_to_proto(&commit);
        let commit_id_bytes =
            compute_git_commit_hash(&proto_commit).map_err(|e| BackendError::Other(e.into()))?;
        proto_commit.commit_id = commit_id_bytes.clone();
        self.store
            .put_commit(
                self.project_id.clone(),
                commit_id_bytes.clone(),
                proto_commit,
            )
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

    fn gc(&self, _index: &dyn Index, _keep_newer: SystemTime) -> BackendResult<()> {
        Ok(())
    }
}

pub struct ServerOpStore {
    store: Arc<dyn Store>,
    repo_id: String,
    root_operation_id: OperationId,
}

impl Debug for ServerOpStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerOpStore")
            .field("repo_id", &self.repo_id)
            .finish()
    }
}

impl ServerOpStore {
    pub fn new(store: Arc<dyn Store>, repo_id: String) -> Self {
        Self {
            store,
            repo_id,
            root_operation_id: OperationId::from_bytes(&cc_common::ROOT_OPERATION_ID_BYTES),
        }
    }
}

#[async_trait]
impl OpStore for ServerOpStore {
    fn name(&self) -> &str {
        "server_op_store"
    }

    fn root_operation_id(&self) -> &OperationId {
        &self.root_operation_id
    }

    async fn read_operation(&self, id: &OperationId) -> OpStoreResult<Operation> {
        if id.as_bytes() == cc_common::ROOT_OPERATION_ID_BYTES {
            let root_op = cc_common::op_store::Operation {
                view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
                parents: vec![],
                metadata: Some(cc_common::op_store::OperationMetadata {
                    start_time_millis: 0,
                    end_time_millis: 0,
                    description: "root()".to_string(),
                    hostname: "".to_string(),
                    username: "".to_string(),
                    is_snapshot: false,
                    workspace_name: None,
                    attributes: std::collections::HashMap::new(),
                }),
                commit_predecessors: vec![],
                commit_predecessors_set: true,
            };
            return Ok(operation_from_proto(root_op));
        }

        let op = self
            .store
            .get_operation(&self.repo_id, id.as_bytes())
            .await
            .map_err(|e| OpStoreError::Other(e.into()))?
            .ok_or_else(|| OpStoreError::ObjectNotFound {
                object_type: "operation".into(),
                hash: id.hex(),
                source: "operation not found in server store".into(),
            })?;

        Ok(operation_from_proto(op))
    }

    async fn write_operation(&self, operation: &Operation) -> OpStoreResult<OperationId> {
        let op_proto = operation_to_proto(operation);
        let op_id_bytes = hash_operation(&op_proto).map_err(|e| OpStoreError::Other(e.into()))?;

        self.store
            .put_operation(self.repo_id.clone(), op_id_bytes.clone(), op_proto)
            .await
            .map_err(|e| OpStoreError::Other(e.into()))?;

        Ok(OperationId::from_bytes(&op_id_bytes))
    }

    async fn read_view(&self, id: &ViewId) -> OpStoreResult<View> {
        if id.as_bytes() == cc_common::ROOT_VIEW_ID_BYTES {
            let root_view = cc_common::op_store::View {
                head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                wc_commit_ids: std::collections::HashMap::new(),
                local_bookmarks: std::collections::HashMap::new(),
                remote_bookmarks: std::collections::HashMap::new(),
            };
            return Ok(view_from_proto(root_view));
        }

        let view = self
            .store
            .get_view(&self.repo_id, id.as_bytes())
            .await
            .map_err(|e| OpStoreError::Other(e.into()))?
            .ok_or_else(|| OpStoreError::ObjectNotFound {
                object_type: "view".into(),
                hash: id.hex(),
                source: "view not found in server store".into(),
            })?;

        Ok(view_from_proto(view))
    }

    async fn write_view(&self, view: &View) -> OpStoreResult<ViewId> {
        let view_proto = view_to_proto(view);
        let view_id_bytes = hash_view(&view_proto).map_err(|e| OpStoreError::Other(e.into()))?;

        self.store
            .put_view(self.repo_id.clone(), view_id_bytes.clone(), view_proto)
            .await
            .map_err(|e| OpStoreError::Other(e.into()))?;

        Ok(ViewId::from_bytes(&view_id_bytes))
    }

    // TODO: Extend function to support partial prefix matching. Currently an open todo on the client side OpStore as well, see lib/src/cc_op_store.rs
    async fn resolve_operation_id_prefix(
        &self,
        prefix: &HexPrefix,
    ) -> OpStoreResult<PrefixResolution<OperationId>> {
        if prefix.matches(&self.root_operation_id) {
            Ok(PrefixResolution::SingleMatch(
                self.root_operation_id.clone(),
            ))
        } else {
            Ok(PrefixResolution::NoMatch)
        }
    }

    async fn gc(
        &self,
        _target_op_ids: &[OperationId],
        _keep_newer: SystemTime,
    ) -> OpStoreResult<()> {
        Ok(())
    }
}

pub struct ServerOpHeadsStore {
    store: Arc<dyn Store>,
    repo_id: String,
    expected_heads: Option<Vec<Vec<u8>>>,
}

impl Debug for ServerOpHeadsStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerOpHeadsStore")
            .field("repo_id", &self.repo_id)
            .finish()
    }
}

pub struct ServerOpHeadsStoreNoLock;
impl OpHeadsStoreLock for ServerOpHeadsStoreNoLock {}

impl ServerOpHeadsStore {
    pub fn new(store: Arc<dyn Store>, repo_id: String) -> Self {
        Self {
            store,
            repo_id,
            expected_heads: None,
        }
    }

    /// Pins `get_op_heads` and `update_op_heads` CAS to a previously validated head snapshot so
    /// concurrent arrivals force a retry instead of bypassing server-side safety checks, and so
    /// ancestor-only head pruning passes exact-set CAS.
    pub fn with_expected_heads(
        store: Arc<dyn Store>,
        repo_id: String,
        expected_heads: Vec<Vec<u8>>,
    ) -> Self {
        Self {
            store,
            repo_id,
            expected_heads: Some(expected_heads),
        }
    }
}

#[async_trait]
impl OpHeadsStore for ServerOpHeadsStore {
    fn name(&self) -> &str {
        "server_op_heads_store"
    }

    async fn update_op_heads(
        &self,
        old_ids: &[OperationId],
        new_id: &OperationId,
    ) -> Result<(), OpHeadsStoreError> {
        let cas_expected: Vec<Vec<u8>> = match &self.expected_heads {
            Some(pinned) => pinned.clone(),
            None => old_ids.iter().map(|id| id.as_bytes().to_vec()).collect(),
        };
        let new_vec = new_id.as_bytes().to_vec();
        let target_id = new_id.clone();

        self.store
            .update_op_heads_compare_and_swap(self.repo_id.clone(), &cas_expected, new_vec)
            .await
            .map_err(|e| OpHeadsStoreError::Write {
                new_op_id: target_id,
                source: e.into(),
            })?;

        Ok(())
    }

    async fn get_op_heads(&self) -> Result<Vec<OperationId>, OpHeadsStoreError> {
        if let Some(pinned) = &self.expected_heads {
            return Ok(pinned.iter().map(|b| OperationId::from_bytes(b)).collect());
        }
        let heads = self
            .store
            .get_op_heads(&self.repo_id)
            .await
            .map_err(|e| OpHeadsStoreError::Read(e.into()))?;

        let head_ids: Vec<OperationId> = heads
            .unwrap_or_else(|| vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()])
            .into_iter()
            .map(|b| OperationId::from_bytes(&b))
            .collect();

        Ok(head_ids)
    }

    async fn lock(&self) -> Result<Box<dyn OpHeadsStoreLock + '_>, OpHeadsStoreError> {
        Ok(Box::new(ServerOpHeadsStoreNoLock))
    }
}

#[derive(Debug)]
pub struct EmptyRevset;

impl Revset for EmptyRevset {
    fn stream<'a>(&self) -> LocalBoxStream<'a, Result<CommitId, RevsetEvaluationError>>
    where
        Self: 'a,
    {
        Box::pin(futures::stream::empty())
    }

    fn commit_change_ids<'a>(
        &self,
    ) -> LocalBoxStream<'a, Result<(CommitId, ChangeId), RevsetEvaluationError>>
    where
        Self: 'a,
    {
        Box::pin(futures::stream::empty())
    }

    fn stream_graph<'a>(
        &self,
    ) -> LocalBoxStream<'a, Result<GraphNode<CommitId>, RevsetEvaluationError>>
    where
        Self: 'a,
    {
        Box::pin(futures::stream::empty())
    }

    fn is_empty(&self) -> bool {
        true
    }

    fn count_estimate(&self) -> Result<(usize, Option<usize>), RevsetEvaluationError> {
        Ok((0, Some(0)))
    }

    fn containing_fn<'a>(&self) -> Box<RevsetContainingFn<'a>>
    where
        Self: 'a,
    {
        Box::new(|_id| Ok(false))
    }
}

/// In-memory implementation of [`Index`], [`ReadonlyIndex`], and [`MutableIndex`]
/// for server-side operations that only manage operation graph metadata.
#[derive(Clone, Debug, Default)]
pub struct ServerIndex;

impl Index for ServerIndex {
    fn shortest_unique_commit_id_prefix_len(&self, _commit_id: &CommitId) -> IndexResult<usize> {
        Ok(12)
    }

    fn resolve_commit_id_prefix(
        &self,
        _prefix: &HexPrefix,
    ) -> IndexResult<PrefixResolution<CommitId>> {
        Ok(PrefixResolution::NoMatch)
    }

    fn has_id(&self, _commit_id: &CommitId) -> IndexResult<bool> {
        Ok(false)
    }

    fn is_ancestor(&self, ancestor_id: &CommitId, descendant_id: &CommitId) -> IndexResult<bool> {
        Ok(ancestor_id == descendant_id)
    }

    fn common_ancestors(
        &self,
        _set1: &[CommitId],
        _set2: &[CommitId],
    ) -> IndexResult<Vec<CommitId>> {
        Ok(vec![])
    }

    fn all_heads_for_gc(&self) -> IndexResult<Box<dyn Iterator<Item = CommitId> + '_>> {
        Err(IndexError::AllHeadsForGcUnsupported)
    }

    fn heads(&self, candidates: &mut dyn Iterator<Item = &CommitId>) -> IndexResult<Vec<CommitId>> {
        Ok(candidates.cloned().collect())
    }

    fn changed_paths_in_commit(
        &self,
        _commit_id: &CommitId,
    ) -> IndexResult<Option<Box<dyn Iterator<Item = RepoPathBuf> + '_>>> {
        Ok(None)
    }

    fn evaluate_revset(
        &self,
        _expression: &ResolvedExpression,
        _store: &Arc<JjStore>,
    ) -> Result<Box<dyn Revset + '_>, RevsetEvaluationError> {
        Ok(Box::new(EmptyRevset))
    }
}

pub struct ServerChangeIdIndex;

impl ChangeIdIndex for ServerChangeIdIndex {
    fn resolve_prefix(
        &self,
        _prefix: &HexPrefix,
    ) -> IndexResult<PrefixResolution<ResolvedChangeTargets>> {
        Ok(PrefixResolution::NoMatch)
    }

    fn shortest_unique_prefix_len(&self, _change_id: &ChangeId) -> IndexResult<usize> {
        Ok(12)
    }
}

impl ReadonlyIndex for ServerIndex {
    fn as_index(&self) -> &dyn Index {
        self
    }

    fn change_id_index(
        &self,
        _heads: &mut dyn Iterator<Item = &CommitId>,
    ) -> Box<dyn ChangeIdIndex> {
        Box::new(ServerChangeIdIndex)
    }

    fn start_modification(&self) -> Box<dyn MutableIndex> {
        Box::new(self.clone())
    }
}

#[async_trait]
impl MutableIndex for ServerIndex {
    fn as_index(&self) -> &dyn Index {
        self
    }

    fn change_id_index(
        &self,
        _heads: &mut dyn Iterator<Item = &CommitId>,
    ) -> Box<dyn ChangeIdIndex + '_> {
        Box::new(ServerChangeIdIndex)
    }

    async fn add_commit(&mut self, _commit: &JjCommit) -> IndexResult<()> {
        Ok(())
    }

    fn merge_in(&mut self, _other: &dyn ReadonlyIndex) -> IndexResult<()> {
        Ok(())
    }
}

/// In-memory [`IndexStore`] for server-side operations with no filesystem dependencies.
#[derive(Debug, Default)]
pub struct ServerIndexStore;

#[async_trait(?Send)]
impl IndexStore for ServerIndexStore {
    fn name(&self) -> &str {
        "server"
    }

    async fn get_index_at_op(
        &self,
        _op: &JjOperation,
        _store: &Arc<JjStore>,
    ) -> IndexStoreResult<Box<dyn ReadonlyIndex>> {
        Ok(Box::new(ServerIndex))
    }

    fn write_index(
        &self,
        _index: Box<dyn MutableIndex>,
        _op: &JjOperation,
    ) -> IndexStoreResult<Box<dyn ReadonlyIndex>> {
        Ok(Box::new(ServerIndex))
    }
}

/// In-memory [`SubmoduleStore`] for server-side operations with no filesystem dependencies.
#[derive(Debug, Default)]
pub struct ServerSubmoduleStore;

impl SubmoduleStore for ServerSubmoduleStore {
    fn name(&self) -> &str {
        "server"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryStore;

    #[tokio::test]
    async fn test_server_backend_write_commit_sets_commit_id_and_read_commit_rejects_corrupt_rows()
    {
        let store = Arc::new(MemoryStore::new());
        store
            .register_project("proj-adapter".to_string(), None)
            .await
            .unwrap();
        let backend = ServerBackend::new(store.clone(), "proj-adapter".to_string());

        let commit = Commit {
            parents: vec![CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES)],
            predecessors: Vec::new(),
            root_tree: jj_lib::merge::Merge::resolved(TreeId::from_bytes(
                &cc_common::EMPTY_TREE_ID_BYTES,
            )),
            conflict_labels: jj_lib::merge::Merge::resolved(String::new()),
            change_id: ChangeId::from_bytes(&[0x33; cc_common::CHANGE_ID_LENGTH]),
            description: "adapter roundtrip".to_string(),
            author: Signature {
                name: "Alice".to_string(),
                email: "alice@example.com".to_string(),
                timestamp: Timestamp {
                    timestamp: MillisSinceEpoch(1_700_000_000_000),
                    tz_offset: 0,
                },
            },
            committer: Signature {
                name: "Alice".to_string(),
                email: "alice@example.com".to_string(),
                timestamp: Timestamp {
                    timestamp: MillisSinceEpoch(1_700_000_000_000),
                    tz_offset: 0,
                },
            },
            secure_sig: None,
        };
        let (written_id, _) = backend.write_commit(commit.clone(), None).await.unwrap();
        let stored = store
            .get_commit("proj-adapter", written_id.as_bytes())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.commit_id, written_id.as_bytes());
        let read_back = backend.read_commit(&written_id).await.unwrap();
        assert_eq!(read_back.description, commit.description);

        // Legacy all-empty per-tree conflict_labels (`["", "", ""]`) on a 3-tree conflicted
        // commit must succeed and canonicalize to unlabeled (`Merge::resolved("")`).
        let legacy_conflict_id = vec![0xc0; cc_common::COMMIT_ID_LENGTH];
        store
            .put_commit(
                "proj-adapter".to_string(),
                legacy_conflict_id.clone(),
                cc_common::backend::Commit {
                    commit_id: legacy_conflict_id.clone(),
                    change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
                    parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                    root_tree_id: vec![
                        vec![0x11; cc_common::TREE_ID_LENGTH],
                        vec![0x22; cc_common::TREE_ID_LENGTH],
                        vec![0x33; cc_common::TREE_ID_LENGTH],
                    ],
                    conflict_labels: vec![String::new(), String::new(), String::new()],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let legacy_read = backend
            .read_commit(&CommitId::from_bytes(&legacy_conflict_id))
            .await
            .expect("legacy all-empty per-tree conflict_labels must succeed");
        assert_eq!(
            legacy_read.conflict_labels,
            jj_lib::merge::Merge::resolved(String::new())
        );

        // Corrupt rows must return BackendError::Other instead of panicking in commit_from_proto.
        let corrupt_cases = [
            // Empty root_tree_id
            cc_common::backend::Commit {
                change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
                parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: Vec::new(),
                ..Default::default()
            },
            // Even-length root_tree_id
            cc_common::backend::Commit {
                change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
                parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: vec![
                    cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                    cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                ],
                ..Default::default()
            },
            // Wrong-length tree ID
            cc_common::backend::Commit {
                change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
                parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: vec![vec![0xaa; 19]],
                ..Default::default()
            },
            // Even-length conflict_labels
            cc_common::backend::Commit {
                change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
                parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
                conflict_labels: vec!["a".to_string(), "b".to_string()],
                ..Default::default()
            },
            // Wrong-length all-empty conflict_labels on 3-tree commit
            cc_common::backend::Commit {
                change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
                parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: vec![
                    cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                    cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                    cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                ],
                conflict_labels: vec![String::new(), String::new()],
                ..Default::default()
            },
            // Wrong-length mixed conflict_labels on 3-tree commit
            cc_common::backend::Commit {
                change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
                parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: vec![
                    cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                    cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                    cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                ],
                conflict_labels: vec!["left".to_string(), String::new()],
                ..Default::default()
            },
            // Non-empty conflict label on resolved commit
            cc_common::backend::Commit {
                change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
                parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
                conflict_labels: vec!["unexpected".to_string()],
                ..Default::default()
            },
            // Newline inside conflict label
            cc_common::backend::Commit {
                change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
                parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: vec![
                    cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                    cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                    cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                ],
                conflict_labels: vec!["a\nb".to_string(), "base".to_string(), "c".to_string()],
                ..Default::default()
            },
            // Wrong-length change_id
            cc_common::backend::Commit {
                change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH - 1],
                parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
                ..Default::default()
            },
            // Empty parent_commit_ids on non-root commit
            cc_common::backend::Commit {
                change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
                parent_commit_ids: Vec::new(),
                root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
                ..Default::default()
            },
            // Wrong-length parent_commit_id
            cc_common::backend::Commit {
                change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
                parent_commit_ids: vec![vec![0xaa; cc_common::COMMIT_ID_LENGTH - 1]],
                root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
                ..Default::default()
            },
            // Mixed ROOT_COMMIT_ID with another parent
            cc_common::backend::Commit {
                change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
                parent_commit_ids: vec![
                    cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
                    vec![0xaa; cc_common::COMMIT_ID_LENGTH],
                ],
                root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
                ..Default::default()
            },
            // Wrong-length predecessor
            cc_common::backend::Commit {
                change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
                parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
                predecessors: vec![vec![0xbb; cc_common::COMMIT_ID_LENGTH - 1]],
                ..Default::default()
            },
        ];

        for (i, bad_commit) in corrupt_cases.into_iter().enumerate() {
            let bad_id_bytes = vec![0xd0 + (i as u8); cc_common::COMMIT_ID_LENGTH];
            store
                .put_commit("proj-adapter".to_string(), bad_id_bytes.clone(), bad_commit)
                .await
                .unwrap();
            let err = backend
                .read_commit(&CommitId::from_bytes(&bad_id_bytes))
                .await
                .unwrap_err();
            assert!(
                matches!(err, BackendError::Other(_)),
                "corrupt case {i} should return BackendError::Other, got {err:?}"
            );
        }
    }
}
