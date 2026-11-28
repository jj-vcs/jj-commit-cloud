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

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use jj_lib::backend::{
    Backend, BackendError, ChangeId, Commit, CommitId, MillisSinceEpoch, Signature, SymlinkId,
    Timestamp, TreeId,
};
use jj_lib::index::{Index, IndexError};
use jj_lib::object_id::ObjectId;
use jj_lib::op_store::{OpStore, OpStoreError, OperationId, ViewId};
use jj_lib::repo_path::RepoPath;

use super::index::COMMIT_GRAPH_CACHE;
use super::op_store::OPERATION_CACHE;
use crate::hash_utils::hash_operation;
use crate::store::StoreError;
use cc_common::backend::Commit as ProtoCommit;
use cc_common::op_store::{Operation as ProtoOperation, View as ProtoView};

use super::*;
use crate::store::{CompositeStoreConfig, MemoryBlobStore, MemoryMetadataStore};

fn dummy_proto_commit(cid: &[u8], parents: Vec<Vec<u8>>) -> ProtoCommit {
    ProtoCommit {
        commit_id: cid.to_vec(),
        change_id: cid[..16].to_vec(),
        parent_commit_ids: parents,
        root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
        description: "test\n".to_string(),
        author: None,
        committer: None,
        predecessors: vec![],
        conflict_labels: vec![],
        secure_sig: None,
        raw_git_commit: vec![],
        external_ids: Default::default(),
    }
}

#[tokio::test]
async fn test_server_backend_write_commit_sets_commit_id_and_read_commit_rejects_corrupt_rows() {
    let meta = Arc::new(MemoryMetadataStore::new());
    let blobs = Arc::new(MemoryBlobStore::new());
    let store = Arc::new(CompositeStore::with_config(
        meta.clone(),
        blobs,
        CompositeStoreConfig {
            inline_blob_threshold: 0,
            validate_dependencies: false,
        },
    ));
    store
        .register_repo_in_project("repo-adapter", Some("proj-adapter"), None)
        .await
        .unwrap();
    let backend = ServerBackend::new(store.clone(), "repo-adapter".to_string());

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
        .unwrap();
    assert_eq!(stored.commit_id, written_id.as_bytes());
    let read_back = backend.read_commit(&written_id).await.unwrap();
    assert_eq!(read_back.description, commit.description);

    // Legacy all-empty per-tree conflict_labels (`["", "", ""]`) on a 3-tree conflicted
    // commit must succeed and canonicalize to unlabeled (`Merge::resolved("")`).
    let legacy_conflict_id = vec![0xc0; cc_common::COMMIT_ID_LENGTH];
    store
        .put_commit(
            "proj-adapter",
            &legacy_conflict_id,
            &ProtoCommit {
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

    // Corrupt rows must return BackendError::Other(StoreError::Corruption) instead of panicking.
    let corrupt_cases = [
        // Empty root_tree_id
        ProtoCommit {
            change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
            parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
            root_tree_id: Vec::new(),
            ..Default::default()
        },
        // Even-length root_tree_id
        ProtoCommit {
            change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
            parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
            root_tree_id: vec![
                cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
            ],
            ..Default::default()
        },
        // Wrong-length tree ID
        ProtoCommit {
            change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
            parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
            root_tree_id: vec![vec![0xaa; 19]],
            ..Default::default()
        },
        // Wrong-length change ID
        ProtoCommit {
            change_id: vec![0x11; 8],
            parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
            root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
            ..Default::default()
        },
        // Even-length conflict_labels
        ProtoCommit {
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
        ProtoCommit {
            change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
            parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
            root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
            conflict_labels: vec!["unexpected".to_string()],
            ..Default::default()
        },
        // Newline inside conflict label
        ProtoCommit {
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
        meta.put_commit("proj-adapter", &bad_id_bytes, &bad_commit, 1)
            .await
            .unwrap();
        let err = backend
            .read_commit(&CommitId::from_bytes(&bad_id_bytes))
            .await
            .unwrap_err();
        match err {
            BackendError::Other(source) => {
                assert!(
                    matches!(
                        source.downcast_ref::<StoreError>(),
                        Some(StoreError::Corruption(_))
                    ),
                    "corrupt case {i} must wrap StoreError::Corruption, got {source:?}"
                );
            }
            other => {
                panic!("corrupt case {i} should return BackendError::Other, got {other:?}")
            }
        }
    }
}

#[tokio::test]
async fn test_server_op_store_rejects_corrupt_operation_and_view_digests() {
    let meta = Arc::new(MemoryMetadataStore::new());
    let store = Arc::new(CompositeStore::with_config(
        meta.clone(),
        Arc::new(MemoryBlobStore::new()),
        CompositeStoreConfig {
            inline_blob_threshold: 0,
            validate_dependencies: false,
        },
    ));
    store.register_repo("corrupt-op-repo", None).await.unwrap();
    let op_store = ServerOpStore::new(store, "corrupt-op-repo".to_string());

    let bad_op_id = vec![0xe1; cc_common::OPERATION_ID_LENGTH];
    meta.put_operation(
        "corrupt-op-repo",
        &bad_op_id,
        &ProtoOperation {
            view_id: vec![0x01; 4],
            parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
            metadata: None,
            commit_predecessors: Vec::new(),
            commit_predecessors_set: true,
        },
    )
    .await
    .unwrap();
    let op_err = op_store
        .read_operation(&OperationId::from_bytes(&bad_op_id))
        .await
        .unwrap_err();
    match op_err {
        OpStoreError::Other(source) => {
            assert!(matches!(
                source.downcast_ref::<StoreError>(),
                Some(StoreError::Corruption(_))
            ));
        }
        other => panic!("expected OpStoreError::Other(Corruption), got {other:?}"),
    }

    let bad_view_id = vec![0xe2; cc_common::VIEW_ID_LENGTH];
    meta.put_view(
        "corrupt-op-repo",
        &bad_view_id,
        &ProtoView {
            head_ids: vec![vec![0xaa; 5]],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: HashMap::new(),
        },
    )
    .await
    .unwrap();
    let view_err = op_store
        .read_view(&ViewId::from_bytes(&bad_view_id))
        .await
        .unwrap_err();
    match view_err {
        OpStoreError::Other(source) => {
            assert!(matches!(
                source.downcast_ref::<StoreError>(),
                Some(StoreError::Corruption(_))
            ));
        }
        other => panic!("expected OpStoreError::Other(Corruption), got {other:?}"),
    }
}

#[tokio::test]
async fn test_server_index_prunes_by_generation_and_errors_on_missing_parents() {
    let meta = Arc::new(MemoryMetadataStore::new());
    let blobs = Arc::new(MemoryBlobStore::new());
    let store = Arc::new(CompositeStore::with_config(
        meta.clone(),
        blobs,
        CompositeStoreConfig {
            inline_blob_threshold: 0,
            validate_dependencies: false,
        },
    ));
    store.register_repo("prune-repo", None).await.unwrap();

    let missing_anc = vec![0xaa; 20];
    let c1 = vec![0x01; 20];
    let c198 = vec![0xc6; 20];
    let c199 = vec![0xc7; 20];

    // Persist `c1` (gen 1), `c198` (gen 199 -> parent `missing_anc`), and `c199` (gen 200 ->
    // parent `c198`), intentionally leaving `missing_anc` absent from storage.
    meta.put_commit(
        "prune-repo",
        &c1,
        &dummy_proto_commit(&c1, vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()]),
        1,
    )
    .await
    .unwrap();
    meta.put_commit(
        "prune-repo",
        &c198,
        &dummy_proto_commit(&c198, vec![missing_anc.clone()]),
        199,
    )
    .await
    .unwrap();
    meta.put_commit(
        "prune-repo",
        &c199,
        &dummy_proto_commit(&c199, vec![c198.clone()]),
        200,
    )
    .await
    .unwrap();

    let jj_c1 = CommitId::from_bytes(&c1);
    let jj_c198 = CommitId::from_bytes(&c198);
    let jj_c199 = CommitId::from_bytes(&c199);

    let idx = ServerIndex::new(store.clone(), "prune-repo".to_string());
    // `heads([c198, c199])` and `is_ancestor(c198, c199)` stop at generation 199 without
    // walking into `missing_anc`.
    assert!(idx.is_ancestor(&jj_c198, &jj_c199).await.unwrap());
    assert!(!idx.is_ancestor(&jj_c199, &jj_c198).await.unwrap());
    assert_eq!(
        idx.heads(&mut [&jj_c198, &jj_c199].into_iter())
            .await
            .unwrap(),
        vec![jj_c199.clone()]
    );

    // Walking past generation 199 toward `c1` (gen 1) encounters `missing_anc` and surfaces
    // a corruption error instead of silently treating `c198` as a root.
    let err = idx.is_ancestor(&jj_c1, &jj_c199).await.unwrap_err();
    assert!(
        err.to_string().contains("corrupt commit graph"),
        "expected corruption error for missing parent, got: {err}"
    );
}

#[tokio::test]
async fn test_cross_reconcile_caches_retain_commits_and_operations() {
    let store = Arc::new(CompositeStore::in_memory());
    store.register_repo("cache-repo", None).await.unwrap();

    let cid = vec![0x44; 20];
    store
        .put_commit(
            "cache-repo",
            &cid,
            &dummy_proto_commit(&cid, vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()]),
        )
        .await
        .unwrap();

    let op_proto = ProtoOperation {
        view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
        parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
        metadata: None,
        commit_predecessors: vec![],
        commit_predecessors_set: true,
    };
    let op_id_bytes = hash_operation(&op_proto).unwrap();
    store
        .put_operation("cache-repo", &op_id_bytes, &op_proto)
        .await
        .unwrap();

    let idx1 = ServerIndex::new(store.clone(), "cache-repo".to_string());
    let jj_cid = CommitId::from_bytes(&cid);
    assert!(idx1.has_id(&jj_cid).await.unwrap());

    let op_store1 = ServerOpStore::new(store.clone(), "cache-repo".to_string());
    let jj_op_id = OperationId::from_bytes(&op_id_bytes);
    let read1 = op_store1.read_operation(&jj_op_id).await.unwrap();

    // Fresh adapter instances constructed in a subsequent reconcile hit the shared caches.
    let key_c = (store_cache_key(&store), "cache-repo".to_string(), jj_cid);
    assert!(
        COMMIT_GRAPH_CACHE
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&key_c)
            .is_some()
    );
    let key_op = (
        store_cache_key(&store),
        "cache-repo".to_string(),
        "cache-repo".to_string(),
        jj_op_id.clone(),
    );
    assert!(
        OPERATION_CACHE
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entries
            .get(&key_op)
            .is_some()
    );

    let op_store2 = ServerOpStore::new(store, "cache-repo".to_string());
    let read2 = op_store2.read_operation(&jj_op_id).await.unwrap();
    assert_eq!(read1.view_id, read2.view_id);
}

#[derive(Clone, Copy)]
#[allow(clippy::enum_variant_names)]
enum FaultMode {
    CommitNoneGenErr,
    CommitErrGenNone,
    CommitSomeGenNone,
    CommitNoneGenSome,
}

struct FaultyCommitMeta {
    inner: MemoryMetadataStore,
    fault_commit_id: Vec<u8>,
    mode: FaultMode,
}

#[async_trait]
impl MetadataStore for FaultyCommitMeta {
    fn backend_name(&self) -> &'static str {
        "faulty_memory"
    }
    async fn register_project(
        &self,
        project_id: &str,
        name: Option<&str>,
    ) -> crate::store::StoreResult<()> {
        self.inner.register_project(project_id, name).await
    }
    async fn is_project_registered(&self, project_id: &str) -> crate::store::StoreResult<bool> {
        self.inner.is_project_registered(project_id).await
    }
    async fn get_repo_project_id(
        &self,
        repo_id: &str,
    ) -> crate::store::StoreResult<Option<String>> {
        self.inner.get_repo_project_id(repo_id).await
    }
    async fn register_repo_in_project(
        &self,
        repo_id: &str,
        project_id: &str,
        name: Option<&str>,
    ) -> crate::store::StoreResult<String> {
        self.inner
            .register_repo_in_project(repo_id, project_id, name)
            .await
    }
    async fn delete_repository(
        &self,
        repo_id: &str,
        expected_project_id: Option<&str>,
    ) -> crate::store::StoreResult<bool> {
        self.inner
            .delete_repository(repo_id, expected_project_id)
            .await
    }
    async fn get_commit(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> crate::store::StoreResult<Option<ProtoCommit>> {
        if commit_id == self.fault_commit_id.as_slice() {
            return match self.mode {
                FaultMode::CommitNoneGenErr | FaultMode::CommitNoneGenSome => Ok(None),
                FaultMode::CommitErrGenNone => {
                    Err(StoreError::Backend("injected commit read failure".into()))
                }
                FaultMode::CommitSomeGenNone => Ok(Some(dummy_proto_commit(
                    commit_id,
                    vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                ))),
            };
        }
        self.inner.get_commit(project_id, commit_id).await
    }
    async fn get_commit_generation(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> crate::store::StoreResult<Option<u64>> {
        if commit_id == self.fault_commit_id.as_slice() {
            return match self.mode {
                FaultMode::CommitNoneGenErr => {
                    Err(StoreError::Unavailable("injected gen unavailable".into()))
                }
                FaultMode::CommitErrGenNone | FaultMode::CommitSomeGenNone => Ok(None),
                FaultMode::CommitNoneGenSome => Ok(Some(7)),
            };
        }
        self.inner
            .get_commit_generation(project_id, commit_id)
            .await
    }
    async fn put_commit(
        &self,
        project_id: &str,
        commit_id: &[u8],
        commit: &ProtoCommit,
        generation: u64,
    ) -> crate::store::StoreResult<()> {
        self.inner
            .put_commit(project_id, commit_id, commit, generation)
            .await
    }
    async fn list_commit_ids(
        &self,
        project_id: &str,
        after_commit_id: Option<&[u8]>,
        limit: usize,
    ) -> crate::store::StoreResult<Vec<crate::store::CommitId>> {
        self.inner
            .list_commit_ids(project_id, after_commit_id, limit)
            .await
    }
    async fn current_commit_watermark(
        &self,
        project_id: &str,
    ) -> crate::store::StoreResult<Vec<u8>> {
        self.inner.current_commit_watermark(project_id).await
    }
    async fn list_commit_ids_since(
        &self,
        project_id: &str,
        since_token: &[u8],
        limit: usize,
    ) -> crate::store::StoreResult<(Vec<crate::store::CommitId>, Vec<u8>, bool)> {
        self.inner
            .list_commit_ids_since(project_id, since_token, limit)
            .await
    }
    async fn resolve_commit_id_prefix(
        &self,
        project_id: &str,
        min_id: &[u8],
        max_id: &[u8],
    ) -> crate::store::StoreResult<Vec<crate::store::CommitId>> {
        self.inner
            .resolve_commit_id_prefix(project_id, min_id, max_id)
            .await
    }
    async fn resolve_change_id_prefix(
        &self,
        project_id: &str,
        min_id: &[u8],
        max_id: &[u8],
        limit: usize,
    ) -> crate::store::StoreResult<Vec<(Vec<u8>, Vec<crate::store::CommitId>)>> {
        self.inner
            .resolve_change_id_prefix(project_id, min_id, max_id, limit)
            .await
    }
    async fn get_tree(
        &self,
        project_id: &str,
        tree_id: &[u8],
    ) -> crate::store::StoreResult<Option<Vec<cc_common::backend::TreeEntry>>> {
        self.inner.get_tree(project_id, tree_id).await
    }
    async fn put_tree(
        &self,
        project_id: &str,
        tree_id: &[u8],
        entries: &[cc_common::backend::TreeEntry],
    ) -> crate::store::StoreResult<()> {
        self.inner.put_tree(project_id, tree_id, entries).await
    }
    async fn find_existing_trees(
        &self,
        project_id: &str,
        tree_ids: &[Vec<u8>],
    ) -> crate::store::StoreResult<HashSet<Vec<u8>>> {
        self.inner.find_existing_trees(project_id, tree_ids).await
    }
    async fn get_symlink(
        &self,
        project_id: &str,
        symlink_id: &[u8],
    ) -> crate::store::StoreResult<Option<String>> {
        self.inner.get_symlink(project_id, symlink_id).await
    }
    async fn put_symlink(
        &self,
        project_id: &str,
        symlink_id: &[u8],
        target: &str,
    ) -> crate::store::StoreResult<()> {
        self.inner.put_symlink(project_id, symlink_id, target).await
    }
    async fn find_existing_symlinks(
        &self,
        project_id: &str,
        symlink_ids: &[Vec<u8>],
    ) -> crate::store::StoreResult<HashSet<Vec<u8>>> {
        self.inner
            .find_existing_symlinks(project_id, symlink_ids)
            .await
    }
    async fn get_blob_record(
        &self,
        project_id: &str,
        blob_id: &[u8],
    ) -> crate::store::StoreResult<Option<crate::store::BlobRecord>> {
        self.inner.get_blob_record(project_id, blob_id).await
    }
    async fn get_blob_sizes_batch(
        &self,
        project_id: &str,
        blob_ids: &[Vec<u8>],
    ) -> crate::store::StoreResult<HashMap<Vec<u8>, u64>> {
        self.inner.get_blob_sizes_batch(project_id, blob_ids).await
    }
    async fn get_global_blob_record(
        &self,
        blob_id: &[u8],
    ) -> crate::store::StoreResult<Option<crate::store::BlobRecord>> {
        self.inner.get_global_blob_record(blob_id).await
    }
    async fn put_blob_record(
        &self,
        project_id: &str,
        record: &crate::store::BlobRecord,
    ) -> crate::store::StoreResult<()> {
        self.inner.put_blob_record(project_id, record).await
    }
    async fn add_blob_replica_location(
        &self,
        blob_id: &[u8],
        location: &str,
    ) -> crate::store::StoreResult<()> {
        self.inner
            .add_blob_replica_location(blob_id, location)
            .await
    }
    async fn get_operation(
        &self,
        repo_id: &str,
        op_id: &[u8],
    ) -> crate::store::StoreResult<Option<ProtoOperation>> {
        self.inner.get_operation(repo_id, op_id).await
    }
    async fn put_operation(
        &self,
        repo_id: &str,
        op_id: &[u8],
        op: &ProtoOperation,
    ) -> crate::store::StoreResult<()> {
        self.inner.put_operation(repo_id, op_id, op).await
    }
    async fn put_operation_for_project(
        &self,
        repo_id: &str,
        expected_project_id: &str,
        op_id: &[u8],
        op: &ProtoOperation,
        require_dependencies_exist: bool,
    ) -> crate::store::StoreResult<()> {
        self.inner
            .put_operation_for_project(
                repo_id,
                expected_project_id,
                op_id,
                op,
                require_dependencies_exist,
            )
            .await
    }
    async fn resolve_operation_id_prefix(
        &self,
        repo_id: &str,
        min_id: &[u8],
        max_id: &[u8],
    ) -> crate::store::StoreResult<Vec<crate::store::OpId>> {
        self.inner
            .resolve_operation_id_prefix(repo_id, min_id, max_id)
            .await
    }
    async fn get_view(
        &self,
        repo_id: &str,
        view_id: &[u8],
    ) -> crate::store::StoreResult<Option<ProtoView>> {
        self.inner.get_view(repo_id, view_id).await
    }
    async fn put_view(
        &self,
        repo_id: &str,
        view_id: &[u8],
        view: &ProtoView,
    ) -> crate::store::StoreResult<()> {
        self.inner.put_view(repo_id, view_id, view).await
    }
    async fn put_view_for_project(
        &self,
        repo_id: &str,
        expected_project_id: &str,
        view_id: &[u8],
        view: &ProtoView,
    ) -> crate::store::StoreResult<()> {
        self.inner
            .put_view_for_project(repo_id, expected_project_id, view_id, view)
            .await
    }
    async fn get_op_heads(
        &self,
        repo_id: &str,
    ) -> crate::store::StoreResult<Option<Vec<crate::store::OpId>>> {
        self.inner.get_op_heads(repo_id).await
    }
    async fn update_op_heads(
        &self,
        repo_id: &str,
        old_ids: &[Vec<u8>],
        new_id: &[u8],
    ) -> crate::store::StoreResult<Vec<crate::store::OpId>> {
        self.inner.update_op_heads(repo_id, old_ids, new_id).await
    }
    async fn update_op_heads_for_project(
        &self,
        repo_id: &str,
        expected_project_id: &str,
        old_head_ids: &[Vec<u8>],
        new_head_id: &[u8],
        require_op_exists: bool,
    ) -> crate::store::StoreResult<Vec<crate::store::OpId>> {
        self.inner
            .update_op_heads_for_project(
                repo_id,
                expected_project_id,
                old_head_ids,
                new_head_id,
                require_op_exists,
            )
            .await
    }
    async fn publish_operation_atomic(
        &self,
        repo_id: &str,
        view_id: &[u8],
        view: &ProtoView,
        op_id: &[u8],
        op: &ProtoOperation,
        old_ids: &[Vec<u8>],
    ) -> crate::store::StoreResult<Vec<crate::store::OpId>> {
        self.inner
            .publish_operation_atomic(repo_id, view_id, view, op_id, op, old_ids)
            .await
    }
    #[allow(clippy::too_many_arguments)]
    async fn publish_operation_for_project(
        &self,
        repo_id: &str,
        expected_project_id: &str,
        view_id: &[u8],
        view: &ProtoView,
        op_id: &[u8],
        op: &ProtoOperation,
        old_op_head_ids: &[Vec<u8>],
        require_dependencies_exist: bool,
    ) -> crate::store::StoreResult<Vec<crate::store::OpId>> {
        self.inner
            .publish_operation_for_project(
                repo_id,
                expected_project_id,
                view_id,
                view,
                op_id,
                op,
                old_op_head_ids,
                require_dependencies_exist,
            )
            .await
    }
    async fn compare_and_swap_op_heads(
        &self,
        repo_id: &str,
        expected_old_ids: &[Vec<u8>],
        new_id: &[u8],
    ) -> crate::store::StoreResult<()> {
        self.inner
            .compare_and_swap_op_heads(repo_id, expected_old_ids, new_id)
            .await
    }
    async fn update_op_heads_cas_for_project(
        &self,
        repo_id: &str,
        expected_project_id: &str,
        expected_exact_heads: &[Vec<u8>],
        new_head_id: &[u8],
        require_op_exists: bool,
    ) -> crate::store::StoreResult<()> {
        self.inner
            .update_op_heads_cas_for_project(
                repo_id,
                expected_project_id,
                expected_exact_heads,
                new_head_id,
                require_op_exists,
            )
            .await
    }
    async fn get_workspace(
        &self,
        repo_id: &str,
        workspace_name: &str,
    ) -> crate::store::StoreResult<Option<cc_common::workspace::WorkspaceState>> {
        self.inner.get_workspace(repo_id, workspace_name).await
    }
    async fn put_workspace_cas(
        &self,
        workspace: &cc_common::workspace::WorkspaceState,
        expected_operation_id: Option<&[u8]>,
    ) -> crate::store::StoreResult<()> {
        self.inner
            .put_workspace_cas(workspace, expected_operation_id)
            .await
    }
    async fn put_workspace_cas_for_project(
        &self,
        expected_project_id: &str,
        workspace: &cc_common::workspace::WorkspaceState,
        expected_operation_id: Option<&[u8]>,
        require_operation_exists: bool,
    ) -> crate::store::StoreResult<()> {
        self.inner
            .put_workspace_cas_for_project(
                expected_project_id,
                workspace,
                expected_operation_id,
                require_operation_exists,
            )
            .await
    }
    async fn list_workspaces(
        &self,
        repo_id: &str,
    ) -> crate::store::StoreResult<Vec<cc_common::workspace::WorkspaceState>> {
        self.inner.list_workspaces(repo_id).await
    }
    async fn delete_workspace(
        &self,
        repo_id: &str,
        workspace_name: &str,
    ) -> crate::store::StoreResult<bool> {
        self.inner.delete_workspace(repo_id, workspace_name).await
    }
}

#[tokio::test]
async fn test_server_index_has_id_propagates_errors_and_detects_inconsistent_rows() {
    let target_bytes = vec![0x91; cc_common::COMMIT_ID_LENGTH];
    let target_id = CommitId::from_bytes(&target_bytes);
    let absent_id = CommitId::from_bytes(&[0x92; cc_common::COMMIT_ID_LENGTH]);

    let cases = [
        (FaultMode::CommitNoneGenErr, "unavailable"),
        (FaultMode::CommitErrGenNone, "backend"),
        (FaultMode::CommitSomeGenNone, "corruption"),
        (FaultMode::CommitNoneGenSome, "corruption"),
    ];
    for (mode, expected_kind) in cases {
        let meta = Arc::new(FaultyCommitMeta {
            inner: MemoryMetadataStore::new(),
            fault_commit_id: target_bytes.clone(),
            mode,
        });
        let store = Arc::new(CompositeStore::new(meta, Arc::new(MemoryBlobStore::new())));
        store.register_repo("fault-repo", None).await.unwrap();
        let idx = ServerIndex::new(store, "fault-repo".to_string());

        assert!(!idx.has_id(&absent_id).await.unwrap());

        let err = idx.has_id(&target_id).await.unwrap_err();
        match err {
            IndexError::Other(source) => {
                let store_err = source
                    .downcast_ref::<StoreError>()
                    .expect("expected StoreError");
                match expected_kind {
                    "unavailable" => assert!(
                        matches!(store_err, StoreError::Unavailable(_)),
                        "{store_err:?}"
                    ),
                    "backend" => {
                        assert!(matches!(store_err, StoreError::Backend(_)), "{store_err:?}")
                    }
                    "corruption" => assert!(
                        matches!(store_err, StoreError::Corruption(_)),
                        "{store_err:?}"
                    ),
                    _ => unreachable!(),
                }
            }
            other => panic!("expected IndexError::Other, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn test_server_backend_read_tree_rejects_duplicate_entry_names() {
    let meta = Arc::new(MemoryMetadataStore::new());
    let store = Arc::new(CompositeStore::with_config(
        meta.clone(),
        Arc::new(MemoryBlobStore::new()),
        CompositeStoreConfig {
            inline_blob_threshold: 0,
            validate_dependencies: false,
        },
    ));
    store.register_repo("dup-tree-repo", None).await.unwrap();

    let make_entry = |name: &str, fill: u8| cc_common::backend::TreeEntry {
        name: name.to_string(),
        value: Some(cc_common::backend::TreeValue {
            value: Some(cc_common::backend::tree_value::Value::File(
                cc_common::backend::File {
                    id: vec![fill; cc_common::FILE_ID_LENGTH],
                    executable: false,
                    copy_id: Vec::new(),
                    size_bytes: None,
                },
            )),
        }),
    };

    let dup_tree_id = vec![0xab; cc_common::TREE_ID_LENGTH];
    meta.put_tree(
        "dup-tree-repo",
        &dup_tree_id,
        &[
            make_entry("dup.txt", 0x11),
            make_entry("other.txt", 0x22),
            make_entry("dup.txt", 0x33),
        ],
    )
    .await
    .unwrap();

    let backend = ServerBackend::new(store, "dup-tree-repo".to_string());
    let err = backend
        .read_tree(RepoPath::root(), &TreeId::from_bytes(&dup_tree_id))
        .await
        .unwrap_err();
    match err {
        BackendError::Other(source) => {
            let store_err = source
                .downcast_ref::<StoreError>()
                .expect("expected StoreError::Corruption");
            assert!(
                matches!(store_err, StoreError::Corruption(msg) if msg.contains("duplicate entry")),
                "expected duplicate-entry Corruption error, got {store_err:?}"
            );
        }
        other => panic!("expected BackendError::Other(Corruption), got {other:?}"),
    }
}

#[tokio::test]
async fn test_server_backend_symlink_nul_rejection() {
    let store = Arc::new(CompositeStore::in_memory());
    store.register_repo("sym-adapter-repo", None).await.unwrap();
    let backend = ServerBackend::new(store.clone(), "sym-adapter-repo".to_string());

    let write_err = backend
        .write_symlink(RepoPath::root(), "bad\0target")
        .await
        .unwrap_err();
    match write_err {
        BackendError::Other(source) => {
            let store_err = source
                .downcast_ref::<StoreError>()
                .expect("expected StoreError::InvalidArgument");
            assert!(
                matches!(store_err, StoreError::InvalidArgument(_)),
                "expected InvalidArgument, got {store_err:?}"
            );
        }
        other => panic!("expected BackendError::Other(InvalidArgument), got {other:?}"),
    }

    let corrupt_id = [0x9bu8; cc_common::SYMLINK_ID_LENGTH];
    store
        .metadata()
        .put_symlink("sym-adapter-repo", &corrupt_id, "corrupt\0target")
        .await
        .unwrap();
    let read_err = backend
        .read_symlink(RepoPath::root(), &SymlinkId::from_bytes(&corrupt_id))
        .await
        .unwrap_err();
    match read_err {
        BackendError::Other(source) => {
            let store_err = source
                .downcast_ref::<StoreError>()
                .expect("expected StoreError::Corruption");
            assert!(
                matches!(store_err, StoreError::Corruption(_)),
                "expected Corruption, got {store_err:?}"
            );
        }
        other => panic!("expected BackendError::Other(Corruption), got {other:?}"),
    }
}
#[tokio::test]
async fn test_server_index_is_ancestor_heads_and_common_ancestors_walk_budgets() {
    let meta = Arc::new(MemoryMetadataStore::new());
    let store = Arc::new(CompositeStore::with_config(
        meta.clone(),
        Arc::new(MemoryBlobStore::new()),
        CompositeStoreConfig {
            inline_blob_threshold: 0,
            validate_dependencies: false,
        },
    ));
    store.register_repo("budget-repo", None).await.unwrap();

    let root = cc_common::ROOT_COMMIT_ID_BYTES.to_vec();
    let c0 = vec![0x10; 20];
    let c1 = vec![0x11; 20];
    let c2 = vec![0x12; 20];
    let c3 = vec![0x13; 20];
    let c_side = vec![0x1f; 20];
    for (cid, parent, gen_num) in [
        (&c0, &root, 1u64),
        (&c1, &c0, 2u64),
        (&c2, &c1, 3u64),
        (&c3, &c2, 4u64),
        (&c_side, &root, 1u64),
    ] {
        meta.put_commit(
            "budget-repo",
            cid,
            &dummy_proto_commit(cid, vec![parent.clone()]),
            gen_num,
        )
        .await
        .unwrap();
    }

    // Legacy commits with unknown generation (`u64::MAX`) must still respect walk caps.
    let u0 = vec![0x20; 20];
    let u1 = vec![0x21; 20];
    let u2 = vec![0x22; 20];
    let u_side = vec![0x2f; 20];
    for (cid, parent) in [(&u0, &root), (&u1, &u0), (&u2, &u1), (&u_side, &root)] {
        meta.put_commit(
            "budget-repo",
            cid,
            &dummy_proto_commit(cid, vec![parent.clone()]),
            u64::MAX,
        )
        .await
        .unwrap();
    }

    let jj_c0 = CommitId::from_bytes(&c0);
    let jj_c1 = CommitId::from_bytes(&c1);
    let jj_c3 = CommitId::from_bytes(&c3);
    let jj_c_side = CommitId::from_bytes(&c_side);
    let jj_u2 = CommitId::from_bytes(&u2);
    let jj_u_side = CommitId::from_bytes(&u_side);

    let assert_budget_err = |err: IndexError, expected_limit: usize| match err {
        IndexError::Other(source) => {
            let budget_err = source
                .downcast_ref::<IndexWalkBudgetExceeded>()
                .unwrap_or_else(|| panic!("expected IndexWalkBudgetExceeded, got {source:?}"));
            assert_eq!(budget_err.limit, expected_limit);
        }
        other => panic!("expected IndexError::Other, got {other:?}"),
    };

    // Per-walk budget fails closed on deep independent branch and legacy u64::MAX commits.
    let tight = ServerIndex::with_walk_limits(
        store.clone(),
        "budget-repo".to_string(),
        IndexWalkLimits {
            max_walk_commits: 2,
            max_total_walk_visits: 100,
        },
    );
    assert_budget_err(tight.is_ancestor(&jj_c_side, &jj_c3).await.unwrap_err(), 2);
    assert_budget_err(tight.is_ancestor(&jj_u_side, &jj_u2).await.unwrap_err(), 2);
    assert_budget_err(
        tight
            .heads(&mut [&jj_c3, &jj_c_side].into_iter())
            .await
            .unwrap_err(),
        2,
    );
    assert_budget_err(
        tight
            .heads(&mut [&jj_u2, &jj_u_side].into_iter())
            .await
            .unwrap_err(),
        2,
    );
    assert_budget_err(
        tight
            .common_ancestors(
                std::slice::from_ref(&jj_c3),
                std::slice::from_ref(&jj_c_side),
            )
            .await
            .unwrap_err(),
        2,
    );

    // Ample budget succeeds for the exact same queries.
    let ample = ServerIndex::with_walk_limits(
        store.clone(),
        "budget-repo".to_string(),
        IndexWalkLimits {
            max_walk_commits: 16,
            max_total_walk_visits: 100,
        },
    );
    assert!(!ample.is_ancestor(&jj_c_side, &jj_c3).await.unwrap());
    assert!(ample.is_ancestor(&jj_c0, &jj_c3).await.unwrap());
    assert!(!ample.is_ancestor(&jj_u_side, &jj_u2).await.unwrap());
    assert_eq!(
        ample
            .heads(&mut [&jj_c3, &jj_c_side, &jj_c0].into_iter())
            .await
            .unwrap(),
        vec![jj_c3.clone(), jj_c_side.clone()]
    );
    assert_eq!(
        ample
            .common_ancestors(
                std::slice::from_ref(&jj_c3),
                std::slice::from_ref(&jj_c_side)
            )
            .await
            .unwrap(),
        vec![CommitId::from_bytes(&root)]
    );

    // Aggregate visit budget caps repeated walks even when commits hit COMMIT_GRAPH_CACHE
    // and is shared across cloned ServerIndex instances.
    let shared_budget = IndexWalkBudget::new(IndexWalkLimits {
        max_walk_commits: 16,
        max_total_walk_visits: 3,
    });
    let idx_a = ServerIndex::with_budget(
        store.clone(),
        "budget-repo".to_string(),
        shared_budget.clone(),
    );
    let idx_b = ServerIndex::with_budget(store, "budget-repo".to_string(), shared_budget);
    // First walk visits c1 and c0 (2 aggregate visits); second visits c1 (3rd) and c0 (4th -> err).
    assert!(!idx_a.is_ancestor(&jj_c_side, &jj_c1).await.unwrap());
    assert_budget_err(idx_b.is_ancestor(&jj_c_side, &jj_c1).await.unwrap_err(), 3);
}

#[tokio::test]
async fn test_server_op_store_operation_cache_invalidated_on_delete_and_scoped_by_project() {
    let meta = Arc::new(MemoryMetadataStore::new());
    let store = Arc::new(CompositeStore::new(
        meta.clone(),
        Arc::new(MemoryBlobStore::new()),
    ));
    store.register_project("proj-a", None).await.unwrap();
    store.register_project("proj-b", None).await.unwrap();
    store
        .register_repo_in_project("cache-del-repo", Some("proj-a"), None)
        .await
        .unwrap();

    let op_proto = ProtoOperation {
        view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
        parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
        metadata: None,
        commit_predecessors: Vec::new(),
        commit_predecessors_set: true,
    };
    let op_id_bytes = hash_operation(&op_proto).unwrap();
    let jj_op_id = OperationId::from_bytes(&op_id_bytes);
    let root_op_id = OperationId::from_bytes(&cc_common::ROOT_OPERATION_ID_BYTES);
    store
        .put_operation("cache-del-repo", &op_id_bytes, &op_proto)
        .await
        .unwrap();

    // Warm OPERATION_CACHE with both a persisted operation and the synthesized root operation.
    let unpinned = ServerOpStore::new(store.clone(), "cache-del-repo".to_string());
    let pinned_a = ServerOpStore::with_project_id(
        store.clone(),
        "cache-del-repo".to_string(),
        "proj-a".to_string(),
    );
    unpinned.read_operation(&jj_op_id).await.unwrap();
    unpinned.read_operation(&root_op_id).await.unwrap();
    pinned_a.read_operation(&jj_op_id).await.unwrap();

    // A rejected DeleteRepository (mismatched project_id -> FailedPrecondition) must NOT
    // evict this repository's cached entries from `OPERATION_CACHE`.
    let cache_key = (
        store_cache_key(&store),
        "cache-del-repo".to_string(),
        "proj-a".to_string(),
        jj_op_id.clone(),
    );
    assert!(matches!(
        store
            .delete_repository("cache-del-repo", Some("proj-b"))
            .await,
        Err(StoreError::FailedPrecondition(_))
    ));
    {
        let cache = OPERATION_CACHE.lock().unwrap_or_else(|e| e.into_inner());
        assert!(cache.entries.get(&cache_key).is_some());
    }

    // After DeleteRepository, neither unpinned nor pinned ServerOpStore may serve cached
    // operations or cached root ops.
    assert!(
        store
            .delete_repository("cache-del-repo", Some("proj-a"))
            .await
            .unwrap()
    );
    for (label, res) in [
        ("unpinned op", unpinned.read_operation(&jj_op_id).await),
        ("unpinned root", unpinned.read_operation(&root_op_id).await),
        ("pinned op", pinned_a.read_operation(&jj_op_id).await),
        ("pinned root", pinned_a.read_operation(&root_op_id).await),
    ] {
        let err = res.expect_err(&format!("{label} must fail after DeleteRepository"));
        match err {
            OpStoreError::Other(source) => {
                assert!(
                    matches!(
                        source.downcast_ref::<StoreError>(),
                        Some(StoreError::RepoNotFound { .. })
                    ),
                    "{label}: expected RepoNotFound, got {source:?}"
                );
            }
            other => {
                panic!("{label}: expected OpStoreError::Other(RepoNotFound), got {other:?}")
            }
        }
    }

    // Re-registering under the same project must still NOT serve the deleted cached op.
    store
        .register_repo_in_project("cache-del-repo", Some("proj-a"), None)
        .await
        .unwrap();
    assert!(matches!(
        unpinned.read_operation(&jj_op_id).await,
        Err(OpStoreError::ObjectNotFound { .. })
    ));
    assert!(matches!(
        pinned_a.read_operation(&jj_op_id).await,
        Err(OpStoreError::ObjectNotFound { .. })
    ));

    // Even if another replica deletes directly in MetadataStore (without invoking local
    // CompositeStore::delete_repository) and re-registers under proj-b, project_id scoping in
    // OPERATION_CACHE prevents serving proj-a's cached operation to proj-b.
    store
        .put_operation("cache-del-repo", &op_id_bytes, &op_proto)
        .await
        .unwrap();
    unpinned.read_operation(&jj_op_id).await.unwrap();

    assert!(
        meta.delete_repository("cache-del-repo", Some("proj-a"))
            .await
            .unwrap()
    );
    meta.register_repo_in_project("cache-del-repo", "proj-b", None)
        .await
        .unwrap();
    assert!(matches!(
        unpinned.read_operation(&jj_op_id).await,
        Err(OpStoreError::ObjectNotFound { .. })
    ));
    let pinned_b = ServerOpStore::with_project_id(
        store.clone(),
        "cache-del-repo".to_string(),
        "proj-b".to_string(),
    );
    assert!(matches!(
        pinned_b.read_operation(&jj_op_id).await,
        Err(OpStoreError::ObjectNotFound { .. })
    ));

    // ServerBackend::with_project_id stays strictly pinned to proj-a even after
    // cache-del-repo is re-bound to proj-b, never writing proj-a's objects into proj-b.
    let pinned_backend = ServerBackend::with_project_id(
        store.clone(),
        "cache-del-repo".to_string(),
        "proj-a".to_string(),
    );
    let mut content = futures::io::Cursor::new(b"pinned proj-a payload".to_vec());
    let file_id = pinned_backend
        .write_file(RepoPath::root(), &mut content)
        .await
        .unwrap();
    assert_eq!(
        store
            .get_file_range("proj-a", file_id.as_bytes(), 0, None)
            .await
            .unwrap()
            .as_ref(),
        b"pinned proj-a payload"
    );
    assert!(matches!(
        store
            .get_file_range("proj-b", file_id.as_bytes(), 0, None)
            .await,
        Err(StoreError::ObjectNotFound { .. })
    ));
}
