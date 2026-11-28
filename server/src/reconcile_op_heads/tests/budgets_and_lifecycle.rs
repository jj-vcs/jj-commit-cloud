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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use cc_common::backend::Commit as ProtoCommit;
use cc_common::op_store::{
    Operation as ProtoOperation, RefTarget, RefTargetTerm, View as ProtoView,
};
use jj_lib::op_store::OpStore;

use super::super::{
    MAX_COMMIT_WALK_VISITED, MAX_DIVERGENT_OP_HEADS, reconcile_repo_op_heads_proto,
    reconcile_repo_op_heads_with_limits, requires_client_rewrite_resolution,
};
use super::{make_test_proto_commit, make_test_proto_op, new_commit_predecessors, put_view_and_op};
use crate::jj_lib_adapters::{IndexWalkLimits, ServerIndex, ServerOpStore};
use crate::store::{
    CompositeStore, CompositeStoreConfig, MemoryBlobStore, MemoryMetadataStore, MetadataStore,
};

struct CountingMetadataStore {
    inner: Arc<MemoryMetadataStore>,
    commit_reads: std::sync::atomic::AtomicUsize,
}

impl CountingMetadataStore {
    fn new(inner: Arc<MemoryMetadataStore>) -> Self {
        Self {
            inner,
            commit_reads: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn commit_reads(&self) -> usize {
        self.commit_reads.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl MetadataStore for CountingMetadataStore {
    fn backend_name(&self) -> &'static str {
        "counting-memory"
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
    async fn get_commit(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> crate::store::StoreResult<Option<ProtoCommit>> {
        self.commit_reads
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.get_commit(project_id, commit_id).await
    }
    async fn get_commit_generation(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> crate::store::StoreResult<Option<u64>> {
        self.inner
            .get_commit_generation(project_id, commit_id)
            .await
    }
    async fn get_commit_with_generation(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> crate::store::StoreResult<Option<(ProtoCommit, u64)>> {
        self.commit_reads
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner
            .get_commit_with_generation(project_id, commit_id)
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
    async fn get_workspace(
        &self,
        repo_id: &str,
        user: &str,
        workspace_name: &str,
    ) -> crate::store::StoreResult<Option<cc_common::workspace::WorkspaceState>> {
        self.inner
            .get_workspace(repo_id, user, workspace_name)
            .await
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
    async fn list_workspaces(
        &self,
        repo_id: &str,
    ) -> crate::store::StoreResult<Vec<cc_common::workspace::WorkspaceState>> {
        self.inner.list_workspaces(repo_id).await
    }
    async fn delete_workspace(
        &self,
        repo_id: &str,
        user: &str,
        workspace_name: &str,
    ) -> crate::store::StoreResult<bool> {
        self.inner
            .delete_workspace(repo_id, user, workspace_name)
            .await
    }
}

#[tokio::test]
async fn test_reconcile_prioritizes_newly_added_heads_over_many_unchanged_heads() {
    let inner = Arc::new(MemoryMetadataStore::new());
    let repo_id = "repo-many-unchanged-heads";
    let project_id = "proj-muh";
    let setup_store = CompositeStore::new(inner.clone(), Arc::new(MemoryBlobStore::new()));
    setup_store
        .register_repo_in_project(repo_id, Some(project_id), None)
        .await
        .unwrap();

    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let c_a0 = vec![0xa0; 20];
    let c_a1 = vec![0xa1; 20];
    let c_b0 = vec![0xb0; 20];
    let c_b1 = vec![0xb1; 20];
    for (cid, parent) in [
        (&c_a0, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
        (&c_a1, c_a0.clone()),
        (&c_b0, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
        (&c_b1, c_b0.clone()),
    ] {
        setup_store
            .put_commit(
                project_id,
                cid,
                &make_test_proto_commit(cid, cid[0], parent),
            )
            .await
            .unwrap();
    }

    // Populate more unrelated unchanged heads than MAX_COMMIT_WALK_VISITED so seeding
    // unchanged heads into `collect_exclusive_commits` would exhaust the walk cap.
    let num_unrelated = MAX_COMMIT_WALK_VISITED + 64;
    let mut unrelated_heads = Vec::with_capacity(num_unrelated);
    for i in 0..num_unrelated {
        let mut cid = vec![0u8; 20];
        cid[0] = 0x55;
        cid[16..20].copy_from_slice(&(i as u32).to_be_bytes());
        inner
            .put_commit(
                project_id,
                &cid,
                &make_test_proto_commit(&cid, 0x55, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
                1,
            )
            .await
            .unwrap();
        unrelated_heads.push(cid);
    }

    let mut base_heads = unrelated_heads.clone();
    base_heads.push(c_a0.clone());
    base_heads.push(c_b0.clone());
    let base_op_id = put_view_and_op(
        &setup_store,
        repo_id,
        base_heads,
        vec![root_op.clone()],
        "base",
        vec![],
    )
    .await;

    let mut heads_a = unrelated_heads.clone();
    heads_a.push(c_a1.clone());
    heads_a.push(c_b0.clone());
    let op_a_id = put_view_and_op(
        &setup_store,
        repo_id,
        heads_a,
        vec![base_op_id.clone()],
        "advance a",
        new_commit_predecessors(c_a1.clone()),
    )
    .await;

    let mut heads_b = unrelated_heads;
    heads_b.push(c_a0.clone());
    heads_b.push(c_b1.clone());
    let op_b_id = put_view_and_op(
        &setup_store,
        repo_id,
        heads_b,
        vec![base_op_id],
        "advance b",
        new_commit_predecessors(c_b1.clone()),
    )
    .await;

    setup_store
        .update_op_heads(repo_id, &[root_op], &op_a_id)
        .await
        .unwrap();
    setup_store
        .update_op_heads(repo_id, &[], &op_b_id)
        .await
        .unwrap();

    let counting_meta = Arc::new(CountingMetadataStore::new(inner));
    let counting_store = Arc::new(CompositeStore::new(
        counting_meta.clone(),
        Arc::new(MemoryBlobStore::new()),
    ));
    let op_store: Arc<dyn OpStore> = Arc::new(ServerOpStore::new(
        counting_store.clone(),
        repo_id.to_string(),
    ));
    let index = ServerIndex::new(counting_store.clone(), repo_id.to_string());
    let needs_client = requires_client_rewrite_resolution(
        &counting_store,
        repo_id,
        &op_store,
        &index,
        &[op_a_id, op_b_id],
    )
    .await
    .unwrap();
    assert!(
        !needs_client,
        "normal advancement with >MAX_COMMIT_WALK_VISITED unchanged heads must not trigger client resolution"
    );
    // `collect_exclusive_commits` seeds only `wanted_heads \ unwanted_heads` and probes
    // `unwanted_heads \ wanted_heads` before unchanged heads, so precheck reads strictly
    // the 4 changed commits (c_a0, c_a1, c_b0, c_b1) and never touches the 4,160 unchanged
    // heads.
    assert_eq!(
        counting_meta.commit_reads(),
        4,
        "requires_client_rewrite_resolution should only fetch newly added heads and their parents, never the {num_unrelated} unchanged heads",
    );

    let resp = reconcile_repo_op_heads_proto(counting_store.clone(), repo_id)
        .await
        .unwrap();
    assert!(
        !resp.client_resolution_required && !resp.op_head.is_empty(),
        "normal advancement with >MAX_COMMIT_WALK_VISITED unchanged heads must reconcile cleanly, got {resp:?}"
    );
    let merged_op = counting_store
        .get_operation(repo_id, &resp.op_head)
        .await
        .unwrap()
        .unwrap();
    let merged_view = counting_store
        .get_view(repo_id, &merged_op.view_id)
        .await
        .unwrap()
        .unwrap();
    let merged_heads: HashSet<Vec<u8>> = merged_view.head_ids.into_iter().collect();
    assert_eq!(merged_heads.len(), num_unrelated + 2);
    assert!(merged_heads.contains(&c_a1));
    assert!(merged_heads.contains(&c_b1));
    assert!(!merged_heads.contains(&c_a0));
    assert!(!merged_heads.contains(&c_b0));
}

#[tokio::test]
async fn test_reconcile_prunes_interior_ancestor_ref_when_descendant_head_is_unchanged() {
    for (use_wc, swap_order) in [(false, false), (false, true), (true, false), (true, true)] {
        let store = Arc::new(CompositeStore::in_memory());
        let repo_id = format!("repo-unchanged-interior-wc-{use_wc}-swap-{swap_order}");
        let project_id = "proj-ui";
        store
            .register_repo_in_project(&repo_id, Some(project_id), None)
            .await
            .unwrap();

        let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
        let c0 = vec![0x90; 20];
        let c1 = vec![0x91; 20];
        let c_side = vec![0x92; 20];
        for (cid, parent) in [
            (&c0, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
            (&c1, c0.clone()),
            (&c_side, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
        ] {
            store
                .put_commit(
                    project_id,
                    cid,
                    &make_test_proto_commit(cid, cid[0], parent),
                )
                .await
                .unwrap();
        }

        let base_op_id = put_view_and_op(
            &store,
            &repo_id,
            vec![c1.clone()],
            vec![root_op.clone()],
            "base c1",
            vec![
                cc_common::op_store::CommitPredecessors {
                    commit_id: c0.clone(),
                    predecessor_ids: vec![],
                },
                cc_common::op_store::CommitPredecessors {
                    commit_id: c1.clone(),
                    predecessor_ids: vec![],
                },
            ],
        )
        .await;

        let op_a_id = put_view_and_op(
            &store,
            &repo_id,
            vec![c1.clone(), c_side.clone()],
            vec![base_op_id.clone()],
            "add c_side",
            new_commit_predecessors(c_side.clone()),
        )
        .await;

        let mut wc_commit_ids = HashMap::new();
        let mut local_bookmarks = HashMap::new();
        if use_wc {
            wc_commit_ids.insert("ws1".to_string(), c0.clone());
        } else {
            local_bookmarks.insert(
                "release".to_string(),
                RefTarget {
                    removes: vec![],
                    adds: vec![RefTargetTerm {
                        commit_id: c0.clone(),
                    }],
                },
            );
        }
        let view_b = ProtoView {
            head_ids: vec![c1.clone()],
            wc_commit_ids,
            local_bookmarks,
            remote_bookmarks: HashMap::new(),
        };
        let view_b_id = crate::hash_utils::hash_view(&view_b).unwrap();
        store.put_view(&repo_id, &view_b_id, &view_b).await.unwrap();
        let op_b = make_test_proto_op(view_b_id, vec![base_op_id], "ref interior c0");
        let op_b_id = crate::hash_utils::hash_operation(&op_b).unwrap();
        store
            .put_operation(&repo_id, &op_b_id, &op_b)
            .await
            .unwrap();

        let (first, second) = if swap_order {
            (op_b_id, op_a_id)
        } else {
            (op_a_id, op_b_id)
        };
        store
            .update_op_heads(&repo_id, &[root_op], &first)
            .await
            .unwrap();
        store.update_op_heads(&repo_id, &[], &second).await.unwrap();

        let resp = reconcile_repo_op_heads_proto(store.clone(), &repo_id)
            .await
            .unwrap();
        assert!(
            !resp.client_resolution_required && !resp.op_head.is_empty(),
            "expected server reconciliation (use_wc={use_wc}, swap={swap_order}), got {resp:?}"
        );
        let merged_op = store
            .get_operation(&repo_id, &resp.op_head)
            .await
            .unwrap()
            .unwrap();
        let merged_view = store
            .get_view(&repo_id, &merged_op.view_id)
            .await
            .unwrap()
            .unwrap();
        let head_set: HashSet<Vec<u8>> = merged_view.head_ids.iter().cloned().collect();
        assert_eq!(
            head_set,
            HashSet::from([c1, c_side]),
            "merged View.head_ids must exclude interior ancestor c0 (use_wc={use_wc}, swap={swap_order})"
        );
        if use_wc {
            assert_eq!(merged_view.wc_commit_ids.get("ws1"), Some(&c0));
        } else {
            let release_target = merged_view.local_bookmarks.get("release").unwrap();
            assert_eq!(release_target.adds.len(), 1);
            assert_eq!(release_target.adds[0].commit_id, c0);
        }
    }
}

#[tokio::test]
async fn test_reconcile_fails_closed_above_divergent_op_head_cap() {
    let store = Arc::new(CompositeStore::in_memory());
    let repo_id = "repo-op-head-cap";
    store.register_repo(repo_id, None).await.unwrap();

    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let mut head_ids = Vec::with_capacity(MAX_DIVERGENT_OP_HEADS + 1);
    for i in 0..=MAX_DIVERGENT_OP_HEADS {
        let mut cid = vec![0u8; 20];
        cid[0] = 0xc0;
        cid[16..20].copy_from_slice(&(i as u32).to_be_bytes());
        store
            .put_commit(
                repo_id,
                &cid,
                &make_test_proto_commit(&cid, 0xc0, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
            )
            .await
            .unwrap();
        let op_id = put_view_and_op(
            &store,
            repo_id,
            vec![cid.clone()],
            vec![root_op.clone()],
            &format!("divergent-{i}"),
            new_commit_predecessors(cid),
        )
        .await;
        if i == 0 {
            store
                .update_op_heads(repo_id, std::slice::from_ref(&root_op), &op_id)
                .await
                .unwrap();
        } else {
            store.update_op_heads(repo_id, &[], &op_id).await.unwrap();
        }
        head_ids.push(op_id);
    }

    let resp = reconcile_repo_op_heads_proto(store.clone(), repo_id)
        .await
        .unwrap();
    assert!(
        resp.client_resolution_required,
        "exceeding MAX_DIVERGENT_OP_HEADS must fail closed to client resolution"
    );
    assert!(resp.op_head.is_empty());
    assert_eq!(
        HashSet::<Vec<u8>>::from_iter(resp.current_op_head_ids),
        HashSet::from_iter(head_ids.iter().cloned())
    );
    assert_eq!(
        store.get_op_heads(repo_id).await.unwrap().len(),
        MAX_DIVERGENT_OP_HEADS + 1
    );
}

#[tokio::test]
async fn test_reconcile_commit_walk_budget_exceeded_defers_and_missing_commit_errors() {
    // Abandoning a chain deeper than MAX_COMMIT_WALK_VISITED exceeds
    // `collect_exclusive_commits` budget and fails closed to client resolution.
    {
        let inner = Arc::new(MemoryMetadataStore::new());
        let store = Arc::new(CompositeStore::with_config(
            inner.clone(),
            Arc::new(MemoryBlobStore::new()),
            CompositeStoreConfig {
                inline_blob_threshold: 0,
                validate_dependencies: false,
            },
        ));
        let repo_id = "repo-walk-budget-exceeded";
        let project_id = "proj-wbe";
        store
            .register_repo_in_project(repo_id, Some(project_id), None)
            .await
            .unwrap();

        let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
        let mut prev = cc_common::ROOT_COMMIT_ID_BYTES.to_vec();
        for i in 0..(MAX_COMMIT_WALK_VISITED + 8) {
            let mut cid = vec![0u8; 20];
            cid[0] = 0x77;
            cid[16..20].copy_from_slice(&(i as u32).to_be_bytes());
            inner
                .put_commit(
                    project_id,
                    &cid,
                    &make_test_proto_commit(&cid, 0x77, prev),
                    (i as u64) + 1,
                )
                .await
                .unwrap();
            prev = cid;
        }
        let c_deep = prev;
        let c_side = vec![0x78; 20];
        store
            .put_commit(
                project_id,
                &c_side,
                &make_test_proto_commit(&c_side, 0x78, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
            )
            .await
            .unwrap();

        let base_op = put_view_and_op(
            &store,
            repo_id,
            vec![c_deep.clone()],
            vec![root_op.clone()],
            "base deep chain",
            vec![],
        )
        .await;
        // Branch A abandons the deep chain; Branch B adds sibling `c_side`.
        let op_a = put_view_and_op(
            &store,
            repo_id,
            vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
            vec![base_op.clone()],
            "abandon deep chain",
            vec![],
        )
        .await;
        let op_b = put_view_and_op(
            &store,
            repo_id,
            vec![c_deep, c_side.clone()],
            vec![base_op],
            "add side",
            new_commit_predecessors(c_side),
        )
        .await;

        store
            .update_op_heads(repo_id, &[root_op], &op_a)
            .await
            .unwrap();
        store.update_op_heads(repo_id, &[], &op_b).await.unwrap();

        let resp = reconcile_repo_op_heads_proto(store.clone(), repo_id)
            .await
            .unwrap();
        assert!(
            resp.client_resolution_required,
            "exceeding MAX_COMMIT_WALK_VISITED must defer to client"
        );
        assert!(resp.op_head.is_empty());
        assert_eq!(
            HashSet::<Vec<u8>>::from_iter(store.get_op_heads(repo_id).await.unwrap()),
            HashSet::from([op_a, op_b])
        );
    }

    // Missing parent commit referenced by a divergent head surfaces typed DATA_LOSS
    // without mutating op heads.
    {
        let inner = Arc::new(MemoryMetadataStore::new());
        let store = Arc::new(CompositeStore::with_config(
            inner.clone(),
            Arc::new(MemoryBlobStore::new()),
            CompositeStoreConfig {
                inline_blob_threshold: 0,
                validate_dependencies: false,
            },
        ));
        let repo_id = "repo-missing-commit-index";
        let project_id = "proj-mci";
        store
            .register_repo_in_project(repo_id, Some(project_id), None)
            .await
            .unwrap();

        let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
        let c_missing = vec![0xd0; 20];
        let c_a = vec![0xd1; 20];
        let c_b = vec![0xd2; 20];
        inner
            .put_commit(
                project_id,
                &c_a,
                &make_test_proto_commit(&c_a, 0xd1, c_missing),
                2,
            )
            .await
            .unwrap();
        inner
            .put_commit(
                project_id,
                &c_b,
                &make_test_proto_commit(&c_b, 0xd2, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
                1,
            )
            .await
            .unwrap();

        let op_a = put_view_and_op(
            &store,
            repo_id,
            vec![c_a.clone()],
            vec![root_op.clone()],
            "add c_a",
            new_commit_predecessors(c_a),
        )
        .await;
        let op_b = put_view_and_op(
            &store,
            repo_id,
            vec![c_b.clone()],
            vec![root_op.clone()],
            "add c_b",
            new_commit_predecessors(c_b),
        )
        .await;

        store
            .update_op_heads(repo_id, &[root_op], &op_a)
            .await
            .unwrap();
        store.update_op_heads(repo_id, &[], &op_b).await.unwrap();

        let err = reconcile_repo_op_heads_proto(store.clone(), repo_id)
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            tonic::Code::DataLoss,
            "missing commit in commit graph must preserve Code::DataLoss, got {err:?}"
        );
        assert_eq!(
            HashSet::<Vec<u8>>::from_iter(store.get_op_heads(repo_id).await.unwrap()),
            HashSet::from([op_a, op_b])
        );
    }
}

#[tokio::test]
async fn test_reconcile_enforces_per_walk_and_aggregate_index_visit_budgets() {
    // Precheck passes with zero index walks, while `RepoLoader::load_at_head`
    // invokes `ServerIndex::heads` over a deep independent branch and hits `max_walk_commits`.
    {
        let inner = Arc::new(MemoryMetadataStore::new());
        let store = Arc::new(CompositeStore::with_config(
            inner.clone(),
            Arc::new(MemoryBlobStore::new()),
            CompositeStoreConfig {
                inline_blob_threshold: 0,
                validate_dependencies: false,
            },
        ));
        let repo_id = "repo-load-at-head-budget";
        let project_id = "proj-lahb";
        store
            .register_repo_in_project(repo_id, Some(project_id), None)
            .await
            .unwrap();

        let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
        let root_commit = cc_common::ROOT_COMMIT_ID_BYTES.to_vec();

        let c_a = vec![0xe1; 20];
        inner
            .put_commit(
                project_id,
                &c_a,
                &make_test_proto_commit(&c_a, 0xe1, root_commit.clone()),
                1,
            )
            .await
            .unwrap();

        let mut prev = root_commit.clone();
        let mut branch_b_commits = Vec::new();
        for step in 0..6u8 {
            let mut cid = vec![0xe2; 20];
            cid[19] = step;
            inner
                .put_commit(
                    project_id,
                    &cid,
                    &make_test_proto_commit(&cid, 0xe2, prev),
                    u64::from(step) + 1,
                )
                .await
                .unwrap();
            prev = cid.clone();
            branch_b_commits.push(cid);
        }
        let c_b_tip = prev;

        let op_a = put_view_and_op(
            &store,
            repo_id,
            vec![c_a.clone()],
            vec![root_op.clone()],
            "branch A adds c_a",
            new_commit_predecessors(c_a.clone()),
        )
        .await;
        let op_b = put_view_and_op(
            &store,
            repo_id,
            vec![c_b_tip.clone()],
            vec![root_op.clone()],
            "branch B adds deep chain",
            branch_b_commits
                .into_iter()
                .map(|cid| cc_common::op_store::CommitPredecessors {
                    commit_id: cid,
                    predecessor_ids: Vec::new(),
                })
                .collect(),
        )
        .await;

        store
            .update_op_heads(repo_id, &[root_op], &op_a)
            .await
            .unwrap();
        store.update_op_heads(repo_id, &[], &op_b).await.unwrap();

        // Tight per-walk limit trips inside `RepoLoader::load_at_head(None)` and fails closed
        // without mutating stored op heads.
        let deferred = reconcile_repo_op_heads_with_limits(
            store.clone(),
            repo_id,
            IndexWalkLimits {
                max_walk_commits: 3,
                max_total_walk_visits: 100,
            },
        )
        .await
        .unwrap();
        assert!(
            deferred.client_resolution_required,
            "exceeding per-walk ServerIndex budget in load_at_head must defer to client"
        );
        assert!(deferred.op_head.is_empty());
        assert_eq!(
            HashSet::<Vec<u8>>::from_iter(deferred.current_op_head_ids),
            HashSet::from([op_a.clone(), op_b.clone()])
        );
        assert_eq!(
            HashSet::<Vec<u8>>::from_iter(store.get_op_heads(repo_id).await.unwrap()),
            HashSet::from([op_a, op_b])
        );

        // Raising per-walk limit allows the same heads to reconcile.
        let ok = reconcile_repo_op_heads_with_limits(
            store.clone(),
            repo_id,
            IndexWalkLimits {
                max_walk_commits: 16,
                max_total_walk_visits: 100,
            },
        )
        .await
        .unwrap();
        assert!(
            !ok.client_resolution_required && !ok.op_head.is_empty(),
            "expected reconciliation to succeed within ample per-walk budget, got {ok:?}"
        );
        let merged_op = store
            .get_operation(repo_id, &ok.op_head)
            .await
            .unwrap()
            .unwrap();
        let merged_view = store
            .get_view(repo_id, &merged_op.view_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            HashSet::<Vec<u8>>::from_iter(merged_view.head_ids),
            HashSet::from([c_a, c_b_tip])
        );
    }

    // Tight aggregate visit cap fails closed even when every single walk is under
    // `max_walk_commits`, and raising `max_total_walk_visits` allows clean reconciliation.
    {
        let inner = Arc::new(MemoryMetadataStore::new());
        let store = Arc::new(CompositeStore::with_config(
            inner.clone(),
            Arc::new(MemoryBlobStore::new()),
            CompositeStoreConfig {
                inline_blob_threshold: 0,
                validate_dependencies: false,
            },
        ));
        let repo_id = "repo-aggregate-visit-budget";
        let project_id = "proj-avb";
        store
            .register_repo_in_project(repo_id, Some(project_id), None)
            .await
            .unwrap();

        let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
        let root_commit = cc_common::ROOT_COMMIT_ID_BYTES.to_vec();
        let c_a0 = vec![0xf1; 20];
        let c_b0 = vec![0xf2; 20];
        for (cid, tag) in [(&c_a0, 0xf1u8), (&c_b0, 0xf2u8)] {
            inner
                .put_commit(
                    project_id,
                    cid,
                    &make_test_proto_commit(cid, tag, root_commit.clone()),
                    1,
                )
                .await
                .unwrap();
        }
        let base_op = put_view_and_op(
            &store,
            repo_id,
            vec![c_a0.clone(), c_b0.clone()],
            vec![root_op.clone()],
            "base",
            vec![],
        )
        .await;

        let mut prev_a = c_a0.clone();
        let mut prev_a_op = base_op.clone();
        for step in 1..=3u8 {
            let mut mid = vec![0xf3; 20];
            mid[18] = step;
            mid[19] = 1;
            let mut tip = vec![0xf3; 20];
            tip[18] = step;
            tip[19] = 2;
            inner
                .put_commit(
                    project_id,
                    &mid,
                    &make_test_proto_commit(&mid, 0xf3, prev_a),
                    u64::from(step) * 2,
                )
                .await
                .unwrap();
            inner
                .put_commit(
                    project_id,
                    &tip,
                    &make_test_proto_commit(&tip, 0xf3, mid.clone()),
                    u64::from(step) * 2 + 1,
                )
                .await
                .unwrap();
            prev_a_op = put_view_and_op(
                &store,
                repo_id,
                vec![tip.clone(), c_b0.clone()],
                vec![prev_a_op],
                &format!("branch A step {step}"),
                vec![
                    cc_common::op_store::CommitPredecessors {
                        commit_id: mid,
                        predecessor_ids: Vec::new(),
                    },
                    cc_common::op_store::CommitPredecessors {
                        commit_id: tip.clone(),
                        predecessor_ids: Vec::new(),
                    },
                ],
            )
            .await;
            prev_a = tip;
        }
        let op_a_tip = prev_a_op;

        let c_b1 = vec![0xf4; 20];
        inner
            .put_commit(
                project_id,
                &c_b1,
                &make_test_proto_commit(&c_b1, 0xf4, c_b0),
                2,
            )
            .await
            .unwrap();
        let op_b = put_view_and_op(
            &store,
            repo_id,
            vec![c_a0, c_b1.clone()],
            vec![base_op],
            "branch B advance",
            new_commit_predecessors(c_b1),
        )
        .await;

        store
            .update_op_heads(repo_id, &[root_op], &op_a_tip)
            .await
            .unwrap();
        store.update_op_heads(repo_id, &[], &op_b).await.unwrap();

        let deferred = reconcile_repo_op_heads_with_limits(
            store.clone(),
            repo_id,
            IndexWalkLimits {
                max_walk_commits: 32,
                max_total_walk_visits: 6,
            },
        )
        .await
        .unwrap();
        assert!(
            deferred.client_resolution_required,
            "exceeding aggregate visit cap must defer to client"
        );
        assert!(deferred.op_head.is_empty());
        assert_eq!(
            HashSet::<Vec<u8>>::from_iter(store.get_op_heads(repo_id).await.unwrap()),
            HashSet::from([op_a_tip, op_b])
        );

        let ok = reconcile_repo_op_heads_with_limits(
            store,
            repo_id,
            IndexWalkLimits {
                max_walk_commits: 32,
                max_total_walk_visits: 200,
            },
        )
        .await
        .unwrap();
        assert!(
            !ok.client_resolution_required && !ok.op_head.is_empty(),
            "expected reconciliation to succeed within ample aggregate budget, got {ok:?}"
        );
    }
}
