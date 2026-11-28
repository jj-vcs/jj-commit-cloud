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

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cc_common::backend::Commit as ProtoCommit;
use cc_common::op_store::{Operation as ProtoOperation, View as ProtoView};

use super::super::{
    ReconcileKillSwitch, ReconcileRepoMode, bypass_response,
    reconcile_repo_op_heads_proto_with_policy,
    reconcile_repo_op_heads_proto_with_policy_and_metrics, repo_reconcile_mutex,
};
use super::{make_test_proto_commit, new_commit_predecessors, put_view_and_op};
use crate::store::{
    CompositeStore, MemoryBlobStore, MemoryMetadataStore, MetadataStore, StoreError, StoreResult,
};

async fn seed_divergent_repo(
    store: &CompositeStore,
    repo_id: &str,
    c1_byte: u8,
    c2_byte: u8,
) -> (Vec<u8>, Vec<u8>) {
    store.register_repo(repo_id, None).await.unwrap();
    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let c1 = vec![c1_byte; 20];
    let c2 = vec![c2_byte; 20];
    for cid in [&c1, &c2] {
        store
            .put_commit(
                repo_id,
                cid,
                &make_test_proto_commit(cid, cid[0], cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
            )
            .await
            .unwrap();
    }
    let op_a = put_view_and_op(
        store,
        repo_id,
        vec![c1.clone()],
        vec![root_op.clone()],
        "branch a",
        new_commit_predecessors(c1),
    )
    .await;
    let op_b = put_view_and_op(
        store,
        repo_id,
        vec![c2.clone()],
        vec![root_op.clone()],
        "branch b",
        new_commit_predecessors(c2),
    )
    .await;
    store
        .update_op_heads(repo_id, std::slice::from_ref(&root_op), &op_a)
        .await
        .unwrap();
    store.update_op_heads(repo_id, &[], &op_b).await.unwrap();
    (op_a, op_b)
}

type CasHook = Box<dyn FnMut(usize) -> Option<StoreError> + Send>;

struct KillSwitchSpyMetadataStore {
    inner: Arc<MemoryMetadataStore>,
    total_calls: AtomicUsize,
    op_reads: AtomicUsize,
    write_calls: AtomicUsize,
    cas_attempts: AtomicUsize,
    cas_hook: Mutex<Option<CasHook>>,
}

impl KillSwitchSpyMetadataStore {
    fn new(inner: Arc<MemoryMetadataStore>) -> Self {
        Self {
            inner,
            total_calls: AtomicUsize::new(0),
            op_reads: AtomicUsize::new(0),
            write_calls: AtomicUsize::new(0),
            cas_attempts: AtomicUsize::new(0),
            cas_hook: Mutex::new(None),
        }
    }

    fn set_cas_hook<F>(&self, hook: F)
    where
        F: FnMut(usize) -> Option<StoreError> + Send + 'static,
    {
        *self.cas_hook.lock().unwrap_or_else(|e| e.into_inner()) = Some(Box::new(hook));
    }

    fn reset_counts(&self) {
        self.total_calls.store(0, Ordering::SeqCst);
        self.op_reads.store(0, Ordering::SeqCst);
        self.write_calls.store(0, Ordering::SeqCst);
        self.cas_attempts.store(0, Ordering::SeqCst);
    }

    fn record_call(&self) {
        self.total_calls.fetch_add(1, Ordering::SeqCst);
    }

    fn record_write(&self) {
        self.total_calls.fetch_add(1, Ordering::SeqCst);
        self.write_calls.fetch_add(1, Ordering::SeqCst);
    }

    fn total_calls(&self) -> usize {
        self.total_calls.load(Ordering::SeqCst)
    }

    fn op_reads(&self) -> usize {
        self.op_reads.load(Ordering::SeqCst)
    }

    fn write_calls(&self) -> usize {
        self.write_calls.load(Ordering::SeqCst)
    }

    fn cas_attempts(&self) -> usize {
        self.cas_attempts.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl MetadataStore for KillSwitchSpyMetadataStore {
    fn backend_name(&self) -> &'static str {
        "kill-switch-spy-memory"
    }
    async fn register_project(&self, project_id: &str, name: Option<&str>) -> StoreResult<()> {
        self.record_write();
        self.inner.register_project(project_id, name).await
    }
    async fn is_project_registered(&self, project_id: &str) -> StoreResult<bool> {
        self.record_call();
        self.inner.is_project_registered(project_id).await
    }
    async fn get_repo_project_id(&self, repo_id: &str) -> StoreResult<Option<String>> {
        self.record_call();
        self.inner.get_repo_project_id(repo_id).await
    }
    async fn register_repo_in_project(
        &self,
        repo_id: &str,
        project_id: &str,
        name: Option<&str>,
    ) -> StoreResult<String> {
        self.record_write();
        self.inner
            .register_repo_in_project(repo_id, project_id, name)
            .await
    }
    async fn delete_repository(
        &self,
        repo_id: &str,
        expected_project_id: Option<&str>,
    ) -> StoreResult<bool> {
        self.record_write();
        self.inner
            .delete_repository(repo_id, expected_project_id)
            .await
    }
    async fn get_commit(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> StoreResult<Option<ProtoCommit>> {
        self.record_call();
        self.inner.get_commit(project_id, commit_id).await
    }
    async fn get_commit_generation(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> StoreResult<Option<u64>> {
        self.record_call();
        self.inner
            .get_commit_generation(project_id, commit_id)
            .await
    }
    async fn get_commit_with_generation(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> StoreResult<Option<(ProtoCommit, u64)>> {
        self.record_call();
        self.inner
            .get_commit_with_generation(project_id, commit_id)
            .await
    }
    async fn get_commit_generations(
        &self,
        project_id: &str,
        commit_ids: &[Vec<u8>],
    ) -> StoreResult<HashMap<Vec<u8>, u64>> {
        self.record_call();
        self.inner
            .get_commit_generations(project_id, commit_ids)
            .await
    }
    async fn put_commit(
        &self,
        project_id: &str,
        commit_id: &[u8],
        commit: &ProtoCommit,
        generation: u64,
    ) -> StoreResult<()> {
        self.record_write();
        self.inner
            .put_commit(project_id, commit_id, commit, generation)
            .await
    }
    async fn list_commit_ids(
        &self,
        project_id: &str,
        after_commit_id: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<Vec<crate::store::CommitId>> {
        self.record_call();
        self.inner
            .list_commit_ids(project_id, after_commit_id, limit)
            .await
    }
    async fn current_commit_watermark(&self, project_id: &str) -> StoreResult<Vec<u8>> {
        self.record_call();
        self.inner.current_commit_watermark(project_id).await
    }
    async fn list_commit_ids_since(
        &self,
        project_id: &str,
        since_token: &[u8],
        limit: usize,
    ) -> StoreResult<(Vec<crate::store::CommitId>, Vec<u8>, bool)> {
        self.record_call();
        self.inner
            .list_commit_ids_since(project_id, since_token, limit)
            .await
    }
    async fn resolve_commit_id_prefix(
        &self,
        project_id: &str,
        min_id: &[u8],
        max_id: &[u8],
    ) -> StoreResult<Vec<crate::store::CommitId>> {
        self.record_call();
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
    ) -> StoreResult<Vec<(Vec<u8>, Vec<crate::store::CommitId>)>> {
        self.record_call();
        self.inner
            .resolve_change_id_prefix(project_id, min_id, max_id, limit)
            .await
    }
    async fn get_tree(
        &self,
        project_id: &str,
        tree_id: &[u8],
    ) -> StoreResult<Option<Vec<cc_common::backend::TreeEntry>>> {
        self.record_call();
        self.inner.get_tree(project_id, tree_id).await
    }
    async fn put_tree(
        &self,
        project_id: &str,
        tree_id: &[u8],
        entries: &[cc_common::backend::TreeEntry],
    ) -> StoreResult<()> {
        self.record_write();
        self.inner.put_tree(project_id, tree_id, entries).await
    }
    async fn find_existing_trees(
        &self,
        project_id: &str,
        tree_ids: &[Vec<u8>],
    ) -> StoreResult<HashSet<Vec<u8>>> {
        self.record_call();
        self.inner.find_existing_trees(project_id, tree_ids).await
    }
    async fn get_symlink(
        &self,
        project_id: &str,
        symlink_id: &[u8],
    ) -> StoreResult<Option<String>> {
        self.record_call();
        self.inner.get_symlink(project_id, symlink_id).await
    }
    async fn put_symlink(
        &self,
        project_id: &str,
        symlink_id: &[u8],
        target: &str,
    ) -> StoreResult<()> {
        self.record_write();
        self.inner.put_symlink(project_id, symlink_id, target).await
    }
    async fn find_existing_symlinks(
        &self,
        project_id: &str,
        symlink_ids: &[Vec<u8>],
    ) -> StoreResult<HashSet<Vec<u8>>> {
        self.record_call();
        self.inner
            .find_existing_symlinks(project_id, symlink_ids)
            .await
    }
    async fn get_blob_record(
        &self,
        project_id: &str,
        blob_id: &[u8],
    ) -> StoreResult<Option<crate::store::BlobRecord>> {
        self.record_call();
        self.inner.get_blob_record(project_id, blob_id).await
    }
    async fn get_blob_sizes_batch(
        &self,
        project_id: &str,
        blob_ids: &[Vec<u8>],
    ) -> StoreResult<HashMap<Vec<u8>, u64>> {
        self.record_call();
        self.inner.get_blob_sizes_batch(project_id, blob_ids).await
    }
    async fn get_global_blob_record(
        &self,
        blob_id: &[u8],
    ) -> StoreResult<Option<crate::store::BlobRecord>> {
        self.record_call();
        self.inner.get_global_blob_record(blob_id).await
    }
    async fn put_blob_record(
        &self,
        project_id: &str,
        record: &crate::store::BlobRecord,
    ) -> StoreResult<()> {
        self.record_write();
        self.inner.put_blob_record(project_id, record).await
    }
    async fn add_blob_replica_location(&self, blob_id: &[u8], location: &str) -> StoreResult<()> {
        self.record_write();
        self.inner
            .add_blob_replica_location(blob_id, location)
            .await
    }
    async fn get_operation(
        &self,
        repo_id: &str,
        op_id: &[u8],
    ) -> StoreResult<Option<ProtoOperation>> {
        self.record_call();
        self.op_reads.fetch_add(1, Ordering::SeqCst);
        self.inner.get_operation(repo_id, op_id).await
    }
    async fn put_operation(
        &self,
        repo_id: &str,
        op_id: &[u8],
        op: &ProtoOperation,
    ) -> StoreResult<()> {
        self.record_write();
        self.inner.put_operation(repo_id, op_id, op).await
    }
    async fn put_operation_for_project(
        &self,
        repo_id: &str,
        expected_project_id: &str,
        op_id: &[u8],
        op: &ProtoOperation,
        require_dependencies_exist: bool,
    ) -> StoreResult<()> {
        self.record_write();
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
    ) -> StoreResult<Vec<crate::store::OpId>> {
        self.record_call();
        self.inner
            .resolve_operation_id_prefix(repo_id, min_id, max_id)
            .await
    }
    async fn get_view(&self, repo_id: &str, view_id: &[u8]) -> StoreResult<Option<ProtoView>> {
        self.record_call();
        self.inner.get_view(repo_id, view_id).await
    }
    async fn put_view(&self, repo_id: &str, view_id: &[u8], view: &ProtoView) -> StoreResult<()> {
        self.record_write();
        self.inner.put_view(repo_id, view_id, view).await
    }
    async fn put_view_for_project(
        &self,
        repo_id: &str,
        expected_project_id: &str,
        view_id: &[u8],
        view: &ProtoView,
    ) -> StoreResult<()> {
        self.record_write();
        self.inner
            .put_view_for_project(repo_id, expected_project_id, view_id, view)
            .await
    }
    async fn get_op_heads(&self, repo_id: &str) -> StoreResult<Option<Vec<crate::store::OpId>>> {
        self.record_call();
        self.inner.get_op_heads(repo_id).await
    }
    async fn update_op_heads(
        &self,
        repo_id: &str,
        old_ids: &[Vec<u8>],
        new_id: &[u8],
    ) -> StoreResult<Vec<crate::store::OpId>> {
        self.record_write();
        self.inner.update_op_heads(repo_id, old_ids, new_id).await
    }
    async fn update_op_heads_for_project(
        &self,
        repo_id: &str,
        expected_project_id: &str,
        old_head_ids: &[Vec<u8>],
        new_head_id: &[u8],
        require_op_exists: bool,
    ) -> StoreResult<Vec<crate::store::OpId>> {
        self.record_write();
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
    ) -> StoreResult<Vec<crate::store::OpId>> {
        self.record_write();
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
    ) -> StoreResult<Vec<crate::store::OpId>> {
        self.record_write();
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
    ) -> StoreResult<()> {
        self.record_write();
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
    ) -> StoreResult<()> {
        self.record_write();
        let attempt = self.cas_attempts.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some(err) = self
            .cas_hook
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_mut()
            .and_then(|hook| hook(attempt))
        {
            return Err(err);
        }
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
    ) -> StoreResult<Option<cc_common::workspace::WorkspaceState>> {
        self.record_call();
        self.inner.get_workspace(repo_id, workspace_name).await
    }
    async fn put_workspace_cas(
        &self,
        workspace: &cc_common::workspace::WorkspaceState,
        expected_operation_id: Option<&[u8]>,
    ) -> StoreResult<()> {
        self.record_write();
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
    ) -> StoreResult<()> {
        self.record_write();
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
    ) -> StoreResult<Vec<cc_common::workspace::WorkspaceState>> {
        self.record_call();
        self.inner.list_workspaces(repo_id).await
    }
    async fn delete_workspace(&self, repo_id: &str, workspace_name: &str) -> StoreResult<bool> {
        self.record_write();
        self.inner.delete_workspace(repo_id, workspace_name).await
    }
}

#[test]
fn test_reconcile_kill_switch_try_from_repo_lists_validation_and_normalization() {
    let empty =
        ReconcileKillSwitch::try_from_repo_lists(Vec::<&str>::new(), Vec::<&str>::new()).unwrap();
    assert!(empty.is_empty());
    assert_eq!(empty.counts(), (0, 0));
    assert_eq!(empty.mode_for("any-repo"), ReconcileRepoMode::Normal);

    let ks = ReconcileKillSwitch::try_from_repo_lists(
        [" repo-b , , repo-a ", "repo-a", "   "],
        ["repo-q2, repo-q1", "repo-q1", ""],
    )
    .unwrap();
    assert!(!ks.is_empty());
    assert_eq!(ks.counts(), (2, 2));
    assert_eq!(
        ks.bypass_repos(),
        vec!["repo-a".to_string(), "repo-b".to_string()]
    );
    assert_eq!(
        ks.quarantine_repos(),
        vec!["repo-q1".to_string(), "repo-q2".to_string()]
    );
    assert_eq!(ks.mode_for("repo-a"), ReconcileRepoMode::Bypass);
    assert_eq!(ks.mode_for("repo-b"), ReconcileRepoMode::Bypass);
    assert_eq!(ks.mode_for("repo-q1"), ReconcileRepoMode::Quarantine);
    assert_eq!(ks.mode_for("repo-q2"), ReconcileRepoMode::Quarantine);
    assert_eq!(ks.mode_for("repo-normal"), ReconcileRepoMode::Normal);

    // Conflicting bypass + quarantine entry must fail closed.
    let conflict = ReconcileKillSwitch::try_from_repo_lists(["shared-repo"], [" shared-repo "])
        .expect_err("overlapping bypass and quarantine repos must fail");
    assert!(
        conflict.contains("cannot be configured in both") && conflict.contains("shared-repo"),
        "unexpected conflict error: {conflict}"
    );

    // ASCII control characters must be rejected before trimming.
    for bad in [
        "\n", "\r", "\t", "\0", "repo-a\n", "repo-a\r", "repo-a\t", "repo-a\0", " , \t ",
    ] {
        let err_b = ReconcileKillSwitch::try_from_repo_lists([bad], Vec::<&str>::new())
            .expect_err("control chars in bypass list must be rejected");
        assert!(
            err_b.contains("control characters"),
            "unexpected bypass error for {bad:?}: {err_b}"
        );
        let err_q = ReconcileKillSwitch::try_from_repo_lists(Vec::<&str>::new(), [bad])
            .expect_err("control chars in quarantine list must be rejected");
        assert!(
            err_q.contains("control characters"),
            "unexpected quarantine error for {bad:?}: {err_q}"
        );
    }

    // Invalid repo identifiers must be rejected via validate_identifier.
    for bad in [
        "../escape",
        "repo/slash",
        "repo with space",
        ".",
        "..",
        "repo@1",
    ] {
        assert!(
            ReconcileKillSwitch::try_from_repo_lists([bad], Vec::<&str>::new()).is_err(),
            "invalid bypass repo_id {bad:?} must be rejected"
        );
        assert!(
            ReconcileKillSwitch::try_from_repo_lists(Vec::<&str>::new(), [bad]).is_err(),
            "invalid quarantine repo_id {bad:?} must be rejected"
        );
    }

    // set_mode(Normal) removes the entry so the map stays compact, and invalid repo_ids are
    // rejected on both setting and clearing without mutating existing entries.
    let dyn_ks = ReconcileKillSwitch::new();
    dyn_ks
        .set_mode("repo-x", ReconcileRepoMode::Bypass)
        .unwrap();
    assert_eq!(dyn_ks.counts(), (1, 0));

    for bad in ["", " repo-a ", "repo-a\n", "repo-a\t", "../bad", ".", ".."] {
        for mode in [
            ReconcileRepoMode::Bypass,
            ReconcileRepoMode::Quarantine,
            ReconcileRepoMode::Normal,
        ] {
            let err = dyn_ks
                .set_mode(bad, mode)
                .expect_err("invalid repo_id in set_mode must be rejected");
            assert!(
                err.contains("invalid repo_id") && err.contains(&format!("{bad:?}")),
                "unexpected set_mode error for {bad:?} ({mode:?}): {err}"
            );
            assert!(
                !err.bytes().any(|b| b.is_ascii_control()),
                "set_mode error must escape control characters for {bad:?}: {err:?}"
            );
        }
    }
    assert_eq!(dyn_ks.counts(), (1, 0));
    assert_eq!(dyn_ks.mode_for("repo-x"), ReconcileRepoMode::Bypass);

    dyn_ks
        .set_mode("repo-x", ReconcileRepoMode::Quarantine)
        .unwrap();
    assert_eq!(dyn_ks.counts(), (0, 1));
    dyn_ks
        .set_mode("repo-x", ReconcileRepoMode::Normal)
        .unwrap();
    assert!(dyn_ks.is_empty());
    assert_eq!(dyn_ks.counts(), (0, 0));
    assert_eq!(dyn_ks.mode_for("repo-x"), ReconcileRepoMode::Normal);
}

#[test]
fn test_bypass_response_rejects_corrupt_head_length() {
    let err = bypass_response("corrupt-repo", vec![vec![0xaa; 5], vec![0xbb; 5]])
        .expect_err("malformed op_head length must return Corruption");
    assert_eq!(err.code(), tonic::Code::DataLoss);
}

#[tokio::test]
async fn test_reconcile_kill_switch_bypass_quarantine_sibling_and_dynamic_toggle() {
    let store = Arc::new(CompositeStore::in_memory());
    let (bypass_a, bypass_b) = seed_divergent_repo(&store, "repo-bypass", 0x11, 0x12).await;
    let (quar_a, quar_b) = seed_divergent_repo(&store, "repo-quarantine", 0x21, 0x22).await;
    let (_norm_a, _norm_b) = seed_divergent_repo(&store, "repo-normal", 0x31, 0x32).await;
    let (dyn_a, dyn_b) = seed_divergent_repo(&store, "repo-dynamic", 0x41, 0x42).await;
    store.register_repo("repo-converged", None).await.unwrap();

    let ks = ReconcileKillSwitch::try_from_repo_lists(
        ["repo-bypass", "repo-converged"],
        ["repo-quarantine"],
    )
    .unwrap();
    let metrics = crate::metrics::ServerMetrics::new();

    // Bypass on divergent repo returns client_resolution_required = true without DB mutation.
    let bypass_resp = reconcile_repo_op_heads_proto_with_policy_and_metrics(
        store.clone(),
        "repo-bypass",
        Some(&ks),
        Some(&metrics),
    )
    .await
    .unwrap();
    assert!(bypass_resp.client_resolution_required);
    assert!(bypass_resp.op_head.is_empty());
    let expected_bypass: HashSet<Vec<u8>> = HashSet::from([bypass_a, bypass_b]);
    assert_eq!(
        HashSet::from_iter(bypass_resp.current_op_head_ids.iter().cloned()),
        expected_bypass
    );
    assert_eq!(
        HashSet::from_iter(store.get_op_heads("repo-bypass").await.unwrap()),
        expected_bypass
    );
    let snap1 = metrics.snapshot();
    assert_eq!(snap1.reconcile_client_fallback_total, 1);
    assert_eq!(snap1.reconcile_op_heads_total, 1);

    // Bypass on converged (length-1) repo still takes the zero-merge single-head fast path.
    let conv_resp = reconcile_repo_op_heads_proto_with_policy_and_metrics(
        store.clone(),
        "repo-converged",
        Some(&ks),
        Some(&metrics),
    )
    .await
    .unwrap();
    assert!(!conv_resp.client_resolution_required);
    assert_eq!(conv_resp.op_head, cc_common::ROOT_OPERATION_ID_BYTES);
    assert_eq!(
        conv_resp.current_op_head_ids,
        vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()]
    );
    let snap2 = metrics.snapshot();
    assert_eq!(snap2.reconcile_client_fallback_total, 1);
    assert_eq!(snap2.reconcile_op_heads_total, 2);

    // Quarantine on divergent repo returns FAILED_PRECONDITION without mutating DB or success counters.
    let quar_err = reconcile_repo_op_heads_proto_with_policy_and_metrics(
        store.clone(),
        "repo-quarantine",
        Some(&ks),
        Some(&metrics),
    )
    .await
    .expect_err("quarantined divergent repo must return FAILED_PRECONDITION");
    assert_eq!(quar_err.code(), tonic::Code::FailedPrecondition);
    assert!(quar_err.message().contains("quarantined"));
    assert_eq!(
        HashSet::from_iter(store.get_op_heads("repo-quarantine").await.unwrap()),
        HashSet::from([quar_a, quar_b])
    );
    let snap3 = metrics.snapshot();
    assert_eq!(snap3.reconcile_client_fallback_total, 1);
    assert_eq!(snap3.reconcile_op_heads_total, 2);

    // Quarantine also fails closed on converged and unregistered repos to shed DB load.
    ks.set_mode("repo-converged", ReconcileRepoMode::Quarantine)
        .unwrap();
    let conv_quar_err =
        reconcile_repo_op_heads_proto_with_policy(store.clone(), "repo-converged", Some(&ks))
            .await
            .expect_err("quarantined converged repo must fail closed");
    assert_eq!(conv_quar_err.code(), tonic::Code::FailedPrecondition);

    ks.set_mode("unregistered-quarantined", ReconcileRepoMode::Quarantine)
        .unwrap();
    let unreg_quar_err = reconcile_repo_op_heads_proto_with_policy(
        store.clone(),
        "unregistered-quarantined",
        Some(&ks),
    )
    .await
    .expect_err("quarantined unregistered repo must fail closed before DB lookup");
    assert_eq!(unreg_quar_err.code(), tonic::Code::FailedPrecondition);

    // Unaffected sibling repo reconciles normally under the same kill switch.
    let norm_resp = reconcile_repo_op_heads_proto_with_policy_and_metrics(
        store.clone(),
        "repo-normal",
        Some(&ks),
        Some(&metrics),
    )
    .await
    .unwrap();
    assert!(!norm_resp.client_resolution_required);
    assert_eq!(norm_resp.op_head.len(), cc_common::OPERATION_ID_LENGTH);
    assert_eq!(
        store.get_op_heads("repo-normal").await.unwrap(),
        vec![norm_resp.op_head]
    );
    let snap4 = metrics.snapshot();
    assert_eq!(snap4.reconcile_client_fallback_total, 1);
    assert_eq!(snap4.reconcile_op_heads_total, 3);

    // Dynamic runtime mode toggle: Normal -> Bypass -> Quarantine -> Normal.
    let expected_dyn: HashSet<Vec<u8>> = HashSet::from([dyn_a, dyn_b]);
    assert_eq!(ks.mode_for("repo-dynamic"), ReconcileRepoMode::Normal);

    ks.set_mode("repo-dynamic", ReconcileRepoMode::Bypass)
        .unwrap();
    let dyn_bypass =
        reconcile_repo_op_heads_proto_with_policy(store.clone(), "repo-dynamic", Some(&ks))
            .await
            .unwrap();
    assert!(dyn_bypass.client_resolution_required);
    assert!(dyn_bypass.op_head.is_empty());
    assert_eq!(
        HashSet::from_iter(dyn_bypass.current_op_head_ids),
        expected_dyn
    );
    assert_eq!(
        HashSet::from_iter(store.get_op_heads("repo-dynamic").await.unwrap()),
        expected_dyn
    );

    ks.set_mode("repo-dynamic", ReconcileRepoMode::Quarantine)
        .unwrap();
    let dyn_quar =
        reconcile_repo_op_heads_proto_with_policy(store.clone(), "repo-dynamic", Some(&ks))
            .await
            .expect_err("toggled Quarantine mode must reject reconcile");
    assert_eq!(dyn_quar.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        HashSet::from_iter(store.get_op_heads("repo-dynamic").await.unwrap()),
        expected_dyn
    );

    ks.set_mode("repo-dynamic", ReconcileRepoMode::Normal)
        .unwrap();
    let dyn_merged =
        reconcile_repo_op_heads_proto_with_policy(store.clone(), "repo-dynamic", Some(&ks))
            .await
            .unwrap();
    assert!(!dyn_merged.client_resolution_required);
    assert_eq!(dyn_merged.op_head.len(), cc_common::OPERATION_ID_LENGTH);
    assert_eq!(
        store.get_op_heads("repo-dynamic").await.unwrap(),
        vec![dyn_merged.op_head]
    );
}

#[tokio::test]
async fn test_reconcile_kill_switch_quarantine_sheds_load_with_zero_db_reads_on_registered_repo() {
    let inner = Arc::new(MemoryMetadataStore::new());
    let spy = Arc::new(KillSwitchSpyMetadataStore::new(inner));
    let store = Arc::new(CompositeStore::new(
        spy.clone(),
        Arc::new(MemoryBlobStore::new()),
    ));
    let (op_a, op_b) = seed_divergent_repo(&store, "repo-zero-db-quarantine", 0x51, 0x52).await;

    let ks =
        ReconcileKillSwitch::try_from_repo_lists(Vec::<&str>::new(), ["repo-zero-db-quarantine"])
            .unwrap();
    spy.reset_counts();

    let err = reconcile_repo_op_heads_proto_with_policy(
        store.clone(),
        "repo-zero-db-quarantine",
        Some(&ks),
    )
    .await
    .expect_err("quarantined registered divergent repo must fail closed");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        spy.total_calls(),
        0,
        "quarantined repo must shed load before any MetadataStore read or write"
    );

    // Heads remain divergent and untouched, and lifting Quarantine reconciles normally.
    assert_eq!(
        HashSet::from_iter(store.get_op_heads("repo-zero-db-quarantine").await.unwrap()),
        HashSet::from([op_a, op_b])
    );
    ks.set_mode("repo-zero-db-quarantine", ReconcileRepoMode::Normal)
        .unwrap();
    spy.reset_counts();
    let merged = reconcile_repo_op_heads_proto_with_policy(
        store.clone(),
        "repo-zero-db-quarantine",
        Some(&ks),
    )
    .await
    .unwrap();
    assert!(!merged.client_resolution_required);
    assert!(spy.total_calls() > 0);
    assert_eq!(
        store.get_op_heads("repo-zero-db-quarantine").await.unwrap(),
        vec![merged.op_head]
    );
}

#[tokio::test]
async fn test_reconcile_kill_switch_post_lock_dynamic_toggle_quarantine_and_bypass() {
    async fn wait_for_queued_lock_holder(lock: &Arc<tokio::sync::Mutex<()>>) {
        // `REPO_RECONCILE_LOCKS` holds only `Weak` pointers, so `strong_count >= 2` proves the
        // spawned task passed the pre-lock checks in `Normal` mode and reached `repo_lock.lock()`.
        for _ in 0..1000 {
            if Arc::strong_count(lock) >= 2 {
                tokio::task::yield_now().await;
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("spawned reconcile task did not reach per-repo lock in time");
    }

    // Normal caller blocks on per-repo mutex -> flipped to Quarantine -> unblocked.
    {
        let repo_id = "repo-post-lock-quarantine";
        let spy = Arc::new(KillSwitchSpyMetadataStore::new(Arc::new(
            MemoryMetadataStore::new(),
        )));
        let store = Arc::new(CompositeStore::new(
            spy.clone(),
            Arc::new(MemoryBlobStore::new()),
        ));
        let (op_a, op_b) = seed_divergent_repo(&store, repo_id, 0x61, 0x62).await;
        let ks = ReconcileKillSwitch::new();

        let lock = repo_reconcile_mutex(repo_id);
        let guard = lock.lock().await;

        let store_task = store.clone();
        let ks_task = ks.clone();
        let handle = tokio::spawn(async move {
            reconcile_repo_op_heads_proto_with_policy(store_task, repo_id, Some(&ks_task)).await
        });

        wait_for_queued_lock_holder(&lock).await;
        assert!(!handle.is_finished());

        ks.set_mode(repo_id, ReconcileRepoMode::Quarantine).unwrap();
        spy.reset_counts();
        drop(guard);

        let err = handle
            .await
            .unwrap()
            .expect_err("post-lock Quarantine re-check must fail closed");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            spy.total_calls(),
            0,
            "post-lock Quarantine check must reject before post-lock DB reads or writes"
        );
        assert_eq!(
            HashSet::from_iter(store.get_op_heads(repo_id).await.unwrap()),
            HashSet::from([op_a, op_b])
        );
    }

    // Normal caller blocks on per-repo mutex -> flipped to Bypass -> unblocked.
    {
        let repo_id = "repo-post-lock-bypass";
        let spy = Arc::new(KillSwitchSpyMetadataStore::new(Arc::new(
            MemoryMetadataStore::new(),
        )));
        let store = Arc::new(CompositeStore::new(
            spy.clone(),
            Arc::new(MemoryBlobStore::new()),
        ));
        let (op_a, op_b) = seed_divergent_repo(&store, repo_id, 0x71, 0x72).await;
        let ks = ReconcileKillSwitch::new();

        let lock = repo_reconcile_mutex(repo_id);
        let guard = lock.lock().await;

        let store_task = store.clone();
        let ks_task = ks.clone();
        let handle = tokio::spawn(async move {
            reconcile_repo_op_heads_proto_with_policy(store_task, repo_id, Some(&ks_task)).await
        });

        wait_for_queued_lock_holder(&lock).await;
        assert!(!handle.is_finished());

        ks.set_mode(repo_id, ReconcileRepoMode::Bypass).unwrap();
        spy.reset_counts();
        drop(guard);

        let resp = handle.await.unwrap().unwrap();
        assert!(resp.client_resolution_required);
        assert!(resp.op_head.is_empty());
        let expected = HashSet::from([op_a, op_b]);
        assert_eq!(HashSet::from_iter(resp.current_op_head_ids), expected);
        assert_eq!(
            spy.write_calls(),
            0,
            "post-lock Bypass must not mutate MetadataStore"
        );
        assert_eq!(
            spy.op_reads(),
            0,
            "post-lock Bypass must not enter jj_lib merge or read operations"
        );
        assert_eq!(
            HashSet::from_iter(store.get_op_heads(repo_id).await.unwrap()),
            expected
        );
    }
}

#[tokio::test]
async fn test_reconcile_kill_switch_mid_flight_transient_cas_conflict_toggle() {
    // Mid-flight Quarantine on first transient CasConflict skips retry/backoff and fails
    // closed immediately with FAILED_PRECONDITION (never ABORTED).
    {
        let repo_id = "repo-midflight-quarantine-attempt1";
        let spy = Arc::new(KillSwitchSpyMetadataStore::new(Arc::new(
            MemoryMetadataStore::new(),
        )));
        let store = Arc::new(CompositeStore::new(
            spy.clone(),
            Arc::new(MemoryBlobStore::new()),
        ));
        let (op_a, op_b) = seed_divergent_repo(&store, repo_id, 0x81, 0x82).await;
        let ks = ReconcileKillSwitch::new();
        let ks_hook = ks.clone();
        spy.set_cas_hook(move |_attempt| {
            ks_hook
                .set_mode(repo_id, ReconcileRepoMode::Quarantine)
                .unwrap();
            Some(StoreError::CasConflict("simulated conflict".to_string()))
        });
        spy.reset_counts();

        let err = reconcile_repo_op_heads_proto_with_policy(store.clone(), repo_id, Some(&ks))
            .await
            .expect_err("mid-flight Quarantine during transient failure must fail closed");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            spy.cas_attempts(),
            1,
            "Quarantine toggle must short-circuit retry loop after attempt 1"
        );
        assert_eq!(
            HashSet::from_iter(store.get_op_heads(repo_id).await.unwrap()),
            HashSet::from([op_a, op_b])
        );
    }

    // Mid-flight Quarantine on final retry attempt (attempt 6 == MAX_RETRIES) returns
    // FAILED_PRECONDITION rather than fallback-triggering ABORTED.
    {
        let repo_id = "repo-midflight-quarantine-final";
        let spy = Arc::new(KillSwitchSpyMetadataStore::new(Arc::new(
            MemoryMetadataStore::new(),
        )));
        let store = Arc::new(CompositeStore::new(
            spy.clone(),
            Arc::new(MemoryBlobStore::new()),
        ));
        let (op_a, op_b) = seed_divergent_repo(&store, repo_id, 0x91, 0x92).await;
        let ks = ReconcileKillSwitch::new();
        let ks_hook = ks.clone();
        spy.set_cas_hook(move |attempt| {
            if attempt == 6 {
                ks_hook
                    .set_mode(repo_id, ReconcileRepoMode::Quarantine)
                    .unwrap();
            }
            Some(StoreError::CasConflict(format!(
                "simulated conflict attempt {attempt}"
            )))
        });
        spy.reset_counts();

        let err = reconcile_repo_op_heads_proto_with_policy(store.clone(), repo_id, Some(&ks))
            .await
            .expect_err("final-attempt Quarantine toggle must fail closed");
        assert_eq!(
            err.code(),
            tonic::Code::FailedPrecondition,
            "quarantined repo must never return ABORTED on transient retry exhaustion"
        );
        assert_eq!(spy.cas_attempts(), 6);
        assert_eq!(
            HashSet::from_iter(store.get_op_heads(repo_id).await.unwrap()),
            HashSet::from([op_a, op_b])
        );
    }

    // Mid-flight Bypass on transient CasConflict skips retry/backoff sleep and returns
    // fresh divergent heads with client_resolution_required = true.
    {
        let repo_id = "repo-midflight-bypass-attempt1";
        let spy = Arc::new(KillSwitchSpyMetadataStore::new(Arc::new(
            MemoryMetadataStore::new(),
        )));
        let store = Arc::new(CompositeStore::new(
            spy.clone(),
            Arc::new(MemoryBlobStore::new()),
        ));
        let (op_a, op_b) = seed_divergent_repo(&store, repo_id, 0xa1, 0xa2).await;
        let ks = ReconcileKillSwitch::new();
        let ks_hook = ks.clone();
        spy.set_cas_hook(move |_attempt| {
            ks_hook
                .set_mode(repo_id, ReconcileRepoMode::Bypass)
                .unwrap();
            Some(StoreError::CasConflict("simulated conflict".to_string()))
        });
        spy.reset_counts();

        let resp = reconcile_repo_op_heads_proto_with_policy(store.clone(), repo_id, Some(&ks))
            .await
            .unwrap();
        assert!(resp.client_resolution_required);
        assert!(resp.op_head.is_empty());
        let expected = HashSet::from([op_a, op_b]);
        assert_eq!(HashSet::from_iter(resp.current_op_head_ids), expected);
        assert_eq!(
            spy.cas_attempts(),
            1,
            "Bypass toggle must short-circuit retry loop after attempt 1 without sleeping"
        );
        assert_eq!(
            HashSet::from_iter(store.get_op_heads(repo_id).await.unwrap()),
            expected
        );
    }
}
