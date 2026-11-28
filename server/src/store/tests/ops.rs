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

use super::super::*;

#[tokio::test]
async fn test_concurrent_op_heads_preserve_siblings_even_after_race() {
    let store = CompositeStore::in_memory();
    store.register_repo("repo-1", None).await.unwrap();

    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let view_id = vec![0xaa; 16];
    store
        .put_view(
            "repo-1",
            &view_id,
            &View {
                head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                wc_commit_ids: Default::default(),
                local_bookmarks: Default::default(),
                remote_bookmarks: Default::default(),
            },
        )
        .await
        .unwrap();

    let make_op = || Operation {
        view_id: view_id.clone(),
        parents: vec![root_op.clone()],
        metadata: None,
        commit_predecessors: vec![],
        commit_predecessors_set: false,
    };
    let op_a = vec![0x10; 16];
    let op_b = vec![0x20; 16];
    store
        .put_operation("repo-1", &op_a, &make_op())
        .await
        .unwrap();
    store
        .put_operation("repo-1", &op_b, &make_op())
        .await
        .unwrap();

    // Agent A advances from [root] -> [op_a].
    let heads_after_a = store
        .update_op_heads("repo-1", std::slice::from_ref(&root_op), &op_a)
        .await
        .unwrap();
    assert_eq!(heads_after_a, vec![op_a.clone()]);

    // Agent B concurrently started from [root] and submits [op_b]; both [op_a, op_b] survive.
    let heads_after_b = store
        .update_op_heads("repo-1", std::slice::from_ref(&root_op), &op_b)
        .await
        .unwrap();
    assert_eq!(heads_after_b, vec![op_a.clone(), op_b.clone()]);

    // Reconciling [op_a, op_b] -> [op_c] collapses heads back to [op_c].
    let op_c = vec![0x30; 16];
    store
        .put_operation("repo-1", &op_c, &make_op())
        .await
        .unwrap();
    let heads_after_c = store
        .update_op_heads("repo-1", &[op_a, op_b], &op_c)
        .await
        .unwrap();
    assert_eq!(heads_after_c, vec![op_c]);
}

#[tokio::test]
async fn test_update_op_heads_retry_does_not_resurrect_superseded_and_preserves_ancestor_pruning() {
    let dir = tempfile::tempdir().unwrap();
    let stores: [Arc<dyn MetadataStore>; 2] = [
        Arc::new(MemoryMetadataStore::new()),
        Arc::new(SqliteMetadataStore::open(&dir.path().join("op_heads_retry.db")).unwrap()),
    ];

    for meta in stores {
        meta.register_repo("repo-retry", None).await.unwrap();
        let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
        let view_id = vec![0xaa; 16];
        let view = View {
            head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: HashMap::new(),
        };
        meta.put_view("repo-retry", &view_id, &view).await.unwrap();

        let make_op = |parents: Vec<Vec<u8>>| Operation {
            view_id: view_id.clone(),
            parents,
            metadata: None,
            commit_predecessors: Vec::new(),
            commit_predecessors_set: true,
        };

        let op_a = vec![0x11; 16];
        let op_b = vec![0x22; 16];
        meta.put_operation("repo-retry", &op_a, &make_op(vec![root_op.clone()]))
            .await
            .unwrap();
        meta.put_operation("repo-retry", &op_b, &make_op(vec![op_a.clone()]))
            .await
            .unwrap();

        // [root] -> [op_a] -> [op_b]
        let heads_a = meta
            .update_op_heads("repo-retry", std::slice::from_ref(&root_op), &op_a)
            .await
            .unwrap();
        assert_eq!(heads_a, vec![op_a.clone()], "{}", meta.backend_name());

        let heads_b = meta
            .update_op_heads("repo-retry", std::slice::from_ref(&op_a), &op_b)
            .await
            .unwrap();
        assert_eq!(heads_b, vec![op_b.clone()], "{}", meta.backend_name());

        // Retrying update_op_heads([root], op_a) after op_a was superseded by op_b must NOT
        // resurrect op_a as a divergent head.
        let retry_a = meta
            .update_op_heads("repo-retry", std::slice::from_ref(&root_op), &op_a)
            .await
            .unwrap();
        assert_eq!(retry_a, vec![op_b.clone()], "{}", meta.backend_name());
        assert_eq!(
            meta.get_op_heads("repo-retry").await.unwrap(),
            Some(vec![op_b.clone()]),
            "{}",
            meta.backend_name()
        );

        // Ancestor pruning: when both an ancestor (op_anc) and descendant (op_desc) are
        // concurrently present in current_heads and already published, calling
        // update_op_heads([op_anc, op_desc], op_desc) must still prune op_anc to [op_desc].
        meta.register_repo("repo-prune", None).await.unwrap();
        meta.put_view("repo-prune", &view_id, &view).await.unwrap();

        let op_anc = vec![0x33; 16];
        let op_desc = vec![0x44; 16];
        meta.put_operation("repo-prune", &op_anc, &make_op(vec![root_op.clone()]))
            .await
            .unwrap();
        meta.put_operation("repo-prune", &op_desc, &make_op(vec![op_anc.clone()]))
            .await
            .unwrap();

        meta.update_op_heads("repo-prune", std::slice::from_ref(&root_op), &op_anc)
            .await
            .unwrap();
        let divergent = meta
            .update_op_heads("repo-prune", &[], &op_desc)
            .await
            .unwrap();
        assert_eq!(
            divergent,
            vec![op_anc.clone(), op_desc.clone()],
            "{}",
            meta.backend_name()
        );

        let pruned = meta
            .update_op_heads("repo-prune", &[op_anc.clone(), op_desc.clone()], &op_desc)
            .await
            .unwrap();
        assert_eq!(pruned, vec![op_desc.clone()], "{}", meta.backend_name());
        assert_eq!(
            meta.get_op_heads("repo-prune").await.unwrap(),
            Some(vec![op_desc]),
            "{}",
            meta.backend_name()
        );
    }
}

#[test]
fn test_check_workspace_cas_precondition_normalizes_legacy_empty_operation_id_on_retry() {
    #[allow(deprecated)]
    let legacy_existing = WorkspaceState {
        repo_id: "repo-ws".to_string(),
        user: "alice".to_string(),
        workspace_name: "default".to_string(),
        commit_id: cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
        operation_id: Vec::new(),
        tree_id: cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
        sparse_patterns: vec![String::new()],
        tree_ids: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
        conflict_labels: Vec::new(),
        sparse_patterns_set: true,
    };
    let mut normalized_retry = legacy_existing.clone();
    normalized_retry.operation_id = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();

    // Retried create (`expected_operation_id = Some(&[])`) against a legacy empty-op row
    // must succeed idempotently (`Ok(true)`) instead of raising CasConflict.
    assert!(
        check_workspace_cas_precondition(Some(&legacy_existing), &normalized_retry, Some(&[]))
            .unwrap()
    );

    // Retried CAS update with a stale expected_operation_id against an identical normalized
    // target also returns `Ok(true)` idempotently.
    assert!(
        check_workspace_cas_precondition(
            Some(&legacy_existing),
            &normalized_retry,
            Some(&[0x99; 16])
        )
        .unwrap()
    );

    // A genuinely different target (commit_id or mutable non-key user) with create-only
    // precondition still conflicts.
    let mut different_target = normalized_retry.clone();
    different_target.commit_id = vec![0xab; 20];
    assert!(matches!(
        check_workspace_cas_precondition(Some(&legacy_existing), &different_target, Some(&[])),
        Err(StoreError::CasConflict(_))
    ));

    let mut different_user = normalized_retry.clone();
    different_user.user = "agent-42".to_string();
    assert!(matches!(
        check_workspace_cas_precondition(Some(&legacy_existing), &different_user, Some(&[])),
        Err(StoreError::CasConflict(_))
    ));
    assert!(matches!(
        check_workspace_cas_precondition(
            Some(&legacy_existing),
            &different_user,
            Some(&[0x99; 16])
        ),
        Err(StoreError::CasConflict(_))
    ));
    // Updating `user` with a matching expected_operation_id proceeds with the write (`Ok(false)`).
    assert!(
        !check_workspace_cas_precondition(
            Some(&legacy_existing),
            &different_user,
            Some(&cc_common::ROOT_OPERATION_ID_BYTES),
        )
        .unwrap()
    );
}

#[tokio::test]
async fn test_composite_store_rejects_root_operation_id_as_new_op_head() {
    let dir = tempfile::tempdir().unwrap();
    let stores = [
        CompositeStore::in_memory(),
        CompositeStore::with_config(
            Arc::new(SqliteMetadataStore::open(&dir.path().join("root_guard.db")).unwrap()),
            Arc::new(MemoryBlobStore::new()),
            CompositeStoreConfig {
                inline_blob_threshold: 0,
                validate_dependencies: false,
            },
        ),
    ];

    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let op_a = vec![0x11; cc_common::OPERATION_ID_LENGTH];
    for store in stores {
        store.register_repo("repo-root-guard", None).await.unwrap();
        store
            .put_operation(
                "repo-root-guard",
                &op_a,
                &Operation {
                    view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
                    parents: vec![root_op.clone()],
                    metadata: None,
                    commit_predecessors: Vec::new(),
                    commit_predecessors_set: true,
                },
            )
            .await
            .unwrap();
        let advanced = store
            .update_op_heads("repo-root-guard", std::slice::from_ref(&root_op), &op_a)
            .await
            .unwrap();
        assert_eq!(advanced, vec![op_a.clone()]);

        assert!(matches!(
            store
                .update_op_heads("repo-root-guard", std::slice::from_ref(&op_a), &root_op)
                .await,
            Err(StoreError::InvalidArgument(_))
        ));
        assert!(matches!(
            store
                .update_op_heads("repo-root-guard", &[], &root_op)
                .await,
            Err(StoreError::InvalidArgument(_))
        ));
        assert!(matches!(
            store
                .compare_and_swap_op_heads(
                    "repo-root-guard",
                    std::slice::from_ref(&op_a),
                    &root_op,
                )
                .await,
            Err(StoreError::InvalidArgument(_))
        ));
        let root_view = View {
            head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: HashMap::new(),
        };
        let root_op_msg = Operation {
            view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
            parents: vec![op_a.clone()],
            metadata: None,
            commit_predecessors: Vec::new(),
            commit_predecessors_set: true,
        };
        assert!(matches!(
            store
                .publish_operation(
                    "repo-root-guard",
                    &cc_common::ROOT_VIEW_ID_BYTES,
                    &root_view,
                    &root_op,
                    &root_op_msg,
                    std::slice::from_ref(&op_a),
                )
                .await,
            Err(StoreError::InvalidArgument(_))
        ));
        assert_eq!(
            store.get_op_heads("repo-root-guard").await.unwrap(),
            vec![op_a.clone()]
        );
    }
}

#[tokio::test]
async fn test_op_heads_tx_rejects_dangling_head_when_operation_missing_or_repo_recreated() {
    use std::sync::atomic::{AtomicBool, Ordering};

    /// Intercepts `put_operation_for_project`, `update_op_heads_for_project`,
    /// `update_op_heads_cas_for_project`, `publish_operation_for_project`, and
    /// `put_workspace_cas_for_project` right before the serializable write transaction runs
    /// (and optionally `get_operation` / `get_view` during validation) to simulate concurrent
    /// `DeleteRepository` (+ optional same- or cross-project re-registration).
    struct RecreateSameProjectBeforeHeadTx {
        inner: Arc<dyn MetadataStore>,
        repo_id: String,
        project_id: String,
        armed: AtomicBool,
        validation_race: std::sync::Mutex<Option<Option<String>>>,
    }

    impl RecreateSameProjectBeforeHeadTx {
        async fn maybe_recreate(&self) -> StoreResult<()> {
            if self.armed.swap(false, Ordering::SeqCst) {
                self.inner
                    .delete_repository(&self.repo_id, Some(&self.project_id))
                    .await?;
                self.inner
                    .register_repo_in_project(&self.repo_id, &self.project_id, None)
                    .await?;
            }
            Ok(())
        }

        async fn maybe_trigger_validation_race(&self) -> StoreResult<()> {
            let action = self.validation_race.lock().unwrap().take();
            if let Some(rebind_project) = action {
                self.inner
                    .delete_repository(&self.repo_id, Some(&self.project_id))
                    .await?;
                if let Some(target_proj) = rebind_project {
                    self.inner
                        .register_repo_in_project(&self.repo_id, &target_proj, None)
                        .await?;
                }
            }
            Ok(())
        }
    }

    #[async_trait]
    impl MetadataStore for RecreateSameProjectBeforeHeadTx {
        fn backend_name(&self) -> &'static str {
            self.inner.backend_name()
        }
        async fn register_project(&self, project_id: &str, name: Option<&str>) -> StoreResult<()> {
            self.inner.register_project(project_id, name).await
        }
        async fn is_project_registered(&self, project_id: &str) -> StoreResult<bool> {
            self.inner.is_project_registered(project_id).await
        }
        async fn get_repo_project_id(&self, repo_id: &str) -> StoreResult<Option<String>> {
            self.inner.get_repo_project_id(repo_id).await
        }
        async fn register_repo_in_project(
            &self,
            repo_id: &str,
            project_id: &str,
            name: Option<&str>,
        ) -> StoreResult<String> {
            self.inner
                .register_repo_in_project(repo_id, project_id, name)
                .await
        }
        async fn delete_repository(
            &self,
            repo_id: &str,
            expected_project_id: Option<&str>,
        ) -> StoreResult<bool> {
            self.inner
                .delete_repository(repo_id, expected_project_id)
                .await
        }
        async fn get_commit(
            &self,
            project_id: &str,
            commit_id: &[u8],
        ) -> StoreResult<Option<Commit>> {
            self.inner.get_commit(project_id, commit_id).await
        }
        async fn get_commit_generation(
            &self,
            project_id: &str,
            commit_id: &[u8],
        ) -> StoreResult<Option<u64>> {
            self.inner
                .get_commit_generation(project_id, commit_id)
                .await
        }
        async fn put_commit(
            &self,
            project_id: &str,
            commit_id: &[u8],
            commit: &Commit,
            generation: u64,
        ) -> StoreResult<()> {
            self.inner
                .put_commit(project_id, commit_id, commit, generation)
                .await
        }
        async fn list_commit_ids(
            &self,
            project_id: &str,
            after_commit_id: Option<&[u8]>,
            limit: usize,
        ) -> StoreResult<Vec<CommitId>> {
            self.inner
                .list_commit_ids(project_id, after_commit_id, limit)
                .await
        }
        async fn current_commit_watermark(&self, project_id: &str) -> StoreResult<Vec<u8>> {
            self.inner.current_commit_watermark(project_id).await
        }
        async fn list_commit_ids_since(
            &self,
            project_id: &str,
            since_token: &[u8],
            limit: usize,
        ) -> StoreResult<(Vec<CommitId>, Vec<u8>, bool)> {
            self.inner
                .list_commit_ids_since(project_id, since_token, limit)
                .await
        }
        async fn resolve_commit_id_prefix(
            &self,
            project_id: &str,
            min_id: &[u8],
            max_id: &[u8],
        ) -> StoreResult<Vec<CommitId>> {
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
        ) -> StoreResult<Vec<(Vec<u8>, Vec<CommitId>)>> {
            self.inner
                .resolve_change_id_prefix(project_id, min_id, max_id, limit)
                .await
        }
        async fn get_tree(
            &self,
            project_id: &str,
            tree_id: &[u8],
        ) -> StoreResult<Option<Vec<TreeEntry>>> {
            self.inner.get_tree(project_id, tree_id).await
        }
        async fn put_tree(
            &self,
            project_id: &str,
            tree_id: &[u8],
            entries: &[TreeEntry],
        ) -> StoreResult<()> {
            self.inner.put_tree(project_id, tree_id, entries).await
        }
        async fn find_existing_trees(
            &self,
            project_id: &str,
            tree_ids: &[Vec<u8>],
        ) -> StoreResult<HashSet<Vec<u8>>> {
            self.inner.find_existing_trees(project_id, tree_ids).await
        }
        async fn get_symlink(
            &self,
            project_id: &str,
            symlink_id: &[u8],
        ) -> StoreResult<Option<String>> {
            self.inner.get_symlink(project_id, symlink_id).await
        }
        async fn put_symlink(
            &self,
            project_id: &str,
            symlink_id: &[u8],
            target: &str,
        ) -> StoreResult<()> {
            self.inner.put_symlink(project_id, symlink_id, target).await
        }
        async fn find_existing_symlinks(
            &self,
            project_id: &str,
            symlink_ids: &[Vec<u8>],
        ) -> StoreResult<HashSet<Vec<u8>>> {
            self.inner
                .find_existing_symlinks(project_id, symlink_ids)
                .await
        }
        async fn get_blob_record(
            &self,
            project_id: &str,
            blob_id: &[u8],
        ) -> StoreResult<Option<BlobRecord>> {
            self.inner.get_blob_record(project_id, blob_id).await
        }
        async fn get_blob_sizes_batch(
            &self,
            project_id: &str,
            blob_ids: &[Vec<u8>],
        ) -> StoreResult<HashMap<Vec<u8>, u64>> {
            self.inner.get_blob_sizes_batch(project_id, blob_ids).await
        }
        async fn get_global_blob_record(&self, blob_id: &[u8]) -> StoreResult<Option<BlobRecord>> {
            self.inner.get_global_blob_record(blob_id).await
        }
        async fn put_blob_record(&self, project_id: &str, record: &BlobRecord) -> StoreResult<()> {
            self.inner.put_blob_record(project_id, record).await
        }
        async fn add_blob_replica_location(
            &self,
            blob_id: &[u8],
            location: &str,
        ) -> StoreResult<()> {
            self.inner
                .add_blob_replica_location(blob_id, location)
                .await
        }
        async fn get_operation(
            &self,
            repo_id: &str,
            op_id: &[u8],
        ) -> StoreResult<Option<Operation>> {
            self.maybe_trigger_validation_race().await?;
            self.inner.get_operation(repo_id, op_id).await
        }
        async fn put_operation(
            &self,
            repo_id: &str,
            op_id: &[u8],
            op: &Operation,
        ) -> StoreResult<()> {
            self.inner.put_operation(repo_id, op_id, op).await
        }
        async fn put_operation_for_project(
            &self,
            repo_id: &str,
            expected_project_id: &str,
            op_id: &[u8],
            op: &Operation,
            require_dependencies_exist: bool,
        ) -> StoreResult<()> {
            self.maybe_recreate().await?;
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
        ) -> StoreResult<Vec<OpId>> {
            self.inner
                .resolve_operation_id_prefix(repo_id, min_id, max_id)
                .await
        }
        async fn get_view(&self, repo_id: &str, view_id: &[u8]) -> StoreResult<Option<View>> {
            self.maybe_trigger_validation_race().await?;
            self.inner.get_view(repo_id, view_id).await
        }
        async fn put_view(&self, repo_id: &str, view_id: &[u8], view: &View) -> StoreResult<()> {
            self.inner.put_view(repo_id, view_id, view).await
        }
        async fn put_view_for_project(
            &self,
            repo_id: &str,
            expected_project_id: &str,
            view_id: &[u8],
            view: &View,
        ) -> StoreResult<()> {
            self.inner
                .put_view_for_project(repo_id, expected_project_id, view_id, view)
                .await
        }
        async fn get_op_heads(&self, repo_id: &str) -> StoreResult<Option<Vec<OpId>>> {
            self.inner.get_op_heads(repo_id).await
        }
        async fn update_op_heads(
            &self,
            repo_id: &str,
            old_ids: &[Vec<u8>],
            new_id: &[u8],
        ) -> StoreResult<Vec<OpId>> {
            self.inner.update_op_heads(repo_id, old_ids, new_id).await
        }
        async fn update_op_heads_for_project(
            &self,
            repo_id: &str,
            expected_project_id: &str,
            old_head_ids: &[Vec<u8>],
            new_head_id: &[u8],
            require_op_exists: bool,
        ) -> StoreResult<Vec<OpId>> {
            self.maybe_recreate().await?;
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
            view: &View,
            op_id: &[u8],
            op: &Operation,
            old_ids: &[Vec<u8>],
        ) -> StoreResult<Vec<OpId>> {
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
            view: &View,
            op_id: &[u8],
            op: &Operation,
            old_op_head_ids: &[Vec<u8>],
            require_dependencies_exist: bool,
        ) -> StoreResult<Vec<OpId>> {
            self.maybe_recreate().await?;
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
            self.maybe_recreate().await?;
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
        ) -> StoreResult<Option<WorkspaceState>> {
            self.inner.get_workspace(repo_id, workspace_name).await
        }
        async fn put_workspace_cas(
            &self,
            workspace: &WorkspaceState,
            expected_operation_id: Option<&[u8]>,
        ) -> StoreResult<()> {
            self.inner
                .put_workspace_cas(workspace, expected_operation_id)
                .await
        }
        async fn put_workspace_cas_for_project(
            &self,
            expected_project_id: &str,
            workspace: &WorkspaceState,
            expected_operation_id: Option<&[u8]>,
            require_operation_exists: bool,
        ) -> StoreResult<()> {
            self.maybe_recreate().await?;
            self.inner
                .put_workspace_cas_for_project(
                    expected_project_id,
                    workspace,
                    expected_operation_id,
                    require_operation_exists,
                )
                .await
        }
        async fn list_workspaces(&self, repo_id: &str) -> StoreResult<Vec<WorkspaceState>> {
            self.inner.list_workspaces(repo_id).await
        }
        async fn delete_workspace(&self, repo_id: &str, workspace_name: &str) -> StoreResult<bool> {
            self.inner.delete_workspace(repo_id, workspace_name).await
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("dangling_head_guard.db");
    let backends: Vec<(&str, Arc<dyn MetadataStore>)> = vec![
        ("memory", Arc::new(MemoryMetadataStore::new())),
        (
            "sqlite",
            Arc::new(SqliteMetadataStore::open(&db_path).unwrap()),
        ),
    ];

    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let op_id = vec![0xab; cc_common::OPERATION_ID_LENGTH];
    let op = Operation {
        view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
        parents: vec![root_op.clone()],
        metadata: None,
        commit_predecessors: Vec::new(),
        commit_predecessors_set: true,
    };

    for (label, raw_meta) in backends {
        let wrapped = Arc::new(RecreateSameProjectBeforeHeadTx {
            inner: raw_meta.clone(),
            repo_id: "repo-dangling".to_string(),
            project_id: "proj-same".to_string(),
            armed: AtomicBool::new(false),
            validation_race: std::sync::Mutex::new(None),
        });
        let store = CompositeStore::new(wrapped.clone(), Arc::new(MemoryBlobStore::new()));
        store
            .register_repo_in_project("repo-dangling", Some("proj-same"), None)
            .await
            .unwrap();

        // Direct transaction check rejects an absent operation row with MissingDependency.
        let direct_upd_err = raw_meta
            .update_op_heads_for_project(
                "repo-dangling",
                "proj-same",
                std::slice::from_ref(&root_op),
                &op_id,
                true,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                direct_upd_err,
                StoreError::MissingDependency {
                    parent_kind: "op_heads",
                    missing_kind: "operation",
                    ..
                }
            ),
            "{label}: expected MissingDependency on direct update_op_heads_for_project, got {direct_upd_err:?}"
        );
        let direct_cas_err = raw_meta
            .update_op_heads_cas_for_project(
                "repo-dangling",
                "proj-same",
                std::slice::from_ref(&root_op),
                &op_id,
                true,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                direct_cas_err,
                StoreError::MissingDependency {
                    parent_kind: "op_heads",
                    missing_kind: "operation",
                    ..
                }
            ),
            "{label}: expected MissingDependency on direct update_op_heads_cas_for_project, got {direct_cas_err:?}"
        );

        // Concurrent DeleteRepository + re-register under the same project right before
        // `update_op_heads` commits must reject with MissingDependency instead of committing
        // a dangling head.
        store
            .put_operation("repo-dangling", &op_id, &op)
            .await
            .unwrap();
        wrapped.armed.store(true, Ordering::SeqCst);
        let race_upd_err = store
            .update_op_heads("repo-dangling", std::slice::from_ref(&root_op), &op_id)
            .await
            .unwrap_err();
        assert!(
            matches!(
                race_upd_err,
                StoreError::MissingDependency {
                    parent_kind: "op_heads",
                    missing_kind: "operation",
                    ..
                }
            ),
            "{label}: expected MissingDependency on same-project recreate race (update_op_heads), got {race_upd_err:?}"
        );
        assert_eq!(
            store.get_op_heads("repo-dangling").await.unwrap(),
            vec![root_op.clone()],
            "{label}: op_heads must remain [root] after rejected update_op_heads"
        );

        // Same-project DeleteRepository + re-register right before
        // `compare_and_swap_op_heads` commits must also reject with MissingDependency.
        store
            .put_operation("repo-dangling", &op_id, &op)
            .await
            .unwrap();
        wrapped.armed.store(true, Ordering::SeqCst);
        let race_cas_err = store
            .compare_and_swap_op_heads("repo-dangling", std::slice::from_ref(&root_op), &op_id)
            .await
            .unwrap_err();
        assert!(
            matches!(
                race_cas_err,
                StoreError::MissingDependency {
                    parent_kind: "op_heads",
                    missing_kind: "operation",
                    ..
                }
            ),
            "{label}: expected MissingDependency on same-project recreate race (compare_and_swap_op_heads), got {race_cas_err:?}"
        );
        assert_eq!(
            store.get_op_heads("repo-dangling").await.unwrap(),
            vec![root_op.clone()],
            "{label}: op_heads must remain [root] after rejected compare_and_swap_op_heads"
        );

        // Same-project DeleteRepository + re-register right before `publish_operation`
        // commits must reject dangling parent ops AND dangling external views atomically.
        let view1_id = vec![0x31; cc_common::VIEW_ID_LENGTH];
        let view2_id = vec![0x32; cc_common::VIEW_ID_LENGTH];
        let view_payload = View {
            head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
            ..Default::default()
        };
        let op2_id = vec![0xac; cc_common::OPERATION_ID_LENGTH];
        let op2_with_parent = Operation {
            view_id: view2_id.clone(),
            parents: vec![op_id.clone()],
            metadata: None,
            commit_predecessors: Vec::new(),
            commit_predecessors_set: true,
        };
        store
            .put_operation("repo-dangling", &op_id, &op)
            .await
            .unwrap();
        wrapped.armed.store(true, Ordering::SeqCst);
        let race_pub_parent_err = store
            .publish_operation(
                "repo-dangling",
                &view2_id,
                &view_payload,
                &op2_id,
                &op2_with_parent,
                std::slice::from_ref(&op_id),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                race_pub_parent_err,
                StoreError::MissingDependency {
                    parent_kind: "operation",
                    missing_kind: "parent operation",
                    ..
                }
            ),
            "{label}: expected MissingDependency(parent operation) on publish_operation recreate race, got {race_pub_parent_err:?}"
        );
        assert_eq!(
            store.get_op_heads("repo-dangling").await.unwrap(),
            vec![root_op.clone()],
            "{label}: op_heads must remain [root] after rejected publish_operation"
        );
        assert!(
            raw_meta
                .get_operation("repo-dangling", &op2_id)
                .await
                .unwrap()
                .is_none(),
            "{label}: rejected publish_operation must not write op row"
        );
        assert!(
            raw_meta
                .get_view("repo-dangling", &view2_id)
                .await
                .unwrap()
                .is_none(),
            "{label}: rejected publish_operation must not write view row"
        );

        // External `op.view_id != view_id` race:
        store
            .put_view("repo-dangling", &view1_id, &view_payload)
            .await
            .unwrap();
        let op_ext_view = Operation {
            view_id: view1_id.clone(),
            parents: vec![root_op.clone()],
            metadata: None,
            commit_predecessors: Vec::new(),
            commit_predecessors_set: true,
        };
        wrapped.armed.store(true, Ordering::SeqCst);
        let race_pub_view_err = store
            .publish_operation(
                "repo-dangling",
                &view2_id,
                &view_payload,
                &op2_id,
                &op_ext_view,
                std::slice::from_ref(&root_op),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                race_pub_view_err,
                StoreError::MissingDependency {
                    parent_kind: "operation",
                    missing_kind: "view",
                    ..
                }
            ),
            "{label}: expected MissingDependency(view) on publish_operation external-view race, got {race_pub_view_err:?}"
        );
        assert_eq!(
            store.get_op_heads("repo-dangling").await.unwrap(),
            vec![root_op.clone()],
            "{label}: op_heads must remain [root] after rejected external-view publish_operation"
        );

        // Concurrent deletion or cross-project re-bind during `publish_operation`
        // validation returns RepoNotFound / FailedPrecondition rather than MissingDependency.
        *wrapped.validation_race.lock().unwrap() = Some(None);
        let del_during_val = store
            .publish_operation(
                "repo-dangling",
                &view2_id,
                &view_payload,
                &op2_id,
                &op2_with_parent,
                std::slice::from_ref(&op_id),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(del_during_val, StoreError::RepoNotFound { .. }),
            "{label}: delete during publish_operation parent validation must return RepoNotFound, got {del_during_val:?}"
        );

        store
            .register_repo_in_project("repo-dangling", Some("proj-same"), None)
            .await
            .unwrap();
        store.register_project("proj-other", None).await.unwrap();
        *wrapped.validation_race.lock().unwrap() = Some(Some("proj-other".to_string()));
        let rebind_during_val = store
            .publish_operation(
                "repo-dangling",
                &view2_id,
                &view_payload,
                &op2_id,
                &op_ext_view,
                std::slice::from_ref(&root_op),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(rebind_during_val, StoreError::FailedPrecondition(_)),
            "{label}: cross-project rebind during publish_operation view validation must return FailedPrecondition, got {rebind_during_val:?}"
        );
        assert!(
            raw_meta
                .delete_repository("repo-dangling", Some("proj-other"))
                .await
                .unwrap()
        );
        store
            .register_repo_in_project("repo-dangling", Some("proj-same"), None)
            .await
            .unwrap();

        // 5b. Same-project DeleteRepository + re-register right before `put_operation`
        // commits must reject dangling view and parent-operation references atomically.
        store
            .put_view("repo-dangling", &view1_id, &view_payload)
            .await
            .unwrap();
        wrapped.armed.store(true, Ordering::SeqCst);
        let race_put_op_view_err = store
            .put_operation("repo-dangling", &op2_id, &op_ext_view)
            .await
            .unwrap_err();
        assert!(
            matches!(
                race_put_op_view_err,
                StoreError::MissingDependency {
                    parent_kind: "operation",
                    missing_kind: "view",
                    ..
                }
            ),
            "{label}: expected MissingDependency(view) on put_operation recreate race, got {race_put_op_view_err:?}"
        );
        assert!(
            raw_meta
                .get_operation("repo-dangling", &op2_id)
                .await
                .unwrap()
                .is_none(),
            "{label}: rejected put_operation must not write op row"
        );

        store
            .put_operation("repo-dangling", &op_id, &op)
            .await
            .unwrap();
        let op_root_view_with_parent = Operation {
            view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
            parents: vec![op_id.clone()],
            metadata: None,
            commit_predecessors: Vec::new(),
            commit_predecessors_set: true,
        };
        wrapped.armed.store(true, Ordering::SeqCst);
        let race_put_op_parent_err = store
            .put_operation("repo-dangling", &op2_id, &op_root_view_with_parent)
            .await
            .unwrap_err();
        assert!(
            matches!(
                race_put_op_parent_err,
                StoreError::MissingDependency {
                    parent_kind: "operation",
                    missing_kind: "parent operation",
                    ..
                }
            ),
            "{label}: expected MissingDependency(parent operation) on put_operation recreate race, got {race_put_op_parent_err:?}"
        );
        assert!(
            raw_meta
                .get_operation("repo-dangling", &op2_id)
                .await
                .unwrap()
                .is_none(),
            "{label}: rejected put_operation must not write op row"
        );

        // 5c. Same-project DeleteRepository + re-register right before `put_workspace`
        // commits must reject dangling non-root `operation_id` atomically.
        store
            .put_operation("repo-dangling", &op_id, &op)
            .await
            .unwrap();
        #[allow(deprecated)]
        let ws_with_op = WorkspaceState {
            repo_id: "repo-dangling".to_string(),
            user: "alice".to_string(),
            workspace_name: "ws-race".to_string(),
            commit_id: cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
            operation_id: op_id.clone(),
            tree_id: cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
            tree_ids: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
            conflict_labels: Vec::new(),
            sparse_patterns: vec![String::new()],
            sparse_patterns_set: true,
        };
        wrapped.armed.store(true, Ordering::SeqCst);
        let race_ws_err = store.put_workspace(&ws_with_op).await.unwrap_err();
        assert!(
            matches!(
                race_ws_err,
                StoreError::MissingDependency {
                    parent_kind: "workspace",
                    missing_kind: "operation",
                    ..
                }
            ),
            "{label}: expected MissingDependency(workspace, operation) on put_workspace recreate race, got {race_ws_err:?}"
        );
        assert!(
            raw_meta
                .get_workspace("repo-dangling", "ws-race")
                .await
                .unwrap()
                .is_none(),
            "{label}: rejected put_workspace must not persist dangling workspace row"
        );

        // With `validate_dependencies = false`, `require_op_exists = false`,
        // `require_dependencies_exist = false`, and `require_operation_exists = false` allow
        // writing operations, workspaces, and op_heads without pre-existing rows.
        let relaxed = CompositeStore::with_config(
            raw_meta.clone(),
            Arc::new(MemoryBlobStore::new()),
            CompositeStoreConfig {
                inline_blob_threshold: 0,
                validate_dependencies: false,
            },
        );
        relaxed
            .put_operation("repo-dangling", &op2_id, &op_ext_view)
            .await
            .unwrap();
        relaxed.put_workspace(&ws_with_op).await.unwrap();
        assert!(
            raw_meta
                .get_workspace("repo-dangling", "ws-race")
                .await
                .unwrap()
                .is_some(),
            "{label}: relaxed store should permit writing workspace without existing op"
        );
        relaxed
            .publish_operation(
                "repo-dangling",
                &view2_id,
                &view_payload,
                &op2_id,
                &op2_with_parent,
                std::slice::from_ref(&root_op),
            )
            .await
            .unwrap();
        assert_eq!(
            relaxed.get_op_heads("repo-dangling").await.unwrap(),
            vec![op2_id.clone()],
            "{label}: relaxed store should permit publishing op without existing parent"
        );
        relaxed
            .compare_and_swap_op_heads("repo-dangling", std::slice::from_ref(&op2_id), &op_id)
            .await
            .unwrap();
        assert_eq!(
            relaxed.get_op_heads("repo-dangling").await.unwrap(),
            vec![op_id.clone()],
            "{label}: relaxed store should permit advancing head"
        );
    }
}
