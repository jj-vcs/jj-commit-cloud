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

    // A genuinely different target with create-only precondition still conflicts.
    let mut different_target = normalized_retry.clone();
    different_target.commit_id = vec![0xab; 20];
    assert!(matches!(
        check_workspace_cas_precondition(Some(&legacy_existing), &different_target, Some(&[])),
        Err(StoreError::CasConflict(_))
    ));
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
