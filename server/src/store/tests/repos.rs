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

#[test]
fn test_bounded_namespace_cache_evicts_oldest_entries() {
    let mut cache = BoundedNamespaceCache::with_capacity(2);
    cache.insert("a".to_string(), ());
    cache.insert("b".to_string(), ());
    cache.insert("c".to_string(), ());
    assert!(!cache.contains("a"));
    assert!(cache.contains("b"));
    assert!(cache.contains("c"));
    // Re-inserting an existing key updates in place without evicting another entry.
    cache.insert("b".to_string(), ());
    assert!(cache.contains("b"));
    assert!(cache.contains("c"));
    cache.insert("d".to_string(), ());
    assert!(!cache.contains("b"));
    assert!(cache.contains("c"));
    assert!(cache.contains("d"));
}

#[tokio::test]
async fn test_delete_repository_memory_and_sqlite_cascades_children_and_preserves_shared_project() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("delete_repo.db");
    let backends: Vec<(&str, Arc<dyn MetadataStore>)> = vec![
        ("memory", Arc::new(MemoryMetadataStore::new())),
        (
            "sqlite",
            Arc::new(SqliteMetadataStore::open(&db_path).unwrap()),
        ),
    ];

    for (label, meta) in backends {
        let store = CompositeStore::new(meta.clone(), Arc::new(MemoryBlobStore::new()));
        store
            .register_project("shared-proj", Some("Shared"))
            .await
            .unwrap();
        store
            .register_repo_in_project("repo-a", Some("shared-proj"), None)
            .await
            .unwrap();
        store
            .register_repo_in_project("repo-b", Some("shared-proj"), None)
            .await
            .unwrap();

        // Shared project objects: blob, symlink, tree, commit.
        let file_id = vec![0x11; cc_common::FILE_ID_LENGTH];
        store
            .put_file("repo-a", &file_id, Bytes::from_static(b"shared payload"))
            .await
            .unwrap();
        let sym_id = vec![0x22; cc_common::SYMLINK_ID_LENGTH];
        store
            .put_symlink("repo-a", &sym_id, "../target")
            .await
            .unwrap();
        let tree_id = vec![0x33; cc_common::TREE_ID_LENGTH];
        store
            .put_tree(
                "repo-a",
                &tree_id,
                &[
                    TreeEntry {
                        name: "file.txt".to_string(),
                        value: Some(TreeValue {
                            value: Some(tree_value::Value::File(File {
                                id: file_id.clone(),
                                executable: false,
                                copy_id: Vec::new(),
                                size_bytes: None,
                            })),
                        }),
                    },
                    TreeEntry {
                        name: "link".to_string(),
                        value: Some(TreeValue {
                            value: Some(tree_value::Value::SymlinkId(sym_id.clone())),
                        }),
                    },
                ],
            )
            .await
            .unwrap();
        let commit_id = vec![0x44; cc_common::COMMIT_ID_LENGTH];
        let commit = Commit {
            commit_id: commit_id.clone(),
            change_id: vec![0x55; cc_common::CHANGE_ID_LENGTH],
            parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
            root_tree_id: vec![tree_id.clone()],
            description: "shared commit\n".to_string(),
            ..Default::default()
        };
        store
            .put_commit("repo-a", &commit_id, &commit)
            .await
            .unwrap();

        // Repo-scoped children for repo-a and repo-b.
        let view_id = vec![0x66; cc_common::VIEW_ID_LENGTH];
        let view = View {
            head_ids: vec![commit_id.clone()],
            ..Default::default()
        };
        let op_id = vec![0x77; cc_common::OPERATION_ID_LENGTH];
        let op = Operation {
            view_id: view_id.clone(),
            parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
            ..Default::default()
        };
        for repo in ["repo-a", "repo-b"] {
            store
                .publish_operation(
                    repo,
                    &view_id,
                    &view,
                    &op_id,
                    &op,
                    &[cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                )
                .await
                .unwrap();
            #[allow(deprecated)]
            let ws = WorkspaceState {
                repo_id: repo.to_string(),
                user: "alice".to_string(),
                workspace_name: "default".to_string(),
                commit_id: commit_id.clone(),
                operation_id: op_id.clone(),
                tree_id: tree_id.clone(),
                tree_ids: vec![tree_id.clone()],
                conflict_labels: Vec::new(),
                sparse_patterns: vec![String::new()],
                sparse_patterns_set: true,
            };
            store.put_workspace(&ws).await.unwrap();
        }

        // Missing repo is idempotent Ok(false), with or without expected project_id.
        assert!(
            !store.delete_repository("no-such-repo", None).await.unwrap(),
            "{label}"
        );
        assert!(
            !store
                .delete_repository("no-such-repo", Some("shared-proj"))
                .await
                .unwrap(),
            "{label}"
        );

        // Mismatched project_id returns FailedPrecondition without deleting repo-a.
        let err = store
            .delete_repository("repo-a", Some("wrong-proj"))
            .await
            .unwrap_err();
        assert!(
            matches!(err, StoreError::FailedPrecondition(_)),
            "{label}: {err:?}"
        );
        assert_eq!(
            store.get_op_heads("repo-a").await.unwrap(),
            vec![op_id.clone()]
        );
        assert_eq!(store.list_workspaces("repo-a").await.unwrap().len(), 1);

        // Matching project_id deletes repo-a and returns Ok(true); second call is Ok(false).
        assert!(
            store
                .delete_repository("repo-a", Some("shared-proj"))
                .await
                .unwrap(),
            "{label}"
        );
        assert!(
            !store
                .delete_repository("repo-a", Some("shared-proj"))
                .await
                .unwrap(),
            "{label}"
        );

        // Cache evicted & repo-scoped endpoints report RepoNotFound.
        assert!(matches!(
            store.get_op_heads("repo-a").await,
            Err(StoreError::RepoNotFound { .. })
        ));
        assert!(matches!(
            store.get_view("repo-a", &view_id).await,
            Err(StoreError::RepoNotFound { .. })
        ));
        assert!(matches!(
            store.get_operation("repo-a", &op_id).await,
            Err(StoreError::RepoNotFound { .. })
        ));
        assert!(matches!(
            store.list_workspaces("repo-a").await,
            Err(StoreError::RepoNotFound { .. })
        ));

        // Direct metadata inspection confirms child rows were deleted.
        assert!(
            meta.get_view("repo-a", &view_id).await.unwrap().is_none(),
            "{label}"
        );
        assert!(
            meta.get_operation("repo-a", &op_id)
                .await
                .unwrap()
                .is_none(),
            "{label}"
        );
        assert!(
            meta.get_op_heads("repo-a").await.unwrap().is_none(),
            "{label}"
        );
        assert!(
            meta.list_workspaces("repo-a").await.unwrap().is_empty(),
            "{label}"
        );

        // Shared project objects remain intact and readable from sibling repo-b and project.
        assert_eq!(
            store
                .get_file_range("repo-b", &file_id, 0, None)
                .await
                .unwrap()
                .as_ref(),
            b"shared payload"
        );
        assert_eq!(
            store.get_symlink("repo-b", &sym_id).await.unwrap(),
            "../target"
        );
        assert_eq!(store.get_tree("repo-b", &tree_id).await.unwrap().len(), 2);
        assert_eq!(
            store
                .get_commit("repo-b", &commit_id)
                .await
                .unwrap()
                .description,
            "shared commit\n"
        );
        assert_eq!(
            store
                .get_commit("shared-proj", &commit_id)
                .await
                .unwrap()
                .description,
            "shared commit\n"
        );
        // Sibling repo-b's children are unaffected.
        assert_eq!(
            store.get_op_heads("repo-b").await.unwrap(),
            vec![op_id.clone()]
        );
        assert_eq!(store.list_workspaces("repo-b").await.unwrap().len(), 1);

        // Re-registering repo-a starts clean without resurrecting old children.
        store
            .register_repo_in_project("repo-a", Some("shared-proj"), None)
            .await
            .unwrap();
        assert_eq!(
            store.get_op_heads("repo-a").await.unwrap(),
            vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()]
        );
        assert!(store.get_view("repo-a", &view_id).await.unwrap().is_none());
        assert!(
            store
                .get_operation("repo-a", &op_id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(store.list_workspaces("repo-a").await.unwrap().is_empty());

        // Deleting directly in MetadataStore (simulating another server replica) must cause
        // get_op_heads to evict the cached binding and return RepoNotFound, and direct
        // repo-scoped writes on a deleted repo must return RepoNotFound without creating rows.
        assert!(
            meta.delete_repository("repo-a", Some("shared-proj"))
                .await
                .unwrap(),
            "{label}"
        );
        assert!(
            matches!(
                store.get_op_heads("repo-a").await,
                Err(StoreError::RepoNotFound { .. })
            ),
            "{label}"
        );
        assert!(
            matches!(
                meta.put_view("repo-a", &view_id, &View::default()).await,
                Err(StoreError::RepoNotFound { .. })
            ),
            "{label}"
        );
        assert!(
            matches!(
                meta.put_operation("repo-a", &op_id, &Operation::default())
                    .await,
                Err(StoreError::RepoNotFound { .. })
            ),
            "{label}"
        );
        assert!(
            matches!(
                meta.update_op_heads("repo-a", &[], &op_id).await,
                Err(StoreError::RepoNotFound { .. })
            ),
            "{label}"
        );
        assert!(
            matches!(
                meta.publish_operation_atomic(
                    "repo-a",
                    &view_id,
                    &View::default(),
                    &op_id,
                    &Operation::default(),
                    &[],
                )
                .await,
                Err(StoreError::RepoNotFound { .. })
            ),
            "{label}"
        );
        assert!(
            matches!(
                meta.compare_and_swap_op_heads("repo-a", &[], &op_id).await,
                Err(StoreError::RepoNotFound { .. })
            ),
            "{label}"
        );
        #[allow(deprecated)]
        let deleted_repo_ws = WorkspaceState {
            repo_id: "repo-a".to_string(),
            user: "alice@example.com".to_string(),
            workspace_name: "default".to_string(),
            commit_id: cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
            operation_id: cc_common::ROOT_OPERATION_ID_BYTES.to_vec(),
            tree_id: cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
            tree_ids: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
            conflict_labels: Vec::new(),
            sparse_patterns: vec![String::new()],
            sparse_patterns_set: true,
        };
        for expected in [None, Some([].as_slice()), Some(op_id.as_slice())] {
            assert!(
                matches!(
                    meta.put_workspace_cas(&deleted_repo_ws, expected).await,
                    Err(StoreError::RepoNotFound { .. })
                ),
                "{label} expected={expected:?}"
            );
        }
        assert!(meta.get_view("repo-a", &view_id).await.unwrap().is_none());
        assert!(
            meta.get_operation("repo-a", &op_id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(meta.list_workspaces("repo-a").await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn test_multi_replica_re_registration_has_zero_stale_window_across_projects() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("multi_replica.db");
    let backends: Vec<(&str, Arc<dyn MetadataStore>)> = vec![
        ("memory", Arc::new(MemoryMetadataStore::new())),
        (
            "sqlite",
            Arc::new(SqliteMetadataStore::open(&db_path).unwrap()),
        ),
    ];

    for (label, meta) in backends {
        let blobs = Arc::new(MemoryBlobStore::new());
        let replica_a = CompositeStore::new(meta.clone(), blobs.clone());
        let replica_b = CompositeStore::new(meta.clone(), blobs);

        replica_a.register_project("proj-a", None).await.unwrap();
        replica_a.register_project("proj-b", None).await.unwrap();
        replica_a
            .register_repo_in_project("repo-1", Some("proj-a"), None)
            .await
            .unwrap();

        // Write proj-a objects through repo-1 and warm replica_b with reads on repo-1.
        let file_a = vec![0x11; cc_common::FILE_ID_LENGTH];
        let sym_a = vec![0x22; cc_common::SYMLINK_ID_LENGTH];
        let tree_a = vec![0x33; cc_common::TREE_ID_LENGTH];
        let commit_a = vec![0x44; cc_common::COMMIT_ID_LENGTH];
        replica_a
            .put_file("repo-1", &file_a, Bytes::from_static(b"proj-a secret"))
            .await
            .unwrap();
        replica_a
            .put_symlink("repo-1", &sym_a, "../proj-a-target")
            .await
            .unwrap();
        replica_a
            .put_tree(
                "repo-1",
                &tree_a,
                &[TreeEntry {
                    name: "secret.txt".to_string(),
                    value: Some(TreeValue {
                        value: Some(tree_value::Value::File(File {
                            id: file_a.clone(),
                            executable: false,
                            copy_id: Vec::new(),
                            size_bytes: None,
                        })),
                    }),
                }],
            )
            .await
            .unwrap();
        replica_a
            .put_commit(
                "repo-1",
                &commit_a,
                &Commit {
                    commit_id: commit_a.clone(),
                    change_id: vec![0x55; cc_common::CHANGE_ID_LENGTH],
                    parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                    root_tree_id: vec![tree_a.clone()],
                    description: "proj-a commit\n".to_string(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Warm replica_b before re-registration.
        assert_eq!(
            replica_b
                .resolve_repo_project("repo-1", "test")
                .await
                .unwrap(),
            "proj-a",
            "{label}"
        );
        assert_eq!(
            replica_b
                .get_file_range("repo-1", &file_a, 0, None)
                .await
                .unwrap()
                .as_ref(),
            b"proj-a secret",
            "{label}"
        );

        // Replica A deletes repo-1 (proj-a) and immediately re-registers repo-1 under proj-b.
        assert!(
            replica_a
                .delete_repository("repo-1", Some("proj-a"))
                .await
                .unwrap(),
            "{label}"
        );
        replica_a
            .register_repo_in_project("repo-1", Some("proj-b"), None)
            .await
            .unwrap();

        // Replica B must observe proj-b immediately with zero stale cache window.
        assert_eq!(
            replica_b
                .resolve_repo_project("repo-1", "test")
                .await
                .unwrap(),
            "proj-b",
            "{label}"
        );
        assert_eq!(
            replica_b
                .resolve_project_namespace("repo-1", "test")
                .await
                .unwrap(),
            "proj-b",
            "{label}"
        );

        // Replica B cannot read any proj-a object through repo-1.
        assert!(
            matches!(
                replica_b.get_file_range("repo-1", &file_a, 0, None).await,
                Err(StoreError::ObjectNotFound { .. })
            ),
            "{label}"
        );
        assert!(
            matches!(
                replica_b.get_symlink("repo-1", &sym_a).await,
                Err(StoreError::ObjectNotFound { .. })
            ),
            "{label}"
        );
        assert!(
            matches!(
                replica_b.get_tree("repo-1", &tree_a).await,
                Err(StoreError::ObjectNotFound { .. })
            ),
            "{label}"
        );
        assert!(
            matches!(
                replica_b.get_commit("repo-1", &commit_a).await,
                Err(StoreError::ObjectNotFound { .. })
            ),
            "{label}"
        );

        // Writes via repo-1 on Replica B route strictly to proj-b, never proj-a.
        let file_b = vec![0x99; cc_common::FILE_ID_LENGTH];
        replica_b
            .put_file("repo-1", &file_b, Bytes::from_static(b"proj-b data"))
            .await
            .unwrap();
        assert_eq!(
            replica_a
                .get_file_range("proj-b", &file_b, 0, None)
                .await
                .unwrap()
                .as_ref(),
            b"proj-b data",
            "{label}"
        );
        assert!(
            matches!(
                replica_a.get_file_range("proj-a", &file_b, 0, None).await,
                Err(StoreError::ObjectNotFound { .. })
            ),
            "{label}"
        );

        // Replica B cannot persist a view referencing proj-a's commit into repo-1 (proj-b).
        let view_id = vec![0x66; cc_common::VIEW_ID_LENGTH];
        let cross_view = View {
            head_ids: vec![commit_a],
            ..Default::default()
        };
        assert!(
            matches!(
                replica_b.put_view("repo-1", &view_id, &cross_view).await,
                Err(StoreError::MissingDependency { .. })
            ),
            "{label}"
        );
        assert!(
            meta.get_view("repo-1", &view_id).await.unwrap().is_none(),
            "{label}"
        );
    }
}

#[tokio::test]
async fn test_project_checked_writes_reject_stale_project_atomically_on_memory_and_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("toctou_for_project.db");
    let backends: Vec<(&str, Arc<dyn MetadataStore>)> = vec![
        ("memory", Arc::new(MemoryMetadataStore::new())),
        (
            "sqlite",
            Arc::new(SqliteMetadataStore::open(&db_path).unwrap()),
        ),
    ];

    for (label, meta) in backends {
        let store = CompositeStore::new(meta.clone(), Arc::new(MemoryBlobStore::new()));
        store.register_project("proj-a", None).await.unwrap();
        store.register_project("proj-b", None).await.unwrap();
        store
            .register_repo_in_project("repo-1", Some("proj-a"), None)
            .await
            .unwrap();

        // Simulate TOCTOU: caller validated commits against proj-a, then another actor
        // deleted repo-1 (proj-a) and re-registered repo-1 under proj-b before the write.
        assert!(
            store
                .delete_repository("repo-1", Some("proj-a"))
                .await
                .unwrap(),
            "{label}"
        );
        store
            .register_repo_in_project("repo-1", Some("proj-b"), None)
            .await
            .unwrap();

        let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
        let view_id = vec![0xaa; cc_common::VIEW_ID_LENGTH];
        let view = View {
            head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
            ..Default::default()
        };
        let op_id = vec![0xbb; cc_common::OPERATION_ID_LENGTH];
        let op = Operation {
            view_id: view_id.clone(),
            parents: vec![root_op.clone()],
            ..Default::default()
        };
        #[allow(deprecated)]
        let ws = WorkspaceState {
            repo_id: "repo-1".to_string(),
            user: "alice@example.com".to_string(),
            workspace_name: "default".to_string(),
            commit_id: cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
            operation_id: root_op.clone(),
            tree_id: cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
            tree_ids: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
            conflict_labels: Vec::new(),
            sparse_patterns: vec![String::new()],
            sparse_patterns_set: true,
        };

        // Every *_for_project write with stale expected_project_id = "proj-a" must fail
        // atomically with FailedPrecondition and persist nothing under repo-1 (proj-b).
        assert!(
            matches!(
                meta.put_view_for_project("repo-1", "proj-a", &view_id, &view)
                    .await,
                Err(StoreError::FailedPrecondition(_))
            ),
            "{label}: put_view_for_project"
        );
        assert!(
            matches!(
                meta.put_operation_for_project("repo-1", "proj-a", &op_id, &op, true)
                    .await,
                Err(StoreError::FailedPrecondition(_))
            ),
            "{label}: put_operation_for_project"
        );
        assert!(
            matches!(
                meta.publish_operation_for_project(
                    "repo-1",
                    "proj-a",
                    &view_id,
                    &view,
                    &op_id,
                    &op,
                    std::slice::from_ref(&root_op),
                    true,
                )
                .await,
                Err(StoreError::FailedPrecondition(_))
            ),
            "{label}: publish_operation_for_project"
        );
        assert!(
            matches!(
                meta.update_op_heads_for_project(
                    "repo-1",
                    "proj-a",
                    std::slice::from_ref(&root_op),
                    &op_id,
                    true,
                )
                .await,
                Err(StoreError::FailedPrecondition(_))
            ),
            "{label}: update_op_heads_for_project"
        );
        assert!(
            matches!(
                meta.update_op_heads_cas_for_project(
                    "repo-1",
                    "proj-a",
                    std::slice::from_ref(&root_op),
                    &op_id,
                    true,
                )
                .await,
                Err(StoreError::FailedPrecondition(_))
            ),
            "{label}: update_op_heads_cas_for_project"
        );
        for expected in [None, Some([].as_slice()), Some(root_op.as_slice())] {
            assert!(
                matches!(
                    meta.put_workspace_cas_for_project("proj-a", &ws, expected, true)
                        .await,
                    Err(StoreError::FailedPrecondition(_))
                ),
                "{label}: put_workspace_cas_for_project expected={expected:?}"
            );
        }
        assert!(
            matches!(
                store
                    .put_workspace_cas_for_project("proj-a", &ws, None)
                    .await,
                Err(StoreError::FailedPrecondition(_))
            ),
            "{label}: CompositeStore::put_workspace_cas_for_project"
        );

        // Confirm nothing leaked into repo-1 under proj-b.
        assert!(meta.get_view("repo-1", &view_id).await.unwrap().is_none());
        assert!(
            meta.get_operation("repo-1", &op_id)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            meta.get_op_heads("repo-1").await.unwrap(),
            Some(vec![root_op.clone()])
        );
        assert!(meta.list_workspaces("repo-1").await.unwrap().is_empty());

        // All 6 succeed when expected_project_id matches "proj-b".
        meta.put_view_for_project("repo-1", "proj-b", &view_id, &view)
            .await
            .unwrap();
        meta.put_operation_for_project("repo-1", "proj-b", &op_id, &op, true)
            .await
            .unwrap();
        let heads = meta
            .update_op_heads_for_project(
                "repo-1",
                "proj-b",
                std::slice::from_ref(&root_op),
                &op_id,
                true,
            )
            .await
            .unwrap();
        assert_eq!(heads, vec![op_id.clone()]);

        let op2_id = vec![0xcc; cc_common::OPERATION_ID_LENGTH];
        let op2 = Operation {
            view_id: view_id.clone(),
            parents: vec![op_id.clone()],
            ..Default::default()
        };
        let heads2 = meta
            .publish_operation_for_project(
                "repo-1",
                "proj-b",
                &view_id,
                &view,
                &op2_id,
                &op2,
                std::slice::from_ref(&op_id),
                true,
            )
            .await
            .unwrap();
        assert_eq!(heads2, vec![op2_id.clone()]);

        let op3_id = vec![0xdd; cc_common::OPERATION_ID_LENGTH];
        let op3 = Operation {
            view_id: view_id.clone(),
            parents: vec![op2_id.clone()],
            ..Default::default()
        };
        meta.put_operation_for_project("repo-1", "proj-b", &op3_id, &op3, true)
            .await
            .unwrap();
        meta.update_op_heads_cas_for_project(
            "repo-1",
            "proj-b",
            std::slice::from_ref(&op2_id),
            &op3_id,
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            meta.get_op_heads("repo-1").await.unwrap(),
            Some(vec![op3_id])
        );

        meta.put_workspace_cas_for_project("proj-b", &ws, Some(&[]), true)
            .await
            .unwrap();
        assert_eq!(meta.list_workspaces("repo-1").await.unwrap().len(), 1);

        // Deleting repo-1 causes all 6 *_for_project writes to return RepoNotFound.
        assert!(
            store
                .delete_repository("repo-1", Some("proj-b"))
                .await
                .unwrap(),
            "{label}"
        );
        assert!(matches!(
            meta.put_view_for_project("repo-1", "proj-b", &view_id, &view)
                .await,
            Err(StoreError::RepoNotFound { .. })
        ));
        assert!(matches!(
            meta.put_operation_for_project("repo-1", "proj-b", &op_id, &op, true)
                .await,
            Err(StoreError::RepoNotFound { .. })
        ));
        assert!(matches!(
            meta.publish_operation_for_project(
                "repo-1",
                "proj-b",
                &view_id,
                &view,
                &op_id,
                &op,
                &[],
                true,
            )
            .await,
            Err(StoreError::RepoNotFound { .. })
        ));
        assert!(matches!(
            meta.update_op_heads_for_project("repo-1", "proj-b", &[], &op_id, true)
                .await,
            Err(StoreError::RepoNotFound { .. })
        ));
        assert!(matches!(
            meta.update_op_heads_cas_for_project("repo-1", "proj-b", &[], &op_id, true)
                .await,
            Err(StoreError::RepoNotFound { .. })
        ));
        assert!(matches!(
            meta.put_workspace_cas_for_project("proj-b", &ws, None, true)
                .await,
            Err(StoreError::RepoNotFound { .. })
        ));
    }
}
