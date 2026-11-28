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
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use bytes::Bytes;
use cc_common::backend::*;
use cc_common::op_store::*;
use cc_common::workspace::WorkspaceState;
use jj_commit_cloud_server::store::{
    BlobRecord, ChangePrefixResolution, CompositeStore, IdPrefixResolution, MemoryBlobStore,
    MetadataStore, PostgresConnectConfig, PostgresMetadataStore, StoreError,
};

fn unique_id(prefix: &str) -> String {
    format!("{prefix}_{:08x}", rand::random::<u32>())
}

fn random_bytes(len: usize) -> Vec<u8> {
    (0..len).map(|_| rand::random::<u8>()).collect()
}

fn pg_url_or_skip(test_name: &str) -> Option<String> {
    let require = std::env::var("JJ_CC_REQUIRE_POSTGRES")
        .ok()
        .is_some_and(|v| matches!(v.trim(), "1" | "true" | "TRUE" | "yes" | "YES"));
    let url = std::env::var("POSTGRES_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| {
            std::env::var("DATABASE_URL")
                .ok()
                .filter(|s| !s.trim().is_empty())
        });
    match url {
        Some(u) => Some(u),
        None if require => panic!(
            "{test_name}: JJ_CC_REQUIRE_POSTGRES is set, but neither POSTGRES_URL nor DATABASE_URL is configured"
        ),
        None => {
            eprintln!(
                "skipping {test_name}: set POSTGRES_URL or DATABASE_URL to run PostgreSQL integration tests"
            );
            None
        }
    }
}

async fn connect_or_skip(test_name: &str) -> Option<Arc<PostgresMetadataStore>> {
    let require = std::env::var("JJ_CC_REQUIRE_POSTGRES")
        .ok()
        .is_some_and(|v| matches!(v.trim(), "1" | "true" | "TRUE" | "yes" | "YES"));
    let url = pg_url_or_skip(test_name)?;
    let cfg = PostgresConnectConfig {
        url,
        max_conns: 16,
        auto_migrate: true,
        tx_retries: Some(Arc::new(AtomicU64::new(0))),
        ..Default::default()
    };
    match PostgresMetadataStore::connect(&cfg).await {
        Ok(meta) => Some(Arc::new(meta)),
        Err(err) if require => panic!(
            "{test_name}: JJ_CC_REQUIRE_POSTGRES is set, but PostgreSQL connect failed: {err}"
        ),
        Err(err) => {
            eprintln!("skipping {test_name}: PostgreSQL not reachable: {err}");
            None
        }
    }
}

fn make_view(head_commit: Vec<u8>) -> View {
    View {
        head_ids: vec![head_commit],
        wc_commit_ids: HashMap::new(),
        local_bookmarks: HashMap::new(),
        remote_bookmarks: HashMap::new(),
    }
}

fn make_op(view_id: &[u8], parents: Vec<Vec<u8>>, desc: &str) -> Operation {
    Operation {
        view_id: view_id.to_vec(),
        parents,
        metadata: Some(OperationMetadata {
            description: desc.to_string(),
            ..Default::default()
        }),
        commit_predecessors: Vec::new(),
        commit_predecessors_set: true,
    }
}

#[tokio::test]
async fn test_postgres_concurrent_auto_migrate_is_idempotent() {
    let Some(url) = pg_url_or_skip("test_postgres_concurrent_auto_migrate_is_idempotent") else {
        return;
    };
    let require = std::env::var("JJ_CC_REQUIRE_POSTGRES")
        .ok()
        .is_some_and(|v| matches!(v.trim(), "1" | "true" | "TRUE" | "yes" | "YES"));

    // Probe connectivity once before launching concurrent startups.
    let probe = PostgresMetadataStore::connect(&PostgresConnectConfig {
        url: url.clone(),
        max_conns: 4,
        auto_migrate: true,
        ..Default::default()
    })
    .await;
    if let Err(err) = probe {
        if require {
            panic!("PostgreSQL connect failed with JJ_CC_REQUIRE_POSTGRES set: {err}");
        }
        eprintln!("skipping concurrent auto-migrate test: {err}");
        return;
    }

    let workers = 6usize;
    let barrier = Arc::new(tokio::sync::Barrier::new(workers));
    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let url = url.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            PostgresMetadataStore::connect(&PostgresConnectConfig {
                url,
                max_conns: 4,
                auto_migrate: true,
                ..Default::default()
            })
            .await
        }));
    }
    for h in handles {
        let store = h
            .await
            .unwrap()
            .expect("concurrent auto_migrate must succeed");
        assert_eq!(store.backend_name(), "postgres");
    }
}

#[tokio::test]
async fn test_postgres_projects_repos_shadowing_and_delete_repository_cascade() {
    let Some(meta) =
        connect_or_skip("test_postgres_projects_repos_shadowing_and_delete_repository_cascade")
            .await
    else {
        return;
    };
    let store = CompositeStore::new(
        meta.clone(),
        Arc::new(MemoryBlobStore::with_location("memory://pg-cascade")),
    );

    let proj_shared = unique_id("proj_shared");
    let proj_other = unique_id("proj_other");
    let repo_del = unique_id("repo_del");
    let repo_keep = unique_id("repo_keep");
    let repo_shadow = unique_id("repo_shadow");
    let alias_shared = unique_id("alias_shared");

    meta.register_project(&proj_shared, Some(&format!("  {alias_shared}  ")))
        .await
        .unwrap();
    assert!(meta.is_project_registered(&proj_shared).await.unwrap());

    // Duplicate trimmed project alias must fail with AlreadyExists.
    let dup_alias = meta
        .register_project(&unique_id("proj_dup"), Some(&alias_shared))
        .await
        .unwrap_err();
    assert!(matches!(dup_alias, StoreError::ProjectAlreadyExists(_)));

    // Whitespace-only project names normalize to None and never collide.
    meta.register_project(&unique_id("proj_blank1"), Some("   "))
        .await
        .unwrap();
    meta.register_project(&unique_id("proj_blank2"), Some(" \t "))
        .await
        .unwrap();

    assert_eq!(
        store
            .register_repo_in_project(&repo_del, Some(&proj_shared), Some("  del-name "))
            .await
            .unwrap(),
        proj_shared
    );
    assert_eq!(
        store
            .register_repo_in_project(&repo_keep, Some(&proj_shared), Some("keep-name"))
            .await
            .unwrap(),
        proj_shared
    );

    // Cross-table namespace shadowing guards.
    let err = meta.register_project(&repo_del, None).await.unwrap_err();
    assert_eq!(tonic::Status::from(err).code(), tonic::Code::AlreadyExists);
    assert!(!meta.is_project_registered(&repo_del).await.unwrap());

    let err = meta
        .register_repo_in_project(&repo_shadow, &repo_del, None)
        .await
        .unwrap_err();
    assert_eq!(tonic::Status::from(err).code(), tonic::Code::AlreadyExists);
    assert!(
        meta.get_repo_project_id(&repo_shadow)
            .await
            .unwrap()
            .is_none()
    );

    let err = meta
        .register_repo_in_project(&proj_shared, &proj_other, None)
        .await
        .unwrap_err();
    assert_eq!(tonic::Status::from(err).code(), tonic::Code::AlreadyExists);

    let err = meta
        .register_repo_in_project(&repo_del, &proj_other, None)
        .await
        .unwrap_err();
    assert_eq!(tonic::Status::from(err).code(), tonic::Code::AlreadyExists);

    // Populate shared project objects through `repo_del` and repo-scoped state in both repos.
    let blob_id = random_bytes(20);
    store
        .put_file(&repo_del, &blob_id, Bytes::from_static(b"shared payload"))
        .await
        .unwrap();
    let sym_id = random_bytes(20);
    store
        .put_symlink(&repo_del, &sym_id, "../target")
        .await
        .unwrap();
    let tree_id = random_bytes(20);
    store
        .put_tree(
            &repo_del,
            &tree_id,
            &[TreeEntry {
                name: "file.txt".to_string(),
                value: Some(TreeValue {
                    value: Some(tree_value::Value::File(File {
                        id: blob_id.clone(),
                        executable: false,
                        copy_id: Vec::new(),
                        size_bytes: None,
                    })),
                }),
            }],
        )
        .await
        .unwrap();
    let commit_id = random_bytes(20);
    let commit = Commit {
        commit_id: commit_id.clone(),
        change_id: random_bytes(16),
        parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![tree_id.clone()],
        description: "shared commit\n".to_string(),
        author: None,
        committer: None,
        predecessors: vec![],
        conflict_labels: vec![],
        secure_sig: None,
        raw_git_commit: vec![],
        external_ids: Default::default(),
    };
    store
        .put_commit(&repo_del, &commit_id, &commit)
        .await
        .unwrap();

    let view_del = random_bytes(16);
    let op_del = random_bytes(16);
    store
        .publish_operation(
            &repo_del,
            &view_del,
            &make_view(commit_id.clone()),
            &op_del,
            &make_op(
                &view_del,
                vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                "del op",
            ),
            &[cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
        )
        .await
        .unwrap();

    let view_keep = random_bytes(16);
    let op_keep = random_bytes(16);
    store
        .publish_operation(
            &repo_keep,
            &view_keep,
            &make_view(commit_id.clone()),
            &op_keep,
            &make_op(
                &view_keep,
                vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                "keep op",
            ),
            &[cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
        )
        .await
        .unwrap();

    #[allow(deprecated)]
    let ws_del = WorkspaceState {
        repo_id: repo_del.clone(),
        user: "dev@example.com".to_string(),
        workspace_name: "default".to_string(),
        commit_id: commit_id.clone(),
        operation_id: op_del.clone(),
        tree_id: tree_id.clone(),
        sparse_patterns: vec!["".to_string()],
        tree_ids: vec![tree_id.clone()],
        conflict_labels: vec![],
        sparse_patterns_set: true,
    };
    store.put_workspace(&ws_del).await.unwrap();
    let mut ws_keep = ws_del.clone();
    ws_keep.repo_id = repo_keep.clone();
    ws_keep.operation_id = op_keep.clone();
    store.put_workspace(&ws_keep).await.unwrap();

    // Deleting with mismatched expected_project_id must fail and leave `repo_del` intact.
    let mismatch_err = store
        .delete_repository(&repo_del, Some(&proj_other))
        .await
        .unwrap_err();
    assert!(matches!(mismatch_err, StoreError::FailedPrecondition(_)));
    assert_eq!(
        meta.get_repo_project_id(&repo_del)
            .await
            .unwrap()
            .as_deref(),
        Some(proj_shared.as_str())
    );

    // Deleting with matching expected_project_id cascades repo-scoped tables and preserves project.
    assert!(
        store
            .delete_repository(&repo_del, Some(&proj_shared))
            .await
            .unwrap()
    );
    assert!(
        !store
            .delete_repository(&repo_del, Some(&proj_shared))
            .await
            .unwrap()
    );

    // Repo-scoped records for `repo_del` are gone, and direct writes return RepoNotFound.
    assert!(meta.get_repo_project_id(&repo_del).await.unwrap().is_none());
    assert!(meta.get_op_heads(&repo_del).await.unwrap().is_none());
    assert!(meta.get_view(&repo_del, &view_del).await.unwrap().is_none());
    assert!(
        meta.get_operation(&repo_del, &op_del)
            .await
            .unwrap()
            .is_none()
    );
    assert!(meta.list_workspaces(&repo_del).await.unwrap().is_empty());
    assert!(matches!(
        meta.put_view(&repo_del, &view_del, &make_view(commit_id.clone()))
            .await,
        Err(StoreError::RepoNotFound { .. })
    ));
    assert!(matches!(
        meta.put_operation(
            &repo_del,
            &op_del,
            &make_op(
                &view_del,
                vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
                "after delete",
            ),
        )
        .await,
        Err(StoreError::RepoNotFound { .. })
    ));
    for expected in [None, Some(&[][..]), Some(op_del.as_slice())] {
        assert!(
            matches!(
                meta.put_workspace_cas(&ws_del, expected).await,
                Err(StoreError::RepoNotFound { .. })
            ),
            "put_workspace_cas on deleted repo with expected={expected:?} must return RepoNotFound"
        );
    }

    // Project-scoped writes against an unregistered project must map FK violations to
    // RepoNotFound, and a failed put_blob_record must roll back its blobs row atomically.
    let missing_project = unique_id("missing_proj");
    let unreg_commit_id = random_bytes(20);
    let mut unreg_commit = commit.clone();
    unreg_commit.commit_id = unreg_commit_id.clone();
    assert!(matches!(
        meta.put_commit(&missing_project, &unreg_commit_id, &unreg_commit, 1)
            .await,
        Err(StoreError::RepoNotFound { .. })
    ));
    assert!(matches!(
        meta.put_tree(&missing_project, &random_bytes(20), &[])
            .await,
        Err(StoreError::RepoNotFound { .. })
    ));
    assert!(matches!(
        meta.put_symlink(&missing_project, &random_bytes(20), "../missing")
            .await,
        Err(StoreError::RepoNotFound { .. })
    ));
    let unreg_blob_id = random_bytes(20);
    let unreg_blob = BlobRecord {
        blob_id: unreg_blob_id.clone(),
        content_size: 4,
        inline_content: Some(Bytes::from_static(b"test")),
        primary_location: None,
        replicated_locations: Vec::new(),
    };
    assert!(matches!(
        meta.put_blob_record(&missing_project, &unreg_blob).await,
        Err(StoreError::RepoNotFound { .. })
    ));
    assert!(
        meta.get_global_blob_record(&unreg_blob_id)
            .await
            .unwrap()
            .is_none(),
        "failed put_blob_record on unregistered project must roll back blobs row"
    );

    // Shared project objects and sibling `repo_keep` survive untouched.
    assert!(meta.is_project_registered(&proj_shared).await.unwrap());
    assert!(
        meta.get_commit(&proj_shared, &commit_id)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        meta.get_tree(&proj_shared, &tree_id)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        meta.get_symlink(&proj_shared, &sym_id)
            .await
            .unwrap()
            .as_deref(),
        Some("../target")
    );
    assert!(
        meta.get_blob_record(&proj_shared, &blob_id)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        meta.get_op_heads(&repo_keep).await.unwrap(),
        Some(vec![op_keep.clone()])
    );
    assert!(
        meta.get_view(&repo_keep, &view_keep)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        meta.get_operation(&repo_keep, &op_keep)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        meta.list_workspaces(&repo_keep).await.unwrap(),
        vec![ws_keep]
    );
}

#[tokio::test]
async fn test_postgres_commits_watermarks_prefixes_blobs_and_symlinks() {
    let Some(meta) =
        connect_or_skip("test_postgres_commits_watermarks_prefixes_blobs_and_symlinks").await
    else {
        return;
    };
    let blobs = Arc::new(MemoryBlobStore::with_location("gs://pg-bucket-uc1"));
    let store = CompositeStore::new(meta.clone(), blobs);

    let repo_a = unique_id("repo_a");
    let repo_b = unique_id("repo_b");
    store.register_repo(&repo_a, Some("alpha")).await.unwrap();
    store.register_repo(&repo_b, Some("beta")).await.unwrap();

    // Blobs: project isolation, sizes batch, global lookup, and replica location append.
    let file_id = random_bytes(20);
    let payload = Bytes::from_static(b"postgres blob payload");
    store
        .put_file(&repo_a, &file_id, payload.clone())
        .await
        .unwrap();
    assert_eq!(
        store
            .get_file_range(&repo_a, &file_id, 0, Some(8))
            .await
            .unwrap()
            .as_ref(),
        b"postgres"
    );
    assert!(matches!(
        store.get_file_range(&repo_b, &file_id, 0, None).await,
        Err(StoreError::ObjectNotFound { .. })
    ));

    meta.add_blob_replica_location(&file_id, "gs://pg-bucket-ew1")
        .await
        .unwrap();
    meta.add_blob_replica_location(&file_id, "gs://pg-bucket-ew1")
        .await
        .unwrap();
    let global = meta
        .get_global_blob_record(&file_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        global.primary_location.as_deref(),
        Some("gs://pg-bucket-uc1")
    );
    assert_eq!(
        global.replicated_locations,
        vec![
            "gs://pg-bucket-uc1".to_string(),
            "gs://pg-bucket-ew1".to_string()
        ]
    );
    let sizes = meta
        .get_blob_sizes_batch(&repo_a, &[file_id.clone(), random_bytes(20)])
        .await
        .unwrap();
    assert_eq!(sizes.get(&file_id), Some(&(payload.len() as u64)));
    assert_eq!(sizes.len(), 1);

    // Symlinks: insert-only & batched existence check.
    let sym_id = random_bytes(20);
    let missing_sym = random_bytes(20);
    store
        .put_symlink(&repo_a, &sym_id, "../original")
        .await
        .unwrap();
    meta.put_symlink(&repo_a, &sym_id, "../forged")
        .await
        .unwrap();
    assert_eq!(
        store.get_symlink(&repo_a, &sym_id).await.unwrap(),
        "../original"
    );
    let found_syms = meta
        .find_existing_symlinks(&repo_a, &[sym_id.clone(), missing_sym.clone()])
        .await
        .unwrap();
    assert!(found_syms.contains(&sym_id));
    assert!(!found_syms.contains(&missing_sym));

    // Commits: generations batch, insert-only immutability, prefixes, and incremental seq sync.
    let mut c1_id = vec![0u8; 20];
    c1_id[0] = 0x10;
    c1_id[19] = 0x01;
    let c1_change = vec![0x91; 16];
    let c1 = Commit {
        commit_id: c1_id.clone(),
        change_id: c1_change.clone(),
        parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
        description: "first pg commit\n".to_string(),
        author: None,
        committer: None,
        predecessors: vec![],
        conflict_labels: vec![],
        secure_sig: None,
        raw_git_commit: vec![],
        external_ids: [("git-sha1".to_string(), c1_id.clone())]
            .into_iter()
            .collect(),
    };
    store.put_commit(&repo_a, &c1_id, &c1).await.unwrap();

    // Immutable insert-only check.
    let mut forged = c1.clone();
    forged.description = "forged\n".to_string();
    store.put_commit(&repo_a, &c1_id, &forged).await.unwrap();
    assert_eq!(
        store.get_commit(&repo_a, &c1_id).await.unwrap().description,
        "first pg commit\n"
    );

    let mut c2_id = vec![0u8; 20];
    c2_id[0] = 0x1f;
    c2_id[19] = 0x02;
    let mut c2 = c1.clone();
    c2.commit_id = c2_id.clone();
    c2.change_id = vec![0xa2; 16];
    c2.parent_commit_ids = vec![c1_id.clone()];
    c2.description = "second pg commit\n".to_string();
    store.put_commit(&repo_a, &c2_id, &c2).await.unwrap();

    let gens = meta
        .get_commit_generations(&repo_a, &[c1_id.clone(), c2_id.clone(), random_bytes(20)])
        .await
        .unwrap();
    assert_eq!(gens.get(&c1_id), Some(&1));
    assert_eq!(gens.get(&c2_id), Some(&2));
    assert_eq!(gens.len(), 2);

    // Prefix resolution (commit hex & change reverse-hex: 0x91 -> "qy").
    assert_eq!(
        store.resolve_commit_prefix(&repo_a, "1").await.unwrap(),
        IdPrefixResolution::AmbiguousMatch
    );
    assert_eq!(
        store.resolve_commit_prefix(&repo_a, "1000").await.unwrap(),
        IdPrefixResolution::SingleMatch(c1_id.clone())
    );
    assert_eq!(
        store
            .resolve_change_prefix(&repo_a, "qyqy", 10)
            .await
            .unwrap(),
        ChangePrefixResolution::SingleMatch {
            change_id: c1_change,
            commit_ids: vec![c1_id.clone()],
        }
    );

    // Full scan with mid-scan commit insertion + incremental `since_token` sync.
    let (p1, tok1, sync1) = store
        .list_commits_with_sync(&repo_a, &[], 1, &[])
        .await
        .unwrap();
    assert_eq!(p1, vec![c1_id.clone()]);
    assert!(!tok1.is_empty());
    assert_eq!(sync1.len(), 8);

    // Insert commit `0x05...` sorting before `c1_id` during the scan.
    let mut c_low_id = vec![0u8; 20];
    c_low_id[0] = 0x05;
    let mut c_low = c1.clone();
    c_low.commit_id = c_low_id.clone();
    c_low.change_id = vec![0xb3; 16];
    store.put_commit(&repo_a, &c_low_id, &c_low).await.unwrap();

    let (p2, tok2, sync2) = store
        .list_commits_with_sync(&repo_a, &tok1, 1, &[])
        .await
        .unwrap();
    assert_eq!(p2, vec![c2_id.clone()]);
    assert!(tok2.is_empty());
    assert_eq!(sync2, sync1);

    let (delta, delta_next, sync3) = store
        .list_commits_with_sync(&repo_a, &[], 10, &sync2)
        .await
        .unwrap();
    assert_eq!(delta, vec![c_low_id]);
    assert!(delta_next.is_empty());
    assert_ne!(sync3, sync2);

    let (empty_delta, _, _) = store
        .list_commits_with_sync(&repo_a, &[], 10, &sync3)
        .await
        .unwrap();
    assert!(empty_delta.is_empty());
}

#[tokio::test]
async fn test_postgres_wide_trees_and_collate_c_utf8_byte_order() {
    let Some(meta) =
        connect_or_skip("test_postgres_wide_trees_and_collate_c_utf8_byte_order").await
    else {
        return;
    };
    let store = CompositeStore::new(meta.clone(), Arc::new(MemoryBlobStore::new()));

    let repo_id = unique_id("wide_tree_repo");
    store.register_repo(&repo_id, None).await.unwrap();
    let file_id = random_bytes(20);
    let payload = Bytes::from_static(b"leaf");
    store
        .put_file(&repo_id, &file_id, payload.clone())
        .await
        .unwrap();

    // Exercise COLLATE "C" UTF-8 byte order explicitly:
    // ASCII byte values: '-' (0x2d) < '.' (0x2e) < 'A' (0x41) < 'Z' (0x5a) < '_' (0x5f) < 'a' (0x61).
    let tricky_names = [
        "foo.rs", "foo-bar", "foo", "Foo", "foo_bar", "FOO", "foo.a", "foo-a",
    ];
    let mut expected_order: Vec<String> = tricky_names.iter().map(|s| (*s).to_string()).collect();
    expected_order.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    assert_eq!(
        expected_order,
        vec![
            "FOO", "Foo", "foo", "foo-a", "foo-bar", "foo.a", "foo.rs", "foo_bar"
        ]
    );

    let tricky_entries: Vec<TreeEntry> = tricky_names
        .iter()
        .map(|name| TreeEntry {
            name: (*name).to_string(),
            value: Some(TreeValue {
                value: Some(tree_value::Value::File(File {
                    id: file_id.clone(),
                    executable: false,
                    copy_id: Vec::new(),
                    size_bytes: None,
                })),
            }),
        })
        .collect();
    let tricky_tree_id = random_bytes(20);
    store
        .put_tree(&repo_id, &tricky_tree_id, &tricky_entries)
        .await
        .unwrap();

    let fetched = store.get_tree(&repo_id, &tricky_tree_id).await.unwrap();
    let fetched_names: Vec<String> = fetched.iter().map(|e| e.name.clone()).collect();
    assert_eq!(fetched_names, expected_order);

    // Paginate through the tricky names 2 at a time to validate `name COLLATE "C" > $3`.
    let mut paged_names = Vec::new();
    let mut cursor = String::new();
    loop {
        let (page, next) = store
            .get_tree_page(&repo_id, &tricky_tree_id, &cursor, 2)
            .await
            .unwrap();
        paged_names.extend(page.into_iter().map(|e| e.name));
        if next.is_empty() {
            break;
        }
        cursor = next;
    }
    assert_eq!(paged_names, expected_order);

    // Wide directory: >4,500 entries chunked via UNNEST(1,000) with ready=TRUE immutability.
    let entry_count = 4_600usize;
    let wide_entries: Vec<TreeEntry> = (0..entry_count)
        .map(|i| TreeEntry {
            name: format!("entry_{i:05}.txt"),
            value: Some(TreeValue {
                value: Some(tree_value::Value::File(File {
                    id: file_id.clone(),
                    executable: false,
                    copy_id: Vec::new(),
                    size_bytes: Some(payload.len() as u64),
                })),
            }),
        })
        .collect();
    let wide_tree_id = random_bytes(20);
    store
        .put_tree(&repo_id, &wide_tree_id, &wide_entries)
        .await
        .unwrap();

    let found = meta
        .find_existing_trees(&repo_id, &[wide_tree_id.clone(), random_bytes(20)])
        .await
        .unwrap();
    assert!(found.contains(&wide_tree_id));
    assert_eq!(found.len(), 1);

    let wide_fetched = store.get_tree(&repo_id, &wide_tree_id).await.unwrap();
    assert_eq!(wide_fetched.len(), entry_count);
    assert_eq!(wide_fetched[0].name, "entry_00000.txt");
    assert_eq!(wide_fetched[entry_count - 1].name, "entry_04599.txt");

    // Re-uploading altered entries for an existing ready tree is ignored (immutable).
    let mut altered = wide_entries.clone();
    altered[0].name = "entry_00000_mutated.txt".to_string();
    meta.put_tree(&repo_id, &wide_tree_id, &altered)
        .await
        .unwrap();
    let after = store.get_tree(&repo_id, &wide_tree_id).await.unwrap();
    assert_eq!(after[0].name, "entry_00000.txt");
}

#[tokio::test]
async fn test_postgres_operations_publish_atomic_and_concurrent_op_heads() {
    let Some(meta) =
        connect_or_skip("test_postgres_operations_publish_atomic_and_concurrent_op_heads").await
    else {
        return;
    };
    let store = Arc::new(CompositeStore::new(
        meta.clone(),
        Arc::new(MemoryBlobStore::new()),
    ));

    // `published` flag semantics across put_operation, publish_operation_atomic,
    // compare_and_swap_op_heads, and update_op_heads.
    let repo_pub = unique_id("repo_pub");
    store.register_repo(&repo_pub, None).await.unwrap();
    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let view_id = random_bytes(16);
    let view = make_view(cc_common::ROOT_COMMIT_ID_BYTES.to_vec());
    meta.put_view(&repo_pub, &view_id, &view).await.unwrap();

    let op_a_id = hex::decode("a7b10000000000000000000000000001").unwrap();
    let op_a = make_op(&view_id, vec![root_op.clone()], "op_a");
    meta.put_operation(&repo_pub, &op_a_id, &op_a)
        .await
        .unwrap();

    let op_sib_id = hex::decode("a7b20000000000000000000000000002").unwrap();
    let op_sib = make_op(&view_id, vec![root_op.clone()], "op_sib");
    let heads = meta
        .publish_operation_atomic(
            &repo_pub,
            &view_id,
            &view,
            &op_sib_id,
            &op_sib,
            std::slice::from_ref(&root_op),
        )
        .await
        .unwrap();
    assert_eq!(heads, vec![op_sib_id.clone()]);

    // Prefix resolution on operations (odd and even nibbles).
    assert_eq!(
        store
            .resolve_operation_prefix(&repo_pub, "a7b")
            .await
            .unwrap(),
        IdPrefixResolution::AmbiguousMatch
    );
    assert_eq!(
        store
            .resolve_operation_prefix(&repo_pub, "a7b1")
            .await
            .unwrap(),
        IdPrefixResolution::SingleMatch(op_a_id.clone())
    );

    // Pre-written unpublished `op_a` is added on first publish.
    let heads_after_a = meta
        .publish_operation_atomic(
            &repo_pub,
            &view_id,
            &view,
            &op_a_id,
            &op_a,
            std::slice::from_ref(&root_op),
        )
        .await
        .unwrap();
    assert_eq!(heads_after_a, vec![op_sib_id.clone(), op_a_id.clone()]);

    // CAS fails on mismatched expected set and succeeds on matching set (order-insensitive).
    let op_merge_id = random_bytes(16);
    let op_merge = make_op(&view_id, vec![op_sib_id.clone(), op_a_id.clone()], "merge");
    meta.put_operation(&repo_pub, &op_merge_id, &op_merge)
        .await
        .unwrap();
    let cas_err = meta
        .compare_and_swap_op_heads(&repo_pub, std::slice::from_ref(&op_sib_id), &op_merge_id)
        .await
        .unwrap_err();
    assert!(matches!(cas_err, StoreError::CasConflict(_)));

    meta.compare_and_swap_op_heads(
        &repo_pub,
        &[op_a_id.clone(), op_sib_id.clone()],
        &op_merge_id,
    )
    .await
    .unwrap();
    assert_eq!(
        meta.get_op_heads(&repo_pub).await.unwrap(),
        Some(vec![op_merge_id.clone()])
    );

    // Retrying publish of superseded `op_a` (even after put_operation) must NOT resurrect it.
    meta.put_operation(&repo_pub, &op_a_id, &op_a)
        .await
        .unwrap();
    let retry_a = meta
        .publish_operation_atomic(
            &repo_pub,
            &view_id,
            &view,
            &op_a_id,
            &op_a,
            std::slice::from_ref(&root_op),
        )
        .await
        .unwrap();
    assert_eq!(retry_a, vec![op_merge_id.clone()]);

    // Concurrent `update_op_heads` race preserves all sibling heads.
    let repo_race = unique_id("repo_race");
    store.register_repo(&repo_race, None).await.unwrap();
    let race_view_id = random_bytes(16);
    store
        .put_view(
            &repo_race,
            &race_view_id,
            &make_view(cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
        )
        .await
        .unwrap();

    let worker_count = 10usize;
    let mut expected_ops = Vec::with_capacity(worker_count);
    for i in 0..worker_count {
        let mut op_id = vec![0u8; 16];
        op_id[0] = 0x80;
        op_id[15] = (i + 1) as u8;
        store
            .put_operation(
                &repo_race,
                &op_id,
                &make_op(&race_view_id, vec![root_op.clone()], "race"),
            )
            .await
            .unwrap();
        expected_ops.push(op_id);
    }

    let barrier = Arc::new(tokio::sync::Barrier::new(worker_count));
    let mut handles = Vec::with_capacity(worker_count);
    for op_id in expected_ops.clone() {
        let store_c = Arc::clone(&store);
        let barrier_c = Arc::clone(&barrier);
        let repo_c = repo_race.clone();
        let root_c = root_op.clone();
        handles.push(tokio::spawn(async move {
            barrier_c.wait().await;
            store_c
                .update_op_heads(&repo_c, &[root_c], &op_id)
                .await
                .unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let final_heads: HashSet<Vec<u8>> = store
        .get_op_heads(&repo_race)
        .await
        .unwrap()
        .into_iter()
        .collect();
    let expected_set: HashSet<Vec<u8>> = expected_ops.iter().cloned().collect();
    assert_eq!(final_heads, expected_set);
}

#[tokio::test]
async fn test_postgres_workspaces_cas_collation_and_delete() {
    let Some(meta) = connect_or_skip("test_postgres_workspaces_cas_collation_and_delete").await
    else {
        return;
    };
    let store = Arc::new(CompositeStore::new(
        meta.clone(),
        Arc::new(MemoryBlobStore::new()),
    ));

    let repo_id = unique_id("repo_ws");
    store.register_repo(&repo_id, None).await.unwrap();

    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let root_commit = cc_common::ROOT_COMMIT_ID_BYTES.to_vec();
    let view_id = random_bytes(16);
    store
        .put_view(&repo_id, &view_id, &make_view(root_commit))
        .await
        .unwrap();
    let op1 = vec![0x11; 16];
    let op2 = vec![0x22; 16];
    let op3 = vec![0x33; 16];
    for (op_id, desc) in [(&op1, "ws-op1"), (&op2, "ws-op2"), (&op3, "ws-op3")] {
        store
            .put_operation(
                &repo_id,
                op_id,
                &make_op(&view_id, vec![root_op.clone()], desc),
            )
            .await
            .unwrap();
    }

    #[allow(deprecated)]
    let ws1 = WorkspaceState {
        repo_id: repo_id.clone(),
        user: "alice@example.com".to_string(),
        workspace_name: "ws-main".to_string(),
        commit_id: vec![0x01; 20],
        operation_id: op1.clone(),
        tree_id: vec![0xaa; 20],
        sparse_patterns: vec!["crates/core".to_string(), "crates/server".to_string()],
        tree_ids: vec![vec![0xaa; 20], vec![0xbb; 20], vec![0xcc; 20]],
        conflict_labels: vec!["left".into(), "base".into(), "right".into()],
        sparse_patterns_set: true,
    };

    // Updating non-existent workspace with non-empty expected_operation_id fails with CasConflict.
    let err = store.put_workspace_cas(&ws1, Some(&op1)).await.unwrap_err();
    assert!(matches!(err, StoreError::CasConflict(_)));

    // Create-only CAS (`Some(b"")`) succeeds once, is idempotent on identical payload, and
    // rejects conflicting concurrent creates.
    store.put_workspace_cas(&ws1, Some(&[])).await.unwrap();
    store.put_workspace_cas(&ws1, Some(&[])).await.unwrap();

    let mut ws1_conflict = ws1.clone();
    ws1_conflict.operation_id = op2.clone();
    let err = store
        .put_workspace_cas(&ws1_conflict, Some(&[]))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::CasConflict(_)));

    // Concurrent create-only CAS race on a fresh workspace allows only one winner.
    let mut race_a = ws1.clone();
    race_a.workspace_name = "ws-race".to_string();
    race_a.operation_id = op1.clone();
    let mut race_b = race_a.clone();
    race_b.operation_id = op2.clone();

    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let s1 = Arc::clone(&store);
    let b1 = Arc::clone(&barrier);
    let h1 = tokio::spawn(async move {
        b1.wait().await;
        s1.put_workspace_cas(&race_a, Some(&[])).await
    });
    let s2 = Arc::clone(&store);
    let b2 = Arc::clone(&barrier);
    let h2 = tokio::spawn(async move {
        b2.wait().await;
        s2.put_workspace_cas(&race_b, Some(&[])).await
    });
    let r1 = h1.await.unwrap();
    let r2 = h2.await.unwrap();
    let successes = [r1.is_ok(), r2.is_ok()].into_iter().filter(|&x| x).count();
    let conflicts = [
        matches!(r1, Err(StoreError::CasConflict(_))),
        matches!(r2, Err(StoreError::CasConflict(_))),
    ]
    .into_iter()
    .filter(|&x| x)
    .count();
    assert_eq!(successes, 1, "expected exactly 1 create-only winner");
    assert_eq!(conflicts, 1, "expected exactly 1 create-only conflict");

    // Advance `ws-main` with matching expected_operation_id, fail on stale expected_operation_id.
    let mut ws1_v2 = ws1.clone();
    ws1_v2.operation_id = op2.clone();
    ws1_v2.commit_id = vec![0x02; 20];
    store.put_workspace_cas(&ws1_v2, Some(&op1)).await.unwrap();

    let mut ws1_v3 = ws1_v2.clone();
    ws1_v3.operation_id = op3.clone();
    let stale_err = store
        .put_workspace_cas(&ws1_v3, Some(&op1))
        .await
        .unwrap_err();
    assert!(matches!(stale_err, StoreError::CasConflict(_)));

    assert_eq!(
        store
            .get_workspace(&repo_id, "alice@example.com", "ws-main")
            .await
            .unwrap(),
        Some(ws1_v2)
    );

    // Add workspaces testing COLLATE "C" ordering across (user_email, workspace_name).
    for (user, ws_name) in [
        ("Bob@example.com", "z"),
        ("alice@example.com", "ws.dot"),
        ("alice@example.com", "ws-dash"),
    ] {
        let mut w = ws1.clone();
        w.user = user.to_string();
        w.workspace_name = ws_name.to_string();
        store.put_workspace(&w).await.unwrap();
    }
    let listed = store.list_workspaces(&repo_id).await.unwrap();
    let keys: Vec<(String, String)> = listed
        .iter()
        .map(|w| (w.user.clone(), w.workspace_name.clone()))
        .collect();
    assert_eq!(
        keys,
        vec![
            ("Bob@example.com".to_string(), "z".to_string()),
            ("alice@example.com".to_string(), "ws-dash".to_string()),
            ("alice@example.com".to_string(), "ws-main".to_string()),
            ("alice@example.com".to_string(), "ws-race".to_string()),
            ("alice@example.com".to_string(), "ws.dot".to_string()),
        ]
    );

    // Delete workspace returns true once, false thereafter.
    assert!(
        store
            .delete_workspace(&repo_id, "alice@example.com", "ws-main")
            .await
            .unwrap()
    );
    assert!(
        !store
            .delete_workspace(&repo_id, "alice@example.com", "ws-main")
            .await
            .unwrap()
    );
    assert!(
        store
            .get_workspace(&repo_id, "alice@example.com", "ws-main")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn test_postgres_list_workspaces_page_keyset_pagination() {
    let Some(meta) = connect_or_skip("test_postgres_list_workspaces_page_keyset_pagination").await
    else {
        return;
    };
    let store = CompositeStore::new(meta.clone(), Arc::new(MemoryBlobStore::new()));

    let repo_id = unique_id("repo_ws_page");
    store.register_repo(&repo_id, None).await.unwrap();

    // Missing repo and zero limit return empty pages.
    assert_eq!(
        meta.list_workspaces_page(&unique_id("missing_repo"), None, 10, None)
            .await
            .unwrap(),
        (Vec::new(), false)
    );
    assert_eq!(
        meta.list_workspaces_page(&repo_id, None, 0, None)
            .await
            .unwrap(),
        (Vec::new(), false)
    );

    // Populate keys covering empty user, ASCII case/punctuation `COLLATE "C"` order, and
    // multiple workspaces per user.
    let entries = [
        ("alice@example.com", "ws.dot"),
        ("", "anon-b"),
        ("Bob@example.com", "z"),
        ("alice@example.com", "ws-dash"),
        ("", "anon-a"),
        ("alice@example.com", "ws_under"),
        ("Bob@example.com", "A"),
    ];
    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let root_commit = cc_common::ROOT_COMMIT_ID_BYTES.to_vec();
    let view_id = random_bytes(16);
    store
        .put_view(&repo_id, &view_id, &make_view(root_commit))
        .await
        .unwrap();
    for (idx, (user, ws_name)) in entries.iter().enumerate() {
        let op_id = vec![idx as u8 + 0x10; 16];
        store
            .put_operation(
                &repo_id,
                &op_id,
                &make_op(
                    &view_id,
                    vec![root_op.clone()],
                    &format!("ws-page-op-{idx}"),
                ),
            )
            .await
            .unwrap();
        #[allow(deprecated)]
        let ws = WorkspaceState {
            repo_id: repo_id.clone(),
            user: (*user).to_string(),
            workspace_name: (*ws_name).to_string(),
            commit_id: vec![idx as u8 + 1; 20],
            operation_id: op_id,
            tree_id: vec![idx as u8 + 0x40; 20],
            sparse_patterns: vec![format!("dir/{idx}")],
            tree_ids: vec![vec![idx as u8 + 0x40; 20]],
            conflict_labels: vec![],
            sparse_patterns_set: true,
        };
        store.put_workspace(&ws).await.unwrap();
    }

    let expected_keys = vec![
        ("".to_string(), "anon-a".to_string()),
        ("".to_string(), "anon-b".to_string()),
        ("Bob@example.com".to_string(), "A".to_string()),
        ("Bob@example.com".to_string(), "z".to_string()),
        ("alice@example.com".to_string(), "ws-dash".to_string()),
        ("alice@example.com".to_string(), "ws.dot".to_string()),
        ("alice@example.com".to_string(), "ws_under".to_string()),
    ];

    let all = meta.list_workspaces(&repo_id).await.unwrap();
    let all_keys: Vec<(String, String)> = all
        .iter()
        .map(|w| (w.user.clone(), w.workspace_name.clone()))
        .collect();
    assert_eq!(all_keys, expected_keys);

    // Paginate through the full set with page sizes 1, 2, 3, and 7 (exact boundary).
    for page_size in [1usize, 2, 3, 7] {
        let mut collected = Vec::new();
        let mut cursor: Option<(String, String)> = None;
        loop {
            let (page, has_more) = meta
                .list_workspaces_page(
                    &repo_id,
                    cursor.as_ref().map(|(u, w)| (u.as_str(), w.as_str())),
                    page_size,
                    None,
                )
                .await
                .unwrap();
            assert!(page.len() <= page_size);
            if let Some(last) = page.last() {
                cursor = Some((last.user.clone(), last.workspace_name.clone()));
            }
            collected.extend(page);
            if !has_more {
                break;
            }
        }
        assert_eq!(
            collected, all,
            "keyset pagination mismatch for page_size={page_size}"
        );
    }

    // Cursor with empty user advances cleanly past `("", "anon-a")`, and past-the-end returns empty.
    let (after_anon_a, has_more_after_anon_a) = meta
        .list_workspaces_page(&repo_id, Some(("", "anon-a")), 2, None)
        .await
        .unwrap();
    assert!(has_more_after_anon_a);
    let after_anon_a_keys: Vec<(&str, &str)> = after_anon_a
        .iter()
        .map(|w| (w.user.as_str(), w.workspace_name.as_str()))
        .collect();
    assert_eq!(
        after_anon_a_keys,
        vec![("", "anon-b"), ("Bob@example.com", "A")]
    );

    assert_eq!(
        meta.list_workspaces_page(&repo_id, Some(("alice@example.com", "ws_under")), 5, None)
            .await
            .unwrap(),
        (Vec::new(), false)
    );
    assert_eq!(
        meta.list_workspaces_page(&repo_id, Some(("alice@example.com", "ws-dash")), 0, None)
            .await
            .unwrap(),
        (Vec::new(), false)
    );

    // Byte-capped pagination returns at least 1 workspace and flags `has_more = true`.
    let (byte_capped, has_more_capped) = meta
        .list_workspaces_page(&repo_id, None, 10, Some(1))
        .await
        .unwrap();
    assert_eq!(byte_capped.len(), 1);
    assert!(has_more_capped);
}

#[tokio::test]
async fn test_postgres_commit_watermark_mvcc_overlapping_transactions() {
    use prost::Message as _;

    let Some(meta) =
        connect_or_skip("test_postgres_commit_watermark_mvcc_overlapping_transactions").await
    else {
        return;
    };
    let store = Arc::new(CompositeStore::new(
        meta.clone(),
        Arc::new(MemoryBlobStore::new()),
    ));
    let repo_id = unique_id("repo_mvcc");
    let other_repo_id = unique_id("repo_other_tx");
    store.register_repo(&repo_id, None).await.unwrap();
    store.register_repo(&other_repo_id, None).await.unwrap();

    let make_commit = |cid: &[u8], desc: &str| Commit {
        commit_id: cid.to_vec(),
        change_id: random_bytes(16),
        parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
        description: desc.to_string(),
        author: None,
        committer: None,
        predecessors: vec![],
        conflict_labels: vec![],
        secure_sig: None,
        raw_git_commit: vec![],
        external_ids: Default::default(),
    };

    // Seed an initial committed commit `c0` and capture a baseline watermark.
    let c0_id = vec![0x10; 20];
    store
        .put_commit(&repo_id, &c0_id, &make_commit(&c0_id, "c0\n"))
        .await
        .unwrap();
    let baseline_token = meta.current_commit_watermark(&repo_id).await.unwrap();

    // An unrelated long-running transaction on `other_repo_id` does NOT stall
    // incremental commit sync on `repo_id`.
    let mut other_client = meta
        .checkout_client_for_test()
        .await
        .expect("pooled postgres client for unrelated tx");
    let other_tx = other_client.transaction().await.unwrap();
    let other_seq: i64 = other_tx
        .query_one(
            "UPDATE projects SET commit_seq = commit_seq + 1
             WHERE project_id = $1 RETURNING commit_seq",
            &[&other_repo_id],
        )
        .await
        .unwrap()
        .get(0);
    let other_c_id = vec![0xee; 20];
    let other_c = make_commit(&other_c_id, "unrelated uncommitted\n");
    other_tx
        .execute(
            "INSERT INTO commits (project_id, commit_id, change_id, generation, seq, proto_bytes)
             VALUES ($1, $2, $3, 1, $4, $5)",
            &[
                &other_repo_id,
                &other_c_id,
                &other_c.change_id,
                &other_seq,
                &other_c.encode_to_vec(),
            ],
        )
        .await
        .unwrap();

    // Hold an explicit in-flight `put_commit` transaction T1 (`c1`) on `repo_id` via
    // `projects.commit_seq`, launch a concurrent `store.put_commit(c2)` (T2), and prove:
    // - T2 serializes behind T1 on the per-project sequence lock (never commits ahead of T1).
    // - Full-scan and incremental watermarks taken while T1 is open do NOT advance past T1.
    // - Once T1 commits, BOTH `c1` and `c2` are immediately visible (even while `other_tx` stays
    //   open) with strictly increasing `seq` and zero skipped commits.
    let mut raw_client = meta
        .checkout_client_for_test()
        .await
        .expect("pooled postgres client for MVCC test");
    let tx1 = raw_client.transaction().await.unwrap();
    let c1_seq: i64 = tx1
        .query_one(
            "UPDATE projects SET commit_seq = commit_seq + 1
             WHERE project_id = $1 RETURNING commit_seq",
            &[&repo_id],
        )
        .await
        .unwrap()
        .get(0);
    let c1_id = vec![0x11; 20];
    let c1 = make_commit(&c1_id, "c1 uncommitted\n");
    tx1.execute(
        "INSERT INTO commits (project_id, commit_id, change_id, generation, seq, proto_bytes)
         VALUES ($1, $2, $3, 1, $4, $5)",
        &[
            &repo_id,
            &c1_id,
            &c1.change_id,
            &c1_seq,
            &c1.encode_to_vec(),
        ],
    )
    .await
    .unwrap();

    // Spawn T2 (`c2`) concurrently while T1 holds the per-project commit_seq lock.
    let c2_id = vec![0x22; 20];
    let c2 = make_commit(&c2_id, "c2 concurrent\n");
    let store_t2 = Arc::clone(&store);
    let repo_t2 = repo_id.clone();
    let c2_id_t2 = c2_id.clone();
    let t2_handle =
        tokio::spawn(async move { store_t2.put_commit(&repo_t2, &c2_id_t2, &c2).await });

    // Give T2 time to queue behind T1 on `projects.commit_seq`, then verify neither `c1` nor `c2`
    // has committed out of order and watermarks stay at `c0`.
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
    assert!(
        !t2_handle.is_finished(),
        "T2 must wait behind T1's per-project sequence lock instead of committing ahead of T1"
    );

    let (scan_ids, _, full_scan_token) = store
        .list_commits_with_sync(&repo_id, &[], 10, &[])
        .await
        .unwrap();
    assert_eq!(scan_ids, vec![c0_id.clone()]);

    let (inc_before, _, token_before) = store
        .list_commits_with_sync(&repo_id, &[], 10, &baseline_token)
        .await
        .unwrap();
    assert!(inc_before.is_empty());
    assert_eq!(token_before, baseline_token);

    // Commit T1 (`c1`); T2 (`c2`) immediately acquires seq+1 and commits.
    tx1.commit().await.unwrap();
    t2_handle.await.unwrap().unwrap();

    // Even while `other_tx` on `other_repo_id` is STILL open, `repo_id` immediately observes
    // BOTH `c1` and `c2` in exact commit order across both full-scan and incremental watermarks.
    let (delta_from_full, _, token_after_full) = store
        .list_commits_with_sync(&repo_id, &[], 10, &full_scan_token)
        .await
        .unwrap();
    assert_eq!(delta_from_full, vec![c1_id.clone(), c2_id.clone()]);
    let (empty_after_full, _, _) = store
        .list_commits_with_sync(&repo_id, &[], 10, &token_after_full)
        .await
        .unwrap();
    assert!(empty_after_full.is_empty());

    let (delta_from_inc, _, token_after_inc) = store
        .list_commits_with_sync(&repo_id, &[], 10, &token_before)
        .await
        .unwrap();
    assert_eq!(delta_from_inc, vec![c1_id, c2_id]);
    let (empty_after_inc, _, _) = store
        .list_commits_with_sync(&repo_id, &[], 10, &token_after_inc)
        .await
        .unwrap();
    assert!(empty_after_inc.is_empty());

    // Concurrent storm of 12 writers on `repo_id` + continuous incremental reader never
    // misses a single commit.
    let storm_count = 12usize;
    let mut expected_storm = HashSet::new();
    let barrier = Arc::new(tokio::sync::Barrier::new(storm_count + 1));
    let mut writer_handles = Vec::with_capacity(storm_count);
    for i in 0..storm_count {
        let mut cid = vec![0x70; 20];
        cid[19] = i as u8;
        expected_storm.insert(cid.clone());
        let commit = make_commit(&cid, &format!("storm {i}\n"));
        let s = Arc::clone(&store);
        let r = repo_id.clone();
        let b = Arc::clone(&barrier);
        writer_handles.push(tokio::spawn(async move {
            b.wait().await;
            s.put_commit(&r, &cid, &commit).await.unwrap();
        }));
    }
    let mut seen_storm = HashSet::new();
    let mut cursor_tok = token_after_inc;
    barrier.wait().await;
    for _ in 0..20 {
        let (batch, _, next_tok) = store
            .list_commits_with_sync(&repo_id, &[], 3, &cursor_tok)
            .await
            .unwrap();
        seen_storm.extend(batch);
        cursor_tok = next_tok;
        if seen_storm.len() == storm_count {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    for h in writer_handles {
        h.await.unwrap();
    }
    // Final drain to guarantee every committed storm entry is returned.
    loop {
        let (batch, next_page, next_tok) = store
            .list_commits_with_sync(&repo_id, &[], 4, &cursor_tok)
            .await
            .unwrap();
        seen_storm.extend(batch);
        cursor_tok = next_tok;
        if next_page.is_empty() {
            break;
        }
    }
    assert_eq!(seen_storm, expected_storm);

    other_tx.rollback().await.unwrap();
    drop(other_client);
    drop(raw_client);
}

#[tokio::test]
async fn test_postgres_project_checked_writes_prevent_cross_project_toctou() {
    let Some(meta) =
        connect_or_skip("test_postgres_project_checked_writes_prevent_cross_project_toctou").await
    else {
        return;
    };

    let proj_a = unique_id("proj_a");
    let proj_b = unique_id("proj_b");
    let repo_id = unique_id("repo_toctou");
    meta.register_project(&proj_a, None).await.unwrap();
    meta.register_project(&proj_b, None).await.unwrap();
    meta.register_repo_in_project(&repo_id, &proj_a, None)
        .await
        .unwrap();

    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let root_commit = cc_common::ROOT_COMMIT_ID_BYTES.to_vec();
    let empty_tree = cc_common::EMPTY_TREE_ID_BYTES.to_vec();

    let view1_id = random_bytes(16);
    let view1 = make_view(root_commit.clone());
    let op1_id = random_bytes(16);
    let op1 = make_op(&view1_id, vec![root_op.clone()], "op1");

    // Wrong project must fail with FailedPrecondition and write nothing.
    assert!(matches!(
        meta.put_view_for_project(&repo_id, &proj_b, &view1_id, &view1)
            .await,
        Err(StoreError::FailedPrecondition(_))
    ));
    assert!(meta.get_view(&repo_id, &view1_id).await.unwrap().is_none());

    assert!(matches!(
        meta.put_operation_for_project(&repo_id, &proj_b, &op1_id, &op1, true)
            .await,
        Err(StoreError::FailedPrecondition(_))
    ));
    assert!(
        meta.get_operation(&repo_id, &op1_id)
            .await
            .unwrap()
            .is_none()
    );

    assert!(matches!(
        meta.publish_operation_for_project(
            &repo_id,
            &proj_b,
            &view1_id,
            &view1,
            &op1_id,
            &op1,
            std::slice::from_ref(&root_op),
            true,
        )
        .await,
        Err(StoreError::FailedPrecondition(_))
    ));
    assert_eq!(
        meta.get_op_heads(&repo_id).await.unwrap(),
        Some(vec![root_op.clone()])
    );

    // Matching project succeeds for view, operation, and atomic publish.
    meta.put_view_for_project(&repo_id, &proj_a, &view1_id, &view1)
        .await
        .unwrap();
    meta.put_operation_for_project(&repo_id, &proj_a, &op1_id, &op1, true)
        .await
        .unwrap();

    assert!(matches!(
        meta.update_op_heads_for_project(
            &repo_id,
            &proj_b,
            std::slice::from_ref(&root_op),
            &op1_id,
            true,
        )
        .await,
        Err(StoreError::FailedPrecondition(_))
    ));
    assert!(matches!(
        meta.update_op_heads_cas_for_project(
            &repo_id,
            &proj_b,
            std::slice::from_ref(&root_op),
            &op1_id,
            true,
        )
        .await,
        Err(StoreError::FailedPrecondition(_))
    ));
    assert_eq!(
        meta.get_op_heads(&repo_id).await.unwrap(),
        Some(vec![root_op.clone()])
    );

    // Requiring operation existence inside the transaction rejects committing a missing operation.
    let missing_op_id = random_bytes(16);
    assert!(matches!(
        meta.update_op_heads_for_project(
            &repo_id,
            &proj_a,
            std::slice::from_ref(&root_op),
            &missing_op_id,
            true,
        )
        .await,
        Err(StoreError::MissingDependency {
            parent_kind: "op_heads",
            missing_kind: "operation",
            ..
        })
    ));
    assert!(matches!(
        meta.update_op_heads_cas_for_project(
            &repo_id,
            &proj_a,
            std::slice::from_ref(&root_op),
            &missing_op_id,
            true,
        )
        .await,
        Err(StoreError::MissingDependency {
            parent_kind: "op_heads",
            missing_kind: "operation",
            ..
        })
    ));
    assert_eq!(
        meta.get_op_heads(&repo_id).await.unwrap(),
        Some(vec![root_op.clone()])
    );

    let heads_after_update = meta
        .update_op_heads_for_project(
            &repo_id,
            &proj_a,
            std::slice::from_ref(&root_op),
            &op1_id,
            true,
        )
        .await
        .unwrap();
    assert_eq!(heads_after_update, vec![op1_id.clone()]);

    let view2_id = random_bytes(16);
    let view2 = make_view(root_commit.clone());
    let op2_id = random_bytes(16);
    let op2 = make_op(&view2_id, vec![op1_id.clone()], "op2");
    let heads_after_publish = meta
        .publish_operation_for_project(
            &repo_id,
            &proj_a,
            &view2_id,
            &view2,
            &op2_id,
            &op2,
            std::slice::from_ref(&op1_id),
            true,
        )
        .await
        .unwrap();
    assert_eq!(heads_after_publish, vec![op2_id.clone()]);

    let op3_id = random_bytes(16);
    let op3 = make_op(&view2_id, vec![op2_id.clone()], "op3");
    meta.put_operation_for_project(&repo_id, &proj_a, &op3_id, &op3, true)
        .await
        .unwrap();
    meta.update_op_heads_cas_for_project(
        &repo_id,
        &proj_a,
        std::slice::from_ref(&op2_id),
        &op3_id,
        true,
    )
    .await
    .unwrap();
    assert_eq!(
        meta.get_op_heads(&repo_id).await.unwrap(),
        Some(vec![op3_id.clone()])
    );

    #[allow(deprecated)]
    let ws = WorkspaceState {
        repo_id: repo_id.clone(),
        user: "dev@example.com".to_string(),
        workspace_name: "default".to_string(),
        commit_id: root_commit,
        operation_id: op3_id.clone(),
        tree_id: empty_tree.clone(),
        sparse_patterns: vec!["".to_string()],
        tree_ids: vec![empty_tree],
        conflict_labels: vec![],
        sparse_patterns_set: true,
    };
    assert!(matches!(
        meta.put_workspace_cas_for_project(&proj_b, &ws, Some(&[]), true)
            .await,
        Err(StoreError::FailedPrecondition(_))
    ));
    assert!(
        meta.get_workspace(&repo_id, "dev@example.com", "default")
            .await
            .unwrap()
            .is_none()
    );

    meta.put_workspace_cas_for_project(&proj_a, &ws, Some(&[]), true)
        .await
        .unwrap();
    assert_eq!(
        meta.get_workspace(&repo_id, "dev@example.com", "default")
            .await
            .unwrap(),
        Some(ws.clone())
    );

    // After repo deletion, project-checked writes return RepoNotFound.
    assert!(
        meta.delete_repository(&repo_id, Some(&proj_a))
            .await
            .unwrap()
    );
    assert!(matches!(
        meta.put_workspace_cas_for_project(&proj_a, &ws, Some(&op3_id), true)
            .await,
        Err(StoreError::RepoNotFound { .. })
    ));
}

#[tokio::test]
async fn test_postgres_update_op_heads_retry_does_not_resurrect_superseded_and_preserves_ancestor_pruning()
 {
    let Some(meta) = connect_or_skip(
        "test_postgres_update_op_heads_retry_does_not_resurrect_superseded_and_preserves_ancestor_pruning",
    )
    .await
    else {
        return;
    };

    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let root_commit = cc_common::ROOT_COMMIT_ID_BYTES.to_vec();
    let view_id = random_bytes(16);
    let view = make_view(root_commit);

    // Retrying `update_op_heads([root], op_a)` after `op_a` was superseded by `op_b`
    // must NOT resurrect `op_a` as a divergent head.
    let repo_retry = unique_id("pg_op_retry");
    meta.register_repo(&repo_retry, None).await.unwrap();
    meta.put_view(&repo_retry, &view_id, &view).await.unwrap();

    let op_a = random_bytes(16);
    let op_b = random_bytes(16);
    meta.put_operation(
        &repo_retry,
        &op_a,
        &make_op(&view_id, vec![root_op.clone()], "op_a"),
    )
    .await
    .unwrap();
    meta.put_operation(
        &repo_retry,
        &op_b,
        &make_op(&view_id, vec![op_a.clone()], "op_b"),
    )
    .await
    .unwrap();

    let heads_a = meta
        .update_op_heads(&repo_retry, std::slice::from_ref(&root_op), &op_a)
        .await
        .unwrap();
    assert_eq!(heads_a, vec![op_a.clone()]);

    let heads_b = meta
        .update_op_heads(&repo_retry, std::slice::from_ref(&op_a), &op_b)
        .await
        .unwrap();
    assert_eq!(heads_b, vec![op_b.clone()]);

    let retry_a = meta
        .update_op_heads(&repo_retry, std::slice::from_ref(&root_op), &op_a)
        .await
        .unwrap();
    assert_eq!(retry_a, vec![op_b.clone()]);
    assert_eq!(
        meta.get_op_heads(&repo_retry).await.unwrap(),
        Some(vec![op_b.clone()])
    );

    // Ancestor pruning: when both `op_anc` and `op_desc` are concurrently present in
    // `op_heads` and already published, `update_op_heads([op_anc, op_desc], op_desc)` must still
    // prune `op_anc` to `[op_desc]`.
    let repo_prune = unique_id("pg_op_prune");
    meta.register_repo(&repo_prune, None).await.unwrap();
    meta.put_view(&repo_prune, &view_id, &view).await.unwrap();

    let op_anc = random_bytes(16);
    let op_desc = random_bytes(16);
    meta.put_operation(
        &repo_prune,
        &op_anc,
        &make_op(&view_id, vec![root_op.clone()], "op_anc"),
    )
    .await
    .unwrap();
    meta.put_operation(
        &repo_prune,
        &op_desc,
        &make_op(&view_id, vec![op_anc.clone()], "op_desc"),
    )
    .await
    .unwrap();

    meta.update_op_heads(&repo_prune, std::slice::from_ref(&root_op), &op_anc)
        .await
        .unwrap();
    let divergent = meta
        .update_op_heads(&repo_prune, &[], &op_desc)
        .await
        .unwrap();
    assert_eq!(divergent, vec![op_anc.clone(), op_desc.clone()]);

    let pruned = meta
        .update_op_heads(&repo_prune, &[op_anc.clone(), op_desc.clone()], &op_desc)
        .await
        .unwrap();
    assert_eq!(pruned, vec![op_desc.clone()]);
    assert_eq!(
        meta.get_op_heads(&repo_prune).await.unwrap(),
        Some(vec![op_desc])
    );
}
