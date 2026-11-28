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

use super::*;
use bytes::Bytes;
use rusqlite::params;

#[test]
fn test_sqlite_concurrent_legacy_column_migration_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("legacy_concurrent.db");
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "
            CREATE TABLE projects (project_id TEXT PRIMARY KEY, name TEXT UNIQUE);
            CREATE TABLE repositories (
                repo_id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL REFERENCES projects(project_id),
                name TEXT,
                op_heads_json TEXT NOT NULL
            );
            CREATE TABLE commits (
                project_id TEXT NOT NULL,
                commit_id BLOB NOT NULL,
                change_id BLOB NOT NULL,
                generation INTEGER NOT NULL,
                proto_bytes BLOB NOT NULL,
                PRIMARY KEY (project_id, commit_id)
            );
            CREATE TABLE workspaces (
                repo_id TEXT NOT NULL,
                workspace_name TEXT NOT NULL,
                user TEXT NOT NULL,
                commit_id BLOB NOT NULL,
                operation_id BLOB NOT NULL,
                tree_id BLOB NOT NULL,
                sparse_patterns_json TEXT NOT NULL,
                PRIMARY KEY (repo_id, workspace_name)
            );
            CREATE TABLE operations (
                repo_id TEXT NOT NULL,
                op_id BLOB NOT NULL,
                proto_bytes BLOB NOT NULL,
                PRIMARY KEY (repo_id, op_id)
            );
            ",
        )
        .unwrap();
    }

    let barrier = Arc::new(std::sync::Barrier::new(4));
    let mut handles = Vec::new();
    for _ in 0..4 {
        let db_path = db_path.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            SqliteMetadataStore::open(&db_path)
        }));
    }
    for h in handles {
        let _store = h
            .join()
            .unwrap()
            .expect("concurrent legacy open must succeed");
    }
}

#[test]
fn test_sqlite_concurrent_fresh_open_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("fresh_concurrent.db");

    let barrier = Arc::new(std::sync::Barrier::new(4));
    let mut handles = Vec::new();
    for _ in 0..4 {
        let db_path = db_path.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            SqliteMetadataStore::open(&db_path)
        }));
    }
    for h in handles {
        let _store = h
            .join()
            .unwrap()
            .expect("concurrent fresh open must succeed");
    }
}

#[test]
fn test_sqlite_open_rejects_empty_path_and_pre_split_poc_databases() {
    assert!(matches!(
        SqliteMetadataStore::open(Path::new("")),
        Err(StoreError::InvalidArgument(_))
    ));
    assert!(matches!(
        SqliteMetadataStore::open(Path::new("   ")),
        Err(StoreError::InvalidArgument(_))
    ));

    let dir = tempfile::tempdir().unwrap();

    // Pre-split PoC DB with legacy `repos` table.
    let legacy_repos_path = dir.path().join("poc_repos.db");
    {
        let conn = Connection::open(&legacy_repos_path).unwrap();
        conn.execute_batch("CREATE TABLE repos (repo_id TEXT PRIMARY KEY);")
            .unwrap();
    }
    let err = SqliteMetadataStore::open(&legacy_repos_path)
        .err()
        .expect("pre-split DB with repos table must fail");
    assert!(
        matches!(err, StoreError::InvalidArgument(_)) && err.to_string().contains("repos"),
        "{err:?}"
    );

    // Pre-split PoC DB with `commits` lacking `proto_bytes`.
    let legacy_commits_path = dir.path().join("poc_commits.db");
    {
        let conn = Connection::open(&legacy_commits_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE commits (repo_id TEXT NOT NULL, commit_id BLOB NOT NULL, data BLOB NOT NULL);",
        )
        .unwrap();
    }
    let err = SqliteMetadataStore::open(&legacy_commits_path)
        .err()
        .expect("pre-split DB lacking commits.proto_bytes must fail");
    assert!(
        matches!(err, StoreError::InvalidArgument(_)) && err.to_string().contains("proto_bytes"),
        "{err:?}"
    );
}

#[cfg(unix)]
#[test]
fn test_sqlite_open_applies_private_permissions_to_created_dir_and_db() {
    use std::os::unix::fs::PermissionsExt as _;

    let dir = tempfile::tempdir().unwrap();
    let nested_dir = dir.path().join("private-sqlite-dir");
    let db_path = nested_dir.join("meta.db");
    let _store = SqliteMetadataStore::open(&db_path).unwrap();

    let dir_mode = std::fs::metadata(&nested_dir).unwrap().permissions().mode() & 0o777;
    assert_eq!(dir_mode, 0o700, "expected 0o700, got {dir_mode:#o}");
    let db_mode = std::fs::metadata(&db_path).unwrap().permissions().mode() & 0o777;
    assert_eq!(db_mode, 0o600, "expected 0o600, got {db_mode:#o}");
    for suffix in ["-wal", "-shm"] {
        let mut sidecar = db_path.as_os_str().to_os_string();
        sidecar.push(suffix);
        if let Ok(meta) = std::fs::metadata(&sidecar) {
            let mode = meta.permissions().mode() & 0o777;
            assert_eq!(
                mode, 0o600,
                "sidecar {suffix} expected 0o600, got {mode:#o}"
            );
        }
    }
}

fn test_view() -> View {
    View {
        head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
        wc_commit_ids: HashMap::new(),
        local_bookmarks: HashMap::new(),
        remote_bookmarks: HashMap::new(),
    }
}

fn test_op(view_id: &[u8], parents: Vec<Vec<u8>>, desc: &str) -> Operation {
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
async fn test_sqlite_publish_operation_tracks_published_flag_accurately() {
    let dir = tempfile::tempdir().unwrap();
    let meta = SqliteMetadataStore::open(&dir.path().join("pub_flag.db")).unwrap();
    meta.register_repo("repo-pub", None).await.unwrap();
    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let view_id = vec![0xaa; 16];
    let view = test_view();
    meta.put_view("repo-pub", &view_id, &view).await.unwrap();

    // WriteOperation(op_a) -> sibling advances old_ids -> PublishOperation(op_a, [root])
    // must add op_a to OpHeads (not drop it).
    let op_a_id = vec![0x11; 16];
    let op_a = test_op(&view_id, vec![root_op.clone()], "op_a");
    meta.put_operation("repo-pub", &op_a_id, &op_a)
        .await
        .unwrap();

    let op_sib_id = vec![0x22; 16];
    let op_sib = test_op(&view_id, vec![root_op.clone()], "op_sib");
    let heads = meta
        .publish_operation_atomic(
            "repo-pub",
            &view_id,
            &view,
            &op_sib_id,
            &op_sib,
            std::slice::from_ref(&root_op),
        )
        .await
        .unwrap();
    assert_eq!(heads, vec![op_sib_id.clone()]);

    let heads_after_a = meta
        .publish_operation_atomic(
            "repo-pub",
            &view_id,
            &view,
            &op_a_id,
            &op_a,
            std::slice::from_ref(&root_op),
        )
        .await
        .unwrap();
    assert_eq!(heads_after_a, vec![op_sib_id.clone(), op_a_id.clone()]);

    // WriteOperation(op_b) -> PublishOperation(op_b, old_ids=[]) adds op_b to OpHeads.
    let op_b_id = vec![0x33; 16];
    let op_b = test_op(&view_id, vec![root_op.clone()], "op_b");
    meta.put_operation("repo-pub", &op_b_id, &op_b)
        .await
        .unwrap();
    let heads_after_b = meta
        .publish_operation_atomic("repo-pub", &view_id, &view, &op_b_id, &op_b, &[])
        .await
        .unwrap();
    assert_eq!(
        heads_after_b,
        vec![op_sib_id.clone(), op_a_id.clone(), op_b_id.clone()]
    );

    // Supersede all three -> retrying PublishOperation(op_a) must NOT resurrect op_a.
    let op_merge_id = vec![0x44; 16];
    let op_merge = test_op(
        &view_id,
        vec![op_sib_id.clone(), op_a_id.clone(), op_b_id.clone()],
        "merge",
    );
    meta.put_operation("repo-pub", &op_merge_id, &op_merge)
        .await
        .unwrap();
    meta.compare_and_swap_op_heads(
        "repo-pub",
        &[op_sib_id.clone(), op_a_id.clone(), op_b_id.clone()],
        &op_merge_id,
    )
    .await
    .unwrap();

    let retry_a = meta
        .publish_operation_atomic(
            "repo-pub",
            &view_id,
            &view,
            &op_a_id,
            &op_a,
            std::slice::from_ref(&root_op),
        )
        .await
        .unwrap();
    assert_eq!(retry_a, vec![op_merge_id.clone()]);

    // Re-calling put_operation(op_a) and put_operation(op_merge) must NOT reset published.
    meta.put_operation("repo-pub", &op_a_id, &op_a)
        .await
        .unwrap();
    meta.put_operation("repo-pub", &op_merge_id, &op_merge)
        .await
        .unwrap();
    let retry_a_after_rewrite = meta
        .publish_operation_atomic(
            "repo-pub",
            &view_id,
            &view,
            &op_a_id,
            &op_a,
            std::slice::from_ref(&root_op),
        )
        .await
        .unwrap();
    assert_eq!(retry_a_after_rewrite, vec![op_merge_id.clone()]);

    // update_op_heads also marks new_id as published.
    let op_next_id = vec![0x55; 16];
    let op_next = test_op(&view_id, vec![op_merge_id.clone()], "next");
    meta.put_operation("repo-pub", &op_next_id, &op_next)
        .await
        .unwrap();
    meta.update_op_heads("repo-pub", std::slice::from_ref(&op_merge_id), &op_next_id)
        .await
        .unwrap();
    let retry_merge = meta
        .publish_operation_atomic("repo-pub", &view_id, &view, &op_merge_id, &op_merge, &[])
        .await
        .unwrap();
    assert_eq!(retry_merge, vec![op_next_id]);
}

#[tokio::test]
async fn test_sqlite_register_project_and_repo_prevent_namespace_shadowing() {
    let dir = tempfile::tempdir().unwrap();
    let meta = SqliteMetadataStore::open(&dir.path().join("shadow.db")).unwrap();
    meta.register_repo_in_project("repo-x", "proj-a", None)
        .await
        .unwrap();

    let err = meta.register_project("repo-x", None).await.unwrap_err();
    assert_eq!(tonic::Status::from(err).code(), tonic::Code::AlreadyExists);
    assert!(!meta.is_project_registered("repo-x").await.unwrap());

    let err = meta
        .register_repo_in_project("repo-y", "repo-x", None)
        .await
        .unwrap_err();
    assert_eq!(tonic::Status::from(err).code(), tonic::Code::AlreadyExists);
    assert!(meta.get_repo_project_id("repo-y").await.unwrap().is_none());

    let err = meta
        .register_repo_in_project("proj-a", "proj-b", None)
        .await
        .unwrap_err();
    assert_eq!(tonic::Status::from(err).code(), tonic::Code::AlreadyExists);

    meta.register_project("proj-a", Some("  Project A  "))
        .await
        .unwrap();
    let dup_alias = meta
        .register_project("proj-dup", Some("Project A"))
        .await
        .unwrap_err();
    assert_eq!(
        tonic::Status::from(dup_alias).code(),
        tonic::Code::AlreadyExists
    );
    // Whitespace-only project names normalize to None and never collide as aliases.
    meta.register_project("proj-blank-1", Some("   "))
        .await
        .unwrap();
    meta.register_project("proj-blank-2", Some(" \t "))
        .await
        .unwrap();
    meta.register_repo_in_project("solo", "solo", None)
        .await
        .unwrap();
    meta.register_project("solo", Some("Solo Project"))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_sqlite_negative_integers_surface_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("neg.db");
    let meta = SqliteMetadataStore::open(&db_path).unwrap();
    meta.register_project("proj-neg", None).await.unwrap();

    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO commits (project_id, commit_id, change_id, generation, proto_bytes, seq)
             VALUES (?1, ?2, ?3, -5, ?4, 1)",
            params![
                "proj-neg",
                vec![0x01u8; 20],
                vec![0x02u8; 16],
                Vec::<u8>::new()
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO blobs (blob_id, content_size, inline_content, primary_location, replicated_locations_json)
             VALUES (?1, -7, NULL, NULL, '[]')",
            params![vec![0x03u8; 20]],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO project_blobs (project_id, blob_id) VALUES (?1, ?2)",
            params!["proj-neg", vec![0x03u8; 20]],
        )
        .unwrap();
    }

    assert!(matches!(
        meta.get_commit_generation("proj-neg", &[0x01; 20]).await,
        Err(StoreError::Corruption(_))
    ));
    assert!(matches!(
        meta.get_commit_with_generation("proj-neg", &[0x01; 20])
            .await,
        Err(StoreError::Corruption(_))
    ));
    assert!(matches!(
        meta.get_commit_generations("proj-neg", &[vec![0x01; 20]])
            .await,
        Err(StoreError::Corruption(_))
    ));
    assert!(matches!(
        meta.get_blob_record("proj-neg", &[0x03; 20]).await,
        Err(StoreError::Corruption(_))
    ));
    assert!(matches!(
        meta.get_blob_sizes_batch("proj-neg", &[vec![0x03; 20]])
            .await,
        Err(StoreError::Corruption(_))
    ));
    assert!(matches!(
        meta.get_global_blob_record(&[0x03; 20]).await,
        Err(StoreError::Corruption(_))
    ));
}

#[tokio::test]
async fn test_sqlite_list_workspaces_page_and_negative_commit_sync_token_rejection() {
    let dir = tempfile::tempdir().unwrap();
    let meta = SqliteMetadataStore::open(&dir.path().join("page_and_seq.db")).unwrap();
    meta.register_repo("ws-repo", None).await.unwrap();

    // Negative SQLite commit watermark sequences must return InvalidArgument.
    for bad_seq in [-1i64, -42, i64::MIN] {
        let err = meta
            .list_commit_ids_since("ws-repo", &bad_seq.to_be_bytes(), 10)
            .await
            .unwrap_err();
        assert!(
            matches!(err, StoreError::InvalidArgument(_)),
            "seq={bad_seq} expected InvalidArgument, got {err:?}"
        );
    }
    // Non-negative sequence 0 succeeds.
    let (ids, _, has_more) = meta
        .list_commit_ids_since("ws-repo", &0i64.to_be_bytes(), 10)
        .await
        .unwrap();
    assert!(ids.is_empty());
    assert!(!has_more);

    // Keyset pagination on workspaces orders strictly by `workspace_name ASC` regardless of `user`.
    for (user, ws_name) in [("carol", "z"), ("agent-42", "b"), ("bob", "a"), ("", "c")] {
        #[allow(deprecated)]
        meta.put_workspace(&WorkspaceState {
            repo_id: "ws-repo".to_string(),
            user: user.to_string(),
            workspace_name: ws_name.to_string(),
            commit_id: cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
            operation_id: cc_common::ROOT_OPERATION_ID_BYTES.to_vec(),
            tree_id: cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
            tree_ids: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
            conflict_labels: Vec::new(),
            sparse_patterns: vec![String::new()],
            sparse_patterns_set: true,
        })
        .await
        .unwrap();
    }
    // Upserting the same `(repo_id, workspace_name)` with a different `user` updates `user`
    // in place instead of inserting a duplicate row.
    #[allow(deprecated)]
    meta.put_workspace(&WorkspaceState {
        repo_id: "ws-repo".to_string(),
        user: "sa-runner-01".to_string(),
        workspace_name: "a".to_string(),
        commit_id: cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
        operation_id: cc_common::ROOT_OPERATION_ID_BYTES.to_vec(),
        tree_id: cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
        tree_ids: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
        conflict_labels: Vec::new(),
        sparse_patterns: vec![String::new()],
        sparse_patterns_set: true,
    })
    .await
    .unwrap();
    assert_eq!(
        meta.list_workspaces_page("ws-repo", None, 0, None)
            .await
            .unwrap(),
        (Vec::new(), false)
    );
    let (p1, has_more1) = meta
        .list_workspaces_page("ws-repo", None, 2, None)
        .await
        .unwrap();
    assert!(has_more1);
    assert_eq!(
        p1.iter()
            .map(|w| (w.user.as_str(), w.workspace_name.as_str()))
            .collect::<Vec<_>>(),
        vec![("sa-runner-01", "a"), ("agent-42", "b")]
    );
    let (p2, has_more2) = meta
        .list_workspaces_page("ws-repo", Some("b"), 2, None)
        .await
        .unwrap();
    assert!(!has_more2);
    assert_eq!(
        p2.iter()
            .map(|w| (w.user.as_str(), w.workspace_name.as_str()))
            .collect::<Vec<_>>(),
        vec![("", "c"), ("carol", "z")]
    );
    assert_eq!(
        meta.list_workspaces_page("ws-repo", Some("z"), 2, None)
            .await
            .unwrap(),
        (Vec::new(), false)
    );
}

#[test]
fn test_sqlite_rejects_legacy_user_email_workspaces_table() {
    let dir = tempfile::tempdir().unwrap();
    let cases = [
        (
            "legacy_ws_user_email.db",
            "CREATE TABLE workspaces (
                repo_id TEXT NOT NULL,
                user_email TEXT NOT NULL,
                workspace_name TEXT NOT NULL,
                commit_id BLOB NOT NULL,
                operation_id BLOB NOT NULL,
                tree_id BLOB NOT NULL,
                sparse_patterns_json TEXT NOT NULL,
                PRIMARY KEY (repo_id, user_email, workspace_name)
            );",
        ),
        (
            "legacy_ws_3col_user_pk.db",
            "CREATE TABLE workspaces (
                repo_id TEXT NOT NULL,
                user TEXT NOT NULL,
                workspace_name TEXT NOT NULL,
                commit_id BLOB NOT NULL,
                operation_id BLOB NOT NULL,
                tree_id BLOB NOT NULL,
                sparse_patterns_json TEXT NOT NULL,
                PRIMARY KEY (repo_id, user, workspace_name)
            );",
        ),
        (
            "legacy_ws_missing_user.db",
            "CREATE TABLE workspaces (
                repo_id TEXT NOT NULL,
                workspace_name TEXT NOT NULL,
                commit_id BLOB NOT NULL,
                operation_id BLOB NOT NULL,
                tree_id BLOB NOT NULL,
                sparse_patterns_json TEXT NOT NULL,
                PRIMARY KEY (repo_id, workspace_name)
            );",
        ),
        (
            "legacy_ws_mixed_case_user_email.db",
            "CREATE TABLE Workspaces (
                repo_id TEXT NOT NULL,
                user_email TEXT NOT NULL,
                workspace_name TEXT NOT NULL,
                commit_id BLOB NOT NULL,
                operation_id BLOB NOT NULL,
                tree_id BLOB NOT NULL,
                sparse_patterns_json TEXT NOT NULL,
                PRIMARY KEY (repo_id, user_email, workspace_name)
            );",
        ),
        (
            "legacy_ws_upper_case_3col_pk.db",
            "CREATE TABLE WORKSPACES (
                repo_id TEXT NOT NULL,
                user TEXT NOT NULL,
                workspace_name TEXT NOT NULL,
                commit_id BLOB NOT NULL,
                operation_id BLOB NOT NULL,
                tree_id BLOB NOT NULL,
                sparse_patterns_json TEXT NOT NULL,
                PRIMARY KEY (repo_id, user, workspace_name)
            );",
        ),
    ];

    for (file_name, ddl) in cases {
        let db_path = dir.path().join(file_name);
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(ddl).unwrap();
        }
        let err = match SqliteMetadataStore::open(&db_path) {
            Ok(_) => panic!("expected InvalidArgument, got Ok"),
            Err(err) => err,
        };
        assert!(
            matches!(err, StoreError::InvalidArgument(_)),
            "{file_name}: expected InvalidArgument for incompatible workspaces table, got {err:?}"
        );
    }

    // Valid freshly-initialized DB must reopen.
    let valid_path = dir.path().join("valid_ws.db");
    drop(SqliteMetadataStore::open(&valid_path).unwrap());
    drop(SqliteMetadataStore::open(&valid_path).unwrap());
}

#[tokio::test]
async fn test_sqlite_compare_and_swap_op_heads_succeeds_and_fails_on_mismatch() {
    let meta = SqliteMetadataStore::in_memory().unwrap();
    let repo_id = "sqlite-cas-repo";
    meta.register_repo(repo_id, None).await.unwrap();

    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let head1 = vec![0x11; cc_common::OPERATION_ID_LENGTH];
    let head2 = vec![0x22; cc_common::OPERATION_ID_LENGTH];
    let head3 = vec![0x33; cc_common::OPERATION_ID_LENGTH];
    let merged = vec![0x44; cc_common::OPERATION_ID_LENGTH];
    let view_id = cc_common::ROOT_VIEW_ID_BYTES.to_vec();
    for op_id in [&head1, &head2, &head3, &merged] {
        meta.put_operation(
            repo_id,
            op_id,
            &test_op(&view_id, vec![root_op.clone()], "op"),
        )
        .await
        .unwrap();
    }

    meta.update_op_heads(repo_id, std::slice::from_ref(&root_op), &head1)
        .await
        .unwrap();
    meta.update_op_heads(repo_id, &[], &head2).await.unwrap();

    // Exact CAS succeeds even when expected_old_ids order differs from stored order.
    meta.compare_and_swap_op_heads(repo_id, &[head2.clone(), head1.clone()], &merged)
        .await
        .unwrap();
    assert_eq!(
        meta.get_op_heads(repo_id).await.unwrap(),
        Some(vec![merged.clone()])
    );

    // Stale CAS fails with CasConflict and leaves current heads unchanged.
    meta.update_op_heads(repo_id, &[], &head3).await.unwrap();
    let err = meta
        .compare_and_swap_op_heads(repo_id, std::slice::from_ref(&merged), &head1)
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::CasConflict(_)), "{err:?}");
    assert_eq!(
        meta.get_op_heads(repo_id).await.unwrap(),
        Some(vec![merged, head3])
    );
}

#[tokio::test]
async fn test_sqlite_reopen_preserves_disk_data() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("reopen_test.db");

    let commit_id = vec![0x10; cc_common::COMMIT_ID_LENGTH];
    let tree_id = vec![0x20; cc_common::TREE_ID_LENGTH];
    let blob_id = vec![0x30; cc_common::FILE_ID_LENGTH];
    let view_id = vec![0x40; cc_common::VIEW_ID_LENGTH];
    let op_id = vec![0x50; cc_common::OPERATION_ID_LENGTH];
    let commit = Commit {
        commit_id: commit_id.clone(),
        change_id: vec![0x11; cc_common::CHANGE_ID_LENGTH],
        parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![tree_id.clone()],
        description: "persisted\n".to_string(),
        ..Default::default()
    };
    let tree_entry = TreeEntry {
        name: "hello.txt".to_string(),
        value: Some(TreeValue {
            value: Some(tree_value::Value::File(File {
                id: blob_id.clone(),
                executable: false,
                copy_id: Vec::new(),
                size_bytes: Some(5),
            })),
        }),
    };
    let view = test_view();
    let op = test_op(
        &view_id,
        vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
        "persisted op",
    );

    {
        let store = SqliteMetadataStore::open(&db_path).unwrap();
        store
            .register_repo_in_project("reopen-repo", "reopen-proj", Some("Reopen"))
            .await
            .unwrap();
        store
            .put_blob_record(
                "reopen-proj",
                &BlobRecord {
                    blob_id: blob_id.clone(),
                    content_size: 5,
                    inline_content: Some(Bytes::from_static(b"hello")),
                    primary_location: None,
                    replicated_locations: Vec::new(),
                },
            )
            .await
            .unwrap();
        store
            .put_tree("reopen-proj", &tree_id, std::slice::from_ref(&tree_entry))
            .await
            .unwrap();
        store
            .put_commit("reopen-proj", &commit_id, &commit, 1)
            .await
            .unwrap();
        store
            .publish_operation_atomic(
                "reopen-repo",
                &view_id,
                &view,
                &op_id,
                &op,
                &[cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
            )
            .await
            .unwrap();
    }

    // Reopen fresh handle from disk and verify all persisted entities remain intact.
    let reopened = SqliteMetadataStore::open(&db_path).unwrap();
    assert_eq!(
        reopened.get_repo_project_id("reopen-repo").await.unwrap(),
        Some("reopen-proj".to_string())
    );
    assert_eq!(
        reopened
            .get_blob_record("reopen-proj", &blob_id)
            .await
            .unwrap()
            .and_then(|r| r.inline_content),
        Some(Bytes::from_static(b"hello"))
    );
    assert_eq!(
        reopened.get_tree("reopen-proj", &tree_id).await.unwrap(),
        Some(vec![tree_entry])
    );
    assert_eq!(
        reopened
            .get_commit_with_generation("reopen-proj", &commit_id)
            .await
            .unwrap(),
        Some((commit, 1))
    );
    assert_eq!(
        reopened.get_view("reopen-repo", &view_id).await.unwrap(),
        Some(view)
    );
    assert_eq!(
        reopened.get_operation("reopen-repo", &op_id).await.unwrap(),
        Some(op)
    );
    assert_eq!(
        reopened.get_op_heads("reopen-repo").await.unwrap(),
        Some(vec![op_id])
    );
}

#[tokio::test]
async fn test_sqlite_corrupt_protobuf_surfaces_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("corrupt_proto.db");
    let meta = SqliteMetadataStore::open(&db_path).unwrap();
    meta.register_repo("r1", None).await.unwrap();

    let bad_proto: &[u8] = &[0xFF, 0xFF, 0xFF, 0xFF];
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO commits (project_id, commit_id, change_id, generation, proto_bytes, seq)
             VALUES (?1, ?2, ?3, 1, ?4, 1)",
            params!["r1", &[1u8; 20][..], &[2u8; 16][..], bad_proto],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO trees (project_id, tree_id) VALUES (?1, ?2)",
            params!["r1", &[3u8; 20][..]],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tree_entries (project_id, tree_id, name, entry_proto)
             VALUES (?1, ?2, 'a.txt', ?3)",
            params!["r1", &[3u8; 20][..], bad_proto],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO operations (repo_id, op_id, proto_bytes, published)
             VALUES (?1, ?2, ?3, 1)",
            params!["r1", &[4u8; 16][..], bad_proto],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO views (repo_id, view_id, proto_bytes)
             VALUES (?1, ?2, ?3)",
            params!["r1", &[5u8; 16][..], bad_proto],
        )
        .unwrap();
    }

    assert!(matches!(
        meta.get_commit("r1", &[1u8; 20]).await,
        Err(StoreError::Corruption(_))
    ));
    assert!(matches!(
        meta.get_commit_with_generation("r1", &[1u8; 20]).await,
        Err(StoreError::Corruption(_))
    ));
    assert!(matches!(
        meta.get_tree("r1", &[3u8; 20]).await,
        Err(StoreError::Corruption(_))
    ));
    assert!(matches!(
        meta.get_tree_page("r1", &[3u8; 20], None, 10).await,
        Err(StoreError::Corruption(_))
    ));
    assert!(matches!(
        meta.get_operation("r1", &[4u8; 16]).await,
        Err(StoreError::Corruption(_))
    ));
    assert!(matches!(
        meta.get_view("r1", &[5u8; 16]).await,
        Err(StoreError::Corruption(_))
    ));
}

#[tokio::test]
async fn test_sqlite_corrupt_tree_ids_json_reports_field_and_repo_name_normalizes() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("tree_ids_and_name.db");
    let meta = SqliteMetadataStore::open(&db_path).unwrap();

    // Repository name normalization trims whitespace and preserves existing name on blank input.
    meta.register_repo_in_project("repo-norm", "proj-norm", Some("  Trimmed Repo  "))
        .await
        .unwrap();
    meta.register_repo_in_project("repo-norm", "proj-norm", Some("   "))
        .await
        .unwrap();
    meta.register_repo_in_project("repo-blank", "proj-norm", Some(" \t "))
        .await
        .unwrap();

    {
        let conn = Connection::open(&db_path).unwrap();
        let stored_name: Option<String> = conn
            .query_row(
                "SELECT name FROM repositories WHERE repo_id = 'repo-norm'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored_name.as_deref(), Some("Trimmed Repo"));

        let blank_name: Option<String> = conn
            .query_row(
                "SELECT name FROM repositories WHERE repo_id = 'repo-blank'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(blank_name, None);

        // Insert workspaces with malformed JSON and invalid hex in tree_ids_json.
        conn.execute(
            "INSERT INTO workspaces
             (repo_id, user, workspace_name, commit_id, operation_id, tree_id, tree_ids_json, conflict_labels_json, sparse_patterns_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, '[]', '[]')",
            params![
                "repo-norm",
                "alice",
                "bad-json",
                &[1u8; 20][..],
                &[2u8; 16][..],
                &[3u8; 20][..],
                "not-json"
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO workspaces
             (repo_id, user, workspace_name, commit_id, operation_id, tree_id, tree_ids_json, conflict_labels_json, sparse_patterns_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, '[]', '[]')",
            params![
                "repo-norm",
                "alice",
                "bad-hex",
                &[1u8; 20][..],
                &[2u8; 16][..],
                &[3u8; 20][..],
                "[\"zzzz\"]"
            ],
        )
        .unwrap();
    }

    for ws_name in ["bad-json", "bad-hex"] {
        let err = meta.get_workspace("repo-norm", ws_name).await.unwrap_err();
        match err {
            StoreError::Corruption(msg) => {
                assert!(
                    msg.contains("tree_ids_json") && !msg.contains("op_heads"),
                    "expected tree_ids_json corruption message for {ws_name}, got: {msg}"
                );
            }
            other => panic!("expected Corruption for {ws_name}, got {other:?}"),
        }
    }
}
