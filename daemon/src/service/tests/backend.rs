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

use std::sync::Arc;

use cc_common::backend::backend_service_server::BackendService;
use cc_common::backend::*;
use cc_common::op_store::op_store_service_server::OpStoreService;
use cc_common::op_store::*;
use cc_common::workspace::workspace_service_server::WorkspaceService;
use cc_common::workspace::*;
use cc_common::{
    EMPTY_TREE_ID_BYTES, ROOT_COMMIT_ID_BYTES, ROOT_OPERATION_ID_BYTES, ROOT_VIEW_ID_BYTES,
};
use tonic::Request;
use tonic::metadata::MetadataMap;

use super::super::{DaemonProxy, MAX_IDENTIFIER_LEN};
use super::{MockUpstream, req_with_upstream, spawn_mock_upstream};
use crate::cache::{CacheScope, ObjectKind, TwoTierCache};
use crate::upstream::{UPSTREAM_HEADER, UpstreamPool};

#[tokio::test]
async fn write_commit_canonicalizes_empty_conflict_labels_and_gates_secure_sig() {
    let mock = MockUpstream::default();
    let url = spawn_mock_upstream(mock.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 8 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new(cache, Arc::new(UpstreamPool::new(None)), None);

    let base_commit = |desc: &str| Commit {
        commit_id: Vec::new(),
        change_id: vec![0x11; 16],
        parent_commit_ids: vec![ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![
            EMPTY_TREE_ID_BYTES.to_vec(),
            EMPTY_TREE_ID_BYTES.to_vec(),
            EMPTY_TREE_ID_BYTES.to_vec(),
        ],
        description: desc.to_string(),
        ..Default::default()
    };

    // WriteCommit does not write through; first ReadCommit fetches the server's
    // canonicalized commit (`conflict_labels` cleared) and caches it for subsequent reads.
    let mut empty_labels = base_commit("empty-labels");
    empty_labels.conflict_labels = vec![String::new(), String::new(), String::new()];
    let cid = BackendService::write_commit(
        &proxy,
        req_with_upstream(
            WriteCommitRequest {
                project_id: "proj".to_string(),
                commit: Some(empty_labels),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .commit_id;
    for _ in 0..2 {
        let read = BackendService::read_commit(
            &proxy,
            req_with_upstream(
                ReadCommitRequest {
                    project_id: "proj".to_string(),
                    commit_id: cid.clone(),
                },
                &url,
                None,
            ),
        )
        .await
        .unwrap()
        .into_inner()
        .commit
        .unwrap();
        assert!(
            read.conflict_labels.is_empty(),
            "server-canonicalized empty conflict_labels must be preserved"
        );
    }
    assert_eq!(mock.state.lock().unwrap().read_commit_calls, 1);

    // Non-empty conflict_labels are preserved on read and cached after first fetch.
    let mut labeled = base_commit("labeled");
    labeled.conflict_labels = vec!["left".into(), "base".into(), "right".into()];
    let labeled_id = BackendService::write_commit(
        &proxy,
        req_with_upstream(
            WriteCommitRequest {
                project_id: "proj".to_string(),
                commit: Some(labeled),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .commit_id;
    let read_labeled = BackendService::read_commit(
        &proxy,
        req_with_upstream(
            ReadCommitRequest {
                project_id: "proj".to_string(),
                commit_id: labeled_id,
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .commit
    .unwrap();
    assert_eq!(read_labeled.conflict_labels, vec!["left", "base", "right"]);
    assert_eq!(mock.state.lock().unwrap().read_commit_calls, 2);

    // Incomplete secure_sig (`data` empty) fetches server-derived data on first ReadCommit.
    let mut incomplete_sig = base_commit("incomplete-sig");
    incomplete_sig.secure_sig = Some(SecureSig {
        data: Vec::new(),
        sig: b"sig-bytes".to_vec(),
    });
    let inc_id = BackendService::write_commit(
        &proxy,
        req_with_upstream(
            WriteCommitRequest {
                project_id: "proj".to_string(),
                commit: Some(incomplete_sig),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .commit_id;
    let fetched_inc = BackendService::read_commit(
        &proxy,
        req_with_upstream(
            ReadCommitRequest {
                project_id: "proj".to_string(),
                commit_id: inc_id,
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .commit
    .unwrap();
    assert_eq!(mock.state.lock().unwrap().read_commit_calls, 3);
    assert_eq!(
        fetched_inc.secure_sig.unwrap().data,
        b"server-derived-signed-payload"
    );

    // Empty `sig` fetches server-cleared secure_sig on first ReadCommit.
    let mut empty_sig = base_commit("empty-sig");
    empty_sig.secure_sig = Some(SecureSig {
        data: b"stale".to_vec(),
        sig: Vec::new(),
    });
    let empty_sig_id = BackendService::write_commit(
        &proxy,
        req_with_upstream(
            WriteCommitRequest {
                project_id: "proj".to_string(),
                commit: Some(empty_sig),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .commit_id;
    let fetched_empty = BackendService::read_commit(
        &proxy,
        req_with_upstream(
            ReadCommitRequest {
                project_id: "proj".to_string(),
                commit_id: empty_sig_id,
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .commit
    .unwrap();
    assert_eq!(mock.state.lock().unwrap().read_commit_calls, 4);
    assert!(fetched_empty.secure_sig.is_none());

    // Complete secure_sig also fetches once on first ReadCommit and hits cache on repeat.
    let mut complete_sig = base_commit("complete-sig");
    complete_sig.secure_sig = Some(SecureSig {
        data: b"signed-payload".to_vec(),
        sig: b"sig-bytes".to_vec(),
    });
    let comp_id = BackendService::write_commit(
        &proxy,
        req_with_upstream(
            WriteCommitRequest {
                project_id: "proj".to_string(),
                commit: Some(complete_sig),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .commit_id;
    for _ in 0..2 {
        let fetched_comp = BackendService::read_commit(
            &proxy,
            req_with_upstream(
                ReadCommitRequest {
                    project_id: "proj".to_string(),
                    commit_id: comp_id.clone(),
                },
                &url,
                None,
            ),
        )
        .await
        .unwrap()
        .into_inner()
        .commit
        .unwrap();
        assert_eq!(fetched_comp.secure_sig.unwrap().data, b"signed-payload");
    }
    assert_eq!(mock.state.lock().unwrap().read_commit_calls, 5);
}

#[tokio::test]
async fn caller_auth_scoping_isolates_cache_and_synthetic_fast_paths_fail_closed() {
    let mock = MockUpstream::default();
    let commit_id = vec![0x77; 20];
    {
        let mut st = mock.state.lock().unwrap();
        st.required_auth = Some("Bearer secret-a".to_string());
        st.commits.insert(
            ("proj".to_string(), commit_id.clone()),
            Commit {
                commit_id: commit_id.clone(),
                change_id: vec![0x11; 16],
                parent_commit_ids: vec![ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: vec![EMPTY_TREE_ID_BYTES.to_vec()],
                description: "private commit".to_string(),
                ..Default::default()
            },
        );
    }
    let url = spawn_mock_upstream(mock.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 8 * 1024 * 1024).unwrap());
    let pool = Arc::new(UpstreamPool::with_allowlist(None, [url.as_str()]).unwrap());
    let proxy = DaemonProxy::new(cache, pool, None);

    // Authorized caller warms cache and hits L1 on second call.
    for _ in 0..2 {
        let resp = BackendService::read_commit(
            &proxy,
            req_with_upstream(
                ReadCommitRequest {
                    project_id: "proj".to_string(),
                    commit_id: commit_id.clone(),
                },
                &url,
                Some("Bearer secret-a"),
            ),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(resp.commit.unwrap().description, "private commit");
    }
    assert_eq!(mock.state.lock().unwrap().read_commit_calls, 1);

    // Unauthenticated caller must NOT read caller A's cached commit.
    let unauth = BackendService::read_commit(
        &proxy,
        req_with_upstream(
            ReadCommitRequest {
                project_id: "proj".to_string(),
                commit_id: commit_id.clone(),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(unauth.code(), tonic::Code::Unauthenticated);

    // Different Bearer token must NOT read caller A's cached commit.
    let wrong_token = BackendService::read_commit(
        &proxy,
        req_with_upstream(
            ReadCommitRequest {
                project_id: "proj".to_string(),
                commit_id,
            },
            &url,
            Some("Bearer secret-b"),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(wrong_token.code(), tonic::Code::PermissionDenied);

    // Synthetic fast-paths fail closed on disallowed upstream, missing upstream, or empty id.
    let bad_url = "http://169.254.169.254:80";
    assert_eq!(
        BackendService::read_tree(
            &proxy,
            req_with_upstream(
                ReadTreeRequest {
                    project_id: "proj".to_string(),
                    path: String::new(),
                    tree_id: EMPTY_TREE_ID_BYTES.to_vec(),
                    page_size: 0,
                    page_token: String::new(),
                },
                bad_url,
                None,
            ),
        )
        .await
        .unwrap_err()
        .code(),
        tonic::Code::PermissionDenied
    );
    assert_eq!(
        BackendService::read_tree(
            &proxy,
            req_with_upstream(
                ReadTreeRequest {
                    project_id: "   ".to_string(),
                    path: String::new(),
                    tree_id: EMPTY_TREE_ID_BYTES.to_vec(),
                    page_size: 0,
                    page_token: String::new(),
                },
                &url,
                None,
            ),
        )
        .await
        .unwrap_err()
        .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        OpStoreService::read_operation(
            &proxy,
            Request::new(ReadOperationRequest {
                repo_id: "repo".to_string(),
                operation_id: ROOT_OPERATION_ID_BYTES.to_vec(),
            }),
        )
        .await
        .unwrap_err()
        .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        OpStoreService::read_operation(
            &proxy,
            req_with_upstream(
                ReadOperationRequest {
                    repo_id: String::new(),
                    operation_id: ROOT_OPERATION_ID_BYTES.to_vec(),
                },
                &url,
                None,
            ),
        )
        .await
        .unwrap_err()
        .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        OpStoreService::read_view(
            &proxy,
            req_with_upstream(
                ReadViewRequest {
                    repo_id: "repo".to_string(),
                    view_id: ROOT_VIEW_ID_BYTES.to_vec(),
                },
                bad_url,
                None,
            ),
        )
        .await
        .unwrap_err()
        .code(),
        tonic::Code::PermissionDenied
    );
    assert_eq!(
        OpStoreService::read_view(
            &proxy,
            req_with_upstream(
                ReadViewRequest {
                    repo_id: "".to_string(),
                    view_id: ROOT_VIEW_ID_BYTES.to_vec(),
                },
                &url,
                None,
            ),
        )
        .await
        .unwrap_err()
        .code(),
        tonic::Code::InvalidArgument
    );

    let too_long = "a".repeat(MAX_IDENTIFIER_LEN + 1);
    for bad_id in [".", "..", "bad/id", "has space", too_long.as_str()] {
        assert_eq!(
            BackendService::read_tree(
                &proxy,
                req_with_upstream(
                    ReadTreeRequest {
                        project_id: bad_id.to_string(),
                        path: String::new(),
                        tree_id: EMPTY_TREE_ID_BYTES.to_vec(),
                        page_size: 0,
                        page_token: String::new(),
                    },
                    &url,
                    None,
                ),
            )
            .await
            .unwrap_err()
            .code(),
            tonic::Code::InvalidArgument,
            "empty tree fast path must reject {bad_id:?}"
        );
        assert_eq!(
            OpStoreService::read_operation(
                &proxy,
                req_with_upstream(
                    ReadOperationRequest {
                        repo_id: bad_id.to_string(),
                        operation_id: ROOT_OPERATION_ID_BYTES.to_vec(),
                    },
                    &url,
                    None,
                ),
            )
            .await
            .unwrap_err()
            .code(),
            tonic::Code::InvalidArgument,
            "root op fast path must reject {bad_id:?}"
        );
        assert_eq!(
            OpStoreService::read_view(
                &proxy,
                req_with_upstream(
                    ReadViewRequest {
                        repo_id: bad_id.to_string(),
                        view_id: ROOT_VIEW_ID_BYTES.to_vec(),
                    },
                    &url,
                    None,
                ),
            )
            .await
            .unwrap_err()
            .code(),
            tonic::Code::InvalidArgument,
            "root view fast path must reject {bad_id:?}"
        );
    }

    // Valid identifier syntax succeeds on synthetic fast paths.
    assert!(
        BackendService::read_tree(
            &proxy,
            req_with_upstream(
                ReadTreeRequest {
                    project_id: "proj_1.2-a".to_string(),
                    path: String::new(),
                    tree_id: EMPTY_TREE_ID_BYTES.to_vec(),
                    page_size: 0,
                    page_token: String::new(),
                },
                &url,
                None,
            ),
        )
        .await
        .is_ok()
    );
}

#[tokio::test]
async fn write_commit_does_not_poison_cold_cache_with_non_authoritative_metadata() {
    let mock = MockUpstream::default();
    let shared_commit_id = vec![0x42; 20];
    let make_commit = |pred_byte: u8, ext_val: &[u8]| {
        let mut external_ids = std::collections::HashMap::new();
        external_ids.insert("sync".to_string(), ext_val.to_vec());
        Commit {
            commit_id: shared_commit_id.clone(),
            change_id: vec![0x22; 16],
            parent_commit_ids: vec![ROOT_COMMIT_ID_BYTES.to_vec()],
            predecessors: vec![vec![pred_byte; 20]],
            root_tree_id: vec![EMPTY_TREE_ID_BYTES.to_vec()],
            description: "shared commit content".to_string(),
            external_ids,
            ..Default::default()
        }
    };

    // Pre-populate upstream `proj-a` with first-writer commit `M1` while the daemon's cache
    // is cold (simulating another client having already won first-writer-wins).
    mock.state.lock().unwrap().commits.insert(
        ("proj-a".to_string(), shared_commit_id.clone()),
        make_commit(0x11, b"ext-a-first"),
    );

    let url = spawn_mock_upstream(mock.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 8 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new(Arc::clone(&cache), Arc::new(UpstreamPool::new(None)), None);

    // Cold-cache daemon forwards a second writer `M2` with differing unhashed `predecessors`
    // and `external_ids` to `proj-a`: must NOT write-through `M2` into cache.
    let id_a_second = BackendService::write_commit(
        &proxy,
        req_with_upstream(
            WriteCommitRequest {
                project_id: "proj-a".to_string(),
                commit: Some(make_commit(0x99, b"ext-a-second-should-not-win")),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .commit_id;
    assert_eq!(id_a_second, shared_commit_id);
    assert_eq!(cache.stats().write_through_objects, 0);
    let scope_a = CacheScope::new(&url, "proj-a");
    assert_eq!(
        cache.lookup_cached(&scope_a, ObjectKind::Commit, &shared_commit_id),
        None,
        "WriteCommit must not populate cold cache with non-authoritative Commit metadata"
    );

    // Project B writes its own authoritative commit under the same `commit_id`.
    let id_b = BackendService::write_commit(
        &proxy,
        req_with_upstream(
            WriteCommitRequest {
                project_id: "proj-b".to_string(),
                commit: Some(make_commit(0x22, b"ext-b-first")),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .commit_id;
    assert_eq!(id_b, shared_commit_id);

    let assert_reads_isolated = |p: &DaemonProxy| {
        let url = url.clone();
        let shared_commit_id = shared_commit_id.clone();
        let p = p.clone();
        async move {
            let read_a = BackendService::read_commit(
                &p,
                req_with_upstream(
                    ReadCommitRequest {
                        project_id: "proj-a".to_string(),
                        commit_id: shared_commit_id.clone(),
                    },
                    &url,
                    None,
                ),
            )
            .await
            .unwrap()
            .into_inner()
            .commit
            .unwrap();
            assert_eq!(read_a.predecessors, vec![vec![0x11; 20]]);
            assert_eq!(
                read_a.external_ids.get("sync").map(Vec::as_slice),
                Some(b"ext-a-first".as_slice())
            );

            let read_b = BackendService::read_commit(
                &p,
                req_with_upstream(
                    ReadCommitRequest {
                        project_id: "proj-b".to_string(),
                        commit_id: shared_commit_id,
                    },
                    &url,
                    None,
                ),
            )
            .await
            .unwrap()
            .into_inner()
            .commit
            .unwrap();
            assert_eq!(read_b.predecessors, vec![vec![0x22; 20]]);
            assert_eq!(
                read_b.external_ids.get("sync").map(Vec::as_slice),
                Some(b"ext-b-first".as_slice())
            );
        }
    };

    // First reads fetch authoritative commits from upstream (2 RPCs total).
    assert_reads_isolated(&proxy).await;
    assert_eq!(mock.state.lock().unwrap().read_commit_calls, 2);

    // Repeat reads hit L1 cache.
    assert_reads_isolated(&proxy).await;
    assert_eq!(mock.state.lock().unwrap().read_commit_calls, 2);

    // Cold L1 restart serves authoritative commits from isolated L2 disk CAS.
    let cold_l1 = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 8 * 1024 * 1024).unwrap());
    let proxy_cold = DaemonProxy::new(cold_l1, Arc::new(UpstreamPool::new(None)), None);
    assert_reads_isolated(&proxy_cold).await;
    assert_eq!(mock.state.lock().unwrap().read_commit_calls, 2);
}

#[tokio::test]
async fn uncached_rpcs_reject_duplicate_and_non_ascii_authorization_before_upstream() {
    let mock = MockUpstream::default();
    mock.state.lock().unwrap().required_auth = Some("Bearer valid-token".to_string());
    let url = spawn_mock_upstream(mock.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 8 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new(Arc::clone(&cache), Arc::new(UpstreamPool::new(None)), None);

    let build_bad_meta = |kind: usize| -> MetadataMap {
        let mut meta = MetadataMap::new();
        meta.insert(UPSTREAM_HEADER, url.parse().unwrap());
        match kind {
            0 => {
                meta.append("authorization", "Bearer valid-token".parse().unwrap());
                meta.append("authorization", "Bearer other-token".parse().unwrap());
            }
            1 => {
                meta.append("authorization", "Bearer valid-token".parse().unwrap());
                meta.append("authorization", "Bearer valid-token".parse().unwrap());
            }
            2 => {
                let raw =
                    tonic::metadata::MetadataValue::try_from(b"Bearer \x80".as_slice()).unwrap();
                meta.insert("authorization", raw);
            }
            _ => unreachable!(),
        }
        meta
    };

    let sample_commit = || WriteCommitRequest {
        project_id: "proj".to_string(),
        commit: Some(Commit {
            commit_id: Vec::new(),
            change_id: vec![0x33; 16],
            parent_commit_ids: vec![ROOT_COMMIT_ID_BYTES.to_vec()],
            root_tree_id: vec![EMPTY_TREE_ID_BYTES.to_vec()],
            description: "auth-check commit".to_string(),
            ..Default::default()
        }),
    };
    let sample_get_ws = || GetWorkspaceRequest {
        repo_id: "repo-1".to_string(),
        workspace_name: "default".to_string(),
    };
    let sample_update_ws = || {
        #[allow(deprecated)]
        let ws = WorkspaceState {
            repo_id: "repo-1".to_string(),
            user: "alice@example.com".to_string(),
            workspace_name: "default".to_string(),
            commit_id: ROOT_COMMIT_ID_BYTES.to_vec(),
            operation_id: ROOT_OPERATION_ID_BYTES.to_vec(),
            tree_id: EMPTY_TREE_ID_BYTES.to_vec(),
            sparse_patterns: vec![String::new()],
            tree_ids: vec![EMPTY_TREE_ID_BYTES.to_vec()],
            conflict_labels: Vec::new(),
            sparse_patterns_set: true,
        };
        UpdateWorkspaceRequest {
            workspace: Some(ws),
            expected_operation_id: None,
        }
    };

    for (case_idx, label) in [
        (0, "duplicate distinct authorization headers"),
        (1, "duplicate identical authorization headers"),
        (2, "non-ASCII authorization header"),
    ] {
        let mut write_req = Request::new(sample_commit());
        *write_req.metadata_mut() = build_bad_meta(case_idx);
        let err = BackendService::write_commit(&proxy, write_req)
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            tonic::Code::Unauthenticated,
            "write_commit must reject {label} with Unauthenticated"
        );

        let mut get_ws_req = Request::new(sample_get_ws());
        *get_ws_req.metadata_mut() = build_bad_meta(case_idx);
        let err = WorkspaceService::get_workspace(&proxy, get_ws_req)
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            tonic::Code::Unauthenticated,
            "get_workspace must reject {label} with Unauthenticated"
        );

        let mut update_ws_req = Request::new(sample_update_ws());
        *update_ws_req.metadata_mut() = build_bad_meta(case_idx);
        let err = WorkspaceService::update_workspace(&proxy, update_ws_req)
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            tonic::Code::Unauthenticated,
            "update_workspace must reject {label} with Unauthenticated"
        );
    }

    {
        let st = mock.state.lock().unwrap();
        assert_eq!(st.write_commit_calls, 0);
        assert_eq!(st.get_workspace_calls, 0);
        assert_eq!(st.update_workspace_calls, 0);
    }
    assert_eq!(cache.stats().upstream_rpcs, 0);

    // Sanity check: a single valid ASCII authorization header reaches upstream and succeeds.
    BackendService::write_commit(
        &proxy,
        req_with_upstream(sample_commit(), &url, Some("Bearer valid-token")),
    )
    .await
    .unwrap();
    WorkspaceService::get_workspace(
        &proxy,
        req_with_upstream(sample_get_ws(), &url, Some("Bearer valid-token")),
    )
    .await
    .unwrap();
    WorkspaceService::update_workspace(
        &proxy,
        req_with_upstream(sample_update_ws(), &url, Some("Bearer valid-token")),
    )
    .await
    .unwrap();

    {
        let st = mock.state.lock().unwrap();
        assert_eq!(st.write_commit_calls, 1);
        assert_eq!(st.get_workspace_calls, 1);
        assert_eq!(st.update_workspace_calls, 1);
    }
    assert_eq!(cache.stats().upstream_rpcs, 3);
}

#[tokio::test]
async fn cross_scope_write_and_read_commit_or_tree_do_not_authorize_victim_shared_cas_objects() {
    let mock = MockUpstream::default();
    let url = spawn_mock_upstream(mock.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 8 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new(Arc::clone(&cache), Arc::new(UpstreamPool::new(None)), None);

    let auth_a = Some("Bearer token-a");
    let auth_b = Some("Bearer token-b");

    // Victim `scope_a` uploads a file, symlink, and subtree into the shared CAS.
    let victim_file = b"top secret victim file payload (42 bytes!)".to_vec();
    let victim_file_id = BackendService::write_file(
        &proxy,
        req_with_upstream(
            WriteFileRequest {
                project_id: "proj-a".to_string(),
                content: victim_file.clone(),
            },
            &url,
            auth_a,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .file_id;

    let victim_symlink_id = BackendService::write_symlink(
        &proxy,
        req_with_upstream(
            WriteSymlinkRequest {
                project_id: "proj-a".to_string(),
                target: "/secret/victim/target".to_string(),
            },
            &url,
            auth_a,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .symlink_id;

    let victim_tree_id = BackendService::write_tree(
        &proxy,
        req_with_upstream(
            WriteTreeRequest {
                project_id: "proj-a".to_string(),
                path: String::new(),
                entries: vec![TreeEntry {
                    name: "secret.txt".to_string(),
                    value: Some(TreeValue {
                        value: Some(tree_value::Value::File(File {
                            id: victim_file_id.clone(),
                            executable: false,
                            copy_id: Vec::new(),
                            size_bytes: None,
                        })),
                    }),
                }],
            },
            &url,
            auth_a,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .tree_id;

    // Enforce upstream ACLs so only `(proj-a, Bearer token-a)` can read the victim objects.
    {
        let mut st = mock.state.lock().unwrap();
        for obj_id in [&victim_file_id, &victim_symlink_id, &victim_tree_id] {
            st.object_acls.insert(
                obj_id.clone(),
                ("proj-a".to_string(), Some("Bearer token-a".to_string())),
            );
        }
    }

    // Test both an attacker in a different project (`proj-b`) and a different caller token
    // (`Bearer token-b`) targeting `proj-a`.
    for (attacker_proj, attacker_auth) in [("proj-b", auth_a), ("proj-a", auth_b)] {
        let attacker_scope = CacheScope::new_with_auth(&url, attacker_proj, attacker_auth);

        // Crafted `WriteTree` referencing victim's file, subtree, and symlink must NOT
        // authorize attacker for any victim child or leak the victim file's cached size.
        let wrapper_with_file_id = BackendService::write_tree(
            &proxy,
            req_with_upstream(
                WriteTreeRequest {
                    project_id: attacker_proj.to_string(),
                    path: String::new(),
                    entries: vec![
                        TreeEntry {
                            name: "stolen-file".to_string(),
                            value: Some(TreeValue {
                                value: Some(tree_value::Value::File(File {
                                    id: victim_file_id.clone(),
                                    executable: false,
                                    copy_id: Vec::new(),
                                    size_bytes: None,
                                })),
                            }),
                        },
                        TreeEntry {
                            name: "stolen-tree".to_string(),
                            value: Some(TreeValue {
                                value: Some(tree_value::Value::TreeId(victim_tree_id.clone())),
                            }),
                        },
                        TreeEntry {
                            name: "stolen-link".to_string(),
                            value: Some(TreeValue {
                                value: Some(tree_value::Value::SymlinkId(
                                    victim_symlink_id.clone(),
                                )),
                            }),
                        },
                    ],
                },
                &url,
                attacker_auth,
            ),
        )
        .await
        .unwrap()
        .into_inner()
        .tree_id;

        assert_eq!(
            cache.blob_size(&attacker_scope, &victim_file_id),
            None,
            "attacker must not learn victim's cached blob size"
        );
        assert!(
            !cache.is_authorized(&attacker_scope, ObjectKind::Tree, &wrapper_with_file_id),
            "WriteTree referencing an unauthorized blob must not write-through enriched sizes"
        );

        // Crafted `WriteTree` with only victim subtree + symlink (which does write through
        // the wrapper tree itself) followed by `ReadTree` must still not authorize children.
        let wrapper_no_file_id = BackendService::write_tree(
            &proxy,
            req_with_upstream(
                WriteTreeRequest {
                    project_id: attacker_proj.to_string(),
                    path: String::new(),
                    entries: vec![
                        TreeEntry {
                            name: "sub".to_string(),
                            value: Some(TreeValue {
                                value: Some(tree_value::Value::TreeId(victim_tree_id.clone())),
                            }),
                        },
                        TreeEntry {
                            name: "lnk".to_string(),
                            value: Some(TreeValue {
                                value: Some(tree_value::Value::SymlinkId(
                                    victim_symlink_id.clone(),
                                )),
                            }),
                        },
                    ],
                },
                &url,
                attacker_auth,
            ),
        )
        .await
        .unwrap()
        .into_inner()
        .tree_id;
        let _ = BackendService::read_tree(
            &proxy,
            req_with_upstream(
                ReadTreeRequest {
                    project_id: attacker_proj.to_string(),
                    path: String::new(),
                    tree_id: wrapper_no_file_id,
                    page_size: 0,
                    page_token: String::new(),
                },
                &url,
                attacker_auth,
            ),
        )
        .await
        .unwrap();

        // Crafted `WriteCommit` + `ReadCommit` referencing victim's `root_tree_id` must NOT
        // authorize attacker for `victim_tree_id`.
        let attacker_commit_id = BackendService::write_commit(
            &proxy,
            req_with_upstream(
                WriteCommitRequest {
                    project_id: attacker_proj.to_string(),
                    commit: Some(Commit {
                        commit_id: Vec::new(),
                        change_id: vec![0x91; 16],
                        parent_commit_ids: vec![ROOT_COMMIT_ID_BYTES.to_vec()],
                        root_tree_id: vec![victim_tree_id.clone()],
                        description: "crafted commit".to_string(),
                        ..Default::default()
                    }),
                },
                &url,
                attacker_auth,
            ),
        )
        .await
        .unwrap()
        .into_inner()
        .commit_id;
        let _ = BackendService::read_commit(
            &proxy,
            req_with_upstream(
                ReadCommitRequest {
                    project_id: attacker_proj.to_string(),
                    commit_id: attacker_commit_id,
                },
                &url,
                attacker_auth,
            ),
        )
        .await
        .unwrap();

        // None of the victim's shared-CAS objects became authorized, and direct reads
        // hit upstream and fail with PermissionDenied instead of serving cached bytes.
        assert!(!cache.is_authorized(&attacker_scope, ObjectKind::File, &victim_file_id));
        assert!(!cache.is_authorized(&attacker_scope, ObjectKind::Tree, &victim_tree_id));
        assert!(!cache.is_authorized(&attacker_scope, ObjectKind::Symlink, &victim_symlink_id));

        let file_err = BackendService::read_file(
            &proxy,
            req_with_upstream(
                ReadFileRequest {
                    project_id: attacker_proj.to_string(),
                    file_id: victim_file_id.clone(),
                    offset: 0,
                    max_bytes: 0,
                },
                &url,
                attacker_auth,
            ),
        )
        .await
        .err()
        .expect("attacker ReadFile must fail on upstream ACL");
        assert_eq!(file_err.code(), tonic::Code::PermissionDenied);

        let tree_err = BackendService::read_tree(
            &proxy,
            req_with_upstream(
                ReadTreeRequest {
                    project_id: attacker_proj.to_string(),
                    path: String::new(),
                    tree_id: victim_tree_id.clone(),
                    page_size: 0,
                    page_token: String::new(),
                },
                &url,
                attacker_auth,
            ),
        )
        .await
        .unwrap_err();
        assert_eq!(tree_err.code(), tonic::Code::PermissionDenied);

        let sym_err = BackendService::read_symlink(
            &proxy,
            req_with_upstream(
                ReadSymlinkRequest {
                    project_id: attacker_proj.to_string(),
                    symlink_id: victim_symlink_id.clone(),
                },
                &url,
                attacker_auth,
            ),
        )
        .await
        .unwrap_err();
        assert_eq!(sym_err.code(), tonic::Code::PermissionDenied);
    }
}

#[tokio::test]
async fn grpc_timeout_forwarded_on_uncached_rpcs_and_omitted_on_coalesced_fetches() {
    let mock = MockUpstream::default();
    let commit_id = vec![0xaa; 20];
    mock.state.lock().unwrap().commits.insert(
        ("proj".to_string(), commit_id.clone()),
        Commit {
            commit_id: commit_id.clone(),
            change_id: vec![0x11; 16],
            root_tree_id: vec![EMPTY_TREE_ID_BYTES.to_vec()],
            description: "deadline check".to_string(),
            ..Default::default()
        },
    );
    let url = spawn_mock_upstream(mock.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 8 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new(cache, Arc::new(UpstreamPool::new(None)), None);

    // Non-coalesced RPC forwards `grpc-timeout`.
    let mut ws_req = req_with_upstream(
        GetWorkspaceRequest {
            repo_id: "proj".to_string(),
            workspace_name: "default".to_string(),
        },
        &url,
        None,
    );
    ws_req
        .metadata_mut()
        .insert("grpc-timeout", "500m".parse().unwrap());
    WorkspaceService::get_workspace(&proxy, ws_req)
        .await
        .unwrap();
    assert_eq!(
        mock.state
            .lock()
            .unwrap()
            .last_get_workspace_timeout
            .as_deref(),
        Some("500m")
    );

    // Shared singleflight RPC omits per-caller `grpc-timeout`.
    let mut commit_req = req_with_upstream(
        ReadCommitRequest {
            project_id: "proj".to_string(),
            commit_id,
        },
        &url,
        None,
    );
    commit_req
        .metadata_mut()
        .insert("grpc-timeout", "500m".parse().unwrap());
    BackendService::read_commit(&proxy, commit_req)
        .await
        .unwrap();
    assert_eq!(mock.state.lock().unwrap().last_read_commit_timeout, None);
}

#[tokio::test]
async fn write_commit_skips_auth_write_through_and_strips_spoofed_actor_keys() {
    let mock = MockUpstream::default();
    mock.state.lock().unwrap().stamp_actor_sub = Some("system:actor:oss-demo:coder-1".to_string());
    let url = spawn_mock_upstream(mock.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 4 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new(Arc::clone(&cache), Arc::new(UpstreamPool::new(None)), None);

    // Authenticated WriteCommit skips commit write-through so ReadCommit observes
    // server-stamped `actor-sub` and `actor-uid` instead of stale client-supplied bytes.
    let mut auth_commit = Commit {
        commit_id: Vec::new(),
        change_id: vec![0x71; 16],
        parent_commit_ids: vec![ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![EMPTY_TREE_ID_BYTES.to_vec()],
        description: "authenticated actor commit".to_string(),
        ..Default::default()
    };
    auth_commit
        .external_ids
        .insert("actor-sub".to_string(), b"spoofed-sub".to_vec());
    auth_commit
        .external_ids
        .insert("actor-uid".to_string(), b"spoofed-uid".to_vec());

    let auth_id = BackendService::write_commit(
        &proxy,
        req_with_upstream(
            WriteCommitRequest {
                project_id: "proj".to_string(),
                commit: Some(auth_commit),
            },
            &url,
            Some("Bearer actor-jwt"),
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .commit_id;

    let read_auth = BackendService::read_commit(
        &proxy,
        req_with_upstream(
            ReadCommitRequest {
                project_id: "proj".to_string(),
                commit_id: auth_id,
            },
            &url,
            Some("Bearer actor-jwt"),
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .commit
    .unwrap();
    assert_eq!(
        mock.state.lock().unwrap().read_commit_calls,
        1,
        "authenticated WriteCommit must not serve pre-attribution commit bytes from cache"
    );
    assert_eq!(
        read_auth.external_ids.get("actor-sub"),
        Some(&b"system:actor:oss-demo:coder-1".to_vec())
    );
    assert_eq!(
        read_auth.external_ids.get("actor-uid"),
        Some(&b"uid-server-verified".to_vec())
    );

    // Unauthenticated WriteCommit also avoids non-authoritative write-through, so the first
    // ReadCommit fetches the server-sanitized commit (spoofed actor keys stripped) and
    // caches that authoritative copy for subsequent reads.
    let mut unauth_commit = Commit {
        commit_id: Vec::new(),
        change_id: vec![0x72; 16],
        parent_commit_ids: vec![ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![EMPTY_TREE_ID_BYTES.to_vec()],
        description: "unauthenticated spoofed commit".to_string(),
        ..Default::default()
    };
    unauth_commit
        .external_ids
        .insert("actor-sub".to_string(), b"spoofed-sub".to_vec());
    unauth_commit
        .external_ids
        .insert("actor-uid".to_string(), b"spoofed-uid".to_vec());
    unauth_commit
        .external_ids
        .insert("perforce-cl".to_string(), b"42".to_vec());

    let unauth_id = BackendService::write_commit(
        &proxy,
        req_with_upstream(
            WriteCommitRequest {
                project_id: "proj".to_string(),
                commit: Some(unauth_commit),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .commit_id;

    let read_unauth = BackendService::read_commit(
        &proxy,
        req_with_upstream(
            ReadCommitRequest {
                project_id: "proj".to_string(),
                commit_id: unauth_id.clone(),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .commit
    .unwrap();
    assert_eq!(
        mock.state.lock().unwrap().read_commit_calls,
        2,
        "unauthenticated WriteCommit must fetch authoritative server-sanitized commit on first read"
    );
    assert!(!read_unauth.external_ids.contains_key("actor-sub"));
    assert!(!read_unauth.external_ids.contains_key("actor-uid"));
    assert_eq!(
        read_unauth.external_ids.get("perforce-cl"),
        Some(&b"42".to_vec())
    );

    let cached_unauth = BackendService::read_commit(
        &proxy,
        req_with_upstream(
            ReadCommitRequest {
                project_id: "proj".to_string(),
                commit_id: unauth_id,
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .commit
    .unwrap();
    assert_eq!(
        mock.state.lock().unwrap().read_commit_calls,
        2,
        "second ReadCommit should serve the cached authoritative server-sanitized commit"
    );
    assert_eq!(cached_unauth, read_unauth);
}
