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

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use cc_common::backend::backend_service_server::{BackendService, BackendServiceServer};
use cc_common::backend::*;
use cc_common::op_store::op_store_service_server::OpStoreService;
use cc_common::op_store::*;
use cc_common::{EMPTY_TREE_ID_BYTES, ROOT_COMMIT_ID_BYTES, ROOT_OPERATION_ID_BYTES};
use futures::{Stream, StreamExt};
use prost::Message;
use tonic::{Request, Response, Status, Streaming};

use super::super::DaemonProxy;
use super::{MockUpstream, git_blob_id, req_with_upstream, spawn_mock_upstream, unused};
use crate::cache::{CacheScope, ObjectKind, TwoTierCache};
use crate::upstream::{UPSTREAM_HEADER, UpstreamPool};

#[tokio::test]
async fn publish_operation_caches_view_and_operation_for_subsequent_reads() {
    let mock = MockUpstream::default();
    let url = spawn_mock_upstream(mock.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 8 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new(cache, Arc::new(UpstreamPool::new(None)), None);

    let view = View {
        head_ids: vec![vec![0xab; 20]],
        ..Default::default()
    };
    let op = Operation {
        view_id: Vec::new(),
        parents: vec![ROOT_OPERATION_ID_BYTES.to_vec()],
        metadata: Some(OperationMetadata {
            description: "published op".to_string(),
            ..Default::default()
        }),
        commit_predecessors: Vec::new(),
        commit_predecessors_set: true,
    };
    let published = OpStoreService::publish_operation(
        &proxy,
        req_with_upstream(
            PublishOperationRequest {
                repo_id: "repo-1".to_string(),
                view: Some(view.clone()),
                operation: Some(op),
                old_op_head_ids: vec![ROOT_OPERATION_ID_BYTES.to_vec()],
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner();

    let read_v = OpStoreService::read_view(
        &proxy,
        req_with_upstream(
            ReadViewRequest {
                repo_id: "repo-1".to_string(),
                view_id: published.view_id.clone(),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(read_v.view.unwrap().head_ids, view.head_ids);

    let read_op = OpStoreService::read_operation(
        &proxy,
        req_with_upstream(
            ReadOperationRequest {
                repo_id: "repo-1".to_string(),
                operation_id: published.operation_id,
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .operation
    .unwrap();
    assert_eq!(read_op.view_id, published.view_id);
    assert_eq!(read_op.metadata.unwrap().description, "published op");
    assert_eq!(mock.state.lock().unwrap().read_view_calls, 0);
    assert_eq!(mock.state.lock().unwrap().read_op_calls, 0);
}

#[tokio::test]
async fn read_view_rejects_mismatched_view_id_without_caching() {
    let mock = MockUpstream::default();
    let requested_view_id = vec![0x5a; 16];
    let wrong_view_id = vec![0x9b; 16];
    {
        let mut st = mock.state.lock().unwrap();
        st.views.insert(
            requested_view_id.clone(),
            View {
                head_ids: vec![vec![0xcd; 20]],
                ..Default::default()
            },
        );
        st.view_id_overrides
            .insert(requested_view_id.clone(), wrong_view_id);
    }
    let url = spawn_mock_upstream(mock.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 8 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new(Arc::clone(&cache), Arc::new(UpstreamPool::new(None)), None);

    let err = OpStoreService::read_view(
        &proxy,
        req_with_upstream(
            ReadViewRequest {
                repo_id: "repo-1".to_string(),
                view_id: requested_view_id.clone(),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Internal);
    assert_eq!(mock.state.lock().unwrap().read_view_calls, 1);

    // Must not have cached the mismatched response; once upstream returns the matching id,
    // a retry reaches upstream and succeeds.
    mock.state
        .lock()
        .unwrap()
        .view_id_overrides
        .remove(&requested_view_id);
    let ok = OpStoreService::read_view(
        &proxy,
        req_with_upstream(
            ReadViewRequest {
                repo_id: "repo-1".to_string(),
                view_id: requested_view_id.clone(),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(ok.view_id, requested_view_id);
    assert_eq!(ok.view.unwrap().head_ids, vec![vec![0xcd; 20]]);
    assert_eq!(mock.state.lock().unwrap().read_view_calls, 2);
}

#[tokio::test]
async fn delete_repository_passes_through_uncached_and_enforces_outbound_validation() {
    let mock = MockUpstream::default();
    {
        let mut st = mock.state.lock().unwrap();
        st.required_auth = Some("Bearer del-secret".to_string());
        st.repos
            .insert("repo-del".to_string(), "proj-alpha".to_string());
    }
    let url = spawn_mock_upstream(mock.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new(Arc::clone(&cache), Arc::new(UpstreamPool::new(None)), None);

    // Duplicate authorization headers must fail before dialing upstream.
    let mut bad_req = Request::new(DeleteRepositoryRequest {
        repo_id: "repo-del".to_string(),
        project_id: Some("proj-alpha".to_string()),
    });
    bad_req
        .metadata_mut()
        .insert(UPSTREAM_HEADER, url.parse().unwrap());
    bad_req
        .metadata_mut()
        .append("authorization", "Bearer del-secret".parse().unwrap());
    bad_req
        .metadata_mut()
        .append("authorization", "Bearer other".parse().unwrap());
    let bad_err = BackendService::delete_repository(&proxy, bad_req)
        .await
        .unwrap_err();
    assert_eq!(bad_err.code(), tonic::Code::Unauthenticated);
    assert_eq!(mock.state.lock().unwrap().delete_repository_calls, 0);
    assert_eq!(cache.stats().upstream_rpcs, 0);

    // Mismatched project_id propagates FAILED_PRECONDITION from upstream.
    let mismatch_err = BackendService::delete_repository(
        &proxy,
        req_with_upstream(
            DeleteRepositoryRequest {
                repo_id: "repo-del".to_string(),
                project_id: Some("wrong-proj".to_string()),
            },
            &url,
            Some("Bearer del-secret"),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(mismatch_err.code(), tonic::Code::FailedPrecondition);

    // Matching project_id deletes upstream and returns existed = true.
    let first = BackendService::delete_repository(
        &proxy,
        req_with_upstream(
            DeleteRepositoryRequest {
                repo_id: "repo-del".to_string(),
                project_id: Some("proj-alpha".to_string()),
            },
            &url,
            Some("Bearer del-secret"),
        ),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(first.existed);

    // Second call goes straight through uncached and returns existed = false.
    let second = BackendService::delete_repository(
        &proxy,
        req_with_upstream(
            DeleteRepositoryRequest {
                repo_id: "repo-del".to_string(),
                project_id: Some("proj-alpha".to_string()),
            },
            &url,
            Some("Bearer del-secret"),
        ),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(!second.existed);

    assert_eq!(mock.state.lock().unwrap().delete_repository_calls, 3);
    assert_eq!(cache.stats().upstream_rpcs, 3);
}

#[tokio::test]
async fn delete_repository_purges_cached_views_ops_commits_and_file_authorizations() {
    let mock = MockUpstream::default();
    let file_bytes = Bytes::from_static(b"project-a secret file");
    let file_id = git_blob_id(&file_bytes);
    let view_id = vec![0x41; 64];
    let op_id = vec![0x42; 64];
    let commit_id = vec![0x43; 20];
    {
        let mut st = mock.state.lock().unwrap();
        st.repos
            .insert("ephemeral-repo".to_string(), "proj-old".to_string());
        st.files.insert(file_id.clone(), file_bytes.clone());
        st.views.insert(
            view_id.clone(),
            View {
                head_ids: vec![commit_id.clone()],
                ..Default::default()
            },
        );
        st.ops.insert(
            op_id.clone(),
            Operation {
                view_id: view_id.clone(),
                parents: vec![ROOT_OPERATION_ID_BYTES.to_vec()],
                ..Default::default()
            },
        );
        st.commits.insert(
            ("ephemeral-repo".to_string(), commit_id.clone()),
            Commit {
                commit_id: commit_id.clone(),
                change_id: vec![0x55; 16],
                parent_commit_ids: vec![ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: vec![EMPTY_TREE_ID_BYTES.to_vec()],
                description: "old-project commit".to_string(),
                ..Default::default()
            },
        );
    }

    let url = spawn_mock_upstream(mock.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 4 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new(Arc::clone(&cache), Arc::new(UpstreamPool::new(None)), None);

    // Warm daemon cache via `ephemeral-repo` (view, op, commit, and file authorization).
    OpStoreService::read_view(
        &proxy,
        req_with_upstream(
            ReadViewRequest {
                repo_id: "ephemeral-repo".to_string(),
                view_id: view_id.clone(),
            },
            &url,
            Some("Bearer caller-1"),
        ),
    )
    .await
    .unwrap();
    OpStoreService::read_operation(
        &proxy,
        req_with_upstream(
            ReadOperationRequest {
                repo_id: "ephemeral-repo".to_string(),
                operation_id: op_id.clone(),
            },
            &url,
            Some("Bearer caller-1"),
        ),
    )
    .await
    .unwrap();
    BackendService::read_commit(
        &proxy,
        req_with_upstream(
            ReadCommitRequest {
                project_id: "ephemeral-repo".to_string(),
                commit_id: commit_id.clone(),
            },
            &url,
            Some("Bearer caller-1"),
        ),
    )
    .await
    .unwrap();
    let mut fstream = BackendService::read_file(
        &proxy,
        req_with_upstream(
            ReadFileRequest {
                project_id: "ephemeral-repo".to_string(),
                file_id: file_id.clone(),
                offset: 0,
                max_bytes: 0,
                path: String::new(),
            },
            &url,
            Some("Bearer caller-1"),
        ),
    )
    .await
    .unwrap()
    .into_inner();
    let mut got = Vec::new();
    while let Some(frame) = fstream.next().await {
        got.extend_from_slice(&frame.unwrap().chunk);
    }
    assert_eq!(got, file_bytes.as_ref());

    // Delete repository on upstream and remove its objects from mock state (simulating server
    // deletion + re-binding under a different project that does not have those objects).
    let del = BackendService::delete_repository(
        &proxy,
        req_with_upstream(
            DeleteRepositoryRequest {
                repo_id: "ephemeral-repo".to_string(),
                project_id: Some("proj-old".to_string()),
            },
            &url,
            Some("Bearer caller-2"),
        ),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(del.existed);

    {
        let mut st = mock.state.lock().unwrap();
        st.views.clear();
        st.ops.clear();
        st.commits.clear();
        st.files.clear();
    }

    // Every previously cached read for `ephemeral-repo` (even under a different caller token)
    // must miss cache and return NOT_FOUND from upstream rather than serving stale data.
    assert_eq!(
        OpStoreService::read_view(
            &proxy,
            req_with_upstream(
                ReadViewRequest {
                    repo_id: "ephemeral-repo".to_string(),
                    view_id: view_id.clone(),
                },
                &url,
                Some("Bearer caller-1"),
            ),
        )
        .await
        .unwrap_err()
        .code(),
        tonic::Code::NotFound
    );
    assert_eq!(
        OpStoreService::read_operation(
            &proxy,
            req_with_upstream(
                ReadOperationRequest {
                    repo_id: "ephemeral-repo".to_string(),
                    operation_id: op_id.clone(),
                },
                &url,
                Some("Bearer caller-1"),
            ),
        )
        .await
        .unwrap_err()
        .code(),
        tonic::Code::NotFound
    );
    assert_eq!(
        BackendService::read_commit(
            &proxy,
            req_with_upstream(
                ReadCommitRequest {
                    project_id: "ephemeral-repo".to_string(),
                    commit_id: commit_id.clone(),
                },
                &url,
                Some("Bearer caller-1"),
            ),
        )
        .await
        .unwrap_err()
        .code(),
        tonic::Code::NotFound
    );
    assert_eq!(
        BackendService::read_file(
            &proxy,
            req_with_upstream(
                ReadFileRequest {
                    project_id: "ephemeral-repo".to_string(),
                    file_id: file_id.clone(),
                    offset: 0,
                    max_bytes: 0,
                    path: String::new(),
                },
                &url,
                Some("Bearer caller-1"),
            ),
        )
        .await
        .err()
        .expect("read_file must fail after repository deletion")
        .code(),
        tonic::Code::NotFound
    );

    // Even an idempotent delete returning `existed == false` purges stale local cache state
    // (for instance when another client deleted the repository directly on the server).
    let stale_scope = CacheScope::new_with_auth(&url, "ephemeral-repo", Some("Bearer caller-1"));
    cache.put_write_through(
        &stale_scope,
        ObjectKind::View,
        &view_id,
        Bytes::from(
            View {
                head_ids: vec![commit_id.clone()],
                ..Default::default()
            }
            .encode_to_vec(),
        ),
    );
    assert!(
        cache
            .lookup_cached(&stale_scope, ObjectKind::View, &view_id)
            .is_some()
    );
    let del_again = BackendService::delete_repository(
        &proxy,
        req_with_upstream(
            DeleteRepositoryRequest {
                repo_id: "ephemeral-repo".to_string(),
                project_id: None,
            },
            &url,
            Some("Bearer caller-1"),
        ),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(!del_again.existed);
    assert_eq!(
        cache.lookup_cached(&stale_scope, ObjectKind::View, &view_id),
        None
    );
}

#[tokio::test]
async fn delete_repository_during_inflight_cold_read_file_does_not_resurrect_authorization() {
    use tokio::sync::Notify;

    struct GatedReadFileUpstream {
        file_bytes: Bytes,
        read_entered: Arc<Notify>,
        release_read: Arc<Notify>,
        deleted: Arc<AtomicBool>,
    }

    #[tonic::async_trait]
    impl BackendService for GatedReadFileUpstream {
        async fn register_project(
            &self,
            _: Request<RegisterProjectRequest>,
        ) -> Result<Response<RegisterProjectResponse>, Status> {
            unused()
        }
        async fn register_repository(
            &self,
            _: Request<RegisterRepositoryRequest>,
        ) -> Result<Response<RegisterRepositoryResponse>, Status> {
            unused()
        }
        async fn delete_repository(
            &self,
            _: Request<DeleteRepositoryRequest>,
        ) -> Result<Response<DeleteRepositoryResponse>, Status> {
            self.deleted.store(true, Ordering::SeqCst);
            Ok(Response::new(DeleteRepositoryResponse { existed: true }))
        }
        async fn read_commit(
            &self,
            _: Request<ReadCommitRequest>,
        ) -> Result<Response<ReadCommitResponse>, Status> {
            unused()
        }
        async fn write_commit(
            &self,
            _: Request<WriteCommitRequest>,
        ) -> Result<Response<WriteCommitResponse>, Status> {
            unused()
        }
        async fn list_commits(
            &self,
            _: Request<ListCommitsRequest>,
        ) -> Result<Response<ListCommitsResponse>, Status> {
            unused()
        }
        async fn resolve_commit_prefix(
            &self,
            _: Request<ResolveCommitPrefixRequest>,
        ) -> Result<Response<ResolveCommitPrefixResponse>, Status> {
            unused()
        }
        async fn resolve_change_prefix(
            &self,
            _: Request<ResolveChangePrefixRequest>,
        ) -> Result<Response<ResolveChangePrefixResponse>, Status> {
            unused()
        }
        async fn read_tree(
            &self,
            _: Request<ReadTreeRequest>,
        ) -> Result<Response<ReadTreeResponse>, Status> {
            unused()
        }
        async fn write_tree(
            &self,
            _: Request<WriteTreeRequest>,
        ) -> Result<Response<WriteTreeResponse>, Status> {
            unused()
        }
        type ReadFileStream =
            Pin<Box<dyn Stream<Item = Result<ReadFileResponse, Status>> + Send + 'static>>;
        async fn read_file(
            &self,
            _: Request<ReadFileRequest>,
        ) -> Result<Response<Self::ReadFileStream>, Status> {
            if self.deleted.load(Ordering::SeqCst) {
                return Err(Status::not_found("repository deleted"));
            }
            self.read_entered.notify_one();
            self.release_read.notified().await;
            let stream = futures::stream::iter(vec![Ok(ReadFileResponse {
                chunk: self.file_bytes.to_vec(),
            })]);
            Ok(Response::new(Box::pin(stream)))
        }
        async fn write_file(
            &self,
            _: Request<WriteFileRequest>,
        ) -> Result<Response<WriteFileResponse>, Status> {
            unused()
        }
        async fn write_file_stream(
            &self,
            _: Request<Streaming<WriteFileChunk>>,
        ) -> Result<Response<WriteFileResponse>, Status> {
            unused()
        }
        async fn read_symlink(
            &self,
            _: Request<ReadSymlinkRequest>,
        ) -> Result<Response<ReadSymlinkResponse>, Status> {
            unused()
        }
        async fn write_symlink(
            &self,
            _: Request<WriteSymlinkRequest>,
        ) -> Result<Response<WriteSymlinkResponse>, Status> {
            unused()
        }
    }

    let file_bytes = Bytes::from_static(b"in-flight cold read secret");
    let file_id = git_blob_id(&file_bytes);
    let read_entered = Arc::new(Notify::new());
    let release_read = Arc::new(Notify::new());
    let deleted = Arc::new(AtomicBool::new(false));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let incoming =
        tonic::transport::server::TcpIncoming::from_listener(listener, true, None).unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(BackendServiceServer::new(GatedReadFileUpstream {
                file_bytes: file_bytes.clone(),
                read_entered: Arc::clone(&read_entered),
                release_read: Arc::clone(&release_read),
                deleted: Arc::clone(&deleted),
            }))
            .serve_with_incoming(incoming),
    );

    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 4 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new(Arc::clone(&cache), Arc::new(UpstreamPool::new(None)), None);
    let scope = CacheScope::new_with_auth(&url, "ephemeral-repo", Some("Bearer caller-1"));

    let inflight_read = tokio::spawn({
        let proxy = proxy.clone();
        let url = url.clone();
        let file_id = file_id.clone();
        async move {
            let mut stream = BackendService::read_file(
                &proxy,
                req_with_upstream(
                    ReadFileRequest {
                        project_id: "ephemeral-repo".to_string(),
                        file_id,
                        offset: 0,
                        max_bytes: 0,
                        path: String::new(),
                    },
                    &url,
                    Some("Bearer caller-1"),
                ),
            )
            .await
            .unwrap()
            .into_inner();
            let mut got = Vec::new();
            while let Some(frame) = stream.next().await {
                got.extend_from_slice(&frame.unwrap().chunk);
            }
            got
        }
    });

    // Hold the cold `ReadFile` upstream call open while `DeleteRepository` completes and
    // purges the namespace (advancing `purge_epoch`).
    read_entered.notified().await;
    let del = BackendService::delete_repository(
        &proxy,
        req_with_upstream(
            DeleteRepositoryRequest {
                repo_id: "ephemeral-repo".to_string(),
                project_id: Some("proj-old".to_string()),
            },
            &url,
            Some("Bearer caller-2"),
        ),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(del.existed);

    // Release upstream `ReadFile`; the in-flight caller receives its bytes, but `spooler`
    // must reject caching/authorizing under the deleted repository's scope.
    release_read.notify_one();
    assert_eq!(inflight_read.await.unwrap(), file_bytes.as_ref());
    assert!(
        !cache.is_authorized(&scope, ObjectKind::File, &file_id),
        "in-flight cold ReadFile must not resurrect authorization marker after DeleteRepository"
    );

    let err = BackendService::read_file(
        &proxy,
        req_with_upstream(
            ReadFileRequest {
                project_id: "ephemeral-repo".to_string(),
                file_id,
                offset: 0,
                max_bytes: 0,
                path: String::new(),
            },
            &url,
            Some("Bearer caller-1"),
        ),
    )
    .await
    .err()
    .expect("subsequent read_file must miss cache and return NOT_FOUND");
    assert_eq!(err.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn register_repository_purges_stale_cache_after_out_of_band_delete() {
    let mock = MockUpstream::default();
    let file_bytes = Bytes::from_static(b"proj-a secret file for oob delete");
    let file_id = git_blob_id(&file_bytes);
    let view_id = vec![0x61; 64];
    let op_id = vec![0x62; 64];
    let commit_id = vec![0x63; 20];
    {
        let mut st = mock.state.lock().unwrap();
        st.repos
            .insert("reused-repo".to_string(), "proj-a".to_string());
        st.files.insert(file_id.clone(), file_bytes.clone());
        st.views.insert(
            view_id.clone(),
            View {
                head_ids: vec![commit_id.clone()],
                ..Default::default()
            },
        );
        st.ops.insert(
            op_id.clone(),
            Operation {
                view_id: view_id.clone(),
                parents: vec![ROOT_OPERATION_ID_BYTES.to_vec()],
                ..Default::default()
            },
        );
        st.commits.insert(
            ("reused-repo".to_string(), commit_id.clone()),
            Commit {
                commit_id: commit_id.clone(),
                change_id: vec![0x77; 16],
                parent_commit_ids: vec![ROOT_COMMIT_ID_BYTES.to_vec()],
                root_tree_id: vec![EMPTY_TREE_ID_BYTES.to_vec()],
                description: "proj-a commit".to_string(),
                ..Default::default()
            },
        );
    }

    let url = spawn_mock_upstream(mock.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 4 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new(Arc::clone(&cache), Arc::new(UpstreamPool::new(None)), None);

    // Warm daemon cache via `reused-repo` while bound to `proj-a`.
    OpStoreService::read_view(
        &proxy,
        req_with_upstream(
            ReadViewRequest {
                repo_id: "reused-repo".to_string(),
                view_id: view_id.clone(),
            },
            &url,
            Some("Bearer caller-a"),
        ),
    )
    .await
    .unwrap();
    OpStoreService::read_operation(
        &proxy,
        req_with_upstream(
            ReadOperationRequest {
                repo_id: "reused-repo".to_string(),
                operation_id: op_id.clone(),
            },
            &url,
            Some("Bearer caller-a"),
        ),
    )
    .await
    .unwrap();
    BackendService::read_commit(
        &proxy,
        req_with_upstream(
            ReadCommitRequest {
                project_id: "reused-repo".to_string(),
                commit_id: commit_id.clone(),
            },
            &url,
            Some("Bearer caller-a"),
        ),
    )
    .await
    .unwrap();
    let mut fstream = BackendService::read_file(
        &proxy,
        req_with_upstream(
            ReadFileRequest {
                project_id: "reused-repo".to_string(),
                file_id: file_id.clone(),
                offset: 0,
                max_bytes: 0,
                path: String::new(),
            },
            &url,
            Some("Bearer caller-a"),
        ),
    )
    .await
    .unwrap()
    .into_inner();
    let mut got = Vec::new();
    while let Some(frame) = fstream.next().await {
        got.extend_from_slice(&frame.unwrap().chunk);
    }
    assert_eq!(got, file_bytes.as_ref());

    // Another client deletes `reused-repo` directly on the server (bypassing this daemon).
    {
        let mut st = mock.state.lock().unwrap();
        st.repos.remove("reused-repo");
        st.views.clear();
        st.ops.clear();
        st.commits.clear();
        st.files.clear();
    }

    let epoch_before = cache.purge_epoch();
    let reg = BackendService::register_repository(
        &proxy,
        req_with_upstream(
            RegisterRepositoryRequest {
                project_id: "proj-b".to_string(),
                repo_id: Some("reused-repo".to_string()),
                name: None,
                hash_algorithm: HashAlgorithm::Sha1 as i32,
            },
            &url,
            Some("Bearer caller-b"),
        ),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(reg.repo_id, "reused-repo");
    assert_eq!(reg.project_id, "proj-b");
    assert!(cache.purge_epoch() > epoch_before);

    // None of `proj-a`'s cached views, ops, commits, or file authorizations may be served.
    assert_eq!(
        OpStoreService::read_view(
            &proxy,
            req_with_upstream(
                ReadViewRequest {
                    repo_id: "reused-repo".to_string(),
                    view_id,
                },
                &url,
                Some("Bearer caller-a"),
            ),
        )
        .await
        .unwrap_err()
        .code(),
        tonic::Code::NotFound
    );
    assert_eq!(
        OpStoreService::read_operation(
            &proxy,
            req_with_upstream(
                ReadOperationRequest {
                    repo_id: "reused-repo".to_string(),
                    operation_id: op_id,
                },
                &url,
                Some("Bearer caller-a"),
            ),
        )
        .await
        .unwrap_err()
        .code(),
        tonic::Code::NotFound
    );
    assert_eq!(
        BackendService::read_commit(
            &proxy,
            req_with_upstream(
                ReadCommitRequest {
                    project_id: "reused-repo".to_string(),
                    commit_id,
                },
                &url,
                Some("Bearer caller-a"),
            ),
        )
        .await
        .unwrap_err()
        .code(),
        tonic::Code::NotFound
    );
    assert_eq!(
        BackendService::read_file(
            &proxy,
            req_with_upstream(
                ReadFileRequest {
                    project_id: "reused-repo".to_string(),
                    file_id,
                    offset: 0,
                    max_bytes: 0,
                    path: String::new(),
                },
                &url,
                Some("Bearer caller-a"),
            ),
        )
        .await
        .err()
        .expect("re-registered repo must not inherit stale file authorization")
        .code(),
        tonic::Code::NotFound
    );

    // Server-allocated `repo_id: None` avoids unnecessary `purge_epoch` bumps.
    let epoch_after_explicit = cache.purge_epoch();
    let auto_reg = BackendService::register_repository(
        &proxy,
        req_with_upstream(
            RegisterRepositoryRequest {
                project_id: "proj-b".to_string(),
                repo_id: None,
                name: None,
                hash_algorithm: HashAlgorithm::Sha1 as i32,
            },
            &url,
            Some("Bearer caller-b"),
        ),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(!auto_reg.repo_id.is_empty());
    assert_eq!(cache.purge_epoch(), epoch_after_explicit);
}

#[tokio::test]
async fn delete_repository_purges_cache_on_unavailable_but_preserves_on_failed_precondition() {
    let mock = MockUpstream::default();
    let view_id = vec![0x81; 64];
    {
        let mut st = mock.state.lock().unwrap();
        st.repos
            .insert("partial-repo".to_string(), "proj-a".to_string());
        st.views.insert(
            view_id.clone(),
            View {
                head_ids: vec![vec![0xaa; 20]],
                ..Default::default()
            },
        );
    }

    let url = spawn_mock_upstream(mock.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 4 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new(Arc::clone(&cache), Arc::new(UpstreamPool::new(None)), None);
    let scope = CacheScope::new_with_auth(&url, "partial-repo", Some("Bearer tok"));

    OpStoreService::read_view(
        &proxy,
        req_with_upstream(
            ReadViewRequest {
                repo_id: "partial-repo".to_string(),
                view_id: view_id.clone(),
            },
            &url,
            Some("Bearer tok"),
        ),
    )
    .await
    .unwrap();
    assert!(
        cache
            .lookup_cached(&scope, ObjectKind::View, &view_id)
            .is_some()
    );

    // Definitive rejection (`FAILED_PRECONDITION`) must preserve cached entries.
    let epoch_before_fp = cache.purge_epoch();
    let fp_err = BackendService::delete_repository(
        &proxy,
        req_with_upstream(
            DeleteRepositoryRequest {
                repo_id: "partial-repo".to_string(),
                project_id: Some("wrong-proj".to_string()),
            },
            &url,
            Some("Bearer tok"),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(fp_err.code(), tonic::Code::FailedPrecondition);
    assert_eq!(cache.purge_epoch(), epoch_before_fp);
    assert!(
        cache
            .lookup_cached(&scope, ObjectKind::View, &view_id)
            .is_some()
    );

    // Partial-drain / transient `UNAVAILABLE` must purge the namespace before returning.
    mock.state.lock().unwrap().delete_repository_error = Some(tonic::Code::Unavailable);
    let unavail_err = BackendService::delete_repository(
        &proxy,
        req_with_upstream(
            DeleteRepositoryRequest {
                repo_id: "partial-repo".to_string(),
                project_id: Some("proj-a".to_string()),
            },
            &url,
            Some("Bearer tok"),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(unavail_err.code(), tonic::Code::Unavailable);
    assert!(cache.purge_epoch() > epoch_before_fp);
    assert_eq!(
        cache.lookup_cached(&scope, ObjectKind::View, &view_id),
        None
    );
}

#[tokio::test]
async fn delete_and_reregister_purge_giant_trees_and_block_inflight_repopulation() {
    use tokio::sync::Notify;

    let entry = |name: &str| TreeEntry {
        name: name.to_string(),
        value: Some(TreeValue {
            value: Some(tree_value::Value::TreeId(EMPTY_TREE_ID_BYTES.to_vec())),
        }),
    };
    let giant_tree_id = vec![0x91; 20];
    let giant_entries: Vec<TreeEntry> = (0..6).map(|i| entry(&format!("entry-{i:02}"))).collect();

    // DeleteRepository and explicit-repo_id RegisterRepository both evict `giant_trees` so
    // a deleted or re-bound repository never returns stale `RESOURCE_EXHAUSTED` from cache.
    let mock = MockUpstream::default();
    {
        let mut st = mock.state.lock().unwrap();
        st.repos
            .insert("giant-repo".to_string(), "proj-a".to_string());
        st.dynamic_trees
            .insert(giant_tree_id.clone(), giant_entries.clone());
    }
    let url = spawn_mock_upstream(mock.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 4 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new_with_tree_limits(
        Arc::clone(&cache),
        Arc::new(UpstreamPool::new(None)),
        None,
        4,
        1024 * 1024,
    );

    // Populate `giant_trees` and confirm a repeat unpaginated call fast-fails locally.
    let err1 = BackendService::read_tree(
        &proxy,
        req_with_upstream(
            ReadTreeRequest {
                project_id: "giant-repo".to_string(),
                path: String::new(),
                tree_id: giant_tree_id.clone(),
                page_size: 0,
                page_token: String::new(),
            },
            &url,
            Some("Bearer caller-a"),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(err1.code(), tonic::Code::ResourceExhausted);
    let rpcs_after_first = cache.stats().upstream_rpcs;
    let err2 = BackendService::read_tree(
        &proxy,
        req_with_upstream(
            ReadTreeRequest {
                project_id: "giant-repo".to_string(),
                path: String::new(),
                tree_id: giant_tree_id.clone(),
                page_size: 0,
                page_token: String::new(),
            },
            &url,
            Some("Bearer caller-a"),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(err2.code(), tonic::Code::ResourceExhausted);
    assert_eq!(cache.stats().upstream_rpcs, rpcs_after_first);

    // Delete `giant-repo` (using a different caller token) and drop the tree upstream.
    BackendService::delete_repository(
        &proxy,
        req_with_upstream(
            DeleteRepositoryRequest {
                repo_id: "giant-repo".to_string(),
                project_id: Some("proj-a".to_string()),
            },
            &url,
            Some("Bearer admin"),
        ),
    )
    .await
    .unwrap();
    mock.state
        .lock()
        .unwrap()
        .dynamic_trees
        .remove(&giant_tree_id);

    // Next unpaginated `ReadTree` must hit upstream and observe `NOT_FOUND`, not stale
    // `RESOURCE_EXHAUSTED` from `giant_trees`.
    let after_del = BackendService::read_tree(
        &proxy,
        req_with_upstream(
            ReadTreeRequest {
                project_id: "giant-repo".to_string(),
                path: String::new(),
                tree_id: giant_tree_id.clone(),
                page_size: 0,
                page_token: String::new(),
            },
            &url,
            Some("Bearer caller-a"),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(after_del.code(), tonic::Code::NotFound);

    // Re-populate `giant_trees`, simulate an out-of-band delete on upstream, then re-register
    // explicit `repo_id` under `proj-b`: `giant_trees` must be evicted again.
    mock.state
        .lock()
        .unwrap()
        .dynamic_trees
        .insert(giant_tree_id.clone(), giant_entries.clone());
    assert_eq!(
        BackendService::read_tree(
            &proxy,
            req_with_upstream(
                ReadTreeRequest {
                    project_id: "giant-repo".to_string(),
                    path: String::new(),
                    tree_id: giant_tree_id.clone(),
                    page_size: 0,
                    page_token: String::new(),
                },
                &url,
                Some("Bearer caller-a"),
            ),
        )
        .await
        .unwrap_err()
        .code(),
        tonic::Code::ResourceExhausted
    );
    mock.state
        .lock()
        .unwrap()
        .dynamic_trees
        .remove(&giant_tree_id);
    BackendService::register_repository(
        &proxy,
        req_with_upstream(
            RegisterRepositoryRequest {
                project_id: "proj-b".to_string(),
                repo_id: Some("giant-repo".to_string()),
                name: None,
                hash_algorithm: HashAlgorithm::Sha1 as i32,
            },
            &url,
            Some("Bearer caller-b"),
        ),
    )
    .await
    .unwrap();
    let after_rereg = BackendService::read_tree(
        &proxy,
        req_with_upstream(
            ReadTreeRequest {
                project_id: "giant-repo".to_string(),
                path: String::new(),
                tree_id: giant_tree_id.clone(),
                page_size: 0,
                page_token: String::new(),
            },
            &url,
            Some("Bearer caller-a"),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(after_rereg.code(), tonic::Code::NotFound);

    // An in-flight `ReadTree` on a giant tree racing `DeleteRepository` must not repopulate
    // `giant_trees` once `purge_epoch` has advanced.
    struct GatedGiantTreeUpstream {
        entries: Vec<TreeEntry>,
        read_entered: Arc<Notify>,
        release_read: Arc<Notify>,
        deleted: Arc<AtomicBool>,
    }

    #[tonic::async_trait]
    impl BackendService for GatedGiantTreeUpstream {
        async fn register_project(
            &self,
            _: Request<RegisterProjectRequest>,
        ) -> Result<Response<RegisterProjectResponse>, Status> {
            unused()
        }
        async fn register_repository(
            &self,
            _: Request<RegisterRepositoryRequest>,
        ) -> Result<Response<RegisterRepositoryResponse>, Status> {
            unused()
        }
        async fn delete_repository(
            &self,
            _: Request<DeleteRepositoryRequest>,
        ) -> Result<Response<DeleteRepositoryResponse>, Status> {
            self.deleted.store(true, Ordering::SeqCst);
            Ok(Response::new(DeleteRepositoryResponse { existed: true }))
        }
        async fn read_commit(
            &self,
            _: Request<ReadCommitRequest>,
        ) -> Result<Response<ReadCommitResponse>, Status> {
            unused()
        }
        async fn write_commit(
            &self,
            _: Request<WriteCommitRequest>,
        ) -> Result<Response<WriteCommitResponse>, Status> {
            unused()
        }
        async fn list_commits(
            &self,
            _: Request<ListCommitsRequest>,
        ) -> Result<Response<ListCommitsResponse>, Status> {
            unused()
        }
        async fn resolve_commit_prefix(
            &self,
            _: Request<ResolveCommitPrefixRequest>,
        ) -> Result<Response<ResolveCommitPrefixResponse>, Status> {
            unused()
        }
        async fn resolve_change_prefix(
            &self,
            _: Request<ResolveChangePrefixRequest>,
        ) -> Result<Response<ResolveChangePrefixResponse>, Status> {
            unused()
        }
        async fn read_tree(
            &self,
            req: Request<ReadTreeRequest>,
        ) -> Result<Response<ReadTreeResponse>, Status> {
            if self.deleted.load(Ordering::SeqCst) {
                return Err(Status::not_found("repo deleted"));
            }
            self.read_entered.notify_one();
            self.release_read.notified().await;
            Ok(Response::new(ReadTreeResponse {
                tree_id: req.into_inner().tree_id,
                entries: self.entries.clone(),
                next_page_token: String::new(),
            }))
        }
        async fn write_tree(
            &self,
            _: Request<WriteTreeRequest>,
        ) -> Result<Response<WriteTreeResponse>, Status> {
            unused()
        }
        type ReadFileStream =
            Pin<Box<dyn Stream<Item = Result<ReadFileResponse, Status>> + Send + 'static>>;
        async fn read_file(
            &self,
            _: Request<ReadFileRequest>,
        ) -> Result<Response<Self::ReadFileStream>, Status> {
            unused()
        }
        async fn write_file(
            &self,
            _: Request<WriteFileRequest>,
        ) -> Result<Response<WriteFileResponse>, Status> {
            unused()
        }
        async fn write_file_stream(
            &self,
            _: Request<Streaming<WriteFileChunk>>,
        ) -> Result<Response<WriteFileResponse>, Status> {
            unused()
        }
        async fn read_symlink(
            &self,
            _: Request<ReadSymlinkRequest>,
        ) -> Result<Response<ReadSymlinkResponse>, Status> {
            unused()
        }
        async fn write_symlink(
            &self,
            _: Request<WriteSymlinkRequest>,
        ) -> Result<Response<WriteSymlinkResponse>, Status> {
            unused()
        }
    }

    let read_entered = Arc::new(Notify::new());
    let release_read = Arc::new(Notify::new());
    let deleted = Arc::new(AtomicBool::new(false));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gated_url = format!("http://{}", listener.local_addr().unwrap());
    let incoming =
        tonic::transport::server::TcpIncoming::from_listener(listener, true, None).unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(BackendServiceServer::new(GatedGiantTreeUpstream {
                entries: giant_entries,
                read_entered: Arc::clone(&read_entered),
                release_read: Arc::clone(&release_read),
                deleted: Arc::clone(&deleted),
            }))
            .serve_with_incoming(incoming),
    );

    let inflight = tokio::spawn({
        let proxy = proxy.clone();
        let gated_url = gated_url.clone();
        let tree_id = giant_tree_id.clone();
        async move {
            BackendService::read_tree(
                &proxy,
                req_with_upstream(
                    ReadTreeRequest {
                        project_id: "race-giant-repo".to_string(),
                        path: String::new(),
                        tree_id,
                        page_size: 0,
                        page_token: String::new(),
                    },
                    &gated_url,
                    Some("Bearer caller-a"),
                ),
            )
            .await
        }
    });

    read_entered.notified().await;
    BackendService::delete_repository(
        &proxy,
        req_with_upstream(
            DeleteRepositoryRequest {
                repo_id: "race-giant-repo".to_string(),
                project_id: None,
            },
            &gated_url,
            Some("Bearer admin"),
        ),
    )
    .await
    .unwrap();
    release_read.notify_one();
    assert_eq!(
        inflight.await.unwrap().unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );

    // Follow-up unpaginated ReadTree must bypass `giant_trees` and hit upstream (`NOT_FOUND`).
    let post_race = BackendService::read_tree(
        &proxy,
        req_with_upstream(
            ReadTreeRequest {
                project_id: "race-giant-repo".to_string(),
                path: String::new(),
                tree_id: giant_tree_id,
                page_size: 0,
                page_token: String::new(),
            },
            &gated_url,
            Some("Bearer caller-a"),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(post_race.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn cancelled_delete_repository_purges_local_cache_and_giant_trees() {
    use tokio::sync::Notify;

    struct HangingDeleteUpstream {
        view: View,
        giant_entries: Vec<TreeEntry>,
        delete_entered: Arc<Notify>,
        deleted: Arc<AtomicBool>,
    }

    #[tonic::async_trait]
    impl BackendService for HangingDeleteUpstream {
        async fn register_project(
            &self,
            _: Request<RegisterProjectRequest>,
        ) -> Result<Response<RegisterProjectResponse>, Status> {
            unused()
        }
        async fn register_repository(
            &self,
            _: Request<RegisterRepositoryRequest>,
        ) -> Result<Response<RegisterRepositoryResponse>, Status> {
            unused()
        }
        async fn delete_repository(
            &self,
            _: Request<DeleteRepositoryRequest>,
        ) -> Result<Response<DeleteRepositoryResponse>, Status> {
            self.deleted.store(true, Ordering::SeqCst);
            self.delete_entered.notify_one();
            std::future::pending::<()>().await;
            unreachable!()
        }
        async fn read_commit(
            &self,
            _: Request<ReadCommitRequest>,
        ) -> Result<Response<ReadCommitResponse>, Status> {
            unused()
        }
        async fn write_commit(
            &self,
            _: Request<WriteCommitRequest>,
        ) -> Result<Response<WriteCommitResponse>, Status> {
            unused()
        }
        async fn list_commits(
            &self,
            _: Request<ListCommitsRequest>,
        ) -> Result<Response<ListCommitsResponse>, Status> {
            unused()
        }
        async fn resolve_commit_prefix(
            &self,
            _: Request<ResolveCommitPrefixRequest>,
        ) -> Result<Response<ResolveCommitPrefixResponse>, Status> {
            unused()
        }
        async fn resolve_change_prefix(
            &self,
            _: Request<ResolveChangePrefixRequest>,
        ) -> Result<Response<ResolveChangePrefixResponse>, Status> {
            unused()
        }
        async fn read_tree(
            &self,
            req: Request<ReadTreeRequest>,
        ) -> Result<Response<ReadTreeResponse>, Status> {
            if self.deleted.load(Ordering::SeqCst) {
                return Err(Status::not_found("repo deleted"));
            }
            Ok(Response::new(ReadTreeResponse {
                tree_id: req.into_inner().tree_id,
                entries: self.giant_entries.clone(),
                next_page_token: String::new(),
            }))
        }
        async fn write_tree(
            &self,
            _: Request<WriteTreeRequest>,
        ) -> Result<Response<WriteTreeResponse>, Status> {
            unused()
        }
        type ReadFileStream =
            Pin<Box<dyn Stream<Item = Result<ReadFileResponse, Status>> + Send + 'static>>;
        async fn read_file(
            &self,
            _: Request<ReadFileRequest>,
        ) -> Result<Response<Self::ReadFileStream>, Status> {
            unused()
        }
        async fn write_file(
            &self,
            _: Request<WriteFileRequest>,
        ) -> Result<Response<WriteFileResponse>, Status> {
            unused()
        }
        async fn write_file_stream(
            &self,
            _: Request<Streaming<WriteFileChunk>>,
        ) -> Result<Response<WriteFileResponse>, Status> {
            unused()
        }
        async fn read_symlink(
            &self,
            _: Request<ReadSymlinkRequest>,
        ) -> Result<Response<ReadSymlinkResponse>, Status> {
            unused()
        }
        async fn write_symlink(
            &self,
            _: Request<WriteSymlinkRequest>,
        ) -> Result<Response<WriteSymlinkResponse>, Status> {
            unused()
        }
    }

    #[tonic::async_trait]
    impl OpStoreService for HangingDeleteUpstream {
        async fn read_view(
            &self,
            req: Request<ReadViewRequest>,
        ) -> Result<Response<ReadViewResponse>, Status> {
            if self.deleted.load(Ordering::SeqCst) {
                return Err(Status::not_found("repo deleted"));
            }
            Ok(Response::new(ReadViewResponse {
                view_id: req.into_inner().view_id,
                view: Some(self.view.clone()),
            }))
        }
        async fn write_view(
            &self,
            _: Request<WriteViewRequest>,
        ) -> Result<Response<WriteViewResponse>, Status> {
            unused()
        }
        async fn read_operation(
            &self,
            _: Request<ReadOperationRequest>,
        ) -> Result<Response<ReadOperationResponse>, Status> {
            unused()
        }
        async fn write_operation(
            &self,
            _: Request<WriteOperationRequest>,
        ) -> Result<Response<WriteOperationResponse>, Status> {
            unused()
        }
        async fn get_op_heads(
            &self,
            _: Request<GetOpHeadsRequest>,
        ) -> Result<Response<GetOpHeadsResponse>, Status> {
            unused()
        }
        async fn update_op_heads(
            &self,
            _: Request<UpdateOpHeadsRequest>,
        ) -> Result<Response<UpdateOpHeadsResponse>, Status> {
            unused()
        }
        async fn resolve_operation_prefix(
            &self,
            _: Request<ResolveOperationPrefixRequest>,
        ) -> Result<Response<ResolveOperationPrefixResponse>, Status> {
            unused()
        }
        async fn publish_operation(
            &self,
            _: Request<PublishOperationRequest>,
        ) -> Result<Response<PublishOperationResponse>, Status> {
            unused()
        }
        async fn reconcile_op_heads(
            &self,
            _: Request<ReconcileOpHeadsRequest>,
        ) -> Result<Response<ReconcileOpHeadsResponse>, Status> {
            unused()
        }
    }

    let view_id = vec![0xa1; 64];
    let tree_id = vec![0xa2; 20];
    let giant_entries: Vec<TreeEntry> = (0..6)
        .map(|i| TreeEntry {
            name: format!("e-{i:02}"),
            value: Some(TreeValue {
                value: Some(tree_value::Value::TreeId(EMPTY_TREE_ID_BYTES.to_vec())),
            }),
        })
        .collect();
    let delete_entered = Arc::new(Notify::new());
    let deleted = Arc::new(AtomicBool::new(false));
    let upstream_a = HangingDeleteUpstream {
        view: View {
            head_ids: vec![vec![0xaa; 20]],
            ..Default::default()
        },
        giant_entries: giant_entries.clone(),
        delete_entered: Arc::clone(&delete_entered),
        deleted: Arc::clone(&deleted),
    };
    let upstream_b = HangingDeleteUpstream {
        view: View {
            head_ids: vec![vec![0xaa; 20]],
            ..Default::default()
        },
        giant_entries,
        delete_entered: Arc::clone(&delete_entered),
        deleted: Arc::clone(&deleted),
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let incoming =
        tonic::transport::server::TcpIncoming::from_listener(listener, true, None).unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(BackendServiceServer::new(upstream_a))
            .add_service(
                cc_common::op_store::op_store_service_server::OpStoreServiceServer::new(upstream_b),
            )
            .serve_with_incoming(incoming),
    );

    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 4 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new_with_tree_limits(
        Arc::clone(&cache),
        Arc::new(UpstreamPool::new(None)),
        None,
        4,
        1024 * 1024,
    );
    let scope = CacheScope::new_with_auth(&url, "cancel-repo", Some("Bearer tok"));

    // Warm both the view cache (L1 + L2) and `giant_trees`.
    OpStoreService::read_view(
        &proxy,
        req_with_upstream(
            ReadViewRequest {
                repo_id: "cancel-repo".to_string(),
                view_id: view_id.clone(),
            },
            &url,
            Some("Bearer tok"),
        ),
    )
    .await
    .unwrap();
    assert!(
        cache
            .lookup_cached(&scope, ObjectKind::View, &view_id)
            .is_some()
    );
    assert_eq!(
        BackendService::read_tree(
            &proxy,
            req_with_upstream(
                ReadTreeRequest {
                    project_id: "cancel-repo".to_string(),
                    path: String::new(),
                    tree_id: tree_id.clone(),
                    page_size: 0,
                    page_token: String::new(),
                },
                &url,
                Some("Bearer tok"),
            ),
        )
        .await
        .unwrap_err()
        .code(),
        tonic::Code::ResourceExhausted
    );

    let epoch_before = cache.purge_epoch();
    let delete_handle = tokio::spawn({
        let proxy = proxy.clone();
        let url = url.clone();
        async move {
            BackendService::delete_repository(
                &proxy,
                req_with_upstream(
                    DeleteRepositoryRequest {
                        repo_id: "cancel-repo".to_string(),
                        project_id: None,
                    },
                    &url,
                    Some("Bearer tok"),
                ),
            )
            .await
        }
    });

    delete_entered.notified().await;
    delete_handle.abort();
    let _ = delete_handle.await;

    // The drop guard must immediately advance `purge_epoch`, suppress L1/L2 view reads before
    // waiting on background disk removal, and clear `giant_trees`.
    assert!(cache.purge_epoch() > epoch_before);
    assert_eq!(
        cache.lookup_cached(&scope, ObjectKind::View, &view_id),
        None,
        "immediate lookup_cached after DeleteRepository cancellation must not read stale L2"
    );
    let immediate_view = OpStoreService::read_view(
        &proxy,
        req_with_upstream(
            ReadViewRequest {
                repo_id: "cancel-repo".to_string(),
                view_id: view_id.clone(),
            },
            &url,
            Some("Bearer tok"),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(immediate_view.code(), tonic::Code::NotFound);

    let tree_after_cancel = BackendService::read_tree(
        &proxy,
        req_with_upstream(
            ReadTreeRequest {
                project_id: "cancel-repo".to_string(),
                path: String::new(),
                tree_id,
                page_size: 0,
                page_token: String::new(),
            },
            &url,
            Some("Bearer tok"),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(tree_after_cancel.code(), tonic::Code::NotFound);

    let ns_dir = dir.path().join("scopes").join(scope.namespace_hex());
    for _ in 0..40 {
        if !ns_dir.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        !ns_dir.exists(),
        "background purge from DeletePurgeGuard must delete L2 scope directory"
    );
    assert_eq!(
        cache.lookup_cached(&scope, ObjectKind::View, &view_id),
        None
    );
    let view_after_cancel = OpStoreService::read_view(
        &proxy,
        req_with_upstream(
            ReadViewRequest {
                repo_id: "cancel-repo".to_string(),
                view_id,
            },
            &url,
            Some("Bearer tok"),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(view_after_cancel.code(), tonic::Code::NotFound);
}
