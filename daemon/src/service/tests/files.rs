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

use std::fs;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use cc_common::backend::backend_service_client::BackendServiceClient;
use cc_common::backend::backend_service_server::{BackendService, BackendServiceServer};
use cc_common::backend::*;
use futures::{Stream, StreamExt};
use sha1::{Digest, Sha1};
use tonic::transport::Channel;
use tonic::{Request, Response, Status, Streaming};

use super::super::DaemonProxy;
use super::super::backend::decode_symlink_target;
use super::{
    FileOnlyUpstream, MockUpstream, git_blob_id, req_with_upstream, slice_file_range_frames,
    spawn_mock_upstream, unused,
};
use crate::cache::{
    CACHE_STREAM_CHUNK_BYTES, CacheScope, DEFAULT_MAX_L2_OBJECT_BYTES, MAX_L1_OBJECT_BYTES,
    ObjectKind, TwoTierCache,
};
use crate::upstream::{UPSTREAM_HEADER, UpstreamPool};

#[tokio::test]
async fn cold_read_file_serves_fetched_bytes_that_neither_cache_tier_kept() {
    let content = Bytes::from(
        (0..150_000u32)
            .map(|i| (i % 251) as u8)
            .collect::<Vec<u8>>(),
    );
    let file_id = {
        let mut hasher = Sha1::new();
        hasher.update(format!("blob {}\0", content.len()).as_bytes());
        hasher.update(&content);
        hasher.finalize().to_vec()
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_url = format!("http://{}", listener.local_addr().unwrap());
    let incoming =
        tonic::transport::server::TcpIncoming::from_listener(listener, true, None).unwrap();
    let upstream = FileOnlyUpstream {
        content: content.clone(),
    };
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(BackendServiceServer::new(upstream))
            .serve_with_incoming(incoming),
    );

    // L1 holds at most 4 bytes and `objects` is a regular file, so every L2 write fails
    // and the fetched blob ends up in neither tier.
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 4).unwrap());
    fs::remove_dir(dir.path().join("objects")).unwrap();
    fs::write(dir.path().join("objects"), b"").unwrap();
    let proxy = DaemonProxy::new(Arc::clone(&cache), Arc::new(UpstreamPool::new(None)), None);

    for (offset, max_bytes) in [(0u64, 0u64), (100, 70_000)] {
        let mut request = Request::new(ReadFileRequest {
            project_id: "repo".to_string(),
            file_id: file_id.clone(),
            offset,
            max_bytes,
        });
        request
            .metadata_mut()
            .insert(UPSTREAM_HEADER, upstream_url.parse().unwrap());
        let frames: Vec<_> = BackendService::read_file(&proxy, request)
            .await
            .unwrap()
            .into_inner()
            .collect()
            .await;
        assert!(
            frames.len() >= 2,
            "expected multi-chunk lazy streaming for range ({offset}, {max_bytes})"
        );
        let mut body = Vec::new();
        for frame in frames {
            let chunk = frame.unwrap().chunk;
            assert!(chunk.len() <= CACHE_STREAM_CHUNK_BYTES);
            body.extend_from_slice(&chunk);
        }
        let end = if max_bytes == 0 {
            content.len()
        } else {
            (offset + max_bytes) as usize
        };
        assert_eq!(body, &content[offset as usize..end]);
    }
    // Only the full cold miss `(0, 0)` attempts L2 caching; the ranged cold miss forwards
    // directly to upstream without touching L2.
    assert_eq!(cache.stats().l2_write_errors, 1);
}

#[tokio::test]
async fn write_file_stream_early_rejects_disallowed_upstream_validates_errors_and_writes_through() {
    let mock = MockUpstream::default();
    let upstream_url = spawn_mock_upstream(mock.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 8 * 1024 * 1024).unwrap());
    let pool = Arc::new(UpstreamPool::with_allowlist(None, [upstream_url.as_str()]).unwrap());
    let proxy = DaemonProxy::new(Arc::clone(&cache), pool, None);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_url = format!("http://{}", listener.local_addr().unwrap());
    let incoming =
        tonic::transport::server::TcpIncoming::from_listener(listener, true, None).unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(BackendServiceServer::new(proxy.clone()))
            .serve_with_incoming(incoming),
    );

    let channel = tonic::transport::Endpoint::from_shared(proxy_url)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = BackendServiceClient::new(channel);

    // Disallowed upstream fails immediately BEFORE reading any inbound chunks.
    let (_hold_tx, rx) = tokio::sync::mpsc::channel::<WriteFileChunk>(1);
    let mut disallowed_req = Request::new(futures::stream::unfold(rx, |mut r| async move {
        r.recv().await.map(|item| (item, r))
    }));
    disallowed_req.metadata_mut().insert(
        UPSTREAM_HEADER,
        "http://169.254.169.254:80".parse().unwrap(),
    );
    let err = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        client.write_file_stream(disallowed_req),
    )
    .await
    .expect("disallowed WriteFileStream must fail immediately without waiting for chunks")
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
    assert_eq!(mock.state.lock().unwrap().write_file_stream_calls, 0);

    let send_frames = |mut c: BackendServiceClient<Channel>,
                       url: String,
                       frames: Vec<WriteFileChunk>| async move {
        let mut req = Request::new(futures::stream::iter(frames));
        req.metadata_mut()
            .insert(UPSTREAM_HEADER, url.parse().unwrap());
        c.write_file_stream(req).await
    };

    // Frame-0 validation errors fail BEFORE contacting upstream.
    assert_eq!(
        send_frames(client.clone(), upstream_url.clone(), vec![])
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        send_frames(
            client.clone(),
            upstream_url.clone(),
            vec![WriteFileChunk {
                project_id: "   ".to_string(),
                chunk: b"hi".to_vec(),
                total_size: None,
                expected_file_id: None,
            }]
        )
        .await
        .unwrap_err()
        .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        send_frames(
            client.clone(),
            upstream_url.clone(),
            vec![WriteFileChunk {
                project_id: "proj".to_string(),
                chunk: b"hi".to_vec(),
                total_size: None,
                expected_file_id: Some(vec![0x01; 5]),
            }]
        )
        .await
        .unwrap_err()
        .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        mock.state.lock().unwrap().write_file_stream_calls,
        0,
        "frame-0 validation errors must fail before opening upstream stream"
    );

    // Mid-stream and EOF validation errors fail with InvalidArgument without caching or
    // leaking staging temp files. Declared `total_size` above `DEFAULT_MAX_L2_OBJECT_BYTES`
    // is NOT rejected with `ResourceExhausted`; here the 0-byte payload fails truncation.
    assert_eq!(
        send_frames(
            client.clone(),
            upstream_url.clone(),
            vec![WriteFileChunk {
                project_id: "proj".to_string(),
                chunk: Vec::new(),
                total_size: Some(DEFAULT_MAX_L2_OBJECT_BYTES + 1),
                expected_file_id: None,
            }]
        )
        .await
        .unwrap_err()
        .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        send_frames(
            client.clone(),
            upstream_url.clone(),
            vec![
                WriteFileChunk {
                    project_id: "proj".to_string(),
                    chunk: b"ab".to_vec(),
                    total_size: Some(10),
                    expected_file_id: None,
                },
                WriteFileChunk {
                    project_id: "proj".to_string(),
                    chunk: b"cd".to_vec(),
                    total_size: None,
                    expected_file_id: None,
                },
            ]
        )
        .await
        .unwrap_err()
        .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        send_frames(
            client.clone(),
            upstream_url.clone(),
            vec![
                WriteFileChunk {
                    project_id: "proj".to_string(),
                    chunk: b"ab".to_vec(),
                    total_size: Some(2),
                    expected_file_id: None,
                },
                WriteFileChunk {
                    project_id: "proj".to_string(),
                    chunk: b"c".to_vec(),
                    total_size: None,
                    expected_file_id: None,
                },
            ]
        )
        .await
        .unwrap_err()
        .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        send_frames(
            client.clone(),
            upstream_url.clone(),
            vec![
                WriteFileChunk {
                    project_id: "proj".to_string(),
                    chunk: b"a".to_vec(),
                    total_size: None,
                    expected_file_id: None,
                },
                WriteFileChunk {
                    project_id: "other-proj".to_string(),
                    chunk: b"b".to_vec(),
                    total_size: None,
                    expected_file_id: None,
                },
            ]
        )
        .await
        .unwrap_err()
        .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(cache.stats().write_through_objects, 0);
    assert_eq!(cache.stats().l1_entries, 0);
    assert_eq!(
        fs::read_dir(cache.staging_dir()).unwrap().count(),
        0,
        "failed WriteFileStream calls must clean up staging temp files"
    );

    // Valid multi-chunk stream succeeds and writes through to cache.
    // Snapshot `write_file_stream_calls` before the valid call because `MockUpstream` uses
    // `tonic::Streaming`, which translates HTTP/2 `RST_STREAM(CANCEL)` (`Code::Cancelled`) on
    // request bodies into `Ok(None)` in a race with handler cancellation; deterministic
    // `RST_STREAM` vs clean EOF verification lives in
    // `write_file_stream_cancels_upstream_with_rst_stream_and_reuses_channel`.
    let calls_before_valid = mock.state.lock().unwrap().write_file_stream_calls;
    let payload = b"streamed-file-payload-1234567890";
    let expected_id = git_blob_id(payload);
    let resp = send_frames(
        client.clone(),
        upstream_url.clone(),
        vec![
            WriteFileChunk {
                project_id: "proj".to_string(),
                chunk: payload[..10].to_vec(),
                total_size: Some(payload.len() as u64),
                expected_file_id: Some(expected_id.clone()),
            },
            WriteFileChunk {
                project_id: String::new(),
                chunk: payload[10..].to_vec(),
                total_size: None,
                expected_file_id: None,
            },
        ],
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(resp.file_id, expected_id);
    assert_eq!(
        mock.state.lock().unwrap().write_file_stream_calls,
        calls_before_valid + 1
    );
    assert_eq!(cache.stats().write_through_objects, 1);
    assert_eq!(fs::read_dir(cache.staging_dir()).unwrap().count(), 0);

    let frames: Vec<_> = client
        .read_file(req_with_upstream(
            ReadFileRequest {
                project_id: "proj".to_string(),
                file_id: expected_id,
                offset: 0,
                max_bytes: 0,
            },
            &upstream_url,
            None,
        ))
        .await
        .unwrap()
        .into_inner()
        .collect()
        .await;
    let read_back: Vec<u8> = frames.into_iter().flat_map(|f| f.unwrap().chunk).collect();
    assert_eq!(read_back, payload);
    assert_eq!(
        mock.state.lock().unwrap().read_file_calls,
        0,
        "ReadFile after WriteFileStream must hit write-through cache"
    );
}

#[tokio::test]
async fn read_file_out_of_range_and_eof_empty_stream() {
    let mock = MockUpstream::default();
    let empty_id = git_blob_id(b"");
    let body = Bytes::from_static(b"abcdef");
    let file_id = git_blob_id(&body);
    let cold_eof_body = Bytes::from_static(b"123456");
    let cold_eof_id = git_blob_id(&cold_eof_body);
    {
        let mut st = mock.state.lock().unwrap();
        st.files.insert(empty_id.clone(), Bytes::new());
        st.files.insert(file_id.clone(), body);
        st.files.insert(cold_eof_id.clone(), cold_eof_body);
    }
    let url = spawn_mock_upstream(mock.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 8 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new(cache, Arc::new(UpstreamPool::new(None)), None);

    // Empty file at offset 0 returns 0 frames (cold miss, then cached hit).
    for _ in 0..2 {
        let frames: Vec<_> = BackendService::read_file(
            &proxy,
            req_with_upstream(
                ReadFileRequest {
                    project_id: "proj".to_string(),
                    file_id: empty_id.clone(),
                    offset: 0,
                    max_bytes: 0,
                },
                &url,
                None,
            ),
        )
        .await
        .unwrap()
        .into_inner()
        .collect()
        .await;
        assert!(frames.is_empty());
    }
    // Empty file at offset 1 returns OUT_OF_RANGE from cache.
    let err = BackendService::read_file(
        &proxy,
        req_with_upstream(
            ReadFileRequest {
                project_id: "proj".to_string(),
                file_id: empty_id,
                offset: 1,
                max_bytes: 0,
            },
            &url,
            None,
        ),
    )
    .await
    .err()
    .expect("offset 1 on empty file must fail");
    assert_eq!(err.code(), tonic::Code::OutOfRange);

    // Cold ranged miss past EOF forwards to upstream (returns OUT_OF_RANGE without caching).
    let err = BackendService::read_file(
        &proxy,
        req_with_upstream(
            ReadFileRequest {
                project_id: "proj".to_string(),
                file_id: file_id.clone(),
                offset: 7,
                max_bytes: 0,
            },
            &url,
            None,
        ),
    )
    .await
    .err()
    .expect("cold offset 7 on 6-byte file must fail");
    assert_eq!(err.code(), tonic::Code::OutOfRange);

    // Warm `file_id` via full ReadFile, then verify cached hit past EOF returns OUT_OF_RANGE
    // and cached hit at offset == len emits 0 frames without touching upstream again.
    let warm_frames: Vec<_> = BackendService::read_file(
        &proxy,
        req_with_upstream(
            ReadFileRequest {
                project_id: "proj".to_string(),
                file_id: file_id.clone(),
                offset: 0,
                max_bytes: 0,
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .collect()
    .await;
    let warm_body: Vec<u8> = warm_frames
        .into_iter()
        .flat_map(|f| f.unwrap().chunk)
        .collect();
    assert_eq!(warm_body, b"abcdef");
    let calls_after_warm = mock.state.lock().unwrap().read_file_calls;

    let err = BackendService::read_file(
        &proxy,
        req_with_upstream(
            ReadFileRequest {
                project_id: "proj".to_string(),
                file_id: file_id.clone(),
                offset: 7,
                max_bytes: 0,
            },
            &url,
            None,
        ),
    )
    .await
    .err()
    .expect("cached offset 7 on 6-byte file must fail");
    assert_eq!(err.code(), tonic::Code::OutOfRange);
    // Cached hit at offset == len emits 0 frames.
    let cached_eof: Vec<_> = BackendService::read_file(
        &proxy,
        req_with_upstream(
            ReadFileRequest {
                project_id: "proj".to_string(),
                file_id,
                offset: 6,
                max_bytes: 10,
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .collect()
    .await;
    assert!(cached_eof.is_empty());
    assert_eq!(mock.state.lock().unwrap().read_file_calls, calls_after_warm);

    // Cold miss at offset == len also emits 0 frames.
    let cold_eof: Vec<_> = BackendService::read_file(
        &proxy,
        req_with_upstream(
            ReadFileRequest {
                project_id: "proj".to_string(),
                file_id: cold_eof_id,
                offset: 6,
                max_bytes: 10,
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .collect()
    .await;
    assert!(cold_eof.is_empty());
}

#[tokio::test]
async fn read_symlink_rejects_nul_and_invalid_utf8_with_data_loss() {
    assert_eq!(decode_symlink_target(b"../ok").unwrap(), "../ok");
    assert_eq!(
        decode_symlink_target(b"bad\0link").unwrap_err().code(),
        tonic::Code::DataLoss
    );
    assert_eq!(
        decode_symlink_target(&[0xff, 0xfe]).unwrap_err().code(),
        tonic::Code::DataLoss
    );

    let mock = MockUpstream::default();
    let bad_id = git_blob_id(b"corrupt\0target");
    mock.state
        .lock()
        .unwrap()
        .symlinks
        .insert(bad_id.clone(), "corrupt\0target".to_string());
    let url = spawn_mock_upstream(mock).await;
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 8 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new(cache, Arc::new(UpstreamPool::new(None)), None);

    let err = BackendService::read_symlink(
        &proxy,
        req_with_upstream(
            ReadSymlinkRequest {
                project_id: "proj".to_string(),
                symlink_id: bad_id,
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::DataLoss);
}

#[tokio::test]
async fn ranged_cold_read_file_forwards_exact_range_without_fetching_full_file() {
    let mock = MockUpstream::default();
    let content = Bytes::from(
        (0..200_000u32)
            .map(|i| (i % 251) as u8)
            .collect::<Vec<u8>>(),
    );
    let file_id = git_blob_id(&content);
    mock.state
        .lock()
        .unwrap()
        .files
        .insert(file_id.clone(), content.clone());

    let url = spawn_mock_upstream(mock.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 8 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new(Arc::clone(&cache), Arc::new(UpstreamPool::new(None)), None);
    let scope = CacheScope::new(&url, "proj");

    let offset = 12_345u64;
    let max_bytes = 4_096u64;
    let frames: Vec<_> = BackendService::read_file(
        &proxy,
        req_with_upstream(
            ReadFileRequest {
                project_id: "proj".to_string(),
                file_id: file_id.clone(),
                offset,
                max_bytes,
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .collect()
    .await;
    let body: Vec<u8> = frames.into_iter().flat_map(|f| f.unwrap().chunk).collect();
    assert_eq!(
        body,
        &content[offset as usize..(offset + max_bytes) as usize]
    );

    {
        let st = mock.state.lock().unwrap();
        assert_eq!(st.read_file_calls, 1);
        assert_eq!(st.read_file_requests, vec![(offset, max_bytes)]);
    }
    assert!(
        !cache.is_authorized(&scope, ObjectKind::File, &file_id),
        "ranged cold miss must not cache or authorize a full blob"
    );
    assert!(
        !cache
            .object_path(&scope, ObjectKind::File, &file_id)
            .exists()
    );
}

#[tokio::test]
async fn write_file_stream_streams_directly_to_upstream_and_spools_to_l2_without_hard_cap() {
    use tokio::sync::Notify;

    struct StreamingProbeUpstream {
        first_chunk_seen: Arc<Notify>,
        files: Arc<Mutex<std::collections::HashMap<Vec<u8>, Bytes>>>,
        read_file_calls: Arc<AtomicUsize>,
    }

    #[tonic::async_trait]
    impl BackendService for StreamingProbeUpstream {
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
            unused()
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
            request: Request<ReadFileRequest>,
        ) -> Result<Response<Self::ReadFileStream>, Status> {
            self.read_file_calls.fetch_add(1, Ordering::SeqCst);
            let req = request.into_inner();
            let content = self
                .files
                .lock()
                .unwrap()
                .get(&req.file_id)
                .cloned()
                .ok_or_else(|| Status::not_found("not found"))?;
            let frames = slice_file_range_frames(&content, req.offset, req.max_bytes)?;
            Ok(Response::new(Box::pin(futures::stream::iter(frames))))
        }
        async fn write_file(
            &self,
            _: Request<WriteFileRequest>,
        ) -> Result<Response<WriteFileResponse>, Status> {
            unused()
        }
        async fn write_file_stream(
            &self,
            request: Request<Streaming<WriteFileChunk>>,
        ) -> Result<Response<WriteFileResponse>, Status> {
            let mut stream = request.into_inner();
            let mut data = Vec::new();
            let mut first = true;
            while let Some(frame) = stream.next().await {
                data.extend_from_slice(&frame?.chunk);
                if first {
                    first = false;
                    self.first_chunk_seen.notify_one();
                }
            }
            let file_id = git_blob_id(&data);
            self.files
                .lock()
                .unwrap()
                .insert(file_id.clone(), Bytes::from(data));
            Ok(Response::new(WriteFileResponse { file_id }))
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

    let first_chunk_seen = Arc::new(Notify::new());
    let files = Arc::new(Mutex::new(std::collections::HashMap::new()));
    let read_file_calls = Arc::new(AtomicUsize::new(0));
    let probe = StreamingProbeUpstream {
        first_chunk_seen: Arc::clone(&first_chunk_seen),
        files: Arc::clone(&files),
        read_file_calls: Arc::clone(&read_file_calls),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_url = format!("http://{}", listener.local_addr().unwrap());
    let incoming =
        tonic::transport::server::TcpIncoming::from_listener(listener, true, None).unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(BackendServiceServer::new(probe))
            .serve_with_incoming(incoming),
    );

    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 8 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new(Arc::clone(&cache), Arc::new(UpstreamPool::new(None)), None);

    let proxy_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_url = format!("http://{}", proxy_listener.local_addr().unwrap());
    let proxy_incoming =
        tonic::transport::server::TcpIncoming::from_listener(proxy_listener, true, None).unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(BackendServiceServer::new(proxy.clone()))
            .serve_with_incoming(proxy_incoming),
    );

    let channel = tonic::transport::Endpoint::from_shared(proxy_url)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = BackendServiceClient::new(channel);

    // Stream > MAX_L1_OBJECT_BYTES (`4 MiB + 64 KiB`) and assert chunk 1 arrives at
    // upstream BEFORE chunk 2 is sent (proving direct streaming without full-file buffering).
    let large_payload: Vec<u8> = (0..(MAX_L1_OBJECT_BYTES + CACHE_STREAM_CHUNK_BYTES))
        .map(|i| (i % 251) as u8)
        .collect();
    let expected_id = git_blob_id(&large_payload);
    let (tx, rx) = tokio::sync::mpsc::channel::<WriteFileChunk>(4);
    let outbound =
        futures::stream::unfold(
            rx,
            |mut r| async move { r.recv().await.map(|item| (item, r)) },
        );
    let mut req = Request::new(outbound);
    req.metadata_mut()
        .insert(UPSTREAM_HEADER, upstream_url.parse().unwrap());

    let upload_task = tokio::spawn({
        let mut client = client.clone();
        async move { client.write_file_stream(req).await }
    });

    tx.send(WriteFileChunk {
        project_id: "proj".to_string(),
        chunk: large_payload[..CACHE_STREAM_CHUNK_BYTES].to_vec(),
        total_size: Some(large_payload.len() as u64),
        expected_file_id: Some(expected_id.clone()),
    })
    .await
    .unwrap();

    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        first_chunk_seen.notified(),
    )
    .await
    .expect("chunk 1 must reach upstream before client sends chunk 2");

    for chunk in large_payload[CACHE_STREAM_CHUNK_BYTES..].chunks(CACHE_STREAM_CHUNK_BYTES) {
        tx.send(WriteFileChunk {
            project_id: String::new(),
            chunk: chunk.to_vec(),
            total_size: None,
            expected_file_id: None,
        })
        .await
        .unwrap();
    }
    drop(tx);

    let resp = upload_task.await.unwrap().unwrap().into_inner();
    assert_eq!(resp.file_id, expected_id);
    assert_eq!(
        cache.stats().l1_entries,
        0,
        ">4 MiB streamed blob must stay in L2 disk CAS, not L1 RAM"
    );
    let scope = CacheScope::new(&upstream_url, "proj");
    assert!(
        cache
            .object_path(&scope, ObjectKind::File, &expected_id)
            .exists()
    );
    assert_eq!(
        fs::read_dir(cache.staging_dir()).unwrap().count(),
        0,
        "staging dir must have no leftover temp files after commit"
    );

    // Subsequent ranged ReadFile must hit L2 with zero upstream RPCs.
    let frames: Vec<_> = client
        .read_file(req_with_upstream(
            ReadFileRequest {
                project_id: "proj".to_string(),
                file_id: expected_id,
                offset: MAX_L1_OBJECT_BYTES as u64,
                max_bytes: 4096,
            },
            &upstream_url,
            None,
        ))
        .await
        .unwrap()
        .into_inner()
        .collect()
        .await;
    let read_back: Vec<u8> = frames.into_iter().flat_map(|f| f.unwrap().chunk).collect();
    assert_eq!(
        read_back,
        &large_payload[MAX_L1_OBJECT_BYTES..MAX_L1_OBJECT_BYTES + 4096]
    );
    assert_eq!(read_file_calls.load(Ordering::SeqCst), 0);

    // Streams exceeding `max_l2_object_bytes` stop L2 spooling, remove staging temp files,
    // and finish uploading directly to upstream with no `RESOURCE_EXHAUSTED` failure.
    let capped_dir = tempfile::tempdir().unwrap();
    let capped_cache = Arc::new(
        TwoTierCache::new_with_l2_object_cap(
            capped_dir.path().to_path_buf(),
            8 * 1024 * 1024,
            128 * 1024,
        )
        .unwrap(),
    );
    let capped_proxy = DaemonProxy::new(
        Arc::clone(&capped_cache),
        Arc::new(UpstreamPool::new(None)),
        None,
    );
    let capped_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let capped_url = format!("http://{}", capped_listener.local_addr().unwrap());
    let capped_incoming =
        tonic::transport::server::TcpIncoming::from_listener(capped_listener, true, None).unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(BackendServiceServer::new(capped_proxy))
            .serve_with_incoming(capped_incoming),
    );
    let mut capped_client = BackendServiceClient::new(
        tonic::transport::Endpoint::from_shared(capped_url)
            .unwrap()
            .connect()
            .await
            .unwrap(),
    );

    let oversized: Vec<u8> = (0..256 * 1024u32).map(|i| (i % 199) as u8).collect();
    let oversized_id = git_blob_id(&oversized);
    for include_total_size in [false, true] {
        let frames: Vec<WriteFileChunk> = oversized
            .chunks(CACHE_STREAM_CHUNK_BYTES)
            .enumerate()
            .map(|(idx, c)| WriteFileChunk {
                project_id: if idx == 0 {
                    "proj".to_string()
                } else {
                    String::new()
                },
                chunk: c.to_vec(),
                total_size: (idx == 0 && include_total_size).then_some(oversized.len() as u64),
                expected_file_id: None,
            })
            .collect();
        let mut req = Request::new(futures::stream::iter(frames));
        req.metadata_mut()
            .insert(UPSTREAM_HEADER, upstream_url.parse().unwrap());
        let resp = capped_client
            .write_file_stream(req)
            .await
            .expect("stream above L2 object cap must succeed without RESOURCE_EXHAUSTED")
            .into_inner();
        assert_eq!(resp.file_id, oversized_id);
    }
    assert_eq!(
        fs::read_dir(capped_cache.staging_dir()).unwrap().count(),
        0,
        "staging temp file must be cleaned up when spooling stops above L2 cap"
    );
    assert!(
        !capped_cache
            .object_path(&scope, ObjectKind::File, &oversized_id)
            .exists()
    );
}

#[tokio::test]
async fn cold_full_read_file_spills_to_l2_staging_and_streams_above_l2_cap_without_error() {
    let mock = MockUpstream::default();
    let l2_cap = (MAX_L1_OBJECT_BYTES + 256 * 1024) as u64;
    let spilled_payload = Bytes::from(
        (0..(MAX_L1_OBJECT_BYTES + 64 * 1024))
            .map(|i| (i % 251) as u8)
            .collect::<Vec<u8>>(),
    );
    let spilled_id = git_blob_id(&spilled_payload);
    let above_cap_payload = Bytes::from(
        (0..(MAX_L1_OBJECT_BYTES + 512 * 1024))
            .map(|i| (i % 241) as u8)
            .collect::<Vec<u8>>(),
    );
    let above_cap_id = git_blob_id(&above_cap_payload);
    {
        let mut st = mock.state.lock().unwrap();
        st.files.insert(spilled_id.clone(), spilled_payload.clone());
        st.files
            .insert(above_cap_id.clone(), above_cap_payload.clone());
    }

    let url = spawn_mock_upstream(mock.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(
        TwoTierCache::new_with_l2_object_cap(dir.path().to_path_buf(), 8 * 1024 * 1024, l2_cap)
            .unwrap(),
    );
    let proxy = DaemonProxy::new(Arc::clone(&cache), Arc::new(UpstreamPool::new(None)), None);
    let scope = CacheScope::new(&url, "proj");

    // Cold full ReadFile > 4 MiB spills to L2 staging and commits to L2 without L1 buffering.
    let frames: Vec<_> = BackendService::read_file(
        &proxy,
        req_with_upstream(
            ReadFileRequest {
                project_id: "proj".to_string(),
                file_id: spilled_id.clone(),
                offset: 0,
                max_bytes: 0,
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .collect()
    .await;
    let got: Vec<u8> = frames.into_iter().flat_map(|f| f.unwrap().chunk).collect();
    assert_eq!(got, spilled_payload);
    assert_eq!(mock.state.lock().unwrap().read_file_calls, 1);
    assert_eq!(cache.stats().l1_entries, 0);
    assert!(
        cache
            .object_path(&scope, ObjectKind::File, &spilled_id)
            .exists()
    );
    assert_eq!(fs::read_dir(cache.staging_dir()).unwrap().count(), 0);

    // Second ReadFile hits L2 without calling upstream.
    let hit_frames: Vec<_> = BackendService::read_file(
        &proxy,
        req_with_upstream(
            ReadFileRequest {
                project_id: "proj".to_string(),
                file_id: spilled_id,
                offset: MAX_L1_OBJECT_BYTES as u64,
                max_bytes: 4096,
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .collect()
    .await;
    let hit_body: Vec<u8> = hit_frames
        .into_iter()
        .flat_map(|f| f.unwrap().chunk)
        .collect();
    assert_eq!(
        hit_body,
        &spilled_payload[MAX_L1_OBJECT_BYTES..MAX_L1_OBJECT_BYTES + 4096]
    );
    assert_eq!(mock.state.lock().unwrap().read_file_calls, 1);

    // Cold full ReadFile exceeding `l2_cap` drops staging file and streams all bytes.
    let big_frames: Vec<_> = BackendService::read_file(
        &proxy,
        req_with_upstream(
            ReadFileRequest {
                project_id: "proj".to_string(),
                file_id: above_cap_id.clone(),
                offset: 0,
                max_bytes: 0,
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .collect()
    .await;
    let big_body: Vec<u8> = big_frames
        .into_iter()
        .flat_map(|f| f.unwrap().chunk)
        .collect();
    assert_eq!(big_body, above_cap_payload);
    assert!(
        !cache
            .object_path(&scope, ObjectKind::File, &above_cap_id)
            .exists()
    );
    assert_eq!(fs::read_dir(cache.staging_dir()).unwrap().count(), 0);
}

#[tokio::test]
async fn write_file_stream_forwards_metadata_and_rejects_duplicate_auth_before_reading_chunks() {
    let mock = MockUpstream::default();
    mock.state.lock().unwrap().required_auth = Some("Bearer stream-token".to_string());
    let upstream_url = spawn_mock_upstream(mock.clone()).await;

    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 8 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new(Arc::clone(&cache), Arc::new(UpstreamPool::new(None)), None);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_url = format!("http://{}", listener.local_addr().unwrap());
    let incoming =
        tonic::transport::server::TcpIncoming::from_listener(listener, true, None).unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(BackendServiceServer::new(proxy))
            .serve_with_incoming(incoming),
    );

    let channel = tonic::transport::Endpoint::from_shared(proxy_url)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = BackendServiceClient::new(channel);

    // Duplicate `authorization` headers fail immediately BEFORE reading any inbound chunks.
    let (_hold_tx, rx) = tokio::sync::mpsc::channel::<WriteFileChunk>(1);
    let mut dup_auth_req = Request::new(futures::stream::unfold(rx, |mut r| async move {
        r.recv().await.map(|item| (item, r))
    }));
    dup_auth_req
        .metadata_mut()
        .insert(UPSTREAM_HEADER, upstream_url.parse().unwrap());
    dup_auth_req
        .metadata_mut()
        .append("authorization", "Bearer stream-token".parse().unwrap());
    dup_auth_req
        .metadata_mut()
        .append("authorization", "Bearer other-token".parse().unwrap());
    let err = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        client.write_file_stream(dup_auth_req),
    )
    .await
    .expect("duplicate authorization must fail immediately without waiting for chunks")
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    assert_eq!(mock.state.lock().unwrap().write_file_stream_calls, 0);

    // Valid stream forwards `authorization` and `grpc-timeout` while stripping
    // `x-jj-cc-upstream`, uploads all chunks, and writes through under the caller's auth scope.
    let payload = b"hello streamed metadata world";
    let expected_id = git_blob_id(payload);
    let chunks = vec![
        WriteFileChunk {
            project_id: "proj-meta".to_string(),
            chunk: payload[..12].to_vec(),
            total_size: Some(payload.len() as u64),
            expected_file_id: Some(expected_id.clone()),
        },
        WriteFileChunk {
            project_id: String::new(),
            chunk: payload[12..].to_vec(),
            total_size: None,
            expected_file_id: None,
        },
    ];
    let mut valid_req = Request::new(futures::stream::iter(chunks));
    valid_req
        .metadata_mut()
        .insert(UPSTREAM_HEADER, upstream_url.parse().unwrap());
    valid_req
        .metadata_mut()
        .insert("authorization", "Bearer stream-token".parse().unwrap());
    valid_req
        .metadata_mut()
        .insert("grpc-timeout", "5S".parse().unwrap());
    let resp = client
        .write_file_stream(valid_req)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.file_id, expected_id);
    {
        let st = mock.state.lock().unwrap();
        assert_eq!(st.write_file_stream_calls, 1);
        assert_eq!(
            st.last_write_file_stream_auth.as_deref(),
            Some("Bearer stream-token")
        );
        assert_eq!(st.last_write_file_stream_timeout.as_deref(), Some("5S"));
        assert!(
            !st.last_write_file_stream_had_upstream_header,
            "x-jj-cc-upstream must not leak to upstream"
        );
        assert_eq!(
            st.files.get(&expected_id).map(|b| b.as_ref()),
            Some(payload.as_slice())
        );
    }

    // Wrong authorization fails at upstream without populating the local cache.
    let mut wrong_auth_req = Request::new(futures::stream::iter(vec![WriteFileChunk {
        project_id: "proj-meta".to_string(),
        chunk: b"denied".to_vec(),
        total_size: Some(6),
        expected_file_id: None,
    }]));
    wrong_auth_req
        .metadata_mut()
        .insert(UPSTREAM_HEADER, upstream_url.parse().unwrap());
    wrong_auth_req
        .metadata_mut()
        .insert("authorization", "Bearer wrong-token".parse().unwrap());
    let denied = client.write_file_stream(wrong_auth_req).await.unwrap_err();
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);
    assert_eq!(fs::read_dir(cache.staging_dir()).unwrap().count(), 0);
}

#[tokio::test]
async fn write_file_stream_cancels_upstream_with_rst_stream_and_reuses_channel() {
    use std::convert::Infallible;
    use std::sync::atomic::AtomicBool;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use cc_common::workspace::GetWorkspaceRequest;
    use cc_common::workspace::workspace_service_client::WorkspaceServiceClient;
    use cc_common::workspace::workspace_service_server::WorkspaceServiceServer;
    use tokio::sync::Notify;
    use tonic::body::BoxBody;
    use tonic::codegen::{Body as _, Service, http};
    use tonic::server::NamedService;

    struct AbortGuard {
        completed_cleanly: bool,
        saw_reset: Arc<AtomicBool>,
        stream_finished: Arc<Notify>,
    }

    impl Drop for AbortGuard {
        fn drop(&mut self) {
            if !self.completed_cleanly {
                self.saw_reset.store(true, Ordering::SeqCst);
                self.stream_finished.notify_one();
            }
        }
    }

    // Raw HTTP/2 body inspector so `tonic::Streaming`'s `Code::Cancelled -> Ok(None)`
    // translation cannot mask whether the proxy sent `RST_STREAM` or clean `END_STREAM`.
    #[derive(Clone)]
    struct RawUpstreamInspector {
        first_frame_seen: Arc<Notify>,
        stream_finished: Arc<Notify>,
        saw_clean_eof: Arc<AtomicBool>,
        saw_reset: Arc<AtomicBool>,
    }

    impl NamedService for RawUpstreamInspector {
        const NAME: &'static str = "commit_cloud.backend.BackendService";
    }

    impl Service<http::Request<BoxBody>> for RawUpstreamInspector {
        type Response = http::Response<BoxBody>;
        type Error = Infallible;
        type Future =
            Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: http::Request<BoxBody>) -> Self::Future {
            let first_frame_seen = Arc::clone(&self.first_frame_seen);
            let stream_finished = Arc::clone(&self.stream_finished);
            let saw_clean_eof = Arc::clone(&self.saw_clean_eof);
            let saw_reset = Arc::clone(&self.saw_reset);

            Box::pin(async move {
                let mut guard = AbortGuard {
                    completed_cleanly: false,
                    saw_reset: Arc::clone(&saw_reset),
                    stream_finished: Arc::clone(&stream_finished),
                };
                let mut body = req.into_body();
                let mut notified = false;
                loop {
                    let next = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await;
                    match next {
                        Some(Ok(frame)) => {
                            if frame.is_data() && !notified {
                                notified = true;
                                first_frame_seen.notify_one();
                            }
                        }
                        Some(Err(_)) => {
                            saw_reset.store(true, Ordering::SeqCst);
                            guard.completed_cleanly = true;
                            stream_finished.notify_one();
                            break;
                        }
                        None => {
                            saw_clean_eof.store(true, Ordering::SeqCst);
                            guard.completed_cleanly = true;
                            stream_finished.notify_one();
                            break;
                        }
                    }
                }
                Ok(http::Response::new(tonic::body::empty_body()))
            })
        }
    }

    let inspector = RawUpstreamInspector {
        first_frame_seen: Arc::new(Notify::new()),
        stream_finished: Arc::new(Notify::new()),
        saw_clean_eof: Arc::new(AtomicBool::new(false)),
        saw_reset: Arc::new(AtomicBool::new(false)),
    };
    let mock_ws = MockUpstream::default();
    let accepted_conns = Arc::new(AtomicUsize::new(0));
    let accepted_for_incoming = Arc::clone(&accepted_conns);

    let upstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_url = format!("http://{}", upstream_listener.local_addr().unwrap());
    let upstream_incoming =
        tonic::transport::server::TcpIncoming::from_listener(upstream_listener, true, None)
            .unwrap()
            .inspect(move |res| {
                if res.is_ok() {
                    accepted_for_incoming.fetch_add(1, Ordering::SeqCst);
                }
            });
    let upstream_task = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(inspector.clone())
            .add_service(WorkspaceServiceServer::new(mock_ws.clone()))
            .serve_with_incoming(upstream_incoming),
    );

    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 8 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new(Arc::clone(&cache), Arc::new(UpstreamPool::new(None)), None);

    let proxy_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_url = format!("http://{}", proxy_listener.local_addr().unwrap());
    let proxy_incoming =
        tonic::transport::server::TcpIncoming::from_listener(proxy_listener, true, None).unwrap();
    let proxy_task = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(BackendServiceServer::new(proxy.clone()).max_decoding_message_size(1024))
            .add_service(WorkspaceServiceServer::new(proxy))
            .serve_with_incoming(proxy_incoming),
    );

    let channel = tonic::transport::Endpoint::from_shared(proxy_url)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let client = BackendServiceClient::new(channel.clone());
    let mut ws_client = WorkspaceServiceClient::new(channel);

    let open_stream = |mut c: BackendServiceClient<Channel>, url: String| {
        let (tx, rx) = tokio::sync::mpsc::channel::<WriteFileChunk>(4);
        let outbound =
            futures::stream::unfold(rx, |mut r| async move { r.recv().await.map(|v| (v, r)) });
        let mut req = Request::new(outbound);
        req.metadata_mut()
            .insert(UPSTREAM_HEADER, url.parse().unwrap());
        let handle = tokio::spawn(async move { c.write_file_stream(req).await });
        (tx, handle)
    };

    // Mid-stream local client stream error (`Code::OutOfRange` from exceeding proxy 1 KiB
    // decode limit on chunk 2) cancels upstream with `RST_STREAM`, never clean EOF.
    {
        inspector.saw_clean_eof.store(false, Ordering::SeqCst);
        inspector.saw_reset.store(false, Ordering::SeqCst);
        let (chunk_tx, call_task) = open_stream(client.clone(), upstream_url.clone());
        chunk_tx
            .send(WriteFileChunk {
                project_id: "proj-1".to_string(),
                chunk: b"partial".to_vec(),
                total_size: Some(4096),
                expected_file_id: None,
            })
            .await
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            inspector.first_frame_seen.notified(),
        )
        .await
        .expect("upstream must receive chunk 1 before chunk 2 errors");
        chunk_tx
            .send(WriteFileChunk {
                project_id: String::new(),
                chunk: vec![0x55; 2048],
                total_size: None,
                expected_file_id: None,
            })
            .await
            .unwrap();
        let err = tokio::time::timeout(Duration::from_secs(5), call_task)
            .await
            .expect("local stream error must return promptly")
            .unwrap()
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::OutOfRange);
        tokio::time::timeout(Duration::from_secs(5), inspector.stream_finished.notified())
            .await
            .expect("upstream stream must observe cancellation");
        drop(chunk_tx);
        assert!(!inspector.saw_clean_eof.load(Ordering::SeqCst));
        assert!(inspector.saw_reset.load(Ordering::SeqCst));
    }

    // Mid-stream daemon validation error (mismatched `project_id` on chunk 2) cancels
    // upstream with `RST_STREAM`, never clean EOF.
    {
        inspector.saw_clean_eof.store(false, Ordering::SeqCst);
        inspector.saw_reset.store(false, Ordering::SeqCst);
        let (chunk_tx, call_task) = open_stream(client.clone(), upstream_url.clone());
        chunk_tx
            .send(WriteFileChunk {
                project_id: "proj-1".to_string(),
                chunk: b"chunk-1".to_vec(),
                total_size: None,
                expected_file_id: None,
            })
            .await
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            inspector.first_frame_seen.notified(),
        )
        .await
        .expect("upstream must receive chunk 1");
        chunk_tx
            .send(WriteFileChunk {
                project_id: "other-proj".to_string(),
                chunk: b"chunk-2".to_vec(),
                total_size: None,
                expected_file_id: None,
            })
            .await
            .unwrap();
        let err = tokio::time::timeout(Duration::from_secs(5), call_task)
            .await
            .expect("validation error must return promptly")
            .unwrap()
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        tokio::time::timeout(Duration::from_secs(5), inspector.stream_finished.notified())
            .await
            .expect("upstream stream must observe cancellation");
        drop(chunk_tx);
        assert!(!inspector.saw_clean_eof.load(Ordering::SeqCst));
        assert!(inspector.saw_reset.load(Ordering::SeqCst));
    }

    // EOF truncation validation error (`bytes_received < total_size`) cancels upstream with
    // `RST_STREAM`, never clean EOF.
    {
        inspector.saw_clean_eof.store(false, Ordering::SeqCst);
        inspector.saw_reset.store(false, Ordering::SeqCst);
        let (chunk_tx, call_task) = open_stream(client.clone(), upstream_url.clone());
        chunk_tx
            .send(WriteFileChunk {
                project_id: "proj-1".to_string(),
                chunk: b"short".to_vec(),
                total_size: Some(100),
                expected_file_id: None,
            })
            .await
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            inspector.first_frame_seen.notified(),
        )
        .await
        .expect("upstream must receive chunk 1");
        drop(chunk_tx);
        let err = tokio::time::timeout(Duration::from_secs(5), call_task)
            .await
            .expect("truncation error must return promptly")
            .unwrap()
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        tokio::time::timeout(Duration::from_secs(5), inspector.stream_finished.notified())
            .await
            .expect("upstream stream must observe cancellation");
        assert!(!inspector.saw_clean_eof.load(Ordering::SeqCst));
        assert!(inspector.saw_reset.load(Ordering::SeqCst));
    }

    // Client aborting mid-upload cancels upstream with `RST_STREAM` and cleans up staging.
    {
        inspector.saw_clean_eof.store(false, Ordering::SeqCst);
        inspector.saw_reset.store(false, Ordering::SeqCst);
        let (chunk_tx, call_task) = open_stream(client.clone(), upstream_url.clone());
        chunk_tx
            .send(WriteFileChunk {
                project_id: "proj-1".to_string(),
                chunk: b"dropped-mid-upload".to_vec(),
                total_size: Some(4096),
                expected_file_id: None,
            })
            .await
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            inspector.first_frame_seen.notified(),
        )
        .await
        .expect("upstream must receive chunk 1");
        call_task.abort();
        drop(chunk_tx);
        tokio::time::timeout(Duration::from_secs(5), inspector.stream_finished.notified())
            .await
            .expect("upstream stream must observe cancellation when client aborts");
        assert!(!inspector.saw_clean_eof.load(Ordering::SeqCst));
        assert!(inspector.saw_reset.load(Ordering::SeqCst));
    }

    // Follow-up RPC through the same proxy succeeds over the exact same pooled HTTP/2
    // connection (`accepted_conns == 1`), with zero partial objects or leftover staging files.
    ws_client
        .get_workspace(req_with_upstream(
            GetWorkspaceRequest {
                repo_id: "repo".to_string(),
                workspace_name: "default".to_string(),
            },
            &upstream_url,
            None,
        ))
        .await
        .expect("follow-up RPC must reuse the healthy upstream channel");
    assert_eq!(mock_ws.state.lock().unwrap().get_workspace_calls, 1);
    assert_eq!(
        accepted_conns.load(Ordering::SeqCst),
        1,
        "local errors and cancellations must reuse the pooled upstream HTTP/2 channel"
    );
    assert_eq!(cache.stats().write_through_objects, 0);
    assert_eq!(cache.stats().l1_entries, 0);
    assert_eq!(fs::read_dir(cache.staging_dir()).unwrap().count(), 0);

    proxy_task.abort();
    upstream_task.abort();
}
