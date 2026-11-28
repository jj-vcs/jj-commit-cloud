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

//! Local daemon gRPC proxy for `BackendService`.
//!
//! # Cross-Process and RPC Boundaries
//! `write_file_stream` forwards chunks through `OutboundChunkStream`, which only signals
//! upstream HTTP/2 `END_STREAM` after an explicit EOF sentinel from a clean local stream close.
//! Local stream errors and client cancellations drop the upstream future (`RST_STREAM`) without
//! signaling clean EOF and do not reset the shared upstream channel.

use crate::connection::RemoteConnectionManager;
use cc_common::backend::backend_service_server::BackendService;
use cc_common::backend::*;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt};
use tonic::{Extensions, Request, Response, Status, Streaming};

/// Outbound request stream for `WriteFileStream` that only emits clean stream EOF
/// (`Poll::Ready(None)`) when the proxy explicitly sends an `Option::None` sentinel.
///
/// If the sender is dropped without sending `None` (due to a local client error or proxy task
/// cancellation), `poll_next` stays `Poll::Pending` instead of signaling clean EOF while hyper's
/// HTTP/2 driver processes the cancelled response future (`RST_STREAM`) and drops this stream.
struct OutboundChunkStream {
    rx: mpsc::Receiver<Option<WriteFileChunk>>,
}

impl Stream for OutboundChunkStream {
    type Item = WriteFileChunk;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(Some(chunk))) => Poll::Ready(Some(chunk)),
            Poll::Ready(Some(None)) => Poll::Ready(None),
            Poll::Ready(None) | Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Clone)]
pub struct DaemonBackendProxy {
    conn_manager: RemoteConnectionManager,
}

impl DaemonBackendProxy {
    pub fn new(conn_manager: RemoteConnectionManager) -> Self {
        Self { conn_manager }
    }
}

#[tonic::async_trait]
impl BackendService for DaemonBackendProxy {
    async fn register_repository(
        &self,
        request: Request<RegisterRepositoryRequest>,
    ) -> Result<Response<RegisterRepositoryResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.register_repository(request).await })
            .await
    }

    async fn read_commit(
        &self,
        request: Request<ReadCommitRequest>,
    ) -> Result<Response<ReadCommitResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.read_commit(request).await })
            .await
    }

    async fn write_commit(
        &self,
        request: Request<WriteCommitRequest>,
    ) -> Result<Response<WriteCommitResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.write_commit(request).await })
            .await
    }

    async fn read_tree(
        &self,
        request: Request<ReadTreeRequest>,
    ) -> Result<Response<ReadTreeResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.read_tree(request).await })
            .await
    }

    async fn write_tree(
        &self,
        request: Request<WriteTreeRequest>,
    ) -> Result<Response<WriteTreeResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.write_tree(request).await })
            .await
    }

    type ReadFileStream = Streaming<ReadFileResponse>;

    async fn read_file(
        &self,
        request: Request<ReadFileRequest>,
    ) -> Result<Response<Self::ReadFileStream>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.read_file(request).await })
            .await
    }

    async fn write_file(
        &self,
        request: Request<WriteFileRequest>,
    ) -> Result<Response<WriteFileResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.write_file(request).await })
            .await
    }

    async fn write_file_stream(
        &self,
        request: Request<Streaming<WriteFileChunk>>,
    ) -> Result<Response<WriteFileResponse>, Status> {
        let (metadata, _extensions, mut inbound) = request.into_parts();
        // Return local client stream errors inside `Ok(Err(_))` so `with_backend`
        // only resets the shared upstream channel on upstream transport failures.
        let res: Result<Response<WriteFileResponse>, Status> = self
            .conn_manager
            .with_backend(move |mut c| async move {
                let (tx, rx) = mpsc::channel::<Option<WriteFileChunk>>(16);
                // `OutboundChunkStream` only yields clean EOF (`END_STREAM`) when we explicitly
                // enqueue `None`. Keeping `tx` outside this inner block also ensures
                // `upstream_fut` drops first (triggering HTTP/2 `RST_STREAM`) on local errors or
                // task cancellation before `tx` drops.
                {
                    let outbound = Request::from_parts(
                        metadata,
                        Extensions::new(),
                        OutboundChunkStream { rx },
                    );
                    let upstream_fut = c.write_file_stream(outbound);
                    tokio::pin!(upstream_fut);

                    let mut pending: Option<WriteFileChunk> = None;
                    loop {
                        if let Some(chunk) = pending.take() {
                            tokio::select! {
                                res = &mut upstream_fut => break res.map(Ok),
                                permit = tx.reserve() => match permit {
                                    Ok(permit) => permit.send(Some(chunk)),
                                    Err(_) => break upstream_fut.await.map(Ok),
                                }
                            }
                        } else {
                            tokio::select! {
                                res = &mut upstream_fut => break res.map(Ok),
                                maybe_chunk = inbound.next() => match maybe_chunk {
                                    Some(Ok(chunk)) => pending = Some(chunk),
                                    Some(Err(local_status)) => break Ok(Err(local_status)),
                                    None => {
                                        // Local client stream finished cleanly: send the explicit
                                        // EOF sentinel while still polling `upstream_fut` so an
                                        // early upstream error cannot deadlock on a full channel.
                                        tokio::select! {
                                            res = &mut upstream_fut => break res.map(Ok),
                                            permit = tx.reserve() => {
                                                if let Ok(permit) = permit {
                                                    permit.send(None);
                                                }
                                            }
                                        }
                                        break upstream_fut.await.map(Ok);
                                    }
                                }
                            }
                        }
                    }
                }
            })
            .await?;
        res
    }

    async fn read_symlink(
        &self,
        request: Request<ReadSymlinkRequest>,
    ) -> Result<Response<ReadSymlinkResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.read_symlink(request).await })
            .await
    }

    async fn write_symlink(
        &self,
        request: Request<WriteSymlinkRequest>,
    ) -> Result<Response<WriteSymlinkResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.write_symlink(request).await })
            .await
    }

    async fn register_project(
        &self,
        request: Request<RegisterProjectRequest>,
    ) -> Result<Response<RegisterProjectResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.register_project(request).await })
            .await
    }

    async fn list_commits(
        &self,
        request: Request<ListCommitsRequest>,
    ) -> Result<Response<ListCommitsResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.list_commits(request).await })
            .await
    }

    async fn resolve_commit_prefix(
        &self,
        request: Request<ResolveCommitPrefixRequest>,
    ) -> Result<Response<ResolveCommitPrefixResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.resolve_commit_prefix(request).await })
            .await
    }

    async fn resolve_change_prefix(
        &self,
        request: Request<ResolveChangePrefixRequest>,
    ) -> Result<Response<ResolveChangePrefixResponse>, Status> {
        self.conn_manager
            .with_backend(|mut c| async move { c.resolve_change_prefix(request).await })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_common::backend::backend_service_client::BackendServiceClient;
    use cc_common::backend::backend_service_server::BackendServiceServer;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::sync::Notify;
    use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
    use tonic::Code;
    use tonic::transport::Server;

    #[derive(Clone, Copy)]
    enum MockStreamMode {
        Accumulate,
        FailInvalidArgumentAfterFirst,
        FailUnavailableAfterFirst,
    }

    #[derive(Clone)]
    struct MockUpstreamBackend {
        mode: MockStreamMode,
        first_chunk_seen: Arc<Notify>,
        stream_finished: Arc<Notify>,
        saw_clean_eof: Arc<AtomicBool>,
        saw_abort: Arc<AtomicBool>,
        received_bytes: Arc<Mutex<Vec<u8>>>,
        received_auth: Arc<Mutex<Option<String>>>,
    }

    impl MockUpstreamBackend {
        fn new(mode: MockStreamMode) -> Self {
            Self {
                mode,
                first_chunk_seen: Arc::new(Notify::new()),
                stream_finished: Arc::new(Notify::new()),
                saw_clean_eof: Arc::new(AtomicBool::new(false)),
                saw_abort: Arc::new(AtomicBool::new(false)),
                received_bytes: Arc::new(Mutex::new(Vec::new())),
                received_auth: Arc::new(Mutex::new(None)),
            }
        }
    }

    struct AbortGuard {
        completed_cleanly: bool,
        saw_abort: Arc<AtomicBool>,
        stream_finished: Arc<Notify>,
    }

    impl Drop for AbortGuard {
        fn drop(&mut self) {
            if !self.completed_cleanly {
                self.saw_abort.store(true, Ordering::SeqCst);
            }
            self.stream_finished.notify_waiters();
        }
    }

    #[tonic::async_trait]
    impl BackendService for MockUpstreamBackend {
        async fn register_repository(
            &self,
            _request: Request<RegisterRepositoryRequest>,
        ) -> Result<Response<RegisterRepositoryResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }
        async fn read_commit(
            &self,
            _request: Request<ReadCommitRequest>,
        ) -> Result<Response<ReadCommitResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }
        async fn write_commit(
            &self,
            _request: Request<WriteCommitRequest>,
        ) -> Result<Response<WriteCommitResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }
        async fn read_tree(
            &self,
            _request: Request<ReadTreeRequest>,
        ) -> Result<Response<ReadTreeResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }
        async fn write_tree(
            &self,
            _request: Request<WriteTreeRequest>,
        ) -> Result<Response<WriteTreeResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }

        type ReadFileStream = ReceiverStream<Result<ReadFileResponse, Status>>;

        async fn read_file(
            &self,
            _request: Request<ReadFileRequest>,
        ) -> Result<Response<Self::ReadFileStream>, Status> {
            Err(Status::unimplemented("mock"))
        }
        async fn write_file(
            &self,
            _request: Request<WriteFileRequest>,
        ) -> Result<Response<WriteFileResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }

        async fn write_file_stream(
            &self,
            request: Request<Streaming<WriteFileChunk>>,
        ) -> Result<Response<WriteFileResponse>, Status> {
            let auth = request
                .metadata()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            *self.received_auth.lock().unwrap() = auth;

            let mut guard = AbortGuard {
                completed_cleanly: false,
                saw_abort: self.saw_abort.clone(),
                stream_finished: self.stream_finished.clone(),
            };

            let mut stream = request.into_inner();
            let mut first = true;
            while let Some(item) = stream.next().await {
                match item {
                    Ok(chunk) => {
                        self.received_bytes
                            .lock()
                            .unwrap()
                            .extend_from_slice(&chunk.chunk);
                        if first {
                            first = false;
                            self.first_chunk_seen.notify_waiters();
                            match self.mode {
                                MockStreamMode::Accumulate => {}
                                MockStreamMode::FailInvalidArgumentAfterFirst => {
                                    guard.completed_cleanly = true;
                                    return Err(Status::invalid_argument("bad chunk"));
                                }
                                MockStreamMode::FailUnavailableAfterFirst => {
                                    guard.completed_cleanly = true;
                                    return Err(Status::unavailable("upstream unavailable"));
                                }
                            }
                        }
                    }
                    Err(err) => {
                        self.saw_abort.store(true, Ordering::SeqCst);
                        return Err(err);
                    }
                }
            }

            guard.completed_cleanly = true;
            self.saw_clean_eof.store(true, Ordering::SeqCst);
            Ok(Response::new(WriteFileResponse {
                file_id: vec![0xab; cc_common::FILE_ID_LENGTH],
            }))
        }

        async fn read_symlink(
            &self,
            _request: Request<ReadSymlinkRequest>,
        ) -> Result<Response<ReadSymlinkResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }
        async fn write_symlink(
            &self,
            _request: Request<WriteSymlinkRequest>,
        ) -> Result<Response<WriteSymlinkResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }
        async fn register_project(
            &self,
            _request: Request<RegisterProjectRequest>,
        ) -> Result<Response<RegisterProjectResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }
        async fn list_commits(
            &self,
            _request: Request<ListCommitsRequest>,
        ) -> Result<Response<ListCommitsResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }
        async fn resolve_commit_prefix(
            &self,
            _request: Request<ResolveCommitPrefixRequest>,
        ) -> Result<Response<ResolveCommitPrefixResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }
        async fn resolve_change_prefix(
            &self,
            _request: Request<ResolveChangePrefixRequest>,
        ) -> Result<Response<ResolveChangePrefixResponse>, Status> {
            Err(Status::unimplemented("mock"))
        }
    }

    async fn spawn_proxy_pair(
        mode: MockStreamMode,
    ) -> (
        BackendServiceClient<tonic::transport::Channel>,
        RemoteConnectionManager,
        MockUpstreamBackend,
        Vec<tokio::task::JoinHandle<()>>,
    ) {
        let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let mock = MockUpstreamBackend::new(mode);
        let mock_svc = mock.clone();
        let upstream_task = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(BackendServiceServer::new(mock_svc))
                .serve_with_incoming(TcpListenerStream::new(upstream_listener))
                .await;
        });

        let conn_manager = RemoteConnectionManager::new(format!("http://{upstream_addr}"));
        let proxy = DaemonBackendProxy::new(conn_manager.clone());
        let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy_listener.local_addr().unwrap();
        let proxy_task = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(BackendServiceServer::new(proxy))
                .serve_with_incoming(TcpListenerStream::new(proxy_listener))
                .await;
        });

        let client = BackendServiceClient::connect(format!("http://{proxy_addr}"))
            .await
            .unwrap();
        (client, conn_manager, mock, vec![proxy_task, upstream_task])
    }

    #[tokio::test]
    async fn write_file_stream_forwards_chunks_and_metadata() {
        let (mut client, conn_manager, mock, tasks) =
            spawn_proxy_pair(MockStreamMode::Accumulate).await;

        let chunks = vec![
            WriteFileChunk {
                project_id: "proj-1".to_string(),
                chunk: b"hello ".to_vec(),
                total_size: Some(11),
                expected_file_id: None,
            },
            WriteFileChunk {
                project_id: String::new(),
                chunk: b"world".to_vec(),
                total_size: None,
                expected_file_id: None,
            },
        ];
        let mut req = Request::new(tokio_stream::iter(chunks));
        req.metadata_mut()
            .insert("authorization", "Bearer test-token".parse().unwrap());

        let resp = client.write_file_stream(req).await.unwrap().into_inner();
        assert_eq!(resp.file_id, vec![0xab; cc_common::FILE_ID_LENGTH]);
        assert!(mock.saw_clean_eof.load(Ordering::SeqCst));
        assert!(!mock.saw_abort.load(Ordering::SeqCst));
        assert_eq!(&*mock.received_bytes.lock().unwrap(), b"hello world");
        assert_eq!(
            mock.received_auth.lock().unwrap().as_deref(),
            Some("Bearer test-token")
        );
        assert!(conn_manager.has_cached_channel().await);

        for t in tasks {
            t.abort();
        }
    }

    #[tokio::test]
    async fn write_file_stream_cancels_upstream_on_local_error_without_resetting_channel() {
        use std::convert::Infallible;
        use tonic::body::BoxBody;
        use tonic::codegen::{Body as _, Service, http};
        use tonic::server::NamedService;

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
            type Future = Pin<
                Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
            >;

            fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }

            fn call(&mut self, req: http::Request<BoxBody>) -> Self::Future {
                let first_frame_seen = self.first_frame_seen.clone();
                let stream_finished = self.stream_finished.clone();
                let saw_clean_eof = self.saw_clean_eof.clone();
                let saw_reset = self.saw_reset.clone();

                Box::pin(async move {
                    let mut guard = AbortGuard {
                        completed_cleanly: false,
                        saw_abort: saw_reset.clone(),
                        stream_finished: stream_finished.clone(),
                    };
                    let mut body = req.into_body();
                    let mut notified = false;
                    loop {
                        let next =
                            std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await;
                        match next {
                            Some(Ok(frame)) => {
                                if frame.is_data() && !notified {
                                    notified = true;
                                    first_frame_seen.notify_waiters();
                                }
                            }
                            Some(Err(_)) => {
                                saw_reset.store(true, Ordering::SeqCst);
                                guard.completed_cleanly = true;
                                stream_finished.notify_waiters();
                                break;
                            }
                            None => {
                                saw_clean_eof.store(true, Ordering::SeqCst);
                                guard.completed_cleanly = true;
                                stream_finished.notify_waiters();
                                break;
                            }
                        }
                    }
                    Ok(http::Response::new(tonic::body::empty_body()))
                })
            }
        }

        let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let inspector = RawUpstreamInspector {
            first_frame_seen: Arc::new(Notify::new()),
            stream_finished: Arc::new(Notify::new()),
            saw_clean_eof: Arc::new(AtomicBool::new(false)),
            saw_reset: Arc::new(AtomicBool::new(false)),
        };
        let inspector_svc = inspector.clone();
        let upstream_task = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(inspector_svc)
                .serve_with_incoming(TcpListenerStream::new(upstream_listener))
                .await;
        });

        let conn_manager = RemoteConnectionManager::new(format!("http://{upstream_addr}"));
        let proxy = DaemonBackendProxy::new(conn_manager.clone());
        let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy_listener.local_addr().unwrap();
        let proxy_task = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(BackendServiceServer::new(proxy).max_decoding_message_size(1024))
                .serve_with_incoming(TcpListenerStream::new(proxy_listener))
                .await;
        });

        let mut client = BackendServiceClient::connect(format!("http://{proxy_addr}"))
            .await
            .unwrap();

        let (chunk_tx, chunk_rx) = tokio::sync::mpsc::channel(4);
        let first_seen = inspector.first_frame_seen.notified();
        let stream_done = inspector.stream_finished.notified();

        let call_task = tokio::spawn(async move {
            client
                .write_file_stream(Request::new(ReceiverStream::new(chunk_rx)))
                .await
        });

        // First chunk fits within the proxy's decode limit and reaches upstream.
        chunk_tx
            .send(WriteFileChunk {
                project_id: "proj-1".to_string(),
                chunk: b"partial".to_vec(),
                total_size: Some(4096),
                expected_file_id: None,
            })
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(5), first_seen)
            .await
            .expect("upstream should receive the first chunk");

        // Second chunk exceeds the proxy's 1 KiB decode limit, triggering `Some(Err(OutOfRange))`
        // on `inbound.next()` mid-stream.
        chunk_tx
            .send(WriteFileChunk {
                project_id: String::new(),
                chunk: vec![0x55; 2048],
                total_size: None,
                expected_file_id: None,
            })
            .await
            .unwrap();

        let local_err = tokio::time::timeout(Duration::from_secs(5), call_task)
            .await
            .expect("local call should return error")
            .unwrap()
            .unwrap_err();
        assert_eq!(local_err.code(), Code::OutOfRange);

        tokio::time::timeout(Duration::from_secs(5), stream_done)
            .await
            .expect("upstream stream should observe cancellation");
        drop(chunk_tx);

        assert!(
            !inspector.saw_clean_eof.load(Ordering::SeqCst),
            "upstream must not observe clean EOF when local client stream errors mid-upload"
        );
        assert!(
            inspector.saw_reset.load(Ordering::SeqCst),
            "upstream stream must observe HTTP/2 reset/cancellation"
        );
        assert!(
            conn_manager.has_cached_channel().await,
            "local client stream error must not reset the shared upstream channel"
        );

        proxy_task.abort();
        upstream_task.abort();
    }

    #[tokio::test]
    async fn write_file_stream_propagates_upstream_errors_and_resets_only_on_transport_errors() {
        // Non-connection error (`INVALID_ARGUMENT`): propagated without resetting channel.
        {
            let (mut client, conn_manager, _mock, tasks) =
                spawn_proxy_pair(MockStreamMode::FailInvalidArgumentAfterFirst).await;
            let (chunk_tx, chunk_rx) = tokio::sync::mpsc::channel(4);
            chunk_tx
                .send(WriteFileChunk {
                    project_id: "proj-1".to_string(),
                    chunk: b"chunk".to_vec(),
                    total_size: None,
                    expected_file_id: None,
                })
                .await
                .unwrap();

            let err = client
                .write_file_stream(Request::new(ReceiverStream::new(chunk_rx)))
                .await
                .unwrap_err();
            drop(chunk_tx);
            assert_eq!(err.code(), Code::InvalidArgument);
            assert!(conn_manager.has_cached_channel().await);

            for t in tasks {
                t.abort();
            }
        }

        // Connection/transport error (`UNAVAILABLE`): propagated and resets cached channel.
        {
            let (mut client, conn_manager, _mock, tasks) =
                spawn_proxy_pair(MockStreamMode::FailUnavailableAfterFirst).await;
            let (chunk_tx, chunk_rx) = tokio::sync::mpsc::channel(4);
            chunk_tx
                .send(WriteFileChunk {
                    project_id: "proj-1".to_string(),
                    chunk: b"chunk".to_vec(),
                    total_size: None,
                    expected_file_id: None,
                })
                .await
                .unwrap();

            let err = client
                .write_file_stream(Request::new(ReceiverStream::new(chunk_rx)))
                .await
                .unwrap_err();
            drop(chunk_tx);
            assert_eq!(err.code(), Code::Unavailable);
            assert!(
                !conn_manager.has_cached_channel().await,
                "upstream UNAVAILABLE error must reset the shared cached channel"
            );

            for t in tasks {
                t.abort();
            }
        }
    }
}
