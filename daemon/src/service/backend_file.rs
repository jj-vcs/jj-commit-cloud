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

//! Streaming `ReadFile`, `WriteFile`, and `WriteFileStream` proxy handlers and stream wrappers.
//!
//! # Authority and Crash-Safety Ordering
//! `WriteFileStream` streams directly to upstream while spooling bytes through `BlobSpooler`
//! and commits to L2 CAS only after upstream confirms the blob digest. Full cold `ReadFile`
//! misses spill above `MAX_L1_OBJECT_BYTES` to L2 staging without imposing a hard size cap.
//!
//! # Cross-Process and RPC Boundaries
//! Ranged cold `ReadFile` misses forward the exact requested byte range to upstream instead
//! of fetching the full object, and `TrackedReadFileStream` keeps the active-RPC guard alive
//! until the response stream completes or drops.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use cc_common::backend::{
    ReadFileRequest, ReadFileResponse, WriteFileChunk, WriteFileRequest, WriteFileResponse,
};
use futures::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

use super::{ActiveRpcGuard, DaemonProxy, STREAM_CHANNEL_CAPACITY, backend_client};
use crate::cache::{CacheRangeError, CacheScope, MAX_L1_OBJECT_BYTES, ObjectKind};
use crate::upstream::extract_auth_header;

pub(super) type DaemonReadFileStream =
    Pin<Box<dyn Stream<Item = Result<ReadFileResponse, Status>> + Send + 'static>>;

enum OutboundWriteMsg {
    Chunk(WriteFileChunk),
    Finish,
}

struct WriteFileChunkStream {
    rx: tokio::sync::mpsc::Receiver<OutboundWriteMsg>,
    finished: bool,
}

impl Stream for WriteFileChunkStream {
    type Item = WriteFileChunk;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(OutboundWriteMsg::Chunk(chunk))) => Poll::Ready(Some(chunk)),
            Poll::Ready(Some(OutboundWriteMsg::Finish)) => {
                self.finished = true;
                Poll::Ready(None)
            }
            // Sender dropped without sending `Finish` (validation failure or cancellation):
            // never report clean EOF (`None`) to hyper so dropping the response future sends
            // HTTP/2 `RST_STREAM` instead of committing a truncated upload on upstream.
            Poll::Ready(None) => Poll::Pending,
            Poll::Pending => Poll::Pending,
        }
    }
}

enum ReadFileStreamSource {
    Cached(tokio::sync::mpsc::Receiver<Result<Vec<u8>, std::io::Error>>),
    Upstream(tokio::sync::mpsc::Receiver<Result<ReadFileResponse, Status>>),
    UpstreamPassthrough(Box<Streaming<ReadFileResponse>>),
}

struct TrackedReadFileStream {
    source: ReadFileStreamSource,
    active: ActiveRpcGuard,
}

impl Stream for TrackedReadFileStream {
    type Item = Result<ReadFileResponse, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match &mut this.source {
            ReadFileStreamSource::Cached(rx) => match rx.poll_recv(cx) {
                Poll::Ready(Some(Ok(chunk))) => {
                    this.active.cache.touch_activity();
                    Poll::Ready(Some(Ok(ReadFileResponse { chunk })))
                }
                Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(Status::internal(format!(
                    "local L2 cache read error: {err}"
                ))))),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            },
            ReadFileStreamSource::Upstream(rx) => match rx.poll_recv(cx) {
                Poll::Ready(Some(Ok(resp))) => {
                    this.active.cache.touch_activity();
                    Poll::Ready(Some(Ok(resp)))
                }
                Poll::Ready(Some(Err(status))) => Poll::Ready(Some(Err(status))),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            },
            ReadFileStreamSource::UpstreamPassthrough(stream) => {
                match Pin::new(stream.as_mut()).poll_next(cx) {
                    Poll::Ready(Some(Ok(resp))) => {
                        this.active.cache.touch_activity();
                        Poll::Ready(Some(Ok(resp)))
                    }
                    Poll::Ready(Some(Err(status))) => Poll::Ready(Some(Err(status))),
                    Poll::Ready(None) => Poll::Ready(None),
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }
}

impl DaemonProxy {
    pub(super) async fn handle_read_file(
        &self,
        request: Request<ReadFileRequest>,
    ) -> Result<Response<DaemonReadFileStream>, Status> {
        let active = self.track_active_rpc();
        let meta = request.metadata().clone();
        let req = request.into_inner();
        let scope = self.scope_for(&meta, &req.project_id)?;
        let offset = req.offset;
        let max_bytes = req.max_bytes;

        if let Some(rx) = self
            .inner
            .cache
            .read_cached_file_range(&scope, &req.file_id, offset, max_bytes)
            .await
            .map_err(|e| match e {
                CacheRangeError::OffsetOutOfRange { .. } => Status::out_of_range(e.to_string()),
                CacheRangeError::Io(io_err) => {
                    Status::internal(format!("local L2 cache read error: {io_err}"))
                }
            })?
        {
            self.maybe_flush_stats();
            return Ok(Response::new(Box::pin(TrackedReadFileStream {
                source: ReadFileStreamSource::Cached(rx),
                active,
            })));
        }

        // Ranged cold miss: forward the caller's exact `(offset, max_bytes)` directly to upstream
        // rather than downloading and buffering the entire file.
        if offset > 0 || max_bytes > 0 {
            let out = self.inner.upstream.prepare_outbound(&meta, req)?;
            let channel = self.inner.upstream.channel_for(&meta).await?;
            self.inner.cache.record_miss();
            self.inner.cache.record_upstream_rpc();
            let mut client = backend_client(channel);
            let upstream_stream = client.read_file(out).await?.into_inner();
            self.maybe_flush_stats();
            return Ok(Response::new(Box::pin(TrackedReadFileStream {
                source: ReadFileStreamSource::UpstreamPassthrough(Box::new(upstream_stream)),
                active,
            })));
        }

        // Full cold miss: stream chunks straight to the client while teeing into RAM up to
        // `MAX_L1_OBJECT_BYTES`, spilling to L2 disk staging up to `max_l2_object_bytes`, and
        // continuing pass-through with no hard size cap above that.
        let file_id = req.file_id.clone();
        // Capture `purge_epoch` before dialing upstream so any `DeleteRepository` purge that
        // completes while `ReadFile` is in flight prevents resurrecting authorization markers.
        let mut spooler = self.inner.cache.new_read_blob_spooler();
        let out = self.inner.upstream.prepare_outbound(&meta, req)?;
        let channel = self.inner.upstream.channel_for(&meta).await?;
        self.inner.cache.record_miss();
        self.inner.cache.record_upstream_rpc();
        let mut client = backend_client(channel);
        let mut upstream_stream = client.read_file(out).await?.into_inner();

        let (tx, rx) = tokio::sync::mpsc::channel(STREAM_CHANNEL_CAPACITY);
        let proxy_for_pump = self.clone();
        tokio::spawn(async move {
            let mut clean_eof = false;
            loop {
                let next = tokio::select! {
                    biased;
                    _ = tx.closed() => break,
                    frame = upstream_stream.next() => frame,
                };
                match next {
                    Some(Ok(resp)) => {
                        proxy_for_pump.inner.cache.touch_activity();
                        spooler.write_chunk(&resp.chunk).await;
                        if tx.send(Ok(resp)).await.is_err() {
                            break;
                        }
                    }
                    Some(Err(status)) => {
                        spooler.discard().await;
                        let _ = tx.send(Err(status)).await;
                        proxy_for_pump.maybe_flush_stats();
                        return;
                    }
                    None => {
                        clean_eof = true;
                        break;
                    }
                }
            }
            if clean_eof {
                spooler.commit(scope, file_id, false).await;
            } else {
                spooler.discard().await;
            }
            proxy_for_pump.maybe_flush_stats();
        });

        Ok(Response::new(Box::pin(TrackedReadFileStream {
            source: ReadFileStreamSource::Upstream(rx),
            active,
        })))
    }

    pub(super) async fn handle_write_file(
        &self,
        request: Request<WriteFileRequest>,
    ) -> Result<Response<WriteFileResponse>, Status> {
        let _active = self.track_active_rpc();
        let meta = request.metadata().clone();
        let req = request.into_inner();
        let scope = self.scope_for(&meta, &req.project_id)?;
        let cache_epoch = self.inner.cache.purge_epoch();
        let content_len = req.content.len();
        let max_l2 = self.inner.cache.max_l2_object_bytes();
        let cacheable =
            content_len <= MAX_L1_OBJECT_BYTES || (max_l2 > 0 && (content_len as u64) <= max_l2);
        let content_bytes = cacheable.then(|| Bytes::from(req.content.clone()));
        let out = self.inner.upstream.prepare_outbound(&meta, req)?;
        let channel = self.inner.upstream.channel_for(&meta).await?;
        self.inner.cache.record_upstream_rpc();
        let mut client = backend_client(channel);
        let res = client.write_file(out).await?.into_inner();
        if let Some(content_bytes) = content_bytes {
            self.inner
                .cache
                .put_write_through_if_epoch_async(
                    scope,
                    ObjectKind::File,
                    res.file_id.clone(),
                    content_bytes,
                    Some(cache_epoch),
                )
                .await;
        }
        self.maybe_flush_stats();
        Ok(Response::new(res))
    }

    pub(super) async fn handle_write_file_stream(
        &self,
        request: Request<Streaming<WriteFileChunk>>,
    ) -> Result<Response<WriteFileResponse>, Status> {
        let _active = self.track_active_rpc();
        let meta = request.metadata().clone();
        // Validate upstream target/allowlist and caller authorization metadata before reading
        // any inbound frames so disallowed requests fail immediately without buffering.
        let target_url = self.inner.upstream.resolve_target_url(&meta)?;
        let auth_header = extract_auth_header(&meta)?;
        let mut inbound = request.into_inner();

        let first_chunk = match inbound.next().await {
            Some(Ok(chunk)) => chunk,
            Some(Err(status)) => return Err(status),
            None => {
                return Err(Status::invalid_argument(
                    "WriteFileStream requires at least one chunk with project_id",
                ));
            }
        };
        self.inner.cache.touch_activity();
        if first_chunk.project_id.trim().is_empty() {
            return Err(Status::invalid_argument(
                "first WriteFileStream chunk must specify a non-empty project_id",
            ));
        }
        let declared_total_size = first_chunk.total_size;
        if let Some(exp_id) = &first_chunk.expected_file_id
            && exp_id.len() != cc_common::FILE_ID_LENGTH
        {
            return Err(Status::invalid_argument(format!(
                "WriteFileChunk expected_file_id must be {} bytes",
                cc_common::FILE_ID_LENGTH
            )));
        }
        let project_id = first_chunk.project_id.clone();
        let mut bytes_received = first_chunk.chunk.len() as u64;
        if let Some(total) = declared_total_size
            && bytes_received > total
        {
            return Err(Status::invalid_argument(format!(
                "WriteFileStream exceeded declared total_size {total}"
            )));
        }

        let scope = CacheScope::new_with_auth(target_url, &project_id, auth_header.as_deref());
        let channel = self.inner.upstream.channel_for(&meta).await?;
        let mut spooler = self.inner.cache.new_write_blob_spooler(declared_total_size);
        spooler.write_chunk(&first_chunk.chunk).await;

        // Declare `tx` before `upstream_fut` so any early error drops `upstream_fut` first
        // (canceling the HTTP/2 stream with RST_STREAM instead of sending a clean EOF).
        let (tx, rx) = tokio::sync::mpsc::channel::<OutboundWriteMsg>(STREAM_CHANNEL_CAPACITY);
        let mut tx = Some(tx);
        let outbound_stream = WriteFileChunkStream {
            rx,
            finished: false,
        };
        let out = self
            .inner
            .upstream
            .prepare_outbound(&meta, outbound_stream)?;
        self.inner.cache.record_upstream_rpc();
        let mut client = backend_client(channel);
        let upstream_fut = client.write_file_stream(out);
        tokio::pin!(upstream_fut);

        if tx
            .as_ref()
            .expect("outbound sender present")
            .try_send(OutboundWriteMsg::Chunk(first_chunk))
            .is_err()
        {
            spooler.discard().await;
            let res = upstream_fut.await?.into_inner();
            self.maybe_flush_stats();
            return Ok(Response::new(res));
        }

        loop {
            let next_frame = tokio::select! {
                biased;
                upstream_res = &mut upstream_fut => {
                    spooler.discard().await;
                    return match upstream_res {
                        Err(status) => Err(status),
                        Ok(resp) => {
                            self.maybe_flush_stats();
                            Ok(Response::new(resp.into_inner()))
                        }
                    };
                }
                frame = inbound.next() => frame,
            };
            let Some(frame) = next_frame else {
                break;
            };
            self.inner.cache.touch_activity();
            let chunk = match frame {
                Ok(c) => c,
                Err(status) => {
                    spooler.discard().await;
                    return Err(status);
                }
            };
            if !chunk.project_id.is_empty() && chunk.project_id != project_id {
                spooler.discard().await;
                return Err(Status::invalid_argument(
                    "WriteFileStream chunk project_id does not match the first chunk",
                ));
            }
            if chunk.total_size.is_some() || chunk.expected_file_id.is_some() {
                spooler.discard().await;
                return Err(Status::invalid_argument(
                    "WriteFileChunk total_size and expected_file_id may only be set on the first chunk",
                ));
            }
            bytes_received = bytes_received.saturating_add(chunk.chunk.len() as u64);
            if let Some(total) = declared_total_size
                && bytes_received > total
            {
                spooler.discard().await;
                return Err(Status::invalid_argument(format!(
                    "WriteFileStream exceeded declared total_size {total}"
                )));
            }
            spooler.write_chunk(&chunk.chunk).await;
            let send_res = tokio::select! {
                biased;
                upstream_res = &mut upstream_fut => {
                    spooler.discard().await;
                    return match upstream_res {
                        Err(status) => Err(status),
                        Ok(resp) => {
                            self.maybe_flush_stats();
                            Ok(Response::new(resp.into_inner()))
                        }
                    };
                }
                res = tx
                    .as_ref()
                    .expect("outbound sender present")
                    .send(OutboundWriteMsg::Chunk(chunk)) => res,
            };
            if send_res.is_err() {
                spooler.discard().await;
                let res = upstream_fut.await?.into_inner();
                self.maybe_flush_stats();
                return Ok(Response::new(res));
            }
        }

        if let Some(total) = declared_total_size
            && bytes_received != total
        {
            spooler.discard().await;
            return Err(Status::invalid_argument(format!(
                "WriteFileStream truncated: declared total_size {total}, received {bytes_received}"
            )));
        }

        let finish_res = tokio::select! {
            biased;
            upstream_res = &mut upstream_fut => {
                spooler.discard().await;
                return match upstream_res {
                    Err(status) => Err(status),
                    Ok(resp) => {
                        self.maybe_flush_stats();
                        Ok(Response::new(resp.into_inner()))
                    }
                };
            }
            res = tx
                .as_ref()
                .expect("outbound sender present")
                .send(OutboundWriteMsg::Finish) => res,
        };
        drop(tx.take());
        if finish_res.is_err() {
            spooler.discard().await;
            let res = upstream_fut.await?.into_inner();
            self.maybe_flush_stats();
            return Ok(Response::new(res));
        }
        let res = match upstream_fut.await {
            Ok(resp) => resp.into_inner(),
            Err(status) => {
                spooler.discard().await;
                return Err(status);
            }
        };
        spooler.commit(scope, res.file_id.clone(), true).await;
        self.maybe_flush_stats();
        Ok(Response::new(res))
    }
}
