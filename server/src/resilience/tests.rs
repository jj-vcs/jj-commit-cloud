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

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::*;

async fn call_service<S, Req>(svc: &mut S, req: Req) -> Result<S::Response, S::Error>
where
    S: Service<Req>,
{
    std::future::poll_fn(|cx| svc.poll_ready(cx)).await?;
    svc.call(req).await
}

async fn next_frame<B: Body + Unpin>(body: &mut B) -> Option<Result<Frame<B::Data>, B::Error>> {
    std::future::poll_fn(|cx| Pin::new(&mut *body).poll_frame(cx)).await
}

#[test]
fn parse_grpc_timeout_handles_all_units_and_rejects_malformed() {
    assert_eq!(parse_grpc_timeout("1H"), Some(Duration::from_secs(3600)));
    assert_eq!(parse_grpc_timeout("2M"), Some(Duration::from_secs(120)));
    assert_eq!(parse_grpc_timeout("10S"), Some(Duration::from_secs(10)));
    assert_eq!(parse_grpc_timeout("500m"), Some(Duration::from_millis(500)));
    assert_eq!(
        parse_grpc_timeout("1000u"),
        Some(Duration::from_micros(1000))
    );
    assert_eq!(
        parse_grpc_timeout("500000n"),
        Some(Duration::from_nanos(500_000))
    );
    assert_eq!(parse_grpc_timeout("0n"), Some(Duration::ZERO));
    assert_eq!(parse_grpc_timeout("0m"), Some(Duration::ZERO));
    assert_eq!(parse_grpc_timeout("00000000S"), Some(Duration::ZERO));
    assert_eq!(
        parse_grpc_timeout("99999999H"),
        Some(Duration::from_secs(99_999_999 * 3600))
    );

    for invalid in [
        "",
        "S",
        "10",
        "10s",
        "10ms",
        "-5S",
        "+5S",
        " 5S",
        "5S ",
        "100000000S",
        "12aS",
    ] {
        assert_eq!(parse_grpc_timeout(invalid), None, "input={invalid:?}");
    }
}

#[test]
fn sanitize_grpc_error_message_strips_controls_and_bounds_utf8_safely() {
    assert_eq!(
        sanitize_grpc_error_message("hello\r\nworld\0\x1f\x7f!"),
        "hello  world   !"
    );

    let raw_invalid = [b'o', b'k', b'\n', 0xff, 0xfe, b'!'];
    let sanitized_bytes = sanitize_grpc_error_bytes(&raw_invalid);
    assert!(sanitized_bytes.starts_with("ok "));
    assert!(sanitized_bytes.contains('\u{fffd}'));
    assert!(!sanitized_bytes.bytes().any(|b| b.is_ascii_control()));

    // Oversized multi-byte UTF-8 (4-byte emoji and 3-byte CJK) truncates on a
    // char boundary without panicking and stays within MAX_GRPC_ERROR_MESSAGE_BYTES.
    let emojis = "🚀".repeat(800);
    let truncated_emojis = sanitize_grpc_error_message(&emojis);
    assert!(truncated_emojis.len() <= MAX_GRPC_ERROR_MESSAGE_BYTES);
    assert!(truncated_emojis.ends_with("..."));

    let cjk = "漢".repeat(1000);
    let truncated_cjk = sanitize_grpc_error_message(&cjk);
    assert!(truncated_cjk.len() <= MAX_GRPC_ERROR_MESSAGE_BYTES);
    assert!(truncated_cjk.ends_with("..."));
}

#[test]
fn sanitize_response_grpc_message_preserves_valid_percent_encoding_and_bounds_headers() {
    // Tonic 0.12 encodes `' '` as `%20` and leaves `'%'` raw; sanitization must preserve
    // exact decoded content without double-encoding and be idempotent across repeated calls.
    let mut normal_resp = tonic::Status::invalid_argument("café 100% done").into_http();
    sanitize_response_grpc_message(normal_resp.headers_mut());
    let after_first = normal_resp
        .headers()
        .get(GRPC_MESSAGE_HEADER)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(!after_first.contains(' '));
    let parsed = tonic::Status::from_header_map(normal_resp.headers()).unwrap();
    assert_eq!(parsed.message(), "café 100% done");

    sanitize_response_grpc_message(normal_resp.headers_mut());
    let after_second = normal_resp
        .headers()
        .get(GRPC_MESSAGE_HEADER)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(after_first, after_second);

    // Leading/trailing and interior control chars encode as `%20` (not raw HTTP OWS spaces).
    let mut ctrl_resp = tonic::Status::internal("\r\nline1\r\nline2\0\n").into_http();
    sanitize_response_grpc_message(ctrl_resp.headers_mut());
    let ctrl_raw = ctrl_resp
        .headers()
        .get(GRPC_MESSAGE_HEADER)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(ctrl_raw.starts_with("%20%20"));
    assert!(ctrl_raw.ends_with("%20%20"));
    assert!(!ctrl_raw.contains(' '));
    let parsed_ctrl = tonic::Status::from_header_map(ctrl_resp.headers()).unwrap();
    assert_eq!(parsed_ctrl.message(), "  line1  line2  ");

    // Oversized multi-byte percent-encoded header truncates on a char boundary.
    let huge_msg = "🎉".repeat(600);
    let mut huge_resp = tonic::Status::internal(huge_msg).into_http();
    sanitize_response_grpc_message(huge_resp.headers_mut());
    let raw_hdr = huge_resp
        .headers()
        .get(GRPC_MESSAGE_HEADER)
        .unwrap()
        .as_bytes();
    assert!(raw_hdr.len() <= MAX_GRPC_ERROR_MESSAGE_BYTES);
    let parsed_huge = tonic::Status::from_header_map(huge_resp.headers()).unwrap();
    assert!(parsed_huge.message().ends_with("..."));
    assert!(parsed_huge.message().starts_with("🎉"));
}

#[test]
fn extract_panic_summary_handles_string_and_non_string_payloads() {
    let s: Box<dyn Any + Send> = Box::new("boom\nsecret");
    assert_eq!(extract_panic_summary(&*s), "boom secret");

    let owned: Box<dyn Any + Send> = Box::new("x".repeat(500));
    let summary = extract_panic_summary(&*owned);
    assert_eq!(summary.len(), MAX_PANIC_SUMMARY_BYTES);
    assert!(summary.ends_with("..."));

    let num: Box<dyn Any + Send> = Box::new(42usize);
    assert_eq!(extract_panic_summary(&*num), "non-string panic payload");
}

struct StreamTestBody {
    step: u8,
    mode: StreamBodyMode,
    saw_deadline_in_poll: Arc<AtomicBool>,
}

#[derive(Clone, Copy)]
enum StreamBodyMode {
    PanicAfterChunk,
    OversizedTrailer,
    StallAfterChunk,
    TrailerThenPanic,
    ErrStatusOversized,
    PanicInTraitMethods,
    PanicOnDrop,
}

impl Body for StreamTestBody {
    type Data = Bytes;
    type Error = tonic::Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        if remaining_rpc_deadline().is_some_and(|rem| !rem.is_zero()) {
            this.saw_deadline_in_poll.store(true, Ordering::SeqCst);
        }
        match (this.mode, this.step) {
            (StreamBodyMode::PanicAfterChunk, 0) => {
                this.step = 1;
                Poll::Ready(Some(Ok(Frame::data(Bytes::from_static(b"chunk-1")))))
            }
            (StreamBodyMode::PanicAfterChunk, _) => {
                panic!("streaming body exploded\nwith detail");
            }
            (StreamBodyMode::OversizedTrailer, 0) => {
                this.step = 1;
                let msg = format!("\nboom\r\n{}", "Z".repeat(4000));
                let status_resp = tonic::Status::internal(msg).into_http();
                let mut trailers = HeaderMap::new();
                trailers.insert("grpc-status", HeaderValue::from_static("13"));
                trailers.insert(
                    GRPC_MESSAGE_HEADER,
                    status_resp
                        .headers()
                        .get(GRPC_MESSAGE_HEADER)
                        .unwrap()
                        .clone(),
                );
                Poll::Ready(Some(Ok(Frame::trailers(trailers))))
            }
            (StreamBodyMode::OversizedTrailer, _) => Poll::Ready(None),
            (StreamBodyMode::StallAfterChunk, 0) => {
                this.step = 1;
                Poll::Ready(Some(Ok(Frame::data(Bytes::from_static(b"first-chunk")))))
            }
            (StreamBodyMode::StallAfterChunk, _) => Poll::Pending,
            (StreamBodyMode::TrailerThenPanic, 0) => {
                this.step = 1;
                let mut trailers = HeaderMap::new();
                trailers.insert("grpc-status", HeaderValue::from_static("0"));
                Poll::Ready(Some(Ok(Frame::trailers(trailers))))
            }
            (StreamBodyMode::TrailerThenPanic, _) => {
                panic!("inner body must never be polled after yielding trailers");
            }
            (StreamBodyMode::ErrStatusOversized, 0) => {
                this.step = 1;
                let msg = "🚀 ".repeat(500);
                Poll::Ready(Some(Err(tonic::Status::invalid_argument(msg))))
            }
            (StreamBodyMode::ErrStatusOversized, _) => Poll::Ready(None),
            (StreamBodyMode::PanicInTraitMethods | StreamBodyMode::PanicOnDrop, _) => {
                Poll::Ready(None)
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        if matches!(self.mode, StreamBodyMode::PanicInTraitMethods) {
            panic!("is_end_stream exploded");
        }
        false
    }

    fn size_hint(&self) -> http_body::SizeHint {
        if matches!(self.mode, StreamBodyMode::PanicInTraitMethods) {
            panic!("size_hint exploded");
        }
        http_body::SizeHint::default()
    }
}

impl Drop for StreamTestBody {
    fn drop(&mut self) {
        if matches!(self.mode, StreamBodyMode::PanicOnDrop) {
            panic!("streaming body drop exploded");
        }
    }
}

#[derive(Clone)]
struct TestService {
    mode: TestMode,
    saw_deadline_in_call: Arc<AtomicBool>,
    saw_deadline_in_fut: Arc<AtomicBool>,
    saw_deadline_in_body: Arc<AtomicBool>,
}

#[derive(Clone, Copy)]
enum TestMode {
    Ok,
    SyncPanic,
    AsyncPanic,
    NonStringAsyncPanic,
    PollReadyPanic,
    OversizedError,
    StreamingPanicBody,
    StreamingOversizedTrailer,
    StreamingStallBody,
    StreamingTrailerThenPanic,
    StreamingErrStatusOversized,
    StreamingTraitMethodPanic,
    StreamingDropPanic,
    SlowPendingFuture,
}

impl Service<http::Request<()>> for TestService {
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if matches!(self.mode, TestMode::PollReadyPanic) {
            panic!("poll_ready exploded");
        }
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: http::Request<()>) -> Self::Future {
        if current_rpc_deadline().is_some() {
            self.saw_deadline_in_call.store(true, Ordering::SeqCst);
        }
        let mode = self.mode;
        let saw_fut = Arc::clone(&self.saw_deadline_in_fut);
        let saw_body = Arc::clone(&self.saw_deadline_in_body);
        match mode {
            TestMode::SyncPanic => panic!("sync boom\nwith newline"),
            TestMode::AsyncPanic => Box::pin(async move {
                tokio::task::yield_now().await;
                panic!("async boom");
            }),
            TestMode::NonStringAsyncPanic => Box::pin(async move {
                std::panic::panic_any(12345u64);
            }),
            TestMode::OversizedError => Box::pin(async move {
                let msg = format!("bad\n{}", "A".repeat(4000));
                Ok(tonic::Status::internal(msg).into_http())
            }),
            TestMode::StreamingPanicBody => Box::pin(async move {
                let body = tonic::body::BoxBody::new(StreamTestBody {
                    step: 0,
                    mode: StreamBodyMode::PanicAfterChunk,
                    saw_deadline_in_poll: saw_body,
                });
                Ok(http::Response::builder()
                    .status(200)
                    .header("content-type", "application/grpc")
                    .body(body)
                    .unwrap())
            }),
            TestMode::StreamingOversizedTrailer => Box::pin(async move {
                let body = tonic::body::BoxBody::new(StreamTestBody {
                    step: 0,
                    mode: StreamBodyMode::OversizedTrailer,
                    saw_deadline_in_poll: saw_body,
                });
                Ok(http::Response::builder()
                    .status(200)
                    .header("content-type", "application/grpc")
                    .body(body)
                    .unwrap())
            }),
            TestMode::StreamingStallBody => Box::pin(async move {
                let body = tonic::body::BoxBody::new(StreamTestBody {
                    step: 0,
                    mode: StreamBodyMode::StallAfterChunk,
                    saw_deadline_in_poll: saw_body,
                });
                Ok(http::Response::builder()
                    .status(200)
                    .header("content-type", "application/grpc")
                    .body(body)
                    .unwrap())
            }),
            TestMode::StreamingTrailerThenPanic => Box::pin(async move {
                let body = tonic::body::BoxBody::new(StreamTestBody {
                    step: 0,
                    mode: StreamBodyMode::TrailerThenPanic,
                    saw_deadline_in_poll: saw_body,
                });
                Ok(http::Response::builder()
                    .status(200)
                    .header("content-type", "application/grpc")
                    .body(body)
                    .unwrap())
            }),
            TestMode::StreamingErrStatusOversized => Box::pin(async move {
                let body = tonic::body::BoxBody::new(StreamTestBody {
                    step: 0,
                    mode: StreamBodyMode::ErrStatusOversized,
                    saw_deadline_in_poll: saw_body,
                });
                Ok(http::Response::builder()
                    .status(200)
                    .header("content-type", "application/grpc")
                    .body(body)
                    .unwrap())
            }),
            TestMode::StreamingTraitMethodPanic => Box::pin(async move {
                let body = tonic::body::BoxBody::new(StreamTestBody {
                    step: 0,
                    mode: StreamBodyMode::PanicInTraitMethods,
                    saw_deadline_in_poll: saw_body,
                });
                Ok(http::Response::builder()
                    .status(200)
                    .header("content-type", "application/grpc")
                    .body(body)
                    .unwrap())
            }),
            TestMode::StreamingDropPanic => Box::pin(async move {
                let body = tonic::body::BoxBody::new(StreamTestBody {
                    step: 0,
                    mode: StreamBodyMode::PanicOnDrop,
                    saw_deadline_in_poll: saw_body,
                });
                Ok(http::Response::builder()
                    .status(200)
                    .header("content-type", "application/grpc")
                    .body(body)
                    .unwrap())
            }),
            TestMode::SlowPendingFuture => Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                Ok(http::Response::builder()
                    .status(200)
                    .header("content-type", "application/grpc")
                    .header("grpc-status", "0")
                    .body(tonic::body::empty_body())
                    .unwrap())
            }),
            TestMode::Ok | TestMode::PollReadyPanic => Box::pin(async move {
                if remaining_rpc_deadline().is_some_and(|rem| !rem.is_zero()) {
                    saw_fut.store(true, Ordering::SeqCst);
                }
                Ok(http::Response::builder()
                    .status(200)
                    .header("content-type", "application/grpc")
                    .header("grpc-status", "0")
                    .body(tonic::body::empty_body())
                    .unwrap())
            }),
        }
    }
}

fn make_test_service(
    mode: TestMode,
) -> (
    TestService,
    Arc<AtomicBool>,
    Arc<AtomicBool>,
    Arc<AtomicBool>,
) {
    let in_call = Arc::new(AtomicBool::new(false));
    let in_fut = Arc::new(AtomicBool::new(false));
    let in_body = Arc::new(AtomicBool::new(false));
    (
        TestService {
            mode,
            saw_deadline_in_call: Arc::clone(&in_call),
            saw_deadline_in_fut: Arc::clone(&in_fut),
            saw_deadline_in_body: Arc::clone(&in_body),
        },
        in_call,
        in_fut,
        in_body,
    )
}

#[tokio::test]
async fn resilience_layer_catches_sync_async_and_poll_ready_panics_and_increments_metric() {
    let metrics = ServerMetrics::new();

    for (idx, mode) in [
        TestMode::SyncPanic,
        TestMode::AsyncPanic,
        TestMode::NonStringAsyncPanic,
        TestMode::PollReadyPanic,
    ]
    .into_iter()
    .enumerate()
    {
        let (inner, _, _, _) = make_test_service(mode);
        let mut svc = ResilienceLayer::new(metrics.clone()).layer(inner);
        let req = http::Request::builder()
            .uri("/commit_cloud.backend.BackendService/ReadCommit")
            .body(())
            .unwrap();
        let resp = call_service(&mut svc, req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("application/grpc")
        );
        assert_eq!(
            resp.headers()
                .get("grpc-status")
                .and_then(|v| v.to_str().ok()),
            Some("13")
        );
        let status = tonic::Status::from_header_map(resp.headers()).unwrap();
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), "internal server error");
        assert_eq!(metrics.snapshot().panics_total, (idx + 1) as u64);
    }

    assert!(
        metrics
            .render_prometheus()
            .contains("cloudjj_panics_total 4\n")
    );
}

#[tokio::test]
async fn resilience_body_catches_streaming_panics_scopes_deadline_and_sanitizes_trailers() {
    let metrics = ServerMetrics::new();

    // Streaming panic after an initial data chunk yields a terminal INTERNAL trailer,
    // terminates the stream, propagates RPC_DEADLINE during poll_frame, and counts panics.
    let (inner_panic, _, _, in_body_panic) = make_test_service(TestMode::StreamingPanicBody);
    let mut svc_panic = ResilienceLayer::new(metrics.clone()).layer(inner_panic);
    let req = http::Request::builder()
        .uri("/commit_cloud.backend.BackendService/ReadFile")
        .header(GRPC_TIMEOUT_HEADER, "5S")
        .body(())
        .unwrap();
    let mut body = call_service(&mut svc_panic, req).await.unwrap().into_body();

    let first = next_frame(&mut body).await.unwrap().unwrap();
    assert_eq!(first.into_data().unwrap(), Bytes::from_static(b"chunk-1"));
    assert!(in_body_panic.load(Ordering::SeqCst));

    let second = next_frame(&mut body).await.unwrap().unwrap();
    let trailers = second.into_trailers().expect("expected terminal trailers");
    assert_eq!(
        trailers.get("grpc-status").and_then(|v| v.to_str().ok()),
        Some("13")
    );
    let status = tonic::Status::from_header_map(&trailers).unwrap();
    assert_eq!(status.code(), tonic::Code::Internal);
    assert_eq!(status.message(), "internal server error");
    assert!(next_frame(&mut body).await.is_none());
    assert_eq!(metrics.snapshot().panics_total, 1);

    // Oversized trailer `grpc-message` with control characters is sanitized and bounded.
    let (inner_trailer, _, _, _) = make_test_service(TestMode::StreamingOversizedTrailer);
    let mut svc_trailer = ResilienceLayer::new(metrics).layer(inner_trailer);
    let req_trailer = http::Request::builder()
        .uri("/commit_cloud.backend.BackendService/ReadFile")
        .body(())
        .unwrap();
    let mut trailer_body = call_service(&mut svc_trailer, req_trailer)
        .await
        .unwrap()
        .into_body();
    let frame = next_frame(&mut trailer_body).await.unwrap().unwrap();
    let trailers = frame.into_trailers().expect("expected sanitized trailers");
    let raw_msg = trailers.get(GRPC_MESSAGE_HEADER).unwrap().to_str().unwrap();
    assert!(raw_msg.len() <= MAX_GRPC_ERROR_MESSAGE_BYTES);
    assert!(raw_msg.starts_with("%20boom%20%20Z"));
    assert!(!raw_msg.contains(' '));
    let parsed = tonic::Status::from_header_map(&trailers).unwrap();
    assert!(parsed.message().starts_with(" boom  Z"));
    assert!(parsed.message().ends_with("..."));
}

#[tokio::test]
async fn resilience_layer_propagates_deadline_and_fails_fast_when_expired() {
    let metrics = ServerMetrics::new();

    // Active timeout propagates into both synchronous `call()` and polled future.
    let (inner, in_call, in_fut, _) = make_test_service(TestMode::Ok);
    let mut svc = ResilienceLayer::new(metrics.clone()).layer(inner);
    let req = http::Request::builder()
        .uri("/commit_cloud.backend.BackendService/ReadCommit")
        .header(GRPC_TIMEOUT_HEADER, "5S")
        .body(())
        .unwrap();
    let resp = call_service(&mut svc, req).await.unwrap();
    assert_eq!(
        resp.headers()
            .get("grpc-status")
            .and_then(|v| v.to_str().ok()),
        Some("0")
    );
    assert!(in_call.load(Ordering::SeqCst));
    assert!(in_fut.load(Ordering::SeqCst));

    // Zero timeout fails fast with DEADLINE_EXCEEDED without invoking inner service.
    let (inner_exp, in_call_exp, in_fut_exp, _) = make_test_service(TestMode::SyncPanic);
    let mut svc_exp = ResilienceLayer::new(metrics.clone()).layer(inner_exp);
    let expired_req = http::Request::builder()
        .uri("/commit_cloud.backend.BackendService/ReadCommit")
        .header(GRPC_TIMEOUT_HEADER, "0m")
        .body(())
        .unwrap();
    let expired_resp = call_service(&mut svc_exp, expired_req).await.unwrap();
    let status = tonic::Status::from_header_map(expired_resp.headers()).unwrap();
    assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
    assert!(!in_call_exp.load(Ordering::SeqCst));
    assert!(!in_fut_exp.load(Ordering::SeqCst));
    assert_eq!(metrics.snapshot().panics_total, 0);

    // Oversized outgoing grpc-message header is sanitized and bounded.
    let (inner_err, _, _, _) = make_test_service(TestMode::OversizedError);
    let mut svc_err = ResilienceLayer::new(metrics).layer(inner_err);
    let err_req = http::Request::builder()
        .uri("/commit_cloud.backend.BackendService/ReadCommit")
        .body(())
        .unwrap();
    let err_resp = call_service(&mut svc_err, err_req).await.unwrap();
    let err_hdr = err_resp
        .headers()
        .get(GRPC_MESSAGE_HEADER)
        .unwrap()
        .as_bytes();
    assert!(err_hdr.len() <= MAX_GRPC_ERROR_MESSAGE_BYTES);
    let err_status = tonic::Status::from_header_map(err_resp.headers()).unwrap();
    assert!(err_status.message().starts_with("bad A"));
    assert!(err_status.message().ends_with("..."));
}

#[tokio::test]
async fn run_with_rpc_deadline_fails_fast_when_expired_and_aborts_on_timeout() {
    let expired = Instant::now();
    tokio::time::sleep(Duration::from_millis(2)).await;
    let err = with_rpc_deadline(Some(expired), async {
        run_with_rpc_deadline("test op", async { Ok::<(), StoreError>(()) }).await
    })
    .await
    .unwrap_err();
    assert!(matches!(err, StoreError::DeadlineExceeded(_)));

    let short = Instant::now().checked_add(Duration::from_millis(20));
    let err = with_rpc_deadline(short, async {
        run_with_rpc_deadline("slow op", async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            Ok::<(), StoreError>(())
        })
        .await
    })
    .await
    .unwrap_err();
    assert!(matches!(err, StoreError::DeadlineExceeded(_)));
}

#[tokio::test]
async fn resilience_body_enforces_mid_stream_deadline_and_terminates_after_inner_trailers() {
    let metrics = ServerMetrics::new();

    // A stalling streaming body under a short timeout emits its first data chunk, then
    // terminates with grpc-status=4 (DEADLINE_EXCEEDED) once the deadline elapses.
    let (inner_stall, _, _, _) = make_test_service(TestMode::StreamingStallBody);
    let mut svc_stall = ResilienceLayer::new(metrics.clone()).layer(inner_stall);
    let req = http::Request::builder()
        .uri("/commit_cloud.backend.BackendService/ReadFile")
        .header(GRPC_TIMEOUT_HEADER, "40m")
        .body(())
        .unwrap();
    let mut body = call_service(&mut svc_stall, req).await.unwrap().into_body();
    let first = next_frame(&mut body).await.unwrap().unwrap();
    assert_eq!(
        first.into_data().unwrap(),
        Bytes::from_static(b"first-chunk")
    );

    let second = next_frame(&mut body).await.unwrap().unwrap();
    let trailers = second.into_trailers().expect("expected deadline trailers");
    assert_eq!(
        trailers.get("grpc-status").and_then(|v| v.to_str().ok()),
        Some("4")
    );
    let status = tonic::Status::from_header_map(&trailers).unwrap();
    assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
    assert_eq!(status.message(), "request deadline exceeded");
    assert!(next_frame(&mut body).await.is_none());

    // Once inner yields a terminal trailers frame, ResilienceBody drops inner and never polls it again.
    let (inner_tr, _, _, _) = make_test_service(TestMode::StreamingTrailerThenPanic);
    let mut svc_tr = ResilienceLayer::new(metrics.clone()).layer(inner_tr);
    let req_tr = http::Request::builder()
        .uri("/commit_cloud.backend.BackendService/ReadFile")
        .body(())
        .unwrap();
    let mut tr_body = call_service(&mut svc_tr, req_tr).await.unwrap().into_body();
    let tr_frame = next_frame(&mut tr_body).await.unwrap().unwrap();
    let tr_hdrs = tr_frame.into_trailers().unwrap();
    assert_eq!(
        tr_hdrs.get("grpc-status").and_then(|v| v.to_str().ok()),
        Some("0")
    );
    assert!(next_frame(&mut tr_body).await.is_none());
    assert_eq!(metrics.snapshot().panics_total, 0);
}

#[tokio::test]
async fn resilience_layer_enforces_strictest_duplicate_grpc_timeout_on_inner_future() {
    let metrics = ServerMetrics::new();
    let (inner, _, _, _) = make_test_service(TestMode::SlowPendingFuture);
    let mut svc = ResilienceLayer::new(metrics.clone()).layer(inner);
    let req = http::Request::builder()
        .uri("/commit_cloud.backend.BackendService/ReadCommit")
        .header(GRPC_TIMEOUT_HEADER, "5S")
        .header(GRPC_TIMEOUT_HEADER, "30m")
        .body(())
        .unwrap();
    let start = Instant::now();
    let resp = call_service(&mut svc, req).await.unwrap();
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "strictest duplicate grpc-timeout must abort slow inner future promptly"
    );
    let status = tonic::Status::from_header_map(resp.headers()).unwrap();
    assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
    assert_eq!(metrics.snapshot().panics_total, 0);
}

#[tokio::test]
async fn resilience_body_guards_trait_methods_and_wire_bounds_errors_and_duplicate_headers() {
    let metrics = ServerMetrics::new();

    // Panics in `is_end_stream` and `size_hint` are caught without double-counting, and the
    // subsequent `poll_frame` emits a terminal INTERNAL trailer.
    let (inner_trait, _, _, _) = make_test_service(TestMode::StreamingTraitMethodPanic);
    let mut svc_trait = ResilienceLayer::new(metrics.clone()).layer(inner_trait);
    let req_trait = http::Request::builder()
        .uri("/commit_cloud.backend.BackendService/ReadFile")
        .body(())
        .unwrap();
    let mut trait_body = call_service(&mut svc_trait, req_trait)
        .await
        .unwrap()
        .into_body();
    assert!(!trait_body.is_end_stream());
    assert_eq!(metrics.snapshot().panics_total, 1);
    let _ = trait_body.size_hint();
    assert_eq!(metrics.snapshot().panics_total, 1);
    let frame = next_frame(&mut trait_body).await.unwrap().unwrap();
    let trailers = frame
        .into_trailers()
        .expect("expected terminal INTERNAL trailer");
    assert_eq!(
        trailers.get("grpc-status").and_then(|v| v.to_str().ok()),
        Some("13")
    );
    assert!(next_frame(&mut trait_body).await.is_none());
    assert_eq!(metrics.snapshot().panics_total, 1);

    // Percent-encoded wire length of sanitized spaces/emojis stays within MAX_GRPC_ERROR_MESSAGE_BYTES.
    for raw in [" ".repeat(1000), "🚀".repeat(400)] {
        let sanitized = sanitize_grpc_error_message(&raw);
        let http_resp = tonic::Status::invalid_argument(sanitized).into_http();
        let hdr = http_resp
            .headers()
            .get(GRPC_MESSAGE_HEADER)
            .unwrap()
            .as_bytes();
        assert!(
            hdr.len() <= MAX_GRPC_ERROR_MESSAGE_BYTES,
            "percent-encoded wire header exceeded limit: {}",
            hdr.len()
        );
    }

    // Duplicate `grpc-message` headers collapse into a single wire-bounded header.
    let mut headers = HeaderMap::new();
    headers.append(
        GRPC_MESSAGE_HEADER,
        HeaderValue::from_str(&"A".repeat(1500)).unwrap(),
    );
    headers.append(
        GRPC_MESSAGE_HEADER,
        HeaderValue::from_str(&"B".repeat(1500)).unwrap(),
    );
    sanitize_response_grpc_message(&mut headers);
    let all_vals: Vec<_> = headers.get_all(GRPC_MESSAGE_HEADER).iter().collect();
    assert_eq!(all_vals.len(), 1);
    assert!(all_vals[0].as_bytes().len() <= MAX_GRPC_ERROR_MESSAGE_BYTES);

    // `ResilienceBody::poll_frame` returning `Err(status)` with oversized multi-byte message
    // emits a wire-bounded `grpc-message` header.
    let (inner_err, _, _, _) = make_test_service(TestMode::StreamingErrStatusOversized);
    let mut svc_err = ResilienceLayer::new(metrics).layer(inner_err);
    let req_err = http::Request::builder()
        .uri("/commit_cloud.backend.BackendService/ReadFile")
        .body(())
        .unwrap();
    let mut err_body = call_service(&mut svc_err, req_err)
        .await
        .unwrap()
        .into_body();
    let err_status = next_frame(&mut err_body)
        .await
        .unwrap()
        .expect_err("expected Err(Status)");
    let err_http = err_status.into_http();
    let err_hdr = err_http
        .headers()
        .get(GRPC_MESSAGE_HEADER)
        .unwrap()
        .as_bytes();
    assert!(err_hdr.len() <= MAX_GRPC_ERROR_MESSAGE_BYTES);
}

#[tokio::test]
async fn spawn_blob_stream_producer_catches_panics_and_enforces_deadline() {
    let metrics = ServerMetrics::new();

    // Producer panic increments `panics_total` and emits `Err(StoreError::Backend)` instead of
    // silently closing the stream with truncation.
    let (tx_panic, mut rx_panic) = tokio::sync::mpsc::channel(4);
    with_rpc_context(None, Some(metrics.clone()), async move {
        spawn_blob_stream_producer("test panic stream", tx_panic, |tx| async move {
            let _ = tx.send(Ok(Bytes::from_static(b"part-1"))).await;
            panic!("secret stream panic payload");
        });
    })
    .await;
    assert_eq!(
        rx_panic.recv().await.unwrap().unwrap(),
        Bytes::from_static(b"part-1")
    );
    let err = rx_panic
        .recv()
        .await
        .expect("producer panic must send error before closing")
        .unwrap_err();
    assert!(matches!(err, StoreError::Backend(_)));
    let status = tonic::Status::from(err);
    assert_eq!(status.code(), tonic::Code::Internal);
    assert!(!status.message().contains("secret stream panic payload"));
    assert_eq!(metrics.snapshot().panics_total, 1);

    // Producer stalling past `RPC_DEADLINE` emits `Err(StoreError::DeadlineExceeded)`.
    let (tx_dl, mut rx_dl) = tokio::sync::mpsc::channel(4);
    let short = Instant::now().checked_add(Duration::from_millis(30));
    with_rpc_context(short, Some(metrics), async move {
        spawn_blob_stream_producer("test deadline stream", tx_dl, |_tx| async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
    })
    .await;
    let dl_err = rx_dl
        .recv()
        .await
        .expect("deadline expiry must emit DeadlineExceeded")
        .unwrap_err();
    assert!(matches!(dl_err, StoreError::DeadlineExceeded(_)));
}

#[tokio::test]
async fn resilience_body_catches_panicking_inner_drop_on_terminate_and_early_drop() {
    // Inner body panicking on Drop during `terminate()` (EOF) is caught.
    let metrics_eof = ServerMetrics::new();
    let (inner_eof, _, _, _) = make_test_service(TestMode::StreamingDropPanic);
    let mut svc_eof = ResilienceLayer::new(metrics_eof.clone()).layer(inner_eof);
    let req_eof = http::Request::builder()
        .uri("/commit_cloud.backend.BackendService/ReadFile")
        .body(())
        .unwrap();
    let mut body_eof = call_service(&mut svc_eof, req_eof)
        .await
        .unwrap()
        .into_body();
    assert!(next_frame(&mut body_eof).await.is_none());
    drop(body_eof);
    assert_eq!(metrics_eof.snapshot().panics_total, 1);

    // Inner body panicking on early `Drop` (before stream completion) is caught.
    let metrics_drop = ServerMetrics::new();
    let (inner_drop, _, _, _) = make_test_service(TestMode::StreamingDropPanic);
    let mut svc_drop = ResilienceLayer::new(metrics_drop.clone()).layer(inner_drop);
    let req_drop = http::Request::builder()
        .uri("/commit_cloud.backend.BackendService/ReadFile")
        .body(())
        .unwrap();
    let body_drop = call_service(&mut svc_drop, req_drop)
        .await
        .unwrap()
        .into_body();
    drop(body_drop);
    assert_eq!(metrics_drop.snapshot().panics_total, 1);
}
