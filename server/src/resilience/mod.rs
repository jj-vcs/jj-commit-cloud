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

//! Server-wide gRPC resilience middleware: panic unwinding, `grpc-message` sanitization,
//! and `grpc-timeout` deadline propagation into storage transactions.
//!
//! # Cross-Process and RPC Boundaries
//! [`ResilienceLayer`] wraps the outermost Tonic `Server` stack (outside [`MetricsLayer`]
//! and [`ServerAuthLayer`]) so panics anywhere in middleware, unary handlers, or
//! server-streaming response bodies unwind into a clean HTTP/2 `INTERNAL` (`grpc-status: 13`)
//! response/trailer instead of aborting connections or the process. Outgoing `grpc-message`
//! headers and trailers are bounded to [`MAX_GRPC_ERROR_MESSAGE_BYTES`] valid UTF-8 bytes
//! without ASCII control characters or raw spaces. Incoming `grpc-timeout` headers propagate
//! via [`RPC_DEADLINE`] into Spanner, PostgreSQL, streaming polls, and reconciliation.

use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::FutureExt as _;
use futures::future::BoxFuture;
use http::header::{HeaderMap, HeaderValue};
use http_body::{Body, Frame, SizeHint};
use tower::{Layer, Service};

use crate::metrics::ServerMetrics;
use crate::store::{StoreError, StoreResult};

/// Maximum percent-encoded wire byte length for outgoing gRPC error messages so oversized
/// storage errors never blow HTTP/2 header frame limits.
pub const MAX_GRPC_ERROR_MESSAGE_BYTES: usize = 2048;

const MAX_PANIC_SUMMARY_BYTES: usize = 256;
const MAX_LOG_PATH_BYTES: usize = 256;
const TRUNCATION_SUFFIX: &str = "...";
const GRPC_MESSAGE_HEADER: &str = "grpc-message";
const GRPC_TIMEOUT_HEADER: &str = "grpc-timeout";

tokio::task_local! {
    static RPC_DEADLINE: Option<Instant>;
    static RPC_METRICS: Option<ServerMetrics>;
}

/// Returns the active gRPC request deadline for the current task, if a finite
/// `grpc-timeout` header was provided.
pub fn current_rpc_deadline() -> Option<Instant> {
    RPC_DEADLINE.try_with(|deadline| *deadline).ok().flatten()
}

/// Returns the remaining budget before the active gRPC request deadline expires:
/// - `None` if no `grpc-timeout` header was set (or it overflowed `Instant`),
/// - `Some(Duration::ZERO)` if the deadline has already elapsed,
/// - `Some(rem)` with `rem > Duration::ZERO` while the deadline is still active.
pub fn remaining_rpc_deadline() -> Option<Duration> {
    current_rpc_deadline().map(|deadline| deadline.saturating_duration_since(Instant::now()))
}

/// Returns the active [`ServerMetrics`] sink bound by [`ResilienceLayer`] for the current task.
pub(crate) fn current_rpc_metrics() -> Option<ServerMetrics> {
    RPC_METRICS.try_with(Clone::clone).ok().flatten()
}

/// Runs `fut` with [`RPC_DEADLINE`] scoped to `deadline`.
pub async fn with_rpc_deadline<F: Future>(deadline: Option<Instant>, fut: F) -> F::Output {
    RPC_DEADLINE.scope(deadline, fut).await
}

/// Runs `fut` with both [`RPC_DEADLINE`] and [`RPC_METRICS`] scoped for the duration of `fut`.
pub(crate) async fn with_rpc_context<F: Future>(
    deadline: Option<Instant>,
    metrics: Option<ServerMetrics>,
    fut: F,
) -> F::Output {
    RPC_DEADLINE
        .scope(deadline, RPC_METRICS.scope(metrics, fut))
        .await
}

/// Increments `panics_total` on `explicit_metrics` when provided, otherwise on the active
/// task-local [`ServerMetrics`] bound by [`ResilienceLayer`].
pub(crate) fn record_panic_with_fallback(explicit_metrics: Option<&ServerMetrics>) {
    if let Some(m) = explicit_metrics {
        m.record_panic();
    } else {
        let _ = RPC_METRICS.try_with(|m| {
            if let Some(m) = m {
                m.record_panic();
            }
        });
    }
}

/// Logs a `tokio::task::JoinError` from a `spawn_blocking` or background worker task,
/// records `panics_total` when the task panicked, and returns `true` iff it was a panic.
pub(crate) fn handle_join_error(
    context: &'static str,
    join_err: tokio::task::JoinError,
    explicit_metrics: Option<&ServerMetrics>,
) -> bool {
    if join_err.is_panic() {
        let payload = join_err.into_panic();
        let summary = extract_panic_summary(&*payload);
        tracing::error!(
            context,
            panic_summary = %summary,
            "Caught panic in background worker task"
        );
        record_panic_with_fallback(explicit_metrics);
        true
    } else {
        tracing::warn!(
            context,
            error = %join_err,
            "Background worker task was cancelled"
        );
        false
    }
}

/// Spawns a streaming blob producer task that inherits the caller's [`RPC_DEADLINE`] and
/// [`ServerMetrics`], enforces the active deadline mid-stream, and catches panics so a
/// failing producer sends a terminal error on `tx` instead of silently truncating with `OK`.
pub(crate) fn spawn_blob_stream_producer<F, Fut>(
    context: &'static str,
    tx: tokio::sync::mpsc::Sender<StoreResult<Bytes>>,
    producer: F,
) where
    F: FnOnce(tokio::sync::mpsc::Sender<StoreResult<Bytes>>) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let deadline = current_rpc_deadline();
    let metrics = current_rpc_metrics();
    tokio::spawn(async move {
        let tx_for_timeout = tx.clone();
        let tx_for_panic = tx.clone();
        let metrics_for_panic = metrics.clone();
        let inner = with_rpc_context(deadline, metrics, async move {
            let fut = producer(tx);
            match deadline {
                Some(d) => {
                    if tokio::time::timeout_at(tokio::time::Instant::from_std(d), fut)
                        .await
                        .is_err()
                    {
                        let _ = tx_for_timeout
                            .send(Err(StoreError::DeadlineExceeded(format!(
                                "gRPC deadline exceeded during {context}"
                            ))))
                            .await;
                    }
                }
                None => fut.await,
            }
        });
        if let Err(payload) = AssertUnwindSafe(inner).catch_unwind().await {
            let summary = extract_panic_summary(&*payload);
            tracing::error!(
                context,
                panic_summary = %summary,
                "Caught panic in blob stream producer task"
            );
            if let Some(m) = metrics_for_panic {
                m.record_panic();
            }
            let _ = tx_for_panic
                .send(Err(StoreError::Backend(format!("{context} panicked"))))
                .await;
        }
    });
}

/// Executes a storage future bounded by [`remaining_rpc_deadline`], failing fast with
/// [`StoreError::DeadlineExceeded`] when the deadline has already expired or elapses
/// before `fut` resolves.
pub(crate) async fn run_with_rpc_deadline<T, Fut>(op_name: &'static str, fut: Fut) -> StoreResult<T>
where
    Fut: Future<Output = StoreResult<T>>,
{
    match remaining_rpc_deadline() {
        Some(rem) if rem.is_zero() => Err(StoreError::DeadlineExceeded(format!(
            "gRPC deadline expired before {op_name}"
        ))),
        Some(rem) => match tokio::time::timeout(rem, fut).await {
            Ok(res) => res,
            Err(_) => Err(StoreError::DeadlineExceeded(format!(
                "gRPC deadline exceeded during {op_name}"
            ))),
        },
        None => fut.await,
    }
}

/// Sanitizes an error message for gRPC `Status` transmission: replaces ASCII control
/// characters (`0x00..=0x1f`, `0x7f`) with single spaces `' '` and bounds its
/// percent-encoded wire length to at most [`MAX_GRPC_ERROR_MESSAGE_BYTES`] bytes on a valid
/// UTF-8 char boundary.
pub fn sanitize_grpc_error_message(msg: &str) -> String {
    let mut total_encoded_len = 0usize;
    let mut exceeds_limit = false;
    for ch in msg.chars() {
        let cleaned_ch = if ch.is_ascii_control() { ' ' } else { ch };
        total_encoded_len = total_encoded_len.saturating_add(encoded_grpc_char_len(cleaned_ch));
        if total_encoded_len > MAX_GRPC_ERROR_MESSAGE_BYTES {
            exceeds_limit = true;
            break;
        }
    }

    let budget = if exceeds_limit {
        MAX_GRPC_ERROR_MESSAGE_BYTES.saturating_sub(TRUNCATION_SUFFIX.len())
    } else {
        MAX_GRPC_ERROR_MESSAGE_BYTES
    };
    let mut cleaned = String::with_capacity(msg.len().min(MAX_GRPC_ERROR_MESSAGE_BYTES));
    let mut encoded_len = 0usize;
    for ch in msg.chars() {
        let cleaned_ch = if ch.is_ascii_control() { ' ' } else { ch };
        let ch_encoded_len = encoded_grpc_char_len(cleaned_ch);
        if encoded_len + ch_encoded_len > budget {
            break;
        }
        cleaned.push(cleaned_ch);
        encoded_len += ch_encoded_len;
    }

    if exceeds_limit && MAX_GRPC_ERROR_MESSAGE_BYTES >= TRUNCATION_SUFFIX.len() {
        cleaned.push_str(TRUNCATION_SUFFIX);
    }
    cleaned
}

/// Byte-slice counterpart to [`sanitize_grpc_error_message`] that converts invalid UTF-8
/// via lossy replacement before stripping control characters and bounding wire length.
pub fn sanitize_grpc_error_bytes(raw: &[u8]) -> String {
    let decoded = String::from_utf8_lossy(raw);
    sanitize_grpc_error_message(&decoded)
}

fn sanitize_bounded_utf8(msg: &str, max_bytes: usize) -> String {
    let mut cleaned = String::with_capacity(msg.len().min(max_bytes));
    let mut truncated = false;
    let budget = max_bytes.saturating_sub(TRUNCATION_SUFFIX.len());

    // Fast path: when the whole message fits in `max_bytes`, emit all characters directly.
    if msg.len() <= max_bytes {
        for ch in msg.chars() {
            if ch.is_ascii_control() {
                cleaned.push(' ');
            } else {
                cleaned.push(ch);
            }
        }
        return cleaned;
    }

    for ch in msg.chars() {
        let out_ch = if ch.is_ascii_control() { ' ' } else { ch };
        let next_len = cleaned.len() + out_ch.len_utf8();
        if next_len > budget {
            truncated = true;
            break;
        }
        cleaned.push(out_ch);
    }

    if truncated && max_bytes >= TRUNCATION_SUFFIX.len() {
        cleaned.push_str(TRUNCATION_SUFFIX);
    }
    cleaned
}

/// Parses a gRPC `grpc-timeout` header value per the gRPC-over-HTTP/2 specification:
/// `<1..=8 ASCII digits><H|M|S|m|u|n>`. Returns `None` for empty or malformed values.
pub fn parse_grpc_timeout(raw: &str) -> Option<Duration> {
    let bytes = raw.as_bytes();
    if bytes.len() < 2 || bytes.len() > 9 {
        return None;
    }
    let (&unit, digit_bytes) = bytes.split_last()?;
    if digit_bytes.is_empty()
        || digit_bytes.len() > 8
        || !digit_bytes.iter().all(u8::is_ascii_digit)
    {
        return None;
    }
    let mut value = 0u64;
    for &b in digit_bytes {
        value = value * 10 + u64::from(b - b'0');
    }
    match unit {
        b'H' => value.checked_mul(3600).map(Duration::from_secs),
        b'M' => value.checked_mul(60).map(Duration::from_secs),
        b'S' => Some(Duration::from_secs(value)),
        b'm' => Some(Duration::from_millis(value)),
        b'u' => Some(Duration::from_micros(value)),
        b'n' => Some(Duration::from_nanos(value)),
        _ => None,
    }
}

fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn percent_decode_grpc_message(raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(raw.len());
    let mut i = 0;
    while i < raw.len() {
        if raw[i] == b'%'
            && i + 2 < raw.len()
            && let (Some(hi), Some(lo)) = (hex_nibble(raw[i + 1]), hex_nibble(raw[i + 2]))
        {
            out.push((hi << 4) | lo);
            i += 3;
            continue;
        }
        out.push(raw[i]);
        i += 1;
    }
    out
}

fn push_percent_encoded_byte(out: &mut String, byte: u8) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    out.push('%');
    out.push(HEX[(byte >> 4) as usize] as char);
    out.push(HEX[(byte & 0x0f) as usize] as char);
}

fn is_unencoded_grpc_header_byte(b: u8) -> bool {
    // Percent-encode `' '` (`0x20`) alongside `'%'` and non-ASCII bytes so HTTP/2 proxies
    // cannot trim leading/trailing OWS after control characters are scrubbed to spaces.
    (0x21..=0x7e).contains(&b) && b != b'%'
}

fn encoded_grpc_char_len(ch: char) -> usize {
    if ch.is_ascii() && is_unencoded_grpc_header_byte(ch as u8) {
        1
    } else {
        ch.len_utf8() * 3
    }
}

/// Sanitizes decoded `grpc-message` bytes: guarantees valid UTF-8 with ASCII control
/// characters replaced by `' '` and percent-encodes per gRPC-over-HTTP/2 (encoding spaces
/// as `%20`) within [`MAX_GRPC_ERROR_MESSAGE_BYTES`].
fn encode_sanitized_grpc_message_bytes(decoded_bytes: &[u8]) -> String {
    let decoded = String::from_utf8_lossy(decoded_bytes);

    let mut total_encoded_len = 0usize;
    let mut exceeds_limit = false;
    for ch in decoded.chars() {
        let cleaned_ch = if ch.is_ascii_control() { ' ' } else { ch };
        total_encoded_len = total_encoded_len.saturating_add(encoded_grpc_char_len(cleaned_ch));
        if total_encoded_len > MAX_GRPC_ERROR_MESSAGE_BYTES {
            exceeds_limit = true;
            break;
        }
    }

    let budget = if exceeds_limit {
        MAX_GRPC_ERROR_MESSAGE_BYTES.saturating_sub(TRUNCATION_SUFFIX.len())
    } else {
        MAX_GRPC_ERROR_MESSAGE_BYTES
    };
    let mut encoded = String::with_capacity(total_encoded_len.min(MAX_GRPC_ERROR_MESSAGE_BYTES));
    let mut buf = [0u8; 4];

    for ch in decoded.chars() {
        let cleaned_ch = if ch.is_ascii_control() { ' ' } else { ch };
        let ch_len = encoded_grpc_char_len(cleaned_ch);
        if encoded.len() + ch_len > budget {
            break;
        }
        if ch_len == 1 {
            encoded.push(cleaned_ch);
        } else {
            for &b in cleaned_ch.encode_utf8(&mut buf).as_bytes() {
                push_percent_encoded_byte(&mut encoded, b);
            }
        }
    }

    if exceeds_limit {
        encoded.push_str(TRUNCATION_SUFFIX);
    }
    encoded
}

fn is_already_clean_grpc_message(raw: &[u8]) -> bool {
    raw.len() <= MAX_GRPC_ERROR_MESSAGE_BYTES
        && raw.iter().all(|&b| is_unencoded_grpc_header_byte(b))
}

fn sanitize_response_grpc_message(headers: &mut HeaderMap) {
    if !headers.contains_key(GRPC_MESSAGE_HEADER) {
        return;
    }

    let values: Vec<Vec<u8>> = headers
        .get_all(GRPC_MESSAGE_HEADER)
        .iter()
        .map(|v| v.as_bytes().to_vec())
        .collect();
    if values.len() == 1 && is_already_clean_grpc_message(&values[0]) {
        return;
    }

    // Collapse multiple `grpc-message` headers into a single bounded value so aggregate wire
    // length across duplicate headers can never exceed `MAX_GRPC_ERROR_MESSAGE_BYTES`.
    let mut combined_decoded = Vec::new();
    for (idx, raw) in values.into_iter().enumerate() {
        if idx > 0 {
            combined_decoded.extend_from_slice(b"; ");
        }
        combined_decoded.extend_from_slice(&percent_decode_grpc_message(&raw));
    }

    headers.remove(GRPC_MESSAGE_HEADER);
    let sanitized = encode_sanitized_grpc_message_bytes(&combined_decoded);
    if let Ok(header_val) = HeaderValue::from_str(&sanitized) {
        headers.insert(GRPC_MESSAGE_HEADER, header_val);
    }
}

pub(crate) fn extract_panic_summary(payload: &(dyn Any + Send)) -> String {
    if let Some(msg) = payload.downcast_ref::<&str>() {
        sanitize_bounded_utf8(msg, MAX_PANIC_SUMMARY_BYTES)
    } else if let Some(msg) = payload.downcast_ref::<String>() {
        sanitize_bounded_utf8(msg, MAX_PANIC_SUMMARY_BYTES)
    } else {
        "non-string panic payload".to_string()
    }
}

fn internal_panic_response() -> http::Response<tonic::body::BoxBody> {
    tonic::Status::internal("internal server error").into_http()
}

fn deadline_exceeded_response() -> http::Response<tonic::body::BoxBody> {
    tonic::Status::deadline_exceeded("request deadline exceeded").into_http()
}

fn internal_panic_trailers() -> Frame<Bytes> {
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", HeaderValue::from_static("13"));
    trailers.insert(
        GRPC_MESSAGE_HEADER,
        HeaderValue::from_static("internal%20server%20error"),
    );
    Frame::trailers(trailers)
}

fn deadline_exceeded_trailers() -> Frame<Bytes> {
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", HeaderValue::from_static("4"));
    trailers.insert(
        GRPC_MESSAGE_HEADER,
        HeaderValue::from_static("request%20deadline%20exceeded"),
    );
    Frame::trailers(trailers)
}

enum ParsedDeadline {
    None,
    Active(Instant),
    Expired,
}

fn parse_request_deadline(headers: &HeaderMap, safe_path: &str) -> ParsedDeadline {
    let mut min_timeout: Option<Duration> = None;
    for raw in headers.get_all(GRPC_TIMEOUT_HEADER) {
        match raw.to_str().ok().and_then(parse_grpc_timeout) {
            Some(dur) => {
                min_timeout = Some(match min_timeout {
                    Some(prev) => prev.min(dur),
                    None => dur,
                });
            }
            None => {
                tracing::debug!(
                    grpc_path = %safe_path,
                    "Ignoring malformed grpc-timeout header"
                );
            }
        }
    }

    match min_timeout {
        None => ParsedDeadline::None,
        Some(dur) if dur.is_zero() => ParsedDeadline::Expired,
        Some(dur) => match Instant::now().checked_add(dur) {
            Some(deadline) => ParsedDeadline::Active(deadline),
            // Saturating very large timeouts (for example `99999999H` on platforms with
            // narrower `Instant` ranges) to no finite deadline avoids panicking on overflow.
            None => ParsedDeadline::None,
        },
    }
}

/// Wraps an outgoing `tonic::body::BoxBody` so server-streaming responses (such as
/// `ReadFile`) execute under [`RPC_DEADLINE`] and [`RPC_METRICS`], enforce mid-stream
/// deadline expiry with a clean `DEADLINE_EXCEEDED` (`grpc-status: 4`) trailer, unwind
/// body-poll and metadata panics into `INTERNAL` (`grpc-status: 13`), and sanitize
/// `grpc-message` trailers.
struct ResilienceBody {
    inner: Option<tonic::body::BoxBody>,
    metrics: ServerMetrics,
    safe_path: String,
    deadline: Option<Instant>,
    sleep: Option<Pin<Box<tokio::time::Sleep>>>,
    terminated: bool,
    body_panicked: AtomicBool,
}

impl ResilienceBody {
    fn drop_inner_safely(&mut self) {
        if let Some(inner) = self.inner.take() {
            let deadline = self.deadline;
            let metrics = self.metrics.clone();
            if let Err(payload) = std::panic::catch_unwind(AssertUnwindSafe(move || {
                RPC_DEADLINE.sync_scope(deadline, || {
                    RPC_METRICS.sync_scope(Some(metrics), || drop(inner));
                });
            })) {
                let summary = extract_panic_summary(&*payload);
                tracing::error!(
                    grpc_path = %self.safe_path,
                    panic_summary = %summary,
                    "Caught panic while dropping gRPC response body"
                );
                if !self.body_panicked.swap(true, Ordering::AcqRel) {
                    self.metrics.record_panic();
                }
            }
        }
    }

    fn terminate(&mut self) {
        self.terminated = true;
        self.sleep = None;
        self.drop_inner_safely();
    }
}

impl Drop for ResilienceBody {
    fn drop(&mut self) {
        self.sleep = None;
        self.drop_inner_safely();
    }
}

impl Body for ResilienceBody {
    type Data = Bytes;
    type Error = tonic::Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        if this.terminated {
            return Poll::Ready(None);
        }
        if this.body_panicked.load(Ordering::Acquire) {
            this.terminate();
            return Poll::Ready(Some(Ok(internal_panic_trailers())));
        }
        if let Some(d) = this.deadline
            && this.sleep.is_some()
            && Instant::now() >= d
        {
            this.terminate();
            return Poll::Ready(Some(Ok(deadline_exceeded_trailers())));
        }

        let Some(inner) = this.inner.as_mut() else {
            this.terminate();
            return Poll::Ready(None);
        };
        let poll_res = std::panic::catch_unwind(AssertUnwindSafe(|| {
            RPC_DEADLINE.sync_scope(this.deadline, || {
                RPC_METRICS.sync_scope(Some(this.metrics.clone()), || {
                    Pin::new(inner).poll_frame(cx)
                })
            })
        }));

        match poll_res {
            Ok(Poll::Pending) => {
                if let Some(sleep) = this.sleep.as_mut()
                    && sleep.as_mut().poll(cx).is_ready()
                {
                    this.terminate();
                    return Poll::Ready(Some(Ok(deadline_exceeded_trailers())));
                }
                Poll::Pending
            }
            Ok(Poll::Ready(None)) => {
                this.terminate();
                Poll::Ready(None)
            }
            Ok(Poll::Ready(Some(Ok(mut frame)))) => {
                if let Some(trailers) = frame.trailers_mut() {
                    sanitize_response_grpc_message(trailers);
                    this.terminate();
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Ok(Poll::Ready(Some(Err(status)))) => {
                this.terminate();
                let sanitized = sanitize_grpc_error_message(status.message());
                Poll::Ready(Some(Err(tonic::Status::with_details_and_metadata(
                    status.code(),
                    sanitized,
                    Bytes::copy_from_slice(status.details()),
                    status.metadata().clone(),
                ))))
            }
            Err(payload) => {
                let summary = extract_panic_summary(&*payload);
                tracing::error!(
                    grpc_path = %this.safe_path,
                    panic_summary = %summary,
                    "Caught panic while streaming gRPC response body"
                );
                if !this.body_panicked.swap(true, Ordering::AcqRel) {
                    this.metrics.record_panic();
                }
                this.terminate();
                Poll::Ready(Some(Ok(internal_panic_trailers())))
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        if self.terminated {
            return true;
        }
        if self.body_panicked.load(Ordering::Acquire) {
            return false;
        }
        let Some(inner) = self.inner.as_ref() else {
            return true;
        };
        match std::panic::catch_unwind(AssertUnwindSafe(|| {
            RPC_DEADLINE.sync_scope(self.deadline, || {
                RPC_METRICS.sync_scope(Some(self.metrics.clone()), || inner.is_end_stream())
            })
        })) {
            Ok(end) => end,
            Err(payload) => {
                let summary = extract_panic_summary(&*payload);
                tracing::error!(
                    grpc_path = %self.safe_path,
                    panic_summary = %summary,
                    "Caught panic in gRPC response body is_end_stream"
                );
                if !self.body_panicked.swap(true, Ordering::AcqRel) {
                    self.metrics.record_panic();
                }
                false
            }
        }
    }

    fn size_hint(&self) -> SizeHint {
        if self.terminated || self.body_panicked.load(Ordering::Acquire) {
            return SizeHint::with_exact(0);
        }
        let Some(inner) = self.inner.as_ref() else {
            return SizeHint::with_exact(0);
        };
        match std::panic::catch_unwind(AssertUnwindSafe(|| {
            RPC_DEADLINE.sync_scope(self.deadline, || {
                RPC_METRICS.sync_scope(Some(self.metrics.clone()), || inner.size_hint())
            })
        })) {
            Ok(hint) => hint,
            Err(payload) => {
                let summary = extract_panic_summary(&*payload);
                tracing::error!(
                    grpc_path = %self.safe_path,
                    panic_summary = %summary,
                    "Caught panic in gRPC response body size_hint"
                );
                if !self.body_panicked.swap(true, Ordering::AcqRel) {
                    self.metrics.record_panic();
                }
                SizeHint::default()
            }
        }
    }
}

/// Tower layer installed outermost on `jj-cc-server` (outside [`MetricsLayer`] and
/// [`ServerAuthLayer`]) to unwind handler/middleware/body panics into gRPC `INTERNAL`
/// responses, sanitize outgoing `grpc-message` headers and trailers, and propagate
/// `grpc-timeout` deadlines into downstream storage transactions.
#[derive(Clone)]
pub struct ResilienceLayer {
    metrics: ServerMetrics,
}

impl ResilienceLayer {
    pub fn new(metrics: ServerMetrics) -> Self {
        Self { metrics }
    }
}

impl<S> Layer<S> for ResilienceLayer {
    type Service = ResilienceService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ResilienceService {
            inner,
            metrics: self.metrics.clone(),
            poll_ready_panicked: false,
        }
    }
}

/// `tower::Service` produced by [`ResilienceLayer`].
pub struct ResilienceService<S> {
    inner: S,
    metrics: ServerMetrics,
    poll_ready_panicked: bool,
}

impl<S: Clone> Clone for ResilienceService<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            metrics: self.metrics.clone(),
            poll_ready_panicked: false,
        }
    }
}

impl<S, ReqBody> Service<http::Request<ReqBody>> for ResilienceService<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<tonic::body::BoxBody>>,
    S::Future: Send + 'static,
{
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.poll_ready_panicked {
            return Poll::Ready(Ok(()));
        }
        match std::panic::catch_unwind(AssertUnwindSafe(|| self.inner.poll_ready(cx))) {
            Ok(poll) => poll,
            Err(payload) => {
                let summary = extract_panic_summary(&*payload);
                tracing::error!(
                    panic_summary = %summary,
                    "Caught panic in gRPC service poll_ready"
                );
                self.metrics.record_panic();
                // Defer emitting gRPC `INTERNAL` until `call()` because `S::Error` is
                // `Infallible` on Tonic routers and cannot carry an error here.
                self.poll_ready_panicked = true;
                Poll::Ready(Ok(()))
            }
        }
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        if self.poll_ready_panicked {
            self.poll_ready_panicked = false;
            return Box::pin(async { Ok(internal_panic_response()) });
        }

        let safe_path = sanitize_bounded_utf8(req.uri().path(), MAX_LOG_PATH_BYTES);
        let deadline = match parse_request_deadline(req.headers(), &safe_path) {
            ParsedDeadline::None => None,
            ParsedDeadline::Active(d) => Some(d),
            ParsedDeadline::Expired => {
                return Box::pin(async { Ok(deadline_exceeded_response()) });
            }
        };

        let metrics = self.metrics.clone();
        let inner_fut = match std::panic::catch_unwind(AssertUnwindSafe(|| {
            RPC_DEADLINE.sync_scope(deadline, || {
                RPC_METRICS.sync_scope(Some(metrics.clone()), || self.inner.call(req))
            })
        })) {
            Ok(fut) => fut,
            Err(payload) => {
                let summary = extract_panic_summary(&*payload);
                tracing::error!(
                    grpc_path = %safe_path,
                    panic_summary = %summary,
                    "Caught synchronous panic in gRPC request handler"
                );
                self.metrics.record_panic();
                return Box::pin(async { Ok(internal_panic_response()) });
            }
        };

        Box::pin(async move {
            let scoped = with_rpc_context(deadline, Some(metrics.clone()), async move {
                match deadline {
                    Some(d) => {
                        match tokio::time::timeout_at(tokio::time::Instant::from_std(d), inner_fut)
                            .await
                        {
                            Ok(res) => Ok(res),
                            Err(_) => Err(()),
                        }
                    }
                    None => Ok(inner_fut.await),
                }
            });
            match AssertUnwindSafe(scoped).catch_unwind().await {
                Ok(Ok(Ok(mut resp))) => {
                    sanitize_response_grpc_message(resp.headers_mut());
                    let is_trailers_only = resp.headers().contains_key("grpc-status");
                    let sleep = if is_trailers_only {
                        None
                    } else {
                        deadline.map(|d| {
                            Box::pin(tokio::time::sleep_until(tokio::time::Instant::from_std(d)))
                        })
                    };
                    let resp = resp.map(|inner| {
                        tonic::body::BoxBody::new(ResilienceBody {
                            inner: Some(inner),
                            metrics,
                            safe_path,
                            deadline,
                            sleep,
                            terminated: false,
                            body_panicked: AtomicBool::new(false),
                        })
                    });
                    Ok(resp)
                }
                Ok(Ok(Err(err))) => Err(err),
                Ok(Err(())) => Ok(deadline_exceeded_response()),
                Err(payload) => {
                    let summary = extract_panic_summary(&*payload);
                    tracing::error!(
                        grpc_path = %safe_path,
                        panic_summary = %summary,
                        "Caught asynchronous panic in gRPC request handler"
                    );
                    metrics.record_panic();
                    Ok(internal_panic_response())
                }
            }
        })
    }
}

#[cfg(test)]
mod tests;
