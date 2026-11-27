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

//! Pure-Rust AWS SigV4 S3-compatible `BlobStore` implementation (`S3BlobStore`).
//!
//! # Authority and Crash-Safety Ordering
//! `CompositeStore` uploads immutable content-addressed blobs via `S3BlobStore::put_blob`
//! before publishing `BlobRecord` rows in `MetadataStore`. Objects use sharded keys
//! (`blobs/<aa>/<bb>/<full_hex>`) and `Cache-Control: private, max-age=31536000, immutable`
//! so private caches avoid origin revalidation without permitting shared proxies to cache
//! authenticated source blobs across callers.
//!
//! # Concurrency and Merge Invariants
//! `put_blob` sets `If-None-Match: *` conditional create and treats HTTP `412` as idempotent
//! completion (and verifies existence or retries on concurrent `409 ConditionalRequestConflict`)
//! so simultaneous uploads of identical content-addressed digests never overwrite existing
//! objects.
//!
//! # Cross-Process and RPC Boundaries
//! Uses `reqwest` over `rustls` with redirects disabled (`Policy::none()`) so signed
//! `Authorization` and `x-amz-security-token` headers never leak across HTTP redirects.
//! Idempotent requests perform bounded exponential-backoff retries with fresh SigV4 timestamps
//! on transient transport and HTTP errors, cap error bodies at 64 KiB, redact secrets from
//! `Debug`/errors, and map HTTP status codes deterministically into [`StoreError`].

use std::fmt;
use std::sync::{Arc, Once};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Body, Client, Method, Response, StatusCode};

use super::{BlobStore, StoreError, StoreResult};

mod config;
mod ops;
mod sigv4;
#[cfg(test)]
mod tests;

pub use config::{
    S3ConnectConfig, normalize_s3_bucket, parse_s3_peer_spec, redact_s3_peer_spec,
    validate_s3_endpoint,
};
use config::{S3Credentials, resolve_s3_connect_config};
use sigv4::{format_amz_timestamps, sha256_hex, sign_v4_encoded_path_with_hash, uri_encode};

pub const DEFAULT_S3_REGION: &str = "us-east-1";

const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;

const MAX_ERROR_SNIPPET_CHARS: usize = 512;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

const READ_TIMEOUT: Duration = Duration::from_secs(120);

const MAX_RETRY_ATTEMPTS: usize = 3;

const RETRY_BASE_DELAY_MS: u64 = 25;

static RUSTLS_PROVIDER_INIT: Once = Once::new();

fn ensure_rustls_crypto_provider() {
    RUSTLS_PROVIDER_INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

struct S3Inner {
    client: Client,
    scheme: &'static str,
    request_host: String,
    base_path: String,
    bucket: String,
    region: String,
    path_style: bool,
    location_uri: String,
    credentials: Option<S3Credentials>,
}

impl fmt::Debug for S3Inner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3BlobStore")
            .field("scheme", &self.scheme)
            .field("request_host", &self.request_host)
            .field("base_path", &self.base_path)
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("path_style", &self.path_style)
            .field("location_uri", &self.location_uri)
            .field("credentials", &self.credentials)
            .finish()
    }
}

#[derive(Clone)]
pub struct S3BlobStore {
    inner: Arc<S3Inner>,
}

impl fmt::Debug for S3BlobStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl S3BlobStore {
    pub async fn connect(config: &S3ConnectConfig) -> StoreResult<Self> {
        Self::connect_with_timeouts(config, CONNECT_TIMEOUT, READ_TIMEOUT).await
    }

    async fn connect_with_timeouts(
        config: &S3ConnectConfig,
        connect_timeout: Duration,
        read_timeout: Duration,
    ) -> StoreResult<Self> {
        let resolved = resolve_s3_connect_config(config)?;
        ensure_rustls_crypto_provider();

        // Use per-read idle timeout instead of a flat total request timeout so large, slow
        // streaming transfers complete as long as bytes keep flowing.
        let mut builder = Client::builder()
            .connect_timeout(connect_timeout)
            .read_timeout(read_timeout)
            .redirect(reqwest::redirect::Policy::none());
        if resolved.is_loopback {
            builder = builder.no_proxy();
        }
        let client = builder.build().map_err(|e| {
            StoreError::Unavailable(format!(
                "failed to initialize S3 HTTP client for {}: {e}",
                resolved.location_uri
            ))
        })?;

        let store = Self {
            inner: Arc::new(S3Inner {
                client,
                scheme: resolved.scheme,
                request_host: resolved.request_host,
                base_path: resolved.base_path,
                bucket: resolved.bucket,
                region: resolved.region,
                path_style: resolved.path_style,
                location_uri: resolved.location_uri,
                credentials: resolved.credentials,
            }),
        };

        if resolved.auto_create_bucket {
            store.ensure_bucket_exists().await?;
        }

        Ok(store)
    }

    pub fn object_key(blob_id: &[u8]) -> StoreResult<String> {
        if blob_id.is_empty() || blob_id.len() > 64 {
            return Err(StoreError::InvalidArgument(
                "blob_id must be between 1 and 64 bytes".to_string(),
            ));
        }
        let hex_id = hex::encode(blob_id);
        let prefix_a = hex_id.get(0..2).unwrap_or("00");
        let prefix_b = hex_id.get(2..4).unwrap_or("00");
        Ok(format!("blobs/{prefix_a}/{prefix_b}/{hex_id}"))
    }

    fn bucket_path(&self) -> String {
        if self.inner.path_style {
            format!("{}/{}", self.inner.base_path, self.inner.bucket)
        } else if self.inner.base_path.is_empty() {
            "/".to_string()
        } else {
            format!("{}/", self.inner.base_path)
        }
    }

    fn object_path(&self, key: &str) -> String {
        let encoded_key = uri_encode(key, false);
        if self.inner.path_style {
            format!(
                "{}/{}/{}",
                self.inner.base_path, self.inner.bucket, encoded_key
            )
        } else {
            format!("{}/{}", self.inner.base_path, encoded_key)
        }
    }

    fn url_for_encoded_path(&self, encoded_path: &str) -> String {
        format!(
            "{}://{}{}",
            self.inner.scheme, self.inner.request_host, encoded_path
        )
    }

    async fn send_request(
        &self,
        op: &str,
        method: Method,
        encoded_path: &str,
        extra_headers: &[(&str, &str)],
        body: Option<Bytes>,
    ) -> StoreResult<Response> {
        let payload_hash = sha256_hex(body.as_deref().unwrap_or(&[]));
        let mut attempt = 0;
        loop {
            let retry_allowed = attempt + 1 < MAX_RETRY_ATTEMPTS;
            if let Some(resp) = self
                .send_request_attempt(
                    op,
                    method.clone(),
                    encoded_path,
                    extra_headers,
                    &payload_hash,
                    body.clone().map(Body::from),
                    retry_allowed,
                )
                .await?
            {
                return Ok(resp);
            }
            sleep_retry_backoff(attempt).await;
            attempt += 1;
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_request_attempt(
        &self,
        op: &str,
        method: Method,
        encoded_path: &str,
        extra_headers: &[(&str, &str)],
        payload_hash: &str,
        body: Option<Body>,
        retry_allowed: bool,
    ) -> StoreResult<Option<Response>> {
        match self
            .send_request_once(method, encoded_path, extra_headers, payload_hash, body)
            .await?
        {
            Ok(resp) => {
                let status = resp.status();
                if is_retryable_http_status(status) {
                    if retry_allowed {
                        drop(resp);
                        return Ok(None);
                    }
                    let err_body = read_bounded_body(resp, MAX_ERROR_BODY_BYTES).await;
                    return Err(map_http_status_err(status, op, &err_body));
                }
                if status == StatusCode::BAD_REQUEST {
                    let err_body = read_bounded_body(resp, MAX_ERROR_BODY_BYTES).await;
                    if is_s3_request_timeout(&err_body) && retry_allowed {
                        return Ok(None);
                    }
                    return Err(map_http_status_err(status, op, &err_body));
                }
                Ok(Some(resp))
            }
            Err(err) if is_retryable_reqwest_err(&err) && retry_allowed => Ok(None),
            Err(err) => Err(map_reqwest_err(err)),
        }
    }

    async fn send_request_once(
        &self,
        method: Method,
        encoded_path: &str,
        extra_headers: &[(&str, &str)],
        payload_hash: &str,
        body: Option<Body>,
    ) -> StoreResult<Result<Response, reqwest::Error>> {
        let url = self.url_for_encoded_path(encoded_path);
        let mut req = self.inner.client.request(method.clone(), &url);
        let mut headers = HeaderMap::new();

        for (name, value) in extra_headers {
            let hname = HeaderName::from_bytes(name.as_bytes()).map_err(|e| {
                StoreError::InvalidArgument(format!("invalid HTTP header name '{name}': {e}"))
            })?;
            let hval = HeaderValue::from_str(value).map_err(|e| {
                StoreError::InvalidArgument(format!("invalid HTTP header value for '{name}': {e}"))
            })?;
            headers.append(hname, hval);
        }

        if let Some(creds) = &self.inner.credentials {
            let now = SystemTime::now();
            let (date_stamp, amz_date) = format_amz_timestamps(now)?;

            let mut headers_to_sign: Vec<(&str, &str)> =
                Vec::with_capacity(extra_headers.len() + 4);
            headers_to_sign.push(("host", &self.inner.request_host));
            headers_to_sign.extend_from_slice(extra_headers);

            let sig = sign_v4_encoded_path_with_hash(
                method.as_str(),
                encoded_path,
                &[],
                &headers_to_sign,
                payload_hash,
                &date_stamp,
                &amz_date,
                &self.inner.region,
                creds,
            );

            headers.insert(
                HeaderName::from_static("x-amz-date"),
                HeaderValue::from_str(&amz_date)
                    .map_err(|e| StoreError::Backend(format!("invalid x-amz-date header: {e}")))?,
            );
            headers.insert(
                HeaderName::from_static("x-amz-content-sha256"),
                HeaderValue::from_str(&sig.payload_hash).map_err(|e| {
                    StoreError::Backend(format!("invalid x-amz-content-sha256 header: {e}"))
                })?,
            );
            if let Some(token) = &creds.session_token {
                headers.insert(
                    HeaderName::from_static("x-amz-security-token"),
                    HeaderValue::from_str(token).map_err(|_| {
                        StoreError::InvalidArgument(
                            "invalid S3 session_token header value".to_string(),
                        )
                    })?,
                );
            }
            headers.insert(
                HeaderName::from_static("authorization"),
                HeaderValue::from_str(&sig.authorization).map_err(|e| {
                    StoreError::Backend(format!("invalid authorization header: {e}"))
                })?,
            );
        }

        req = req.headers(headers);
        if let Some(body) = body {
            req = req.body(body);
        }

        Ok(req.send().await)
    }

    async fn send_put_blob_with_retry<F, Fut>(
        &self,
        blob_id: &[u8],
        encoded_path: &str,
        extra_headers: &[(&str, &str)],
        payload_hash: &str,
        mut make_body: F,
    ) -> StoreResult<()>
    where
        F: FnMut(usize) -> Fut,
        Fut: std::future::Future<Output = StoreResult<Option<Body>>>,
    {
        let mut attempt = 0;
        loop {
            let retry_allowed = attempt + 1 < MAX_RETRY_ATTEMPTS;
            let body = make_body(attempt).await?;
            let Some(resp) = self
                .send_request_attempt(
                    "PUT object",
                    Method::PUT,
                    encoded_path,
                    extra_headers,
                    payload_hash,
                    body,
                    retry_allowed,
                )
                .await?
            else {
                sleep_retry_backoff(attempt).await;
                attempt += 1;
                continue;
            };

            let status = resp.status();
            let err_body = read_bounded_body(resp, MAX_ERROR_BODY_BYTES).await;
            match status {
                StatusCode::OK | StatusCode::CREATED | StatusCode::NO_CONTENT => return Ok(()),
                // Object already exists with identical content-addressed digest.
                StatusCode::PRECONDITION_FAILED => return Ok(()),
                // AWS S3 returns 409 ConditionalRequestConflict when concurrent conditional creates
                // race on the same key. Verify the winning object is readable or retry until visible.
                StatusCode::CONFLICT
                    if String::from_utf8_lossy(&err_body)
                        .contains("ConditionalRequestConflict") =>
                {
                    if self.has_blob(blob_id).await? {
                        return Ok(());
                    }
                    if retry_allowed {
                        sleep_retry_backoff(attempt).await;
                        attempt += 1;
                        continue;
                    }
                    return Err(StoreError::Unavailable(
                        "S3 PUT object hit ConditionalRequestConflict and object is not yet visible"
                            .to_string(),
                    ));
                }
                _ => return Err(map_http_status_err(status, "PUT object", &err_body)),
            }
        }
    }

    async fn ensure_bucket_exists(&self) -> StoreResult<()> {
        let bucket_path = self.bucket_path();
        let head_resp = self
            .send_request("HEAD bucket", Method::HEAD, &bucket_path, &[], None)
            .await?;
        let head_status = head_resp.status();
        if head_status.is_success() {
            let _ = read_bounded_body(head_resp, MAX_ERROR_BODY_BYTES).await;
            return Ok(());
        }
        if head_status != StatusCode::NOT_FOUND {
            let body = read_bounded_body(head_resp, MAX_ERROR_BODY_BYTES).await;
            return Err(map_http_status_err(head_status, "HEAD bucket", &body));
        }
        let _ = read_bounded_body(head_resp, MAX_ERROR_BODY_BYTES).await;

        let (create_headers, create_body): (Vec<(&str, &str)>, Bytes) = if self.inner.region
            == DEFAULT_S3_REGION
        {
            (Vec::new(), Bytes::new())
        } else {
            let xml = format!(
                "<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><LocationConstraint>{}</LocationConstraint></CreateBucketConfiguration>",
                self.inner.region
            );
            (vec![("content-type", "application/xml")], Bytes::from(xml))
        };

        let put_resp = self
            .send_request(
                "PUT bucket",
                Method::PUT,
                &bucket_path,
                &create_headers,
                Some(create_body),
            )
            .await?;
        let put_status = put_resp.status();
        let put_body = read_bounded_body(put_resp, MAX_ERROR_BODY_BYTES).await;
        if put_status.is_success() {
            return Ok(());
        }
        if put_status == StatusCode::CONFLICT {
            let text = String::from_utf8_lossy(&put_body);
            if text.contains("BucketAlreadyOwnedByYou") {
                return Ok(());
            }
            if text.contains("BucketAlreadyExists") || text.trim().is_empty() {
                // Some S3-compatible servers return BucketAlreadyExists or an empty 409 body on
                // concurrent creation; verify via HEAD that the bucket is accessible to
                // our credentials rather than owned by another account.
                let verify_resp = self
                    .send_request(
                        "HEAD bucket after 409 conflict",
                        Method::HEAD,
                        &bucket_path,
                        &[],
                        None,
                    )
                    .await?;
                let verify_status = verify_resp.status();
                let verify_body = read_bounded_body(verify_resp, MAX_ERROR_BODY_BYTES).await;
                if verify_status.is_success() {
                    return Ok(());
                }
                return Err(map_http_status_err(
                    verify_status,
                    "HEAD bucket after 409 conflict",
                    &verify_body,
                ));
            }
        }
        Err(map_http_status_err(put_status, "PUT bucket", &put_body))
    }

    async fn head_content_length(&self, encoded_path: &str) -> StoreResult<Option<u64>> {
        let resp = self
            .send_request("HEAD object", Method::HEAD, encoded_path, &[], None)
            .await?;
        let status = resp.status();
        if status == StatusCode::NOT_FOUND {
            let _ = read_bounded_body(resp, MAX_ERROR_BODY_BYTES).await;
            return Ok(None);
        }
        if status.is_success() {
            let len = resp
                .headers()
                .get(reqwest::header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.trim().parse::<u64>().ok())
                .ok_or_else(|| {
                    StoreError::Backend(
                        "S3 HEAD response missing valid Content-Length header".to_string(),
                    )
                })?;
            let _ = read_bounded_body(resp, MAX_ERROR_BODY_BYTES).await;
            return Ok(Some(len));
        }
        let body = read_bounded_body(resp, MAX_ERROR_BODY_BYTES).await;
        Err(map_http_status_err(status, "HEAD object", &body))
    }
}

fn parse_content_range_total(headers: &HeaderMap) -> Option<u64> {
    let value = headers.get("content-range")?.to_str().ok()?.trim();
    let (_, total_str) = value.rsplit_once('/')?;
    total_str.trim().parse::<u64>().ok()
}

fn parse_content_range_start(headers: &HeaderMap) -> Option<u64> {
    let value = headers.get("content-range")?.to_str().ok()?.trim();
    let rest = value
        .get(..6)
        .filter(|p| p.eq_ignore_ascii_case("bytes "))
        .map(|_| value[6..].trim_start())?;
    let (start_str, _) = rest.split_once('-')?;
    start_str.trim().parse::<u64>().ok()
}

fn parse_content_length(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(reqwest::header::CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
}

async fn read_bounded_body(mut resp: Response, limit: usize) -> Vec<u8> {
    let mut out = Vec::new();
    while let Ok(Some(chunk)) = resp.chunk().await {
        let remaining = limit.saturating_sub(out.len());
        if remaining == 0 {
            break;
        }
        let take = chunk.len().min(remaining);
        out.extend_from_slice(&chunk[..take]);
        if out.len() >= limit {
            break;
        }
    }
    out
}

fn sanitize_error_snippet(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    let collapsed: String = text
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    let trimmed = collapsed.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if trimmed.chars().count() <= MAX_ERROR_SNIPPET_CHARS {
        trimmed.to_string()
    } else {
        let truncated: String = trimmed.chars().take(MAX_ERROR_SNIPPET_CHARS).collect();
        format!("{truncated}...")
    }
}

fn is_retryable_reqwest_err(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect() || err.is_request()
}

fn is_retryable_body_err(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect() || err.is_body() || err.is_request()
}

fn is_retryable_http_status(status: StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 429 | 500 | 502..=504)
}

fn is_s3_request_timeout(body: &[u8]) -> bool {
    String::from_utf8_lossy(body).contains("<Code>RequestTimeout</Code>")
}

async fn sleep_retry_backoff(attempt: usize) {
    let base_ms = RETRY_BASE_DELAY_MS.saturating_mul(1u64 << attempt.min(6));
    let jitter_ms = rand::random::<u64>() % base_ms.max(1);
    tokio::time::sleep(Duration::from_millis(base_ms + jitter_ms)).await;
}

fn map_reqwest_err(err: reqwest::Error) -> StoreError {
    if err.is_timeout() || err.is_connect() || err.is_body() || err.is_request() {
        StoreError::Unavailable(format!("S3 transport error: {err}"))
    } else {
        StoreError::Backend(format!("S3 HTTP client error: {err}"))
    }
}

fn is_s3_auth_error_code(xml: &str) -> bool {
    [
        "<Code>InvalidAccessKeyId</Code>",
        "<Code>SignatureDoesNotMatch</Code>",
        "<Code>ExpiredToken</Code>",
        "<Code>InvalidToken</Code>",
        "<Code>TokenRefreshRequired</Code>",
    ]
    .iter()
    .any(|code| xml.contains(code))
}

fn map_http_status_err(status: StatusCode, op: &str, body: &[u8]) -> StoreError {
    let raw_text = String::from_utf8_lossy(body);
    let snippet = sanitize_error_snippet(body);
    let detail = if snippet.is_empty() {
        format!("S3 {op} failed with HTTP {status}")
    } else {
        format!("S3 {op} failed with HTTP {status}: {snippet}")
    };

    match status.as_u16() {
        400 => {
            if is_s3_auth_error_code(&raw_text) {
                StoreError::Unauthenticated(detail)
            } else if raw_text.contains("<Code>RequestTimeout</Code>") {
                StoreError::Unavailable(detail)
            } else {
                StoreError::InvalidArgument(detail)
            }
        }
        401 => StoreError::Unauthenticated(detail),
        403 => {
            if is_s3_auth_error_code(&raw_text) {
                StoreError::Unauthenticated(detail)
            } else {
                StoreError::PermissionDenied(detail)
            }
        }
        408 | 500 | 502..=504 => StoreError::Unavailable(detail),
        416 => StoreError::OutOfRange(detail),
        429 => StoreError::ResourceExhausted(detail),
        300..=399 => StoreError::Backend(format!(
            "S3 {op} received unexpected HTTP redirect {status} (check S3 endpoint and region)"
        )),
        _ => StoreError::Backend(detail),
    }
}
