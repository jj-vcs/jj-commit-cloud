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

//! Google Cloud Storage `BlobStore` implementation (`GcsBlobStore`).
//!
//! # Authority and Crash-Safety Ordering
//! `CompositeStore` uploads immutable content-addressed blobs via `GcsBlobStore::put_blob`
//! before publishing `BlobRecord` rows in `MetadataStore`. Objects use sharded keys
//! (`blobs/<aa>/<bb>/<full_hex>`) and `Cache-Control: private, max-age=31536000, immutable`
//! so private caches avoid origin revalidation without permitting shared proxies to cache
//! authenticated source blobs across callers.
//!
//! # Concurrency and Merge Invariants
//! `put_blob` sets `if_generation_match(0)` on every upload and treats HTTP `412`
//! (`Code::FailedPrecondition`) as idempotent completion. When hundreds of concurrent
//! agents upload identical content, duplicate writes short-circuit without hitting
//! Cloud Storage's 1-write-per-second per-object overwrite limit.
//!
//! # Cross-Process and RPC Boundaries
//! Generic over `S: google_cloud_storage::stub::Storage` so production HTTP/gRPC
//! transports and deterministic test stubs exercise identical request construction,
//! byte-range `ReadRange` framing, and status-code translation.

use google_cloud_auth::credentials::anonymous;
use google_cloud_gax::error::rpc::Code as RpcCode;
use google_cloud_storage::client::Storage;
use google_cloud_storage::error::{ReadError, WriteError};
use google_cloud_storage::stub::{DefaultStorage, Storage as StorageStub};

use super::{StoreError, StoreResult};

mod streaming;
#[cfg(test)]
mod tests;

pub const IMMUTABLE_CACHE_CONTROL: &str = "private, max-age=31536000, immutable";
const MAX_EMULATOR_RESPONSE_BYTES: u64 = 64 * 1024;

#[derive(Clone, Debug, Default)]
pub struct GcsConnectConfig {
    /// Bucket identifier (`my-bucket`, `gs://my-bucket`, or `projects/_/buckets/my-bucket`).
    pub bucket: String,
    /// Optional custom Cloud Storage endpoint (e.g. local emulator or VPC-SC endpoint).
    pub endpoint: Option<String>,
}

impl GcsConnectConfig {
    /// Validates bucket naming and endpoint syntax/TLS policy without opening network connections.
    pub fn validate(&self) -> StoreResult<()> {
        let _ = resolve_gcs_connect_config(self)?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct GcsBlobStore<S = DefaultStorage>
where
    S: StorageStub + 'static,
{
    client: Storage<S>,
    bucket_resource: String,
    location_uri: String,
}

impl GcsBlobStore<DefaultStorage> {
    pub async fn connect(config: &GcsConnectConfig) -> StoreResult<Self> {
        let resolved = resolve_gcs_connect_config(config)?;
        let mut builder = Storage::builder();

        if let Some(parsed) = resolved.endpoint {
            if parsed.is_http {
                let bucket_id = resolved
                    .location_uri
                    .strip_prefix("gs://")
                    .unwrap_or(&config.bucket);
                ensure_emulator_bucket_http10(&parsed.connect_addr, &parsed.host_header, bucket_id)
                    .await?;
                builder = builder.with_credentials(anonymous::Builder::new().build());
            }
            builder = builder.with_endpoint(parsed.url);
        }

        let client = builder.build().await.map_err(|e| {
            StoreError::Unavailable(format!(
                "failed to initialize Cloud Storage client for {}: {e}",
                resolved.location_uri
            ))
        })?;

        Ok(Self {
            client,
            bucket_resource: resolved.bucket_resource,
            location_uri: resolved.location_uri,
        })
    }
}

impl<S> GcsBlobStore<S>
where
    S: StorageStub + 'static,
{
    pub fn from_storage(bucket: &str, client: Storage<S>) -> StoreResult<Self> {
        let (bucket_resource, location_uri) = normalize_bucket_identifiers(bucket)?;
        Ok(Self {
            client,
            bucket_resource,
            location_uri,
        })
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
}

pub fn normalize_bucket_identifiers(raw_bucket: &str) -> StoreResult<(String, String)> {
    if raw_bucket.bytes().any(|b| b.is_ascii_control()) {
        return Err(StoreError::InvalidArgument(format!(
            "Cloud Storage bucket identifier {raw_bucket:?} contains control characters"
        )));
    }
    let trimmed = raw_bucket.trim();
    let trimmed = trimmed.strip_suffix('/').unwrap_or(trimmed);
    let bucket_id = if let Some(rest) = trimmed.strip_prefix("gs://") {
        rest
    } else if let Some(rest) = trimmed.strip_prefix("projects/_/buckets/") {
        rest
    } else {
        trimmed
    };

    let bytes = bucket_id.as_bytes();
    let valid_len = (3..=222).contains(&bytes.len());
    let valid_chars = bytes
        .iter()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'-' | b'_' | b'.'));
    let valid_edges = bytes
        .first()
        .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        && bytes
            .last()
            .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit());
    let no_double_dot = !bucket_id.contains("..");

    if !valid_len || !valid_chars || !valid_edges || !no_double_dot {
        return Err(StoreError::InvalidArgument(format!(
            "invalid Cloud Storage bucket identifier '{raw_bucket}'"
        )));
    }

    Ok((
        format!("projects/_/buckets/{bucket_id}"),
        format!("gs://{bucket_id}"),
    ))
}

async fn ensure_emulator_bucket_http10(
    connect_addr: &str,
    host_header: &str,
    bucket_id: &str,
) -> StoreResult<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let payload = serde_json::json!({ "name": bucket_id }).to_string();
    let req = format!(
        "POST /storage/v1/b?project=local-project HTTP/1.0\r\nHost: {host_header}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{payload}",
        payload.len()
    );
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    let raw = loop {
        let attempt = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            let mut stream = tokio::net::TcpStream::connect(connect_addr).await?;
            stream.write_all(req.as_bytes()).await?;
            let mut buf = Vec::new();
            stream
                .take(MAX_EMULATOR_RESPONSE_BYTES)
                .read_to_end(&mut buf)
                .await?;
            if buf.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "empty HTTP response from Cloud Storage emulator",
                ));
            }
            Ok::<Vec<u8>, std::io::Error>(buf)
        })
        .await;
        match attempt {
            Ok(Ok(buf)) => break buf,
            _ if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            }
            Ok(Err(e)) => {
                return Err(StoreError::Unavailable(format!(
                    "failed talking to Cloud Storage emulator {connect_addr}: {e}"
                )));
            }
            Err(_) => {
                return Err(StoreError::Unavailable(format!(
                    "timed out initializing bucket '{bucket_id}' on Cloud Storage emulator {connect_addr}"
                )));
            }
        }
    };

    let status_line = String::from_utf8_lossy(&raw);
    let code = status_line
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|tok| tok.parse::<u16>().ok())
        .unwrap_or(0);
    if (200..300).contains(&code) || code == 409 {
        Ok(())
    } else {
        Err(StoreError::Unavailable(format!(
            "Cloud Storage emulator {connect_addr} rejected bucket '{bucket_id}' creation (HTTP {code})"
        )))
    }
}

fn is_precondition_failed(err: &google_cloud_gax::error::Error) -> bool {
    err.http_status_code().is_some_and(|code| code == 412)
        || err
            .status()
            .is_some_and(|s| s.code == RpcCode::FailedPrecondition)
}

fn is_not_found(err: &google_cloud_gax::error::Error) -> bool {
    err.http_status_code().is_some_and(|code| code == 404)
        || err.status().is_some_and(|s| s.code == RpcCode::NotFound)
}

fn is_out_of_range(err: &google_cloud_gax::error::Error) -> bool {
    err.http_status_code().is_some_and(|code| code == 416)
        || err.status().is_some_and(|s| s.code == RpcCode::OutOfRange)
}

fn parse_content_range_total(err: &google_cloud_gax::error::Error) -> Option<u64> {
    let value = err
        .http_headers()?
        .get("content-range")?
        .to_str()
        .ok()?
        .trim();
    let (_, total_str) = value.rsplit_once('/')?;
    total_str.trim().parse::<u64>().ok()
}

fn is_checksum_mismatch(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = err.source();
    while let Some(source) = current {
        if matches!(
            source.downcast_ref::<ReadError>(),
            Some(ReadError::ChecksumMismatch(_))
        ) || matches!(
            source.downcast_ref::<WriteError>(),
            Some(WriteError::ChecksumMismatch { .. })
        ) {
            return true;
        }
        current = source.source();
    }
    false
}

fn map_gcs_err(err: google_cloud_gax::error::Error) -> StoreError {
    if is_checksum_mismatch(&err) {
        return StoreError::Corruption(format!("Cloud Storage checksum mismatch: {err}"));
    }
    if is_out_of_range(&err) {
        tracing::warn!(error = %err, "Cloud Storage rejected request with out-of-range");
        return StoreError::OutOfRange("requested range out of bounds in object store".to_string());
    }
    if err.is_authentication() {
        return StoreError::Unauthenticated(err.to_string());
    }
    if err.is_timeout() && crate::resilience::remaining_rpc_deadline().is_some_and(|r| r.is_zero())
    {
        return StoreError::DeadlineExceeded(err.to_string());
    }
    if err.is_connect() || err.is_timeout() || err.is_io() || err.is_exhausted() {
        return StoreError::Unavailable(err.to_string());
    }
    if let Some(status) = err.status() {
        match status.code {
            RpcCode::InvalidArgument => {
                tracing::warn!(error = %err, "Cloud Storage rejected request with INVALID_ARGUMENT");
                return StoreError::InvalidArgument(
                    "invalid request argument rejected by object store".to_string(),
                );
            }
            RpcCode::ResourceExhausted => return StoreError::ResourceExhausted(err.to_string()),
            RpcCode::Unauthenticated => return StoreError::Unauthenticated(err.to_string()),
            RpcCode::PermissionDenied => return StoreError::PermissionDenied(err.to_string()),
            RpcCode::DeadlineExceeded
                if crate::resilience::remaining_rpc_deadline().is_some_and(|r| r.is_zero()) =>
            {
                return StoreError::DeadlineExceeded(err.to_string());
            }
            RpcCode::Unavailable | RpcCode::DeadlineExceeded => {
                return StoreError::Unavailable(err.to_string());
            }
            _ => {}
        }
    }
    if let Some(code) = err.http_status_code() {
        match code {
            400 => {
                tracing::warn!(error = %err, "Cloud Storage rejected request with HTTP 400");
                return StoreError::InvalidArgument(
                    "invalid request argument rejected by object store".to_string(),
                );
            }
            401 => return StoreError::Unauthenticated(err.to_string()),
            403 => return StoreError::PermissionDenied(err.to_string()),
            408 | 500 | 502..=504 => return StoreError::Unavailable(err.to_string()),
            429 => return StoreError::ResourceExhausted(err.to_string()),
            _ => {}
        }
    }
    StoreError::Backend(format!("Cloud Storage error: {err}"))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ParsedAuthority {
    host: String,
    port: Option<u16>,
    is_ipv6: bool,
}

impl ParsedAuthority {
    fn host_header(&self) -> String {
        match (self.is_ipv6, self.port) {
            (true, Some(p)) => format!("[{}]:{p}", self.host),
            (true, None) => format!("[{}]", self.host),
            (false, Some(p)) => format!("{}:{p}", self.host),
            (false, None) => self.host.clone(),
        }
    }

    fn connect_addr(&self, default_port: u16) -> String {
        let port = self.port.unwrap_or(default_port);
        if self.is_ipv6 {
            format!("[{}]:{port}", self.host)
        } else {
            format!("{}:{port}", self.host)
        }
    }

    fn is_loopback(&self) -> bool {
        if self.host.eq_ignore_ascii_case("localhost") {
            return true;
        }
        self.host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ParsedGcsEndpoint {
    url: String,
    is_http: bool,
    host_header: String,
    connect_addr: String,
}

fn parse_port(port_str: &str, raw: &str) -> StoreResult<u16> {
    if port_str.is_empty() || !port_str.bytes().all(|b| b.is_ascii_digit()) {
        return Err(StoreError::InvalidArgument(format!(
            "invalid port in Cloud Storage endpoint '{raw}'"
        )));
    }
    let port = port_str.parse::<u16>().map_err(|_| {
        StoreError::InvalidArgument(format!("invalid port in Cloud Storage endpoint '{raw}'"))
    })?;
    if port == 0 {
        return Err(StoreError::InvalidArgument(format!(
            "port cannot be 0 in Cloud Storage endpoint '{raw}'"
        )));
    }
    Ok(port)
}

fn is_valid_hostname(host: &str) -> bool {
    if host.is_empty()
        || host.starts_with('.')
        || host.ends_with('.')
        || host.starts_with('-')
        || host.ends_with('-')
        || host.contains("..")
    {
        return false;
    }
    host.bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_'))
}

fn parse_authority(
    authority: &str,
    allow_bare_ipv6: bool,
    raw: &str,
) -> StoreResult<ParsedAuthority> {
    if authority.is_empty()
        || authority
            .bytes()
            .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
    {
        return Err(StoreError::InvalidArgument(format!(
            "invalid Cloud Storage endpoint authority '{raw}'"
        )));
    }
    if authority.contains('@') {
        return Err(StoreError::InvalidArgument(format!(
            "Cloud Storage endpoint '{raw}' must not contain userinfo ('@')"
        )));
    }

    if let Some(after_open) = authority.strip_prefix('[') {
        let Some((ipv6_str, rest)) = after_open.split_once(']') else {
            return Err(StoreError::InvalidArgument(format!(
                "unclosed IPv6 bracket in Cloud Storage endpoint '{raw}'"
            )));
        };
        if ipv6_str.is_empty() || ipv6_str.parse::<std::net::Ipv6Addr>().is_err() {
            return Err(StoreError::InvalidArgument(format!(
                "invalid IPv6 host in Cloud Storage endpoint '{raw}'"
            )));
        }
        let port = if rest.is_empty() {
            None
        } else if let Some(port_str) = rest.strip_prefix(':') {
            Some(parse_port(port_str, raw)?)
        } else {
            return Err(StoreError::InvalidArgument(format!(
                "unexpected trailing characters after ']' in Cloud Storage endpoint '{raw}'"
            )));
        };
        return Ok(ParsedAuthority {
            host: ipv6_str.to_string(),
            port,
            is_ipv6: true,
        });
    }

    if authority.contains('[') || authority.contains(']') {
        return Err(StoreError::InvalidArgument(format!(
            "unexpected bracket in Cloud Storage endpoint '{raw}'"
        )));
    }

    let colon_count = authority.matches(':').count();
    if colon_count > 1 {
        if allow_bare_ipv6 && authority.parse::<std::net::Ipv6Addr>().is_ok() {
            return Ok(ParsedAuthority {
                host: authority.to_string(),
                port: None,
                is_ipv6: true,
            });
        }
        return Err(StoreError::InvalidArgument(format!(
            "invalid host:port in Cloud Storage endpoint '{raw}'"
        )));
    }

    let (host, port) = if let Some((h, port_str)) = authority.split_once(':') {
        (h, Some(parse_port(port_str, raw)?))
    } else {
        (authority, None)
    };

    if !is_valid_hostname(host) {
        return Err(StoreError::InvalidArgument(format!(
            "invalid host in Cloud Storage endpoint '{raw}'"
        )));
    }

    Ok(ParsedAuthority {
        host: host.to_string(),
        port,
        is_ipv6: false,
    })
}

struct ResolvedGcsConfig {
    bucket_resource: String,
    location_uri: String,
    endpoint: Option<ParsedGcsEndpoint>,
}

fn non_empty_endpoint(raw: &str) -> Option<&str> {
    if raw.bytes().any(|b| b.is_ascii_control()) {
        return Some(raw);
    }
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

fn resolve_gcs_connect_config(config: &GcsConnectConfig) -> StoreResult<ResolvedGcsConfig> {
    let (bucket_resource, location_uri) = normalize_bucket_identifiers(&config.bucket)?;
    let emulator_env = std::env::var("STORAGE_EMULATOR_HOST")
        .ok()
        .filter(|s| non_empty_endpoint(s).is_some());
    let effective_endpoint = config
        .endpoint
        .as_deref()
        .and_then(non_empty_endpoint)
        .map(str::to_string)
        .or_else(|| emulator_env.clone());

    let endpoint = match effective_endpoint {
        Some(ep) => {
            let from_emulator_env = emulator_env.as_deref().map(str::trim) == Some(ep.trim());
            Some(parse_gcs_endpoint(&ep, from_emulator_env)?)
        }
        None => None,
    };

    Ok(ResolvedGcsConfig {
        bucket_resource,
        location_uri,
        endpoint,
    })
}

pub fn validate_gcs_endpoint(raw_endpoint: &str) -> StoreResult<()> {
    let emulator_env = std::env::var("STORAGE_EMULATOR_HOST")
        .ok()
        .filter(|s| non_empty_endpoint(s).is_some());
    let from_emulator_env = emulator_env.as_deref().map(str::trim) == Some(raw_endpoint.trim());
    let _ = parse_gcs_endpoint(raw_endpoint, from_emulator_env)?;
    Ok(())
}

fn parse_gcs_endpoint(
    raw_endpoint: &str,
    from_emulator_env: bool,
) -> StoreResult<ParsedGcsEndpoint> {
    if raw_endpoint.bytes().any(|b| b.is_ascii_control()) {
        return Err(StoreError::InvalidArgument(format!(
            "Cloud Storage endpoint '{raw_endpoint}' contains control characters"
        )));
    }
    let trimmed = raw_endpoint.trim();
    if trimmed.is_empty() || trimmed.bytes().any(|b| b.is_ascii_whitespace()) {
        return Err(StoreError::InvalidArgument(format!(
            "invalid Cloud Storage endpoint '{raw_endpoint}'"
        )));
    }
    if trimmed.contains('?') || trimmed.contains('#') || trimmed.contains('\\') {
        return Err(StoreError::InvalidArgument(format!(
            "Cloud Storage endpoint '{raw_endpoint}' must not contain query, fragment, or backslash characters"
        )));
    }

    let (explicit_http, rest) = if let Some((scheme, after)) = trimmed.split_once("://") {
        if scheme.eq_ignore_ascii_case("http") {
            (Some(true), after)
        } else if scheme.eq_ignore_ascii_case("https") {
            (Some(false), after)
        } else {
            return Err(StoreError::InvalidArgument(format!(
                "unsupported scheme in Cloud Storage endpoint '{raw_endpoint}' (expected http:// or https://)"
            )));
        }
    } else {
        (None, trimmed)
    };

    let (authority_str, path_suffix) = match rest.find('/') {
        Some(idx) => (&rest[..idx], &rest[idx..]),
        None => (rest, ""),
    };

    let authority = parse_authority(authority_str, explicit_http.is_none(), raw_endpoint)?;
    let is_loopback = authority.is_loopback();

    let is_http = match explicit_http {
        Some(true) => {
            if !is_loopback && !from_emulator_env {
                return Err(StoreError::InvalidArgument(format!(
                    "refusing plaintext http:// Cloud Storage endpoint '{raw_endpoint}' \
                     unless host is loopback or matches STORAGE_EMULATOR_HOST"
                )));
            }
            true
        }
        Some(false) => false,
        None => {
            if is_loopback || from_emulator_env {
                true
            } else {
                return Err(StoreError::InvalidArgument(format!(
                    "Cloud Storage endpoint '{raw_endpoint}' is missing a scheme (expected https://...)"
                )));
            }
        }
    };

    let normalized_path = if is_http {
        if !path_suffix.is_empty() && path_suffix != "/" {
            return Err(StoreError::InvalidArgument(format!(
                "plaintext http:// Cloud Storage emulator endpoint '{raw_endpoint}' \
                 must not include a path prefix"
            )));
        }
        ""
    } else {
        path_suffix
    };

    let scheme = if is_http { "http" } else { "https" };
    let host_header = authority.host_header();
    let default_port = if is_http { 80 } else { 443 };
    let connect_addr = authority.connect_addr(default_port);
    let url = format!("{scheme}://{host_header}{normalized_path}");

    Ok(ParsedGcsEndpoint {
        url,
        is_http,
        host_header,
        connect_addr,
    })
}

#[cfg(test)]
fn is_loopback_endpoint_host(host_port: &str) -> bool {
    parse_authority(host_port.trim(), true, host_port)
        .is_ok_and(|authority| authority.is_loopback())
}
