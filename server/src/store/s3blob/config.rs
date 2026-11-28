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

//! S3 bucket, region, endpoint, credential, and peer-specification validation and redaction.
//!
//! # Cross-Process and RPC Boundaries
//! Canonicalizes S3 endpoints with `url::Url` so the signed `Host` header matches `reqwest`
//! byte-for-byte, rejects embedded userinfo or control characters, and redacts credentials
//! before formatting configuration errors or `Debug` output.

use std::fmt;
use std::net::Ipv4Addr;

use reqwest::header::HeaderValue;

use super::DEFAULT_S3_REGION;
use super::sigv4::uri_encode;
use crate::store::{StoreError, StoreResult};

#[derive(Clone)]
pub struct S3ConnectConfig {
    /// Bucket name (`my-bucket` or `s3://my-bucket`).
    pub bucket: String,
    /// Optional custom S3-compatible endpoint (e.g. `http://127.0.0.1:9000`, `http://minio:9000`,
    /// or `https://s3.us-east-1.amazonaws.com`).
    pub endpoint: Option<String>,
    /// AWS SigV4 signing region (defaults to `us-east-1`).
    pub region: String,
    /// Optional access key ID (omit both access_key and secret_key for anonymous endpoints).
    pub access_key: Option<String>,
    /// Optional secret access key (required whenever `access_key` is set).
    pub secret_key: Option<String>,
    /// Optional temporary session token (`x-amz-security-token`).
    pub session_token: Option<String>,
    /// Whether to use path-style URLs (`/<bucket>/<key>`). Defaults to `true` when `endpoint` is
    /// set and `false` for standard AWS virtual-hosted endpoints.
    pub path_style: Option<bool>,
    /// Whether `connect` ensures the bucket exists via `HEAD` + `PUT`. Defaults to `true` for
    /// `http://` endpoints and `false` otherwise.
    pub auto_create_bucket: Option<bool>,
}

impl Default for S3ConnectConfig {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            endpoint: None,
            region: DEFAULT_S3_REGION.to_string(),
            access_key: None,
            secret_key: None,
            session_token: None,
            path_style: None,
            auto_create_bucket: None,
        }
    }
}

impl fmt::Debug for S3ConnectConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3ConnectConfig")
            .field("bucket", &redact_endpoint_for_error(&self.bucket))
            .field(
                "endpoint",
                &self.endpoint.as_deref().map(redact_endpoint_for_error),
            )
            .field("region", &self.region)
            .field("access_key", &self.access_key)
            .field(
                "secret_key",
                &self.secret_key.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .field("path_style", &self.path_style)
            .field("auto_create_bucket", &self.auto_create_bucket)
            .finish()
    }
}

impl S3ConnectConfig {
    /// Validates bucket naming, region, endpoint syntax, and credential pairing without opening
    /// network connections.
    pub fn validate(&self) -> StoreResult<()> {
        let _ = resolve_s3_connect_config(self)?;
        Ok(())
    }
}

#[derive(Clone)]
pub(super) struct S3Credentials {
    pub(super) access_key: String,
    pub(super) secret_key: String,
    pub(super) session_token: Option<String>,
}

impl fmt::Debug for S3Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Credentials")
            .field("access_key", &self.access_key)
            .field("secret_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

fn is_valid_s3_bucket_name(bucket_id: &str) -> bool {
    let bytes = bucket_id.as_bytes();
    let valid_len = (3..=63).contains(&bytes.len());
    let valid_chars = bytes
        .iter()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'-'));
    let valid_labels = bucket_id
        .split('.')
        .all(|label| !label.is_empty() && !label.starts_with('-') && !label.ends_with('-'));
    let not_ipv4 = bucket_id.parse::<Ipv4Addr>().is_err();
    valid_len && valid_chars && valid_labels && not_ipv4
}

fn is_valid_s3_region_name(trimmed: &str) -> bool {
    let bytes = trimmed.as_bytes();
    let valid_len = (1..=64).contains(&bytes.len());
    let valid_chars = bytes
        .iter()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-');
    let valid_edges = bytes
        .first()
        .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        && bytes
            .last()
            .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit());
    valid_len && valid_chars && valid_edges
}

/// Validates and normalizes an S3 bucket identifier (`my-bucket`, `s3://my-bucket`, or
/// `s3://my-bucket/`) into its canonical bare bucket name.
pub fn normalize_s3_bucket(raw: &str) -> StoreResult<String> {
    let safe_raw = redact_endpoint_for_error(raw);
    if raw.bytes().any(|b| b.is_ascii_control()) {
        return Err(StoreError::InvalidArgument(format!(
            "S3 bucket identifier '{safe_raw}' contains control characters"
        )));
    }
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.bytes().any(|b| b.is_ascii_whitespace()) {
        return Err(StoreError::InvalidArgument(format!(
            "invalid S3 bucket identifier '{safe_raw}'"
        )));
    }
    let trimmed = trimmed.strip_suffix('/').unwrap_or(trimmed);
    let bucket_id = trimmed.strip_prefix("s3://").unwrap_or(trimmed);

    if !is_valid_s3_bucket_name(bucket_id) {
        return Err(StoreError::InvalidArgument(format!(
            "invalid S3 bucket identifier '{safe_raw}'"
        )));
    }

    Ok(bucket_id.to_string())
}

/// Validates an S3 endpoint URL (`http://...` or `https://...`) without opening network
/// connections.
pub fn validate_s3_endpoint(raw_endpoint: &str) -> StoreResult<()> {
    let _ = parse_s3_endpoint(raw_endpoint)?;
    Ok(())
}

fn split_s3_peer_bucket_and_region(raw_lhs: &str) -> Option<(&str, Option<&str>)> {
    if raw_lhs.bytes().any(|b| b.is_ascii_control()) {
        return None;
    }
    let trimmed = raw_lhs.trim();
    if trimmed.is_empty() {
        return None;
    }
    let prefix_len = if trimmed.starts_with("s3://") { 5 } else { 0 };
    let body = &trimmed[prefix_len..];
    if body.is_empty()
        || body.contains(['@', '?', '#'])
        || body.bytes().any(|b| b.is_ascii_whitespace())
    {
        return None;
    }
    if let Some((bucket_part, region_part)) = body.split_once(':') {
        if bucket_part.is_empty()
            || region_part.is_empty()
            || bucket_part.contains('/')
            || region_part.contains([':', '/'])
        {
            return None;
        }
        Some((
            &trimmed[..prefix_len + bucket_part.len()],
            Some(region_part),
        ))
    } else {
        let bare = body.strip_suffix('/').unwrap_or(body);
        if bare.is_empty() || bare.contains('/') {
            return None;
        }
        Some((trimmed, None))
    }
}

/// Parses an S3 peer specification formatted as `<bucket>`, `<bucket>:<region>`,
/// `<bucket>=<endpoint>`, or `<bucket>:<region>=<endpoint>` (where `<bucket>` may include an
/// `s3://` prefix), inheriting region, endpoint, credentials, and style defaults from `defaults`
/// and validating the resulting [`S3ConnectConfig`].
pub fn parse_s3_peer_spec(
    raw_spec: &str,
    defaults: &S3ConnectConfig,
) -> StoreResult<S3ConnectConfig> {
    const EXPECTED_SYNTAX: &str =
        "(expected <bucket>, <bucket>:<region>, or <bucket>[:<region>]=<endpoint>)";
    let spec = if raw_spec.bytes().any(|b| b.is_ascii_control()) {
        raw_spec
    } else {
        raw_spec.trim()
    };
    if spec.is_empty() {
        return Err(StoreError::InvalidArgument(format!(
            "S3 peer bucket spec cannot be empty {EXPECTED_SYNTAX}"
        )));
    }

    let (raw_lhs, endpoint) = match spec.split_once('=') {
        Some((b, ep)) => {
            if !is_safe_s3_peer_bucket_lhs(b) {
                let safe_spec = redact_s3_peer_spec(spec);
                return Err(StoreError::InvalidArgument(format!(
                    "invalid S3 peer bucket spec '{safe_spec}' {EXPECTED_SYNTAX}"
                )));
            }
            let ep_clean = if ep.bytes().any(|c| c.is_ascii_control()) {
                ep
            } else {
                ep.trim()
            };
            if ep_clean.is_empty() {
                let safe_spec = redact_s3_peer_spec(spec);
                return Err(StoreError::InvalidArgument(format!(
                    "invalid S3 peer bucket spec '{safe_spec}' {EXPECTED_SYNTAX}"
                )));
            }
            (b.trim(), Some(ep_clean.to_string()))
        }
        None => (spec, defaults.endpoint.clone()),
    };

    let Some((bucket, region_override)) = split_s3_peer_bucket_and_region(raw_lhs) else {
        let safe_spec = redact_s3_peer_spec(spec);
        return Err(StoreError::InvalidArgument(format!(
            "invalid S3 peer bucket spec '{safe_spec}' {EXPECTED_SYNTAX}"
        )));
    };
    let region = region_override
        .map(str::to_string)
        .unwrap_or_else(|| defaults.region.clone());

    let cfg = S3ConnectConfig {
        bucket: bucket.to_string(),
        endpoint,
        region,
        access_key: defaults.access_key.clone(),
        secret_key: defaults.secret_key.clone(),
        session_token: defaults.session_token.clone(),
        path_style: defaults.path_style,
        auto_create_bucket: defaults.auto_create_bucket,
    };
    cfg.validate()?;
    Ok(cfg)
}

pub(super) fn validate_s3_region(raw_region: &str) -> StoreResult<String> {
    let safe_region = redact_endpoint_for_error(raw_region);
    if raw_region.bytes().any(|b| b.is_ascii_control()) {
        return Err(StoreError::InvalidArgument(format!(
            "S3 region '{safe_region}' contains control characters"
        )));
    }
    let trimmed = raw_region.trim();
    if !is_valid_s3_region_name(trimmed) {
        return Err(StoreError::InvalidArgument(format!(
            "invalid S3 region '{safe_region}' (expected non-empty [a-z0-9-])"
        )));
    }
    Ok(trimmed.to_string())
}

fn validate_credentials(
    access_key: Option<&str>,
    secret_key: Option<&str>,
    session_token: Option<&str>,
) -> StoreResult<Option<S3Credentials>> {
    match (access_key, secret_key) {
        (None, None) => {
            if session_token.is_some() {
                return Err(StoreError::InvalidArgument(
                    "S3 session_token cannot be set without access_key and secret_key".to_string(),
                ));
            }
            Ok(None)
        }
        (Some(_), None) | (None, Some(_)) => Err(StoreError::InvalidArgument(
            "S3 credentials require both access_key and secret_key".to_string(),
        )),
        (Some(ak), Some(sk)) => {
            if ak.is_empty()
                || ak
                    .bytes()
                    .any(|b| !b.is_ascii_graphic() || b == b'/' || b == b',')
            {
                return Err(StoreError::InvalidArgument(
                    "invalid S3 access_key (must be non-empty printable ASCII without whitespace, '/', ',', or control characters)"
                        .to_string(),
                ));
            }
            if sk.is_empty() || sk.bytes().any(|b| b.is_ascii_control()) {
                return Err(StoreError::InvalidArgument(
                    "invalid S3 secret_key (must be non-empty without control characters)"
                        .to_string(),
                ));
            }
            let validated_token = match session_token {
                Some(tok) => {
                    if tok.is_empty()
                        || tok.bytes().any(|b| b.is_ascii_control())
                        || HeaderValue::from_str(tok).is_err()
                    {
                        return Err(StoreError::InvalidArgument(
                            "invalid S3 session_token (must be non-empty HTTP header text without control characters)"
                                .to_string(),
                        ));
                    }
                    Some(tok.to_string())
                }
                None => None,
            };
            Ok(Some(S3Credentials {
                access_key: ak.to_string(),
                secret_key: sk.to_string(),
                session_token: validated_token,
            }))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ParsedAuthority {
    host: String,
    port: Option<u16>,
    pub(super) is_ip: bool,
    is_loopback: bool,
}

impl ParsedAuthority {
    fn host_header(&self) -> String {
        match self.port {
            Some(p) => format!("{}:{p}", self.host),
            None => self.host.clone(),
        }
    }

    pub(super) fn is_loopback(&self) -> bool {
        self.is_loopback
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ParsedS3Endpoint {
    pub(super) scheme: &'static str,
    is_http: bool,
    pub(super) authority: ParsedAuthority,
    pub(super) host_header: String,
    pub(super) base_path: String,
}

fn is_valid_hostname(host: &str) -> bool {
    if host.is_empty() {
        return false;
    }
    let valid_chars = host
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_'));
    let valid_labels = host
        .split('.')
        .all(|label| !label.is_empty() && !label.starts_with('-') && !label.ends_with('-'));
    valid_chars && valid_labels
}

fn sanitize_control_chars(s: &str) -> String {
    if !s.bytes().any(|b| b.is_ascii_control()) {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        if ch.is_ascii_control() {
            out.extend(ch.escape_default());
        } else {
            out.push(ch);
        }
    }
    out
}

fn is_valid_uri_scheme(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'-' | b'.'))
}

fn redact_endpoint_for_error(raw: &str) -> String {
    let (prefix, body) = match raw.find("://") {
        Some(scheme_end) if is_valid_uri_scheme(&raw[..scheme_end]) => {
            (&raw[..scheme_end + 3], &raw[scheme_end + 3..])
        }
        _ => ("", raw),
    };
    let first_qf = body.find(['?', '#']);
    let at_any = body.rfind('@');
    if let (Some(qf_pos), Some(at_pos)) = (first_qf, at_any)
        && at_pos > qf_pos
    {
        // Fail closed when `@` appears after `?` or `#`, which is ambiguous between a password
        // containing `?`/`#` and a query string containing `@` alongside secrets.
        return sanitize_control_chars(&format!("{prefix}***"));
    }

    let (user_prefix, after_user) = match at_any {
        Some(at_pos) => ("***@", &body[at_pos + 1..]),
        None => ("", body),
    };
    let redacted_tail = match after_user.find(['?', '#']) {
        Some(qf_pos) => {
            let delim = after_user.as_bytes()[qf_pos] as char;
            format!("{}{delim}<redacted>", &after_user[..qf_pos])
        }
        None => after_user.to_string(),
    };
    sanitize_control_chars(&format!("{prefix}{user_prefix}{redacted_tail}"))
}

fn is_safe_s3_peer_bucket_lhs(lhs: &str) -> bool {
    if lhs.bytes().any(|b| b.is_ascii_control()) {
        return false;
    }
    let lhs_trimmed = lhs.trim();
    let lhs_core = lhs_trimmed
        .get(..5)
        .filter(|p| p.eq_ignore_ascii_case("s3://"))
        .map_or(lhs_trimmed, |_| &lhs_trimmed[5..]);
    if lhs_core.contains(['@', '?', '#']) || lhs_core.bytes().any(|b| b.is_ascii_whitespace()) {
        return false;
    }
    if let Some((bucket_part, region_part)) = lhs_core.split_once(':') {
        is_valid_s3_bucket_name(bucket_part) && is_valid_s3_region_name(region_part)
    } else {
        let lhs_core = lhs_core.strip_suffix('/').unwrap_or(lhs_core);
        !lhs_core.is_empty() && !lhs_core.contains('/')
    }
}

fn rhs_has_unschemed_at(rhs: &str) -> bool {
    let Some(at_pos) = rhs.find('@') else {
        return false;
    };
    !matches!(
        rhs.find("://"),
        Some(scheme_end) if scheme_end < at_pos && is_valid_uri_scheme(&rhs[..scheme_end])
    )
}

/// Redacts userinfo (`@`), query strings (`?`), fragments (`#`), and ASCII control characters from
/// an S3 peer specification (`<bucket>`, `<bucket>:<region>`, or `<bucket>[:<region>]=<endpoint>`)
/// before formatting errors.
pub fn redact_s3_peer_spec(raw_spec: &str) -> String {
    if let Some((lhs, rhs)) = raw_spec.split_once('=')
        && is_safe_s3_peer_bucket_lhs(lhs)
        && !rhs_has_unschemed_at(rhs.trim_start())
    {
        return format!("{lhs}={}", redact_endpoint_for_error(rhs));
    }
    redact_endpoint_for_error(raw_spec)
}

fn percent_decode_path_segment(seg: &str, safe_raw: &str) -> StoreResult<String> {
    let bytes = seg.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return Err(StoreError::InvalidArgument(format!(
                    "S3 endpoint '{safe_raw}' contains malformed percent-encoding in path"
                )));
            }
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            let (Some(hi), Some(lo)) = (hi, lo) else {
                return Err(StoreError::InvalidArgument(format!(
                    "S3 endpoint '{safe_raw}' contains malformed percent-encoding in path"
                )));
            };
            let decoded = ((hi << 4) | lo) as u8;
            if decoded.is_ascii_control() || decoded == b'/' {
                return Err(StoreError::InvalidArgument(format!(
                    "S3 endpoint '{safe_raw}' contains invalid percent-encoded path byte"
                )));
            }
            out.push(decoded);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| {
        StoreError::InvalidArgument(format!(
            "S3 endpoint '{safe_raw}' contains invalid UTF-8 in percent-encoded path"
        ))
    })
}

pub(super) fn parse_s3_endpoint(raw_endpoint: &str) -> StoreResult<ParsedS3Endpoint> {
    let safe_raw = redact_endpoint_for_error(raw_endpoint);
    if raw_endpoint.bytes().any(|b| b.is_ascii_control()) {
        return Err(StoreError::InvalidArgument(format!(
            "S3 endpoint '{safe_raw}' contains control characters"
        )));
    }
    let trimmed = raw_endpoint.trim();
    if trimmed.is_empty() || trimmed.bytes().any(|b| b.is_ascii_whitespace()) {
        return Err(StoreError::InvalidArgument(format!(
            "invalid S3 endpoint '{safe_raw}'"
        )));
    }
    if trimmed.contains('?') || trimmed.contains('#') || trimmed.contains('\\') {
        return Err(StoreError::InvalidArgument(format!(
            "S3 endpoint '{safe_raw}' must not contain query, fragment, or backslash characters"
        )));
    }

    let Some((raw_scheme, rest)) = trimmed.split_once("://") else {
        return Err(StoreError::InvalidArgument(format!(
            "S3 endpoint '{safe_raw}' is missing a scheme (expected http:// or https://)"
        )));
    };
    let (scheme, is_http) = if raw_scheme.eq_ignore_ascii_case("http") {
        ("http", true)
    } else if raw_scheme.eq_ignore_ascii_case("https") {
        ("https", false)
    } else {
        return Err(StoreError::InvalidArgument(format!(
            "unsupported scheme in S3 endpoint '{safe_raw}' (expected http:// or https://)"
        )));
    };

    let (authority_str, path_suffix) = match rest.find('/') {
        Some(idx) => (&rest[..idx], &rest[idx..]),
        None => (rest, ""),
    };
    if authority_str.is_empty() {
        return Err(StoreError::InvalidArgument(format!(
            "invalid S3 endpoint authority '{safe_raw}'"
        )));
    }
    if authority_str.contains('@') {
        return Err(StoreError::InvalidArgument(format!(
            "S3 endpoint '{safe_raw}' must not contain userinfo ('@')"
        )));
    }
    if authority_str.ends_with(':') {
        return Err(StoreError::InvalidArgument(format!(
            "invalid port in S3 endpoint '{safe_raw}'"
        )));
    }

    let base_path = if path_suffix.is_empty() || path_suffix == "/" {
        String::new()
    } else {
        let stripped = path_suffix.strip_suffix('/').unwrap_or(path_suffix);
        if stripped.contains("//") {
            return Err(StoreError::InvalidArgument(format!(
                "S3 endpoint '{safe_raw}' contains empty path segments"
            )));
        }
        // Percent-decode each raw segment before single-encoding with SigV4 RFC 3986 rules so
        // pre-encoded prefixes are not double-encoded and `%2e`/`%2e%2e` dot segments are caught.
        let mut encoded = String::new();
        for seg in stripped.strip_prefix('/').unwrap_or(stripped).split('/') {
            if seg.is_empty() {
                return Err(StoreError::InvalidArgument(format!(
                    "S3 endpoint '{safe_raw}' contains empty path segments"
                )));
            }
            let decoded = percent_decode_path_segment(seg, &safe_raw)?;
            if decoded == "." || decoded == ".." {
                return Err(StoreError::InvalidArgument(format!(
                    "S3 endpoint '{safe_raw}' contains invalid path segment '{seg}'"
                )));
            }
            encoded.push('/');
            encoded.push_str(&uri_encode(&decoded, true));
        }
        encoded
    };

    // Parse with `url::Url` (the exact WHATWG parser used by `reqwest`) so canonical host
    // serialization, IP detection, IPv4/IPv6 normalization (including `[::ffff:7f00:1]` and
    // shorthand IPv4), and default port stripping match the wire `Host` header byte-for-byte.
    let parsed_url = url::Url::parse(trimmed)
        .map_err(|_| StoreError::InvalidArgument(format!("invalid S3 endpoint '{safe_raw}'")))?;
    if !parsed_url.username().is_empty() || parsed_url.password().is_some() {
        return Err(StoreError::InvalidArgument(format!(
            "S3 endpoint '{safe_raw}' must not contain userinfo ('@')"
        )));
    }
    if parsed_url.port() == Some(0) {
        return Err(StoreError::InvalidArgument(format!(
            "port cannot be 0 in S3 endpoint '{safe_raw}'"
        )));
    }
    let Some(host) = parsed_url.host() else {
        return Err(StoreError::InvalidArgument(format!(
            "invalid host in S3 endpoint '{safe_raw}'"
        )));
    };
    let Some(canonical_host_str) = parsed_url.host_str() else {
        return Err(StoreError::InvalidArgument(format!(
            "invalid host in S3 endpoint '{safe_raw}'"
        )));
    };

    let (is_ip, is_loopback) = match host {
        url::Host::Ipv4(v4) => (true, v4.is_loopback()),
        url::Host::Ipv6(v6) => (
            true,
            v6.is_loopback() || v6.to_ipv4_mapped().is_some_and(|v4| v4.is_loopback()),
        ),
        url::Host::Domain(domain) => {
            if !is_valid_hostname(domain) {
                return Err(StoreError::InvalidArgument(format!(
                    "invalid host in S3 endpoint '{safe_raw}'"
                )));
            }
            let loopback = domain.eq_ignore_ascii_case("localhost")
                || domain.to_ascii_lowercase().ends_with(".localhost");
            (false, loopback)
        }
    };

    let authority = ParsedAuthority {
        host: canonical_host_str.to_string(),
        port: parsed_url.port(),
        is_ip,
        is_loopback,
    };
    let host_header = authority.host_header();
    if !base_path.is_empty() {
        let probe_url =
            url::Url::parse(&format!("{scheme}://{host_header}{base_path}/")).map_err(|_| {
                StoreError::InvalidArgument(format!("invalid S3 endpoint '{safe_raw}'"))
            })?;
        if probe_url.path().strip_suffix('/') != Some(base_path.as_str()) {
            return Err(StoreError::InvalidArgument(format!(
                "S3 endpoint '{safe_raw}' path is not canonical"
            )));
        }
    }
    Ok(ParsedS3Endpoint {
        scheme,
        is_http,
        authority,
        host_header,
        base_path,
    })
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

pub(super) struct ResolvedS3Config {
    pub(super) scheme: &'static str,
    pub(super) request_host: String,
    pub(super) base_path: String,
    pub(super) bucket: String,
    pub(super) region: String,
    pub(super) path_style: bool,
    pub(super) auto_create_bucket: bool,
    pub(super) is_loopback: bool,
    pub(super) location_uri: String,
    pub(super) credentials: Option<S3Credentials>,
}

pub(super) fn resolve_s3_connect_config(config: &S3ConnectConfig) -> StoreResult<ResolvedS3Config> {
    let bucket = normalize_s3_bucket(&config.bucket)?;
    let region = validate_s3_region(&config.region)?;
    let credentials = validate_credentials(
        config.access_key.as_deref(),
        config.secret_key.as_deref(),
        config.session_token.as_deref(),
    )?;

    let endpoint_opt = match config.endpoint.as_deref().and_then(non_empty_endpoint) {
        Some(ep) => Some(parse_s3_endpoint(ep)?),
        None => None,
    };

    let has_custom_endpoint = endpoint_opt.is_some();
    let path_style = config.path_style.unwrap_or(has_custom_endpoint);
    let is_http = endpoint_opt.as_ref().is_some_and(|e| e.is_http);
    let auto_create_bucket = config.auto_create_bucket.unwrap_or(is_http);

    let (scheme, request_host, base_path, is_loopback) = match endpoint_opt {
        Some(ep) => {
            if !path_style && ep.authority.is_ip {
                return Err(StoreError::InvalidArgument(
                    "S3 virtual-hosted style requires a DNS hostname endpoint, not an IP address"
                        .to_string(),
                ));
            }
            let host = if path_style {
                ep.host_header
            } else {
                format!("{bucket}.{}", ep.host_header)
            };
            (ep.scheme, host, ep.base_path, ep.authority.is_loopback())
        }
        None => {
            let aws_host = format!("s3.{region}.amazonaws.com");
            let host = if path_style {
                aws_host
            } else {
                format!("{bucket}.{aws_host}")
            };
            ("https", host, String::new(), false)
        }
    };

    if !path_style {
        let vh_url_str = format!("{scheme}://{request_host}{base_path}/");
        let vh_url = url::Url::parse(&vh_url_str).map_err(|e| {
            StoreError::InvalidArgument(format!(
                "invalid virtual-hosted S3 endpoint for bucket '{bucket}': {e}"
            ))
        })?;
        let Some(url::Host::Domain(vh_domain)) = vh_url.host() else {
            return Err(StoreError::InvalidArgument(
                "S3 virtual-hosted style requires a DNS hostname endpoint, not an IP address"
                    .to_string(),
            ));
        };
        if !is_valid_hostname(vh_domain)
            || vh_domain.len() > 253
            || vh_domain
                .split('.')
                .any(|label| label.is_empty() || label.len() > 63)
        {
            return Err(StoreError::InvalidArgument(format!(
                "invalid virtual-hosted S3 hostname '{vh_domain}'"
            )));
        }
        let canonical_vh_host = match vh_url.port() {
            Some(p) => format!("{vh_domain}:{p}"),
            None => vh_domain.to_string(),
        };
        if canonical_vh_host != request_host {
            return Err(StoreError::InvalidArgument(format!(
                "virtual-hosted S3 host '{request_host}' is not canonical (expected '{canonical_vh_host}')"
            )));
        }
    }

    let location_uri = format!("s3://{bucket}");
    Ok(ResolvedS3Config {
        scheme,
        request_host,
        base_path,
        bucket,
        region,
        path_style,
        auto_create_bucket,
        is_loopback,
        location_uri,
        credentials,
    })
}
