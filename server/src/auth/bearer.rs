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

//! Static Bearer token validation, token-file loading, memory scrubbing, and bind-address
//! authentication policy for `jj-cc-server`.
//!
//! # Authority and Crash-Safety Ordering
//! Configured static tokens are validated for graphic ASCII syntax, hashed with SHA-256, and
//! scrubbed from plaintext buffers. Non-loopback bind hosts fail closed unless authentication
//! or `--allow-unauthenticated` is explicitly configured.
//!
//! # Concurrency and Merge Invariants
//! Incoming credentials are compared across all configured SHA-256 digests in constant time
//! without short-circuiting so neither prefix mismatches nor token ordering leak timing data.

use std::io::Read as _;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;

use sha2::{Digest as _, Sha256};
use subtle::{Choice, ConstantTimeEq as _};
use tonic::{Request, Status, service::Interceptor};

use super::MAX_AUTH_TOKEN_BYTES;

/// Public upper bound on a single static Bearer token's byte length so synchronous SHA-256
/// hashing inside `Interceptor::call` cannot be abused with multi-megabyte headers.
pub const MAX_BEARER_TOKEN_BYTES: usize = 4096;

pub(super) const MAX_TOKEN_FILE_BYTES: u64 = 64 * 1024;

/// Constant-time gRPC Bearer token validator.
///
/// When `tokens` is `None` (no `--auth-token` / `--auth-token-file` or env equivalents
/// configured), all requests pass through unchanged on loopback or when
/// `--allow-unauthenticated` (`JJ_CC_ALLOW_UNAUTHENTICATED`) is set.
#[derive(Clone, Default)]
pub struct BearerAuthInterceptor {
    tokens: Option<Arc<Vec<[u8; 32]>>>,
}

/// Prints the number of configured tokens, never their values.
impl std::fmt::Debug for BearerAuthInterceptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let token_count = self.tokens.as_ref().map_or(0, |tokens| tokens.len());
        f.debug_struct("BearerAuthInterceptor")
            .field("token_count", &token_count)
            .finish()
    }
}

fn validate_token_syntax(token: &str) -> Result<(), String> {
    if token.len() > MAX_BEARER_TOKEN_BYTES {
        return Err(format!(
            "configured Bearer token exceeds maximum length of {MAX_BEARER_TOKEN_BYTES} bytes"
        ));
    }
    if !token.bytes().all(|b| b.is_ascii_graphic() && b != b'#') {
        return Err(
            "configured Bearer token contains whitespace, '#', or non-graphic ASCII characters; \
             check --auth-token / --auth-token-file"
                .to_string(),
        );
    }
    Ok(())
}

fn strip_whitespace_delimited_comment(line: &str) -> &str {
    let bytes = line.as_bytes();
    for (idx, &b) in bytes.iter().enumerate() {
        if b == b'#' && (idx == 0 || bytes[idx - 1].is_ascii_whitespace()) {
            return &line[..idx];
        }
    }
    line
}

impl BearerAuthInterceptor {
    /// Parses and validates graphic-ASCII Bearer tokens, rejecting tokens that contain
    /// whitespace, `'#'`, control bytes, or non-ASCII characters without echoing secrets.
    pub fn try_from_tokens<I, S>(raw_tokens: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut normalized = Vec::new();
        for raw in raw_tokens {
            let without_bom = raw
                .as_ref()
                .strip_prefix('\u{feff}')
                .unwrap_or(raw.as_ref());
            for part in without_bom.split(',') {
                let trimmed = part.trim();
                if trimmed.is_empty() {
                    continue;
                }
                validate_token_syntax(trimmed)?;
                let digest: [u8; 32] = Sha256::digest(trimmed.as_bytes()).into();
                if !normalized.contains(&digest) {
                    normalized.push(digest);
                }
            }
        }

        if normalized.is_empty() {
            Ok(Self { tokens: None })
        } else {
            Ok(Self {
                tokens: Some(Arc::new(normalized)),
            })
        }
    }

    #[cfg(test)]
    pub fn from_tokens<I, S>(raw_tokens: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self::try_from_tokens(raw_tokens)
            .expect("configured Bearer tokens must be graphic ASCII without whitespace or '#'")
    }

    pub fn is_enabled(&self) -> bool {
        self.tokens.is_some()
    }

    /// Constant-time match of a raw bearer token string against configured static token digests.
    pub(super) fn verify_token_str(&self, token: &str) -> bool {
        let Some(expected_tokens) = &self.tokens else {
            return false;
        };
        if token.is_empty()
            || token.len() > MAX_BEARER_TOKEN_BYTES
            || !token.bytes().all(|b| b.is_ascii_graphic() && b != b'#')
        {
            return false;
        }
        let provided_digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let matched = expected_tokens
            .iter()
            .fold(Choice::from(0u8), |acc, expected| {
                acc | provided_digest.ct_eq(expected)
            });
        bool::from(matched)
    }
}

pub(super) fn scrub_bytes(mut bytes: Vec<u8>) {
    bytes.fill(0);
    std::hint::black_box(bytes.as_slice());
}

/// Best-effort zeroization of plaintext token buffers before dropping them.
pub fn scrub_tokens(tokens: &mut Vec<String>) {
    for tok in tokens.drain(..) {
        scrub_bytes(tok.into_bytes());
    }
}

/// Owned Bearer credential whose heap buffer is zeroized on drop, including on async task
/// cancellation.
pub(super) struct ScrubbedToken(String);

impl ScrubbedToken {
    pub(super) fn new(token: String) -> Self {
        Self(token)
    }

    pub(super) fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for ScrubbedToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ScrubbedToken(<redacted>)")
    }
}

impl Drop for ScrubbedToken {
    fn drop(&mut self) {
        scrub_bytes(std::mem::take(&mut self.0).into_bytes());
    }
}

/// Reads newline- or comma-separated graphic-ASCII Bearer tokens from `path` (up to 64 KiB),
/// stripping whitespace-delimited `#` comments and blank entries while rejecting embedded `#`
/// characters so secrets are never silently truncated.
pub fn load_tokens_from_file(path: &Path) -> Result<Vec<String>, String> {
    let file = std::fs::File::open(path)
        .map_err(|e| format!("failed to read --auth-token-file '{}': {e}", path.display()))?;
    let mut raw_bytes = Vec::new();
    if let Err(e) = file
        .take(MAX_TOKEN_FILE_BYTES + 1)
        .read_to_end(&mut raw_bytes)
    {
        scrub_bytes(raw_bytes);
        return Err(format!(
            "failed to read --auth-token-file '{}': {e}",
            path.display()
        ));
    }
    if raw_bytes.len() as u64 > MAX_TOKEN_FILE_BYTES {
        scrub_bytes(raw_bytes);
        return Err(format!(
            "--auth-token-file '{}' exceeds maximum size of {MAX_TOKEN_FILE_BYTES} bytes",
            path.display()
        ));
    }
    let raw_string = match String::from_utf8(raw_bytes) {
        Ok(s) => s,
        Err(utf8_err) => {
            scrub_bytes(utf8_err.into_bytes());
            return Err(format!(
                "--auth-token-file '{}' is not valid UTF-8",
                path.display()
            ));
        }
    };
    let parse_res = (|| {
        let raw = raw_string.strip_prefix('\u{feff}').unwrap_or(&raw_string);
        let mut tokens = Vec::new();
        for line in raw.lines() {
            let without_comment = strip_whitespace_delimited_comment(line);
            for part in without_comment.split(',') {
                let trimmed = part.trim();
                if trimmed.is_empty() {
                    continue;
                }
                if let Err(e) = validate_token_syntax(trimmed) {
                    scrub_tokens(&mut tokens);
                    return Err(format!(
                        "invalid token in --auth-token-file '{}': {e}",
                        path.display()
                    ));
                }
                tokens.push(trimmed.to_string());
            }
        }
        if tokens.is_empty() {
            return Err(format!(
                "--auth-token-file '{}' contains no non-empty tokens",
                path.display()
            ));
        }
        Ok(tokens)
    })();
    scrub_bytes(raw_string.into_bytes());
    parse_res
}

fn parse_host_ip(host: &str) -> Result<Option<IpAddr>, String> {
    let trimmed = host.trim();
    if trimmed.is_empty() {
        return Err("--host must not be empty".to_string());
    }
    if trimmed.starts_with('[') || trimmed.ends_with(']') {
        let inner = trimmed
            .strip_prefix('[')
            .and_then(|s| s.strip_suffix(']'))
            .ok_or_else(|| format!("invalid bracketed IPv6 --host '{trimmed}'"))?;
        let v6 = inner
            .parse::<Ipv6Addr>()
            .map_err(|_| format!("invalid bracketed IPv6 --host '{trimmed}'"))?;
        return Ok(Some(IpAddr::V6(v6)));
    }
    if let Ok(ip) = trimmed.parse::<IpAddr>() {
        return Ok(Some(ip));
    }
    Ok(None)
}

/// Syntactic check returning `true` when `host` is `localhost`, `127.0.0.0/8`, or `::1` /
/// `[::1]`. Callers binding sockets must also use [`resolve_listen_addrs`] to verify that DNS
/// or `/etc/hosts` resolution yields exclusively loopback addresses when running unauthenticated.
pub fn is_loopback_host(host: &str) -> bool {
    match parse_host_ip(host) {
        Ok(Some(ip)) => ip.is_loopback(),
        Ok(None) => host.trim().eq_ignore_ascii_case("localhost"),
        Err(_) => false,
    }
}

const BIND_AUTH_REMEDIATION: &str = "configure --auth-token-file (JJ_CC_SERVER_AUTH_TOKEN_FILE), \
    --auth-token (JJ_CC_SERVER_AUTH_TOKEN), --auth-oidc-issuer (JJ_CC_AUTH_OIDC_ISSUER), \
    or --auth-k8s-tokenreview-url (JJ_CC_AUTH_K8S_TOKENREVIEW_URL), or pass \
    --allow-unauthenticated (JJ_CC_ALLOW_UNAUTHENTICATED=true)";

/// Refuses to expose an unauthenticated gRPC listener on a non-loopback `--host` unless the
/// operator explicitly opts in via `--allow-unauthenticated` (`JJ_CC_ALLOW_UNAUTHENTICATED`).
pub fn enforce_bind_auth_policy(
    host: &str,
    auth_enabled: bool,
    allow_unauthenticated: bool,
) -> Result<(), String> {
    let _ = parse_host_ip(host)?;
    if auth_enabled || is_loopback_host(host) || allow_unauthenticated {
        return Ok(());
    }
    Err(format!(
        "refusing to bind non-loopback host '{host}' without authentication; \
         {BIND_AUTH_REMEDIATION}"
    ))
}

/// Resolves `(host, port)` into concrete listener [`SocketAddr`]s (supporting unbracketed or
/// bracketed IPv6 literals as well as hostnames) and enforces that every resolved address is
/// loopback unless authentication is enabled or `--allow-unauthenticated` is set.
pub async fn resolve_listen_addrs(
    host: &str,
    port: u16,
    auth_enabled: bool,
    allow_unauthenticated: bool,
) -> Result<Vec<SocketAddr>, String> {
    enforce_bind_auth_policy(host, auth_enabled, allow_unauthenticated)?;
    let addrs = match parse_host_ip(host)? {
        Some(ip) => vec![SocketAddr::new(ip, port)],
        None => {
            let trimmed = host.trim();
            let resolved: Vec<SocketAddr> = tokio::net::lookup_host((trimmed, port))
                .await
                .map_err(|e| format!("failed to resolve --host '{trimmed}:{port}': {e}"))?
                .collect();
            if resolved.is_empty() {
                return Err(format!(
                    "--host '{trimmed}' resolved to zero socket addresses"
                ));
            }
            resolved
        }
    };
    if !auth_enabled && !allow_unauthenticated && !addrs.iter().all(|a| a.ip().is_loopback()) {
        return Err(format!(
            "refusing to bind host '{}' without authentication because it resolved to \
             non-loopback address(es) {addrs:?}; {BIND_AUTH_REMEDIATION}",
            host.trim()
        ));
    }
    Ok(addrs)
}

impl BearerAuthInterceptor {
    pub fn validate_request<T>(&self, req: Request<T>) -> Result<Request<T>, Status> {
        let Some(expected_tokens) = &self.tokens else {
            return Ok(req);
        };

        let mut auth_headers = req.metadata().get_all("authorization").iter();
        let raw_header = match (auth_headers.next(), auth_headers.next()) {
            (Some(header), None) => header.to_str().map_err(|_| {
                Status::unauthenticated("authorization metadata header must be valid ASCII")
            })?,
            (Some(_), Some(_)) => {
                return Err(Status::unauthenticated(
                    "multiple authorization metadata headers are not allowed",
                ));
            }
            (None, _) => {
                return Err(Status::unauthenticated(
                    "missing authorization metadata header",
                ));
            }
        };

        let provided = extract_bearer_token(raw_header).ok_or_else(|| {
            Status::unauthenticated("authorization header must use valid Bearer <token> format")
        })?;
        let provided_digest: [u8; 32] = Sha256::digest(provided.as_bytes()).into();

        // Fold across every configured digest with bitwise `|` so neither byte mismatches
        // nor which configured token matched can short-circuit.
        let matched = expected_tokens
            .iter()
            .fold(Choice::from(0u8), |acc, expected| {
                acc | provided_digest.ct_eq(expected)
            });

        if bool::from(matched) {
            Ok(req)
        } else {
            Err(Status::unauthenticated("invalid bearer token"))
        }
    }
}

impl Interceptor for BearerAuthInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        self.validate_request(request)
    }
}

fn extract_bearer_token(header_val: &str) -> Option<&str> {
    // Authentication schemes are case-insensitive (RFC 7235 section 2.1).
    let (scheme, rest) = header_val.trim().split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = rest.trim();
    (!token.is_empty()
        && token.len() <= MAX_BEARER_TOKEN_BYTES
        && token.bytes().all(|b| b.is_ascii_graphic() && b != b'#'))
    .then_some(token)
}

pub(super) fn extract_http_bearer_token(headers: &http::HeaderMap) -> Result<&str, Status> {
    let mut auth_headers = headers.get_all(http::header::AUTHORIZATION).iter();
    let raw_header = match (auth_headers.next(), auth_headers.next()) {
        (Some(header), None) => header.to_str().map_err(|_| {
            Status::unauthenticated("authorization metadata header must be valid ASCII")
        })?,
        (Some(_), Some(_)) => {
            return Err(Status::unauthenticated(
                "multiple authorization metadata headers are not allowed",
            ));
        }
        (None, _) => {
            return Err(Status::unauthenticated(
                "missing authorization metadata header",
            ));
        }
    };

    let (scheme, rest) = raw_header.trim().split_once(' ').ok_or_else(|| {
        Status::unauthenticated("authorization header must use valid Bearer <token> format")
    })?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return Err(Status::unauthenticated(
            "authorization header must use valid Bearer <token> format",
        ));
    }
    let token = rest.trim();
    if token.is_empty()
        || token.len() > MAX_AUTH_TOKEN_BYTES
        || !token.bytes().all(|b| b.is_ascii_graphic())
    {
        return Err(Status::unauthenticated(
            "authorization header must use valid Bearer <token> format",
        ));
    }
    Ok(token)
}
