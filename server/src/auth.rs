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

//! Optional constant-time Bearer token authentication interceptor for `jj-cc-server`.
//!
//! # Authority and Crash-Safety Ordering
//! `BearerAuthInterceptor` runs at the `tonic` service boundary before any `Store` read or
//! write executes, rejecting unauthenticated requests with `Code::Unauthenticated` prior to
//! backend storage access.
//!
//! # Concurrency and Merge Invariants
//! Supports multiple active tokens simultaneously (`--auth-token`, `--auth-token-file`, or
//! environment equivalents) so operators can rotate credentials across fleets of agents
//! without downtime. Tokens are hashed to fixed-length digests and every configured token
//! is compared in constant time so candidate length is not leaked. Binding to a non-loopback
//! `--host` fails closed unless authentication is enabled or `--allow-unauthenticated` is set.
//!
//! # Cross-Process and RPC Boundaries
//! Applied to `BackendService`, `OpStoreService`, and `WorkspaceService` while leaving
//! `tonic_health` open so Kubernetes, GKE, GCE, and Cloud Run readiness probes succeed
//! without credentials.

use std::io::Read as _;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use sha2::{Digest as _, Sha256};
use subtle::{Choice, ConstantTimeEq as _};
use tonic::{Request, Status, service::Interceptor};

/// Public upper bound on a single Bearer token's byte length so synchronous SHA-256 hashing
/// inside `Interceptor::call` cannot be abused with multi-megabyte headers.
pub const MAX_BEARER_TOKEN_BYTES: usize = 4096;

const MAX_TOKEN_FILE_BYTES: u64 = 64 * 1024;

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
}

fn scrub_bytes(mut bytes: Vec<u8>) {
    bytes.fill(0);
    std::hint::black_box(bytes.as_slice());
}

/// Best-effort zeroization of plaintext token buffers before dropping them.
pub fn scrub_tokens(tokens: &mut Vec<String>) {
    for tok in tokens.drain(..) {
        scrub_bytes(tok.into_bytes());
    }
}

/// Reads newline- or comma-separated graphic-ASCII Bearer tokens from `path` (up to 64 KiB),
/// stripping whitespace-delimited `#` comments and blank entries while rejecting embedded `#`
/// characters so secrets are never silently truncated.
pub fn load_tokens_from_file(path: &std::path::Path) -> Result<Vec<String>, String> {
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

const BIND_AUTH_REMEDIATION: &str = "configure --auth-token-file (JJ_CC_SERVER_AUTH_TOKEN_FILE) \
    or --auth-token (JJ_CC_SERVER_AUTH_TOKEN), or pass --allow-unauthenticated \
    (JJ_CC_ALLOW_UNAUTHENTICATED=true)";

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

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    struct TempTokenFile(std::path::PathBuf);
    impl TempTokenFile {
        fn new(contents: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "jj-cc-auth-test-{}-{}.txt",
                std::process::id(),
                uuid::Uuid::new_v4()
            ));
            std::fs::write(&path, contents).unwrap();
            Self(path)
        }
    }
    impl Drop for TempTokenFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    #[test]
    fn disabled_interceptor_allows_unauthenticated_requests() {
        let mut auth = BearerAuthInterceptor::from_tokens(Vec::<String>::new());
        assert!(!auth.is_enabled());
        assert!(auth.call(Request::new(())).is_ok());
    }

    #[test]
    fn enabled_interceptor_enforces_tokens_and_supports_rotation() {
        let mut auth = BearerAuthInterceptor::from_tokens([" token-v1 ", "token-v2"]);
        assert!(auth.is_enabled());

        let err = auth.call(Request::new(())).unwrap_err();
        assert_eq!(err.code(), Code::Unauthenticated);

        let mut wrong = Request::new(());
        wrong
            .metadata_mut()
            .insert("authorization", "Bearer wrong-secret".parse().unwrap());
        assert_eq!(auth.call(wrong).unwrap_err().code(), Code::Unauthenticated);

        for valid_token in ["token-v1", "token-v2"] {
            let mut ok_req = Request::new(());
            ok_req.metadata_mut().insert(
                "authorization",
                format!("Bearer {valid_token}").parse().unwrap(),
            );
            assert!(auth.call(ok_req).is_ok());
        }
    }

    #[test]
    fn bearer_scheme_is_case_insensitive_and_rejects_internal_spaces_and_hashes() {
        let mut auth = BearerAuthInterceptor::from_tokens(["secret-token"]);
        for header in [
            "Bearer secret-token",
            "bearer secret-token",
            "BEARER  secret-token",
        ] {
            let mut req = Request::new(());
            req.metadata_mut()
                .insert("authorization", header.parse().unwrap());
            assert!(auth.call(req).is_ok(), "{header}");
        }
        for header in [
            "Basic secret-token",
            "Bearersecret-token",
            "Bearer ",
            "Bearer secret token",
            "Bearer secret#token",
        ] {
            let mut req = Request::new(());
            req.metadata_mut()
                .insert("authorization", header.parse().unwrap());
            assert_eq!(
                auth.call(req).unwrap_err().code(),
                Code::Unauthenticated,
                "{header}"
            );
        }
    }

    #[test]
    fn try_from_tokens_rejects_whitespace_hashes_and_oversized_tokens_without_leaking() {
        assert!(
            !BearerAuthInterceptor::try_from_tokens(["", " ", "\u{feff} , "])
                .unwrap()
                .is_enabled()
        );
        assert!(
            BearerAuthInterceptor::try_from_tokens(["\u{feff}clean-token"])
                .unwrap()
                .is_enabled()
        );

        let oversized = "x".repeat(MAX_BEARER_TOKEN_BYTES + 1);
        for bad in [
            "Bearer leak-me-123",
            "leak-me-123 extra",
            "leak-me-123\t1",
            "K#leak-me-123",
            oversized.as_str(),
        ] {
            let err = BearerAuthInterceptor::try_from_tokens([bad]).unwrap_err();
            assert!(!err.contains("leak-me-123"), "error leaked secret: {err}");
        }

        let auth = BearerAuthInterceptor::from_tokens(["super-secret-value"]);
        let debug = format!("{auth:?}");
        assert!(!debug.contains("super-secret-value"), "{debug}");
        assert!(debug.contains("token_count: 1"), "{debug}");
    }

    #[test]
    fn duplicate_authorization_headers_are_rejected() {
        let mut auth = BearerAuthInterceptor::from_tokens(["secret-token"]);
        let mut req = Request::new(());
        req.metadata_mut()
            .append("authorization", "Bearer secret-token".parse().unwrap());
        req.metadata_mut()
            .append("authorization", "Bearer wrong-token".parse().unwrap());
        assert_eq!(auth.call(req).unwrap_err().code(), Code::Unauthenticated);
    }

    #[test]
    fn bind_auth_policy_validates_brackets_and_requires_auth_on_non_loopback() {
        for loopback in ["127.0.0.1", "127.0.1.1", "::1", "[::1]", "localhost"] {
            assert!(is_loopback_host(loopback), "{loopback}");
            assert!(enforce_bind_auth_policy(loopback, false, false).is_ok());
        }
        for invalid_bracket in ["[127.0.0.1]", "[localhost]", "[::1", "::1]", ""] {
            assert!(!is_loopback_host(invalid_bracket), "{invalid_bracket}");
            assert!(enforce_bind_auth_policy(invalid_bracket, true, true).is_err());
        }
        for remote in ["0.0.0.0", "::", "[::]", "10.0.0.5", "my-server.internal"] {
            assert!(!is_loopback_host(remote), "{remote}");
            let err = enforce_bind_auth_policy(remote, false, false).unwrap_err();
            for expected in [
                "--auth-token-file",
                "JJ_CC_SERVER_AUTH_TOKEN_FILE",
                "--auth-token",
                "JJ_CC_SERVER_AUTH_TOKEN",
                "--allow-unauthenticated",
                "JJ_CC_ALLOW_UNAUTHENTICATED=true",
            ] {
                assert!(err.contains(expected), "missing {expected:?} in: {err}");
            }
            assert!(enforce_bind_auth_policy(remote, true, false).is_ok());
            assert!(enforce_bind_auth_policy(remote, false, true).is_ok());
        }
    }

    #[test]
    fn load_tokens_from_file_strips_spaced_comments_and_rejects_embedded_hash_secrets() {
        let valid = TempTokenFile::new(
            "\u{feff}# rotation file\n  tok-a , # inline comment after comma\n tok-b , tok-c # trailing\n",
        );
        let parsed = load_tokens_from_file(&valid.0).unwrap();
        assert_eq!(parsed, vec!["tok-a", "tok-b", "tok-c"]);

        // Embedded `#` inside a secret must fail closed rather than silently truncating to `"K"`.
        let embedded_hash = TempTokenFile::new("K#secret-remainder-xyz\n");
        let hash_err = load_tokens_from_file(&embedded_hash.0).unwrap_err();
        assert!(!hash_err.contains("secret-remainder-xyz"), "{hash_err}");

        let empty = TempTokenFile::new("  # comment only \n  , # inline only \n");
        assert!(load_tokens_from_file(&empty.0).is_err());

        let spaced = TempTokenFile::new("tok-first-ok\ntok-with space\n");
        let err = load_tokens_from_file(&spaced.0).unwrap_err();
        assert!(!err.contains("tok-first-ok") && !err.contains("tok-with"));

        let oversized_content = "a".repeat((MAX_TOKEN_FILE_BYTES as usize) + 1);
        let oversized = TempTokenFile::new(&oversized_content);
        let size_err = load_tokens_from_file(&oversized.0).unwrap_err();
        assert!(size_err.contains("exceeds maximum size"), "{size_err}");

        let mut tokens = vec!["secret-1".to_string(), "secret-2".to_string()];
        scrub_tokens(&mut tokens);
        assert!(tokens.is_empty());
    }
}
