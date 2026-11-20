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
//! Applied to `BackendService` and `OpStoreService` while leaving `tonic_health` open so
//! Kubernetes, GKE, GCE, and Cloud Run readiness probes succeed without credentials.

use std::io::Read as _;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

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
                normalized.push(sha256_digest(trimmed.as_bytes()));
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

/// Reads newline- or comma-separated graphic-ASCII Bearer tokens from `path` (up to 64 KiB),
/// stripping whitespace-delimited `#` comments and blank entries while rejecting embedded `#`
/// characters so secrets are never silently truncated.
fn scrub_bytes(mut bytes: Vec<u8>) {
    bytes.fill(0);
    std::hint::black_box(bytes.as_slice());
}

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
                validate_token_syntax(trimmed).map_err(|e| {
                    format!(
                        "invalid token in --auth-token-file '{}': {e}",
                        path.display()
                    )
                })?;
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
        "refusing to bind non-loopback host '{host}' without authentication; configure \
         --auth-token-file / --auth-token (JJ_CC_SERVER_AUTH_TOKEN) or pass \
         --allow-unauthenticated (JJ_CC_ALLOW_UNAUTHENTICATED=true)"
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
             non-loopback address(es) {addrs:?}; configure --auth-token-file / --auth-token \
             (JJ_CC_SERVER_AUTH_TOKEN) or pass --allow-unauthenticated",
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
        let provided_digest = sha256_digest(provided.as_bytes());

        let mut matched = 0u8;
        for expected in expected_tokens.iter() {
            matched |= constant_time_eq_32(&provided_digest, expected);
        }

        if matched == 1 {
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

fn sha256_compress_block(h: &mut [u32; 8], chunk: &[u8; 64]) {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut w = [0u32; 64];
    for (i, word) in chunk.as_chunks::<4>().0.iter().enumerate() {
        w[i] = u32::from_be_bytes(*word);
    }
    for i in 16..64 {
        let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
        let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
        w[i] = w[i - 16]
            .wrapping_add(s0)
            .wrapping_add(w[i - 7])
            .wrapping_add(s1);
    }
    let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = *h;
    for i in 0..64 {
        let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
        let ch = (e & f) ^ ((!e) & g);
        let t1 = hh
            .wrapping_add(s1)
            .wrapping_add(ch)
            .wrapping_add(K[i])
            .wrapping_add(w[i]);
        let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
        let maj = (a & b) ^ (a & c) ^ (b & c);
        let t2 = s0.wrapping_add(maj);
        hh = g;
        g = f;
        f = e;
        e = d.wrapping_add(t1);
        d = c;
        c = b;
        b = a;
        a = t1.wrapping_add(t2);
    }
    for (state, delta) in h.iter_mut().zip([a, b, c, d, e, f, g, hh]) {
        *state = state.wrapping_add(delta);
    }
    w.fill(0);
    std::hint::black_box(&w);
}

fn sha256_digest(input: &[u8]) -> [u8; 32] {
    // Stack-streamed SHA-256 (FIPS 180-4) so constant-time comparison always operates
    // on fixed 32-byte digests without copying raw caller tokens onto the heap.
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    let (full_chunks, rem) = input.as_chunks::<64>();
    for chunk in full_chunks {
        sha256_compress_block(&mut h, chunk);
    }
    let mut tail = [0u8; 64];
    tail[..rem.len()].copy_from_slice(rem);
    tail[rem.len()] = 0x80;
    let bit_len = (input.len() as u64).wrapping_mul(8).to_be_bytes();
    if rem.len() < 56 {
        tail[56..64].copy_from_slice(&bit_len);
        sha256_compress_block(&mut h, &tail);
    } else {
        sha256_compress_block(&mut h, &tail);
        let mut tail2 = [0u8; 64];
        tail2[56..64].copy_from_slice(&bit_len);
        sha256_compress_block(&mut h, &tail2);
    }
    tail.fill(0);
    std::hint::black_box(&tail);

    let mut out = [0u8; 32];
    for (i, word) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

fn constant_time_eq_32(a: &[u8; 32], b: &[u8; 32]) -> u8 {
    let mut diff = 0u8;
    for (&x, &y) in a.iter().zip(b.iter()) {
        diff |= std::hint::black_box(x ^ y);
    }
    u8::from(std::hint::black_box(diff) == 0)
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
    fn sha256_matches_fips_180_4_test_vectors() {
        let hex_of = |b: &[u8]| -> String {
            sha256_digest(b)
                .iter()
                .map(|x| format!("{x:02x}"))
                .collect()
        };
        assert_eq!(
            hex_of(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            hex_of(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            hex_of(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnop"),
            "aa353e009edbaebfc6e494c8d847696896cb8b398e0173a4b5c1b636292d87c7"
        );
        assert_eq!(
            hex_of(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
        assert_eq!(
            hex_of(&[b'a'; 64]),
            "ffe054fe7ae0cb6dc65c3af9b61d5209f439851db43d0ba5997337df154668eb"
        );
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
            assert!(enforce_bind_auth_policy(remote, false, false).is_err());
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

        let spaced = TempTokenFile::new("tok-with space\n");
        let err = load_tokens_from_file(&spaced.0).unwrap_err();
        assert!(!err.contains("tok-with"));

        let oversized_content = "a".repeat((MAX_TOKEN_FILE_BYTES as usize) + 1);
        let oversized = TempTokenFile::new(&oversized_content);
        let size_err = load_tokens_from_file(&oversized.0).unwrap_err();
        assert!(size_err.contains("exceeds maximum size"), "{size_err}");
    }
}
