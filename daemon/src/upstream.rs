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

//! Persistent upstream HTTP/2 channel pool and metadata header forwarding for jj-cc-daemon.
//!
//! # Authority and Crash-Safety Ordering
//! `UpstreamPool` holds stateless HTTP/2 transport channels keyed by normalized upstream
//! `server_url` strings; all persistent state authority remains on jj-cc-server and
//! [`crate::cache::TwoTierCache`].
//!
//! # Concurrency and Merge Invariants
//! Tonic [`Channel`] handles multiplex concurrent HTTP/2 streams without external locking
//! once connected; `channels` keeps a bounded LRU cache of connections per normalized URL
//! and coalesces concurrent cold connects per origin via singleflight.
//!
//! # Cross-Process and RPC Boundaries
//! Inbound calls from cc-lib or jj-vfs pass `x-jj-cc-upstream` metadata so a single
//! per-user daemon routes across multiple local and cloud Commit Cloud endpoints while
//! forwarding each request's own `authorization` Bearer header (and `grpc-timeout` deadline
//! on non-coalesced RPCs). The daemon holds no credentials of its own.

use std::collections::{HashMap, HashSet};
use std::net::Ipv6Addr;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

use lru::LruCache;
use rustls::pki_types::{IpAddr as PkiIpAddr, ServerName};
use tokio::sync::watch;
use tonic::metadata::{Ascii, MetadataMap, MetadataValue};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint, Uri};
use tonic::{Request, Status};

use crate::cache::clone_status;

/// Request metadata header set by `cc-lib` when routing through a shared per-user daemon
/// so one local daemon can proxy multiple workspaces targeting distinct `server_url`s.
pub const UPSTREAM_HEADER: &str = "x-jj-cc-upstream";

/// Standard gRPC over HTTP/2 deadline metadata header.
pub const GRPC_TIMEOUT_HEADER: &str = "grpc-timeout";

const MAX_UPSTREAM_CHANNELS: NonZeroUsize = NonZeroUsize::new(64).unwrap();

static RUSTLS_PROVIDER_INIT: Once = Once::new();

fn ensure_rustls_crypto_provider() {
    RUSTLS_PROVIDER_INIT.call_once(|| {
        // Workspace feature unification can enable both `ring` and `aws_lc_rs` on rustls 0.23,
        // causing `ClientConfig::builder()` inside tonic to panic without an explicit default.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

#[derive(Clone)]
enum ConnectOutcome {
    Ok(Channel),
    Err(Arc<Status>),
}

type ConnectInflightMap = Mutex<HashMap<String, watch::Receiver<Option<ConnectOutcome>>>>;

struct ConnectGuard<'a> {
    inflight: &'a ConnectInflightMap,
    key: String,
    result: watch::Sender<Option<ConnectOutcome>>,
}

impl ConnectGuard<'_> {
    fn publish_ok(&self, channel: &Channel) {
        self.result
            .send_replace(Some(ConnectOutcome::Ok(channel.clone())));
    }

    fn publish_err(&self, err: &Status) {
        self.result
            .send_replace(Some(ConnectOutcome::Err(Arc::new(clone_status(err)))));
    }
}

impl Drop for ConnectGuard<'_> {
    fn drop(&mut self) {
        self.inflight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.key);
    }
}

enum ConnectFlight<'a> {
    Leader(ConnectGuard<'a>),
    Waiter(watch::Receiver<Option<ConnectOutcome>>),
}

pub struct UpstreamPool {
    default_upstream: Option<String>,
    allowed_origins: Option<HashSet<String>>,
    channels: Mutex<LruCache<String, Channel>>,
    inflight: ConnectInflightMap,
}

/// Extracts a single valid ASCII `grpc-timeout` header (`<1..=8 digits><H|M|S|m|u|n>`).
/// Duplicate or malformed headers are dropped rather than forwarded.
fn extract_grpc_timeout(metadata: &MetadataMap) -> Option<MetadataValue<Ascii>> {
    let mut values = metadata.get_all(GRPC_TIMEOUT_HEADER).iter();
    let val = match (values.next(), values.next()) {
        (Some(val), None) => val,
        _ => return None,
    };
    let s = val.to_str().ok()?;
    if !(2..=9).contains(&s.len()) {
        return None;
    }
    let (digits, unit) = s.split_at(s.len() - 1);
    if !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if !matches!(unit, "H" | "M" | "S" | "m" | "u" | "n") {
        return None;
    }
    Some(val.clone())
}

/// Extracts the single optional ASCII `authorization` metadata header, rejecting duplicate
/// headers or non-ASCII values with `Code::Unauthenticated`.
pub fn extract_auth_header(metadata: &MetadataMap) -> Result<Option<String>, Status> {
    let mut values = metadata.get_all("authorization").iter();
    match (values.next(), values.next()) {
        (Some(_), Some(_)) => Err(Status::unauthenticated(
            "multiple authorization metadata headers are not allowed",
        )),
        (Some(val), None) => val.to_str().map(|s| Some(s.to_string())).map_err(|_| {
            Status::unauthenticated("authorization metadata header must be valid ASCII")
        }),
        (None, _) => Ok(None),
    }
}

pub fn normalize_origin(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.contains('#') {
        return Err("upstream URI must not include a fragment".to_string());
    }
    let uri: Uri = trimmed
        .parse()
        .map_err(|e| format!("invalid upstream URI: {e}"))?;
    let scheme = uri
        .scheme_str()
        .ok_or_else(|| "upstream URI is missing scheme (expected http or https)".to_string())?
        .to_ascii_lowercase();
    if scheme != "http" && scheme != "https" {
        return Err("unsupported upstream scheme (expected http or https)".to_string());
    }
    let auth_part = uri
        .authority()
        .ok_or_else(|| "upstream URI is missing host authority".to_string())?;
    if auth_part.as_str().contains('@') {
        return Err("upstream URI must not include userinfo credentials".to_string());
    }
    if let Some(pq) = uri.path_and_query()
        && pq.as_str() != "/"
    {
        return Err("upstream URI must not include a path or query".to_string());
    }
    let raw_host = auth_part.host();
    if raw_host.is_empty() {
        return Err("upstream URI is missing host".to_string());
    }
    let Some(rest) = auth_part.as_str().strip_prefix(raw_host) else {
        return Err("upstream URI has malformed authority".to_string());
    };
    let port = if rest.is_empty() {
        None
    } else if let Some(port_str) = rest.strip_prefix(':') {
        if port_str.is_empty() || !port_str.bytes().all(|b| b.is_ascii_digit()) {
            return Err("upstream URI has invalid port".to_string());
        }
        let parsed = port_str
            .parse::<u16>()
            .map_err(|_| "upstream URI has out-of-range port".to_string())?;
        if parsed == 0 {
            return Err("upstream URI has invalid port 0".to_string());
        }
        Some(parsed)
    } else {
        return Err("upstream URI has unexpected trailing authority characters".to_string());
    };

    let host = if let Some(bracketed) = raw_host.strip_prefix('[').and_then(|s| s.strip_suffix(']'))
    {
        let addr: Ipv6Addr = bracketed
            .parse()
            .map_err(|e| format!("upstream URI has invalid IPv6 host: {e}"))?;
        format!("[{addr}]")
    } else if raw_host.contains('[') || raw_host.contains(']') {
        return Err("upstream URI has malformed host".to_string());
    } else {
        let lower = raw_host.to_ascii_lowercase();
        if !matches!(
            ServerName::try_from(lower.as_str()),
            Ok(ServerName::DnsName(_) | ServerName::IpAddress(PkiIpAddr::V4(_)))
        ) {
            return Err("upstream URI has invalid host".to_string());
        }
        lower
    };

    let default_port = if scheme == "https" { 443 } else { 80 };
    let authority = match port {
        Some(p) if p != default_port => format!("{host}:{p}"),
        _ => host,
    };
    Ok(format!("{scheme}://{authority}"))
}

impl UpstreamPool {
    pub fn new(default_upstream: Option<String>) -> Self {
        Self {
            default_upstream,
            allowed_origins: None,
            channels: Mutex::new(LruCache::new(MAX_UPSTREAM_CHANNELS)),
            inflight: Mutex::new(HashMap::new()),
        }
    }

    /// Creates a pool with an optional upstream origin allowlist.
    ///
    /// Passing an empty `allowed_upstreams` iterator disables allowlist enforcement so any valid
    /// `http://` or `https://` upstream origin is permitted. Passing one or more elements that
    /// contain only whitespace or empty comma-separated entries returns an error rather than
    /// silently disabling enforcement.
    ///
    /// Each non-empty comma-separated entry is canonicalized with [`normalize_origin`]. When the
    /// allowlist is non-empty and `default_upstream` is set (non-empty after trimming), the
    /// normalized `default_upstream` is implicitly included in the allowed set so fallback routing
    /// to the configured default upstream always succeeds.
    pub fn with_allowlist<I, S>(
        default_upstream: Option<String>,
        allowed_upstreams: I,
    ) -> Result<Self, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let normalized_default = match default_upstream.as_deref().map(str::trim) {
            Some(def) if !def.is_empty() => Some(normalize_origin(def)?),
            _ => None,
        };
        let mut origins = HashSet::new();
        let mut any_entry = false;
        for raw in allowed_upstreams {
            any_entry = true;
            for part in raw.as_ref().split(',') {
                let trimmed = part.trim();
                if !trimmed.is_empty() {
                    origins.insert(normalize_origin(trimmed)?);
                }
            }
        }
        if any_entry && origins.is_empty() {
            return Err("upstream allowlist contains no valid upstream URLs".to_string());
        }
        if !origins.is_empty()
            && let Some(def) = &normalized_default
        {
            origins.insert(def.clone());
        }
        Ok(Self {
            default_upstream: normalized_default,
            allowed_origins: (!origins.is_empty()).then_some(origins),
            channels: Mutex::new(LruCache::new(MAX_UPSTREAM_CHANNELS)),
            inflight: Mutex::new(HashMap::new()),
        })
    }

    pub fn resolve_target_url(&self, metadata: &MetadataMap) -> Result<String, Status> {
        let mut header_values = metadata.get_all(UPSTREAM_HEADER).iter();
        let header_candidate = match (header_values.next(), header_values.next()) {
            (Some(_), Some(_)) => {
                return Err(Status::invalid_argument(format!(
                    "duplicate {UPSTREAM_HEADER} metadata headers are not allowed"
                )));
            }
            (Some(val), None) => {
                let s = val
                    .to_str()
                    .map_err(|e| {
                        Status::invalid_argument(format!("invalid {UPSTREAM_HEADER}: {e}"))
                    })?
                    .trim();
                (!s.is_empty()).then(|| s.to_string())
            }
            (None, _) => None,
        };

        let candidate = header_candidate
            .or_else(|| {
                self.default_upstream
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(ToOwned::to_owned)
            })
            .ok_or_else(|| {
                Status::invalid_argument(
                    "missing target server URL (no x-jj-cc-upstream metadata and no default upstream configured)",
                )
            })?;

        let normalized = normalize_origin(&candidate).map_err(Status::invalid_argument)?;
        if let Some(allowed) = &self.allowed_origins
            && !allowed.contains(&normalized)
        {
            return Err(Status::permission_denied(format!(
                "upstream '{normalized}' is not permitted by allowlist"
            )));
        }
        Ok(normalized)
    }

    fn join_connect(&self, key: &str) -> ConnectFlight<'_> {
        let mut map = self.inflight.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(existing) = map.get(key) {
            return ConnectFlight::Waiter(existing.clone());
        }
        let (result, receiver) = watch::channel(None);
        map.insert(key.to_string(), receiver);
        ConnectFlight::Leader(ConnectGuard {
            inflight: &self.inflight,
            key: key.to_string(),
            result,
        })
    }

    pub async fn channel_for(&self, metadata: &MetadataMap) -> Result<Channel, Status> {
        let target_url = self.resolve_target_url(metadata)?;
        let guard = loop {
            if let Some(existing) = self
                .channels
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&target_url)
                .cloned()
            {
                return Ok(existing);
            }
            let mut waiter = match self.join_connect(&target_url) {
                ConnectFlight::Leader(guard) => break guard,
                ConnectFlight::Waiter(rx) => rx,
            };
            match waiter.wait_for(Option::is_some).await.map(|v| (*v).clone()) {
                Ok(Some(ConnectOutcome::Ok(channel))) => return Ok(channel),
                Ok(Some(ConnectOutcome::Err(err))) => return Err(clone_status(&err)),
                _ => {
                    // Leader was cancelled/dropped before publishing; retry so a surviving
                    // caller can observe a completed connect or lead a new attempt.
                }
            }
        };

        // Re-check after winning leadership in case a prior connect finished right before
        // `join_connect`.
        if let Some(existing) = self
            .channels
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&target_url)
            .cloned()
        {
            guard.publish_ok(&existing);
            return Ok(existing);
        }

        match connect_upstream(&target_url).await {
            Ok(channel) => {
                let channel = {
                    let mut channels = self.channels.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(existing) = channels.get(&target_url).cloned() {
                        existing
                    } else {
                        channels.put(target_url, channel.clone());
                        channel
                    }
                };
                guard.publish_ok(&channel);
                Ok(channel)
            }
            Err(err) => {
                guard.publish_err(&err);
                Err(err)
            }
        }
    }

    fn prepare_outbound_inner<T>(
        incoming_meta: &MetadataMap,
        payload: T,
        forward_timeout: bool,
    ) -> Result<Request<T>, Status> {
        let mut req = Request::new(payload);
        if let Some(auth) = extract_auth_header(incoming_meta)? {
            let mut val: MetadataValue<Ascii> = auth.parse().map_err(|_| {
                Status::unauthenticated("authorization metadata header must be valid ASCII")
            })?;
            val.set_sensitive(true);
            req.metadata_mut().insert("authorization", val);
        }
        if forward_timeout && let Some(timeout) = extract_grpc_timeout(incoming_meta) {
            req.metadata_mut().insert(GRPC_TIMEOUT_HEADER, timeout);
        }
        Ok(req)
    }

    pub fn prepare_outbound<T>(
        &self,
        incoming_meta: &MetadataMap,
        payload: T,
    ) -> Result<Request<T>, Status> {
        Self::prepare_outbound_inner(incoming_meta, payload, true)
    }

    /// Prepares an outbound request for a shared singleflight fetch: forwards the caller's
    /// `authorization` header while omitting per-caller `grpc-timeout` so a short-deadline leader
    /// cannot fail longer-deadline coalesced waiters with `DEADLINE_EXCEEDED`.
    pub fn prepare_coalesced_outbound<T>(
        &self,
        incoming_meta: &MetadataMap,
        payload: T,
    ) -> Result<Request<T>, Status> {
        Self::prepare_outbound_inner(incoming_meta, payload, false)
    }
}

async fn connect_upstream(target_url: &str) -> Result<Channel, Status> {
    // gRPC servers reject keepalive pings sent more often than every 5 minutes (and any
    // ping on an idle connection) with GOAWAY `too_many_pings`.
    let mut endpoint = Endpoint::from_shared(target_url.to_string())
        .map_err(|e| Status::invalid_argument(format!("invalid upstream URL {target_url}: {e}")))?
        .tcp_nodelay(true)
        .connect_timeout(Duration::from_secs(10))
        .http2_keep_alive_interval(Duration::from_secs(300))
        .keep_alive_timeout(Duration::from_secs(20));

    if target_url
        .get(..8)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("https://"))
    {
        let target_url_for_err = target_url.to_string();
        endpoint = tokio::task::spawn_blocking(move || {
            ensure_rustls_crypto_provider();
            let mut tls = ClientTlsConfig::new().with_native_roots();
            if let Some(host) = endpoint.uri().host()
                && let Some(unbracketed) = host.strip_prefix('[').and_then(|s| s.strip_suffix(']'))
            {
                // Tonic forwards `uri.host()` verbatim to `ServerName::try_from`, which rejects
                // bracketed IPv6 literals (`[::1]`) unless the brackets are stripped here.
                tls = tls.domain_name(unbracketed.to_string());
            }
            endpoint.tls_config(tls).map_err(|e| {
                Status::internal(format!("TLS init failed for {target_url_for_err}: {e}"))
            })
        })
        .await
        .map_err(|e| Status::internal(format!("TLS init task failed for {target_url}: {e}")))??;
    }

    endpoint
        .connect()
        .await
        .map_err(|e| Status::unavailable(format!("failed to connect upstream {target_url}: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    #[test]
    fn prepare_outbound_forwards_only_the_callers_own_authorization() {
        let pool = UpstreamPool::new(Some("http://127.0.0.1:1".to_string()));
        let tokenless = pool.prepare_outbound(&MetadataMap::new(), ()).unwrap();
        assert!(tokenless.metadata().get("authorization").is_none());

        let mut incoming = MetadataMap::new();
        incoming.insert("authorization", "Bearer caller-token".parse().unwrap());
        incoming.insert(UPSTREAM_HEADER, "http://127.0.0.1:1".parse().unwrap());
        let forwarded = pool.prepare_outbound(&incoming, ()).unwrap();
        let auth_val = forwarded.metadata().get("authorization").unwrap();
        assert_eq!(auth_val.to_str().unwrap(), "Bearer caller-token");
        assert!(auth_val.is_sensitive());
        assert_eq!(
            forwarded.metadata().get_all("authorization").iter().count(),
            1
        );
        assert!(forwarded.metadata().get(UPSTREAM_HEADER).is_none());
    }

    #[test]
    fn prepare_outbound_rejects_duplicate_and_non_ascii_authorization() {
        let pool = UpstreamPool::new(Some("http://127.0.0.1:1".to_string()));

        let mut duplicate = MetadataMap::new();
        duplicate.append("authorization", "Bearer token-1".parse().unwrap());
        duplicate.append("authorization", "Bearer token-2".parse().unwrap());
        assert_eq!(
            pool.prepare_outbound(&duplicate, ()).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );

        let mut identical_dup = MetadataMap::new();
        identical_dup.append("authorization", "Bearer token-1".parse().unwrap());
        identical_dup.append("authorization", "Bearer token-1".parse().unwrap());
        assert_eq!(
            pool.prepare_outbound(&identical_dup, ())
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );

        let mut non_ascii = MetadataMap::new();
        let raw_val = MetadataValue::try_from(b"Bearer \x80".as_slice()).unwrap();
        non_ascii.insert("authorization", raw_val);
        assert_eq!(
            pool.prepare_outbound(&non_ascii, ()).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
    }

    #[test]
    fn allowlist_restricts_upstreams_and_normalizes_origins() {
        let pool = UpstreamPool::with_allowlist(
            None,
            ["https://CommitCloud.Example.com:8443/, http://127.0.0.1:8080"],
        )
        .unwrap();

        let mut ok_meta = MetadataMap::new();
        ok_meta.insert(
            UPSTREAM_HEADER,
            "https://commitcloud.example.com:8443/".parse().unwrap(),
        );
        assert_eq!(
            pool.resolve_target_url(&ok_meta).unwrap(),
            "https://commitcloud.example.com:8443"
        );

        let mut denied_meta = MetadataMap::new();
        denied_meta.insert(
            UPSTREAM_HEADER,
            "http://169.254.169.254:80".parse().unwrap(),
        );
        let err = pool.resolve_target_url(&denied_meta).unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn normalize_origin_rejects_malformed_ports_hosts_and_fragments() {
        let invalid = [
            "http://127.0.0.1:99999",
            "http://127.0.0.1:abc",
            "http://127.0.0.1:",
            "http://127.0.0.1:0",
            "http://127.0.0.1:+8080",
            "http://127.0.0.1:-80",
            "http://:8080",
            "http://",
            "http:///",
            "http://[::1]garbage",
            "http://[::1]8080",
            "http://[not-ipv6]:8080",
            "http://[]:8080",
            "http://127.0.0.1:8080#frag",
            "http://127.0.0.1:8080/#frag",
            "http://user@127.0.0.1:8080",
            "http://127.0.0.1:8080/path",
            "http://127.0.0.1:8080/?query=1",
            "ftp://127.0.0.1:8080",
            "https://bad!host",
            "http://bad!host:8080",
            "https://-bad.example.com",
            "https://bad-.example.com",
            "https://a..b.example.com",
            "https://.",
            "https://999.999.999.999",
            "https://bad%20host",
        ];
        for raw in invalid {
            assert!(
                normalize_origin(raw).is_err(),
                "expected normalize_origin({raw:?}) to fail"
            );
        }
    }

    #[test]
    fn normalize_origin_redacts_credentials_queries_and_fragments_from_errors() {
        let cases = [
            ("https://user:secret@host", &["user:secret", "secret"][..]),
            ("https://alice:secret@host", &["alice", "secret"][..]),
            ("https://host/?token=secret", &["token", "secret"][..]),
            ("https://host/#frag-secret", &["frag-secret", "secret"][..]),
            (
                "https://alice:secret@host/?token=query-secret#frag-secret",
                &["alice", "secret", "token", "query-secret", "frag-secret"][..],
            ),
            ("https://alice:secret", &["alice", "secret"][..]),
            (
                "ftp://alice:secret@host/?token=secret",
                &["alice", "secret", "token"][..],
            ),
        ];

        let pool = UpstreamPool::new(None);
        for (raw, forbidden) in cases {
            let err = normalize_origin(raw).unwrap_err();
            for needle in forbidden {
                assert!(
                    !err.contains(needle),
                    "normalize_origin({raw:?}) error {err:?} leaked {needle:?}"
                );
            }

            let mut meta = MetadataMap::new();
            meta.insert(UPSTREAM_HEADER, raw.parse().unwrap());
            let status = pool.resolve_target_url(&meta).unwrap_err();
            assert_eq!(status.code(), tonic::Code::InvalidArgument);
            for needle in forbidden {
                assert!(
                    !status.message().contains(needle),
                    "resolve_target_url({raw:?}) status {:?} leaked {needle:?}",
                    status.message()
                );
            }
        }
    }

    #[tokio::test]
    async fn channel_for_rejects_invalid_hostname_with_invalid_argument() {
        let pool = UpstreamPool::new(None);
        let mut meta = MetadataMap::new();
        meta.insert(UPSTREAM_HEADER, "https://bad!host".parse().unwrap());
        let err = pool.channel_for(&meta).await.unwrap_err();
        assert_eq!(
            err.code(),
            tonic::Code::InvalidArgument,
            "expected InvalidArgument for invalid hostname before TLS init, got: {err}"
        );
    }

    #[test]
    fn normalize_origin_canonicalizes_ipv6_and_default_ports() {
        assert_eq!(
            normalize_origin("https://[0:0:0:0:0:0:0:1]:443/").unwrap(),
            "https://[::1]"
        );
        assert_eq!(
            normalize_origin("http://[2001:0DB8:0000:0000::0001]:8080/").unwrap(),
            "http://[2001:db8::1]:8080"
        );
        assert_eq!(normalize_origin("http://[::1]:80").unwrap(), "http://[::1]");
        assert_eq!(
            normalize_origin("https://Example.COM:443").unwrap(),
            "https://example.com"
        );
    }

    #[test]
    fn resolve_target_url_rejects_duplicate_upstream_headers() {
        let pool = UpstreamPool::new(Some("http://127.0.0.1:8080".to_string()));
        let mut meta = MetadataMap::new();
        meta.append(UPSTREAM_HEADER, "http://127.0.0.1:8080".parse().unwrap());
        meta.append(UPSTREAM_HEADER, "http://127.0.0.1:9090".parse().unwrap());
        let err = pool.resolve_target_url(&meta).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);

        let mut identical = MetadataMap::new();
        identical.append(UPSTREAM_HEADER, "http://127.0.0.1:8080".parse().unwrap());
        identical.append(UPSTREAM_HEADER, "http://127.0.0.1:8080".parse().unwrap());
        let err = pool.resolve_target_url(&identical).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn channel_for_https_installs_crypto_provider_without_panicking() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let pool = UpstreamPool::new(Some(format!("https://127.0.0.1:{port}")));
        let err = pool.channel_for(&MetadataMap::new()).await.unwrap_err();
        assert_eq!(
            err.code(),
            tonic::Code::Unavailable,
            "expected clean connection failure after TLS init, got: {err}"
        );
    }

    #[tokio::test]
    async fn channel_for_https_ipv6_unbrackets_tls_server_name() {
        let listener = TcpListener::bind("[::1]:0")
            .or_else(|_| TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let pool = UpstreamPool::new(Some(format!("https://[::1]:{port}")));
        let err = pool.channel_for(&MetadataMap::new()).await.unwrap_err();
        assert_eq!(
            err.code(),
            tonic::Code::Unavailable,
            "expected connection failure after IPv6 TLS init (not Internal), got: {err}"
        );
    }

    #[test]
    fn extract_auth_header_validates_single_ascii_header() {
        let empty = MetadataMap::new();
        assert_eq!(extract_auth_header(&empty).unwrap(), None);

        let mut single = MetadataMap::new();
        single.insert("authorization", "Bearer token-1".parse().unwrap());
        assert_eq!(
            extract_auth_header(&single).unwrap(),
            Some("Bearer token-1".to_string())
        );

        let mut duplicate = MetadataMap::new();
        duplicate.append("authorization", "Bearer token-1".parse().unwrap());
        duplicate.append("authorization", "Bearer token-2".parse().unwrap());
        assert_eq!(
            extract_auth_header(&duplicate).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );

        let mut identical_dup = MetadataMap::new();
        identical_dup.append("authorization", "Bearer token-1".parse().unwrap());
        identical_dup.append("authorization", "Bearer token-1".parse().unwrap());
        assert_eq!(
            extract_auth_header(&identical_dup).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );

        let mut non_ascii = MetadataMap::new();
        let raw_val = tonic::metadata::MetadataValue::try_from(b"Bearer \x80".as_slice()).unwrap();
        non_ascii.insert("authorization", raw_val);
        assert_eq!(
            extract_auth_header(&non_ascii).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
    }

    #[test]
    fn with_allowlist_documents_empty_iterator_and_implicit_default() {
        let open = UpstreamPool::with_allowlist(None, std::iter::empty::<&str>()).unwrap();
        let mut any_meta = MetadataMap::new();
        any_meta.insert(UPSTREAM_HEADER, "http://127.0.0.1:9000".parse().unwrap());
        assert_eq!(
            open.resolve_target_url(&any_meta).unwrap(),
            "http://127.0.0.1:9000"
        );

        assert!(UpstreamPool::with_allowlist(None, [" , "]).is_err());

        let with_default = UpstreamPool::with_allowlist(
            Some("https://Default.Example.COM:443/".to_string()),
            ["https://other.example.com:8443"],
        )
        .unwrap();
        assert_eq!(
            with_default
                .resolve_target_url(&MetadataMap::new())
                .unwrap(),
            "https://default.example.com"
        );
        let mut denied = MetadataMap::new();
        denied.insert(
            UPSTREAM_HEADER,
            "https://denied.example.com".parse().unwrap(),
        );
        assert_eq!(
            with_default.resolve_target_url(&denied).unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
    }

    #[test]
    fn prepare_outbound_forwards_valid_grpc_timeout_and_coalesced_omits_it() {
        let pool = UpstreamPool::new(Some("http://127.0.0.1:1".to_string()));

        for valid in ["500m", "30S", "1H", "15M", "12345678u", "0n"] {
            let mut incoming = MetadataMap::new();
            incoming.insert("authorization", "Bearer tok".parse().unwrap());
            incoming.insert(GRPC_TIMEOUT_HEADER, valid.parse().unwrap());
            incoming.insert(UPSTREAM_HEADER, "http://127.0.0.1:1".parse().unwrap());

            let forwarded = pool.prepare_outbound(&incoming, ()).unwrap();
            assert_eq!(
                forwarded
                    .metadata()
                    .get(GRPC_TIMEOUT_HEADER)
                    .and_then(|v| v.to_str().ok()),
                Some(valid),
                "prepare_outbound must forward valid grpc-timeout {valid:?}"
            );
            assert!(forwarded.metadata().get(UPSTREAM_HEADER).is_none());

            let coalesced = pool.prepare_coalesced_outbound(&incoming, ()).unwrap();
            assert!(
                coalesced.metadata().get(GRPC_TIMEOUT_HEADER).is_none(),
                "prepare_coalesced_outbound must omit grpc-timeout"
            );
            assert_eq!(
                coalesced
                    .metadata()
                    .get("authorization")
                    .and_then(|v| v.to_str().ok()),
                Some("Bearer tok")
            );
        }

        // Duplicate or malformed `grpc-timeout` values are dropped without failing the RPC.
        let mut dup = MetadataMap::new();
        dup.append(GRPC_TIMEOUT_HEADER, "10S".parse().unwrap());
        dup.append(GRPC_TIMEOUT_HEADER, "20S".parse().unwrap());
        assert!(
            pool.prepare_outbound(&dup, ())
                .unwrap()
                .metadata()
                .get(GRPC_TIMEOUT_HEADER)
                .is_none()
        );

        for invalid in ["", "S", "500x", "123456789S", "+5S", "-5S", "5 S"] {
            let mut incoming = MetadataMap::new();
            if let Ok(val) = invalid.parse() {
                incoming.insert(GRPC_TIMEOUT_HEADER, val);
                assert!(
                    pool.prepare_outbound(&incoming, ())
                        .unwrap()
                        .metadata()
                        .get(GRPC_TIMEOUT_HEADER)
                        .is_none(),
                    "invalid grpc-timeout {invalid:?} must not be forwarded"
                );
            }
        }
    }

    #[tokio::test]
    async fn channel_for_coalesces_concurrent_cold_connects_and_recovers_on_cancel() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{addr}");
        let accept_count = Arc::new(AtomicUsize::new(0));
        let accept_count_srv = Arc::clone(&accept_count);
        let incoming = futures::stream::unfold(listener, move |l| {
            let cnt = Arc::clone(&accept_count_srv);
            async move {
                match l.accept().await {
                    Ok((stream, _)) => {
                        let prev = cnt.fetch_add(1, Ordering::SeqCst);
                        if prev == 0 {
                            tokio::time::sleep(Duration::from_millis(40)).await;
                        }
                        Some((Ok::<_, std::io::Error>(crate::TcpConn::new(stream)), l))
                    }
                    Err(e) => Some((Err(e), l)),
                }
            }
        });
        let (_health_reporter, health_service) = tonic_health::server::health_reporter();
        let server_task = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(health_service)
                .serve_with_incoming(incoming),
        );

        let pool = Arc::new(UpstreamPool::new(Some(url.clone())));
        let mut set = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let pool = Arc::clone(&pool);
            set.spawn(async move { pool.channel_for(&MetadataMap::new()).await });
        }
        while let Some(res) = set.join_next().await {
            res.unwrap().expect("coalesced connect should succeed");
        }
        assert_eq!(
            accept_count.load(Ordering::SeqCst),
            1,
            "concurrent cold channel_for calls must coalesce into a single upstream connection"
        );
        assert!(pool.inflight.lock().unwrap().is_empty());

        // Dropping an in-flight connect leader without publishing lets a surviving waiter take
        // over and connect instead of stranding.
        let pool2 = Arc::new(UpstreamPool::new(Some(url.clone())));
        let ConnectFlight::Leader(leader) = pool2.join_connect(&url) else {
            panic!("first caller leads");
        };
        let waiter = tokio::spawn({
            let pool2 = Arc::clone(&pool2);
            async move { pool2.channel_for(&MetadataMap::new()).await }
        });
        tokio::task::yield_now().await;
        drop(leader);
        tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("waiter must not hang after leader drop")
            .unwrap()
            .expect("waiter should take over and connect");

        server_task.abort();
    }
}
