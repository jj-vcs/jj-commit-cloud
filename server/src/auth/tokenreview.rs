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

//! Kubernetes `TokenReview` verification with positive LRU caching, singleflight miss
//! deduplication, and projected ServiceAccount token rotation.
//!
//! # Concurrency and Merge Invariants
//! Concurrent cache misses for the same SHA-256 token digest coalesce behind a `watch`
//! channel whose RAII guard cleans up on cancellation so waiters promote a new leader instead
//! of hanging. Distinct in-flight digests and outbound `TokenReview` concurrency are both
//! bounded (with a timeout on semaphore permit waits so saturated queues fail fast), and
//! projected ServiceAccount tokens cache briefly with a single reload retry on HTTP 401.

use std::collections::HashMap;
use std::io::Read as _;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use lru::LruCache;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncReadExt as _;

use super::bearer::{MAX_TOKEN_FILE_BYTES, ScrubbedToken, scrub_bytes};
use super::{
    CallerIdentity, CallerKind, HTTP_REQUEST_TIMEOUT, MAX_AUTH_TOKEN_BYTES, MAX_HTTP_BODY_BYTES,
    VerifyError, build_http_client, read_bounded_json, validate_endpoint_url,
    validate_identity_text,
};

const DEFAULT_TOKENREVIEW_CACHE_TTL: Duration = Duration::from_secs(30);
const TOKENREVIEW_CACHE_CAPACITY: usize = 1024;
const MAX_CONCURRENT_TOKENREVIEWS: usize = 32;
const MAX_IN_FLIGHT_TOKENREVIEWS: usize = 256;
const SA_TOKEN_CACHE_TTL: Duration = Duration::from_secs(5);
const TOKENREVIEW_API_PATH: &str = "/apis/authentication.k8s.io/v1/tokenreviews";

/// Configuration for verifying Kubernetes ServiceAccount tokens via `TokenReview`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct K8sTokenReviewConfig {
    pub url: String,
    pub audience: Option<String>,
    pub ca_cert: Option<PathBuf>,
    pub sa_token_file: Option<PathBuf>,
}

impl K8sTokenReviewConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            audience: None,
            ca_cert: None,
            sa_token_file: None,
        }
    }
}

fn normalize_tokenreview_url(raw: &str) -> Result<String, String> {
    let (trimmed, mut parsed) = validate_endpoint_url(raw, "Kubernetes TokenReview", false)?;
    let path = parsed.path().trim_end_matches('/');
    if path.ends_with(TOKENREVIEW_API_PATH) {
        return Ok(trimmed.trim_end_matches('/').to_string());
    }
    let normalized_path = if path.is_empty() {
        TOKENREVIEW_API_PATH.to_string()
    } else {
        format!("{path}{TOKENREVIEW_API_PATH}")
    };
    parsed.set_path(&normalized_path);
    Ok(parsed.into())
}

fn parse_sa_token_bytes(raw_bytes: Vec<u8>, path: &Path) -> Result<String, String> {
    if raw_bytes.len() as u64 > MAX_TOKEN_FILE_BYTES {
        scrub_bytes(raw_bytes);
        return Err(format!(
            "Kubernetes ServiceAccount token file '{}' exceeds maximum size of {MAX_TOKEN_FILE_BYTES} bytes",
            path.display()
        ));
    }
    let raw_string = match String::from_utf8(raw_bytes) {
        Ok(s) => s,
        Err(utf8_err) => {
            scrub_bytes(utf8_err.into_bytes());
            return Err(format!(
                "Kubernetes ServiceAccount token file '{}' is not valid UTF-8",
                path.display()
            ));
        }
    };
    let trimmed = raw_string.trim();
    let result = if trimmed.is_empty()
        || trimmed.len() > MAX_AUTH_TOKEN_BYTES
        || !trimmed.bytes().all(|b| b.is_ascii_graphic())
    {
        Err(format!(
            "Kubernetes ServiceAccount token file '{}' contains an empty or malformed token",
            path.display()
        ))
    } else {
        Ok(trimmed.to_string())
    };
    scrub_bytes(raw_string.into_bytes());
    result
}

fn read_sa_token_file(path: &Path) -> Result<String, String> {
    let file = std::fs::File::open(path).map_err(|e| {
        format!(
            "failed to read Kubernetes ServiceAccount token file '{}': {e}",
            path.display()
        )
    })?;
    let mut raw_bytes = Vec::new();
    if let Err(e) = file
        .take(MAX_TOKEN_FILE_BYTES + 1)
        .read_to_end(&mut raw_bytes)
    {
        scrub_bytes(raw_bytes);
        return Err(format!(
            "failed to read Kubernetes ServiceAccount token file '{}': {e}",
            path.display()
        ));
    }
    parse_sa_token_bytes(raw_bytes, path)
}

async fn read_sa_token_file_async(path: &Path) -> Result<String, String> {
    let file = tokio::fs::File::open(path).await.map_err(|e| {
        format!(
            "failed to read Kubernetes ServiceAccount token file '{}': {e}",
            path.display()
        )
    })?;
    let mut raw_bytes = Vec::new();
    if let Err(e) = file
        .take(MAX_TOKEN_FILE_BYTES + 1)
        .read_to_end(&mut raw_bytes)
        .await
    {
        scrub_bytes(raw_bytes);
        return Err(format!(
            "failed to read Kubernetes ServiceAccount token file '{}': {e}",
            path.display()
        ));
    }
    parse_sa_token_bytes(raw_bytes, path)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TokenReviewRequest<'a> {
    api_version: &'static str,
    kind: &'static str,
    spec: TokenReviewSpec<'a>,
}

#[derive(Serialize)]
struct TokenReviewSpec<'a> {
    token: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    audiences: Option<[&'a str; 1]>,
}

#[derive(Deserialize)]
struct TokenReviewResponse {
    #[serde(default)]
    status: Option<TokenReviewStatus>,
}

#[derive(Deserialize)]
struct TokenReviewStatus {
    #[serde(default)]
    authenticated: bool,
    #[serde(default)]
    user: Option<TokenReviewUser>,
    #[serde(default)]
    audiences: Option<Vec<String>>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Deserialize)]
struct TokenReviewUser {
    #[serde(default)]
    username: String,
    #[serde(default)]
    uid: Option<String>,
}

struct CachedTokenReview {
    identity: CallerIdentity,
    expires_at: Instant,
}

struct CachedSaToken {
    token: Arc<ScrubbedToken>,
    fetched_at: Instant,
}

struct InFlightTokenReview {
    id: u64,
    rx: tokio::sync::watch::Receiver<Option<Result<CallerIdentity, VerifyError>>>,
}

/// RAII guard for an active `K8sTokenReviewValidator` singleflight leader.
///
/// Always removes the map entry before waking waiters; if dropped without calling `complete`
/// (e.g. due to gRPC cancellation), closes the `watch` channel with `None` so waiters wake
/// up and promote a new leader instead of hanging.
struct TokenReviewFlightGuard<'a> {
    in_flight: &'a Mutex<HashMap<[u8; 32], InFlightTokenReview>>,
    digest: [u8; 32],
    flight_id: u64,
    tx: Option<tokio::sync::watch::Sender<Option<Result<CallerIdentity, VerifyError>>>>,
}

impl TokenReviewFlightGuard<'_> {
    fn remove_entry(&self) {
        let mut in_flight = self.in_flight.lock().unwrap_or_else(|e| e.into_inner());
        if in_flight
            .get(&self.digest)
            .is_some_and(|entry| entry.id == self.flight_id)
        {
            in_flight.remove(&self.digest);
        }
    }

    fn complete(mut self, result: Result<CallerIdentity, VerifyError>) {
        self.remove_entry();
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(Some(result));
        }
    }
}

impl Drop for TokenReviewFlightGuard<'_> {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            self.remove_entry();
            drop(tx);
        }
    }
}

/// Async Kubernetes `TokenReview` verifier with a bounded SHA-256-keyed positive cache,
/// per-digest singleflight miss deduplication, bounded outbound concurrency, and short-lived
/// projected ServiceAccount token caching with HTTP 401 rotation retry.
pub struct K8sTokenReviewValidator {
    endpoint_url: String,
    audience: Option<String>,
    sa_token_file: Option<PathBuf>,
    cache_ttl: Duration,
    max_in_flight: usize,
    permit_acquire_timeout: Duration,
    client: reqwest::Client,
    cache: Mutex<LruCache<[u8; 32], CachedTokenReview>>,
    in_flight: Mutex<HashMap<[u8; 32], InFlightTokenReview>>,
    next_flight_id: AtomicU64,
    in_flight_semaphore: tokio::sync::Semaphore,
    sa_token_cache: Mutex<Option<CachedSaToken>>,
    sa_token_reload_lock: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for K8sTokenReviewValidator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("K8sTokenReviewValidator")
            .field("endpoint_url", &self.endpoint_url)
            .field("audience", &self.audience)
            .field("sa_token_file", &self.sa_token_file)
            .finish()
    }
}

impl K8sTokenReviewValidator {
    pub fn new(config: K8sTokenReviewConfig) -> Result<Self, String> {
        Self::with_cache_ttl(config, DEFAULT_TOKENREVIEW_CACHE_TTL)
    }

    pub fn with_cache_ttl(
        config: K8sTokenReviewConfig,
        cache_ttl: Duration,
    ) -> Result<Self, String> {
        let endpoint_url = normalize_tokenreview_url(&config.url)?;
        let audience = match config.audience {
            Some(aud) => {
                let trimmed = aud.trim();
                if trimmed.is_empty() {
                    return Err("Kubernetes TokenReview audience must not be empty".to_string());
                }
                Some(trimmed.to_string())
            }
            None => None,
        };
        let initial_sa_token = match &config.sa_token_file {
            Some(sa_path) => {
                // Fail fast on missing/malformed projected token paths at startup and warm the
                // short-lived in-memory cache (rotated dynamically on TTL expiry or HTTP 401).
                let token = Arc::new(ScrubbedToken::new(read_sa_token_file(sa_path)?));
                Some(CachedSaToken {
                    token,
                    fetched_at: Instant::now(),
                })
            }
            None => None,
        };
        let client = build_http_client(config.ca_cert.as_deref())?;
        let capacity = NonZeroUsize::new(TOKENREVIEW_CACHE_CAPACITY)
            .expect("TOKENREVIEW_CACHE_CAPACITY is non-zero");
        Ok(Self {
            endpoint_url,
            audience,
            sa_token_file: config.sa_token_file,
            cache_ttl,
            max_in_flight: MAX_IN_FLIGHT_TOKENREVIEWS,
            permit_acquire_timeout: HTTP_REQUEST_TIMEOUT,
            client,
            cache: Mutex::new(LruCache::new(capacity)),
            in_flight: Mutex::new(HashMap::new()),
            next_flight_id: AtomicU64::new(0),
            in_flight_semaphore: tokio::sync::Semaphore::new(MAX_CONCURRENT_TOKENREVIEWS),
            sa_token_cache: Mutex::new(initial_sa_token),
            sa_token_reload_lock: tokio::sync::Mutex::new(()),
        })
    }

    #[cfg(test)]
    pub(super) fn with_limits_for_test(
        config: K8sTokenReviewConfig,
        cache_ttl: Duration,
        max_concurrent: usize,
        max_in_flight: usize,
        permit_acquire_timeout: Duration,
    ) -> Result<Self, String> {
        let mut validator = Self::with_cache_ttl(config, cache_ttl)?;
        validator.max_in_flight = max_in_flight;
        validator.permit_acquire_timeout = permit_acquire_timeout;
        validator.in_flight_semaphore = tokio::sync::Semaphore::new(max_concurrent);
        Ok(validator)
    }

    #[cfg(test)]
    pub(super) fn in_flight_len(&self) -> usize {
        self.in_flight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    pub fn endpoint_url(&self) -> &str {
        &self.endpoint_url
    }

    fn lookup_cached(&self, token_digest: &[u8; 32]) -> Option<CallerIdentity> {
        if self.cache_ttl.is_zero() {
            return None;
        }
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = cache.get(token_digest) {
            if entry.expires_at > Instant::now() {
                return Some(entry.identity.clone());
            }
            cache.pop(token_digest);
        }
        None
    }

    async fn load_sa_token(
        &self,
        sa_path: &Path,
        rejected_token: Option<&Arc<ScrubbedToken>>,
    ) -> Result<(Arc<ScrubbedToken>, bool), VerifyError> {
        if rejected_token.is_none() {
            let cache = self
                .sa_token_cache
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(cached) = cache.as_ref()
                && cached.fetched_at.elapsed() < SA_TOKEN_CACHE_TTL
            {
                return Ok((Arc::clone(&cached.token), true));
            }
        }

        let _reload_guard = self.sa_token_reload_lock.lock().await;
        {
            let cache = self
                .sa_token_cache
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(cached) = cache.as_ref()
                && cached.fetched_at.elapsed() < SA_TOKEN_CACHE_TTL
            {
                match rejected_token {
                    None => return Ok((Arc::clone(&cached.token), true)),
                    Some(rejected) if !Arc::ptr_eq(&cached.token, rejected) => {
                        return Ok((Arc::clone(&cached.token), false));
                    }
                    Some(_) => {}
                }
            }
        }

        match read_sa_token_file_async(sa_path).await {
            Ok(fresh_raw) => {
                let token = Arc::new(ScrubbedToken::new(fresh_raw));
                let mut cache = self
                    .sa_token_cache
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                *cache = Some(CachedSaToken {
                    token: Arc::clone(&token),
                    fetched_at: Instant::now(),
                });
                Ok((token, false))
            }
            Err(err) => {
                if let Some(rejected) = rejected_token {
                    let mut cache = self
                        .sa_token_cache
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    if cache
                        .as_ref()
                        .is_some_and(|cached| Arc::ptr_eq(&cached.token, rejected))
                    {
                        *cache = None;
                    }
                }
                Err(VerifyError::Backend(err))
            }
        }
    }

    async fn send_tokenreview_request(
        &self,
        payload: &TokenReviewRequest<'_>,
        sa_token: Option<&ScrubbedToken>,
    ) -> Result<reqwest::Response, VerifyError> {
        let mut req = self.client.post(&self.endpoint_url).json(payload);
        if let Some(sa_token) = sa_token {
            req = req.bearer_auth(sa_token.as_str());
        }
        req.send().await.map_err(|e| {
            VerifyError::Backend(format!("Kubernetes TokenReview request failed: {e}"))
        })
    }

    async fn execute_tokenreview(&self, token: &str) -> Result<CallerIdentity, VerifyError> {
        let payload = TokenReviewRequest {
            api_version: "authentication.k8s.io/v1",
            kind: "TokenReview",
            spec: TokenReviewSpec {
                token,
                audiences: self.audience.as_deref().map(|aud| [aud]),
            },
        };

        let resp = if let Some(sa_path) = &self.sa_token_file {
            let (sa_token, used_cached) = self.load_sa_token(sa_path, None).await?;
            let first_resp = self
                .send_tokenreview_request(&payload, Some(sa_token.as_ref()))
                .await?;
            if first_resp.status() == reqwest::StatusCode::UNAUTHORIZED && used_cached {
                let (refreshed_token, _) = self.load_sa_token(sa_path, Some(&sa_token)).await?;
                if refreshed_token.as_str() != sa_token.as_str() {
                    self.send_tokenreview_request(&payload, Some(refreshed_token.as_ref()))
                        .await?
                } else {
                    first_resp
                }
            } else {
                first_resp
            }
        } else {
            self.send_tokenreview_request(&payload, None).await?
        };

        if !resp.status().is_success() {
            return Err(VerifyError::Backend(format!(
                "Kubernetes TokenReview returned HTTP {}",
                resp.status().as_u16()
            )));
        }

        let review: TokenReviewResponse =
            read_bounded_json(resp, MAX_HTTP_BODY_BYTES, "Kubernetes TokenReview")
                .await
                .map_err(VerifyError::Backend)?;
        let status = review.status.ok_or_else(|| {
            VerifyError::Backend("Kubernetes TokenReview response missing status".to_string())
        })?;
        if !status.authenticated {
            let reason = status
                .error
                .unwrap_or_else(|| "token not authenticated".to_string());
            return Err(VerifyError::InvalidCredential(format!(
                "Kubernetes TokenReview rejected token: {reason}"
            )));
        }
        if let Some(expected_aud) = &self.audience
            && let Some(audiences) = &status.audiences
            && !audiences.is_empty()
            && !audiences.iter().any(|aud| aud == expected_aud)
        {
            return Err(VerifyError::InvalidCredential(
                "Kubernetes TokenReview response audience does not match configured audience"
                    .to_string(),
            ));
        }
        let user = status.user.ok_or_else(|| {
            VerifyError::Backend("Kubernetes TokenReview response missing status.user".to_string())
        })?;
        let subject = validate_identity_text(&user.username, "status.user.username")
            .map_err(VerifyError::Backend)?;
        let actor_uid = match user.uid {
            Some(uid) => {
                if uid.bytes().any(|b| b.is_ascii_control()) {
                    return Err(VerifyError::Backend(
                        "invalid status.user.uid claim (contains ASCII control characters)"
                            .to_string(),
                    ));
                }
                let trimmed = uid.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(
                        validate_identity_text(trimmed, "status.user.uid")
                            .map_err(VerifyError::Backend)?,
                    )
                }
            }
            None => None,
        };

        Ok(CallerIdentity {
            subject,
            actor_uid,
            actor_name: None,
            atespace: None,
            is_worker_for_actor: false,
            kind: CallerKind::K8sServiceAccount,
        })
    }

    pub(super) async fn verify_detailed(&self, token: &str) -> Result<CallerIdentity, VerifyError> {
        let token_digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        loop {
            if let Some(identity) = self.lookup_cached(&token_digest) {
                return Ok(identity);
            }

            let flight_role = {
                let mut in_flight = self.in_flight.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(existing) = in_flight.get(&token_digest) {
                    Err(existing.rx.clone())
                } else {
                    if in_flight.len() >= self.max_in_flight {
                        return Err(VerifyError::Backend(
                            "Kubernetes TokenReview in-flight limit reached".to_string(),
                        ));
                    }
                    let flight_id = self.next_flight_id.fetch_add(1, Ordering::Relaxed);
                    let (tx, rx) = tokio::sync::watch::channel(None);
                    in_flight.insert(token_digest, InFlightTokenReview { id: flight_id, rx });
                    Ok(TokenReviewFlightGuard {
                        in_flight: &self.in_flight,
                        digest: token_digest,
                        flight_id,
                        tx: Some(tx),
                    })
                }
            };

            match flight_role {
                Err(mut rx) => {
                    let coalesced = loop {
                        if let Some(res) = rx.borrow_and_update().clone() {
                            break Some(res);
                        }
                        if rx.changed().await.is_err() {
                            break rx.borrow_and_update().clone();
                        }
                    };
                    if let Some(res) = coalesced {
                        return res;
                    }
                    // Leader was cancelled before completing; retry so a waiter can take over.
                }
                Ok(guard) => {
                    // Re-check cache once after claiming leadership to close the narrow race where
                    // a prior leader populated `cache` and exited `in_flight` right before we
                    // acquired `in_flight`.
                    if let Some(identity) = self.lookup_cached(&token_digest) {
                        guard.complete(Ok(identity.clone()));
                        return Ok(identity);
                    }

                    let result = match tokio::time::timeout(
                        self.permit_acquire_timeout,
                        self.in_flight_semaphore.acquire(),
                    )
                    .await
                    {
                        Ok(Ok(_permit)) => self.execute_tokenreview(token).await,
                        Ok(Err(_)) => Err(VerifyError::Backend(
                            "Kubernetes TokenReview concurrency semaphore closed".to_string(),
                        )),
                        Err(_) => Err(VerifyError::Backend(
                            "Kubernetes TokenReview concurrency queue timed out".to_string(),
                        )),
                    };

                    if let Ok(identity) = &result
                        && !self.cache_ttl.is_zero()
                        && let Some(expires_at) = Instant::now().checked_add(self.cache_ttl)
                    {
                        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
                        cache.put(
                            token_digest,
                            CachedTokenReview {
                                identity: identity.clone(),
                                expires_at,
                            },
                        );
                    }
                    guard.complete(result.clone());
                    return result;
                }
            }
        }
    }

    /// Verifies `token` via Kubernetes `TokenReview`, serving fresh positive hits from cache.
    pub async fn verify(&self, token: &str) -> Result<CallerIdentity, String> {
        self.verify_detailed(token)
            .await
            .map_err(VerifyError::into_message)
    }
}
