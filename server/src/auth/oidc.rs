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

//! OIDC and Agent Substrate actor JWT validation backed by cached JWKS discovery.
//!
//! # Authority and Crash-Safety Ordering
//! Discovery documents must match the configured issuer exactly and cannot downgrade an
//! HTTPS issuer to a plaintext `jwks_uri`. Tokens claiming the configured issuer stay pinned
//! to OIDC verification and fail closed instead of falling through to other authenticators.
//!
//! # Concurrency and Merge Invariants
//! Initial JWKS loads and miss-triggered refreshes serialize behind a singleflight mutex with
//! cooldown backoff so unknown-`kid` bursts and identity-provider outages cannot flood remote
//! discovery or JWKS endpoints.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use base64::Engine as _;
use jsonwebtoken::jwk::{
    AlgorithmParameters, EllipticCurve, Jwk, JwkSet, KeyAlgorithm, KeyOperations, PublicKeyUse,
};
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::Deserialize;

use super::{
    CallerIdentity, CallerKind, MAX_HTTP_BODY_BYTES, VerifyError, build_http_client,
    read_bounded_json, validate_endpoint_url, validate_identity_text,
};

const DEFAULT_JWKS_CACHE_TTL: Duration = Duration::from_secs(15 * 60);
const DEFAULT_JWKS_REFRESH_COOLDOWN: Duration = Duration::from_secs(10);
pub(super) const MAX_CANDIDATE_KEYS: usize = 8;

/// Configuration for validating OIDC / Agent Substrate Actor JWTs via JWKS.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OidcAuthConfig {
    pub issuer: String,
    pub audience: String,
    pub jwks_uri: Option<String>,
    pub ca_cert: Option<PathBuf>,
    pub enforce_actor_repo_suffix: bool,
}

impl OidcAuthConfig {
    pub fn new(issuer: impl Into<String>, audience: impl Into<String>) -> Self {
        Self {
            issuer: issuer.into(),
            audience: audience.into(),
            jwks_uri: None,
            ca_cert: None,
            enforce_actor_repo_suffix: false,
        }
    }
}

fn parse_optional_claim_string(
    map: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<Option<String>, String> {
    match map.get(key) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(s)) => {
            if s.bytes().any(|b| b.is_ascii_control()) {
                return Err(format!(
                    "invalid actor.ate.dev.{key} claim (contains ASCII control characters)"
                ));
            }
            let trimmed = s.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(validate_identity_text(trimmed, key)?))
            }
        }
        Some(_) => Err(format!("actor.ate.dev.{key} claim must be a JSON string")),
    }
}

pub(super) fn parse_worker_for_actor(val: Option<&serde_json::Value>) -> bool {
    match val {
        None | Some(serde_json::Value::Null) | Some(serde_json::Value::Bool(false)) => false,
        Some(serde_json::Value::String(s)) => !s.trim().is_empty(),
        // Fail safe (read-only) for `true` and any present object, array, or numeric claim
        // (including `{}` marker objects emitted by Go `*struct{}` presence claims).
        Some(
            serde_json::Value::Bool(true)
            | serde_json::Value::Object(_)
            | serde_json::Value::Array(_)
            | serde_json::Value::Number(_),
        ) => true,
    }
}

fn looks_like_jwt(token: &str) -> bool {
    let mut parts = token.split('.');
    let Some(p1) = parts.next() else {
        return false;
    };
    let Some(p2) = parts.next() else {
        return false;
    };
    let Some(p3) = parts.next() else {
        return false;
    };
    parts.next().is_none() && !p1.is_empty() && !p2.is_empty() && !p3.is_empty()
}

fn jwk_matches_algorithm(jwk: &Jwk, alg: Algorithm) -> bool {
    if let Some(use_val) = &jwk.common.public_key_use
        && *use_val != PublicKeyUse::Signature
    {
        return false;
    }
    if let Some(ops) = &jwk.common.key_operations
        && !ops.contains(&KeyOperations::Verify)
    {
        return false;
    }
    if let Some(key_alg) = jwk.common.key_algorithm {
        let expected = match alg {
            Algorithm::RS256 => KeyAlgorithm::RS256,
            Algorithm::ES256 => KeyAlgorithm::ES256,
            _ => return false,
        };
        if key_alg != expected {
            return false;
        }
    }
    match (alg, &jwk.algorithm) {
        (Algorithm::RS256, AlgorithmParameters::RSA(_)) => true,
        (Algorithm::ES256, AlgorithmParameters::EllipticCurve(ec)) => {
            ec.curve == EllipticCurve::P256
        }
        _ => false,
    }
}

#[derive(Deserialize)]
pub(super) struct OidcDiscoveryDocument {
    pub(super) issuer: String,
    pub(super) jwks_uri: String,
}

pub(super) fn validate_discovery_document(
    configured_issuer: &str,
    doc: &OidcDiscoveryDocument,
) -> Result<String, String> {
    if doc.issuer != configured_issuer {
        return Err("OIDC discovery issuer does not match configured issuer".to_string());
    }
    let (_, parsed_issuer) = validate_endpoint_url(configured_issuer, "OIDC issuer", false)?;
    let (validated_jwks_uri, parsed_jwks_uri) =
        validate_endpoint_url(&doc.jwks_uri, "discovered OIDC jwks_uri", true)?;
    if parsed_issuer.scheme() == "https" && parsed_jwks_uri.scheme() != "https" {
        return Err(
            "discovered OIDC jwks_uri must use https:// when configured issuer uses https://"
                .to_string(),
        );
    }
    Ok(validated_jwks_uri)
}

#[derive(Deserialize)]
struct UnverifiedIssuerClaim {
    #[serde(default)]
    iss: Option<String>,
}

#[derive(Deserialize)]
struct OidcClaims {
    sub: String,
    #[serde(rename = "actor.ate.dev", default)]
    actor: Option<serde_json::Value>,
    #[serde(rename = "worker-for-actor", default)]
    worker_for_actor: Option<serde_json::Value>,
}

#[derive(Default)]
struct JwksCacheState {
    jwks: Option<JwkSet>,
    fetched_at: Option<Instant>,
    generation: u64,
    last_forced_refresh: Option<Instant>,
    last_initial_fetch_failure: Option<Instant>,
}

struct InitialJwksLookup {
    keys: Vec<DecodingKey>,
    fetched_at: Instant,
    generation: u64,
    just_fetched: bool,
}

enum DecodeFailure {
    KeyOrSignature(String),
    ClaimsOrFormat(String),
}

impl DecodeFailure {
    fn into_message(self) -> String {
        match self {
            Self::KeyOrSignature(msg) | Self::ClaimsOrFormat(msg) => msg,
        }
    }
}

/// Async OIDC / Agent Substrate Actor JWT validator backed by cached JWKS discovery.
pub struct OidcValidator {
    issuer: String,
    audience: String,
    explicit_jwks_uri: Option<String>,
    enforce_actor_repo_suffix: bool,
    jwks_ttl: Duration,
    refresh_cooldown: Duration,
    client: reqwest::Client,
    cache: tokio::sync::RwLock<JwksCacheState>,
    refresh_lock: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for OidcValidator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OidcValidator")
            .field("issuer", &self.issuer)
            .field("audience", &self.audience)
            .field("explicit_jwks_uri", &self.explicit_jwks_uri)
            .field("enforce_actor_repo_suffix", &self.enforce_actor_repo_suffix)
            .finish()
    }
}

impl OidcValidator {
    pub fn new(config: OidcAuthConfig) -> Result<Self, String> {
        Self::with_cache_timings(
            config,
            DEFAULT_JWKS_CACHE_TTL,
            DEFAULT_JWKS_REFRESH_COOLDOWN,
        )
    }

    pub fn with_cache_timings(
        config: OidcAuthConfig,
        jwks_ttl: Duration,
        refresh_cooldown: Duration,
    ) -> Result<Self, String> {
        let (issuer, _) = validate_endpoint_url(&config.issuer, "OIDC issuer", false)?;
        let audience = config.audience.trim().to_string();
        if audience.is_empty() {
            return Err("OIDC audience must not be empty".to_string());
        }
        let explicit_jwks_uri = match config.jwks_uri {
            Some(uri) => {
                let (validated, _) = validate_endpoint_url(&uri, "OIDC jwks_uri", true)?;
                Some(validated)
            }
            None => None,
        };
        let client = build_http_client(config.ca_cert.as_deref())?;
        Ok(Self {
            issuer,
            audience,
            explicit_jwks_uri,
            enforce_actor_repo_suffix: config.enforce_actor_repo_suffix,
            jwks_ttl,
            refresh_cooldown,
            client,
            cache: tokio::sync::RwLock::new(JwksCacheState::default()),
            refresh_lock: tokio::sync::Mutex::new(()),
        })
    }

    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    pub fn audience(&self) -> &str {
        &self.audience
    }

    pub fn enforce_actor_repo_suffix(&self) -> bool {
        self.enforce_actor_repo_suffix
    }

    async fn fetch_jwks_from_remote(&self) -> Result<JwkSet, String> {
        let jwks_uri = if let Some(explicit) = &self.explicit_jwks_uri {
            explicit.clone()
        } else {
            let discovery_url = format!(
                "{}/.well-known/openid-configuration",
                self.issuer.trim_end_matches('/')
            );
            let resp = self
                .client
                .get(&discovery_url)
                .send()
                .await
                .map_err(|e| format!("OIDC discovery request failed: {e}"))?;
            if !resp.status().is_success() {
                return Err(format!(
                    "OIDC discovery returned HTTP {}",
                    resp.status().as_u16()
                ));
            }
            let doc: OidcDiscoveryDocument =
                read_bounded_json(resp, MAX_HTTP_BODY_BYTES, "OIDC discovery").await?;
            validate_discovery_document(&self.issuer, &doc)?
        };

        let resp = self
            .client
            .get(&jwks_uri)
            .send()
            .await
            .map_err(|e| format!("JWKS request failed: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!(
                "JWKS endpoint returned HTTP {}",
                resp.status().as_u16()
            ));
        }
        read_bounded_json(resp, MAX_HTTP_BODY_BYTES, "JWKS").await
    }

    pub(super) fn find_candidate_keys(
        jwks: &JwkSet,
        kid: Option<&str>,
        alg: Algorithm,
    ) -> Vec<DecodingKey> {
        jwks.keys
            .iter()
            .filter(|jwk| {
                if !jwk_matches_algorithm(jwk, alg) {
                    return false;
                }
                match kid {
                    Some(expected_kid) => jwk.common.key_id.as_deref() == Some(expected_kid),
                    None => true,
                }
            })
            .filter_map(|jwk| DecodingKey::from_jwk(jwk).ok())
            .take(MAX_CANDIDATE_KEYS)
            .collect()
    }

    async fn get_candidate_keys_initial(
        &self,
        kid: Option<&str>,
        alg: Algorithm,
    ) -> Result<InitialJwksLookup, String> {
        {
            let guard = self.cache.read().await;
            if let (Some(jwks), Some(fetched_at)) = (&guard.jwks, guard.fetched_at)
                && fetched_at.elapsed() < self.jwks_ttl
            {
                return Ok(InitialJwksLookup {
                    keys: Self::find_candidate_keys(jwks, kid, alg),
                    fetched_at,
                    generation: guard.generation,
                    just_fetched: false,
                });
            }
            if let Some(failed_at) = guard.last_initial_fetch_failure
                && failed_at.elapsed() < self.refresh_cooldown
            {
                return Err("JWKS fetch backoff active after recent failure".to_string());
            }
        }

        let _refresh_guard = self.refresh_lock.lock().await;
        {
            let guard = self.cache.read().await;
            if let (Some(jwks), Some(fetched_at)) = (&guard.jwks, guard.fetched_at)
                && fetched_at.elapsed() < self.jwks_ttl
            {
                return Ok(InitialJwksLookup {
                    keys: Self::find_candidate_keys(jwks, kid, alg),
                    fetched_at,
                    generation: guard.generation,
                    just_fetched: true,
                });
            }
            if let Some(failed_at) = guard.last_initial_fetch_failure
                && failed_at.elapsed() < self.refresh_cooldown
            {
                return Err("JWKS fetch backoff active after recent failure".to_string());
            }
        }

        // Re-run OIDC discovery (unless explicit jwks_uri is pinned) on initial load and after
        // TTL expiry so IDP jwks_uri rotation recovers automatically. Record failures for
        // `refresh_cooldown` so an IDP outage fails fast instead of serially hammering the IDP.
        let jwks = match self.fetch_jwks_from_remote().await {
            Ok(jwks) => jwks,
            Err(err) => {
                let mut guard = self.cache.write().await;
                guard.last_initial_fetch_failure = Some(Instant::now());
                return Err(err);
            }
        };
        let keys = Self::find_candidate_keys(&jwks, kid, alg);
        let fetched_at = Instant::now();
        let mut guard = self.cache.write().await;
        guard.jwks = Some(jwks);
        guard.fetched_at = Some(fetched_at);
        guard.last_initial_fetch_failure = None;
        guard.generation = guard.generation.wrapping_add(1);
        let generation = guard.generation;
        Ok(InitialJwksLookup {
            keys,
            fetched_at,
            generation,
            just_fetched: true,
        })
    }

    async fn refresh_jwks_on_miss(
        &self,
        kid: Option<&str>,
        alg: Algorithm,
        observed_fetched_at: Instant,
        observed_generation: u64,
    ) -> Result<Option<Vec<DecodingKey>>, String> {
        let _refresh_guard = self.refresh_lock.lock().await;
        {
            let guard = self.cache.read().await;
            // If another concurrent request refreshed the cache while we waited for
            // `refresh_lock`, reuse the newly cached JWKS (for both kid-bearing and kid-less
            // tokens) before consulting the forced-refresh cooldown.
            if let Some(jwks) = &guard.jwks
                && (guard.fetched_at != Some(observed_fetched_at)
                    || guard.generation != observed_generation)
            {
                return Ok(Some(Self::find_candidate_keys(jwks, kid, alg)));
            }
            if let Some(last_forced) = guard.last_forced_refresh
                && last_forced.elapsed() < self.refresh_cooldown
            {
                return Ok(None);
            }
        }

        let fetch_res = self.fetch_jwks_from_remote().await;
        let completed_at = Instant::now();
        let mut guard = self.cache.write().await;
        // Record the forced-refresh timestamp after the network attempt finishes (including on
        // failure/timeout) so unknown-kid storms cannot repeatedly hammer an unreachable IDP.
        guard.last_forced_refresh = Some(completed_at);
        let jwks = fetch_res?;
        let keys = Self::find_candidate_keys(&jwks, kid, alg);
        guard.jwks = Some(jwks);
        guard.fetched_at = Some(completed_at);
        guard.last_initial_fetch_failure = None;
        guard.generation = guard.generation.wrapping_add(1);
        Ok(Some(keys))
    }

    fn decode_with_keys(
        &self,
        token: &str,
        alg: Algorithm,
        keys: &[DecodingKey],
    ) -> Result<OidcClaims, DecodeFailure> {
        let mut validation = Validation::new(alg);
        validation.validate_exp = true;
        validation.validate_nbf = true;
        validation.validate_aud = true;
        validation.set_issuer(&[&self.issuer]);
        validation.set_audience(&[&self.audience]);
        validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);

        let mut last_err = None;
        for key in keys {
            match jsonwebtoken::decode::<OidcClaims>(token, key, &validation) {
                Ok(data) => return Ok(data.claims),
                Err(err) => match err.kind() {
                    jsonwebtoken::errors::ErrorKind::InvalidSignature
                    | jsonwebtoken::errors::ErrorKind::InvalidKeyFormat
                    | jsonwebtoken::errors::ErrorKind::InvalidEcdsaKey
                    | jsonwebtoken::errors::ErrorKind::InvalidRsaKey(_) => {
                        last_err = Some(err);
                    }
                    _ => {
                        // Cryptographic signature verified (or token format/claims failed);
                        // trying additional keys or refreshing JWKS cannot validate this token.
                        return Err(DecodeFailure::ClaimsOrFormat(format!(
                            "JWT verification failed: {err}"
                        )));
                    }
                },
            }
        }
        Err(DecodeFailure::KeyOrSignature(last_err.map_or_else(
            || "no matching JWK found".to_string(),
            |e| format!("JWT verification failed: {e}"),
        )))
    }

    pub(super) fn unverified_issuer_matches(&self, token: &str) -> bool {
        // Decode only the base64url payload segment (tolerating trailing `=` padding) without
        // parsing `Header.alg`, so tokens with unsupported/unrecognized algorithms (for example
        // `ES512` or `none`) or padded base64url payloads claiming our OIDC issuer are still
        // routed authoritatively to OIDC verification and fail closed instead of falling through
        // to Kubernetes TokenReview.
        let mut parts = token.split('.');
        let (Some(header), Some(payload), Some(_sig), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return false;
        };
        if header.is_empty() || payload.is_empty() {
            return false;
        }
        let unpadded_payload = payload.trim_end_matches('=');
        if unpadded_payload.is_empty() {
            return false;
        }
        let Ok(decoded) = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(unpadded_payload)
        else {
            return false;
        };
        serde_json::from_slice::<UnverifiedIssuerClaim>(&decoded)
            .ok()
            .and_then(|claims| claims.iss)
            .is_some_and(|iss| iss == self.issuer)
    }

    pub(super) async fn verify_detailed(&self, token: &str) -> Result<CallerIdentity, VerifyError> {
        if !looks_like_jwt(token) {
            return Err(VerifyError::InvalidCredential(
                "token is not a compact JWT".to_string(),
            ));
        }
        let header = jsonwebtoken::decode_header(token)
            .map_err(|e| VerifyError::InvalidCredential(format!("invalid JWT header: {e}")))?;
        if header.crit.is_some() {
            return Err(VerifyError::InvalidCredential(
                "unsupported JWT crit header".to_string(),
            ));
        }
        let alg = match header.alg {
            Algorithm::RS256 => Algorithm::RS256,
            Algorithm::ES256 => Algorithm::ES256,
            other => {
                return Err(VerifyError::InvalidCredential(format!(
                    "unsupported JWT algorithm {other:?} (only RS256 and ES256 are allowed)"
                )));
            }
        };

        // Pre-filter on unverified `iss` before fetching JWKS so tokens from other issuers
        // (such as Kubernetes ServiceAccount JWTs) fall through immediately without triggering
        // OIDC discovery/JWKS network I/O or burning refresh cooldown.
        if !self.unverified_issuer_matches(token) {
            return Err(VerifyError::InvalidCredential(
                "JWT issuer does not match configured OIDC issuer".to_string(),
            ));
        }

        let kid = header.kid.as_deref();
        let initial = self
            .get_candidate_keys_initial(kid, alg)
            .await
            .map_err(VerifyError::Backend)?;
        let claims = if !initial.keys.is_empty() {
            match self.decode_with_keys(token, alg, &initial.keys) {
                Ok(claims) => claims,
                Err(DecodeFailure::ClaimsOrFormat(err)) => {
                    return Err(VerifyError::InvalidCredential(err));
                }
                Err(DecodeFailure::KeyOrSignature(err)) => {
                    // Only attempt a cooldown-gated forced refresh for kid-less tokens whose
                    // signature did not match a warm cached key set (key rotation without kid).
                    // Skip refreshing when this request already fetched fresh JWKS.
                    if kid.is_none() && !initial.just_fetched {
                        if let Some(refreshed_keys) = self
                            .refresh_jwks_on_miss(kid, alg, initial.fetched_at, initial.generation)
                            .await
                            .map_err(VerifyError::Backend)?
                            && !refreshed_keys.is_empty()
                        {
                            self.decode_with_keys(token, alg, &refreshed_keys)
                                .map_err(|e| VerifyError::InvalidCredential(e.into_message()))?
                        } else {
                            return Err(VerifyError::InvalidCredential(err));
                        }
                    } else {
                        return Err(VerifyError::InvalidCredential(err));
                    }
                }
            }
        } else if initial.just_fetched {
            return Err(VerifyError::InvalidCredential(
                "no matching JWK found".to_string(),
            ));
        } else {
            let Some(refreshed_keys) = self
                .refresh_jwks_on_miss(kid, alg, initial.fetched_at, initial.generation)
                .await
                .map_err(VerifyError::Backend)?
            else {
                return Err(VerifyError::InvalidCredential(
                    "unknown JWT kid and JWKS refresh cooldown is active".to_string(),
                ));
            };
            if refreshed_keys.is_empty() {
                return Err(VerifyError::InvalidCredential(
                    "no matching JWK found after JWKS refresh".to_string(),
                ));
            }
            self.decode_with_keys(token, alg, &refreshed_keys)
                .map_err(|e| VerifyError::InvalidCredential(e.into_message()))?
        };

        let subject =
            validate_identity_text(&claims.sub, "sub").map_err(VerifyError::InvalidCredential)?;
        let is_worker_for_actor = parse_worker_for_actor(claims.worker_for_actor.as_ref());
        let (kind, actor_uid, actor_name, atespace) = match &claims.actor {
            None | Some(serde_json::Value::Null) => (CallerKind::OidcUser, None, None, None),
            Some(serde_json::Value::Object(map)) => {
                let atespace = parse_optional_claim_string(map, "atespace")
                    .map_err(VerifyError::InvalidCredential)?;
                let actor_name = parse_optional_claim_string(map, "actorName")
                    .map_err(VerifyError::InvalidCredential)?;
                let actor_uid = parse_optional_claim_string(map, "actorUID")
                    .map_err(VerifyError::InvalidCredential)?;
                (CallerKind::OidcActor, actor_uid, actor_name, atespace)
            }
            Some(_) => {
                return Err(VerifyError::InvalidCredential(
                    "actor.ate.dev claim must be a JSON object when present".to_string(),
                ));
            }
        };

        Ok(CallerIdentity {
            subject,
            actor_uid,
            actor_name,
            atespace,
            is_worker_for_actor,
            kind,
        })
    }

    /// Cryptographically verifies an OIDC JWT and parses Agent Substrate actor claims.
    pub async fn verify(&self, token: &str) -> Result<CallerIdentity, String> {
        self.verify_detailed(token)
            .await
            .map_err(VerifyError::into_message)
    }
}
