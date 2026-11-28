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

//! Authentication, OIDC/Kubernetes token verification, and actor policy for `jj-cc-server`.
//!
//! # Authority and Crash-Safety Ordering
//! [`ServerAuthLayer`] and [`BearerAuthInterceptor`] execute at the `tonic` service boundary
//! before any `Store` read or write runs, rejecting unauthenticated or unauthorized callers
//! before touching backend storage and inserting verified [`CallerIdentity`] into request
//! extensions for repository-suffix and commit-attribution enforcement.
//!
//! # Concurrency and Merge Invariants
//! Static tokens are SHA-256 hashed and compared in constant time across all configured secrets.
//! OIDC JWKS fetches and refreshes serialize behind a singleflight mutex with cooldown backoff so
//! unknown-`kid` bursts and IDP outages cannot DoS the IDP; Kubernetes `TokenReview` deduplicates
//! concurrent misses per SHA-256 digest, bounds outbound requests, caches projected SA tokens
//! briefly with HTTP 401 rotation retry, and caches positive results in a bounded LRU.
//!
//! # Cross-Process and RPC Boundaries
//! `/grpc.health.v1.Health/*` stays open for readiness probes; `worker-for-actor` credentials
//! are restricted to read-only RPCs (`Read*`, `Get*`, `List*`, `Resolve*`,
//! `CheckWorkingCopyChanges`), failing closed with `PERMISSION_DENIED` on mutating or unknown
//! methods. Non-loopback binds fail closed unless auth or `--allow-unauthenticated` is set.

use std::future::Future;
use std::io::Read as _;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Once};
use std::task::{Context, Poll};
use std::time::Duration;

use serde::de::DeserializeOwned;
use tonic::Status;
use tracing::{debug, warn};

mod bearer;
mod oidc;
#[cfg(test)]
mod tests;
mod tokenreview;

pub use bearer::{
    BearerAuthInterceptor, MAX_BEARER_TOKEN_BYTES, enforce_bind_auth_policy, is_loopback_host,
    load_tokens_from_file, resolve_listen_addrs, scrub_tokens,
};
use bearer::{ScrubbedToken, extract_http_bearer_token};
pub use oidc::{OidcAuthConfig, OidcValidator};
pub use tokenreview::{K8sTokenReviewConfig, K8sTokenReviewValidator};

/// Upper bound on an incoming Bearer credential (JWT or Kubernetes ServiceAccount token) in
/// [`ServerAuthLayer`].
pub const MAX_AUTH_TOKEN_BYTES: usize = 16 * 1024;

/// `Commit.external_ids` key storing the verified caller subject (`sub` or K8s username).
pub const ACTOR_SUB_EXTERNAL_ID_KEY: &str = "actor-sub";

/// `Commit.external_ids` key storing the verified actor or ServiceAccount UID when present.
pub const ACTOR_UID_EXTERNAL_ID_KEY: &str = "actor-uid";

const MAX_CA_CERT_FILE_BYTES: u64 = 1024 * 1024;
const MAX_HTTP_BODY_BYTES: usize = 1024 * 1024;
const MAX_ACTOR_EXTERNAL_ID_LEN: usize = 256;
const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Distinguishes how a gRPC caller was authenticated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallerKind {
    StaticToken,
    OidcActor,
    OidcUser,
    K8sServiceAccount,
}

/// Verified identity attached to gRPC request extensions by [`ServerAuthLayer`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CallerIdentity {
    pub subject: String,
    pub actor_uid: Option<String>,
    pub actor_name: Option<String>,
    pub atespace: Option<String>,
    pub is_worker_for_actor: bool,
    pub kind: CallerKind,
}

/// Combined server authentication configuration spanning static tokens, OIDC, and Kubernetes
/// `TokenReview`.
#[derive(Clone, Debug, Default)]
pub struct ServerAuthConfig {
    pub static_tokens: BearerAuthInterceptor,
    pub oidc: Option<OidcAuthConfig>,
    pub k8s_tokenreview: Option<K8sTokenReviewConfig>,
}

impl ServerAuthConfig {
    /// Returns `true` when any authentication backend is enabled.
    pub fn is_enabled(&self) -> bool {
        self.static_tokens.is_enabled() || self.oidc.is_some() || self.k8s_tokenreview.is_some()
    }

    /// Returns `true` when OIDC actor `repo_id` suffix enforcement is enabled.
    pub fn enforce_actor_repo_suffix(&self) -> bool {
        self.oidc
            .as_ref()
            .is_some_and(|oidc| oidc.enforce_actor_repo_suffix)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum VerifyError {
    InvalidCredential(String),
    Backend(String),
}

impl VerifyError {
    fn into_message(self) -> String {
        match self {
            Self::InvalidCredential(msg) | Self::Backend(msg) => msg,
        }
    }
}

fn ensure_crypto_providers() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        // Workspace feature unification enables both `ring` and `aws-lc-rs` on rustls 0.23,
        // so install ring once to match `cc-lib` and `jj-cc-daemon`.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let _ = jsonwebtoken::crypto::aws_lc::DEFAULT_PROVIDER.install_default();
    });
}

fn validate_endpoint_url(
    raw: &str,
    label: &str,
    allow_query: bool,
) -> Result<(String, url::Url), String> {
    if raw.bytes().any(|b| b.is_ascii_control()) {
        return Err(format!(
            "{label} URL must not contain ASCII control characters"
        ));
    }
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(format!("{label} must not be empty"));
    }
    let parsed =
        url::Url::parse(trimmed).map_err(|e| format!("invalid {label} URL '{trimmed}': {e}"))?;
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(format!("{label} URL must not contain embedded credentials"));
    }
    if !allow_query && parsed.query().is_some() {
        return Err(format!("{label} URL must not contain a query string"));
    }
    if parsed.fragment().is_some() {
        return Err(format!("{label} URL must not contain a URL fragment"));
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| format!("{label} URL '{trimmed}' must include a host"))?;
    match parsed.scheme() {
        "https" => {}
        "http" if is_loopback_host(host) => {}
        "http" => {
            return Err(format!(
                "refusing plaintext http:// {label} URL for non-loopback host '{host}'; use https://"
            ));
        }
        other => {
            return Err(format!(
                "unsupported scheme '{other}' in {label} URL (expected https:// or loopback http://)"
            ));
        }
    }
    Ok((trimmed.to_string(), parsed))
}

fn build_http_client(ca_cert: Option<&Path>) -> Result<reqwest::Client, String> {
    ensure_crypto_providers();
    let mut builder = reqwest::Client::builder()
        .connect_timeout(HTTP_CONNECT_TIMEOUT)
        .timeout(HTTP_REQUEST_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none());

    if let Some(ca_path) = ca_cert {
        let file = std::fs::File::open(ca_path)
            .map_err(|e| format!("failed to read CA cert '{}': {e}", ca_path.display()))?;
        let mut pem_bytes = Vec::new();
        file.take(MAX_CA_CERT_FILE_BYTES + 1)
            .read_to_end(&mut pem_bytes)
            .map_err(|e| format!("failed to read CA cert '{}': {e}", ca_path.display()))?;
        if pem_bytes.len() as u64 > MAX_CA_CERT_FILE_BYTES {
            return Err(format!(
                "CA cert '{}' exceeds maximum size of {MAX_CA_CERT_FILE_BYTES} bytes",
                ca_path.display()
            ));
        }
        let certs = reqwest::Certificate::from_pem_bundle(&pem_bytes)
            .map_err(|e| format!("invalid PEM CA bundle '{}': {e}", ca_path.display()))?;
        if certs.is_empty() {
            return Err(format!(
                "CA cert '{}' contains no PEM certificates",
                ca_path.display()
            ));
        }
        builder = builder.tls_certs_only(certs);
    }

    builder
        .build()
        .map_err(|e| format!("failed to construct HTTP client: {e}"))
}

async fn read_bounded_json<T: DeserializeOwned>(
    mut resp: reqwest::Response,
    max_bytes: usize,
    label: &str,
) -> Result<T, String> {
    if let Some(len) = resp.content_length()
        && len > max_bytes as u64
    {
        return Err(format!(
            "{label} response exceeds maximum size of {max_bytes} bytes"
        ));
    }
    let mut buf = Vec::new();
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| format!("failed to read {label} response body: {e}"))?
    {
        if buf.len().saturating_add(chunk.len()) > max_bytes {
            return Err(format!(
                "{label} response exceeds maximum size of {max_bytes} bytes"
            ));
        }
        buf.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&buf).map_err(|e| format!("failed to parse {label} JSON: {e}"))
}

fn validate_identity_text(value: &str, field: &str) -> Result<String, String> {
    if value.is_empty()
        || value.trim().is_empty()
        || value.len() > MAX_ACTOR_EXTERNAL_ID_LEN
        || value.bytes().any(|b| b.is_ascii_control())
    {
        return Err(format!(
            "invalid {field} claim (must be 1..={MAX_ACTOR_EXTERNAL_ID_LEN} non-control UTF-8 bytes)"
        ));
    }
    Ok(value.to_string())
}

/// Returns `true` when `path` targets the unauthenticated gRPC health service
/// (`/grpc.health.v1.Health/{Check,Watch}`).
pub fn is_health_check_path(path: &str) -> bool {
    matches!(
        path,
        "/grpc.health.v1.Health/Check" | "/grpc.health.v1.Health/Watch"
    )
}

/// Returns `true` when `path` is a known read-only Commit Cloud gRPC method.
///
/// Unknown services and methods fail closed (`false`) so `worker-for-actor` credentials can
/// never invoke mutating or newly introduced write endpoints.
pub fn is_read_only_grpc_path(path: &str) -> bool {
    matches!(
        path,
        "/commit_cloud.backend.BackendService/ReadCommit"
            | "/commit_cloud.backend.BackendService/ListCommits"
            | "/commit_cloud.backend.BackendService/ResolveCommitPrefix"
            | "/commit_cloud.backend.BackendService/ResolveChangePrefix"
            | "/commit_cloud.backend.BackendService/ReadTree"
            | "/commit_cloud.backend.BackendService/ReadFile"
            | "/commit_cloud.backend.BackendService/ReadSymlink"
            | "/commit_cloud.op_store.OpStoreService/ReadOperation"
            | "/commit_cloud.op_store.OpStoreService/ResolveOperationPrefix"
            | "/commit_cloud.op_store.OpStoreService/ReadView"
            | "/commit_cloud.op_store.OpStoreService/GetOpHeads"
            | "/commit_cloud.workspace.WorkspaceService/GetWorkspace"
            | "/commit_cloud.workspace.WorkspaceService/ListWorkspaces"
            | "/commit_cloud.workspace.WorkspaceService/CheckWorkingCopyChanges"
    )
}

fn is_single_alnum_segment(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric())
}

/// Enforces that OIDC actor callers only write to repositories matching the injective
/// `<alnum_prefix>_<atespace>_<actorName>` naming grammar:
///
/// - Both `atespace` and `actorName` claims must be present, non-empty, and contain no `'_'`
///   delimiter (DNS-1123 Kubernetes atespaces and actor names use `[a-z0-9-]`).
/// - `repo_id` must split on `'_'` into exactly three segments `[prefix, repo_atespace,
///   repo_actor]` where `prefix` is a non-empty ASCII alphanumeric segment,
///   `repo_atespace == atespace`, and `repo_actor == actorName`.
///
/// Because `'_'` is rejected in both claims and `repo_id` decomposes uniquely on `'_'` into three
/// parts, no two distinct `(atespace, actorName)` tuples can ever match the same `repo_id`.
///
/// Non-actor callers (`StaticToken`, `OidcUser`, `K8sServiceAccount`) and unauthenticated
/// callers are unaffected, as are all callers when `enforce` is `false`.
pub fn enforce_actor_repo_suffix(
    identity: Option<&CallerIdentity>,
    enforce: bool,
    repo_id: &str,
) -> Result<(), Status> {
    if !enforce {
        return Ok(());
    }
    let Some(caller) = identity else {
        return Ok(());
    };
    if caller.kind != CallerKind::OidcActor {
        return Ok(());
    }
    let Some(atespace) = caller
        .atespace
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    else {
        return Err(Status::permission_denied(
            "OIDC actor token is missing atespace claim required for repository writes",
        ));
    };
    let Some(actor_name) = caller
        .actor_name
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    else {
        return Err(Status::permission_denied(
            "OIDC actor token is missing actorName claim required for repository writes",
        ));
    };
    if atespace.contains('_') || actor_name.contains('_') {
        return Err(Status::permission_denied(
            "OIDC actor atespace and actorName claims must not contain '_'",
        ));
    }

    let mut parts = repo_id.split('_');
    let matches_format = match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(prefix), Some(repo_atespace), Some(repo_actor), None) => {
            is_single_alnum_segment(prefix) && repo_atespace == atespace && repo_actor == actor_name
        }
        _ => false,
    };
    if matches_format {
        Ok(())
    } else {
        Err(Status::permission_denied(
            "OIDC actor is not authorized to write to this repository",
        ))
    }
}

/// Stamps server-verified `actor-sub` and optional `actor-uid` provenance entries into
/// `commit.external_ids`, unconditionally overwriting or clearing any client-supplied values so
/// callers cannot spoof actor attribution.
pub fn apply_commit_actor_attribution(
    identity: Option<&CallerIdentity>,
    commit: &mut cc_common::backend::Commit,
) {
    commit.external_ids.remove(ACTOR_SUB_EXTERNAL_ID_KEY);
    commit.external_ids.remove(ACTOR_UID_EXTERNAL_ID_KEY);

    let Some(caller) = identity else {
        return;
    };
    if !matches!(
        caller.kind,
        CallerKind::OidcActor | CallerKind::OidcUser | CallerKind::K8sServiceAccount
    ) {
        return;
    }

    if !caller.subject.is_empty() {
        commit.external_ids.insert(
            ACTOR_SUB_EXTERNAL_ID_KEY.to_string(),
            caller.subject.as_bytes().to_vec(),
        );
    }
    if let Some(uid) = caller
        .actor_uid
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        commit.external_ids.insert(
            ACTOR_UID_EXTERNAL_ID_KEY.to_string(),
            uid.as_bytes().to_vec(),
        );
    }
}

struct ServerAuthState {
    static_tokens: BearerAuthInterceptor,
    oidc: Option<OidcValidator>,
    k8s_tokenreview: Option<K8sTokenReviewValidator>,
}

impl std::fmt::Debug for ServerAuthState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerAuthState")
            .field("static_tokens", &self.static_tokens)
            .field("oidc", &self.oidc)
            .field("k8s_tokenreview", &self.k8s_tokenreview)
            .finish()
    }
}

impl ServerAuthState {
    fn is_enabled(&self) -> bool {
        self.static_tokens.is_enabled() || self.oidc.is_some() || self.k8s_tokenreview.is_some()
    }

    fn enforce_actor_repo_suffix(&self) -> bool {
        self.oidc
            .as_ref()
            .is_some_and(OidcValidator::enforce_actor_repo_suffix)
    }

    async fn authenticate_bearer_token(&self, token: &str) -> Result<CallerIdentity, Status> {
        if self.static_tokens.verify_token_str(token) {
            return Ok(CallerIdentity {
                subject: "static-bearer-token".to_string(),
                actor_uid: None,
                actor_name: None,
                atespace: None,
                is_worker_for_actor: false,
                kind: CallerKind::StaticToken,
            });
        }

        if let Some(oidc) = &self.oidc
            && oidc.unverified_issuer_matches(token)
        {
            // Tokens claiming the configured OIDC issuer are authoritative for the OIDC
            // validator and must never fall through to Kubernetes TokenReview (which does
            // not parse Substrate actor or worker-for-actor claims).
            return match oidc.verify_detailed(token).await {
                Ok(identity) => Ok(identity),
                Err(VerifyError::Backend(reason)) => {
                    warn!("OIDC JWT verification backend failure: {reason}");
                    Err(Status::unauthenticated("invalid bearer token"))
                }
                Err(VerifyError::InvalidCredential(reason)) => {
                    debug!("OIDC JWT verification rejected credential: {reason}");
                    Err(Status::unauthenticated("invalid bearer token"))
                }
            };
        }

        if let Some(k8s) = &self.k8s_tokenreview {
            match k8s.verify_detailed(token).await {
                Ok(identity) => return Ok(identity),
                Err(VerifyError::Backend(reason)) => {
                    warn!("Kubernetes TokenReview backend failure: {reason}");
                }
                Err(VerifyError::InvalidCredential(reason)) => {
                    debug!("Kubernetes TokenReview rejected credential: {reason}");
                }
            }
        }

        Err(Status::unauthenticated("invalid bearer token"))
    }
}

/// Async `tower::Layer` authenticating gRPC requests against static tokens, OIDC JWKS, and
/// Kubernetes `TokenReview`, enforcing `worker-for-actor` read-only access, and injecting
/// [`CallerIdentity`] into request extensions.
#[derive(Clone, Debug)]
pub struct ServerAuthLayer {
    inner: Arc<ServerAuthState>,
}

impl ServerAuthLayer {
    pub fn new(config: ServerAuthConfig) -> Result<Self, String> {
        let oidc = config.oidc.map(OidcValidator::new).transpose()?;
        let k8s_tokenreview = config
            .k8s_tokenreview
            .map(K8sTokenReviewValidator::new)
            .transpose()?;
        Ok(Self {
            inner: Arc::new(ServerAuthState {
                static_tokens: config.static_tokens,
                oidc,
                k8s_tokenreview,
            }),
        })
    }

    /// Constructs a [`ServerAuthLayer`] from pre-built validators (useful for custom cache TTLs
    /// and cooldown timings).
    pub fn from_validators(
        static_tokens: BearerAuthInterceptor,
        oidc: Option<OidcValidator>,
        k8s_tokenreview: Option<K8sTokenReviewValidator>,
    ) -> Self {
        Self {
            inner: Arc::new(ServerAuthState {
                static_tokens,
                oidc,
                k8s_tokenreview,
            }),
        }
    }

    pub fn disabled() -> Self {
        Self {
            inner: Arc::new(ServerAuthState {
                static_tokens: BearerAuthInterceptor::default(),
                oidc: None,
                k8s_tokenreview: None,
            }),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.inner.is_enabled()
    }

    pub fn enforce_actor_repo_suffix(&self) -> bool {
        self.inner.enforce_actor_repo_suffix()
    }
}

impl<S> tower::Layer<S> for ServerAuthLayer {
    type Service = ServerAuthService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ServerAuthService {
            inner,
            auth: Arc::clone(&self.inner),
        }
    }
}

/// `tower::Service` produced by [`ServerAuthLayer`].
#[derive(Clone, Debug)]
pub struct ServerAuthService<S> {
    inner: S,
    auth: Arc<ServerAuthState>,
}

impl<S, ReqBody> tower::Service<http::Request<ReqBody>> for ServerAuthService<S>
where
    ReqBody: Send + 'static,
    S: tower::Service<http::Request<ReqBody>, Response = http::Response<tonic::body::BoxBody>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: http::Request<ReqBody>) -> Self::Future {
        // Swap with a fresh clone so the instance that was polled ready handles the request.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let auth = Arc::clone(&self.auth);

        Box::pin(async move {
            let path = req.uri().path();
            if is_health_check_path(path) || !auth.is_enabled() {
                return inner.call(req).await;
            }

            let token = match extract_http_bearer_token(req.headers()) {
                Ok(tok) => ScrubbedToken::new(tok.to_string()),
                Err(status) => return Ok(status.into_http()),
            };

            let auth_res = auth.authenticate_bearer_token(token.as_str()).await;
            drop(token);

            match auth_res {
                Ok(identity) => {
                    if identity.is_worker_for_actor && !is_read_only_grpc_path(path) {
                        return Ok(Status::permission_denied(
                            "worker-for-actor credentials are restricted to read-only RPCs",
                        )
                        .into_http());
                    }
                    req.extensions_mut().insert(identity);
                    inner.call(req).await
                }
                Err(status) => Ok(status.into_http()),
            }
        })
    }
}
