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

//! End-to-end integration tests for [`ServerAuthLayer`] over a live `tonic::transport::Server`
//! backed by mock OIDC Discovery/JWKS and Kubernetes `TokenReview` HTTP endpoints.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_lc_rs::encoding::{AsDer, Pkcs8V1Der};
use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::rsa::{KeyPair as RsaKeyPair, KeySize};
use aws_lc_rs::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair};
use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine as _;
use cc_common::backend::backend_service_client::BackendServiceClient;
use cc_common::backend::backend_service_server::{BackendService, BackendServiceServer};
use cc_common::backend::{
    Commit, DeleteRepositoryRequest, DeleteRepositoryResponse, ListCommitsRequest,
    ListCommitsResponse, ReadCommitRequest, ReadCommitResponse, ReadFileRequest, ReadFileResponse,
    ReadSymlinkRequest, ReadSymlinkResponse, ReadTreeRequest, ReadTreeResponse,
    RegisterProjectRequest, RegisterProjectResponse, RegisterRepositoryRequest,
    RegisterRepositoryResponse, ResolveChangePrefixRequest, ResolveChangePrefixResponse,
    ResolveCommitPrefixRequest, ResolveCommitPrefixResponse, Signature, Timestamp,
    WriteCommitRequest, WriteCommitResponse, WriteFileChunk, WriteFileRequest, WriteFileResponse,
    WriteSymlinkRequest, WriteSymlinkResponse, WriteTreeRequest, WriteTreeResponse,
};
use cc_common::op_store::op_store_service_client::OpStoreServiceClient;
use cc_common::op_store::op_store_service_server::{OpStoreService, OpStoreServiceServer};
use cc_common::op_store::{
    GetOpHeadsRequest, GetOpHeadsResponse, PublishOperationRequest, PublishOperationResponse,
    ReadOperationRequest, ReadOperationResponse, ReadViewRequest, ReadViewResponse,
    ReconcileOpHeadsRequest, ReconcileOpHeadsResponse, ResolveOperationPrefixRequest,
    ResolveOperationPrefixResponse, UpdateOpHeadsRequest, UpdateOpHeadsResponse,
    WriteOperationRequest, WriteOperationResponse, WriteViewRequest, WriteViewResponse,
};
use cc_common::workspace::workspace_service_client::WorkspaceServiceClient;
use cc_common::workspace::workspace_service_server::{WorkspaceService, WorkspaceServiceServer};
use cc_common::workspace::{
    CheckWorkingCopyChangesRequest, CheckWorkingCopyChangesResponse, DeleteWorkspaceRequest,
    DeleteWorkspaceResponse, GetWorkspaceRequest, GetWorkspaceResponse, ListWorkspacesRequest,
    ListWorkspacesResponse, UpdateWorkspaceRequest, UpdateWorkspaceResponse, WorkspaceState,
};
use futures::Stream;
use jj_commit_cloud_server::auth::{
    ACTOR_SUB_EXTERNAL_ID_KEY, ACTOR_UID_EXTERNAL_ID_KEY, BearerAuthInterceptor, CallerIdentity,
    CallerKind, K8sTokenReviewConfig, K8sTokenReviewValidator, MAX_AUTH_TOKEN_BYTES,
    OidcAuthConfig, OidcValidator, ServerAuthConfig, ServerAuthLayer,
    apply_commit_actor_attribution, enforce_actor_repo_suffix,
};
use jj_commit_cloud_server::hash_utils::verify_or_compute_git_commit_hash;
use jsonwebtoken::jwk::{Jwk, JwkSet, PublicKeyUse};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Code, Request, Response, Status, Streaming};
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_client::HealthClient;

struct TestSigningKey {
    kid: String,
    alg: Algorithm,
    encoding_key: EncodingKey,
    jwk: Jwk,
}

impl TestSigningKey {
    fn generate_rs256(kid: &str) -> Self {
        let _ = jsonwebtoken::crypto::aws_lc::DEFAULT_PROVIDER.install_default();
        let kp = RsaKeyPair::generate(KeySize::Rsa2048).expect("RSA-2048 generation");
        let pkcs8: Pkcs8V1Der<'static> = kp.as_der().expect("PKCS#8 DER export");
        let b64 = base64::engine::general_purpose::STANDARD.encode(pkcs8.as_ref());
        let mut pem = String::from("-----BEGIN PRIVATE KEY-----\n");
        for chunk in b64.as_bytes().chunks(64) {
            pem.push_str(std::str::from_utf8(chunk).unwrap());
            pem.push('\n');
        }
        pem.push_str("-----END PRIVATE KEY-----\n");
        let encoding_key = EncodingKey::from_rsa_pem(pem.as_bytes()).expect("RSA EncodingKey");
        let mut jwk =
            Jwk::from_encoding_key(&encoding_key, Algorithm::RS256).expect("RS256 JWK export");
        jwk.common.key_id = Some(kid.to_string());
        jwk.common.public_key_use = Some(PublicKeyUse::Signature);
        Self {
            kid: kid.to_string(),
            alg: Algorithm::RS256,
            encoding_key,
            jwk,
        }
    }

    fn generate_es256(kid: &str) -> Self {
        let _ = jsonwebtoken::crypto::aws_lc::DEFAULT_PROVIDER.install_default();
        let rng = SystemRandom::new();
        let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng)
            .expect("P-256 generation");
        let encoding_key = EncodingKey::from_ec_der(pkcs8.as_ref());
        let mut jwk =
            Jwk::from_encoding_key(&encoding_key, Algorithm::ES256).expect("ES256 JWK export");
        jwk.common.key_id = Some(kid.to_string());
        jwk.common.public_key_use = Some(PublicKeyUse::Signature);
        Self {
            kid: kid.to_string(),
            alg: Algorithm::ES256,
            encoding_key,
            jwk,
        }
    }

    fn sign(&self, claims: &Value) -> String {
        let mut header = Header::new(self.alg);
        header.kid = Some(self.kid.clone());
        jsonwebtoken::encode(&header, claims, &self.encoding_key).expect("sign JWT")
    }

    fn sign_without_kid(&self, claims: &Value) -> String {
        let header = Header::new(self.alg);
        jsonwebtoken::encode(&header, claims, &self.encoding_key).expect("sign JWT")
    }

    fn sign_with_crit(&self, claims: &Value, crit: Vec<String>) -> String {
        let mut header = Header::new(self.alg);
        header.kid = Some(self.kid.clone());
        header.crit = Some(crit);
        jsonwebtoken::encode(&header, claims, &self.encoding_key).expect("sign JWT")
    }
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_secs()
}

struct MockOidcState {
    issuer: String,
    jwks_path: RwLock<String>,
    jwks: RwLock<JwkSet>,
    jwks_delay: RwLock<Duration>,
    outage: AtomicBool,
    jwks_fetches: AtomicUsize,
    discovery_fetches: AtomicUsize,
}

struct MockOidcServer {
    issuer: String,
    state: Arc<MockOidcState>,
    _task: tokio::task::JoinHandle<()>,
}

impl MockOidcServer {
    async fn start(initial_keys: Vec<Jwk>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let issuer = format!("http://{addr}");
        let state = Arc::new(MockOidcState {
            issuer: issuer.clone(),
            jwks_path: RwLock::new("/jwks.json".to_string()),
            jwks: RwLock::new(JwkSet { keys: initial_keys }),
            jwks_delay: RwLock::new(Duration::ZERO),
            outage: AtomicBool::new(false),
            jwks_fetches: AtomicUsize::new(0),
            discovery_fetches: AtomicUsize::new(0),
        });
        let serve_jwks = |st: Arc<MockOidcState>, path: &'static str| async move {
            let delay = *st.jwks_delay.read().unwrap();
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            if st.outage.load(Ordering::SeqCst) {
                st.jwks_fetches.fetch_add(1, Ordering::SeqCst);
                return (
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({ "error": "jwks unavailable" })),
                );
            }
            let active = st.jwks_path.read().unwrap().clone();
            if active != path {
                return (
                    axum::http::StatusCode::NOT_FOUND,
                    Json(json!({ "error": "retired jwks_uri" })),
                );
            }
            st.jwks_fetches.fetch_add(1, Ordering::SeqCst);
            let jwks = st.jwks.read().unwrap().clone();
            (
                axum::http::StatusCode::OK,
                Json(serde_json::to_value(jwks).unwrap()),
            )
        };
        let app = Router::new()
            .route(
                "/.well-known/openid-configuration",
                get(|State(st): State<Arc<MockOidcState>>| async move {
                    if st.outage.load(Ordering::SeqCst) {
                        let delay = *st.jwks_delay.read().unwrap();
                        if !delay.is_zero() {
                            tokio::time::sleep(delay).await;
                        }
                        st.discovery_fetches.fetch_add(1, Ordering::SeqCst);
                        return (
                            axum::http::StatusCode::SERVICE_UNAVAILABLE,
                            Json(json!({ "error": "discovery unavailable" })),
                        );
                    }
                    st.discovery_fetches.fetch_add(1, Ordering::SeqCst);
                    let path = st.jwks_path.read().unwrap().clone();
                    (
                        axum::http::StatusCode::OK,
                        Json(json!({
                            "issuer": st.issuer,
                            "jwks_uri": format!("{}{}", st.issuer, path),
                        })),
                    )
                }),
            )
            .route(
                "/jwks.json",
                get(move |State(st): State<Arc<MockOidcState>>| serve_jwks(st, "/jwks.json")),
            )
            .route(
                "/jwks-v2.json",
                get(move |State(st): State<Arc<MockOidcState>>| serve_jwks(st, "/jwks-v2.json")),
            )
            .with_state(Arc::clone(&state));
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self {
            issuer,
            state,
            _task: task,
        }
    }

    fn set_keys(&self, keys: Vec<Jwk>) {
        *self.state.jwks.write().unwrap() = JwkSet { keys };
    }

    fn set_jwks_delay(&self, delay: Duration) {
        *self.state.jwks_delay.write().unwrap() = delay;
    }

    fn set_outage(&self, outage: bool) {
        self.state.outage.store(outage, Ordering::SeqCst);
    }

    fn rotate_jwks_path(&self, new_path: &str, keys: Vec<Jwk>) {
        *self.state.jwks.write().unwrap() = JwkSet { keys };
        *self.state.jwks_path.write().unwrap() = new_path.to_string();
    }

    fn jwks_fetches(&self) -> usize {
        self.state.jwks_fetches.load(Ordering::SeqCst)
    }

    fn discovery_fetches(&self) -> usize {
        self.state.discovery_fetches.load(Ordering::SeqCst)
    }
}

struct MockK8sReviewEntry {
    username: String,
    uid: Option<String>,
    required_audience: Option<String>,
    response_audiences: Option<Vec<String>>,
}

struct MockK8sState {
    expected_sa_token: RwLock<Option<String>>,
    valid_tokens: RwLock<HashMap<String, MockK8sReviewEntry>>,
    delay: RwLock<Duration>,
    requests: AtomicUsize,
    unauthorized_responses: AtomicUsize,
    last_auth_header: Mutex<Option<String>>,
    last_request_body: Mutex<Option<Value>>,
}

struct MockK8sServer {
    base_url: String,
    state: Arc<MockK8sState>,
    _task: tokio::task::JoinHandle<()>,
}

impl MockK8sServer {
    async fn start(expected_sa_token: Option<String>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");
        let state = Arc::new(MockK8sState {
            expected_sa_token: RwLock::new(expected_sa_token),
            valid_tokens: RwLock::new(HashMap::new()),
            delay: RwLock::new(Duration::ZERO),
            requests: AtomicUsize::new(0),
            unauthorized_responses: AtomicUsize::new(0),
            last_auth_header: Mutex::new(None),
            last_request_body: Mutex::new(None),
        });
        let app = Router::new()
            .route(
                "/apis/authentication.k8s.io/v1/tokenreviews",
                post(
                    |State(st): State<Arc<MockK8sState>>,
                     headers: HeaderMap,
                     Json(body): Json<Value>| async move {
                        st.requests.fetch_add(1, Ordering::SeqCst);
                        let delay = *st.delay.read().unwrap();
                        if !delay.is_zero() {
                            tokio::time::sleep(delay).await;
                        }
                        let auth_header = headers
                            .get("authorization")
                            .and_then(|v| v.to_str().ok())
                            .map(str::to_string);
                        *st.last_auth_header.lock().unwrap() = auth_header.clone();
                        *st.last_request_body.lock().unwrap() = Some(body.clone());

                        if let Some(expected) = st.expected_sa_token.read().unwrap().as_deref()
                            && auth_header.as_deref() != Some(&format!("Bearer {expected}"))
                        {
                            st.unauthorized_responses.fetch_add(1, Ordering::SeqCst);
                            return (
                                axum::http::StatusCode::UNAUTHORIZED,
                                Json(json!({ "status": { "authenticated": false } })),
                            );
                        }

                        let api_ok = body.get("apiVersion").and_then(Value::as_str)
                            == Some("authentication.k8s.io/v1");
                        let kind_ok =
                            body.get("kind").and_then(Value::as_str) == Some("TokenReview");
                        let token = body
                            .pointer("/spec/token")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        let audiences: Vec<&str> = body
                            .pointer("/spec/audiences")
                            .and_then(Value::as_array)
                            .map(|arr| arr.iter().filter_map(Value::as_str).collect())
                            .unwrap_or_default();

                        if !api_ok || !kind_ok {
                            return (
                                axum::http::StatusCode::BAD_REQUEST,
                                Json(json!({ "status": { "authenticated": false } })),
                            );
                        }

                        let map = st.valid_tokens.read().unwrap();
                        if let Some(entry) = map.get(token) {
                            let aud_ok = match entry.required_audience.as_deref() {
                                Some(req_aud) => audiences.contains(&req_aud),
                                None => true,
                            };
                            if aud_ok {
                                let mut status = json!({
                                    "authenticated": true,
                                    "user": {
                                        "username": entry.username,
                                        "uid": entry.uid,
                                    }
                                });
                                if let Some(resp_auds) = &entry.response_audiences {
                                    status["audiences"] = json!(resp_auds);
                                }
                                return (
                                    axum::http::StatusCode::CREATED,
                                    Json(json!({
                                        "apiVersion": "authentication.k8s.io/v1",
                                        "kind": "TokenReview",
                                        "status": status,
                                    })),
                                );
                            }
                        }

                        (
                            axum::http::StatusCode::CREATED,
                            Json(json!({
                                "apiVersion": "authentication.k8s.io/v1",
                                "kind": "TokenReview",
                                "status": {
                                    "authenticated": false,
                                    "error": "invalid token"
                                }
                            })),
                        )
                    },
                ),
            )
            .with_state(Arc::clone(&state));
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self {
            base_url,
            state,
            _task: task,
        }
    }

    fn register_token(
        &self,
        token: &str,
        username: &str,
        uid: Option<&str>,
        required_audience: Option<&str>,
    ) {
        self.register_token_with_response_audiences(token, username, uid, required_audience, None);
    }

    fn register_token_with_response_audiences(
        &self,
        token: &str,
        username: &str,
        uid: Option<&str>,
        required_audience: Option<&str>,
        response_audiences: Option<Vec<String>>,
    ) {
        self.state.valid_tokens.write().unwrap().insert(
            token.to_string(),
            MockK8sReviewEntry {
                username: username.to_string(),
                uid: uid.map(str::to_string),
                required_audience: required_audience.map(str::to_string),
                response_audiences,
            },
        );
    }

    fn revoke_token(&self, token: &str) {
        self.state.valid_tokens.write().unwrap().remove(token);
    }

    fn set_delay(&self, delay: Duration) {
        *self.state.delay.write().unwrap() = delay;
    }

    fn set_expected_sa_token(&self, token: Option<String>) {
        *self.state.expected_sa_token.write().unwrap() = token;
    }

    fn requests(&self) -> usize {
        self.state.requests.load(Ordering::SeqCst)
    }

    fn unauthorized_responses(&self) -> usize {
        self.state.unauthorized_responses.load(Ordering::SeqCst)
    }
}

/// Captures the `CallerIdentity` injected by `ServerAuthLayer` on every gRPC call.
#[derive(Clone)]
struct SpyServices {
    last_identity: Arc<Mutex<Option<CallerIdentity>>>,
    enforce_repo_suffix: bool,
}

impl SpyServices {
    fn new(enforce_repo_suffix: bool) -> Self {
        Self {
            last_identity: Arc::new(Mutex::new(None)),
            enforce_repo_suffix,
        }
    }

    fn record_identity<T>(&self, req: &Request<T>) -> Option<CallerIdentity> {
        let id = req.extensions().get::<CallerIdentity>().cloned();
        *self.last_identity.lock().unwrap() = id.clone();
        id
    }

    fn take_last_identity(&self) -> Option<CallerIdentity> {
        self.last_identity.lock().unwrap().take()
    }
}

#[tonic::async_trait]
impl BackendService for SpyServices {
    async fn register_project(
        &self,
        req: Request<RegisterProjectRequest>,
    ) -> Result<Response<RegisterProjectResponse>, Status> {
        self.record_identity(&req);
        Ok(Response::new(RegisterProjectResponse {
            project_id: "proj".to_string(),
        }))
    }

    async fn register_repository(
        &self,
        req: Request<RegisterRepositoryRequest>,
    ) -> Result<Response<RegisterRepositoryResponse>, Status> {
        let id = self.record_identity(&req);
        let repo_id = req
            .get_ref()
            .repo_id
            .clone()
            .unwrap_or_else(|| "default".to_string());
        enforce_actor_repo_suffix(id.as_ref(), self.enforce_repo_suffix, &repo_id)?;
        Ok(Response::new(RegisterRepositoryResponse {
            repo_id,
            project_id: "proj".to_string(),
        }))
    }

    async fn delete_repository(
        &self,
        req: Request<DeleteRepositoryRequest>,
    ) -> Result<Response<DeleteRepositoryResponse>, Status> {
        let id = self.record_identity(&req);
        enforce_actor_repo_suffix(
            id.as_ref(),
            self.enforce_repo_suffix,
            &req.get_ref().repo_id,
        )?;
        Ok(Response::new(DeleteRepositoryResponse { existed: true }))
    }

    async fn read_commit(
        &self,
        req: Request<ReadCommitRequest>,
    ) -> Result<Response<ReadCommitResponse>, Status> {
        self.record_identity(&req);
        Ok(Response::new(ReadCommitResponse { commit: None }))
    }

    async fn write_commit(
        &self,
        req: Request<WriteCommitRequest>,
    ) -> Result<Response<WriteCommitResponse>, Status> {
        let id = self.record_identity(&req);
        let mut commit = req
            .into_inner()
            .commit
            .ok_or_else(|| Status::invalid_argument("missing commit"))?;
        apply_commit_actor_attribution(id.as_ref(), &mut commit);
        let commit_id =
            verify_or_compute_git_commit_hash(&mut commit).map_err(Status::invalid_argument)?;
        Ok(Response::new(WriteCommitResponse { commit_id }))
    }

    async fn list_commits(
        &self,
        req: Request<ListCommitsRequest>,
    ) -> Result<Response<ListCommitsResponse>, Status> {
        self.record_identity(&req);
        Ok(Response::new(ListCommitsResponse::default()))
    }

    async fn resolve_commit_prefix(
        &self,
        req: Request<ResolveCommitPrefixRequest>,
    ) -> Result<Response<ResolveCommitPrefixResponse>, Status> {
        self.record_identity(&req);
        Ok(Response::new(ResolveCommitPrefixResponse::default()))
    }

    async fn resolve_change_prefix(
        &self,
        req: Request<ResolveChangePrefixRequest>,
    ) -> Result<Response<ResolveChangePrefixResponse>, Status> {
        self.record_identity(&req);
        Ok(Response::new(ResolveChangePrefixResponse::default()))
    }

    async fn read_tree(
        &self,
        req: Request<ReadTreeRequest>,
    ) -> Result<Response<ReadTreeResponse>, Status> {
        self.record_identity(&req);
        Ok(Response::new(ReadTreeResponse::default()))
    }

    async fn write_tree(
        &self,
        req: Request<WriteTreeRequest>,
    ) -> Result<Response<WriteTreeResponse>, Status> {
        self.record_identity(&req);
        Ok(Response::new(WriteTreeResponse::default()))
    }

    type ReadFileStream = Pin<Box<dyn Stream<Item = Result<ReadFileResponse, Status>> + Send>>;

    async fn read_file(
        &self,
        req: Request<ReadFileRequest>,
    ) -> Result<Response<Self::ReadFileStream>, Status> {
        self.record_identity(&req);
        Ok(Response::new(Box::pin(tokio_stream::empty())))
    }

    async fn write_file(
        &self,
        req: Request<WriteFileRequest>,
    ) -> Result<Response<WriteFileResponse>, Status> {
        self.record_identity(&req);
        Ok(Response::new(WriteFileResponse::default()))
    }

    async fn write_file_stream(
        &self,
        req: Request<Streaming<WriteFileChunk>>,
    ) -> Result<Response<WriteFileResponse>, Status> {
        self.record_identity(&req);
        Ok(Response::new(WriteFileResponse::default()))
    }

    async fn read_symlink(
        &self,
        req: Request<ReadSymlinkRequest>,
    ) -> Result<Response<ReadSymlinkResponse>, Status> {
        self.record_identity(&req);
        Ok(Response::new(ReadSymlinkResponse::default()))
    }

    async fn write_symlink(
        &self,
        req: Request<WriteSymlinkRequest>,
    ) -> Result<Response<WriteSymlinkResponse>, Status> {
        self.record_identity(&req);
        Ok(Response::new(WriteSymlinkResponse::default()))
    }
}

#[tonic::async_trait]
impl OpStoreService for SpyServices {
    async fn read_operation(
        &self,
        req: Request<ReadOperationRequest>,
    ) -> Result<Response<ReadOperationResponse>, Status> {
        self.record_identity(&req);
        Ok(Response::new(ReadOperationResponse::default()))
    }

    async fn write_operation(
        &self,
        req: Request<WriteOperationRequest>,
    ) -> Result<Response<WriteOperationResponse>, Status> {
        self.record_identity(&req);
        Ok(Response::new(WriteOperationResponse::default()))
    }

    async fn resolve_operation_prefix(
        &self,
        req: Request<ResolveOperationPrefixRequest>,
    ) -> Result<Response<ResolveOperationPrefixResponse>, Status> {
        self.record_identity(&req);
        Ok(Response::new(ResolveOperationPrefixResponse::default()))
    }

    async fn read_view(
        &self,
        req: Request<ReadViewRequest>,
    ) -> Result<Response<ReadViewResponse>, Status> {
        self.record_identity(&req);
        Ok(Response::new(ReadViewResponse::default()))
    }

    async fn write_view(
        &self,
        req: Request<WriteViewRequest>,
    ) -> Result<Response<WriteViewResponse>, Status> {
        self.record_identity(&req);
        Ok(Response::new(WriteViewResponse::default()))
    }

    async fn get_op_heads(
        &self,
        req: Request<GetOpHeadsRequest>,
    ) -> Result<Response<GetOpHeadsResponse>, Status> {
        self.record_identity(&req);
        Ok(Response::new(GetOpHeadsResponse::default()))
    }

    async fn update_op_heads(
        &self,
        req: Request<UpdateOpHeadsRequest>,
    ) -> Result<Response<UpdateOpHeadsResponse>, Status> {
        self.record_identity(&req);
        Ok(Response::new(UpdateOpHeadsResponse::default()))
    }

    async fn reconcile_op_heads(
        &self,
        req: Request<ReconcileOpHeadsRequest>,
    ) -> Result<Response<ReconcileOpHeadsResponse>, Status> {
        self.record_identity(&req);
        Ok(Response::new(ReconcileOpHeadsResponse::default()))
    }

    async fn publish_operation(
        &self,
        req: Request<PublishOperationRequest>,
    ) -> Result<Response<PublishOperationResponse>, Status> {
        let id = self.record_identity(&req);
        enforce_actor_repo_suffix(
            id.as_ref(),
            self.enforce_repo_suffix,
            &req.get_ref().repo_id,
        )?;
        Ok(Response::new(PublishOperationResponse::default()))
    }
}

#[tonic::async_trait]
impl WorkspaceService for SpyServices {
    async fn get_workspace(
        &self,
        req: Request<GetWorkspaceRequest>,
    ) -> Result<Response<GetWorkspaceResponse>, Status> {
        self.record_identity(&req);
        Ok(Response::new(GetWorkspaceResponse::default()))
    }

    async fn update_workspace(
        &self,
        req: Request<UpdateWorkspaceRequest>,
    ) -> Result<Response<UpdateWorkspaceResponse>, Status> {
        let id = self.record_identity(&req);
        if let Some(ws) = &req.get_ref().workspace {
            enforce_actor_repo_suffix(id.as_ref(), self.enforce_repo_suffix, &ws.repo_id)?;
        }
        Ok(Response::new(UpdateWorkspaceResponse {
            success: true,
            workspace: None,
        }))
    }

    async fn list_workspaces(
        &self,
        req: Request<ListWorkspacesRequest>,
    ) -> Result<Response<ListWorkspacesResponse>, Status> {
        self.record_identity(&req);
        Ok(Response::new(ListWorkspacesResponse::default()))
    }

    async fn check_working_copy_changes(
        &self,
        req: Request<CheckWorkingCopyChangesRequest>,
    ) -> Result<Response<CheckWorkingCopyChangesResponse>, Status> {
        self.record_identity(&req);
        Ok(Response::new(CheckWorkingCopyChangesResponse::default()))
    }

    async fn delete_workspace(
        &self,
        req: Request<DeleteWorkspaceRequest>,
    ) -> Result<Response<DeleteWorkspaceResponse>, Status> {
        let id = self.record_identity(&req);
        enforce_actor_repo_suffix(
            id.as_ref(),
            self.enforce_repo_suffix,
            &req.get_ref().repo_id,
        )?;
        Ok(Response::new(DeleteWorkspaceResponse { existed: false }))
    }
}

struct TestGrpcServer {
    channel: Channel,
    spy: SpyServices,
    _task: tokio::task::JoinHandle<()>,
}

impl TestGrpcServer {
    async fn start(auth_layer: ServerAuthLayer) -> Self {
        let spy = SpyServices::new(auth_layer.enforce_actor_repo_suffix());
        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_service_status("", tonic_health::ServingStatus::Serving)
            .await;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = TcpListenerStream::new(listener);
        let spy_clone = spy.clone();

        let task = tokio::spawn(async move {
            let _ = Server::builder()
                .http2_max_header_list_size(64 * 1024)
                .layer(auth_layer)
                .add_service(health_service)
                .add_service(BackendServiceServer::new(spy_clone.clone()))
                .add_service(OpStoreServiceServer::new(spy_clone.clone()))
                .add_service(WorkspaceServiceServer::new(spy_clone))
                .serve_with_incoming(incoming)
                .await;
        });

        let channel = Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        Self {
            channel,
            spy,
            _task: task,
        }
    }
}

fn with_bearer<T>(msg: T, token: &str) -> Request<T> {
    let mut req = Request::new(msg);
    req.metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    req
}

fn sample_commit() -> Commit {
    Commit {
        commit_id: vec![],
        change_id: vec![0x42; 16],
        parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
        description: "agent change\n".to_string(),
        author: Some(Signature {
            name: "Agent".to_string(),
            email: "agent@example.com".to_string(),
            timestamp: Some(Timestamp {
                millis_since_epoch: 1_750_000_000_000,
                tz_offset: 0,
            }),
        }),
        committer: Some(Signature {
            name: "Agent".to_string(),
            email: "agent@example.com".to_string(),
            timestamp: Some(Timestamp {
                millis_since_epoch: 1_750_000_000_000,
                tz_offset: 0,
            }),
        }),
        predecessors: vec![],
        conflict_labels: vec![],
        secure_sig: None,
        raw_git_commit: vec![],
        external_ids: HashMap::new(),
    }
}

#[tokio::test]
async fn rs256_and_es256_substrate_actor_jwt_injects_caller_identity() {
    let rsa_key = TestSigningKey::generate_rs256("rsa-k1");
    let ec_key = TestSigningKey::generate_es256("ec-k1");
    let oidc = MockOidcServer::start(vec![rsa_key.jwk.clone(), ec_key.jwk.clone()]).await;

    let auth_layer = ServerAuthLayer::new(ServerAuthConfig {
        oidc: Some(OidcAuthConfig::new(&oidc.issuer, "cloud-jj")),
        ..Default::default()
    })
    .unwrap();
    let srv = TestGrpcServer::start(auth_layer).await;
    let mut client = BackendServiceClient::new(srv.channel.clone());

    let now = now_unix_secs();

    // RS256 Agent Substrate actor JWT.
    let rs256_jwt = rsa_key.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "system:actor:oss-demo:coder-1",
        "exp": now + 600,
        "nbf": now - 10,
        "actor.ate.dev": {
            "atespace": "oss-demo",
            "actorName": "coder-1",
            "actorUID": "11111111-2222-3333-4444-555555555555"
        }
    }));
    client
        .read_commit(with_bearer(ReadCommitRequest::default(), &rs256_jwt))
        .await
        .expect("RS256 actor JWT should succeed");
    assert_eq!(
        srv.spy.take_last_identity(),
        Some(CallerIdentity {
            subject: "system:actor:oss-demo:coder-1".to_string(),
            actor_uid: Some("11111111-2222-3333-4444-555555555555".to_string()),
            actor_name: Some("coder-1".to_string()),
            atespace: Some("oss-demo".to_string()),
            is_worker_for_actor: false,
            kind: CallerKind::OidcActor,
        })
    );

    // ES256 Agent Substrate actor JWT.
    let es256_jwt = ec_key.sign(&json!({
        "iss": oidc.issuer,
        "aud": ["other-aud", "cloud-jj"],
        "sub": "system:actor:oss-demo:reviewer-1",
        "exp": now + 600,
        "actor.ate.dev": {
            "atespace": "oss-demo",
            "actorName": "reviewer-1",
            "actorUID": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        }
    }));
    client
        .read_commit(with_bearer(ReadCommitRequest::default(), &es256_jwt))
        .await
        .expect("ES256 actor JWT should succeed");
    assert_eq!(
        srv.spy.take_last_identity(),
        Some(CallerIdentity {
            subject: "system:actor:oss-demo:reviewer-1".to_string(),
            actor_uid: Some("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee".to_string()),
            actor_name: Some("reviewer-1".to_string()),
            atespace: Some("oss-demo".to_string()),
            is_worker_for_actor: false,
            kind: CallerKind::OidcActor,
        })
    );

    // Standard OIDC user JWT without `actor.ate.dev`.
    let user_jwt = ec_key.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "human-dev@example.com",
        "exp": now + 600,
    }));
    client
        .read_commit(with_bearer(ReadCommitRequest::default(), &user_jwt))
        .await
        .expect("OIDC user JWT should succeed");
    assert_eq!(
        srv.spy.take_last_identity(),
        Some(CallerIdentity {
            subject: "human-dev@example.com".to_string(),
            actor_uid: None,
            actor_name: None,
            atespace: None,
            is_worker_for_actor: false,
            kind: CallerKind::OidcUser,
        })
    );
}

#[tokio::test]
async fn invalid_jwts_and_malformed_headers_rejected_with_unauthenticated() {
    let rsa_key = TestSigningKey::generate_rs256("rsa-k1");
    let other_rsa_key = TestSigningKey::generate_rs256("rsa-k1");
    let oidc = MockOidcServer::start(vec![rsa_key.jwk.clone()]).await;

    let auth_layer = ServerAuthLayer::new(ServerAuthConfig {
        oidc: Some(OidcAuthConfig::new(&oidc.issuer, "cloud-jj")),
        ..Default::default()
    })
    .unwrap();
    let srv = TestGrpcServer::start(auth_layer).await;
    let mut client = BackendServiceClient::new(srv.channel.clone());

    let now = now_unix_secs();

    // Expired (> 60s leeway).
    let expired = rsa_key.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "alice",
        "exp": now - 300,
    }));
    // Future nbf (> 60s leeway).
    let future_nbf = rsa_key.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "alice",
        "exp": now + 1200,
        "nbf": now + 300,
    }));
    // Wrong aud.
    let wrong_aud = rsa_key.sign(&json!({
        "iss": oidc.issuer,
        "aud": "wrong-audience",
        "sub": "alice",
        "exp": now + 600,
    }));
    // Wrong iss.
    let wrong_iss = rsa_key.sign(&json!({
        "iss": "https://evil.example.com",
        "aud": "cloud-jj",
        "sub": "alice",
        "exp": now + 600,
    }));
    // Wrong signing key (tampered signature).
    let tampered_sig = other_rsa_key.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "alice",
        "exp": now + 600,
    }));
    // Missing sub.
    let missing_sub = rsa_key.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "exp": now + 600,
    }));
    // Empty sub.
    let empty_sub = rsa_key.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "   ",
        "exp": now + 600,
    }));
    // HS256 symmetric algorithm attack.
    let hs256 = jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &json!({
            "iss": oidc.issuer,
            "aud": "cloud-jj",
            "sub": "alice",
            "exp": now + 600,
        }),
        &EncodingKey::from_secret(b"shared-secret"),
    )
    .unwrap();
    // `alg: none` unsigned token attack.
    let b64 = |val: &Value| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(val).unwrap())
    };
    let alg_none = format!(
        "{}.{}.",
        b64(&json!({"alg": "none", "typ": "JWT"})),
        b64(&json!({"iss": oidc.issuer, "aud": "cloud-jj", "sub": "alice", "exp": now + 600}))
    );
    // Malformed `actor.ate.dev` (non-object) must fail closed rather than downgrade to OidcUser.
    let malformed_actor = rsa_key.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "alice",
        "exp": now + 600,
        "actor.ate.dev": "not-an-object",
    }));
    // RFC 7515 `crit` critical JWS header parameters must be rejected (both non-empty and empty).
    let crit_non_empty = rsa_key.sign_with_crit(
        &json!({
            "iss": oidc.issuer,
            "aud": "cloud-jj",
            "sub": "alice",
            "exp": now + 600,
        }),
        vec!["x-custom-ext".to_string()],
    );
    let crit_empty = rsa_key.sign_with_crit(
        &json!({
            "iss": oidc.issuer,
            "aud": "cloud-jj",
            "sub": "alice",
            "exp": now + 600,
        }),
        vec![],
    );
    // Oversized > 16 KiB bearer token.
    let oversized = "a".repeat(MAX_AUTH_TOKEN_BYTES + 1);

    for (label, tok) in [
        ("expired", expired),
        ("future_nbf", future_nbf),
        ("wrong_aud", wrong_aud),
        ("wrong_iss", wrong_iss),
        ("tampered_sig", tampered_sig),
        ("missing_sub", missing_sub),
        ("empty_sub", empty_sub),
        ("malformed_actor", malformed_actor),
        ("hs256", hs256),
        ("alg_none", alg_none),
        ("crit_non_empty", crit_non_empty),
        ("crit_empty", crit_empty),
        ("oversized", oversized),
    ] {
        let err = client
            .read_commit(with_bearer(ReadCommitRequest::default(), &tok))
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            Code::Unauthenticated,
            "expected UNAUTHENTICATED for {label}, got {err:?}"
        );
    }

    // Duplicate authorization headers and missing header must return UNAUTHENTICATED.
    let missing_err = client
        .read_commit(Request::new(ReadCommitRequest::default()))
        .await
        .unwrap_err();
    assert_eq!(missing_err.code(), Code::Unauthenticated);

    let valid = rsa_key.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "alice",
        "exp": now + 600,
    }));
    let mut dup = Request::new(ReadCommitRequest::default());
    dup.metadata_mut()
        .append("authorization", format!("Bearer {valid}").parse().unwrap());
    dup.metadata_mut()
        .append("authorization", format!("Bearer {valid}").parse().unwrap());
    assert_eq!(
        client.read_commit(dup).await.unwrap_err().code(),
        Code::Unauthenticated
    );
}

#[tokio::test]
async fn jwks_key_rotation_refreshes_on_unknown_kid_and_respects_cooldown() {
    let key1 = TestSigningKey::generate_rs256("kid-v1");
    let key2 = TestSigningKey::generate_es256("kid-v2");
    let key3 = TestSigningKey::generate_es256("kid-v3");
    let oidc = MockOidcServer::start(vec![key1.jwk.clone()]).await;

    let validator = OidcValidator::with_cache_timings(
        OidcAuthConfig::new(&oidc.issuer, "cloud-jj"),
        Duration::from_secs(900),
        Duration::from_secs(60),
    )
    .unwrap();
    let auth_layer =
        ServerAuthLayer::from_validators(BearerAuthInterceptor::default(), Some(validator), None);
    let srv = TestGrpcServer::start(auth_layer).await;
    let mut client = BackendServiceClient::new(srv.channel.clone());

    let now = now_unix_secs();

    // Warm JWKS cache with kid-v1 (1st JWKS fetch).
    let jwt1 = key1.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "actor-1",
        "exp": now + 600,
    }));
    client
        .read_commit(with_bearer(ReadCommitRequest::default(), &jwt1))
        .await
        .expect("initial kid-v1 token should succeed");
    assert_eq!(oidc.jwks_fetches(), 1);

    // Rotate IDP to kid-v2; unknown kid triggers a forced refresh (2nd JWKS fetch) and succeeds.
    oidc.set_keys(vec![key1.jwk.clone(), key2.jwk.clone()]);
    let jwt2 = key2.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "actor-2",
        "exp": now + 600,
    }));
    client
        .read_commit(with_bearer(ReadCommitRequest::default(), &jwt2))
        .await
        .expect("rotated kid-v2 token should trigger JWKS refresh and succeed");
    assert_eq!(oidc.jwks_fetches(), 2);

    // Subsequent unknown kid within cooldown MUST NOT hammer JWKS endpoint.
    oidc.set_keys(vec![key2.jwk.clone(), key3.jwk.clone()]);
    let jwt3 = key3.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "actor-3",
        "exp": now + 600,
    }));
    for _ in 0..3 {
        let err = client
            .read_commit(with_bearer(ReadCommitRequest::default(), &jwt3))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Unauthenticated);
    }
    assert_eq!(
        oidc.jwks_fetches(),
        2,
        "unknown-kid requests during cooldown must not refetch JWKS"
    );
}

#[tokio::test]
async fn worker_for_actor_allows_read_only_and_denies_mutating_rpcs() {
    let key = TestSigningKey::generate_es256("ec-worker");
    let oidc = MockOidcServer::start(vec![key.jwk.clone()]).await;

    let auth_layer = ServerAuthLayer::new(ServerAuthConfig {
        oidc: Some(OidcAuthConfig::new(&oidc.issuer, "cloud-jj")),
        ..Default::default()
    })
    .unwrap();
    let srv = TestGrpcServer::start(auth_layer).await;
    let mut backend = BackendServiceClient::new(srv.channel.clone());
    let mut op_store = OpStoreServiceClient::new(srv.channel.clone());
    let mut workspace = WorkspaceServiceClient::new(srv.channel.clone());

    let now = now_unix_secs();

    // Test truthy / fail-closed encodings of `worker-for-actor`: bool true, non-empty string,
    // non-empty object, empty object `{}` (e.g. Go `*struct{}` presence claims), and empty array.
    for (label, claim_value) in [
        ("bool_true", json!(true)),
        ("non_empty_string", json!("worker-pod-7")),
        ("non_empty_object", json!({"worker": "pod-7"})),
        ("empty_object", json!({})),
        ("empty_array", json!([])),
    ] {
        let jwt = key.sign(&json!({
            "iss": oidc.issuer,
            "aud": "cloud-jj",
            "sub": "system:actor:ns:agent-w",
            "exp": now + 600,
            "actor.ate.dev": {
                "atespace": "ns",
                "actorName": "agent-w",
                "actorUID": "uid-w"
            },
            "worker-for-actor": claim_value,
        }));

        // Read-only RPCs succeed.
        backend
            .read_commit(with_bearer(ReadCommitRequest::default(), &jwt))
            .await
            .unwrap_or_else(|e| panic!("{label} ReadCommit failed: {e}"));
        assert!(srv.spy.take_last_identity().unwrap().is_worker_for_actor);

        backend
            .list_commits(with_bearer(ListCommitsRequest::default(), &jwt))
            .await
            .unwrap_or_else(|e| panic!("{label} ListCommits failed: {e}"));
        op_store
            .get_op_heads(with_bearer(GetOpHeadsRequest::default(), &jwt))
            .await
            .unwrap_or_else(|e| panic!("{label} GetOpHeads failed: {e}"));
        workspace
            .get_workspace(with_bearer(GetWorkspaceRequest::default(), &jwt))
            .await
            .unwrap_or_else(|e| panic!("{label} GetWorkspace failed: {e}"));
        workspace
            .check_working_copy_changes(with_bearer(
                CheckWorkingCopyChangesRequest::default(),
                &jwt,
            ))
            .await
            .unwrap_or_else(|e| panic!("{label} CheckWorkingCopyChanges failed: {e}"));

        // Mutating RPCs fail with PERMISSION_DENIED before reaching handlers.
        let write_commit_err = backend
            .write_commit(with_bearer(
                WriteCommitRequest {
                    project_id: "proj".to_string(),
                    commit: Some(sample_commit()),
                },
                &jwt,
            ))
            .await
            .unwrap_err();
        assert_eq!(write_commit_err.code(), Code::PermissionDenied, "{label}");

        let reg_repo_err = backend
            .register_repository(with_bearer(RegisterRepositoryRequest::default(), &jwt))
            .await
            .unwrap_err();
        assert_eq!(reg_repo_err.code(), Code::PermissionDenied, "{label}");

        let del_repo_err = backend
            .delete_repository(with_bearer(
                DeleteRepositoryRequest {
                    repo_id: "repo-1".to_string(),
                    project_id: None,
                },
                &jwt,
            ))
            .await
            .unwrap_err();
        assert_eq!(del_repo_err.code(), Code::PermissionDenied, "{label}");

        let pub_op_err = op_store
            .publish_operation(with_bearer(PublishOperationRequest::default(), &jwt))
            .await
            .unwrap_err();
        assert_eq!(pub_op_err.code(), Code::PermissionDenied, "{label}");

        let reconcile_err = op_store
            .reconcile_op_heads(with_bearer(ReconcileOpHeadsRequest::default(), &jwt))
            .await
            .unwrap_err();
        assert_eq!(reconcile_err.code(), Code::PermissionDenied, "{label}");

        let update_ws_err = workspace
            .update_workspace(with_bearer(UpdateWorkspaceRequest::default(), &jwt))
            .await
            .unwrap_err();
        assert_eq!(update_ws_err.code(), Code::PermissionDenied, "{label}");

        let delete_ws_err = workspace
            .delete_workspace(with_bearer(DeleteWorkspaceRequest::default(), &jwt))
            .await
            .unwrap_err();
        assert_eq!(delete_ws_err.code(), Code::PermissionDenied, "{label}");

        // Genuinely unknown / future gRPC methods also fail closed with PERMISSION_DENIED rather
        // than falling through to UNIMPLEMENTED.
        let mut raw_grpc = tonic::client::Grpc::new(srv.channel.clone());
        raw_grpc.ready().await.unwrap();
        let unknown_rpc_path: http::uri::PathAndQuery =
            "/commit_cloud.backend.BackendService/FutureMutatingRpc"
                .parse()
                .unwrap();
        let unknown_rpc_err = raw_grpc
            .unary::<RegisterRepositoryRequest, RegisterRepositoryResponse, _>(
                with_bearer(RegisterRepositoryRequest::default(), &jwt),
                unknown_rpc_path,
                tonic::codec::ProstCodec::default(),
            )
            .await
            .unwrap_err();
        assert_eq!(
            unknown_rpc_err.code(),
            Code::PermissionDenied,
            "{label} FutureMutatingRpc"
        );
    }

    // Falsy encodings (`false`, `""`, `"   "`, `null`) retain write permissions.
    for claim_value in [json!(false), json!(""), json!("   "), json!(null)] {
        let jwt = key.sign(&json!({
            "iss": oidc.issuer,
            "aud": "cloud-jj",
            "sub": "system:actor:ns:agent-w",
            "exp": now + 600,
            "worker-for-actor": claim_value,
        }));
        backend
            .register_repository(with_bearer(RegisterRepositoryRequest::default(), &jwt))
            .await
            .expect("falsy worker-for-actor should allow writes");
    }
}

#[tokio::test]
async fn enforce_actor_repo_suffix_policy_over_grpc() {
    let key = TestSigningKey::generate_rs256("rsa-suffix");
    let oidc = MockOidcServer::start(vec![key.jwk.clone()]).await;
    let k8s = MockK8sServer::start(None).await;
    k8s.register_token(
        "k8s-tok",
        "system:serviceaccount:default:bot",
        Some("k8s-uid"),
        None,
    );

    let mut oidc_cfg = OidcAuthConfig::new(&oidc.issuer, "cloud-jj");
    oidc_cfg.enforce_actor_repo_suffix = true;

    let auth_layer = ServerAuthLayer::new(ServerAuthConfig {
        static_tokens: BearerAuthInterceptor::try_from_tokens(["static-admin"]).unwrap(),
        oidc: Some(oidc_cfg),
        k8s_tokenreview: Some(K8sTokenReviewConfig::new(&k8s.base_url)),
    })
    .unwrap();
    let srv = TestGrpcServer::start(auth_layer).await;
    let mut backend = BackendServiceClient::new(srv.channel.clone());
    let mut workspace = WorkspaceServiceClient::new(srv.channel.clone());

    let now = now_unix_secs();
    let actor_jwt = key.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "system:actor:ns:coder-alpha",
        "exp": now + 600,
        "actor.ate.dev": {
            "atespace": "ns",
            "actorName": "coder-alpha",
            "actorUID": "uid-alpha"
        }
    }));

    // Strictly `<alnum>_<atespace>_<actorName>` is allowed.
    backend
        .register_repository(with_bearer(
            RegisterRepositoryRequest {
                repo_id: Some("deadbeef_ns_coder-alpha".to_string()),
                ..Default::default()
            },
            &actor_jwt,
        ))
        .await
        .expect("expected deadbeef_ns_coder-alpha to succeed");

    // Bare 2-segment repos, hyphen/dot/mixed delimiters, mismatched actorName, missing atespace,
    // wrong atespace, and delimiterless prefixes are rejected with PERMISSION_DENIED.
    for denied_repo in [
        "ns_coder-alpha",
        "ns-coder-alpha",
        "deadbeef-ns-coder-alpha",
        "deadbeef.ns.coder-alpha",
        "deadbeef.ns_coder-alpha",
        "deadbeef_ns-coder-alpha",
        "coder-alpha",
        "deadbeef_coder-alpha",
        "deadbeef_ns_coder-beta",
        "deadbeef_other-ns_coder-alpha",
        "evilns_coder-alpha",
        "shared-main",
    ] {
        let err = workspace
            .update_workspace(with_bearer(
                UpdateWorkspaceRequest {
                    workspace: Some(WorkspaceState {
                        repo_id: denied_repo.to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                &actor_jwt,
            ))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::PermissionDenied, "{denied_repo}");
    }

    // Hyphenated atespace + hyphenated actorName succeeds, while cross-atespace, cross-actor,
    // unscoped, mixed-delimiter, and underscore-in-claim attempts all fail closed.
    let demo_coder_jwt = key.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "system:actor:demo:coder-1",
        "exp": now + 600,
        "actor.ate.dev": {
            "atespace": "demo",
            "actorName": "coder-1",
            "actorUID": "uid-demo-coder"
        }
    }));
    let prod_coder_jwt = key.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "system:actor:prod-space:coder-1",
        "exp": now + 600,
        "actor.ate.dev": {
            "atespace": "prod-space",
            "actorName": "coder-1",
            "actorUID": "uid-prod-coder"
        }
    }));
    for (tok, repo) in [
        (&demo_coder_jwt, "a1b2c3d4_demo_coder-1"),
        (&prod_coder_jwt, "1234abcd_prod-space_coder-1"),
    ] {
        backend
            .register_repository(with_bearer(
                RegisterRepositoryRequest {
                    repo_id: Some(repo.to_string()),
                    ..Default::default()
                },
                tok,
            ))
            .await
            .unwrap_or_else(|e| panic!("expected {repo} to succeed: {e}"));
    }

    // Victim (demo, coder-1) vs attacker (coder, 1).
    let attacker_coder_1_jwt = key.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "system:actor:coder:1",
        "exp": now + 600,
        "actor.ate.dev": {
            "atespace": "coder",
            "actorName": "1",
            "actorUID": "uid-attack-1"
        }
    }));
    // Victim (prod-space, coder-1) vs attacker (space, coder-1).
    let attacker_space_coder_jwt = key.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "system:actor:space:coder-1",
        "exp": now + 600,
        "actor.ate.dev": {
            "atespace": "space",
            "actorName": "coder-1",
            "actorUID": "uid-attack-2"
        }
    }));
    // Victim (demo, coder-1) vs attacker (atespace=None, actorName=coder-1).
    let attacker_no_space_jwt = key.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "system:actor:coder-1",
        "exp": now + 600,
        "actor.ate.dev": {
            "actorName": "coder-1",
            "actorUID": "uid-attack-3"
        }
    }));
    // Victim (prod, space-coder) vs attacker (prod-space, coder).
    let attacker_prod_space_coder_jwt = key.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "system:actor:prod-space:coder",
        "exp": now + 600,
        "actor.ate.dev": {
            "atespace": "prod-space",
            "actorName": "coder",
            "actorUID": "uid-attack-4"
        }
    }));
    // Underscore inside actor claims must also fail closed.
    let underscore_claim_jwt = key.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "system:actor:prod_space:coder",
        "exp": now + 600,
        "actor.ate.dev": {
            "atespace": "prod_space",
            "actorName": "coder",
            "actorUID": "uid-underscore"
        }
    }));
    for (label, tok, repo_id) in [
        (
            "attack_1_coder_1_on_underscore",
            &attacker_coder_1_jwt,
            "a1b2c3d4_demo_coder-1",
        ),
        (
            "attack_1_coder_1_on_legacy_hyphen",
            &attacker_coder_1_jwt,
            "demo-coder-1",
        ),
        (
            "attack_2_space_on_prod_space",
            &attacker_space_coder_jwt,
            "1234abcd_prod-space_coder-1",
        ),
        (
            "attack_2_space_on_legacy_hyphen",
            &attacker_space_coder_jwt,
            "prod-space-coder-1",
        ),
        (
            "attack_3_unscoped_on_demo_coder",
            &attacker_no_space_jwt,
            "a1b2c3d4_demo_coder-1",
        ),
        (
            "attack_3_unscoped_on_two_part",
            &attacker_no_space_jwt,
            "a1b2c3d4_coder-1",
        ),
        (
            "attack_4_boundary_shift",
            &attacker_prod_space_coder_jwt,
            "a1b2c3d4_prod_space-coder",
        ),
        (
            "attack_5_mixed_delimiter",
            &demo_coder_jwt,
            "a1b2c3d4.demo_coder-1",
        ),
        (
            "underscore_in_claim",
            &underscore_claim_jwt,
            "a1b2c3d4_prod_space_coder",
        ),
    ] {
        let err = backend
            .delete_repository(with_bearer(
                DeleteRepositoryRequest {
                    repo_id: repo_id.to_string(),
                    project_id: None,
                },
                tok,
            ))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::PermissionDenied, "{label} on {repo_id}");
    }

    // Non-actor callers (OIDC user, static token, K8s SA) can write to arbitrary repos.
    let user_jwt = key.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "human@example.com",
        "exp": now + 600,
    }));
    for caller_token in [user_jwt.as_str(), "static-admin", "k8s-tok"] {
        backend
            .register_repository(with_bearer(
                RegisterRepositoryRequest {
                    repo_id: Some("any-shared-repo".to_string()),
                    ..Default::default()
                },
                caller_token,
            ))
            .await
            .expect("non-actor caller should bypass actor repo suffix restriction");
    }
}

#[tokio::test]
async fn k8s_tokenreview_accepts_caches_rotates_sa_token_and_rejects_invalid() {
    let k8s = MockK8sServer::start(Some("server-sa-v1".to_string())).await;
    let oidc_key = TestSigningKey::generate_rs256("oidc-k1");
    let oidc = MockOidcServer::start(vec![oidc_key.jwk.clone()]).await;

    // A Kubernetes projected ServiceAccount token is itself a 3-part JWT with a different issuer;
    // verify it falls through from OIDC to K8s TokenReview without burning OIDC JWKS.
    let k8s_jwt_like_token = {
        let other = TestSigningKey::generate_rs256("k8s-kid-99");
        other.sign(&json!({
            "iss": "https://kubernetes.default.svc.cluster.local",
            "aud": "cloud-jj",
            "sub": "system:serviceaccount:jj:runner",
            "exp": now_unix_secs() + 600,
        }))
    };
    k8s.register_token(
        &k8s_jwt_like_token,
        "system:serviceaccount:jj:runner",
        Some("sa-uid-42"),
        Some("cloud-jj"),
    );

    let sa_file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(sa_file.path(), "server-sa-v1\n").unwrap();

    let mut k8s_cfg = K8sTokenReviewConfig::new(&k8s.base_url);
    k8s_cfg.audience = Some("cloud-jj".to_string());
    k8s_cfg.sa_token_file = Some(sa_file.path().to_path_buf());

    let k8s_validator =
        K8sTokenReviewValidator::with_cache_ttl(k8s_cfg, Duration::from_secs(30)).unwrap();
    let oidc_validator = OidcValidator::new(OidcAuthConfig::new(&oidc.issuer, "cloud-jj")).unwrap();
    let auth_layer = ServerAuthLayer::from_validators(
        BearerAuthInterceptor::default(),
        Some(oidc_validator),
        Some(k8s_validator),
    );
    let srv = TestGrpcServer::start(auth_layer).await;
    let mut backend = BackendServiceClient::new(srv.channel.clone());

    // First call hits mock K8s TokenReview and populates cache without fetching OIDC JWKS.
    backend
        .read_commit(with_bearer(
            ReadCommitRequest::default(),
            &k8s_jwt_like_token,
        ))
        .await
        .expect("authenticated K8s SA token should succeed");
    assert_eq!(
        oidc.jwks_fetches(),
        0,
        "K8s SA JWT with non-OIDC issuer must not trigger OIDC JWKS fetch"
    );
    assert_eq!(
        oidc.discovery_fetches(),
        0,
        "K8s SA JWT with non-OIDC issuer must not trigger OIDC discovery"
    );
    assert_eq!(k8s.requests(), 1);
    assert_eq!(
        *k8s.state.last_auth_header.lock().unwrap(),
        Some("Bearer server-sa-v1".to_string())
    );
    assert_eq!(
        srv.spy.take_last_identity(),
        Some(CallerIdentity {
            subject: "system:serviceaccount:jj:runner".to_string(),
            actor_uid: Some("sa-uid-42".to_string()),
            actor_name: None,
            atespace: None,
            is_worker_for_actor: false,
            kind: CallerKind::K8sServiceAccount,
        })
    );

    // Second call hits positive SHA-256 cache without calling K8s API again.
    backend
        .read_commit(with_bearer(
            ReadCommitRequest::default(),
            &k8s_jwt_like_token,
        ))
        .await
        .expect("cached K8s SA token should succeed");
    assert_eq!(k8s.requests(), 1);

    // Rotate server's projected SA token file and authenticate a second caller token.
    // The cached `server-sa-v1` token triggers an HTTP 401 from the mock API server, prompting an
    // immediate cache invalidation, reload of `server-sa-v2`, and single retry.
    std::fs::write(sa_file.path(), "server-sa-v2\n").unwrap();
    k8s.set_expected_sa_token(Some("server-sa-v2".to_string()));
    k8s.register_token(
        "k8s-opaque-token-2",
        "system:serviceaccount:jj:builder",
        None,
        Some("cloud-jj"),
    );
    backend
        .read_commit(with_bearer(
            ReadCommitRequest::default(),
            "k8s-opaque-token-2",
        ))
        .await
        .expect("rotated server SA token file should be picked up dynamically");
    assert_eq!(k8s.unauthorized_responses(), 1);
    assert_eq!(k8s.requests(), 3);
    assert_eq!(
        *k8s.state.last_auth_header.lock().unwrap(),
        Some("Bearer server-sa-v2".to_string())
    );

    // Unauthenticated token is rejected with UNAUTHENTICATED using the newly cached `server-sa-v2`
    // without another HTTP 401 retry.
    let err = backend
        .read_commit(with_bearer(ReadCommitRequest::default(), "unknown-k8s-tok"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);
    assert_eq!(k8s.unauthorized_responses(), 1);
    assert_eq!(k8s.requests(), 4);
}

#[tokio::test]
async fn static_bearer_token_and_health_check_coexist_with_oidc_and_k8s() {
    let key = TestSigningKey::generate_rs256("rsa-coexist");
    let oidc = MockOidcServer::start(vec![key.jwk.clone()]).await;
    let k8s = MockK8sServer::start(None).await;

    let auth_layer = ServerAuthLayer::new(ServerAuthConfig {
        static_tokens: BearerAuthInterceptor::try_from_tokens(["dev-static-secret"]).unwrap(),
        oidc: Some(OidcAuthConfig::new(&oidc.issuer, "cloud-jj")),
        k8s_tokenreview: Some(K8sTokenReviewConfig::new(&k8s.base_url)),
    })
    .unwrap();
    let srv = TestGrpcServer::start(auth_layer).await;

    // Health check stays open with no credentials.
    let mut health = HealthClient::new(srv.channel.clone());
    let health_resp = health
        .check(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect("unauthenticated health check should succeed")
        .into_inner();
    assert_eq!(health_resp.status, 1); // SERVING

    // Static token succeeds without any OIDC or K8s HTTP calls.
    let mut backend = BackendServiceClient::new(srv.channel.clone());
    backend
        .read_commit(with_bearer(
            ReadCommitRequest::default(),
            "dev-static-secret",
        ))
        .await
        .expect("static bearer token should succeed");
    assert_eq!(oidc.jwks_fetches(), 0);
    assert_eq!(k8s.requests(), 0);
    assert_eq!(
        srv.spy.take_last_identity().map(|id| id.kind),
        Some(CallerKind::StaticToken)
    );
}

#[tokio::test]
async fn commit_actor_attribution_preserves_git_sha1_and_prevents_spoofing() {
    let key = TestSigningKey::generate_es256("ec-attr");
    let oidc = MockOidcServer::start(vec![key.jwk.clone()]).await;
    let auth_layer = ServerAuthLayer::new(ServerAuthConfig {
        oidc: Some(OidcAuthConfig::new(&oidc.issuer, "cloud-jj")),
        ..Default::default()
    })
    .unwrap();
    let srv = TestGrpcServer::start(auth_layer).await;
    let mut backend = BackendServiceClient::new(srv.channel.clone());

    let mut expected_commit = sample_commit();
    let expected_sha1 = verify_or_compute_git_commit_hash(&mut expected_commit).unwrap();

    let long_subject = format!("system:actor:atespace-oss:{}", "x".repeat(80));
    assert!(long_subject.len() > 64);

    let actor_jwt = key.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": long_subject,
        "exp": now_unix_secs() + 600,
        "actor.ate.dev": {
            "atespace": "atespace-oss",
            "actorName": "writer-bot",
            "actorUID": "uid-verified-999"
        }
    }));

    // Client attempts to spoof `actor-sub` and `actor-uid` on WriteCommit.
    let mut spoofed = sample_commit();
    spoofed
        .external_ids
        .insert(ACTOR_SUB_EXTERNAL_ID_KEY.to_string(), b"spoofed".to_vec());
    spoofed
        .external_ids
        .insert(ACTOR_UID_EXTERNAL_ID_KEY.to_string(), b"spoofed".to_vec());

    let resp = backend
        .write_commit(with_bearer(
            WriteCommitRequest {
                project_id: "proj".to_string(),
                commit: Some(spoofed.clone()),
            },
            &actor_jwt,
        ))
        .await
        .expect("WriteCommit with actor attribution should succeed")
        .into_inner();
    assert_eq!(
        resp.commit_id, expected_sha1,
        "Git SHA-1 commit_id must be identical with actor attribution attached"
    );

    // Directly assert the resulting external_ids overwritten by apply_commit_actor_attribution.
    let identity = srv.spy.take_last_identity().unwrap();
    apply_commit_actor_attribution(Some(&identity), &mut spoofed);
    assert_eq!(
        spoofed.external_ids.get(ACTOR_SUB_EXTERNAL_ID_KEY),
        Some(&long_subject.as_bytes().to_vec())
    );
    assert_eq!(
        spoofed.external_ids.get(ACTOR_UID_EXTERNAL_ID_KEY),
        Some(&b"uid-verified-999".to_vec())
    );
    assert_eq!(
        verify_or_compute_git_commit_hash(&mut spoofed).unwrap(),
        expected_sha1
    );
}

#[tokio::test]
async fn real_services_persist_actor_attribution_and_enforce_repo_suffix() {
    use jj_commit_cloud_server::backend::CommitCloudBackendService;
    use jj_commit_cloud_server::metrics::{MetricsLayer, ServerMetrics};
    use jj_commit_cloud_server::op_store::CommitCloudOpStoreService;
    use jj_commit_cloud_server::store::CompositeStore;
    use jj_commit_cloud_server::workspace::CommitCloudWorkspaceService;

    let key = TestSigningKey::generate_es256("ec-real");
    let oidc = MockOidcServer::start(vec![key.jwk.clone()]).await;

    let mut oidc_cfg = OidcAuthConfig::new(&oidc.issuer, "cloud-jj");
    oidc_cfg.enforce_actor_repo_suffix = true;
    let auth_layer = ServerAuthLayer::new(ServerAuthConfig {
        oidc: Some(oidc_cfg),
        ..Default::default()
    })
    .unwrap();
    let enforce = auth_layer.enforce_actor_repo_suffix();

    let store = Arc::new(CompositeStore::in_memory());
    let metrics = ServerMetrics::new();
    let backend_svc = CommitCloudBackendService::with_metrics(store.clone(), metrics.clone())
        .with_actor_repo_suffix_enforcement(enforce);
    let op_store_svc = CommitCloudOpStoreService::with_metrics(store.clone(), metrics.clone())
        .with_actor_repo_suffix_enforcement(enforce);
    let workspace_svc = CommitCloudWorkspaceService::with_metrics(store, metrics.clone())
        .with_actor_repo_suffix_enforcement(enforce);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = TcpListenerStream::new(listener);
    let _server = tokio::spawn(async move {
        let _ = Server::builder()
            .layer(MetricsLayer::new(metrics))
            .layer(auth_layer)
            .add_service(BackendServiceServer::new(backend_svc))
            .add_service(OpStoreServiceServer::new(op_store_svc))
            .add_service(WorkspaceServiceServer::new(workspace_svc))
            .serve_with_incoming(incoming)
            .await;
    });

    let channel = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut backend = BackendServiceClient::new(channel.clone());
    let mut op_store = OpStoreServiceClient::new(channel.clone());
    let mut workspace = WorkspaceServiceClient::new(channel);

    let now = now_unix_secs();
    let actor_jwt = key.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "system:actor:oss-demo:coder-1",
        "exp": now + 600,
        "actor.ate.dev": {
            "atespace": "oss-demo",
            "actorName": "coder-1",
            "actorUID": "actor-uid-777"
        }
    }));

    // Omitting `repo_id` as an OIDC actor under suffix enforcement returns an actionable
    // INVALID_ARGUMENT error instead of synthesizing an unusable UUIDv4 and returning PERMISSION_DENIED.
    let omitted_repo_err = backend
        .register_repository(with_bearer(
            RegisterRepositoryRequest {
                project_id: "demo-proj".to_string(),
                repo_id: None,
                name: None,
                hash_algorithm: 0,
            },
            &actor_jwt,
        ))
        .await
        .unwrap_err();
    assert_eq!(omitted_repo_err.code(), Code::InvalidArgument);
    assert!(
        omitted_repo_err.message().contains("explicit repo_id"),
        "unexpected error message: {}",
        omitted_repo_err.message()
    );

    // Register own repo `<hash>_<atespace>_<actorName>` succeeds; foreign repo, bare 2-segment
    // repo, hyphenated repo, or repo missing `atespace` is rejected.
    backend
        .register_repository(with_bearer(
            RegisterRepositoryRequest {
                project_id: "demo-proj".to_string(),
                repo_id: Some("1234abcd_oss-demo_coder-1".to_string()),
                name: None,
                hash_algorithm: 0,
            },
            &actor_jwt,
        ))
        .await
        .expect("matching repo suffix should succeed");

    for foreign_repo in [
        "1234abcd_oss-demo_coder-2",
        "1234abcd-oss-demo-coder-1",
        "oss-demo_coder-1",
        "1234abcd_coder-1",
        "1234abcd_other-demo_coder-1",
    ] {
        let denied_reg = backend
            .register_repository(with_bearer(
                RegisterRepositoryRequest {
                    project_id: "demo-proj".to_string(),
                    repo_id: Some(foreign_repo.to_string()),
                    name: None,
                    hash_algorithm: 0,
                },
                &actor_jwt,
            ))
            .await
            .unwrap_err();
        assert_eq!(
            denied_reg.code(),
            Code::PermissionDenied,
            "register {foreign_repo}"
        );
    }

    let denied_del = backend
        .delete_repository(with_bearer(
            DeleteRepositoryRequest {
                repo_id: "1234abcd_oss-demo_coder-2".to_string(),
                project_id: None,
            },
            &actor_jwt,
        ))
        .await
        .unwrap_err();
    assert_eq!(denied_del.code(), Code::PermissionDenied);

    let denied_op = op_store
        .write_view(with_bearer(
            WriteViewRequest {
                repo_id: "1234abcd_oss-demo_coder-2".to_string(),
                view: None,
            },
            &actor_jwt,
        ))
        .await
        .unwrap_err();
    assert_eq!(denied_op.code(), Code::PermissionDenied);

    let denied_write_op = op_store
        .write_operation(with_bearer(
            WriteOperationRequest {
                repo_id: "1234abcd_oss-demo_coder-2".to_string(),
                operation: None,
            },
            &actor_jwt,
        ))
        .await
        .unwrap_err();
    assert_eq!(denied_write_op.code(), Code::PermissionDenied);

    let denied_update_heads = op_store
        .update_op_heads(with_bearer(
            UpdateOpHeadsRequest {
                repo_id: "1234abcd_oss-demo_coder-2".to_string(),
                old_op_head_ids: Vec::new(),
                new_op_head_id: vec![0xaa; 16],
            },
            &actor_jwt,
        ))
        .await
        .unwrap_err();
    assert_eq!(denied_update_heads.code(), Code::PermissionDenied);

    let denied_reconcile = op_store
        .reconcile_op_heads(with_bearer(
            ReconcileOpHeadsRequest {
                repo_id: "1234abcd_oss-demo_coder-2".to_string(),
            },
            &actor_jwt,
        ))
        .await
        .unwrap_err();
    assert_eq!(denied_reconcile.code(), Code::PermissionDenied);

    let denied_publish = op_store
        .publish_operation(with_bearer(
            PublishOperationRequest {
                repo_id: "1234abcd_oss-demo_coder-2".to_string(),
                view: None,
                operation: None,
                old_op_head_ids: Vec::new(),
            },
            &actor_jwt,
        ))
        .await
        .unwrap_err();
    assert_eq!(denied_publish.code(), Code::PermissionDenied);

    let denied_update_ws = workspace
        .update_workspace(with_bearer(
            UpdateWorkspaceRequest {
                workspace: Some(WorkspaceState {
                    repo_id: "1234abcd_oss-demo_coder-2".to_string(),
                    user: "alice".to_string(),
                    workspace_name: "default".to_string(),
                    ..Default::default()
                }),
                expected_operation_id: None,
            },
            &actor_jwt,
        ))
        .await
        .unwrap_err();
    assert_eq!(denied_update_ws.code(), Code::PermissionDenied);

    let denied_ws = workspace
        .delete_workspace(with_bearer(
            DeleteWorkspaceRequest {
                repo_id: "1234abcd_oss-demo_coder-2".to_string(),
                user: "alice".to_string(),
                workspace_name: "default".to_string(),
            },
            &actor_jwt,
        ))
        .await
        .unwrap_err();
    assert_eq!(denied_ws.code(), Code::PermissionDenied);

    // WriteCommit persists verified actor-sub and actor-uid, strips spoofed values, keeps SHA-1
    // identical, and succeeds even when the client already supplies 16 custom external_ids.
    let mut expected_commit = sample_commit();
    let expected_sha1 = verify_or_compute_git_commit_hash(&mut expected_commit).unwrap();
    let mut commit = sample_commit();
    for idx in 0..16 {
        commit
            .external_ids
            .insert(format!("custom-key-{idx}"), vec![idx as u8]);
    }
    commit
        .external_ids
        .insert(ACTOR_SUB_EXTERNAL_ID_KEY.to_string(), b"spoofed".to_vec());
    commit
        .external_ids
        .insert(ACTOR_UID_EXTERNAL_ID_KEY.to_string(), b"spoofed".to_vec());

    let commit_id = backend
        .write_commit(with_bearer(
            WriteCommitRequest {
                project_id: "demo-proj".to_string(),
                commit: Some(commit),
            },
            &actor_jwt,
        ))
        .await
        .expect("WriteCommit should succeed")
        .into_inner()
        .commit_id;
    assert_eq!(commit_id, expected_sha1);

    let stored = backend
        .read_commit(with_bearer(
            ReadCommitRequest {
                project_id: "demo-proj".to_string(),
                commit_id,
            },
            &actor_jwt,
        ))
        .await
        .expect("ReadCommit should succeed")
        .into_inner()
        .commit
        .expect("commit present");
    assert_eq!(
        stored.external_ids.get(ACTOR_SUB_EXTERNAL_ID_KEY),
        Some(&b"system:actor:oss-demo:coder-1".to_vec())
    );
    assert_eq!(
        stored.external_ids.get(ACTOR_UID_EXTERNAL_ID_KEY),
        Some(&b"actor-uid-777".to_vec())
    );

    let deleted = backend
        .delete_repository(with_bearer(
            DeleteRepositoryRequest {
                repo_id: "1234abcd_oss-demo_coder-1".to_string(),
                project_id: Some("demo-proj".to_string()),
            },
            &actor_jwt,
        ))
        .await
        .expect("deleting own repo should succeed")
        .into_inner();
    assert!(deleted.existed);
}

#[tokio::test]
async fn jwk_key_ops_excluding_verify_is_rejected() {
    let mut key = TestSigningKey::generate_es256("ec-encrypt-only");
    key.jwk.common.key_operations = Some(vec![jsonwebtoken::jwk::KeyOperations::Encrypt]);
    let oidc = MockOidcServer::start(vec![key.jwk.clone()]).await;
    let validator = OidcValidator::new(OidcAuthConfig::new(&oidc.issuer, "cloud-jj")).unwrap();
    let jwt = key.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "alice",
        "exp": now_unix_secs() + 600,
    }));
    assert!(
        validator.verify(&jwt).await.is_err(),
        "JWK whose key_ops excludes Verify must not verify JWTs"
    );
}

#[tokio::test]
async fn oidc_issuer_failure_does_not_fall_through_to_k8s_tokenreview() {
    let key = TestSigningKey::generate_es256("ec-authoritative");
    let oidc = MockOidcServer::start(vec![key.jwk.clone()]).await;
    let k8s = MockK8sServer::start(None).await;

    let now = now_unix_secs();
    // Register tokens claiming the configured OIDC issuer in MockK8sServer to simulate an API
    // server that also trusts the same JWTs without enforcing Substrate actor policy.
    let expired_actor_jwt = key.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "system:actor:oss-demo:coder-1",
        "exp": now - 300,
        "worker-for-actor": true,
    }));
    let malformed_actor_jwt = key.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "system:actor:oss-demo:coder-1",
        "exp": now + 600,
        "actor.ate.dev": "not-an-object",
    }));
    let b64 = |val: &Value| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(val).unwrap())
    };
    let payload_b64 = b64(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "system:actor:oss-demo:coder-1",
        "exp": now + 600,
        "worker-for-actor": true,
    }));
    let es512_jwt = format!(
        "{}.{}.fakesig",
        b64(&json!({"alg": "ES512", "typ": "JWT"})),
        payload_b64
    );
    let alg_none_with_sig_jwt = format!(
        "{}.{}.dummysig",
        b64(&json!({"alg": "none", "typ": "JWT"})),
        payload_b64
    );
    let alg_none_empty_sig_jwt = format!(
        "{}.{}.",
        b64(&json!({"alg": "none", "typ": "JWT"})),
        payload_b64
    );
    let mut padded_sub = String::from("system:actor:oss-demo:coder-1");
    let padded_payload_b64 = loop {
        let encoded = base64::engine::general_purpose::URL_SAFE.encode(
            serde_json::to_vec(&json!({
                "iss": oidc.issuer,
                "aud": "cloud-jj",
                "sub": padded_sub,
                "exp": now + 600,
                "worker-for-actor": true,
            }))
            .unwrap(),
        );
        if encoded.ends_with('=') {
            break encoded;
        }
        padded_sub.push('x');
    };
    let padded_payload_jwt = format!(
        "{}.{}.dummysig",
        b64(&json!({"alg": "RS256", "typ": "JWT"})),
        padded_payload_b64
    );
    let crit_header_jwt = key.sign_with_crit(
        &json!({
            "iss": oidc.issuer,
            "aud": "cloud-jj",
            "sub": "system:actor:oss-demo:coder-1",
            "exp": now + 600,
            "worker-for-actor": true,
        }),
        vec!["x-unsupported-crit".to_string()],
    );
    for tok in [
        &expired_actor_jwt,
        &malformed_actor_jwt,
        &es512_jwt,
        &alg_none_with_sig_jwt,
        &alg_none_empty_sig_jwt,
        &padded_payload_jwt,
        &crit_header_jwt,
    ] {
        k8s.register_token(tok, "system:actor:oss-demo:coder-1", Some("uid-1"), None);
    }

    let auth_layer = ServerAuthLayer::new(ServerAuthConfig {
        oidc: Some(OidcAuthConfig::new(&oidc.issuer, "cloud-jj")),
        k8s_tokenreview: Some(K8sTokenReviewConfig::new(&k8s.base_url)),
        ..Default::default()
    })
    .unwrap();
    let srv = TestGrpcServer::start(auth_layer).await;
    let mut backend = BackendServiceClient::new(srv.channel.clone());

    for (label, tok) in [
        ("expired_actor_jwt", &expired_actor_jwt),
        ("malformed_actor_jwt", &malformed_actor_jwt),
        ("es512_jwt", &es512_jwt),
        ("alg_none_with_sig_jwt", &alg_none_with_sig_jwt),
        ("alg_none_empty_sig_jwt", &alg_none_empty_sig_jwt),
        ("padded_payload_jwt", &padded_payload_jwt),
        ("crit_header_jwt", &crit_header_jwt),
    ] {
        let err = backend
            .write_commit(with_bearer(
                WriteCommitRequest {
                    project_id: "proj".to_string(),
                    commit: Some(sample_commit()),
                },
                tok,
            ))
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            Code::Unauthenticated,
            "{label} must fail closed with UNAUTHENTICATED"
        );
    }
    assert_eq!(
        k8s.requests(),
        0,
        "rejected JWTs claiming the configured OIDC issuer must never fall through to TokenReview"
    );
}

#[tokio::test]
async fn oidc_ttl_expiry_rediscovers_rotated_jwks_uri() {
    let key1 = TestSigningKey::generate_es256("ec-uri-v1");
    let key2 = TestSigningKey::generate_es256("ec-uri-v2");
    let oidc = MockOidcServer::start(vec![key1.jwk.clone()]).await;

    let validator = OidcValidator::with_cache_timings(
        OidcAuthConfig::new(&oidc.issuer, "cloud-jj"),
        Duration::from_millis(40),
        Duration::from_secs(60),
    )
    .unwrap();
    let now = now_unix_secs();

    let jwt1 = key1.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "actor-v1",
        "exp": now + 600,
    }));
    assert_eq!(validator.verify(&jwt1).await.unwrap().subject, "actor-v1");
    assert_eq!(oidc.discovery_fetches(), 1);
    assert_eq!(oidc.jwks_fetches(), 1);

    // Rotate discovery's `jwks_uri` to `/jwks-v2.json` (retiring `/jwks.json` to HTTP 404)
    // and wait for `jwks_ttl` to expire.
    oidc.rotate_jwks_path("/jwks-v2.json", vec![key2.jwk.clone()]);
    tokio::time::sleep(Duration::from_millis(60)).await;

    let jwt2 = key2.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "actor-v2",
        "exp": now + 600,
    }));
    assert_eq!(validator.verify(&jwt2).await.unwrap().subject, "actor-v2");
    assert_eq!(
        oidc.discovery_fetches(),
        2,
        "TTL expiry must re-run OIDC discovery to pick up rotated jwks_uri"
    );
    assert_eq!(oidc.jwks_fetches(), 2);
}

#[tokio::test]
async fn kidless_expired_jwt_does_not_trigger_forced_jwks_refresh() {
    let key = TestSigningKey::generate_es256("ec-kidless");
    let oidc = MockOidcServer::start(vec![key.jwk.clone()]).await;
    let validator = OidcValidator::with_cache_timings(
        OidcAuthConfig::new(&oidc.issuer, "cloud-jj"),
        Duration::from_secs(900),
        Duration::from_secs(60),
    )
    .unwrap();
    let now = now_unix_secs();

    // Warm JWKS cache.
    let valid_jwt = key.sign_without_kid(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "alice",
        "exp": now + 600,
    }));
    assert_eq!(validator.verify(&valid_jwt).await.unwrap().subject, "alice");
    assert_eq!(oidc.jwks_fetches(), 1);

    // An expired kid-less JWT signed by the valid cached key must NOT burn a forced JWKS refresh.
    let expired_kidless = key.sign_without_kid(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "alice",
        "exp": now - 300,
    }));
    assert!(validator.verify(&expired_kidless).await.is_err());
    assert_eq!(
        oidc.jwks_fetches(),
        1,
        "expired kid-less JWT must not trigger forced JWKS refresh"
    );
}

#[tokio::test]
async fn concurrent_kidless_key_rotation_succeeds_for_all_waiters_with_single_refresh() {
    let key1 = TestSigningKey::generate_es256("ec-rot-v1");
    let key2 = TestSigningKey::generate_es256("ec-rot-v2");
    let oidc = MockOidcServer::start(vec![key1.jwk.clone()]).await;

    let validator = OidcValidator::with_cache_timings(
        OidcAuthConfig::new(&oidc.issuer, "cloud-jj"),
        Duration::from_secs(900),
        Duration::from_secs(60),
    )
    .unwrap();
    let now = now_unix_secs();

    // Warm JWKS cache with key1.
    let warmup_jwt = key1.sign_without_kid(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "warmup-actor",
        "exp": now + 600,
    }));
    assert_eq!(
        validator.verify(&warmup_jwt).await.unwrap().subject,
        "warmup-actor"
    );
    assert_eq!(oidc.jwks_fetches(), 1);

    // Rotate IDP to key2 and hold the JWKS response briefly so the second concurrent kid-less
    // request deterministically reads the stale cache and queues behind `refresh_lock`.
    oidc.set_keys(vec![key2.jwk.clone()]);
    oidc.set_jwks_delay(Duration::from_millis(60));

    let jwt_a = key2.sign_without_kid(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "concurrent-a",
        "exp": now + 600,
    }));
    let jwt_b = key2.sign_without_kid(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "concurrent-b",
        "exp": now + 600,
    }));

    let (res_a, res_b) = tokio::join!(validator.verify(&jwt_a), validator.verify(&jwt_b));
    assert_eq!(
        res_a
            .expect("first concurrent kid-less rotation request should succeed")
            .subject,
        "concurrent-a"
    );
    assert_eq!(
        res_b.expect("second concurrent kid-less rotation request must reuse refreshed JWKS instead of hitting cooldown").subject,
        "concurrent-b"
    );
    assert_eq!(
        oidc.jwks_fetches(),
        2,
        "concurrent kid-less rotation must perform exactly 1 refresh fetch"
    );
}

#[tokio::test]
async fn cold_cache_and_ttl_expiry_unknown_key_do_not_double_fetch_or_burn_cooldown() {
    let key1 = TestSigningKey::generate_es256("ec-k1");
    let key2 = TestSigningKey::generate_es256("ec-k2");
    let key3 = TestSigningKey::generate_es256("ec-k3");
    let oidc = MockOidcServer::start(vec![key1.jwk.clone()]).await;

    let validator = OidcValidator::with_cache_timings(
        OidcAuthConfig::new(&oidc.issuer, "cloud-jj"),
        Duration::from_secs(900),
        Duration::from_secs(60),
    )
    .unwrap();
    let now = now_unix_secs();

    // Cold cache with an unknown `kid` must perform only 1 JWKS fetch (no immediate second
    // fetch via `refresh_jwks_on_miss`) and must NOT burn `last_forced_refresh` cooldown.
    let unknown_kid_jwt = key2.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "actor-k2",
        "exp": now + 600,
    }));
    assert!(validator.verify(&unknown_kid_jwt).await.is_err());
    assert_eq!(
        oidc.jwks_fetches(),
        1,
        "cold-cache unknown kid must not double-fetch JWKS"
    );
    assert_eq!(
        oidc.discovery_fetches(),
        1,
        "cold-cache unknown kid must not double-fetch OIDC discovery"
    );

    // Immediately rotating IDP to include `key2` within the warm TTL window must still trigger a
    // forced refresh (proving the cold-cache miss did not burn cooldown).
    oidc.set_keys(vec![key1.jwk.clone(), key2.jwk.clone()]);
    assert_eq!(
        validator
            .verify(&unknown_kid_jwt)
            .await
            .expect("real rotation right after cold-cache miss must still refresh")
            .subject,
        "actor-k2"
    );
    assert_eq!(oidc.jwks_fetches(), 2);

    // TTL-expired request with an invalid kid-less signature must also perform only 1 JWKS
    // fetch and leave forced-refresh cooldown untouched on a fresh validator instance.
    let oidc_ttl = MockOidcServer::start(vec![key1.jwk.clone()]).await;
    let ttl_validator = OidcValidator::with_cache_timings(
        OidcAuthConfig::new(&oidc_ttl.issuer, "cloud-jj"),
        Duration::from_millis(120),
        Duration::from_secs(60),
    )
    .unwrap();
    let warm_jwt = key1.sign_without_kid(&json!({
        "iss": oidc_ttl.issuer,
        "aud": "cloud-jj",
        "sub": "actor-k1",
        "exp": now + 600,
    }));
    assert_eq!(
        ttl_validator.verify(&warm_jwt).await.unwrap().subject,
        "actor-k1"
    );
    assert_eq!(oidc_ttl.jwks_fetches(), 1);

    // Wait for `jwks_ttl` to expire, then send an invalid kid-less token signed by `key3`.
    tokio::time::sleep(Duration::from_millis(160)).await;
    let invalid_kidless_jwt = key3.sign_without_kid(&json!({
        "iss": oidc_ttl.issuer,
        "aud": "cloud-jj",
        "sub": "actor-k3",
        "exp": now + 600,
    }));
    assert!(ttl_validator.verify(&invalid_kidless_jwt).await.is_err());
    assert_eq!(
        oidc_ttl.jwks_fetches(),
        2,
        "TTL-expired invalid kid-less JWT must perform only 1 fetch"
    );

    // Immediately rotate IDP to include `key3` and verify a real rotation still refreshes without
    // being blocked by a burned cooldown.
    oidc_ttl.set_keys(vec![key1.jwk.clone(), key3.jwk.clone()]);
    assert_eq!(
        ttl_validator
            .verify(&invalid_kidless_jwt)
            .await
            .expect("real rotation right after TTL-expired miss must still refresh")
            .subject,
        "actor-k3"
    );
    assert_eq!(oidc_ttl.jwks_fetches(), 3);
}

#[tokio::test]
async fn jwks_cold_cache_and_ttl_expiry_outage_backs_off_within_cooldown_and_recovers() {
    let key = TestSigningKey::generate_es256("ec-outage");
    let oidc = MockOidcServer::start(vec![key.jwk.clone()]).await;
    oidc.set_outage(true);
    oidc.set_jwks_delay(Duration::from_millis(40));

    let validator = OidcValidator::with_cache_timings(
        OidcAuthConfig::new(&oidc.issuer, "cloud-jj"),
        Duration::from_millis(80),
        Duration::from_millis(120),
    )
    .unwrap();
    let now = now_unix_secs();
    let jwt = key.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "outage-actor",
        "exp": now + 600,
    }));

    // Cold-cache outage: concurrent waiters behind `refresh_lock` AND subsequent sequential
    // callers within `refresh_cooldown` must fast-fail without issuing additional IDP fetches.
    let (res_a, res_b) = tokio::join!(validator.verify(&jwt), validator.verify(&jwt));
    assert!(res_a.is_err());
    assert!(res_b.is_err());
    for _ in 0..3 {
        assert!(validator.verify(&jwt).await.is_err());
    }
    assert_eq!(
        oidc.discovery_fetches(),
        1,
        "cold-cache IDP outage must back off within refresh_cooldown"
    );
    assert_eq!(oidc.jwks_fetches(), 0);

    // Recover IDP and wait past `refresh_cooldown`; next request loads JWKS and succeeds.
    oidc.set_outage(false);
    oidc.set_jwks_delay(Duration::ZERO);
    tokio::time::sleep(Duration::from_millis(140)).await;
    assert_eq!(
        validator
            .verify(&jwt)
            .await
            .expect("OIDC verification should recover after outage backoff expires")
            .subject,
        "outage-actor"
    );
    assert_eq!(oidc.discovery_fetches(), 2);
    assert_eq!(oidc.jwks_fetches(), 1);

    // TTL-expired cache outage: wait for `jwks_ttl` to expire, trigger outage again, and verify
    // only 1 retry hits the IDP within `refresh_cooldown`, then recover afterward.
    tokio::time::sleep(Duration::from_millis(100)).await;
    oidc.set_outage(true);
    oidc.set_jwks_delay(Duration::from_millis(40));

    let (res_c, res_d) = tokio::join!(validator.verify(&jwt), validator.verify(&jwt));
    assert!(res_c.is_err());
    assert!(res_d.is_err());
    for _ in 0..3 {
        assert!(validator.verify(&jwt).await.is_err());
    }
    assert_eq!(
        oidc.discovery_fetches(),
        3,
        "TTL-expired IDP outage must back off within refresh_cooldown"
    );

    oidc.set_outage(false);
    oidc.set_jwks_delay(Duration::ZERO);
    tokio::time::sleep(Duration::from_millis(140)).await;
    assert_eq!(
        validator
            .verify(&jwt)
            .await
            .expect("OIDC verification should recover after TTL-expired outage backoff")
            .subject,
        "outage-actor"
    );
    assert_eq!(oidc.discovery_fetches(), 4);
    assert_eq!(oidc.jwks_fetches(), 2);
}

#[tokio::test]
async fn oidc_explicit_jwks_uri_bypasses_discovery() {
    let key = TestSigningKey::generate_es256("ec-explicit-uri");
    let oidc = MockOidcServer::start(vec![]).await;
    oidc.rotate_jwks_path("/jwks-v2.json", vec![key.jwk.clone()]);

    let mut cfg = OidcAuthConfig::new(&oidc.issuer, "cloud-jj");
    cfg.jwks_uri = Some(format!("{}/jwks-v2.json", oidc.issuer));
    let validator = OidcValidator::new(cfg).unwrap();

    let jwt = key.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "explicit-jwks-actor",
        "exp": now_unix_secs() + 600,
    }));
    let identity = validator
        .verify(&jwt)
        .await
        .expect("explicit jwks_uri should verify JWT without discovery");
    assert_eq!(identity.subject, "explicit-jwks-actor");
    assert_eq!(
        oidc.discovery_fetches(),
        0,
        "explicit jwks_uri must bypass /.well-known/openid-configuration"
    );
    assert_eq!(oidc.jwks_fetches(), 1);
}

#[tokio::test]
async fn k8s_tokenreview_ttl_expiry_singleflight_and_cancellation_safety() {
    let k8s = MockK8sServer::start(None).await;
    k8s.register_token(
        "ttl-token",
        "system:serviceaccount:jj:ttl-sa",
        Some("ttl-uid"),
        Some("cloud-jj"),
    );

    let mut cfg = K8sTokenReviewConfig::new(&k8s.base_url);
    cfg.audience = Some("cloud-jj".to_string());
    let validator =
        Arc::new(K8sTokenReviewValidator::with_cache_ttl(cfg, Duration::from_millis(80)).unwrap());

    // Positive cache serves hits within TTL even after revocation, then re-verifies and rejects
    // once TTL expires.
    let first = validator
        .verify("ttl-token")
        .await
        .expect("initial K8s TokenReview should succeed");
    assert_eq!(first.subject, "system:serviceaccount:jj:ttl-sa");
    assert_eq!(k8s.requests(), 1);

    k8s.revoke_token("ttl-token");
    assert_eq!(
        validator
            .verify("ttl-token")
            .await
            .expect("cached hit within TTL should succeed")
            .subject,
        "system:serviceaccount:jj:ttl-sa"
    );
    assert_eq!(k8s.requests(), 1);

    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(
        validator.verify("ttl-token").await.is_err(),
        "revoked token must be rejected after cache TTL expiry"
    );
    assert_eq!(k8s.requests(), 2);

    // Concurrent cache misses for the same token digest coalesce into a single TokenReview POST.
    k8s.register_token(
        "burst-token",
        "system:serviceaccount:jj:burst-sa",
        Some("burst-uid"),
        Some("cloud-jj"),
    );
    k8s.set_delay(Duration::from_millis(80));

    let results = futures::future::join_all((0..8).map(|_| {
        let v = Arc::clone(&validator);
        async move { v.verify("burst-token").await }
    }))
    .await;
    for res in results {
        let id = res.expect("coalesced TokenReview caller should succeed");
        assert_eq!(id.subject, "system:serviceaccount:jj:burst-sa");
        assert_eq!(id.actor_uid.as_deref(), Some("burst-uid"));
    }
    assert_eq!(
        k8s.requests(),
        3,
        "8 concurrent misses for the same token must fire exactly 1 TokenReview POST"
    );

    // Cancelling the singleflight leader wakes coalesced waiters so a waiter promotes
    // to leader instead of hanging.
    k8s.register_token(
        "cancel-token",
        "system:serviceaccount:jj:cancel-sa",
        Some("cancel-uid"),
        Some("cloud-jj"),
    );
    k8s.set_delay(Duration::from_millis(200));

    let leader = tokio::spawn({
        let v = Arc::clone(&validator);
        async move { v.verify("cancel-token").await }
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while k8s.requests() < 4 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for singleflight leader to reach mock K8s server"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let waiter = tokio::spawn({
        let v = Arc::clone(&validator);
        async move { v.verify("cancel-token").await }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        k8s.requests(),
        4,
        "waiter must coalesce onto active leader flight instead of firing a duplicate POST"
    );

    k8s.set_delay(Duration::ZERO);
    leader.abort();
    let _ = leader.await;

    let promoted = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("waiter must not hang when singleflight leader is cancelled")
        .expect("waiter task should not panic")
        .expect("waiter should promote to leader and succeed");
    assert_eq!(promoted.subject, "system:serviceaccount:jj:cancel-sa");
    assert_eq!(k8s.requests(), 5);

    // Extreme cache TTL (`Duration::MAX`) must not panic on `Instant` addition overflow.
    let mut max_ttl_cfg = K8sTokenReviewConfig::new(&k8s.base_url);
    max_ttl_cfg.audience = Some("cloud-jj".to_string());
    let max_ttl_validator =
        K8sTokenReviewValidator::with_cache_ttl(max_ttl_cfg, Duration::MAX).unwrap();
    let max_ttl_id = max_ttl_validator
        .verify("burst-token")
        .await
        .expect("Duration::MAX cache TTL must not overflow-panic");
    assert_eq!(max_ttl_id.subject, "system:serviceaccount:jj:burst-sa");
}

#[tokio::test]
async fn real_services_write_commit_attribution_matrix_and_first_writer_preservation() {
    use jj_commit_cloud_server::backend::CommitCloudBackendService;
    use jj_commit_cloud_server::metrics::{MetricsLayer, ServerMetrics};
    use jj_commit_cloud_server::store::CompositeStore;

    let key = TestSigningKey::generate_es256("ec-matrix");
    let oidc = MockOidcServer::start(vec![key.jwk.clone()]).await;
    let k8s = MockK8sServer::start(None).await;
    k8s.register_token(
        "k8s-with-uid",
        "system:serviceaccount:ci:runner",
        Some("k8s-uid-55"),
        None,
    );
    k8s.register_token("k8s-no-uid", "system:serviceaccount:ci:minimal", None, None);

    let mut oidc_cfg = OidcAuthConfig::new(&oidc.issuer, "cloud-jj");
    oidc_cfg.enforce_actor_repo_suffix = true;
    let auth_layer = ServerAuthLayer::new(ServerAuthConfig {
        static_tokens: BearerAuthInterceptor::try_from_tokens(["static-matrix-secret"]).unwrap(),
        oidc: Some(oidc_cfg),
        k8s_tokenreview: Some(K8sTokenReviewConfig::new(&k8s.base_url)),
    })
    .unwrap();
    let enforce = auth_layer.enforce_actor_repo_suffix();

    let store = Arc::new(CompositeStore::in_memory());
    let metrics = ServerMetrics::new();
    let backend_svc = CommitCloudBackendService::with_metrics(store, metrics.clone())
        .with_actor_repo_suffix_enforcement(enforce);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = TcpListenerStream::new(listener);
    let _server = tokio::spawn(async move {
        let _ = Server::builder()
            .layer(MetricsLayer::new(metrics))
            .layer(auth_layer)
            .add_service(BackendServiceServer::new(backend_svc))
            .serve_with_incoming(incoming)
            .await;
    });

    let channel = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut backend = BackendServiceClient::new(channel);

    let now = now_unix_secs();
    let actor_jwt = key.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "system:actor:ns:writer-a",
        "exp": now + 600,
        "actor.ate.dev": {
            "atespace": "ns",
            "actorName": "writer-a",
            "actorUID": "uid-writer-a"
        }
    }));
    let user_jwt = key.sign(&json!({
        "iss": oidc.issuer,
        "aud": "cloud-jj",
        "sub": "human@example.com",
        "exp": now + 600,
    }));

    backend
        .register_project(with_bearer(
            RegisterProjectRequest {
                project_id: Some("attr-proj".to_string()),
                name: None,
            },
            "static-matrix-secret",
        ))
        .await
        .expect("register project should succeed");

    // Non-actor callers can still omit `repo_id` even when actor suffix enforcement is enabled.
    for (label, tok) in [
        ("oidc_user", user_jwt.as_str()),
        ("k8s_sa", "k8s-with-uid"),
        ("static_token", "static-matrix-secret"),
    ] {
        let resp = backend
            .register_repository(with_bearer(
                RegisterRepositoryRequest {
                    project_id: "attr-proj".to_string(),
                    repo_id: None,
                    name: None,
                    hash_algorithm: 0,
                },
                tok,
            ))
            .await
            .unwrap_or_else(|e| panic!("{label} RegisterRepository(None) failed: {e}"))
            .into_inner();
        assert!(!resp.repo_id.is_empty(), "{label} should allocate repo_id");
    }

    let make_spoofed_commit = |desc: &str| {
        let mut commit = sample_commit();
        commit.description = format!("{desc}\n");
        commit
            .external_ids
            .insert(ACTOR_SUB_EXTERNAL_ID_KEY.to_string(), b"spoof-sub".to_vec());
        commit
            .external_ids
            .insert(ACTOR_UID_EXTERNAL_ID_KEY.to_string(), b"spoof-uid".to_vec());
        commit
    };

    // OidcActor stamps verified actor-sub + actor-uid, and a repeat WriteCommit of the same
    // commit by a second caller preserves the first writer's attribution.
    let commit_a = make_spoofed_commit("commit-a-first-writer");
    let commit_a_id = backend
        .write_commit(with_bearer(
            WriteCommitRequest {
                project_id: "attr-proj".to_string(),
                commit: Some(commit_a.clone()),
            },
            &actor_jwt,
        ))
        .await
        .expect("OidcActor WriteCommit should succeed")
        .into_inner()
        .commit_id;
    let repeat_id = backend
        .write_commit(with_bearer(
            WriteCommitRequest {
                project_id: "attr-proj".to_string(),
                commit: Some(commit_a),
            },
            "k8s-with-uid",
        ))
        .await
        .expect("idempotent repeat WriteCommit by second caller should succeed")
        .into_inner()
        .commit_id;
    assert_eq!(repeat_id, commit_a_id);
    let stored_a = backend
        .read_commit(with_bearer(
            ReadCommitRequest {
                project_id: "attr-proj".to_string(),
                commit_id: commit_a_id,
            },
            "static-matrix-secret",
        ))
        .await
        .unwrap()
        .into_inner()
        .commit
        .unwrap();
    assert_eq!(
        stored_a.external_ids.get(ACTOR_SUB_EXTERNAL_ID_KEY),
        Some(&b"system:actor:ns:writer-a".to_vec())
    );
    assert_eq!(
        stored_a.external_ids.get(ACTOR_UID_EXTERNAL_ID_KEY),
        Some(&b"uid-writer-a".to_vec())
    );

    // K8sServiceAccount with UID stamps username + uid.
    let commit_b_id = backend
        .write_commit(with_bearer(
            WriteCommitRequest {
                project_id: "attr-proj".to_string(),
                commit: Some(make_spoofed_commit("commit-b-k8s-with-uid")),
            },
            "k8s-with-uid",
        ))
        .await
        .expect("K8sServiceAccount (with UID) WriteCommit should succeed")
        .into_inner()
        .commit_id;
    let stored_b = backend
        .read_commit(with_bearer(
            ReadCommitRequest {
                project_id: "attr-proj".to_string(),
                commit_id: commit_b_id,
            },
            "static-matrix-secret",
        ))
        .await
        .unwrap()
        .into_inner()
        .commit
        .unwrap();
    assert_eq!(
        stored_b.external_ids.get(ACTOR_SUB_EXTERNAL_ID_KEY),
        Some(&b"system:serviceaccount:ci:runner".to_vec())
    );
    assert_eq!(
        stored_b.external_ids.get(ACTOR_UID_EXTERNAL_ID_KEY),
        Some(&b"k8s-uid-55".to_vec())
    );

    // K8sServiceAccount without UID stamps username and strips spoofed actor-uid.
    let commit_c_id = backend
        .write_commit(with_bearer(
            WriteCommitRequest {
                project_id: "attr-proj".to_string(),
                commit: Some(make_spoofed_commit("commit-c-k8s-no-uid")),
            },
            "k8s-no-uid",
        ))
        .await
        .expect("K8sServiceAccount (without UID) WriteCommit should succeed")
        .into_inner()
        .commit_id;
    let stored_c = backend
        .read_commit(with_bearer(
            ReadCommitRequest {
                project_id: "attr-proj".to_string(),
                commit_id: commit_c_id,
            },
            "static-matrix-secret",
        ))
        .await
        .unwrap()
        .into_inner()
        .commit
        .unwrap();
    assert_eq!(
        stored_c.external_ids.get(ACTOR_SUB_EXTERNAL_ID_KEY),
        Some(&b"system:serviceaccount:ci:minimal".to_vec())
    );
    assert!(
        !stored_c
            .external_ids
            .contains_key(ACTOR_UID_EXTERNAL_ID_KEY)
    );

    // OidcUser stamps sub and clears spoofed actor-uid.
    let commit_d_id = backend
        .write_commit(with_bearer(
            WriteCommitRequest {
                project_id: "attr-proj".to_string(),
                commit: Some(make_spoofed_commit("commit-d-oidc-user")),
            },
            &user_jwt,
        ))
        .await
        .expect("OidcUser WriteCommit should succeed")
        .into_inner()
        .commit_id;
    let stored_d = backend
        .read_commit(with_bearer(
            ReadCommitRequest {
                project_id: "attr-proj".to_string(),
                commit_id: commit_d_id,
            },
            "static-matrix-secret",
        ))
        .await
        .unwrap()
        .into_inner()
        .commit
        .unwrap();
    assert_eq!(
        stored_d.external_ids.get(ACTOR_SUB_EXTERNAL_ID_KEY),
        Some(&b"human@example.com".to_vec())
    );
    assert!(
        !stored_d
            .external_ids
            .contains_key(ACTOR_UID_EXTERNAL_ID_KEY)
    );

    // StaticToken strips both spoofed actor keys while preserving unrelated external_ids.
    let mut commit_e = make_spoofed_commit("commit-e-static-token");
    commit_e
        .external_ids
        .insert("git-sha1".to_string(), vec![0xab; 20]);
    let commit_e_id = backend
        .write_commit(with_bearer(
            WriteCommitRequest {
                project_id: "attr-proj".to_string(),
                commit: Some(commit_e),
            },
            "static-matrix-secret",
        ))
        .await
        .expect("StaticToken WriteCommit should succeed")
        .into_inner()
        .commit_id;
    let stored_e = backend
        .read_commit(with_bearer(
            ReadCommitRequest {
                project_id: "attr-proj".to_string(),
                commit_id: commit_e_id,
            },
            "static-matrix-secret",
        ))
        .await
        .unwrap()
        .into_inner()
        .commit
        .unwrap();
    assert!(
        !stored_e
            .external_ids
            .contains_key(ACTOR_SUB_EXTERNAL_ID_KEY)
    );
    assert!(
        !stored_e
            .external_ids
            .contains_key(ACTOR_UID_EXTERNAL_ID_KEY)
    );
    assert_eq!(stored_e.external_ids.get("git-sha1"), Some(&vec![0xab; 20]));
}

#[tokio::test]
async fn k8s_tokenreview_validates_response_audiences_when_configured() {
    let k8s = MockK8sServer::start(None).await;
    k8s.register_token_with_response_audiences(
        "tok-matching-aud",
        "system:serviceaccount:jj:matched",
        Some("uid-1"),
        None,
        Some(vec!["other-aud".to_string(), "cloud-jj".to_string()]),
    );
    k8s.register_token_with_response_audiences(
        "tok-mismatched-aud",
        "system:serviceaccount:jj:wrong-aud",
        Some("uid-2"),
        None,
        Some(vec!["different-aud".to_string()]),
    );
    k8s.register_token_with_response_audiences(
        "tok-empty-aud",
        "system:serviceaccount:jj:legacy",
        Some("uid-3"),
        None,
        Some(vec![]),
    );

    let mut cfg = K8sTokenReviewConfig::new(&k8s.base_url);
    cfg.audience = Some("cloud-jj".to_string());
    let auth_layer = ServerAuthLayer::new(ServerAuthConfig {
        k8s_tokenreview: Some(cfg),
        ..Default::default()
    })
    .unwrap();
    let srv = TestGrpcServer::start(auth_layer).await;
    let mut backend = BackendServiceClient::new(srv.channel.clone());

    // Response containing the configured audience succeeds.
    backend
        .read_commit(with_bearer(
            ReadCommitRequest::default(),
            "tok-matching-aud",
        ))
        .await
        .expect("matching status.audiences should succeed");
    assert_eq!(k8s.requests(), 1);

    // Response reporting non-empty `status.audiences` without the configured audience fails closed
    // even when `authenticated: true`, and is not cached.
    for expected_requests in [2, 3] {
        let err = backend
            .read_commit(with_bearer(
                ReadCommitRequest::default(),
                "tok-mismatched-aud",
            ))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Unauthenticated);
        assert_eq!(k8s.requests(), expected_requests);
    }

    // Empty `status.audiences` (non-audience-aware authenticators) remains tolerated.
    backend
        .read_commit(with_bearer(ReadCommitRequest::default(), "tok-empty-aud"))
        .await
        .expect("empty status.audiences should remain tolerated");
    assert_eq!(k8s.requests(), 4);

    // Without a configured audience, any `status.audiences` is accepted.
    let unpinned = K8sTokenReviewValidator::new(K8sTokenReviewConfig::new(&k8s.base_url)).unwrap();
    let id = unpinned
        .verify("tok-mismatched-aud")
        .await
        .expect("validator without configured audience should accept status.audiences");
    assert_eq!(id.subject, "system:serviceaccount:jj:wrong-aud");
}
