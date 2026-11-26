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

//! gRPC health probing, bearer-token resolution, and readiness polling for `cargo xtask`.
//!
//! # Cross-Process and RPC Boundaries
//! Probes `tonic_health` `Health::Check` over HTTP/2 gRPC, installing an explicit rustls `ring`
//! provider for HTTPS endpoints and stripping brackets from IPv6 literals for TLS SNI. Polling
//! loops inspect watched child processes or container states each tick to fail fast on exit.

use std::path::Path;
use std::process::Command;
use std::sync::Once;
use std::time::{Duration, Instant};

use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

use super::WatchedChild;
use crate::gcp::mint_gcloud_identity_token;
use crate::state::{AuthMode, BoxError};

pub(crate) fn is_https_url(url: &str) -> bool {
    url.get(..8)
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("https://"))
}

pub(super) fn resolve_bearer_token_with<F>(
    auth: AuthMode,
    server_token: Option<&str>,
    env_token: Option<String>,
    mint_identity_token: F,
) -> Result<Option<String>, BoxError>
where
    F: FnOnce() -> Result<String, BoxError>,
{
    if let Some(token) = server_token.map(str::trim).filter(|t| !t.is_empty()) {
        return Ok(Some(token.to_string()));
    }
    let clean_env = env_token
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty());
    Ok(match auth {
        AuthMode::None => clean_env,
        AuthMode::GcloudIdentityToken => match clean_env {
            Some(t) => Some(t),
            None => Some(mint_identity_token()?),
        },
    })
}

/// Returns the bearer token to send: the token `up` configured on the server, else non-empty
/// `JJ_CC_AUTH_TOKEN`, else (Cloud Run) a fresh `gcloud` identity token.
pub fn resolve_bearer_token(
    auth: AuthMode,
    server_token: Option<&str>,
) -> Result<Option<String>, BoxError> {
    resolve_bearer_token_with(
        auth,
        server_token,
        std::env::var("JJ_CC_AUTH_TOKEN").ok(),
        mint_gcloud_identity_token,
    )
}

static RUSTLS_PROVIDER_INIT: Once = Once::new();

fn ensure_rustls_crypto_provider() {
    RUSTLS_PROVIDER_INIT.call_once(|| {
        // Workspace feature unification can enable both `ring` and `aws_lc_rs` on rustls 0.23,
        // causing `ClientConfig::builder()` inside tonic to panic without an explicit default.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

pub(super) fn build_health_endpoint(url: &str) -> Result<Endpoint, BoxError> {
    let mut ep = Channel::from_shared(url.to_string())?
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(5));
    if is_https_url(url) {
        ensure_rustls_crypto_provider();
        let mut tls = ClientTlsConfig::new().with_enabled_roots();
        if let Some(host) = ep.uri().host()
            && let Some(unbracketed) = host.strip_prefix('[').and_then(|s| s.strip_suffix(']'))
        {
            // Tonic passes `uri.host()` verbatim to `ServerName::try_from`, which rejects
            // bracketed IPv6 literals (`[::1]`) unless the brackets are stripped here.
            tls = tls.domain_name(unbracketed.to_string());
        }
        ep = ep.tls_config(tls)?;
    }
    Ok(ep)
}

pub(super) async fn check_grpc_health_with_token(
    url: &str,
    token: Option<&str>,
) -> Result<bool, BoxError> {
    let ep = build_health_endpoint(url)?;
    let channel = ep.connect().await?;

    let mut req = tonic::Request::new(HealthCheckRequest {
        service: String::new(),
    });
    if let Some(tok) = token {
        let header_val: MetadataValue<_> = format!("Bearer {}", tok.trim()).parse()?;
        req.metadata_mut().insert("authorization", header_val);
    }

    let mut client = HealthClient::new(channel);
    let resp = client.check(req).await?;
    Ok(resp.into_inner().status == ServingStatus::Serving as i32)
}

pub async fn check_grpc_health(
    url: &str,
    auth: AuthMode,
    server_token: Option<&str>,
) -> Result<bool, BoxError> {
    let token = resolve_bearer_token(auth, server_token)?;
    check_grpc_health_with_token(url, token.as_deref()).await
}

fn container_is_running(rt: &Path, container_name: &str) -> Option<bool> {
    let out = Command::new(rt)
        .args(["inspect", "--format", "{{.State.Running}}", container_name])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    match String::from_utf8_lossy(&out.stdout).trim() {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn check_containers_running(rt: &Path, containers: &[String]) -> Result<(), BoxError> {
    for container in containers {
        if container_is_running(rt, container) == Some(false) {
            return Err(format!(
                "container '{container}' exited during startup; inspect logs with `{} logs {container}`",
                rt.display()
            )
            .into());
        }
    }
    Ok(())
}

pub(super) async fn wait_until_healthy_with_containers(
    url: &str,
    auth: AuthMode,
    server_token: Option<&str>,
    timeout: Duration,
    rt: &Path,
    containers: &[String],
) -> Result<(), BoxError> {
    wait_until_healthy_with_containers_poll(
        url,
        auth,
        server_token,
        timeout,
        Duration::from_millis(250),
        rt,
        containers,
    )
    .await
}

pub(super) async fn wait_until_healthy_with_containers_poll(
    url: &str,
    auth: AuthMode,
    server_token: Option<&str>,
    timeout: Duration,
    poll_interval: Duration,
    rt: &Path,
    containers: &[String],
) -> Result<(), BoxError> {
    let token = resolve_bearer_token(auth, server_token)?;
    let deadline = Instant::now() + timeout;
    let mut last_err = String::new();
    while Instant::now() < deadline {
        check_containers_running(rt, containers)?;
        match check_grpc_health_with_token(url, token.as_deref()).await {
            Ok(true) => return Ok(()),
            Ok(false) => last_err = "service reported non-SERVING status".to_string(),
            Err(e) => last_err = e.to_string(),
        }
        tokio::time::sleep(poll_interval).await;
    }
    check_containers_running(rt, containers)?;
    Err(format!("timed out waiting for gRPC health at {url}: {last_err}").into())
}

pub async fn wait_until_healthy(
    url: &str,
    auth: AuthMode,
    server_token: Option<&str>,
    timeout: Duration,
) -> Result<(), BoxError> {
    let token = resolve_bearer_token(auth, server_token)?;
    let deadline = Instant::now() + timeout;
    let mut last_err = String::new();
    while Instant::now() < deadline {
        match check_grpc_health_with_token(url, token.as_deref()).await {
            Ok(true) => return Ok(()),
            Ok(false) => last_err = "service reported non-SERVING status".to_string(),
            Err(e) => last_err = e.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(format!("timed out waiting for gRPC health at {url}: {last_err}").into())
}

pub async fn wait_until_healthy_with_children(
    url: &str,
    auth: AuthMode,
    server_token: Option<&str>,
    timeout: Duration,
    children: &mut [WatchedChild],
) -> Result<(), BoxError> {
    let token = resolve_bearer_token(auth, server_token)?;
    let deadline = Instant::now() + timeout;
    let mut last_err = String::new();
    while Instant::now() < deadline {
        for watched in children.iter_mut() {
            if let Some(status) = watched.child.try_wait()? {
                return Err(format!(
                    "{} (PID {}) exited during startup with {status} (log: {})",
                    watched.label,
                    watched.child.id(),
                    watched.log_path.display()
                )
                .into());
            }
        }
        match check_grpc_health_with_token(url, token.as_deref()).await {
            Ok(true) => return Ok(()),
            Ok(false) => last_err = "service reported non-SERVING status".to_string(),
            Err(e) => last_err = e.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let logs: Vec<String> = children
        .iter()
        .map(|w| w.log_path.display().to_string())
        .collect();
    if logs.is_empty() {
        Err(format!("timed out waiting for gRPC health at {url}: {last_err}").into())
    } else {
        Err(format!(
            "timed out waiting for gRPC health at {url}: {last_err} (log: {})",
            logs.join(", ")
        )
        .into())
    }
}
