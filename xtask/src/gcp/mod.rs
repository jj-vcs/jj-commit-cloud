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

//! Google Cloud (Cloud Run, GKE, GCE) provisioning and teardown drivers for `cargo xtask`.
//!
//! # Authority and Crash-Safety Ordering
//! `gcloud_resource_exists` and `kubectl_resource_exists` treat only explicit not-found errors as
//! absent and propagate IAM, RBAC, or network failures. `up gke` checks pre-existing clusters for
//! `deployment/jj-cc-server` and `secret/jj-cc-server-auth` before mutating cluster state.
//! Ownership flags (`*_created` and `gcs_buckets_created`) are persisted via `save_state` before
//! invoking slow `gcloud ... create` commands so interrupted runs remain purgeable. `down` stops
//! compute and retains a storage-only state record whenever created cloud storage remains
//! (non-purge `down`, or `purge` when a created Spanner instance still holds another environment's
//! database); redeploys carry ownership forward and refuse to drop storage.
//!
//! # Cross-Process and RPC Boundaries
//! GKE generates a bearer token piped via stdin into Kubernetes `Secret` `jj-cc-server-auth` in
//! `--namespace default` and reaches `ClusterIP` via loopback `kubectl port-forward`; GCE defaults
//! to private IAP tunnels. Both monitor child processes during health checks to fail fast on exit.

mod deploy;
mod infra;
mod teardown;
#[cfg(test)]
mod tests;

use std::net::IpAddr;

pub use deploy::up_gcp;
pub use infra::{GcpTools, locate_kubectl, mint_gcloud_identity_token, preflight_gcp_tools};
pub use teardown::down_gcp;

use crate::state::{BoxError, EnvState, ManagedResources};

/// Command-line marker of the `kubectl port-forward` that reaches a GKE deployment.
const GKE_PORT_FORWARD_MARKER: &str = "port-forward service/jj-cc-server";

/// Kubernetes Deployment name declared in `deploy/k8s/gcp/gke.yaml`.
const GKE_DEPLOYMENT_NAME: &str = "jj-cc-server";

/// Kubernetes Secret mounted at `/etc/jj-cc/auth/tokens` by `deploy/k8s/gcp/gke.yaml`.
const GKE_AUTH_SECRET_NAME: &str = "jj-cc-server-auth";

/// Kubernetes namespace matching the Workload Identity binding `default/jj-cc-server-ksa`.
const GKE_NAMESPACE: &str = "default";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GcpMode {
    CloudRun,
    Gce,
    Gke,
}

impl GcpMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::CloudRun => "cloud-run",
            Self::Gce => "gce",
            Self::Gke => "gke",
        }
    }
}

#[derive(Debug)]
pub struct GcpDeployOptions {
    pub project_id: String,
    pub region: String,
    pub peer_region: Option<String>,
    pub spanner_instance: String,
    pub spanner_database: String,
    pub gke_cluster: String,
    pub image_override: Option<String>,
    pub local_port: u16,
    pub local_peer_port: u16,
    pub gce_public_ip: bool,
    /// Source ranges allowed to reach a public GCE server (`--allow-cidr`).
    pub allow_cidrs: Vec<String>,
}

/// Default name of an env-scoped GCP resource: `base` for the `default` env, which keeps the
/// names earlier versions used, and `base-<env>` for named envs, so separate `--env` slots in
/// one project never share or purge each other's databases, buckets, clusters, or services.
pub fn env_resource_name(base: &str, env_name: &str) -> String {
    if env_name == "default" {
        base.to_string()
    } else {
        format!("{base}-{env_name}")
    }
}

/// Rejects env names that cannot appear in GCP resource names. The tightest limit is the
/// 30-character Spanner database ID `commit-cloud-<env>`.
fn validate_gcp_env_name(env_name: &str) -> Result<(), BoxError> {
    let valid = (1..=17).contains(&env_name.len())
        && env_name.starts_with(|c: char| c.is_ascii_lowercase())
        && !env_name.ends_with('-')
        && env_name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    if valid {
        Ok(())
    } else {
        Err(format!(
            "`--env {env_name}` cannot name GCP resources: use 1-17 lowercase letters, digits, or hyphens, starting with a letter"
        )
        .into())
    }
}

fn blob_bucket_name(project_id: &str, env_name: &str, region: &str) -> Result<String, BoxError> {
    let bucket = format!("{project_id}-jj-blobs-{env_name}-{region}");
    if bucket.len() > 63 {
        return Err(format!(
            "bucket name `{bucket}` is longer than 63 characters; use a shorter `--env`"
        )
        .into());
    }
    Ok(bucket)
}

/// Parses an IPv4 `--allow-cidr` value. Zero-length prefixes are rejected so a public GCE server
/// only accepts connections from explicit ranges, and IPv6 CIDRs are rejected because `xtask up
/// gce` provisions IPv4-only VMs.
pub fn parse_allow_cidr(value: &str) -> Result<String, String> {
    let (addr, prefix) = value
        .split_once('/')
        .ok_or_else(|| format!("`{value}` is not an IPv4 CIDR range such as 203.0.113.0/24"))?;
    let ip: IpAddr = addr
        .parse()
        .map_err(|_| format!("`{addr}` is not an IP address"))?;
    if ip.is_ipv6() {
        return Err(format!(
            "IPv6 CIDR `{value}` is not supported: `xtask up gce` provisions IPv4-only VMs; pass an IPv4 CIDR such as 203.0.113.0/24"
        ));
    }
    match prefix.parse::<u8>() {
        Ok(len) if (1..=32).contains(&len) => Ok(value.to_string()),
        _ => Err(format!(
            "prefix `/{prefix}` must be between /1 and /32; xtask never opens the server to every address"
        )),
    }
}

/// Generates a 244-bit bearer token from two v4 UUIDs for GCE public-IP or GKE auth.
fn generate_auth_token() -> String {
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

/// Kubernetes context that `gcloud container clusters get-credentials` writes for a cluster.
fn gke_kube_context(project_id: &str, region: &str, cluster: &str) -> String {
    format!("gke_{project_id}_{region}_{cluster}")
}

/// Human-readable list of created GCP cloud storage/cluster resources tracked in `res`.
pub fn describe_created_cloud_storage(res: &ManagedResources) -> String {
    let mut items = Vec::new();
    if res.gke_cluster_created {
        let cluster = res.gke_cluster.as_deref().unwrap_or("<unknown>");
        if let Some(region) = res.gke_region.as_deref() {
            items.push(format!("GKE cluster {cluster} ({region})"));
        } else {
            items.push(format!("GKE cluster {cluster}"));
        }
    }
    if res.spanner_instance_created {
        let inst = res.spanner_instance.as_deref().unwrap_or("<unknown>");
        items.push(format!("Spanner instance {inst}"));
    }
    if res.spanner_database_created {
        let inst = res.spanner_instance.as_deref().unwrap_or("<unknown>");
        let db = res.spanner_database.as_deref().unwrap_or("<unknown>");
        items.push(format!("Spanner database {inst}/{db}"));
    }
    for bucket in &res.gcs_buckets_created {
        items.push(format!("Cloud Storage bucket gs://{bucket}"));
    }
    items.join(", ")
}

fn validate_gcp_dns_id(label: &str, value: &str, max_len: usize) -> Result<(), BoxError> {
    let valid = (1..=max_len).contains(&value.len())
        && value.starts_with(|c: char| c.is_ascii_lowercase())
        && !value.ends_with('-')
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    if valid {
        Ok(())
    } else {
        Err(format!(
            "invalid {label} `{value}`: must be 1-{max_len} lowercase ASCII letters, digits, or hyphens, starting with a letter and not ending with a hyphen"
        )
        .into())
    }
}

fn validate_spanner_database_id(value: &str) -> Result<(), BoxError> {
    let valid = (1..=30).contains(&value.len())
        && value.starts_with(|c: char| c.is_ascii_lowercase())
        && value
            .bytes()
            .last()
            .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_');
    if valid {
        Ok(())
    } else {
        Err(format!(
            "invalid `--spanner-database` `{value}`: must be 1-30 lowercase ASCII letters, digits, hyphens, or underscores, starting with a letter and ending with a letter or digit"
        )
        .into())
    }
}

fn validate_image_override(value: &str) -> Result<(), BoxError> {
    let valid = !value.is_empty()
        && !value.starts_with('-')
        && value
            .bytes()
            .all(|b| b.is_ascii_graphic() && !matches!(b, b'"' | b'\'' | b'\\'));
    if valid {
        Ok(())
    } else {
        Err(format!(
            "invalid `--image` `{value}`: must be a non-empty container image URI without whitespace, quotes, or backslashes"
        )
        .into())
    }
}

/// Pure validation of GCP deployment options before checking local ports or calling `gcloud`.
pub fn validate_gcp_options(
    env_name: &str,
    mode: GcpMode,
    opts: &GcpDeployOptions,
) -> Result<(), BoxError> {
    validate_gcp_env_name(env_name)?;
    validate_gcp_dns_id("`--project`", &opts.project_id, 30)?;
    validate_gcp_dns_id("`--region`", &opts.region, 63)?;
    if let Some(peer_reg) = &opts.peer_region {
        validate_gcp_dns_id("`--peer-region`", peer_reg, 63)?;
    }
    validate_gcp_dns_id("`--spanner-instance`", &opts.spanner_instance, 64)?;
    validate_spanner_database_id(&opts.spanner_database)?;
    validate_gcp_dns_id("`--gke-cluster`", &opts.gke_cluster, 40)?;
    if let Some(img) = &opts.image_override {
        validate_image_override(img)?;
    }
    if mode == GcpMode::Gke && opts.peer_region.is_some() {
        return Err(
            "`--peer-region` is supported on `up cloud-run` and `up gce`, not `up gke`".into(),
        );
    }
    if opts.peer_region.as_deref() == Some(opts.region.as_str()) {
        return Err(format!(
            "`--peer-region` ({}) must differ from `--region` ({})",
            opts.region, opts.region
        )
        .into());
    }
    if opts.gce_public_ip && mode != GcpMode::Gce {
        return Err("`--gce-public-ip` only applies to `up gce`".into());
    }
    if opts.gce_public_ip && opts.allow_cidrs.is_empty() {
        return Err(
            "`--gce-public-ip` requires `--allow-cidr <CIDR>` naming the networks that may connect"
                .into(),
        );
    }
    if mode == GcpMode::Gke || (mode == GcpMode::Gce && !opts.gce_public_ip) {
        if opts.local_port == 0 {
            return Err("`--port` must be between 1 and 65535".into());
        }
        if mode == GcpMode::Gce && opts.peer_region.is_some() && opts.local_peer_port == 0 {
            return Err("`--peer-port` must be between 1 and 65535".into());
        }
    }
    let primary_local_port = if opts.peer_region.is_some() && opts.local_port == 8080 {
        8081
    } else {
        opts.local_port
    };
    if mode == GcpMode::Gce
        && !opts.gce_public_ip
        && opts.peer_region.is_some()
        && primary_local_port == opts.local_peer_port
    {
        return Err(format!(
            "GCE IAP tunnel primary port ({primary_local_port}) and peer port ({}) must differ; pass distinct `--port` and `--peer-port`",
            opts.local_peer_port
        )
        .into());
    }
    let _ = blob_bucket_name(&opts.project_id, env_name, &opts.region)?;
    if let Some(peer_reg) = &opts.peer_region {
        let _ = blob_bucket_name(&opts.project_id, env_name, peer_reg)?;
    }
    Ok(())
}

/// Refuses a GCP redeploy or `--replace` when the new `up` invocation would stop tracking any
/// created cloud resource (project, GKE cluster, Spanner instance/database, or Cloud Storage
/// bucket) recorded in `previous`.
pub fn ensure_gcp_ownership_preserved(
    previous: &EnvState,
    env_name: &str,
    mode: GcpMode,
    opts: &GcpDeployOptions,
) -> Result<(), BoxError> {
    if !previous.resources.has_created_cloud_storage() {
        return Ok(());
    }
    let primary_bucket = blob_bucket_name(&opts.project_id, env_name, &opts.region)?;
    let peer_bucket = match &opts.peer_region {
        Some(peer_reg) => Some(blob_bucket_name(&opts.project_id, env_name, peer_reg)?),
        None => None,
    };

    let same_project = previous.project_id.as_deref() == Some(opts.project_id.as_str());
    let mut dropped = Vec::new();

    if previous.resources.gke_cluster_created {
        let kept = same_project
            && mode == GcpMode::Gke
            && previous.resources.gke_cluster.as_deref() == Some(opts.gke_cluster.as_str())
            && previous.resources.gke_region.as_deref() == Some(opts.region.as_str());
        if !kept {
            let cluster = previous
                .resources
                .gke_cluster
                .as_deref()
                .unwrap_or("<unknown>");
            let region = previous
                .resources
                .gke_region
                .as_deref()
                .unwrap_or("<unknown>");
            dropped.push(format!("GKE cluster {cluster} ({region})"));
        }
    }
    if previous.resources.spanner_instance_created {
        let kept = same_project
            && previous.resources.spanner_instance.as_deref()
                == Some(opts.spanner_instance.as_str());
        if !kept {
            let inst = previous
                .resources
                .spanner_instance
                .as_deref()
                .unwrap_or("<unknown>");
            dropped.push(format!("Spanner instance {inst}"));
        }
    }
    if previous.resources.spanner_database_created {
        let kept = same_project
            && previous.resources.spanner_instance.as_deref()
                == Some(opts.spanner_instance.as_str())
            && previous.resources.spanner_database.as_deref()
                == Some(opts.spanner_database.as_str());
        if !kept {
            let inst = previous
                .resources
                .spanner_instance
                .as_deref()
                .unwrap_or("<unknown>");
            let db = previous
                .resources
                .spanner_database
                .as_deref()
                .unwrap_or("<unknown>");
            dropped.push(format!("Spanner database {inst}/{db}"));
        }
    }
    for bucket in &previous.resources.gcs_buckets_created {
        let kept = same_project
            && (bucket == &primary_bucket || peer_bucket.as_deref() == Some(bucket.as_str()));
        if !kept {
            dropped.push(format!("Cloud Storage bucket gs://{bucket}"));
        }
    }

    if dropped.is_empty() {
        return Ok(());
    }
    let suffix = if env_name == "default" {
        String::new()
    } else {
        format!(" --env {env_name}")
    };
    let project_note = if !same_project {
        format!(
            " (previous project: {}, requested project: {})",
            previous.project_id.as_deref().unwrap_or("<none>"),
            opts.project_id
        )
    } else {
        String::new()
    };
    Err(format!(
        "environment '{env_name}' still tracks created GCP resource(s) that `up {}` would stop tracking{project_note}: {}. Run `cargo xtask down{suffix} --purge` first or choose a different `--env` slot",
        mode.as_str(),
        dropped.join(", "),
    )
    .into())
}

/// Carries forward matching `*_created` storage ownership metadata from a replaced or
/// storage-only previous state within the same GCP project.
fn carry_forward_gcp_ownership(
    previous: Option<&EnvState>,
    target: &mut EnvState,
    opts: &GcpDeployOptions,
    primary_bucket: &str,
    peer_bucket: Option<&str>,
    mode: GcpMode,
) {
    let Some(prev) = previous else {
        return;
    };
    if prev.project_id.as_deref() != Some(opts.project_id.as_str()) {
        return;
    }
    let same_instance =
        prev.resources.spanner_instance.as_deref() == Some(opts.spanner_instance.as_str());
    if same_instance && prev.resources.spanner_instance_created {
        target.resources.spanner_instance = Some(opts.spanner_instance.clone());
        target.resources.spanner_instance_created = true;
    }
    if same_instance
        && prev.resources.spanner_database.as_deref() == Some(opts.spanner_database.as_str())
        && prev.resources.spanner_database_created
    {
        target.resources.spanner_database = Some(opts.spanner_database.clone());
        target.resources.spanner_database_created = true;
    }
    if mode == GcpMode::Gke
        && prev.resources.gke_cluster.as_deref() == Some(opts.gke_cluster.as_str())
        && prev.resources.gke_region.as_deref() == Some(opts.region.as_str())
        && prev.resources.gke_cluster_created
    {
        target.resources.gke_cluster = Some(opts.gke_cluster.clone());
        target.resources.gke_region = Some(opts.region.clone());
        target.resources.gke_cluster_created = true;
    }
    for bucket in std::iter::once(primary_bucket).chain(peer_bucket) {
        if prev
            .resources
            .gcs_buckets_created
            .iter()
            .any(|b| b == bucket)
        {
            if !target.resources.gcs_buckets.iter().any(|b| b == bucket) {
                target.resources.gcs_buckets.push(bucket.to_string());
            }
            if !target
                .resources
                .gcs_buckets_created
                .iter()
                .any(|b| b == bucket)
            {
                target
                    .resources
                    .gcs_buckets_created
                    .push(bucket.to_string());
            }
        }
    }
}
