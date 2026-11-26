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

//! Google Cloud CLI discovery, existence checks, and shared infrastructure provisioning.
//!
//! # Authority and Crash-Safety Ordering
//! `gcloud_resource_exists` and `kubectl_resource_exists` treat only explicit not-found errors as
//! absent and propagate IAM, RBAC, or network failures. Ownership flags (`*_created` and
//! `gcs_buckets_created`) are persisted via `save_state` before running slow `gcloud ... create`
//! commands so interrupted provisions remain purgeable on retry.
//!
//! # Cross-Process and RPC Boundaries
//! `apply_gke_auth_secret` streams the Kubernetes `Secret` manifest over `kubectl apply -f -`
//! stdin so bearer tokens never appear in process arguments or temporary files on disk.

use std::ffi::{OsStr, OsString};
use std::io::Write;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use super::{GKE_AUTH_SECRET_NAME, GKE_DEPLOYMENT_NAME, GKE_NAMESPACE, GcpDeployOptions, GcpMode};
use crate::state::{BoxError, EnvState, save_state};

const GCLOUD_CANDIDATES: &[&str] = if cfg!(windows) {
    &["gcloud.cmd", "gcloud"]
} else {
    &["gcloud"]
};

const KUBECTL_CANDIDATES: &[&str] = if cfg!(windows) {
    &["kubectl.exe", "kubectl"]
} else {
    &["kubectl"]
};

pub(super) fn locate_gcloud() -> Result<PathBuf, BoxError> {
    locate_gcloud_with(
        std::env::var("GCLOUD_BIN").ok().as_deref(),
        GCLOUD_CANDIDATES,
    )
}

pub(super) fn locate_gcloud_with(
    explicit: Option<&str>,
    candidates: &[&str],
) -> Result<PathBuf, BoxError> {
    if let Some(explicit) = explicit.map(str::trim).filter(|s| !s.is_empty()) {
        let p = PathBuf::from(explicit);
        if p.exists() {
            return Ok(p);
        }
        return Err(format!("GCLOUD_BIN points to missing executable: {}", p.display()).into());
    }
    for cand in candidates {
        let p = PathBuf::from(cand);
        if Command::new(&p)
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|s| s.success())
        {
            return Ok(p);
        }
    }
    Err("`gcloud` CLI was not found on PATH (or set GCLOUD_BIN); install Google Cloud SDK before running GCP modes".into())
}

pub fn locate_kubectl() -> Result<PathBuf, BoxError> {
    locate_kubectl_with(
        std::env::var("KUBECTL_BIN").ok().as_deref(),
        KUBECTL_CANDIDATES,
    )
}

pub(super) fn locate_kubectl_with(
    explicit: Option<&str>,
    candidates: &[&str],
) -> Result<PathBuf, BoxError> {
    if let Some(explicit) = explicit.map(str::trim).filter(|s| !s.is_empty()) {
        let p = PathBuf::from(explicit);
        if p.exists() {
            return Ok(p);
        }
        return Err(format!("KUBECTL_BIN points to missing executable: {}", p.display()).into());
    }
    for cand in candidates {
        let p = PathBuf::from(cand);
        if Command::new(&p)
            .args(["version", "--client"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|s| s.success())
        {
            return Ok(p);
        }
    }
    Err("`kubectl` CLI was not found on PATH (or set KUBECTL_BIN); install kubectl before running Kubernetes commands".into())
}

pub struct GcpTools {
    pub gcloud: PathBuf,
    pub kubectl: Option<PathBuf>,
}

pub fn preflight_gcp_tools(mode: GcpMode) -> Result<GcpTools, BoxError> {
    let gcloud = locate_gcloud()?;
    let kubectl = if mode == GcpMode::Gke {
        Some(locate_kubectl()?)
    } else {
        None
    };
    Ok(GcpTools { gcloud, kubectl })
}

pub fn mint_gcloud_identity_token() -> Result<String, BoxError> {
    let gcloud = locate_gcloud()?;
    let out = Command::new(gcloud)
        .args(["auth", "print-identity-token"])
        .output()?;
    if !out.status.success() {
        return Err(format!(
            "`gcloud auth print-identity-token` failed: {}",
            String::from_utf8_lossy(&out.stderr)
        )
        .into());
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

pub(super) fn run_gcloud<I, S>(gcloud: &Path, args: I) -> Result<String, BoxError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let owned_args: Vec<OsString> = args
        .into_iter()
        .map(|s| s.as_ref().to_os_string())
        .collect();
    let out = Command::new(gcloud).args(&owned_args).output()?;
    if !out.status.success() {
        let display_args: Vec<String> = owned_args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        return Err(format!(
            "`gcloud {}` failed:\n{}",
            display_args.join(" "),
            String::from_utf8_lossy(&out.stderr)
        )
        .into());
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

pub(super) fn classify_gcloud_exists(
    status_success: bool,
    stderr: &str,
    cmd_desc: &str,
) -> Result<bool, BoxError> {
    if status_success {
        Ok(true)
    } else if is_not_found(stderr) {
        Ok(false)
    } else {
        Err(format!("`gcloud {cmd_desc}` failed:\n{}", stderr.trim()).into())
    }
}

/// Checks whether a GCP resource exists: returns `Ok(true)` on exit 0, `Ok(false)` only when
/// stderr reports not-found, and `Err` for permission, auth, or network failures.
pub(super) fn gcloud_resource_exists(gcloud: &Path, args: &[&str]) -> Result<bool, BoxError> {
    let out = Command::new(gcloud).args(args).output()?;
    let stderr = String::from_utf8_lossy(&out.stderr);
    classify_gcloud_exists(out.status.success(), &stderr, &args.join(" "))
}

/// Fails when an env-named resource already exists. State files record only resources xtask
/// created, so `up` refuses to adopt one left behind by a run whose state file was lost.
pub(super) fn ensure_absent(
    gcloud: &Path,
    describe_args: &[&str],
    what: &str,
    delete_cmd: &str,
) -> Result<(), BoxError> {
    if gcloud_resource_exists(gcloud, describe_args)? {
        return Err(format!(
            "{what} already exists but is not recorded in this environment's state; delete it with `{delete_cmd}` or use another `--env`"
        )
        .into());
    }
    Ok(())
}

/// Returns true only when the Kubernetes API server explicitly reports `NotFound`, so auth
/// plugin, missing-context, RBAC, and network errors fail closed.
pub(super) fn is_kubectl_not_found(stderr: &str) -> bool {
    stderr
        .to_ascii_lowercase()
        .contains("error from server (notfound)")
}

pub(super) fn classify_kubectl_exists(
    status_success: bool,
    stderr: &str,
    cmd_desc: &str,
) -> Result<bool, BoxError> {
    if status_success {
        Ok(true)
    } else if is_kubectl_not_found(stderr) {
        Ok(false)
    } else {
        Err(format!("`kubectl {cmd_desc}` failed:\n{}", stderr.trim()).into())
    }
}

/// Checks whether a Kubernetes resource exists: returns `Ok(true)` on exit 0, `Ok(false)` only
/// when stderr reports `Error from server (NotFound)`, and `Err` for RBAC, context, auth, or
/// network failures.
pub(super) fn kubectl_resource_exists(kubectl: &Path, args: &[&str]) -> Result<bool, BoxError> {
    let out = Command::new(kubectl).args(args).output()?;
    let stderr = String::from_utf8_lossy(&out.stderr);
    classify_kubectl_exists(out.status.success(), &stderr, &args.join(" "))
}

/// Refuses to clobber an existing `jj-cc-server` deployment or rotate `Secret/jj-cc-server-auth`
/// on a pre-existing GKE cluster before `up gke` mutates cluster state.
pub(super) fn ensure_gke_workload_absent(
    kubectl: &Path,
    kube_context: &str,
) -> Result<(), BoxError> {
    for (kind, name) in [
        ("deployment", GKE_DEPLOYMENT_NAME),
        ("secret", GKE_AUTH_SECRET_NAME),
    ] {
        let args = [
            "--context",
            kube_context,
            "--namespace",
            GKE_NAMESPACE,
            "get",
            kind,
            name,
        ];
        if kubectl_resource_exists(kubectl, &args)? {
            return Err(format!(
                "GKE {kind} {name} in namespace {GKE_NAMESPACE} (context {kube_context}) already exists but is not recorded in this environment's state; delete it with `kubectl --context {kube_context} --namespace {GKE_NAMESPACE} delete {kind} {name}` or use another `--gke-cluster` / `--env`"
            )
            .into());
        }
    }
    Ok(())
}

/// Chooses an `UP` Compute Engine zone in `region`, preferring `<region>-a` when available and
/// falling back to the first alphabetically sorted `UP` zone (for example `europe-west1-b`).
pub(super) fn pick_zone_from_list(region: &str, raw_zones: &str) -> Result<String, BoxError> {
    let prefix = format!("{region}-");
    let mut zones: Vec<String> = raw_zones
        .lines()
        .map(str::trim)
        .filter(|z| !z.is_empty() && z.starts_with(&prefix))
        .map(ToString::to_string)
        .collect();
    zones.sort();
    zones.dedup();
    if zones.is_empty() {
        return Err(format!("no UP Compute Engine zones found in region `{region}`").into());
    }
    let preferred = format!("{region}-a");
    if zones.iter().any(|z| z == &preferred) {
        Ok(preferred)
    } else {
        Ok(zones.remove(0))
    }
}

pub(super) fn gce_zone_list_filter(region: &str) -> String {
    format!("name~^{region}-[a-z]+$ AND status=UP")
}

pub(super) fn select_gce_zone(
    gcloud: &Path,
    project_id: &str,
    region: &str,
) -> Result<String, BoxError> {
    let proj_flag = format!("--project={project_id}");
    let filter = gce_zone_list_filter(region);
    let raw = run_gcloud(
        gcloud,
        [
            "compute",
            "zones",
            "list",
            &proj_flag,
            "--filter",
            &filter,
            "--format=value(name)",
        ],
    )?;
    pick_zone_from_list(region, &raw)
}

pub(super) fn parse_gce_nat_ip(vm_name: &str, raw: &str) -> Result<String, BoxError> {
    let ip = raw.trim();
    if ip.is_empty() || ip.parse::<IpAddr>().is_err() {
        return Err(
            format!("GCE VM {vm_name} did not report a valid external natIP (got {ip:?})").into(),
        );
    }
    Ok(ip.to_string())
}

/// Applies the bearer-token Kubernetes Secret via stdin so the secret never appears in process
/// arguments or temporary files on disk.
pub(super) fn apply_gke_auth_secret(
    kubectl: &Path,
    kube_context: &str,
    secret_name: &str,
    token: &str,
) -> Result<(), BoxError> {
    let manifest = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": secret_name },
        "type": "Opaque",
        "stringData": { "tokens": format!("{token}\n") }
    });
    let mut child = Command::new(kubectl)
        .args([
            "--context",
            kube_context,
            "--namespace",
            GKE_NAMESPACE,
            "apply",
            "-f",
            "-",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    if let Some(mut stdin) = child.stdin.take() {
        serde_json::to_writer(&mut stdin, &manifest)?;
        stdin.write_all(b"\n")?;
    }
    let out = child.wait_with_output()?;
    if !out.status.success() {
        return Err(format!(
            "`kubectl apply` for Secret {secret_name} failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        )
        .into());
    }
    Ok(())
}

pub(super) struct SharedGcpInfra {
    pub(super) sa_email: String,
    pub(super) image_uri: String,
}

/// Provisions project APIs, Cloud Spanner instance and database, runtime IAM service account,
/// Cloud Storage buckets, and the server container image in Artifact Registry.
pub(super) fn provision_shared_infra(
    gcloud: &Path,
    root: &Path,
    mode: GcpMode,
    opts: &GcpDeployOptions,
    primary_bucket: &str,
    peer_bucket: Option<&str>,
    state: &mut EnvState,
) -> Result<SharedGcpInfra, BoxError> {
    let mut apis = vec![
        "spanner.googleapis.com",
        "storage.googleapis.com",
        "artifactregistry.googleapis.com",
        "cloudbuild.googleapis.com",
        "iam.googleapis.com",
    ];
    match mode {
        GcpMode::CloudRun => apis.push("run.googleapis.com"),
        GcpMode::Gce => {
            apis.push("compute.googleapis.com");
            apis.push("iap.googleapis.com");
        }
        GcpMode::Gke => apis.push("container.googleapis.com"),
    }
    println!(
        "==> Enabling Google Cloud APIs in project {}...",
        opts.project_id
    );
    let mut enable_args = vec!["services", "enable"];
    enable_args.extend(apis);
    let proj_flag = format!("--project={}", opts.project_id);
    enable_args.push(&proj_flag);
    run_gcloud(gcloud, &enable_args)?;

    // Record Spanner ownership before slow create commands so interrupted creations remain
    // purgeable without claiming pre-existing resources.
    state.resources.spanner_instance = Some(opts.spanner_instance.clone());
    save_state(root, state)?;
    if !gcloud_resource_exists(
        gcloud,
        &[
            "spanner",
            "instances",
            "describe",
            &opts.spanner_instance,
            &proj_flag,
        ],
    )? {
        let sp_cfg = if opts.peer_region.is_some() {
            "nam-eur-asia1".to_string()
        } else {
            format!("regional-{}", opts.region)
        };
        state.resources.spanner_instance_created = true;
        save_state(root, state)?;
        println!(
            "==> Creating Cloud Spanner instance {} ({sp_cfg}, 100 PUs)...",
            opts.spanner_instance
        );
        run_gcloud(
            gcloud,
            [
                "spanner",
                "instances",
                "create",
                &opts.spanner_instance,
                &proj_flag,
                &format!("--config={sp_cfg}"),
                "--description=jj-commit-cloud metadata",
                "--processing-units=100",
            ],
        )?;
    }

    state.resources.spanner_database = Some(opts.spanner_database.clone());
    save_state(root, state)?;
    if !gcloud_resource_exists(
        gcloud,
        &[
            "spanner",
            "databases",
            "describe",
            &opts.spanner_database,
            &format!("--instance={}", opts.spanner_instance),
            &proj_flag,
        ],
    )? {
        let ddl_file = root.join("server/sql/spanner_schema.sql");
        state.resources.spanner_database_created = true;
        save_state(root, state)?;
        println!(
            "==> Creating Cloud Spanner database {} from {}...",
            opts.spanner_database,
            ddl_file.display()
        );
        let inst_flag = format!("--instance={}", opts.spanner_instance);
        run_gcloud(
            gcloud,
            [
                OsStr::new("spanner"),
                OsStr::new("databases"),
                OsStr::new("create"),
                OsStr::new(&opts.spanner_database),
                OsStr::new(&inst_flag),
                OsStr::new(&proj_flag),
                OsStr::new("--ddl-file"),
                ddl_file.as_os_str(),
            ],
        )?;
    }

    let sa_name = "jj-cc-server-sa";
    let sa_email = format!("{sa_name}@{}.iam.gserviceaccount.com", opts.project_id);
    if !gcloud_resource_exists(
        gcloud,
        &["iam", "service-accounts", "describe", &sa_email, &proj_flag],
    )? {
        run_gcloud(
            gcloud,
            [
                "iam",
                "service-accounts",
                "create",
                sa_name,
                &proj_flag,
                "--display-name=jj-cc-server runtime",
            ],
        )?;
    }
    run_gcloud(
        gcloud,
        [
            "spanner",
            "databases",
            "add-iam-policy-binding",
            &opts.spanner_database,
            &format!("--instance={}", opts.spanner_instance),
            &proj_flag,
            &format!("--member=serviceAccount:{sa_email}"),
            "--role=roles/spanner.databaseUser",
        ],
    )?;

    ensure_gcs_bucket(
        gcloud,
        root,
        state,
        &opts.project_id,
        primary_bucket,
        &opts.region,
        &sa_email,
    )?;
    if let (Some(b), Some(peer_reg)) = (&peer_bucket, &opts.peer_region) {
        ensure_gcs_bucket(
            gcloud,
            root,
            state,
            &opts.project_id,
            b,
            peer_reg,
            &sa_email,
        )?;
    }

    let image_uri = if let Some(img) = &opts.image_override {
        img.clone()
    } else {
        let ar_repo = "jj-commit-cloud";
        let loc_flag = format!("--location={}", opts.region);
        if !gcloud_resource_exists(
            gcloud,
            &[
                "artifacts",
                "repositories",
                "describe",
                ar_repo,
                &loc_flag,
                &proj_flag,
            ],
        )? {
            run_gcloud(
                gcloud,
                [
                    "artifacts",
                    "repositories",
                    "create",
                    ar_repo,
                    &proj_flag,
                    "--repository-format=docker",
                    &loc_flag,
                ],
            )?;
        }
        run_gcloud(
            gcloud,
            [
                "artifacts",
                "repositories",
                "add-iam-policy-binding",
                ar_repo,
                &loc_flag,
                &proj_flag,
                &format!("--member=serviceAccount:{sa_email}"),
                "--role=roles/artifactregistry.reader",
            ],
        )?;
        let uri = format!(
            "{}-docker.pkg.dev/{}/{ar_repo}/jj-cc-server:latest",
            opts.region, opts.project_id
        );
        println!("==> Building server image via Cloud Build ({uri})...");
        let status = Command::new(gcloud)
            .current_dir(root)
            .args(["builds", "submit", &proj_flag, &format!("--tag={uri}"), "."])
            .status()?;
        if !status.success() {
            return Err("`gcloud builds submit` failed".into());
        }
        uri
    };

    Ok(SharedGcpInfra {
        sa_email,
        image_uri,
    })
}

pub(super) fn ensure_cloud_run_service_absent(
    gcloud: &Path,
    project_id: &str,
    region: &str,
    service: &str,
) -> Result<(), BoxError> {
    let proj_flag = format!("--project={project_id}");
    let reg_flag = format!("--region={region}");
    ensure_absent(
        gcloud,
        &[
            "run", "services", "describe", service, &reg_flag, &proj_flag,
        ],
        &format!("Cloud Run service {service} in {region}"),
        &format!("gcloud run services delete {service} {reg_flag} {proj_flag}"),
    )
}

pub(super) fn ensure_gce_vm_absent(
    gcloud: &Path,
    project_id: &str,
    zone: &str,
    vm_name: &str,
) -> Result<(), BoxError> {
    let proj_flag = format!("--project={project_id}");
    let zone_flag = format!("--zone={zone}");
    ensure_absent(
        gcloud,
        &[
            "compute",
            "instances",
            "describe",
            vm_name,
            &zone_flag,
            &proj_flag,
        ],
        &format!("VM {vm_name}"),
        &format!("gcloud compute instances delete {vm_name} {zone_flag} {proj_flag}"),
    )
}

/// Creates `bucket` unless it exists, records it (and whether this environment created it, before
/// invoking `create`) in `state`, then grants the runtime service account object access.
pub(super) fn ensure_gcs_bucket(
    gcloud: &Path,
    root: &Path,
    state: &mut EnvState,
    project_id: &str,
    bucket: &str,
    location: &str,
    sa_email: &str,
) -> Result<(), BoxError> {
    let gs_uri = format!("gs://{bucket}");
    let proj_flag = format!("--project={project_id}");
    if !state.resources.gcs_buckets.iter().any(|b| b == bucket) {
        state.resources.gcs_buckets.push(bucket.to_string());
    }
    save_state(root, state)?;
    if !gcloud_resource_exists(
        gcloud,
        &["storage", "buckets", "describe", &gs_uri, &proj_flag],
    )? {
        if !state
            .resources
            .gcs_buckets_created
            .iter()
            .any(|b| b == bucket)
        {
            state.resources.gcs_buckets_created.push(bucket.to_string());
        }
        save_state(root, state)?;
        println!("==> Creating Cloud Storage bucket {gs_uri} in {location}...");
        run_gcloud(
            gcloud,
            [
                "storage",
                "buckets",
                "create",
                &gs_uri,
                &proj_flag,
                &format!("--location={location}"),
                "--uniform-bucket-level-access",
                "--public-access-prevention",
                "--soft-delete-duration=7d",
            ],
        )?;
    }
    run_gcloud(
        gcloud,
        [
            "storage",
            "buckets",
            "add-iam-policy-binding",
            &gs_uri,
            &format!("--member=serviceAccount:{sa_email}"),
            "--role=roles/storage.objectUser",
        ],
    )?;
    Ok(())
}

/// Returns true when gcloud's error output says the resource does not exist.
pub(super) fn is_not_found(stderr: &str) -> bool {
    let stderr = stderr.to_ascii_lowercase();
    [
        "not found",
        "not_found",
        "could not find",
        "cannot find",
        "does not exist",
        "matched no",
        "unknown service account",
    ]
    .iter()
    .any(|needle| stderr.contains(needle))
}

/// Runs a gcloud delete command. A resource that is already gone counts as deleted, so a
/// retried `down` converges after a partial failure.
pub(super) fn delete_gcloud_resource(gcloud: &Path, args: &[&str]) -> Result<(), String> {
    let out = Command::new(gcloud)
        .args(args)
        .output()
        .map_err(|e| e.to_string())?;
    let stderr = String::from_utf8_lossy(&out.stderr);
    if out.status.success() || is_not_found(&stderr) {
        Ok(())
    } else {
        Err(stderr.trim().to_string())
    }
}

pub(super) fn run_kubectl<I, S>(kubectl: &Path, args: I) -> Result<(), String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let out = Command::new(kubectl)
        .args(args)
        .output()
        .map_err(|e| e.to_string())?;
    if out.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&out.stderr).trim().to_string())
    }
}
