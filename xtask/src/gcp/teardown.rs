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

//! Crash-safe Google Cloud resource teardown for `cargo xtask down`.
//!
//! # Authority and Crash-Safety Ordering
//! Deletes compute resources first and removes each identifier from `.jj-cc/<env>.json` as soon
//! as deletion succeeds so partial failures converge on retry. Shared storage (Spanner instance
//! and database, Cloud Storage buckets, GKE cluster) is removed only with `--purge` and only
//! when this environment created it; whenever created cloud storage remains, a storage-only
//! state record (`compute_stopped = true`) is retained.

use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

use super::infra::{
    delete_gcloud_resource, is_not_found, locate_gcloud, locate_kubectl, run_gcloud, run_kubectl,
};
use super::{GKE_NAMESPACE, gke_kube_context};
use crate::local::stop_managed_processes;
use crate::state::{BoxError, EnvState, save_state, state_file_path};

/// Records a failed deletion in `failures`; returns whether the resource is gone.
fn deleted(failures: &mut Vec<String>, what: &str, result: Result<(), String>) -> bool {
    match result {
        Ok(()) => true,
        Err(e) => {
            failures.push(format!("{what}: {e}"));
            false
        }
    }
}

pub fn down_gcp(root: &Path, state: &mut EnvState, purge: bool) -> Result<(), BoxError> {
    down_gcp_with(root, state, purge, locate_gcloud, locate_kubectl)
}

pub(super) fn down_gcp_with<FG, FK>(
    root: &Path,
    state: &mut EnvState,
    purge: bool,
    resolve_gcloud: FG,
    resolve_kubectl: FK,
) -> Result<(), BoxError>
where
    FG: FnOnce() -> Result<PathBuf, BoxError>,
    FK: FnOnce() -> Result<PathBuf, BoxError>,
{
    let failures = stop_managed_processes(state);
    if state.project_id.is_none() {
        return teardown_result(root, state, purge, failures);
    }
    let gcloud = resolve_gcloud()?;
    down_gcp_resources(&gcloud, resolve_kubectl, root, state, purge, failures)
}

/// Deletes the cloud resources recorded in `state`. Each resource leaves the state file as soon
/// as it is deleted, so after a failure the file lists exactly what still exists. Shared
/// storage (Spanner instance and database, buckets, GKE cluster) is only deleted with `purge`,
/// and only when this environment's `up` created it. Whenever created cloud storage remains after
/// compute teardown (on non-purge `down`, or on `purge` when a created Spanner instance still
/// holds another environment's database), a storage-only state record (`compute_stopped = true`)
/// is retained so a later `down --purge` can still clean it up.
pub(super) fn down_gcp_resources<FK>(
    gcloud: &Path,
    resolve_kubectl: FK,
    root: &Path,
    state: &mut EnvState,
    purge: bool,
    mut failures: Vec<String>,
) -> Result<(), BoxError>
where
    FK: FnOnce() -> Result<PathBuf, BoxError>,
{
    let proj_flag = format!("--project={}", state.project_id.clone().unwrap_or_default());

    for svc in state.resources.cloud_run_services.clone() {
        let Some((region, name)) = svc.split_once('/') else {
            continue;
        };
        let result = delete_gcloud_resource(
            gcloud,
            &[
                "run",
                "services",
                "delete",
                name,
                &format!("--region={region}"),
                &proj_flag,
                "--quiet",
            ],
        );
        if deleted(&mut failures, &format!("Cloud Run service {svc}"), result) {
            state.resources.cloud_run_services.retain(|s| s != &svc);
            save_state(root, state)?;
        }
    }

    for vm in state.resources.gce_instances.clone() {
        let Some((zone, name)) = vm.split_once('/') else {
            continue;
        };
        let result = delete_gcloud_resource(
            gcloud,
            &[
                "compute",
                "instances",
                "delete",
                name,
                &format!("--zone={zone}"),
                &proj_flag,
                "--quiet",
            ],
        );
        if deleted(&mut failures, &format!("VM {vm}"), result) {
            state.resources.gce_instances.retain(|v| v != &vm);
            save_state(root, state)?;
        }
    }
    if let Some(fw) = state.resources.gce_firewall_rule.clone() {
        let result = delete_gcloud_resource(
            gcloud,
            &[
                "compute",
                "firewall-rules",
                "delete",
                &fw,
                &proj_flag,
                "--quiet",
            ],
        );
        if deleted(&mut failures, &format!("firewall rule {fw}"), result) {
            state.resources.gce_firewall_rule = None;
            save_state(root, state)?;
        }
    }

    if purge
        && state.resources.gke_cluster_created
        && let (Some(cluster), Some(region)) = (
            state.resources.gke_cluster.clone(),
            state.resources.gke_region.clone(),
        )
    {
        let result = delete_gcloud_resource(
            gcloud,
            &[
                "container",
                "clusters",
                "delete",
                &cluster,
                &format!("--region={region}"),
                &proj_flag,
                "--quiet",
            ],
        );
        if deleted(&mut failures, &format!("GKE cluster {cluster}"), result) {
            // The cluster's workloads and Secrets went with it.
            if let Some(manifest) = state.resources.kube_manifest.take() {
                let _ = fs::remove_file(manifest);
            }
            state.resources.gke_auth_secret = None;
            state.resources.gke_cluster = None;
            state.resources.gke_region = None;
            state.resources.gke_cluster_created = false;
            state.resources.kube_context = None;
            save_state(root, state)?;
        }
    }
    let derived_context = state.resources.kube_context.clone().or_else(|| {
        match (
            &state.project_id,
            &state.resources.gke_region,
            &state.resources.gke_cluster,
        ) {
            (Some(p), Some(r), Some(c)) => Some(gke_kube_context(p, r, c)),
            _ => None,
        }
    });
    // Resolve `kubectl` lazily at most once, only if Kubernetes objects still need deletion after
    // any created-cluster deletion above. Record resolution errors in `failures` rather than
    // returning early so remaining GCP teardown (buckets, Spanner DB/instance) still executes.
    let kubectl_res: Option<Result<PathBuf, String>> =
        if state.resources.kube_manifest.is_some() || state.resources.gke_auth_secret.is_some() {
            Some(resolve_kubectl().map_err(|e| e.to_string()))
        } else {
            None
        };
    if let Some(manifest) = state.resources.kube_manifest.clone() {
        let result = match (kubectl_res.as_ref(), &derived_context) {
            (Some(Ok(kubectl)), Some(ctx)) => run_kubectl(
                kubectl,
                [
                    OsStr::new("--context"),
                    OsStr::new(ctx),
                    OsStr::new("--namespace"),
                    OsStr::new(GKE_NAMESPACE),
                    OsStr::new("delete"),
                    OsStr::new("--ignore-not-found"),
                    OsStr::new("-f"),
                    OsStr::new(&manifest),
                ],
            ),
            (Some(Err(e)), _) => Err(e.clone()),
            (_, None) => Err(
                "no recorded kube context; not running kubectl against the current one".to_string(),
            ),
            (None, Some(_)) => Err("`kubectl` CLI was not resolved for GKE teardown".to_string()),
        };
        if deleted(
            &mut failures,
            &format!("Kubernetes objects in {manifest}"),
            result,
        ) {
            let _ = fs::remove_file(&manifest);
            state.resources.kube_manifest = None;
            save_state(root, state)?;
        }
    }
    if let Some(secret_name) = state.resources.gke_auth_secret.clone() {
        let result = match (kubectl_res.as_ref(), &derived_context) {
            (Some(Ok(kubectl)), Some(ctx)) => run_kubectl(
                kubectl,
                [
                    "--context",
                    ctx,
                    "--namespace",
                    GKE_NAMESPACE,
                    "delete",
                    "--ignore-not-found",
                    "secret",
                    &secret_name,
                ],
            ),
            (Some(Err(e)), _) => Err(e.clone()),
            (_, None) => Err(
                "no recorded kube context; not running kubectl against the current one".to_string(),
            ),
            (None, Some(_)) => Err("`kubectl` CLI was not resolved for GKE teardown".to_string()),
        };
        if deleted(
            &mut failures,
            &format!("Kubernetes Secret {secret_name}"),
            result,
        ) {
            state.resources.gke_auth_secret = None;
            save_state(root, state)?;
        }
    }

    if purge {
        for bucket in state.resources.gcs_buckets_created.clone() {
            let result = delete_gcloud_resource(
                gcloud,
                &[
                    "storage",
                    "rm",
                    "--recursive",
                    &format!("gs://{bucket}"),
                    &proj_flag,
                    "--quiet",
                ],
            );
            if deleted(&mut failures, &format!("bucket gs://{bucket}"), result) {
                state.resources.gcs_buckets.retain(|b| b != &bucket);
                state.resources.gcs_buckets_created.retain(|b| b != &bucket);
                save_state(root, state)?;
            }
        }
        if state.resources.spanner_database_created
            && let (Some(db), Some(inst)) = (
                state.resources.spanner_database.clone(),
                state.resources.spanner_instance.clone(),
            )
        {
            let result = delete_gcloud_resource(
                gcloud,
                &[
                    "spanner",
                    "databases",
                    "delete",
                    &db,
                    &format!("--instance={inst}"),
                    &proj_flag,
                    "--quiet",
                ],
            );
            if deleted(&mut failures, &format!("Spanner database {db}"), result) {
                state.resources.spanner_database = None;
                state.resources.spanner_database_created = false;
                save_state(root, state)?;
            }
        }
        // Other environments may have added databases to an instance this one created, so
        // delete the instance only once our created database is gone and the instance is empty.
        if state.resources.spanner_instance_created
            && !state.resources.spanner_database_created
            && let Some(inst) = state.resources.spanner_instance.clone()
        {
            let instance_flag = format!("--instance={inst}");
            let listed = run_gcloud(
                gcloud,
                [
                    "spanner",
                    "databases",
                    "list",
                    &instance_flag,
                    &proj_flag,
                    "--format=value(name)",
                ],
            )
            .map_err(|e| e.to_string());
            match listed {
                Ok(remaining) if remaining.trim().is_empty() => {
                    let result = delete_gcloud_resource(
                        gcloud,
                        &[
                            "spanner",
                            "instances",
                            "delete",
                            &inst,
                            &proj_flag,
                            "--quiet",
                        ],
                    );
                    if deleted(&mut failures, &format!("Spanner instance {inst}"), result) {
                        state.resources.spanner_instance = None;
                        state.resources.spanner_instance_created = false;
                        save_state(root, state)?;
                    }
                }
                Ok(remaining) => {
                    let names: Vec<&str> = remaining
                        .lines()
                        .filter_map(|l| l.rsplit('/').next())
                        .collect();
                    println!(
                        "    Kept Spanner instance {inst}: it still holds databases {}.",
                        names.join(", ")
                    );
                }
                Err(e) if is_not_found(&e) => {
                    println!(
                        "    Spanner instance {inst} is already absent; clearing tracked state."
                    );
                    state.resources.spanner_instance = None;
                    state.resources.spanner_instance_created = false;
                    save_state(root, state)?;
                }
                Err(e) => failures.push(format!("Spanner instance {inst}: {e}")),
            }
        }
    }

    report_kept_resources(state, purge, failures.is_empty());
    if failures.is_empty() {
        if state.resources.has_created_cloud_storage() {
            state.compute_stopped = true;
            state.auth_token = None;
            state.endpoints.primary = "stopped".to_string();
            state.endpoints.peer = None;
            state.resources.processes.clear();
            state.resources.pids.clear();
            save_state(root, state)?;
        } else {
            state.compute_stopped = false;
        }
    }
    teardown_result(root, state, purge, failures)
}

/// Prints the shared resources `down` left in place.
fn report_kept_resources(state: &EnvState, purge: bool, teardown_succeeded: bool) {
    let res = &state.resources;
    let mut created = Vec::new();
    let mut preexisting = Vec::new();
    let mut kept = |was_created: bool, what: String| {
        if was_created {
            created.push(what)
        } else {
            preexisting.push(what)
        }
    };
    if let Some(cluster) = &res.gke_cluster {
        kept(res.gke_cluster_created, format!("GKE cluster {cluster}"));
    }
    if let Some(inst) = &res.spanner_instance {
        kept(
            res.spanner_instance_created,
            format!("Spanner instance {inst}"),
        );
    }
    if let Some(db) = &res.spanner_database {
        kept(
            res.spanner_database_created,
            format!("Spanner database {db}"),
        );
    }
    for bucket in &res.gcs_buckets {
        kept(
            res.gcs_buckets_created.contains(bucket),
            format!("bucket gs://{bucket}"),
        );
    }
    if !created.is_empty() {
        let env_suffix = if state.env_name == "default" {
            String::new()
        } else {
            format!(" --env {}", state.env_name)
        };
        if !purge {
            println!(
                "    Kept {} (state retained; run `cargo xtask down{env_suffix} --purge` to delete them too).",
                created.join(", ")
            );
        } else if teardown_succeeded {
            println!(
                "    Retained state for {} because it still contains another environment's database; rerun `cargo xtask down{env_suffix} --purge` once other databases are removed.",
                created.join(", ")
            );
        }
    }
    if !preexisting.is_empty() {
        println!(
            "    Kept {}: they existed before `up`, so `down` never deletes them.",
            preexisting.join(", ")
        );
    }
}

pub(super) fn teardown_result(
    root: &Path,
    state: &EnvState,
    purge: bool,
    failures: Vec<String>,
) -> Result<(), BoxError> {
    if failures.is_empty() {
        return Ok(());
    }
    let env_suffix = if state.env_name == "default" {
        String::new()
    } else {
        format!(" --env {}", state.env_name)
    };
    let purge_suffix = if purge { " --purge" } else { "" };
    Err(format!(
        "teardown left {} resource(s) running; {} still records them, so rerun `cargo xtask down{env_suffix}{purge_suffix}` after fixing the cause:\n  {}",
        failures.len(),
        state_file_path(root, &state.env_name).display(),
        failures.join("\n  ")
    )
    .into())
}
