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

//! Local Kubernetes (`kind` / `podman kube play`) deployment driver for `cargo xtask`.
//!
//! # Authority and Crash-Safety Ordering
//! `kind` deployments refuse to adopt pre-existing clusters and persist `kind_cluster_created`
//! before running `kind create cluster`, so `down` never deletes clusters it did not create.
//! Local `ClusterIP` services are reached via loopback `kubectl port-forward` processes tracked
//! in `.jj-cc/<env>.json`. All `kind` invocations set `KIND_EXPERIMENTAL_PROVIDER=podman` when
//! using Podman.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use crate::gcp::locate_kubectl;
use crate::local::{
    ContainerRuntimeChoice, WatchedChild, detect_container_runtime, ensure_loopback_port_free,
    ensure_server_container_image, spawn_bg_child, stop_managed_processes, wait_until_healthy,
    wait_until_healthy_with_children,
};
use crate::state::{AuthMode, BoxError, EnvState, ManagedProcess, save_state};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum K8sMode {
    Memory,
    Sqlite,
    Postgres,
    Spanner,
    Multiregion,
}

impl K8sMode {
    fn mode_name(self) -> &'static str {
        match self {
            Self::Memory => "k8s-memory",
            Self::Sqlite => "k8s-sqlite",
            Self::Postgres => "k8s-postgres",
            Self::Spanner => "k8s-spanner",
            Self::Multiregion => "k8s-multiregion",
        }
    }

    pub fn manifest_rel_path(self) -> &'static str {
        match self {
            Self::Memory => "deploy/k8s/memory.yaml",
            Self::Sqlite => "deploy/k8s/sqlite.yaml",
            Self::Postgres => "deploy/k8s/postgres.yaml",
            Self::Spanner => "deploy/k8s/spanner.yaml",
            Self::Multiregion => "deploy/k8s/multiregion.yaml",
        }
    }

    fn pod_name(self) -> &'static str {
        match self {
            Self::Memory => "jj-cc-memory",
            Self::Sqlite => "jj-cc-sqlite",
            Self::Postgres => "jj-cc-postgres",
            Self::Spanner => "jj-cc-spanner",
            Self::Multiregion => "jj-cc-multiregion",
        }
    }

    fn service_name(self) -> &'static str {
        match self {
            Self::Memory | Self::Sqlite | Self::Postgres | Self::Spanner => "jj-cc-server",
            Self::Multiregion => "jj-cc-multiregion",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum K8sProvider {
    Auto,
    Kind,
    PodmanKube,
}

fn has_tool(bin: &str) -> bool {
    Command::new(bin)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
        || Command::new(bin)
            .args(["version", "--client"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|s| s.success())
}

/// Builds a `kind` command, setting `KIND_EXPERIMENTAL_PROVIDER=podman` when Podman is selected.
fn kind_command(rt: &str) -> Command {
    kind_command_with_bin(Path::new("kind"), rt)
}

fn kind_command_with_bin(kind_bin: &Path, rt: &str) -> Command {
    let mut cmd = Command::new(kind_bin);
    if rt == "podman" {
        cmd.env("KIND_EXPERIMENTAL_PROVIDER", "podman");
    }
    cmd
}

/// Returns whether a `kind` cluster named `cluster_name` currently exists.
pub fn kind_cluster_exists(cluster_name: &str, rt: &str) -> Result<bool, BoxError> {
    let out = kind_command(rt).args(["get", "clusters"]).output()?;
    if !out.status.success() {
        return Err(format!(
            "`kind get clusters` failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        )
        .into());
    }
    let clusters_out = String::from_utf8_lossy(&out.stdout);
    Ok(clusters_out.lines().any(|l| l.trim() == cluster_name))
}

/// Validates `--env` as an RFC-1123 lowercase `kind` cluster suffix (`jj-cc-<env>` <= 63 chars).
fn validate_kind_cluster_env_name(env_name: &str) -> Result<String, BoxError> {
    let cluster_name = format!("jj-cc-{env_name}");
    let valid = !env_name.is_empty()
        && cluster_name.len() <= 63
        && !env_name.starts_with('-')
        && !env_name.ends_with('-')
        && env_name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    if valid {
        Ok(cluster_name)
    } else {
        Err(format!(
            "`--env {env_name}` produces invalid kind cluster name '{cluster_name}': use lowercase ASCII letters, digits, or hyphens (no leading/trailing hyphen) with total cluster name <= 63 characters"
        )
        .into())
    }
}

/// Validates `--env` and port combinations for local Kubernetes modes.
fn validate_k8s_options(
    provider: K8sProvider,
    env_name: &str,
    mode: K8sMode,
    port: u16,
    peer_port: u16,
) -> Result<(u16, Option<u16>), BoxError> {
    if port == 0 {
        return Err("`--port` must be between 1 and 65535".into());
    }
    if mode == K8sMode::Multiregion && peer_port == 0 {
        return Err("`--peer-port` must be between 1 and 65535".into());
    }
    let primary_port = if mode == K8sMode::Multiregion && port == 8080 {
        8081
    } else {
        port
    };
    if provider == K8sProvider::Kind {
        let _ = validate_kind_cluster_env_name(env_name)?;
    }
    if provider == K8sProvider::PodmanKube {
        if env_name != "default" {
            return Err(format!(
                "`--env {env_name}` is not supported with `podman-kube` because static manifests hardcode pod names and hostPort bindings; use `--env default` or `--k8s-provider kind`"
            )
            .into());
        }
        let expected_primary = if mode == K8sMode::Multiregion {
            8081
        } else {
            8080
        };
        if primary_port != expected_primary || (mode == K8sMode::Multiregion && peer_port != 8082) {
            return Err(
                "`podman-kube` uses fixed hostPort bindings in static manifests (8080, or 8081/8082 for `k8s-multiregion`); omit `--port`/`--peer-port` or use `--k8s-provider kind`"
                    .into(),
            );
        }
    }
    if mode == K8sMode::Multiregion {
        if primary_port == peer_port {
            return Err(format!(
                "k8s-multiregion primary port ({primary_port}) and peer port ({peer_port}) must differ; pass distinct `--port` and `--peer-port`"
            )
            .into());
        }
        Ok((primary_port, Some(peer_port)))
    } else {
        Ok((primary_port, None))
    }
}

/// Pre-validated local Kubernetes deployment plan resolved before `--replace` teardown or state
/// writes.
#[derive(Debug, PartialEq, Eq)]
pub struct K8sUpPlan {
    pub mode: K8sMode,
    pub primary_port: u16,
    pub peer_port: Option<u16>,
    target: K8sUpTarget,
}

#[derive(Debug, PartialEq, Eq)]
enum K8sUpTarget {
    PodmanKube,
    Kind {
        cluster_name: String,
        container_runtime: String,
        kubectl_bin: PathBuf,
    },
}

/// Validates Kubernetes provider, options, manifest path, and required CLIs upfront without
/// checking loopback ports or tearing down any existing environment.
#[allow(clippy::too_many_arguments)]
pub fn prepare_k8s_up(
    root: &Path,
    env_name: &str,
    mode: K8sMode,
    provider: K8sProvider,
    runtime_choice: ContainerRuntimeChoice,
    port: u16,
    peer_port: u16,
) -> Result<K8sUpPlan, BoxError> {
    prepare_k8s_up_with(
        root,
        env_name,
        mode,
        provider,
        runtime_choice,
        port,
        peer_port,
        || has_tool("kind"),
        locate_kubectl,
        || has_tool("podman"),
        detect_container_runtime,
    )
}

#[allow(clippy::too_many_arguments)]
fn prepare_k8s_up_with<FKind, FKube, FPod, FRt>(
    root: &Path,
    env_name: &str,
    mode: K8sMode,
    provider: K8sProvider,
    runtime_choice: ContainerRuntimeChoice,
    port: u16,
    peer_port: u16,
    has_kind: FKind,
    resolve_kubectl: FKube,
    has_podman: FPod,
    detect_runtime: FRt,
) -> Result<K8sUpPlan, BoxError>
where
    FKind: FnOnce() -> bool,
    FKube: FnOnce() -> Result<PathBuf, BoxError>,
    FPod: FnOnce() -> bool,
    FRt: FnOnce(ContainerRuntimeChoice) -> Result<String, BoxError>,
{
    let manifest = root.join(mode.manifest_rel_path());
    match provider {
        K8sProvider::Kind => {
            let (primary_port, peer_port_opt) =
                validate_k8s_options(K8sProvider::Kind, env_name, mode, port, peer_port)?;
            if !manifest.exists() {
                return Err(format!("missing Kubernetes manifest {}", manifest.display()).into());
            }
            if !has_kind() {
                return Err("`--k8s-provider kind` requires `kind` on PATH".into());
            }
            let kubectl_bin = resolve_kubectl()?;
            let container_runtime = detect_runtime(runtime_choice)?;
            let cluster_name = validate_kind_cluster_env_name(env_name)?;
            Ok(K8sUpPlan {
                mode,
                primary_port,
                peer_port: peer_port_opt,
                target: K8sUpTarget::Kind {
                    cluster_name,
                    container_runtime,
                    kubectl_bin,
                },
            })
        }
        K8sProvider::PodmanKube => {
            let (primary_port, peer_port_opt) =
                validate_k8s_options(K8sProvider::PodmanKube, env_name, mode, port, peer_port)?;
            if !manifest.exists() {
                return Err(format!("missing Kubernetes manifest {}", manifest.display()).into());
            }
            if !has_podman() {
                return Err("`--k8s-provider podman-kube` requires `podman` on PATH".into());
            }
            Ok(K8sUpPlan {
                mode,
                primary_port,
                peer_port: peer_port_opt,
                target: K8sUpTarget::PodmanKube,
            })
        }
        K8sProvider::Auto => {
            let auto_kind = if has_kind() {
                resolve_kubectl().ok()
            } else {
                None
            };
            if let Some(kubectl_bin) = auto_kind {
                let (primary_port, peer_port_opt) =
                    validate_k8s_options(K8sProvider::Kind, env_name, mode, port, peer_port)?;
                if !manifest.exists() {
                    return Err(
                        format!("missing Kubernetes manifest {}", manifest.display()).into(),
                    );
                }
                let container_runtime = detect_runtime(runtime_choice)?;
                let cluster_name = validate_kind_cluster_env_name(env_name)?;
                Ok(K8sUpPlan {
                    mode,
                    primary_port,
                    peer_port: peer_port_opt,
                    target: K8sUpTarget::Kind {
                        cluster_name,
                        container_runtime,
                        kubectl_bin,
                    },
                })
            } else if has_podman() {
                let (primary_port, peer_port_opt) =
                    validate_k8s_options(K8sProvider::PodmanKube, env_name, mode, port, peer_port)?;
                if !manifest.exists() {
                    return Err(
                        format!("missing Kubernetes manifest {}", manifest.display()).into(),
                    );
                }
                Ok(K8sUpPlan {
                    mode,
                    primary_port,
                    peer_port: peer_port_opt,
                    target: K8sUpTarget::PodmanKube,
                })
            } else {
                Err(
                    "local Kubernetes mode requires either `kind` + `kubectl` or `podman` (`podman kube play`)"
                        .into(),
                )
            }
        }
    }
}

pub async fn up_k8s_with_plan(
    root: &Path,
    env_name: &str,
    plan: K8sUpPlan,
    rebuild_image: bool,
) -> Result<EnvState, BoxError> {
    let mode = plan.mode;
    let primary_port = plan.primary_port;
    let peer_port_opt = plan.peer_port;
    let primary_url = format!("http://127.0.0.1:{primary_port}");
    let peer_url = peer_port_opt.map(|p| format!("http://127.0.0.1:{p}"));

    match plan.target {
        K8sUpTarget::PodmanKube => {
            ensure_loopback_port_free(primary_port)?;
            if let Some(pp) = peer_port_opt {
                ensure_loopback_port_free(pp)?;
            }
            if mode == K8sMode::Postgres {
                ensure_loopback_port_free(50052)?;
            }
            let mut state = EnvState::new(
                env_name,
                mode.mode_name(),
                "podman-kube",
                &primary_url,
                peer_url,
                AuthMode::None,
            );
            if mode == K8sMode::Postgres {
                state.endpoints.metrics = Some("http://127.0.0.1:50052".to_string());
            }
            state.resources.kube_manifest = Some(mode.manifest_rel_path().to_string());
            save_state(root, &state)?;

            ensure_server_container_image(root, "podman", rebuild_image)?;

            println!(
                "==> Applying Kubernetes manifest {} via `podman kube play`...",
                mode.manifest_rel_path()
            );
            let out = Command::new("podman")
                .current_dir(root)
                .args(["kube", "play", "--replace", mode.manifest_rel_path()])
                .output()?;
            if !out.status.success() {
                return Err(format!(
                    "`podman kube play` failed: {}",
                    String::from_utf8_lossy(&out.stderr)
                )
                .into());
            }

            if mode == K8sMode::Postgres {
                ensure_podman_postgres_pod_ready(mode.pod_name(), Duration::from_secs(45)).await?;
            }

            wait_until_healthy(
                &state.endpoints.primary,
                state.auth,
                None,
                Duration::from_secs(35),
            )
            .await?;
            if let Some(peer) = &state.endpoints.peer {
                wait_until_healthy(peer, state.auth, None, Duration::from_secs(35)).await?;
            }
            Ok(state)
        }
        K8sUpTarget::Kind {
            cluster_name,
            container_runtime: rt,
            kubectl_bin,
        } => {
            ensure_loopback_port_free(primary_port)?;
            if let Some(pp) = peer_port_opt {
                ensure_loopback_port_free(pp)?;
            }

            if kind_cluster_exists(&cluster_name, &rt)? {
                return Err(format!(
                    "kind cluster '{cluster_name}' already exists and is not recorded in this environment's state; delete it with `kind delete cluster --name {cluster_name}` or use another `--env`"
                )
                .into());
            }

            let ctx = format!("kind-{cluster_name}");
            let mut state = EnvState::new(
                env_name,
                mode.mode_name(),
                "kind",
                &primary_url,
                peer_url,
                AuthMode::None,
            );
            state.resources.container_runtime = Some(rt.clone());
            state.resources.kind_cluster = Some(cluster_name.clone());
            state.resources.kind_cluster_created = true;
            state.resources.kube_context = Some(ctx.clone());
            state.resources.kube_manifest = Some(mode.manifest_rel_path().to_string());
            save_state(root, &state)?;

            ensure_server_container_image(root, &rt, rebuild_image)?;

            println!("==> Creating local kind Kubernetes cluster {cluster_name}...");
            let create_status = kind_command(&rt)
                .args(["create", "cluster", "--name", &cluster_name])
                .status()?;
            if !create_status.success() {
                return Err("`kind create cluster` failed".into());
            }

            println!(
                "==> Loading localhost/jj-cc-server:local into kind cluster {cluster_name}..."
            );
            let load_status = kind_command(&rt)
                .args([
                    "load",
                    "docker-image",
                    "localhost/jj-cc-server:local",
                    "--name",
                    &cluster_name,
                ])
                .status()?;
            if !load_status.success() {
                return Err("`kind load docker-image` failed".into());
            }

            let apply_status = Command::new(&kubectl_bin)
                .current_dir(root)
                .args(["--context", &ctx, "apply", "-f", mode.manifest_rel_path()])
                .status()?;
            if !apply_status.success() {
                return Err("`kubectl apply` failed".into());
            }

            let pod_target = format!("pod/{}", mode.pod_name());
            println!("==> Waiting for {pod_target} to become Ready in {cluster_name}...");
            let wait_status = Command::new(&kubectl_bin)
                .args([
                    "--context",
                    &ctx,
                    "wait",
                    "--for=condition=Ready",
                    &pod_target,
                    "--timeout=120s",
                ])
                .status()?;
            if !wait_status.success() {
                return Err(format!("`kubectl wait` failed for {pod_target}").into());
            }

            // Local manifests expose ClusterIP services only so unauthenticated pods never bind
            // on 0.0.0.0; forward loopback ports via a managed `kubectl port-forward`.
            ensure_loopback_port_free(primary_port)?;
            if let Some(pp) = peer_port_opt {
                ensure_loopback_port_free(pp)?;
            }
            let svc_target = format!("service/{}", mode.service_name());
            let pf_marker = format!("port-forward {svc_target}");
            let pf_log = root
                .join(".jj-cc")
                .join(format!("{env_name}-kind-port-forward.log"));
            let forward_metrics = mode == K8sMode::Postgres
                && primary_port == 8080
                && ensure_loopback_port_free(50052).is_ok();
            let pf_args = build_kind_port_forward_args(
                &ctx,
                &svc_target,
                primary_port,
                peer_port_opt,
                forward_metrics,
            );
            if let Some(pp) = peer_port_opt {
                println!(
                    "==> Starting kubectl port-forward 127.0.0.1:{primary_port},:{pp} -> {svc_target}..."
                );
            } else if forward_metrics {
                state.endpoints.metrics = Some("http://127.0.0.1:50052".to_string());
                println!(
                    "==> Starting kubectl port-forward 127.0.0.1:{primary_port},:50052 -> {svc_target}:8080,:50052..."
                );
            } else {
                println!(
                    "==> Starting kubectl port-forward 127.0.0.1:{primary_port} -> {svc_target}:8080..."
                );
            }

            let pf_child = spawn_bg_child(&kubectl_bin, &pf_args, &[], &pf_log)?;
            state
                .resources
                .processes
                .push(ManagedProcess::new(pf_child.id(), &pf_marker));
            save_state(root, &state)?;
            let mut watched = vec![WatchedChild::new(
                "kubectl port-forward (kind)",
                pf_child,
                &pf_log,
            )];

            wait_until_healthy_with_children(
                &state.endpoints.primary,
                state.auth,
                None,
                Duration::from_secs(45),
                &mut watched,
            )
            .await?;
            if let Some(peer) = &state.endpoints.peer {
                wait_until_healthy_with_children(
                    peer,
                    state.auth,
                    None,
                    Duration::from_secs(45),
                    &mut watched,
                )
                .await?;
            }
            Ok(state)
        }
    }
}

fn build_kind_port_forward_args(
    ctx: &str,
    svc_target: &str,
    primary_port: u16,
    peer_port: Option<u16>,
    forward_metrics: bool,
) -> Vec<String> {
    let mut pf_args = vec![
        "--context".to_string(),
        ctx.to_string(),
        "port-forward".to_string(),
        svc_target.to_string(),
    ];
    if let Some(pp) = peer_port {
        pf_args.push(format!("{primary_port}:8081"));
        pf_args.push(format!("{pp}:8082"));
    } else {
        pf_args.push(format!("{primary_port}:8080"));
        if forward_metrics {
            pf_args.push("50052:50052".to_string());
        }
    }
    pf_args.push("--address=127.0.0.1".to_string());
    pf_args
}

async fn ensure_podman_postgres_pod_ready(
    pod_name: &str,
    timeout: Duration,
) -> Result<(), BoxError> {
    ensure_podman_postgres_pod_ready_with_bin(
        Path::new("podman"),
        pod_name,
        timeout,
        Duration::from_millis(250),
    )
    .await
}

fn podman_container_running(podman_bin: &Path, container_name: &str) -> Option<bool> {
    let out = Command::new(podman_bin)
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

/// Waits for `postgres` and `minio` inside a `podman kube play` pod to accept connections
/// before restarting `jj-cc-server` if it exited while Postgres was running `initdb`.
async fn ensure_podman_postgres_pod_ready_with_bin(
    podman_bin: &Path,
    pod_name: &str,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<(), BoxError> {
    let pg_container = format!("{pod_name}-postgres");
    let minio_container = format!("{pod_name}-minio");
    let server_container = format!("{pod_name}-jj-cc-server");
    let deadline = Instant::now() + timeout;

    let mut pg_ready = false;
    while Instant::now() < deadline {
        if podman_container_running(podman_bin, &pg_container) == Some(false) {
            return Err(format!(
                "PostgreSQL container '{pg_container}' exited before becoming ready"
            )
            .into());
        }
        let ok = Command::new(podman_bin)
            .args([
                "exec",
                &pg_container,
                "pg_isready",
                "-h",
                "127.0.0.1",
                "-p",
                "5432",
                "-U",
                "jj",
                "-d",
                "commit_cloud",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|s| s.success());
        if ok {
            pg_ready = true;
            break;
        }
        tokio::time::sleep(poll_interval).await;
    }
    if !pg_ready {
        return Err(format!(
            "timed out waiting for PostgreSQL container '{pg_container}' to become ready"
        )
        .into());
    }

    // MinIO's Chainguard image is distroless, so probe 127.0.0.1:9000 across the shared pod
    // network namespace from the Alpine PostgreSQL container.
    let mut minio_ready = false;
    while Instant::now() < deadline {
        if podman_container_running(podman_bin, &minio_container) == Some(false) {
            return Err(format!(
                "MinIO container '{minio_container}' exited before becoming ready"
            )
            .into());
        }
        let ok = Command::new(podman_bin)
            .args([
                "exec",
                &pg_container,
                "sh",
                "-c",
                "if command -v wget >/dev/null 2>&1; then wget -q -O /dev/null http://127.0.0.1:9000/minio/health/live; else nc -z 127.0.0.1 9000; fi",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|s| s.success());
        if ok {
            minio_ready = true;
            break;
        }
        tokio::time::sleep(poll_interval).await;
    }
    if !minio_ready {
        return Err(format!(
            "timed out waiting for MinIO container '{minio_container}' to become ready"
        )
        .into());
    }

    if podman_container_running(podman_bin, &server_container) == Some(false) {
        let out = Command::new(podman_bin)
            .args(["start", &server_container])
            .output()?;
        if !out.status.success() {
            return Err(format!(
                "`podman start {server_container}` failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )
            .into());
        }
    }
    Ok(())
}

pub fn down_k8s(root: &Path, state: &EnvState, purge: bool) -> Result<(), BoxError> {
    down_k8s_with(root, state, purge, locate_kubectl, Path::new("kind"))
}

fn down_k8s_with<FK>(
    root: &Path,
    state: &EnvState,
    purge: bool,
    resolve_kubectl: FK,
    kind_bin: &Path,
) -> Result<(), BoxError>
where
    FK: FnOnce() -> Result<PathBuf, BoxError>,
{
    let mut failures = stop_managed_processes(state);

    if state.runtime == "podman-kube" {
        if let Some(manifest) = &state.resources.kube_manifest {
            let mut cmd = Command::new("podman");
            cmd.current_dir(root).args(["kube", "down"]);
            if purge {
                cmd.arg("--force");
            }
            cmd.arg(manifest);
            let out = cmd.output()?;
            let stderr = String::from_utf8_lossy(&out.stderr);
            let lower = stderr.to_ascii_lowercase();
            if !out.status.success() && !lower.contains("no such") && !lower.contains("not found") {
                failures.push(format!("`podman kube down` failed: {}", stderr.trim()));
            }
        }
    } else if state.runtime == "kind"
        && let Some(cluster) = &state.resources.kind_cluster
    {
        let cfg_path = root.join(".jj-cc").join(format!("{cluster}-kind.yaml"));
        if cfg_path.exists() {
            let _ = fs::remove_file(&cfg_path);
        }
        if state.resources.kind_cluster_created {
            let rt = state
                .resources
                .container_runtime
                .clone()
                .or_else(|| detect_container_runtime(ContainerRuntimeChoice::Auto).ok())
                .unwrap_or_else(|| "docker".to_string());
            let out = kind_command_with_bin(kind_bin, &rt)
                .args(["delete", "cluster", "--name", cluster])
                .output()?;
            if !out.status.success() {
                failures.push(format!(
                    "`kind delete cluster --name {cluster}` failed: {}",
                    String::from_utf8_lossy(&out.stderr).trim()
                ));
            }
        } else {
            if let Some(manifest) = &state.resources.kube_manifest {
                let default_ctx = format!("kind-{cluster}");
                let ctx = state
                    .resources
                    .kube_context
                    .as_deref()
                    .unwrap_or(&default_ctx);
                match resolve_kubectl() {
                    Ok(kubectl_bin) => {
                        match Command::new(&kubectl_bin)
                            .current_dir(root)
                            .args([
                                "--context",
                                ctx,
                                "delete",
                                "--ignore-not-found",
                                "-f",
                                manifest,
                            ])
                            .output()
                        {
                            Ok(out) if out.status.success() => {}
                            Ok(out) => failures.push(format!(
                                "`kubectl --context {ctx} delete -f {manifest}` failed: {}",
                                String::from_utf8_lossy(&out.stderr).trim()
                            )),
                            Err(e) => failures.push(format!(
                                "`kubectl --context {ctx} delete -f {manifest}` failed: {e}"
                            )),
                        }
                    }
                    Err(e) => failures.push(format!(
                        "`kubectl --context {ctx} delete -f {manifest}` failed: {e}"
                    )),
                }
            }
            println!(
                "    Kept kind cluster '{cluster}': it was not recorded as created by this environment."
            );
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "could not tear down Kubernetes resources: {}",
            failures.join("; ")
        )
        .into())
    }
}

#[cfg(test)]
mod tests;
