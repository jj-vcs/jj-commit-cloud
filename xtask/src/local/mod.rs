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

//! Local native and container (Docker/Podman) lifecycle drivers for `cargo xtask`.
//!
//! # Authority and Crash-Safety Ordering
//! Persists managed container IDs and process PIDs into `.jj-cc/<env>.json` before startup so
//! `cargo xtask down` tears down partial starts. Native server launches clear ambient
//! `jj-cc-server` environment variables so host settings cannot alter local topologies. Process
//! teardown verifies command-line markers before signaling `SIGTERM` and escalates to `SIGKILL`
//! across the child process group on Unix if the process remains alive.

mod containers;
mod health;
mod native;
mod process;
#[cfg(test)]
mod tests;

use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

pub use health::{
    check_grpc_health, resolve_bearer_token, wait_until_healthy, wait_until_healthy_with_children,
};
pub use native::ensure_loopback_port_free;
pub use process::{WatchedChild, spawn_bg_child, stop_managed_processes};

use containers::{remove_container_resource, up_local_containers};
use native::{resolve_spanner_emulator_endpoints, up_local_native};

use crate::state::{BoxError, EnvState, cargo_target_dir};

/// Ambient `jj-cc-server` configuration environment variables cleared before spawning managed
/// local or ephemeral native server processes so host settings never bleed into test stacks.
pub const SERVER_SANITIZED_ENV_VARS: &[&str] = &[
    "HOST",
    "PORT",
    "STORE_TYPE",
    "BLOB_STORE",
    "BLOB_PATH",
    "SQLITE_PATH",
    "POSTGRES_URL",
    "DATABASE_URL",
    "POSTGRES_MAX_CONNS",
    "POSTGRES_AUTO_MIGRATE",
    "POSTGRES_CA_CERT",
    "GCS_BUCKET",
    "GCS_ENDPOINT",
    "STORAGE_EMULATOR_HOST",
    "GCS_PEER_BUCKETS",
    "GCS_REPLICATION_RETRY_MS",
    "GCS_ASYNC_WRITE_FANOUT",
    "S3_BUCKET",
    "S3_ENDPOINT",
    "S3_REGION",
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "S3_ACCESS_KEY_ID",
    "AWS_ACCESS_KEY_ID",
    "S3_SECRET_ACCESS_KEY",
    "AWS_SECRET_ACCESS_KEY",
    "S3_SESSION_TOKEN",
    "AWS_SESSION_TOKEN",
    "S3_PATH_STYLE",
    "S3_AUTO_CREATE_BUCKET",
    "S3_PEER_BUCKETS",
    "LOCAL_PEER_BLOB_DIRS",
    "BLOB_CACHE_BYTES",
    "BLOB_LOCATION_ID",
    "INLINE_BLOB_THRESHOLD_BYTES",
    "SPANNER_DATABASE",
    "SPANNER_ENDPOINT",
    "SPANNER_EMULATOR",
    "SPANNER_EMULATOR_HOST",
    "SPANNER_EMULATOR_REST_HOST",
    "SPANNER_OMNI",
    "SPANNER_OMNI_ENDPOINT",
    "SPANNER_AUTO_MIGRATE",
    "JJ_CC_SERVER_AUTH_TOKEN",
    "JJ_CC_SERVER_AUTH_TOKEN_FILE",
    "JJ_CC_ALLOW_UNAUTHENTICATED",
    "JJ_CC_AUTH_OIDC_ISSUER",
    "JJ_CC_AUTH_OIDC_AUDIENCE",
    "JJ_CC_AUTH_OIDC_JWKS_URI",
    "JJ_CC_AUTH_OIDC_CA_CERT",
    "JJ_CC_AUTH_ENFORCE_ACTOR_REPO_SUFFIX",
    "JJ_CC_AUTH_K8S_TOKENREVIEW_URL",
    "JJ_CC_AUTH_K8S_TOKENREVIEW_AUD",
    "JJ_CC_AUTH_K8S_CA_CERT",
    "JJ_CC_AUTH_K8S_SA_TOKEN_FILE",
    "METRICS_LISTEN",
];

fn sanitize_server_env(cmd: &mut Command) {
    for var in SERVER_SANITIZED_ENV_VARS {
        cmd.env_remove(var);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum LocalDriver {
    Auto,
    Container,
    Native,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum ContainerRuntimeChoice {
    Auto,
    Docker,
    Podman,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LocalMode {
    Memory,
    Sqlite,
    Postgres,
    Spanner,
    Multiregion,
}

impl LocalMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Sqlite => "sqlite",
            Self::Postgres => "postgres",
            Self::Spanner => "spanner",
            Self::Multiregion => "multiregion",
        }
    }
}

/// Resolves primary and optional peer loopback ports for local modes, rejecting zero ports and
/// duplicate ports upfront in `multiregion`.
fn resolve_local_ports(
    mode: LocalMode,
    port: u16,
    peer_port: u16,
) -> Result<(u16, Option<u16>), BoxError> {
    if port == 0 {
        return Err("`--port` must be between 1 and 65535".into());
    }
    let primary_port = if mode == LocalMode::Multiregion && port == 8080 {
        8081
    } else {
        port
    };
    if mode == LocalMode::Multiregion {
        if peer_port == 0 {
            return Err("`--peer-port` must be between 1 and 65535".into());
        }
        if primary_port == peer_port {
            return Err(format!(
                "multiregion primary port ({primary_port}) and peer port ({peer_port}) must differ; pass distinct `--port` and `--peer-port`"
            )
            .into());
        }
        Ok((primary_port, Some(peer_port)))
    } else {
        Ok((primary_port, None))
    }
}

pub fn detect_container_runtime(choice: ContainerRuntimeChoice) -> Result<String, BoxError> {
    let candidates: &[&str] = match choice {
        ContainerRuntimeChoice::Docker => &["docker"],
        ContainerRuntimeChoice::Podman => &["podman"],
        ContainerRuntimeChoice::Auto => &["docker", "podman"],
    };
    for bin in candidates {
        if Command::new(bin)
            .arg("info")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|s| s.success())
        {
            return Ok((*bin).to_string());
        }
    }
    Err("no running `docker` or `podman` daemon was found on PATH".into())
}

pub fn ensure_server_container_image(
    root: &Path,
    runtime: &str,
    force_rebuild: bool,
) -> Result<(), BoxError> {
    let image_exists = Command::new(runtime)
        .args(["image", "inspect", "localhost/jj-cc-server:local"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success());

    if !image_exists || force_rebuild {
        println!("==> Building container image localhost/jj-cc-server:local via {runtime}...");
        let status = Command::new(runtime)
            .current_dir(root)
            .args([
                "build",
                "--target",
                "server",
                "-t",
                "localhost/jj-cc-server:local",
                "-t",
                "jj-cc-server:local",
                ".",
            ])
            .status()?;
        if !status.success() {
            return Err(format!("`{runtime} build` failed").into());
        }
    }
    Ok(())
}

pub fn build_and_export_binaries(
    root: &Path,
    container: bool,
    export_bin: bool,
    runtime_choice: ContainerRuntimeChoice,
) -> Result<(), BoxError> {
    if container {
        let rt = detect_container_runtime(runtime_choice)?;
        ensure_server_container_image(root, &rt, true)?;
        println!("==> Building container image jj-cc-cli:local via {rt}...");
        let status = Command::new(&rt)
            .current_dir(root)
            .args(["build", "--target", "cli", "-t", "jj-cc-cli:local", "."])
            .status()?;
        if !status.success() {
            return Err(format!("`{rt} build --target cli` failed").into());
        }
        if export_bin {
            let out_dir = root.join("bin");
            fs::create_dir_all(&out_dir)?;
            let mut dest = OsString::from("type=local,dest=");
            dest.push(&out_dir);
            let status = Command::new(&rt)
                .current_dir(root)
                .args(["build", "--target", "bins", "--output"])
                .arg(&dest)
                .arg(".")
                .status()?;
            if !status.success() {
                return Err("container binary export failed".into());
            }
            println!("==> Exported binaries to {}", out_dir.display());
        }
    } else {
        println!("==> Building workspace binaries with Cargo...");
        let status = Command::new("cargo")
            .current_dir(root)
            .args([
                "build",
                "-p",
                "jj-commit-cloud-server",
                "--bin",
                "jj-cc-server",
                "-p",
                "jj-commit-cloud-daemon",
                "--bin",
                "jj-cc-daemon",
                "-p",
                "cli",
                "--bin",
                "jj",
            ])
            .status()?;
        if !status.success() {
            return Err("`cargo build` failed".into());
        }
        if export_bin {
            let out_dir = root.join("bin");
            fs::create_dir_all(&out_dir)?;
            for name in ["jj-cc-server", "jj-cc-daemon", "jj"] {
                let exe = if cfg!(windows) {
                    format!("{name}.exe")
                } else {
                    name.to_string()
                };
                fs::copy(
                    cargo_target_dir(root).join("debug").join(&exe),
                    out_dir.join(&exe),
                )?;
            }
            println!("==> Copied binaries to {}", out_dir.display());
        }
    }
    Ok(())
}

/// Pre-validated local deployment plan resolved before `--replace` teardown or state writes.
#[derive(Debug, PartialEq, Eq)]
pub struct LocalUpPlan {
    pub mode: LocalMode,
    pub primary_port: u16,
    pub peer_port: Option<u16>,
    target: LocalUpTarget,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum LocalUpTarget {
    Container {
        runtime: String,
    },
    Native {
        native_pg_url: Option<String>,
        native_spanner: Option<NativeSpannerSource>,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum NativeSpannerSource {
    External { grpc: String, rest: String },
    LocalDir { gateway: PathBuf, emulator: PathBuf },
}

pub(super) fn resolve_native_pg_url<F>(mut get_env: F) -> Result<String, BoxError>
where
    F: FnMut(&str) -> Option<String>,
{
    get_env("POSTGRES_URL")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            get_env("DATABASE_URL")
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .ok_or_else(|| {
            "native Postgres mode requires POSTGRES_URL (or DATABASE_URL), or a container runtime (`docker`/`podman`)"
                .into()
        })
}

pub(super) fn resolve_native_spanner_source<F>(
    mut get_env: F,
) -> Result<NativeSpannerSource, BoxError>
where
    F: FnMut(&str) -> Option<String>,
{
    if let Some((grpc, rest)) = resolve_spanner_emulator_endpoints(
        get_env("SPANNER_EMULATOR_HOST"),
        get_env("SPANNER_EMULATOR_REST_HOST"),
    ) {
        return Ok(NativeSpannerSource::External { grpc, rest });
    }
    if let Some(emu_dir) = get_env("JJ_CC_SPANNER_EMULATOR_DIR")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    {
        let dir = PathBuf::from(emu_dir);
        let gateway = dir.join("gateway_main");
        let emulator = dir.join("emulator_main");
        if gateway.exists() && emulator.exists() {
            return Ok(NativeSpannerSource::LocalDir { gateway, emulator });
        }
    }
    Err(
        "native Spanner mode requires SPANNER_EMULATOR_HOST, JJ_CC_SPANNER_EMULATOR_DIR, or a container runtime (`docker`/`podman`)"
            .into(),
    )
}

/// Validates local ports, driver/runtime selection, and required native environment variables
/// without binding loopback ports or tearing down any existing environment.
pub fn prepare_local_up(
    mode: LocalMode,
    driver: LocalDriver,
    runtime_choice: ContainerRuntimeChoice,
    port: u16,
    peer_port: u16,
) -> Result<LocalUpPlan, BoxError> {
    prepare_local_up_with(
        mode,
        driver,
        runtime_choice,
        port,
        peer_port,
        detect_container_runtime,
        |key| std::env::var(key).ok(),
    )
}

pub(super) fn prepare_local_up_with<FD, FE>(
    mode: LocalMode,
    driver: LocalDriver,
    runtime_choice: ContainerRuntimeChoice,
    port: u16,
    peer_port: u16,
    detect_runtime: FD,
    mut get_env: FE,
) -> Result<LocalUpPlan, BoxError>
where
    FD: FnOnce(ContainerRuntimeChoice) -> Result<String, BoxError>,
    FE: FnMut(&str) -> Option<String>,
{
    let (primary_port, resolved_peer_port) = resolve_local_ports(mode, port, peer_port)?;

    let container_rt = match driver {
        LocalDriver::Container => Some(detect_runtime(runtime_choice)?),
        LocalDriver::Native => None,
        LocalDriver::Auto => detect_runtime(runtime_choice).ok(),
    };

    let target = if let Some(runtime) = container_rt {
        LocalUpTarget::Container { runtime }
    } else {
        let native_pg_url = if mode == LocalMode::Postgres {
            Some(resolve_native_pg_url(&mut get_env)?)
        } else {
            None
        };
        let native_spanner = if matches!(mode, LocalMode::Spanner | LocalMode::Multiregion) {
            Some(resolve_native_spanner_source(&mut get_env)?)
        } else {
            None
        };
        LocalUpTarget::Native {
            native_pg_url,
            native_spanner,
        }
    };

    Ok(LocalUpPlan {
        mode,
        primary_port,
        peer_port: resolved_peer_port,
        target,
    })
}

pub async fn up_local_with_plan(
    root: &Path,
    env_name: &str,
    plan: LocalUpPlan,
    rebuild_image: bool,
) -> Result<EnvState, BoxError> {
    match plan.target {
        LocalUpTarget::Container { runtime } => {
            up_local_containers(
                root,
                env_name,
                plan.mode,
                &runtime,
                plan.primary_port,
                plan.peer_port,
                rebuild_image,
            )
            .await
        }
        LocalUpTarget::Native {
            native_pg_url,
            native_spanner,
        } => {
            up_local_native(
                root,
                env_name,
                plan.mode,
                plan.primary_port,
                plan.peer_port,
                native_pg_url,
                native_spanner,
            )
            .await
        }
    }
}

pub fn down_local(state: &EnvState, purge: bool) -> Result<(), BoxError> {
    let mut failures = Vec::new();
    if state.runtime == "native" {
        failures.extend(stop_managed_processes(state));
        if purge && failures.is_empty() {
            for d in &state.resources.data_dirs {
                if Path::new(d).exists()
                    && let Err(e) = fs::remove_dir_all(d)
                {
                    failures.push(format!("data dir {d}: {e}"));
                }
            }
        }
    } else {
        let rt = &state.runtime;
        for c in state.resources.containers.iter().rev() {
            if let Err(e) = remove_container_resource(rt, &["rm", "-f", c]) {
                failures.push(format!("container {c}: {e}"));
            }
        }
        if let Some(net) = &state.resources.network {
            let net_args: &[&str] = if rt == "podman" {
                &["network", "rm", "-f", net]
            } else {
                &["network", "rm", net]
            };
            if let Err(e) = remove_container_resource(rt, net_args) {
                failures.push(format!("network {net}: {e}"));
            }
        }
        if purge {
            for v in &state.resources.volumes {
                if let Err(e) = remove_container_resource(rt, &["volume", "rm", "-f", v]) {
                    failures.push(format!("volume {v}: {e}"));
                }
            }
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "could not tear down local resources: {}",
            failures.join("; ")
        )
        .into())
    }
}
