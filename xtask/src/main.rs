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

//! Cross-platform workflow driver (`cargo xtask`) for building, deploying, smoke-testing,
//! and tearing down `jj-commit-cloud` across local containers, local Kubernetes, and GCP.
//!
//! # Authority and Crash-Safety Ordering
//! `xtask up` writes `.jj-cc/state.json` (or `.jj-cc/<env>.json`) *before* launching
//! containers, `kind` clusters, background processes, or Google Cloud services, and
//! appends managed resource identifiers incrementally. `down` deletes only cloud storage that
//! `up` created, retains a storage-only record whenever created cloud storage remains (on
//! non-purge `down`, or on `down --purge` when a created Spanner instance still holds another
//! environment's database), and refuses `--replace` or cross-mode/cross-region redeploys that
//! would drop it.
//!
//! # Concurrency and Merge Invariants
//! Separate named environments (`--env <name>`, excluding reserved `state`) isolate state files,
//! container prefixes, and default GCP resource names so a developer can keep a GCP deployment
//! active alongside a local stack; pass `--port` to run two container, `kind`, or native stacks
//! at once.
//!
//! # Cross-Process and RPC Boundaries
//! Readiness checks probe `tonic_health` `Health::Check` over HTTP/2 gRPC (attaching TLS
//! roots and bearer or `gcloud` IAM identity tokens) before reporting `SERVING`.

mod bench;
mod gcp;
mod k8s;
mod local;
mod smoke;
mod state;

use std::ffi::OsString;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::parser::ValueSource;
use clap::{ArgMatches, CommandFactory, FromArgMatches, Parser, Subcommand, ValueEnum};

use bench::{BenchOptions, run_bench};
use gcp::{
    GcpDeployOptions, GcpMode, GcpTools, describe_created_cloud_storage, down_gcp,
    ensure_gcp_ownership_preserved, env_resource_name, preflight_gcp_tools, up_gcp,
    validate_gcp_options,
};
use k8s::{
    K8sMode, K8sProvider, K8sUpPlan, down_k8s, kind_cluster_exists, prepare_k8s_up,
    up_k8s_with_plan,
};
use local::{
    ContainerRuntimeChoice, LocalDriver, LocalMode, LocalUpPlan, build_and_export_binaries,
    check_grpc_health, detect_container_runtime, down_local, prepare_local_up, up_local_with_plan,
};
use smoke::{SmokeOptions, normalize_optional_url, run_smoke};
use state::{BoxError, EnvState, load_state, remove_state, state_file_path, workspace_root};

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum DeployMode {
    /// Ephemeral single-node server in memory
    Memory,
    /// Durable single-node server backed by SQLite + local content-addressed blobs
    Sqlite,
    /// Local PostgreSQL 17 + MinIO (S3-compatible) stack
    Postgres,
    /// Local Cloud Spanner Emulator + Cloud Storage Emulator (`fake-gcs-server`)
    Spanner,
    /// Two local regional servers (`:8081` US, `:8082` EU) sharing Spanner with isolated Cloud
    /// Storage buckets
    Multiregion,
    /// Local Kubernetes stack (alias for `k8s-spanner`) via `kind` or `podman kube play`
    K8s,
    /// Local Kubernetes in-memory server pod
    K8sMemory,
    /// Local Kubernetes SQLite server pod with PersistentVolumeClaim
    K8sSqlite,
    /// Local Kubernetes PostgreSQL 17 + MinIO + server pod
    K8sPostgres,
    /// Local Kubernetes Spanner + Cloud Storage emulator + server pod
    K8sSpanner,
    /// Local Kubernetes two-region (`:8081` US, `:8082` EU) pod
    K8sMultiregion,
    /// Google Cloud Run serverless HTTP/2 deployment backed by Cloud Spanner + Cloud Storage
    CloudRun,
    /// Google Compute Engine Container-Optimized OS server VM backed by Cloud Spanner + Cloud
    /// Storage
    Gce,
    /// Google Kubernetes Engine Autopilot deployment backed by Cloud Spanner + Cloud Storage
    Gke,
}

impl DeployMode {
    fn is_gcp(self) -> bool {
        matches!(self, Self::CloudRun | Self::Gce | Self::Gke)
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Sqlite => "sqlite",
            Self::Postgres => "postgres",
            Self::Spanner => "spanner",
            Self::Multiregion => "multiregion",
            Self::K8s => "k8s",
            Self::K8sMemory => "k8s-memory",
            Self::K8sSqlite => "k8s-sqlite",
            Self::K8sPostgres => "k8s-postgres",
            Self::K8sSpanner => "k8s-spanner",
            Self::K8sMultiregion => "k8s-multiregion",
            Self::CloudRun => "cloud-run",
            Self::Gce => "gce",
            Self::Gke => "gke",
        }
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "cargo xtask",
    about = "Build, deploy, and smoke-test jj-commit-cloud across local, Kubernetes, and GCP modes"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Build server and custom jj CLI (natively or inside multi-stage container)
    Build {
        /// Build inside the multi-stage Dockerfile via docker/podman
        #[arg(long)]
        container: bool,
        /// Export compiled binaries into `./bin/`
        #[arg(long)]
        export_bin: bool,
        /// Container runtime when `--container` is set
        #[arg(long, value_enum, default_value_t = ContainerRuntimeChoice::Auto)]
        runtime: ContainerRuntimeChoice,
    },

    /// Start a local, Kubernetes, or Google Cloud Commit Cloud environment
    Up {
        /// Target deployment topology
        #[arg(value_enum, default_value_t = DeployMode::Sqlite)]
        mode: DeployMode,

        /// Environment state slot name (stored at `.jj-cc/state.json` for `default`)
        #[arg(long, default_value = "default", value_parser = state::validate_env_name)]
        env: String,

        /// Tear down any existing environment in the same `--env` slot before starting
        #[arg(long)]
        replace: bool,

        /// Force rebuilding the local container image before starting
        #[arg(long)]
        rebuild: bool,

        /// Driver for local non-Kubernetes modes (`auto` prefers docker/podman containers)
        #[arg(long, value_enum, default_value_t = LocalDriver::Auto)]
        driver: LocalDriver,

        /// Container runtime preference
        #[arg(long, value_enum, default_value_t = ContainerRuntimeChoice::Auto)]
        runtime: ContainerRuntimeChoice,

        /// Local Kubernetes provider (`auto` prefers `kind`+`kubectl`, then `podman kube play`)
        #[arg(long, value_enum, default_value_t = K8sProvider::Auto)]
        k8s_provider: K8sProvider,

        /// Primary localhost port for local modes, GCE IAP tunnels, and GKE `kubectl port-forward`
        /// (`8081` automatically in multi-region local/k8s or GCE IAP setups)
        #[arg(long, default_value_t = 8080, value_parser = clap::value_parser!(u16).range(1..))]
        port: u16,

        /// Secondary peer localhost port for multi-region local/k8s modes and GCE IAP tunnels
        #[arg(long, default_value_t = 8082, value_parser = clap::value_parser!(u16).range(1..))]
        peer_port: u16,

        /// Google Cloud project ID (required for `cloud-run`, `gce`, and `gke`)
        #[arg(long, env = "PROJECT_ID")]
        project: Option<String>,

        /// Primary Google Cloud region
        #[arg(long, default_value = "us-central1")]
        region: String,

        /// Optional secondary Google Cloud region for multi-region cloud deployments
        #[arg(long)]
        peer_region: Option<String>,

        /// Cloud Spanner instance ID for GCP modes (shared by environments; `down --purge`
        /// deletes it only if this environment created it and no databases remain)
        #[arg(long, default_value = "jj-cc-dev")]
        spanner_instance: String,

        /// Cloud Spanner database ID for GCP modes [default: `commit-cloud`, or
        /// `commit-cloud-<env>` for named environments]
        #[arg(long)]
        spanner_database: Option<String>,

        /// GKE Autopilot cluster name when `mode = gke` [default: `jj-cc-cluster`, or
        /// `jj-cc-cluster-<env>` for named environments]
        #[arg(long)]
        gke_cluster: Option<String>,

        /// Optional pre-built container image URI for GCP modes (skips `gcloud builds submit`)
        #[arg(long)]
        image: Option<String>,

        /// Assign public external IPs to GCE VMs instead of tunneling over Identity-Aware Proxy
        /// (IAP); requires `--allow-cidr`, and the server then requires a generated bearer token
        #[arg(long, requires = "allow_cidr")]
        gce_public_ip: bool,

        /// Source range allowed to reach public GCE VMs (repeatable or comma-separated, e.g.
        /// `203.0.113.0/24`)
        #[arg(long, requires = "gce_public_ip", value_delimiter = ',', value_parser = gcp::parse_allow_cidr)]
        allow_cidr: Vec<String>,
    },

    /// Inspect the active environment and probe live gRPC server health
    Status {
        /// Environment state slot name
        #[arg(long, default_value = "default", value_parser = state::validate_env_name)]
        env: String,
    },

    /// Run an end-to-end repo init, version commit, clone, and historical checkout test
    Smoke {
        /// Environment state slot name
        #[arg(long, default_value = "default", value_parser = state::validate_env_name)]
        env: String,

        /// Optional explicit primary server URL (overrides `.jj-cc/state.json`)
        #[arg(long, env = "SMOKE_SERVER_URL")]
        server: Option<String>,

        /// Optional explicit peer replica server URL for cross-region checkout testing
        #[arg(long, env = "SMOKE_PEER_SERVER_URL")]
        peer_server: Option<String>,

        /// Optional path to compiled custom `jj` binary
        #[arg(long)]
        jj_bin: Option<PathBuf>,

        /// Keep temporary smoke workspaces on disk after completion
        #[arg(long)]
        keep: bool,
    },

    /// Run a concurrent multi-agent load and op-head reconciliation benchmark
    Bench {
        /// Environment state slot name
        #[arg(long, default_value = "default", value_parser = state::validate_env_name)]
        env: String,

        /// Optional explicit target server URL (overrides `.jj-cc/state.json`)
        #[arg(long, env = "BENCH_SERVER_URL")]
        server: Option<String>,

        /// Spawn a self-contained ephemeral SQLite server instead of requiring `xtask up`
        #[arg(long)]
        ephemeral: bool,

        /// Number of concurrent simulated coding agents
        #[arg(long, default_value_t = 12)]
        agents: usize,

        /// Number of sequential commits authored per agent
        #[arg(long, default_value_t = 3)]
        commits_per_agent: usize,

        /// Give each agent its own `repo_id` sharing one `project_id` (default shares one
        /// `repo_id`)
        #[arg(long)]
        isolated_repos: bool,

        /// Route benchmark traffic through `jj-cc-daemon` and measure warm cache hit ratio
        #[arg(long)]
        daemon: bool,

        /// Build `jj`, `jj-cc-daemon`, and ephemeral `jj-cc-server` with `--release`
        #[arg(long)]
        release: bool,

        /// Optional path to compiled custom `jj` binary
        #[arg(long)]
        jj_bin: Option<PathBuf>,
    },

    /// Tear down the active environment recorded in `.jj-cc/state.json`
    Down {
        /// Optional explicit mode fallback if `.jj-cc/state.json` was manually removed
        #[arg(value_enum)]
        mode: Option<DeployMode>,

        /// Environment state slot name
        #[arg(long, default_value = "default", value_parser = state::validate_env_name)]
        env: String,

        /// Also delete data volumes, plus the Spanner database and instance, Cloud Storage
        /// buckets, and GKE cluster that this environment's `up` created
        #[arg(long)]
        purge: bool,
    },
}

const GCP_ONLY_UP_FLAGS: &[(&str, &str)] = &[
    ("project", "--project"),
    ("region", "--region"),
    ("peer_region", "--peer-region"),
    ("spanner_instance", "--spanner-instance"),
    ("spanner_database", "--spanner-database"),
    ("gke_cluster", "--gke-cluster"),
    ("image", "--image"),
    ("gce_public_ip", "--gce-public-ip"),
    ("allow_cidr", "--allow-cidr"),
];

fn validate_up_flag_applicability(mode: DeployMode, matches: &ArgMatches) -> Result<(), String> {
    let Some(("up", up_matches)) = matches.subcommand() else {
        return Ok(());
    };
    if !mode.is_gcp() {
        let invalid: Vec<&str> = GCP_ONLY_UP_FLAGS
            .iter()
            .filter_map(|(id, flag)| {
                (up_matches.value_source(id) == Some(ValueSource::CommandLine)).then_some(*flag)
            })
            .collect();
        if !invalid.is_empty() {
            return Err(format!(
                "GCP-only flag(s) {} do not apply to local/Kubernetes mode `{}`",
                invalid.join(", "),
                mode.as_str()
            ));
        }
    } else if mode != DeployMode::Gke
        && up_matches.value_source("gke_cluster") == Some(ValueSource::CommandLine)
    {
        return Err("`--gke-cluster` only applies to `up gke`".to_string());
    }
    Ok(())
}

fn resolve_bench_server_override(
    ephemeral: bool,
    server: Option<String>,
    server_source: Option<ValueSource>,
) -> Result<Option<String>, String> {
    if ephemeral && server_source == Some(ValueSource::CommandLine) {
        return Err("`--server` cannot be combined with `--ephemeral`".to_string());
    }
    if ephemeral {
        return Ok(None);
    }
    Ok(normalize_optional_url(server.as_deref()))
}

fn parse_cli_from<I, T>(args: I) -> Result<Cli, BoxError>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let matches = Cli::command().try_get_matches_from(args)?;
    let mut cli = Cli::from_arg_matches(&matches)?;
    match &mut cli.command {
        Commands::Up { mode, .. } => {
            validate_up_flag_applicability(*mode, &matches)?;
        }
        Commands::Smoke {
            server,
            peer_server,
            ..
        } => {
            *server = normalize_optional_url(server.as_deref());
            *peer_server = normalize_optional_url(peer_server.as_deref());
        }
        Commands::Bench {
            server, ephemeral, ..
        } => {
            let server_source = matches
                .subcommand()
                .and_then(|(name, sub)| (name == "bench").then(|| sub.value_source("server")))
                .flatten();
            *server = resolve_bench_server_override(*ephemeral, server.take(), server_source)?;
        }
        _ => {}
    }
    Ok(cli)
}

fn env_flag_suffix(env: &str) -> String {
    if env == "default" {
        String::new()
    } else {
        format!(" --env {env}")
    }
}

fn format_next_steps(env: &str) -> String {
    let suffix = env_flag_suffix(env);
    format!("`cargo xtask smoke{suffix}` or `cargo xtask down{suffix}`")
}

#[derive(Debug, PartialEq, Eq)]
enum ExistingEnvPlan {
    ReuseStoppedGcpState,
    ClearStoppedState,
    ReplaceActive { carry_forward_gcp: bool },
}

/// Validates transitioning an existing `--env` slot before any compute teardown or state removal so
/// tracked created GCP cloud storage is never dropped or orphaned across `--replace` or redeploys.
fn plan_existing_env_transition(
    existing: &EnvState,
    env: &str,
    mode: DeployMode,
    replace: bool,
    gcp_target: Option<(GcpMode, &GcpDeployOptions)>,
) -> Result<ExistingEnvPlan, BoxError> {
    let suffix = env_flag_suffix(env);
    let has_created_cloud_storage = existing.resources.has_created_cloud_storage();

    if !mode.is_gcp() && has_created_cloud_storage {
        return Err(format!(
            "environment '{env}' still tracks created GCP cloud storage from mode '{}' ({}); cannot switch this slot to non-GCP mode `{}` without dropping those resources. Run `cargo xtask down{suffix} --purge` first or choose a different `--env` slot",
            existing.mode,
            describe_created_cloud_storage(&existing.resources),
            mode.as_str(),
        )
        .into());
    }

    if existing.compute_stopped {
        if mode.is_gcp() && existing.runtime == "gcloud" {
            if let Some((gcp_mode, opts)) = gcp_target {
                ensure_gcp_ownership_preserved(existing, env, gcp_mode, opts)?;
            }
            return Ok(ExistingEnvPlan::ReuseStoppedGcpState);
        }
        if has_created_cloud_storage {
            return Err(format!(
                "environment '{env}' still tracks created GCP cloud storage from mode '{}' ({}); run `cargo xtask down{suffix} --purge` first or choose a different `--env` slot",
                existing.mode,
                describe_created_cloud_storage(&existing.resources),
            )
            .into());
        }
        if replace {
            return Ok(ExistingEnvPlan::ClearStoppedState);
        }
        return Err(format!(
            "environment '{env}' still records stopped state from mode '{}'; run `cargo xtask down{suffix} --purge` first or pass `--replace`",
            existing.mode
        )
        .into());
    }

    if !replace {
        return Err(format!(
            "environment '{}' is already active (mode: {}, primary: {}); run `cargo xtask down{suffix}` first or pass `--replace`",
            env, existing.mode, existing.endpoints.primary
        )
        .into());
    }

    if mode.is_gcp() && existing.runtime == "gcloud" {
        if let Some((gcp_mode, opts)) = gcp_target {
            ensure_gcp_ownership_preserved(existing, env, gcp_mode, opts)?;
        }
        return Ok(ExistingEnvPlan::ReplaceActive {
            carry_forward_gcp: true,
        });
    }
    if has_created_cloud_storage {
        return Err(format!(
            "environment '{env}' still tracks created GCP cloud storage from mode '{}' ({}); run `cargo xtask down{suffix} --purge` first or choose a different `--env` slot",
            existing.mode,
            describe_created_cloud_storage(&existing.resources),
        )
        .into());
    }
    Ok(ExistingEnvPlan::ReplaceActive {
        carry_forward_gcp: false,
    })
}

fn finalize_replaced_env_state(
    root: &std::path::Path,
    env: &str,
    carry_forward_gcp: bool,
    existing: EnvState,
) -> Result<Option<EnvState>, BoxError> {
    if carry_forward_gcp && existing.compute_stopped {
        Ok(Some(existing))
    } else {
        remove_state(root, env)?;
        Ok(None)
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = match parse_cli_from(std::env::args_os()) {
        Ok(cli) => cli,
        Err(e) => {
            if let Some(clap_err) = e.downcast_ref::<clap::Error>() {
                clap_err.exit();
            }
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

enum PreparedUpTarget {
    Local(LocalUpPlan),
    K8s(K8sUpPlan),
    Gcp {
        mode: GcpMode,
        opts: GcpDeployOptions,
        tools: GcpTools,
    },
}

#[allow(clippy::too_many_arguments)]
fn build_gcp_deploy_options(
    env: &str,
    gcp_mode: GcpMode,
    project: Option<String>,
    region: String,
    peer_region: Option<String>,
    spanner_instance: String,
    spanner_database: Option<String>,
    gke_cluster: Option<String>,
    image: Option<String>,
    port: u16,
    peer_port: u16,
    gce_public_ip: bool,
    allow_cidr: Vec<String>,
) -> Result<GcpDeployOptions, BoxError> {
    let project_id = project
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or("`--project <PROJECT_ID>` (or `PROJECT_ID` env var) is required for GCP modes")?;
    let opts = GcpDeployOptions {
        project_id,
        region,
        peer_region,
        spanner_instance,
        spanner_database: spanner_database
            .unwrap_or_else(|| env_resource_name("commit-cloud", env)),
        gke_cluster: gke_cluster.unwrap_or_else(|| env_resource_name("jj-cc-cluster", env)),
        image_override: image,
        local_port: port,
        local_peer_port: peer_port,
        gce_public_ip,
        allow_cidrs: allow_cidr,
    };
    validate_gcp_options(env, gcp_mode, &opts)?;
    Ok(opts)
}

async fn run(cli: Cli) -> Result<(), BoxError> {
    let root = workspace_root();

    match cli.command {
        Commands::Build {
            container,
            export_bin,
            runtime,
        } => {
            build_and_export_binaries(&root, container, export_bin, runtime)?;
        }
        Commands::Up {
            mode,
            env,
            replace,
            rebuild,
            driver,
            runtime,
            k8s_provider,
            port,
            peer_port,
            project,
            region,
            peer_region,
            spanner_instance,
            spanner_database,
            gke_cluster,
            image,
            gce_public_ip,
            allow_cidr,
        } => {
            // Validate options, identifiers, manifests, required env vars, and required CLIs
            // upfront so invalid invocations fail before `--replace` tears down a live stack.
            let prepared = match mode {
                DeployMode::Memory => PreparedUpTarget::Local(prepare_local_up(
                    LocalMode::Memory,
                    driver,
                    runtime,
                    port,
                    peer_port,
                )?),
                DeployMode::Sqlite => PreparedUpTarget::Local(prepare_local_up(
                    LocalMode::Sqlite,
                    driver,
                    runtime,
                    port,
                    peer_port,
                )?),
                DeployMode::Postgres => PreparedUpTarget::Local(prepare_local_up(
                    LocalMode::Postgres,
                    driver,
                    runtime,
                    port,
                    peer_port,
                )?),
                DeployMode::Spanner => PreparedUpTarget::Local(prepare_local_up(
                    LocalMode::Spanner,
                    driver,
                    runtime,
                    port,
                    peer_port,
                )?),
                DeployMode::Multiregion => PreparedUpTarget::Local(prepare_local_up(
                    LocalMode::Multiregion,
                    driver,
                    runtime,
                    port,
                    peer_port,
                )?),
                DeployMode::K8s | DeployMode::K8sSpanner => PreparedUpTarget::K8s(prepare_k8s_up(
                    &root,
                    &env,
                    K8sMode::Spanner,
                    k8s_provider,
                    runtime,
                    port,
                    peer_port,
                )?),
                DeployMode::K8sMemory => PreparedUpTarget::K8s(prepare_k8s_up(
                    &root,
                    &env,
                    K8sMode::Memory,
                    k8s_provider,
                    runtime,
                    port,
                    peer_port,
                )?),
                DeployMode::K8sSqlite => PreparedUpTarget::K8s(prepare_k8s_up(
                    &root,
                    &env,
                    K8sMode::Sqlite,
                    k8s_provider,
                    runtime,
                    port,
                    peer_port,
                )?),
                DeployMode::K8sPostgres => PreparedUpTarget::K8s(prepare_k8s_up(
                    &root,
                    &env,
                    K8sMode::Postgres,
                    k8s_provider,
                    runtime,
                    port,
                    peer_port,
                )?),
                DeployMode::K8sMultiregion => PreparedUpTarget::K8s(prepare_k8s_up(
                    &root,
                    &env,
                    K8sMode::Multiregion,
                    k8s_provider,
                    runtime,
                    port,
                    peer_port,
                )?),
                DeployMode::CloudRun | DeployMode::Gce | DeployMode::Gke => {
                    let gcp_mode = match mode {
                        DeployMode::CloudRun => GcpMode::CloudRun,
                        DeployMode::Gce => GcpMode::Gce,
                        DeployMode::Gke => GcpMode::Gke,
                        _ => unreachable!(),
                    };
                    let opts = build_gcp_deploy_options(
                        &env,
                        gcp_mode,
                        project,
                        region,
                        peer_region,
                        spanner_instance,
                        spanner_database,
                        gke_cluster,
                        image,
                        port,
                        peer_port,
                        gce_public_ip,
                        allow_cidr,
                    )?;
                    let tools = preflight_gcp_tools(gcp_mode)?;
                    PreparedUpTarget::Gcp {
                        mode: gcp_mode,
                        opts,
                        tools,
                    }
                }
            };

            let mut previous_gcp_state: Option<EnvState> = None;
            if let Some(mut existing) = load_state(&root, &env)? {
                let gcp_target = match &prepared {
                    PreparedUpTarget::Gcp { mode, opts, .. } => Some((*mode, opts)),
                    _ => None,
                };
                let plan =
                    plan_existing_env_transition(&existing, &env, mode, replace, gcp_target)?;
                match plan {
                    ExistingEnvPlan::ReuseStoppedGcpState => {
                        previous_gcp_state = Some(existing);
                    }
                    ExistingEnvPlan::ClearStoppedState => {
                        println!(
                            "==> Clearing stopped state for '{env}' (mode: {})...",
                            existing.mode
                        );
                        remove_state(&root, &env)?;
                    }
                    ExistingEnvPlan::ReplaceActive { carry_forward_gcp } => {
                        println!(
                            "==> Replacing existing '{}' environment (mode: {})...",
                            env, existing.mode
                        );
                        teardown_state(&root, &mut existing, false)?;
                        previous_gcp_state =
                            finalize_replaced_env_state(&root, &env, carry_forward_gcp, existing)?;
                    }
                }
            }

            let state = match prepared {
                PreparedUpTarget::Local(plan) => {
                    up_local_with_plan(&root, &env, plan, rebuild).await?
                }
                PreparedUpTarget::K8s(plan) => up_k8s_with_plan(&root, &env, plan, rebuild).await?,
                PreparedUpTarget::Gcp { mode, opts, tools } => {
                    up_gcp(
                        &root,
                        &env,
                        mode,
                        &opts,
                        &tools,
                        previous_gcp_state.as_ref(),
                    )
                    .await?
                }
            };

            let state_path = state_file_path(&root, &env);
            println!(
                "==> Environment '{}' is SERVING (mode: {})",
                env, state.mode
            );
            println!("    Primary endpoint: {}", state.endpoints.primary);
            if let Some(peer) = &state.endpoints.peer {
                println!("    Peer endpoint:    {peer}");
            }
            if let Some(metrics) = &state.endpoints.metrics {
                println!("    Metrics endpoint: {metrics} (/healthz, /statz, /metrics)");
            }
            println!("    State recorded:   {}", state_path.display());
            if state.auth_token.is_some() {
                println!(
                    "    Bearer token:     `auth_token` in the state file; export it as JJ_CC_AUTH_TOKEN for jj commands"
                );
            }
            println!("    Next steps:       {}", format_next_steps(&env));
        }
        Commands::Status { env } => {
            let Some(state) = load_state(&root, &env)? else {
                println!("No active environment in slot '{env}'.");
                return Ok(());
            };
            println!("Environment: {}", state.env_name);
            println!("Mode:        {} (runtime: {})", state.mode, state.runtime);
            if state.compute_stopped {
                let suffix = env_flag_suffix(&env);
                println!("Compute:     STOPPED (created cloud storage retained)");
                println!(
                    "Next steps:  run `cargo xtask down{suffix} --purge` to delete created storage, or `cargo xtask up ...` to redeploy"
                );
                return Ok(());
            }
            let token = state.auth_token.as_deref();
            let primary_ok = check_grpc_health(&state.endpoints.primary, state.auth, token)
                .await
                .unwrap_or(false);
            println!(
                "Primary:     {} [{}]",
                state.endpoints.primary,
                if primary_ok { "SERVING" } else { "UNREACHABLE" }
            );
            if let Some(peer) = &state.endpoints.peer {
                let peer_ok = check_grpc_health(peer, state.auth, token)
                    .await
                    .unwrap_or(false);
                println!(
                    "Peer:        {peer} [{}]",
                    if peer_ok { "SERVING" } else { "UNREACHABLE" }
                );
            }
            if let Some(metrics) = &state.endpoints.metrics {
                println!("Metrics:     {metrics} (/healthz, /statz, /metrics)");
            }
        }
        Commands::Smoke {
            env,
            server,
            peer_server,
            jj_bin,
            keep,
        } => {
            run_smoke(&SmokeOptions {
                env_name: env,
                server_override: server,
                peer_server_override: peer_server,
                jj_bin_override: jj_bin,
                keep_workspaces: keep,
            })?;
        }
        Commands::Bench {
            env,
            server,
            ephemeral,
            agents,
            commits_per_agent,
            isolated_repos,
            daemon,
            release,
            jj_bin,
        } => {
            run_bench(&BenchOptions {
                env_name: env,
                server_override: server,
                ephemeral,
                agents,
                commits_per_agent,
                isolated_repos,
                use_daemon: daemon,
                release,
                jj_bin_override: jj_bin,
            })
            .await?;
        }
        Commands::Down { mode, env, purge } => {
            if let Some(mut state) = load_state(&root, &env)? {
                if state.compute_stopped && !purge {
                    let suffix = env_flag_suffix(&env);
                    println!(
                        "Environment '{env}' compute is already stopped; created cloud storage remains recorded in {}. Pass `--purge` (`cargo xtask down{suffix} --purge`) to delete it.",
                        state_file_path(&root, &env).display()
                    );
                    return Ok(());
                }
                println!(
                    "==> Tearing down environment '{}' (mode: {}, runtime: {})...",
                    env, state.mode, state.runtime
                );
                teardown_state(&root, &mut state, purge)?;
                if state.compute_stopped {
                    println!(
                        "==> Environment '{env}' compute stopped (storage-only state retained)."
                    );
                } else {
                    remove_state(&root, &env)?;
                    println!("==> Environment '{env}' stopped.");
                }
            } else if let Some(fallback_mode) = mode {
                println!(
                    "==> No state file for '{env}'; attempting fallback cleanup for {fallback_mode:?}..."
                );
                let mut fallback = synthesize_fallback_state(&root, &env, fallback_mode)?;
                teardown_state(&root, &mut fallback, purge)?;
            } else {
                println!("No active environment recorded for '{env}'.");
            }
        }
    }

    Ok(())
}

fn teardown_state(
    root: &std::path::Path,
    state: &mut EnvState,
    purge: bool,
) -> Result<(), BoxError> {
    match state.runtime.as_str() {
        "gcloud" => down_gcp(root, state, purge),
        "kind" | "podman-kube" => down_k8s(root, state, purge),
        _ => down_local(state, purge),
    }
}

fn detect_kind_cluster_runtime<F>(
    cluster_name: &str,
    primary_rt: &str,
    mut check_exists: F,
) -> Option<String>
where
    F: FnMut(&str, &str) -> Result<bool, BoxError>,
{
    if check_exists(cluster_name, primary_rt).unwrap_or(false) {
        return Some(primary_rt.to_string());
    }
    for alt in ["docker", "podman"] {
        if alt != primary_rt && check_exists(cluster_name, alt).unwrap_or(false) {
            return Some(alt.to_string());
        }
    }
    None
}

fn synthesize_fallback_state(
    root: &std::path::Path,
    env_name: &str,
    mode: DeployMode,
) -> Result<EnvState, BoxError> {
    if mode.is_gcp() {
        return Err(format!(
            "cannot tear down GCP mode `{}` without a state file (expected {}); inspect and delete cloud resources explicitly with `gcloud`",
            mode.as_str(),
            state_file_path(root, env_name).display()
        )
        .into());
    }
    let rt = detect_container_runtime(ContainerRuntimeChoice::Auto)
        .unwrap_or_else(|_| "podman".to_string());
    let prefix = format!("jj-cc-{env_name}");
    let mut st = EnvState::new(
        env_name,
        mode.as_str(),
        &rt,
        "http://127.0.0.1:8080",
        None,
        state::AuthMode::None,
    );
    match mode {
        DeployMode::K8s
        | DeployMode::K8sMemory
        | DeployMode::K8sSqlite
        | DeployMode::K8sPostgres
        | DeployMode::K8sSpanner
        | DeployMode::K8sMultiregion => {
            let kmode = match mode {
                DeployMode::K8sMemory => K8sMode::Memory,
                DeployMode::K8sSqlite => K8sMode::Sqlite,
                DeployMode::K8sPostgres => K8sMode::Postgres,
                DeployMode::K8sMultiregion => K8sMode::Multiregion,
                _ => K8sMode::Spanner,
            };
            st.resources.kube_manifest = Some(kmode.manifest_rel_path().to_string());
            let cluster_name = format!("jj-cc-{env_name}");
            if let Some(found_rt) =
                detect_kind_cluster_runtime(&cluster_name, &rt, kind_cluster_exists)
            {
                st.runtime = "kind".to_string();
                st.resources.container_runtime = Some(found_rt);
                st.resources.kind_cluster = Some(cluster_name.clone());
                st.resources.kind_cluster_created = true;
                st.resources.kube_context = Some(format!("kind-{cluster_name}"));
            } else {
                st.runtime = "podman-kube".to_string();
            }
        }
        _ => {
            st.resources.containers = vec![
                format!("{prefix}-server"),
                format!("{prefix}-server-us"),
                format!("{prefix}-server-eu"),
                format!("{prefix}-minio"),
                format!("{prefix}-postgres"),
                format!("{prefix}-gcs"),
                format!("{prefix}-spanner"),
            ];
            st.resources.network = Some(format!("{prefix}-net"));
            st.resources.volumes = vec![
                format!("{prefix}-sqlite-data"),
                format!("{prefix}-postgres-data"),
                format!("{prefix}-minio-data"),
            ];
        }
    }
    Ok(st)
}

#[cfg(test)]
pub(crate) fn lock_test_proc() -> std::sync::MutexGuard<'static, ()> {
    static TEST_PROC_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    TEST_PROC_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

#[cfg(test)]
mod tests;
