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

//! Google Cloud compute topology deployment (Cloud Run, GCE, and GKE) for `cargo xtask up`.
//!
//! # Authority and Crash-Safety Ordering
//! Pre-validates options, verifies storage ownership preservation, and checks loopback tunnel
//! ports before mutating state or creating compute resources. Each compute resource is persisted
//! into `.jj-cc/<env>.json` before its creation command runs.
//!
//! # Cross-Process and RPC Boundaries
//! GKE serves behind a `ClusterIP` service reached via loopback `kubectl port-forward`, and GCE
//! defaults to private IAP TCP tunnels. Both register [`WatchedChild`] handles so health polling
//! fails fast if a tunnel exits early.

use std::ffi::OsString;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use super::infra::{
    SharedGcpInfra, apply_gke_auth_secret, ensure_absent, ensure_cloud_run_service_absent,
    ensure_gce_vm_absent, ensure_gke_workload_absent, gcloud_resource_exists, locate_kubectl,
    parse_gce_nat_ip, provision_shared_infra, run_gcloud, select_gce_zone,
};
use super::{
    GKE_AUTH_SECRET_NAME, GKE_NAMESPACE, GKE_PORT_FORWARD_MARKER, GcpDeployOptions, GcpMode,
    GcpTools, blob_bucket_name, carry_forward_gcp_ownership, ensure_gcp_ownership_preserved,
    env_resource_name, generate_auth_token, gke_kube_context, validate_gcp_options,
};
use crate::local::{
    WatchedChild, ensure_loopback_port_free, spawn_bg_child, wait_until_healthy_with_children,
};
use crate::state::{AuthMode, BoxError, EnvState, ManagedProcess, ensure_private_dir, save_state};

pub async fn up_gcp(
    root: &Path,
    env_name: &str,
    mode: GcpMode,
    opts: &GcpDeployOptions,
    tools: &GcpTools,
    previous_state: Option<&EnvState>,
) -> Result<EnvState, BoxError> {
    validate_gcp_options(env_name, mode, opts)?;
    if let Some(prev) = previous_state {
        ensure_gcp_ownership_preserved(prev, env_name, mode, opts)?;
    }

    let primary_local_port = if opts.peer_region.is_some() && opts.local_port == 8080 {
        8081
    } else {
        opts.local_port
    };
    if mode == GcpMode::Gce && !opts.gce_public_ip {
        ensure_loopback_port_free(primary_local_port)?;
        if opts.peer_region.is_some() {
            ensure_loopback_port_free(opts.local_peer_port)?;
        }
    }
    if mode == GcpMode::Gke {
        ensure_loopback_port_free(opts.local_port)?;
    }

    if opts.gce_public_ip {
        eprintln!(
            "warning: `--gce-public-ip` serves plaintext HTTP/2 without TLS; the bearer token travels unencrypted over the network. Use the default IAP tunnel or put a TLS load balancer in front of GCE on untrusted networks."
        );
    }

    let primary_bucket = blob_bucket_name(&opts.project_id, env_name, &opts.region)?;
    let peer_bucket = match &opts.peer_region {
        Some(peer_reg) => Some(blob_bucket_name(&opts.project_id, env_name, peer_reg)?),
        None => None,
    };
    let gcloud = tools.gcloud.clone();
    let kubectl = if mode == GcpMode::Gke {
        match &tools.kubectl {
            Some(path) => Some(path.clone()),
            None => Some(locate_kubectl()?),
        }
    } else {
        None
    };
    let auth = match mode {
        GcpMode::CloudRun => AuthMode::GcloudIdentityToken,
        GcpMode::Gce | GcpMode::Gke => AuthMode::None,
    };

    let mut state = EnvState::new(env_name, mode.as_str(), "gcloud", "pending", None, auth);
    state.project_id = Some(opts.project_id.clone());
    carry_forward_gcp_ownership(
        previous_state,
        &mut state,
        opts,
        &primary_bucket,
        peer_bucket.as_deref(),
        mode,
    );
    save_state(root, &state)?;

    let proj_flag = format!("--project={}", opts.project_id);
    let SharedGcpInfra {
        sa_email,
        image_uri,
    } = provision_shared_infra(
        &gcloud,
        root,
        mode,
        opts,
        &primary_bucket,
        peer_bucket.as_deref(),
        &mut state,
    )?;

    let spanner_db_uri = format!(
        "projects/{}/instances/{}/databases/{}",
        opts.project_id, opts.spanner_instance, opts.spanner_database
    );
    let primary_gcs_uri = format!("gs://{primary_bucket}");
    let peer_gcs_uri = peer_bucket.as_ref().map(|b| format!("gs://{b}"));

    let mut watched_children: Vec<WatchedChild> = Vec::new();
    match mode {
        GcpMode::CloudRun => {
            let service = env_resource_name("jj-cc-server", env_name);
            ensure_cloud_run_service_absent(&gcloud, &opts.project_id, &opts.region, &service)?;
            state
                .resources
                .cloud_run_services
                .push(format!("{}/{service}", opts.region));
            save_state(root, &state)?;
            let url_primary = deploy_cloud_run_region(
                &gcloud,
                &opts.project_id,
                &opts.region,
                &service,
                &image_uri,
                &sa_email,
                &spanner_db_uri,
                &primary_gcs_uri,
                peer_gcs_uri.as_deref(),
            )?;
            state.endpoints.primary = url_primary;

            if let (Some(peer_reg), Some(peer_gcs)) = (&opts.peer_region, &peer_gcs_uri) {
                ensure_cloud_run_service_absent(&gcloud, &opts.project_id, peer_reg, &service)?;
                state
                    .resources
                    .cloud_run_services
                    .push(format!("{peer_reg}/{service}"));
                save_state(root, &state)?;
                let url_peer = deploy_cloud_run_region(
                    &gcloud,
                    &opts.project_id,
                    peer_reg,
                    &service,
                    &image_uri,
                    &sa_email,
                    &spanner_db_uri,
                    peer_gcs,
                    Some(&primary_gcs_uri),
                )?;
                state.endpoints.peer = Some(url_peer);
            }
            save_state(root, &state)?;
        }
        GcpMode::Gce => {
            let fw_name = format!("jj-cc-{env_name}-allow-grpc");
            let vm_tag = format!("jj-cc-{env_name}-server");
            ensure_absent(
                &gcloud,
                &[
                    "compute",
                    "firewall-rules",
                    "describe",
                    &fw_name,
                    &proj_flag,
                ],
                &format!("firewall rule {fw_name}"),
                &format!("gcloud compute firewall-rules delete {fw_name} {proj_flag}"),
            )?;
            // The server port is open only to IAP's TCP forwarding range or to the ranges
            // passed with `--allow-cidr`, and only on this environment's VMs; xtask never
            // creates a rule without source ranges, which gcloud would open to 0.0.0.0/0.
            let source_ranges = if opts.gce_public_ip {
                opts.allow_cidrs.join(",")
            } else {
                "35.235.240.0/20".to_string()
            };
            if source_ranges.is_empty() {
                return Err("refusing to create a firewall rule without source ranges".into());
            }
            state.resources.gce_firewall_rule = Some(fw_name.clone());
            save_state(root, &state)?;
            run_gcloud(
                &gcloud,
                [
                    "compute",
                    "firewall-rules",
                    "create",
                    &fw_name,
                    &proj_flag,
                    "--allow=tcp:8080",
                    &format!("--target-tags={vm_tag}"),
                    &format!("--source-ranges={source_ranges}"),
                ],
            )?;
            if opts.gce_public_ip {
                // A public IP needs authentication on top of the source ranges.
                state.auth_token = Some(generate_auth_token());
                save_state(root, &state)?;
            }

            let zone_primary = select_gce_zone(&gcloud, &opts.project_id, &opts.region)?;
            let vm_primary = format!("jj-cc-{env_name}-{}", opts.region);
            ensure_gce_vm_absent(&gcloud, &opts.project_id, &zone_primary, &vm_primary)?;
            state
                .resources
                .gce_instances
                .push(format!("{zone_primary}/{vm_primary}"));
            save_state(root, &state)?;
            let ip_primary = deploy_gce_vm(
                &gcloud,
                &opts.project_id,
                &opts.region,
                &zone_primary,
                &vm_primary,
                &vm_tag,
                &image_uri,
                &sa_email,
                &spanner_db_uri,
                &primary_gcs_uri,
                peer_gcs_uri.as_deref(),
                state.auth_token.as_deref(),
            )?;
            if opts.gce_public_ip {
                state.endpoints.primary = format!("http://{ip_primary}:8080");
            } else {
                let (tunnel, watched) = spawn_iap_tunnel(
                    &gcloud,
                    root,
                    &opts.project_id,
                    &zone_primary,
                    &vm_primary,
                    primary_local_port,
                )?;
                state.resources.processes.push(tunnel);
                watched_children.push(watched);
                state.endpoints.primary = format!("http://127.0.0.1:{primary_local_port}");
            }
            save_state(root, &state)?;

            if let (Some(peer_reg), Some(peer_gcs)) = (&opts.peer_region, &peer_gcs_uri) {
                let zone_peer = select_gce_zone(&gcloud, &opts.project_id, peer_reg)?;
                let vm_peer = format!("jj-cc-{env_name}-{peer_reg}");
                ensure_gce_vm_absent(&gcloud, &opts.project_id, &zone_peer, &vm_peer)?;
                state
                    .resources
                    .gce_instances
                    .push(format!("{zone_peer}/{vm_peer}"));
                save_state(root, &state)?;
                let ip_peer = deploy_gce_vm(
                    &gcloud,
                    &opts.project_id,
                    peer_reg,
                    &zone_peer,
                    &vm_peer,
                    &vm_tag,
                    &image_uri,
                    &sa_email,
                    &spanner_db_uri,
                    peer_gcs,
                    Some(&primary_gcs_uri),
                    state.auth_token.as_deref(),
                )?;
                if opts.gce_public_ip {
                    state.endpoints.peer = Some(format!("http://{ip_peer}:8080"));
                } else {
                    let (tunnel, watched) = spawn_iap_tunnel(
                        &gcloud,
                        root,
                        &opts.project_id,
                        &zone_peer,
                        &vm_peer,
                        opts.local_peer_port,
                    )?;
                    state.resources.processes.push(tunnel);
                    watched_children.push(watched);
                    state.endpoints.peer =
                        Some(format!("http://127.0.0.1:{}", opts.local_peer_port));
                }
                save_state(root, &state)?;
            }
        }
        GcpMode::Gke => {
            let cluster_exists = gcloud_resource_exists(
                &gcloud,
                &[
                    "container",
                    "clusters",
                    "describe",
                    &opts.gke_cluster,
                    &format!("--region={}", opts.region),
                    &proj_flag,
                ],
            )?;
            state.resources.gke_cluster = Some(opts.gke_cluster.clone());
            state.resources.gke_region = Some(opts.region.clone());
            if !cluster_exists {
                state.resources.gke_cluster_created = true;
            }
            save_state(root, &state)?;
            if !cluster_exists {
                println!(
                    "==> Creating GKE Autopilot cluster {} in {}...",
                    opts.gke_cluster, opts.region
                );
                run_gcloud(
                    &gcloud,
                    [
                        "container",
                        "clusters",
                        "create-auto",
                        &opts.gke_cluster,
                        &format!("--region={}", opts.region),
                        &proj_flag,
                    ],
                )?;
            }

            run_gcloud(
                &gcloud,
                [
                    "container",
                    "clusters",
                    "get-credentials",
                    &opts.gke_cluster,
                    &format!("--region={}", opts.region),
                    &proj_flag,
                ],
            )?;
            // Pin every kubectl call (here and in `down`) to this cluster: the current context
            // can change at any time, for example when `up k8s` creates a kind cluster.
            let kube_context = gke_kube_context(&opts.project_id, &opts.region, &opts.gke_cluster);
            let kubectl = kubectl.as_deref().unwrap_or_else(|| Path::new("kubectl"));
            if cluster_exists {
                ensure_gke_workload_absent(kubectl, &kube_context)?;
            }
            state.resources.kube_context = Some(kube_context.clone());
            save_state(root, &state)?;
            run_gcloud(
                &gcloud,
                [
                    "iam",
                    "service-accounts",
                    "add-iam-policy-binding",
                    &sa_email,
                    &proj_flag,
                    "--role=roles/iam.workloadIdentityUser",
                    &format!(
                        "--member=serviceAccount:{}.svc.id.goog[default/jj-cc-server-ksa]",
                        opts.project_id
                    ),
                ],
            )?;

            // GKE manifest fails closed via `JJ_CC_SERVER_AUTH_TOKEN_FILE: /etc/jj-cc/auth/tokens`
            // backed by Secret `jj-cc-server-auth`. Generate and persist the token before applying.
            let token = generate_auth_token();
            state.auth_token = Some(token.clone());
            state.resources.gke_auth_secret = Some(GKE_AUTH_SECRET_NAME.to_string());
            save_state(root, &state)?;
            apply_gke_auth_secret(kubectl, &kube_context, GKE_AUTH_SECRET_NAME, &token)?;

            let template = fs::read_to_string(root.join("deploy/k8s/gcp/gke.yaml"))?;
            let rendered = template
                .replace("__GCP_SERVICE_ACCOUNT__", &sa_email)
                .replace("__IMAGE_URI__", &image_uri)
                .replace("__SPANNER_DATABASE__", &spanner_db_uri)
                .replace("__GCS_BUCKET__", &primary_gcs_uri)
                .replace(
                    "__GCS_PEER_BUCKETS__",
                    peer_gcs_uri.as_deref().unwrap_or(""),
                );
            let state_dir = root.join(".jj-cc");
            ensure_private_dir(&state_dir)?;
            let rendered_path = state_dir.join(format!("gke-rendered-{env_name}.yaml"));
            fs::write(&rendered_path, rendered)?;
            state.resources.kube_manifest = Some(rendered_path.display().to_string());
            save_state(root, &state)?;

            let status = Command::new(kubectl)
                .args([
                    "--context",
                    &kube_context,
                    "--namespace",
                    GKE_NAMESPACE,
                    "apply",
                    "-f",
                ])
                .arg(&rendered_path)
                .status()?;
            if !status.success() {
                return Err("`kubectl apply` to GKE failed".into());
            }
            let status = Command::new(kubectl)
                .args([
                    "--context",
                    &kube_context,
                    "--namespace",
                    GKE_NAMESPACE,
                    "rollout",
                    "status",
                    "deployment/jj-cc-server",
                    "--timeout=10m",
                ])
                .status()?;
            if !status.success() {
                return Err("GKE deployment jj-cc-server did not become ready".into());
            }

            // The Service is ClusterIP-only, so the server is never on a public IP. Reach it
            // through a local `kubectl port-forward`, managed like the GCE IAP tunnel.
            ensure_loopback_port_free(opts.local_port)?;
            println!(
                "==> Starting kubectl port-forward 127.0.0.1:{} -> service/jj-cc-server:8080...",
                opts.local_port
            );
            let pf_log = state_dir.join(format!("{env_name}-port-forward.log"));
            let port_spec = format!("{}:8080", opts.local_port);
            let pf_child = spawn_bg_child(
                kubectl,
                [
                    "--context",
                    &kube_context,
                    "--namespace",
                    GKE_NAMESPACE,
                    "port-forward",
                    "service/jj-cc-server",
                    &port_spec,
                    "--address=127.0.0.1",
                ],
                &[],
                &pf_log,
            )?;
            state
                .resources
                .processes
                .push(ManagedProcess::new(pf_child.id(), GKE_PORT_FORWARD_MARKER));
            watched_children.push(WatchedChild::new(
                "kubectl port-forward (GKE)",
                pf_child,
                &pf_log,
            ));
            state.endpoints.primary = format!("http://127.0.0.1:{}", opts.local_port);
            save_state(root, &state)?;
        }
    }

    let token = state.auth_token.as_deref();
    wait_until_healthy_with_children(
        &state.endpoints.primary,
        state.auth,
        token,
        Duration::from_secs(90),
        &mut watched_children,
    )
    .await?;
    if let Some(peer) = &state.endpoints.peer {
        wait_until_healthy_with_children(
            peer,
            state.auth,
            token,
            Duration::from_secs(90),
            &mut watched_children,
        )
        .await?;
    }
    Ok(state)
}

#[allow(clippy::too_many_arguments)]
fn deploy_cloud_run_region(
    gcloud: &Path,
    project_id: &str,
    region: &str,
    service: &str,
    image_uri: &str,
    sa_email: &str,
    spanner_db_uri: &str,
    gcs_uri: &str,
    peer_gcs_uri: Option<&str>,
) -> Result<String, BoxError> {
    println!("==> Deploying Cloud Run service {service} in {region}...");
    let peer_str = peer_gcs_uri.unwrap_or("");
    let env_arg = format!(
        "--set-env-vars=^@^JJ_CC_ALLOW_UNAUTHENTICATED=true@STORE_TYPE=spanner@SPANNER_DATABASE={spanner_db_uri}@BLOB_STORE=gcs@GCS_BUCKET={gcs_uri}@GCS_PEER_BUCKETS={peer_str}"
    );
    let proj_flag = format!("--project={project_id}");
    let reg_flag = format!("--region={region}");
    run_gcloud(
        gcloud,
        [
            "run",
            "deploy",
            service,
            &proj_flag,
            &reg_flag,
            &format!("--image={image_uri}"),
            &format!("--service-account={sa_email}"),
            "--no-allow-unauthenticated",
            "--use-http2",
            &env_arg,
        ],
    )?;
    run_gcloud(
        gcloud,
        [
            "run",
            "services",
            "describe",
            service,
            &proj_flag,
            &reg_flag,
            "--format=value(status.url)",
        ],
    )
}

/// Creates a Container-Optimized OS VM running the server image. With `auth_token`, the VM gets
/// a public IP and the server requires that bearer token; the token goes through a private
/// env file so it never appears in the local process list.
#[allow(clippy::too_many_arguments)]
fn deploy_gce_vm(
    gcloud: &Path,
    project_id: &str,
    region: &str,
    zone: &str,
    vm_name: &str,
    network_tag: &str,
    image_uri: &str,
    sa_email: &str,
    spanner_db_uri: &str,
    gcs_uri: &str,
    peer_gcs_uri: Option<&str>,
    auth_token: Option<&str>,
) -> Result<String, BoxError> {
    let public_ip = auth_token.is_some();
    let proj_flag = format!("--project={project_id}");
    let zone_flag = format!("--zone={zone}");
    if !public_ip {
        let _ = run_gcloud(
            gcloud,
            [
                "compute",
                "networks",
                "subnets",
                "update",
                "default",
                &format!("--region={region}"),
                &proj_flag,
                "--enable-private-ip-google-access",
            ],
        );
    }
    println!("==> Launching GCE Container-Optimized OS VM {vm_name} in {zone}...");
    let peer_str = peer_gcs_uri.unwrap_or("");
    let env_spanner = format!("--container-env=SPANNER_DATABASE={spanner_db_uri}");
    let env_gcs = format!("--container-env=GCS_BUCKET={gcs_uri},GCS_PEER_BUCKETS={peer_str}");
    let sa_flag = format!("--service-account={sa_email}");
    let img_flag = format!("--container-image={image_uri}");
    let tags_flag = format!("--tags={network_tag}");
    let mut create_args: Vec<OsString> = vec![
        OsString::from("compute"),
        OsString::from("instances"),
        OsString::from("create-with-container"),
        OsString::from(vm_name),
        OsString::from(&proj_flag),
        OsString::from(&zone_flag),
        OsString::from("--machine-type=e2-standard-2"),
        OsString::from(&tags_flag),
        OsString::from(&sa_flag),
        OsString::from("--scopes=https://www.googleapis.com/auth/cloud-platform"),
        OsString::from(&img_flag),
        OsString::from("--container-env=HOST=0.0.0.0,PORT=8080,STORE_TYPE=spanner,BLOB_STORE=gcs"),
        OsString::from(&env_spanner),
        OsString::from(&env_gcs),
    ];
    let token_env = if let Some(token) = auth_token {
        let mut file = tempfile::NamedTempFile::new()?;
        writeln!(file, "JJ_CC_SERVER_AUTH_TOKEN={token}")?;
        file.flush()?;
        create_args.push(OsString::from("--container-env-file"));
        create_args.push(file.path().as_os_str().to_os_string());
        Some(file)
    } else {
        create_args.push(OsString::from(
            "--container-env=JJ_CC_ALLOW_UNAUTHENTICATED=true",
        ));
        create_args.push(OsString::from("--no-address"));
        None
    };
    run_gcloud(gcloud, &create_args)?;
    drop(token_env);
    if public_ip {
        let raw_ip = run_gcloud(
            gcloud,
            [
                "compute",
                "instances",
                "describe",
                vm_name,
                &zone_flag,
                &proj_flag,
                "--format=get(networkInterfaces[0].accessConfigs[0].natIP)",
            ],
        )?;
        parse_gce_nat_ip(vm_name, &raw_ip)
    } else {
        Ok(String::new())
    }
}

fn spawn_iap_tunnel(
    gcloud: &Path,
    root: &Path,
    project_id: &str,
    zone: &str,
    vm_name: &str,
    local_port: u16,
) -> Result<(ManagedProcess, WatchedChild), BoxError> {
    ensure_loopback_port_free(local_port)?;
    println!("==> Starting IAP TCP tunnel 127.0.0.1:{local_port} -> {vm_name}:8080...");
    let log_path = root
        .join(".jj-cc")
        .join(format!("{vm_name}-iap-tunnel.log"));
    let child = spawn_bg_child(
        gcloud,
        [
            "compute",
            "start-iap-tunnel",
            vm_name,
            "8080",
            &format!("--local-host-port=127.0.0.1:{local_port}"),
            &format!("--zone={zone}"),
            &format!("--project={project_id}"),
        ],
        &[],
        &log_path,
    )?;
    let proc = ManagedProcess::new(child.id(), &format!("start-iap-tunnel {vm_name}"));
    let watched = WatchedChild::new(format!("IAP tunnel ({vm_name})"), child, log_path);
    Ok((proc, watched))
}
