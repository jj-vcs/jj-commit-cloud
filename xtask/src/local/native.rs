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

//! Native host server and local Cloud Spanner emulator startup for `cargo xtask`.
//!
//! # Authority and Crash-Safety Ordering
//! Verifies loopback ports are free before building binaries, persists managed data directories
//! and child PID markers into `.jj-cc/<env>.json` immediately after spawn, and monitors child
//! processes during health polling so early crashes report their exit status and log path.

use std::ffi::{OsStr, OsString};
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use super::process::{SERVER_PROCESS_MARKER, spawn_bg_server_child, stop_process};
use super::{
    ContainerRuntimeChoice, LocalMode, NativeSpannerSource, WatchedChild,
    build_and_export_binaries, resolve_native_pg_url, resolve_native_spanner_source,
    spawn_bg_child, wait_until_healthy_with_children,
};
use crate::state::{
    AuthMode, BoxError, EnvState, ManagedProcess, cargo_target_dir, ensure_private_dir, save_state,
};

pub(super) async fn up_local_native(
    root: &Path,
    env_name: &str,
    mode: LocalMode,
    primary_port: u16,
    peer_port: Option<u16>,
    native_pg_url: Option<String>,
    native_spanner: Option<NativeSpannerSource>,
) -> Result<EnvState, BoxError> {
    let native_pg_url = if mode == LocalMode::Postgres {
        match native_pg_url {
            Some(url) => Some(url),
            None => Some(resolve_native_pg_url(|k| std::env::var(k).ok())?),
        }
    } else {
        None
    };

    ensure_loopback_port_free(primary_port)?;
    if let Some(pp) = peer_port {
        ensure_loopback_port_free(pp)?;
    }

    let exe_name = if cfg!(windows) {
        "jj-cc-server.exe"
    } else {
        "jj-cc-server"
    };
    build_and_export_binaries(root, false, false, ContainerRuntimeChoice::Auto)?;
    let server_bin = cargo_target_dir(root).join("debug").join(exe_name);
    let primary_url = format!("http://127.0.0.1:{primary_port}");
    let peer_url = peer_port.map(|p| format!("http://127.0.0.1:{p}"));

    let state_dir = root.join(".jj-cc");
    ensure_private_dir(&state_dir)?;
    let data_dir = state_dir.join(format!("data-{env_name}"));
    fs::create_dir_all(&data_dir)?;
    let log_path = state_dir.join(format!("{env_name}.log"));
    let peer_log_path = state_dir.join(format!("{env_name}-peer.log"));

    let mut state = EnvState::new(
        env_name,
        mode.as_str(),
        "native",
        &primary_url,
        peer_url,
        AuthMode::None,
    );
    state
        .resources
        .data_dirs
        .push(data_dir.display().to_string());
    save_state(root, &state)?;
    println!(
        "==> Starting native servers (log: {})...",
        log_path.display()
    );

    let mut children = Vec::new();
    match mode {
        LocalMode::Memory => {
            let port_arg = format!("--port={primary_port}");
            let child = spawn_bg_server_child(
                &server_bin,
                [
                    "--host=127.0.0.1",
                    port_arg.as_str(),
                    "--store-type=memory",
                    "--blob-store=memory",
                ],
                &[],
                &log_path,
            )?;
            state
                .resources
                .processes
                .push(ManagedProcess::new(child.id(), SERVER_PROCESS_MARKER));
            children.push(WatchedChild::new("native jj-cc-server", child, &log_path));
            save_state(root, &state)?;
        }
        LocalMode::Sqlite => {
            let db = data_dir.join("commit_cloud.db");
            let blobs = data_dir.join("blobs");
            let port_arg = format!("--port={primary_port}");
            let child = spawn_bg_server_child(
                &server_bin,
                [
                    OsStr::new("--host=127.0.0.1"),
                    OsStr::new(&port_arg),
                    OsStr::new("--store-type=sqlite"),
                    OsStr::new("--blob-store=local"),
                    OsStr::new("--sqlite-path"),
                    db.as_os_str(),
                    OsStr::new("--blob-path"),
                    blobs.as_os_str(),
                ],
                &[],
                &log_path,
            )?;
            state
                .resources
                .processes
                .push(ManagedProcess::new(child.id(), SERVER_PROCESS_MARKER));
            children.push(WatchedChild::new("native jj-cc-server", child, &log_path));
            save_state(root, &state)?;
        }
        LocalMode::Postgres => {
            let pg_url = native_pg_url.expect("validated before building binaries");
            let blobs = data_dir.join("blobs");
            let port_arg = format!("--port={primary_port}");
            let mut env_pairs: Vec<(&str, String)> = vec![
                ("POSTGRES_URL", pg_url),
                ("POSTGRES_AUTO_MIGRATE", "true".to_string()),
            ];
            for key in ["POSTGRES_MAX_CONNS", "POSTGRES_CA_CERT"] {
                if let Ok(val) = std::env::var(key)
                    && !val.trim().is_empty()
                {
                    env_pairs.push((key, val));
                }
            }
            let mut args: Vec<OsString> = vec![
                OsString::from("--host=127.0.0.1"),
                OsString::from(&port_arg),
                OsString::from("--store-type=postgres"),
            ];
            if primary_port == 8080 && ensure_loopback_port_free(50052).is_ok() {
                args.push(OsString::from("--metrics-listen=127.0.0.1:50052"));
                state.endpoints.metrics = Some("http://127.0.0.1:50052".to_string());
            }
            if let Ok(bucket) = std::env::var("S3_BUCKET")
                && !bucket.trim().is_empty()
            {
                args.push(OsString::from("--blob-store=s3"));
                env_pairs.push(("S3_BUCKET", bucket));
                for key in [
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
                ] {
                    if let Ok(val) = std::env::var(key)
                        && !val.trim().is_empty()
                    {
                        env_pairs.push((key, val));
                    }
                }
            } else {
                args.push(OsString::from("--blob-store=local"));
                args.push(OsString::from("--blob-path"));
                args.push(blobs.into_os_string());
            }
            let env_refs: Vec<(&str, &str)> =
                env_pairs.iter().map(|(k, v)| (*k, v.as_str())).collect();
            let child = spawn_bg_server_child(&server_bin, args, &env_refs, &log_path)?;
            state
                .resources
                .processes
                .push(ManagedProcess::new(child.id(), SERVER_PROCESS_MARKER));
            children.push(WatchedChild::new("native jj-cc-server", child, &log_path));
            save_state(root, &state)?;
        }
        LocalMode::Spanner | LocalMode::Multiregion => {
            let emu_log = state_dir.join(format!("{env_name}-spanner-emulator.log"));
            let spanner_source = match native_spanner {
                Some(src) => src,
                None => resolve_native_spanner_source(|k| std::env::var(k).ok())?,
            };
            let (sp_grpc, sp_rest, emu_child) =
                start_native_spanner_emulator(&spanner_source, &emu_log)?;
            if let Some(child) = emu_child {
                state
                    .resources
                    .processes
                    .push(ManagedProcess::new(child.id(), "gateway_main"));
                children.push(WatchedChild::new(
                    "native Spanner emulator",
                    child,
                    &emu_log,
                ));
                save_state(root, &state)?;
            }
            let uc1_dir = data_dir.join("blobs-uc1");
            fs::create_dir_all(&uc1_dir)?;
            let ew1_dir = data_dir.join("blobs-ew1");
            let us_port = format!("--port={primary_port}");
            let mut us_peer_val = OsString::from("gs://blobs-ew1=");
            us_peer_val.push(&ew1_dir);
            let mut us_args: Vec<&OsStr> = vec![
                OsStr::new("--host=127.0.0.1"),
                OsStr::new(&us_port),
                OsStr::new("--store-type=spanner"),
                OsStr::new(
                    "--spanner-database=projects/local-project/instances/local-instance/databases/commit-cloud",
                ),
                OsStr::new("--spanner-auto-migrate"),
                OsStr::new("--blob-store=local"),
                OsStr::new("--blob-path"),
                uc1_dir.as_os_str(),
                OsStr::new("--blob-location-id=gs://blobs-uc1"),
            ];
            if mode == LocalMode::Multiregion {
                // Each region lists the other's directory as a peer, so a blob written through
                // either server can be read through the other.
                fs::create_dir_all(&ew1_dir)?;
                us_args.push(OsStr::new("--local-peer-blob-dirs"));
                us_args.push(us_peer_val.as_os_str());
            }
            let child_us = spawn_bg_server_child(
                &server_bin,
                us_args,
                &[
                    ("SPANNER_EMULATOR_HOST", &sp_grpc),
                    ("SPANNER_EMULATOR_REST_HOST", &sp_rest),
                ],
                &log_path,
            )?;
            state
                .resources
                .processes
                .push(ManagedProcess::new(child_us.id(), SERVER_PROCESS_MARKER));
            children.push(WatchedChild::new(
                "native jj-cc-server (primary)",
                child_us,
                &log_path,
            ));
            save_state(root, &state)?;

            if let Some(pp) = peer_port {
                let eu_port = format!("--port={pp}");
                let mut eu_peer_val = OsString::from("gs://blobs-uc1=");
                eu_peer_val.push(&uc1_dir);
                let child_eu = spawn_bg_server_child(
                    &server_bin,
                    [
                        OsStr::new("--host=127.0.0.1"),
                        OsStr::new(&eu_port),
                        OsStr::new("--store-type=spanner"),
                        OsStr::new(
                            "--spanner-database=projects/local-project/instances/local-instance/databases/commit-cloud",
                        ),
                        OsStr::new("--spanner-auto-migrate"),
                        OsStr::new("--blob-store=local"),
                        OsStr::new("--blob-path"),
                        ew1_dir.as_os_str(),
                        OsStr::new("--blob-location-id=gs://blobs-ew1"),
                        OsStr::new("--local-peer-blob-dirs"),
                        eu_peer_val.as_os_str(),
                    ],
                    &[
                        ("SPANNER_EMULATOR_HOST", &sp_grpc),
                        ("SPANNER_EMULATOR_REST_HOST", &sp_rest),
                    ],
                    &peer_log_path,
                )?;
                state
                    .resources
                    .processes
                    .push(ManagedProcess::new(child_eu.id(), SERVER_PROCESS_MARKER));
                children.push(WatchedChild::new(
                    "native jj-cc-server (peer)",
                    child_eu,
                    &peer_log_path,
                ));
                save_state(root, &state)?;
            }
        }
    }

    wait_until_healthy_with_children(
        &state.endpoints.primary,
        state.auth,
        None,
        Duration::from_secs(20),
        &mut children,
    )
    .await?;
    if let Some(peer) = &state.endpoints.peer {
        wait_until_healthy_with_children(
            peer,
            state.auth,
            None,
            Duration::from_secs(20),
            &mut children,
        )
        .await?;
    }
    Ok(state)
}

pub fn ensure_loopback_port_free(port: u16) -> Result<(), BoxError> {
    if port == 0 {
        return Err("port must be between 1 and 65535 (ephemeral port 0 is not supported)".into());
    }
    match std::net::TcpListener::bind(("127.0.0.1", port)) {
        Ok(listener) => {
            drop(listener);
            Ok(())
        }
        Err(e) => Err(format!(
            "127.0.0.1:{port} is unavailable ({e}); stop the existing process or pass `--port`"
        )
        .into()),
    }
}

pub(super) fn resolve_spanner_emulator_endpoints(
    host_env: Option<String>,
    rest_env: Option<String>,
) -> Option<(String, String)> {
    let host = host_env
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())?;
    let rest = rest_env
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "http://127.0.0.1:9020".to_string());
    Some((host, rest))
}

pub(super) fn wait_for_native_spanner_emulator(
    child: &mut std::process::Child,
    grpc_addr: std::net::SocketAddr,
    rest_addr: std::net::SocketAddr,
    timeout: Duration,
    log_path: &Path,
) -> Result<(), BoxError> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait()? {
            return Err(format!(
                "native Spanner emulator (PID {}) exited during startup with {status} (see {})",
                child.id(),
                log_path.display()
            )
            .into());
        }
        if std::net::TcpStream::connect_timeout(&grpc_addr, Duration::from_millis(100)).is_ok()
            && std::net::TcpStream::connect_timeout(&rest_addr, Duration::from_millis(100)).is_ok()
        {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    if let Some(status) = child.try_wait()? {
        return Err(format!(
            "native Spanner emulator (PID {}) exited during startup with {status} (see {})",
            child.id(),
            log_path.display()
        )
        .into());
    }
    let pid = child.id();
    let _ = stop_process(pid, &["gateway_main"]);
    if child.try_wait()?.is_none() {
        let _ = child.kill();
    }
    let _ = child.wait();
    Err(format!(
        "native Spanner emulator did not listen on {grpc_addr}/{rest_addr} within {}s (see {})",
        timeout.as_secs(),
        log_path.display()
    )
    .into())
}

fn start_native_spanner_emulator(
    source: &NativeSpannerSource,
    log_path: &Path,
) -> Result<(String, String, Option<std::process::Child>), BoxError> {
    match source {
        NativeSpannerSource::External { grpc, rest } => Ok((grpc.clone(), rest.clone(), None)),
        NativeSpannerSource::LocalDir { gateway, emulator } => {
            ensure_loopback_port_free(9010)?;
            ensure_loopback_port_free(9020)?;
            let mut grpc_flag = OsString::from("-grpc_binary=");
            grpc_flag.push(emulator);
            let mut child = spawn_bg_child(
                gateway,
                [
                    grpc_flag.as_os_str(),
                    OsStr::new("-hostname=127.0.0.1"),
                    OsStr::new("-grpc_port=9010"),
                    OsStr::new("-http_port=9020"),
                    OsStr::new("-disable_switch_user_and_group=true"),
                ],
                &[],
                log_path,
            )?;
            let grpc_addr = std::net::SocketAddr::from(([127, 0, 0, 1], 9010));
            let rest_addr = std::net::SocketAddr::from(([127, 0, 0, 1], 9020));
            wait_for_native_spanner_emulator(
                &mut child,
                grpc_addr,
                rest_addr,
                Duration::from_secs(10),
                log_path,
            )?;
            Ok((
                "127.0.0.1:9010".to_string(),
                "http://127.0.0.1:9020".to_string(),
                Some(child),
            ))
        }
    }
}
