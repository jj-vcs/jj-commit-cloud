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

//! Local Docker and Podman container topology orchestration for `cargo xtask`.
//!
//! # Authority and Crash-Safety Ordering
//! Persists the isolated container network, volume names, and container names into
//! `.jj-cc/<env>.json` before starting each container so `cargo xtask down` cleans up partial
//! topologies. PostgreSQL and MinIO readiness probes fail fast if a backing container exits
//! during startup.

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use super::health::wait_until_healthy_with_containers;
use super::{LocalMode, ensure_loopback_port_free, ensure_server_container_image};
use crate::state::{AuthMode, BoxError, EnvState, save_state};

pub(super) async fn up_local_containers(
    root: &Path,
    env_name: &str,
    mode: LocalMode,
    rt: &str,
    primary_port: u16,
    peer_port: Option<u16>,
    rebuild_image: bool,
) -> Result<EnvState, BoxError> {
    ensure_loopback_port_free(primary_port)?;
    if let Some(pp) = peer_port {
        ensure_loopback_port_free(pp)?;
    }

    let primary_url = format!("http://127.0.0.1:{primary_port}");
    let peer_url = peer_port.map(|p| format!("http://127.0.0.1:{p}"));

    let prefix = format!("jj-cc-{env_name}");
    let net_name = format!("{prefix}-net");
    let mut state = EnvState::new(
        env_name,
        mode.as_str(),
        rt,
        &primary_url,
        peer_url,
        AuthMode::None,
    );
    state.resources.network = Some(net_name.clone());
    save_state(root, &state)?;

    ensure_server_container_image(root, rt, rebuild_image)?;

    let _ = Command::new(rt)
        .args(["network", "create", &net_name])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();

    match mode {
        LocalMode::Memory => {
            let cname = format!("{prefix}-server");
            state.resources.containers.push(cname.clone());
            save_state(root, &state)?;
            run_detached_container(
                rt,
                &cname,
                &net_name,
                &[(primary_port, 8080)],
                &[
                    ("JJ_CC_ALLOW_UNAUTHENTICATED", "true"),
                    ("STORE_TYPE", "memory"),
                    ("BLOB_STORE", "memory"),
                ],
                &[],
                "localhost/jj-cc-server:local",
                &[],
            )?;
        }
        LocalMode::Sqlite => {
            let vol = format!("{prefix}-sqlite-data");
            let cname = format!("{prefix}-server");
            state.resources.volumes.push(vol.clone());
            state.resources.containers.push(cname.clone());
            save_state(root, &state)?;
            let _ = Command::new(rt)
                .args(["volume", "create", &vol])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
            run_detached_container(
                rt,
                &cname,
                &net_name,
                &[(primary_port, 8080)],
                &[
                    ("JJ_CC_ALLOW_UNAUTHENTICATED", "true"),
                    ("STORE_TYPE", "sqlite"),
                    ("BLOB_STORE", "local"),
                    ("SQLITE_PATH", "/data/commit_cloud.db"),
                    ("BLOB_PATH", "/data/blobs"),
                ],
                &[(&vol, "/data")],
                "localhost/jj-cc-server:local",
                &[],
            )?;
        }
        LocalMode::Postgres => {
            let pg_vol = format!("{prefix}-postgres-data");
            let minio_vol = format!("{prefix}-minio-data");
            let pg_name = format!("{prefix}-postgres");
            let minio_name = format!("{prefix}-minio");
            let cname = format!("{prefix}-server");
            state.resources.volumes.push(pg_vol.clone());
            state.resources.volumes.push(minio_vol.clone());
            state.resources.containers.push(pg_name.clone());
            state.resources.containers.push(minio_name.clone());
            state.resources.containers.push(cname.clone());
            let metrics_port = if primary_port == 8080 && ensure_loopback_port_free(50052).is_ok() {
                state.endpoints.metrics = Some("http://127.0.0.1:50052".to_string());
                Some(50052u16)
            } else {
                None
            };
            save_state(root, &state)?;

            for vol in [&pg_vol, &minio_vol] {
                let _ = Command::new(rt)
                    .args(["volume", "create", vol])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
            }

            run_detached_container(
                rt,
                &pg_name,
                &net_name,
                &[],
                &[
                    ("POSTGRES_USER", "jj"),
                    ("POSTGRES_PASSWORD", "jj"),
                    ("POSTGRES_DB", "commit_cloud"),
                ],
                &[(&pg_vol, "/var/lib/postgresql/data")],
                "docker.io/library/postgres:17-alpine",
                &[],
            )?;
            run_detached_container(
                rt,
                &minio_name,
                &net_name,
                &[(0, 9000)],
                &[
                    ("MINIO_ROOT_USER", "minioadmin"),
                    ("MINIO_ROOT_PASSWORD", "minioadmin"),
                ],
                &[(&minio_vol, "/data")],
                "cgr.dev/chainguard/minio:latest",
                &[
                    "server",
                    "/data",
                    "--address",
                    ":9000",
                    "--console-address",
                    ":9001",
                ],
            )?;

            wait_for_postgres_container(rt, &pg_name, Duration::from_secs(30)).await?;
            wait_for_minio_container(rt, &minio_name, Duration::from_secs(30)).await?;

            let pg_url = format!("postgres://jj:jj@{pg_name}:5432/commit_cloud?sslmode=disable");
            let s3_ep = format!("http://{minio_name}:9000");
            let mut ports = vec![(primary_port, 8080)];
            if let Some(mp) = metrics_port {
                ports.push((mp, 50052));
            }
            run_detached_container(
                rt,
                &cname,
                &net_name,
                &ports,
                &[
                    ("JJ_CC_ALLOW_UNAUTHENTICATED", "true"),
                    ("METRICS_LISTEN", "0.0.0.0:50052"),
                    ("STORE_TYPE", "postgres"),
                    ("POSTGRES_URL", &pg_url),
                    ("POSTGRES_AUTO_MIGRATE", "true"),
                    ("BLOB_STORE", "s3"),
                    ("S3_BUCKET", "jj-blobs"),
                    ("S3_ENDPOINT", &s3_ep),
                    ("S3_REGION", "us-east-1"),
                    ("S3_ACCESS_KEY_ID", "minioadmin"),
                    ("S3_SECRET_ACCESS_KEY", "minioadmin"),
                    ("S3_PATH_STYLE", "true"),
                    ("S3_AUTO_CREATE_BUCKET", "true"),
                ],
                &[],
                "localhost/jj-cc-server:local",
                &[],
            )?;
        }
        LocalMode::Spanner | LocalMode::Multiregion => {
            let sp_name = format!("{prefix}-spanner");
            let gcs_name = format!("{prefix}-gcs");
            state.resources.containers.push(sp_name.clone());
            state.resources.containers.push(gcs_name.clone());
            save_state(root, &state)?;

            run_detached_container(
                rt,
                &sp_name,
                &net_name,
                &[],
                &[],
                &[],
                "gcr.io/cloud-spanner-emulator/emulator:latest",
                &[],
            )?;
            run_detached_container(
                rt,
                &gcs_name,
                &net_name,
                &[],
                &[],
                &[],
                "docker.io/fsouza/fake-gcs-server:latest",
                &[
                    "-scheme",
                    "http",
                    "-port",
                    "4443",
                    "-external-url",
                    &format!("http://{gcs_name}:4443"),
                    "-backend",
                    "memory",
                ],
            )?;

            let sp_grpc = format!("{sp_name}:9010");
            let sp_rest = format!("http://{sp_name}:9020");
            let gcs_ep = format!("http://{gcs_name}:4443");

            if mode == LocalMode::Spanner {
                let cname = format!("{prefix}-server");
                state.resources.containers.push(cname.clone());
                save_state(root, &state)?;
                run_detached_container(
                    rt,
                    &cname,
                    &net_name,
                    &[(primary_port, 8080)],
                    &[
                        ("JJ_CC_ALLOW_UNAUTHENTICATED", "true"),
                        ("STORE_TYPE", "spanner"),
                        ("SPANNER_EMULATOR_HOST", &sp_grpc),
                        ("SPANNER_EMULATOR_REST_HOST", &sp_rest),
                        (
                            "SPANNER_DATABASE",
                            "projects/local-project/instances/local-instance/databases/commit-cloud",
                        ),
                        ("SPANNER_AUTO_MIGRATE", "true"),
                        ("BLOB_STORE", "gcs"),
                        ("STORAGE_EMULATOR_HOST", &gcs_ep),
                        ("GCS_BUCKET", "gs://blobs-uc1"),
                    ],
                    &[],
                    "localhost/jj-cc-server:local",
                    &[],
                )?;
            } else {
                let peer_port = peer_port.expect("multiregion always resolves peer_port");
                let us_name = format!("{prefix}-server-us");
                let eu_name = format!("{prefix}-server-eu");
                state.resources.containers.push(us_name.clone());
                state.resources.containers.push(eu_name.clone());
                save_state(root, &state)?;

                // Each region lists the other's bucket as a peer, so a blob written through
                // either server can be read through the other.
                run_detached_container(
                    rt,
                    &us_name,
                    &net_name,
                    &[(primary_port, 8080)],
                    &[
                        ("JJ_CC_ALLOW_UNAUTHENTICATED", "true"),
                        ("STORE_TYPE", "spanner"),
                        ("SPANNER_EMULATOR_HOST", &sp_grpc),
                        ("SPANNER_EMULATOR_REST_HOST", &sp_rest),
                        (
                            "SPANNER_DATABASE",
                            "projects/local-project/instances/local-instance/databases/commit-cloud",
                        ),
                        ("SPANNER_AUTO_MIGRATE", "true"),
                        ("BLOB_STORE", "gcs"),
                        ("STORAGE_EMULATOR_HOST", &gcs_ep),
                        ("GCS_BUCKET", "gs://blobs-uc1"),
                        ("GCS_PEER_BUCKETS", "gs://blobs-ew1"),
                    ],
                    &[],
                    "localhost/jj-cc-server:local",
                    &[],
                )?;
                run_detached_container(
                    rt,
                    &eu_name,
                    &net_name,
                    &[(peer_port, 8080)],
                    &[
                        ("JJ_CC_ALLOW_UNAUTHENTICATED", "true"),
                        ("STORE_TYPE", "spanner"),
                        ("SPANNER_EMULATOR_HOST", &sp_grpc),
                        ("SPANNER_EMULATOR_REST_HOST", &sp_rest),
                        (
                            "SPANNER_DATABASE",
                            "projects/local-project/instances/local-instance/databases/commit-cloud",
                        ),
                        ("SPANNER_AUTO_MIGRATE", "true"),
                        ("BLOB_STORE", "gcs"),
                        ("STORAGE_EMULATOR_HOST", &gcs_ep),
                        ("GCS_BUCKET", "gs://blobs-ew1"),
                        ("GCS_PEER_BUCKETS", "gs://blobs-uc1"),
                    ],
                    &[],
                    "localhost/jj-cc-server:local",
                    &[],
                )?;
            }
        }
    }

    wait_until_healthy_with_containers(
        &state.endpoints.primary,
        state.auth,
        None,
        Duration::from_secs(30),
        Path::new(rt),
        &state.resources.containers,
    )
    .await?;
    if let Some(peer) = &state.endpoints.peer {
        wait_until_healthy_with_containers(
            peer,
            state.auth,
            None,
            Duration::from_secs(30),
            Path::new(rt),
            &state.resources.containers,
        )
        .await?;
    }
    Ok(state)
}

#[allow(clippy::too_many_arguments)]
fn run_detached_container(
    rt: &str,
    name: &str,
    net_name: &str,
    ports: &[(u16, u16)],
    envs: &[(&str, &str)],
    volumes: &[(&str, &str)],
    image: &str,
    extra_args: &[&str],
) -> Result<(), BoxError> {
    let _ = Command::new(rt)
        .args(["rm", "-f", name])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();

    let mut cmd = Command::new(rt);
    cmd.args(["run", "-d", "--name", name, "--network", net_name]);
    for (host_p, cont_p) in ports {
        if *host_p == 0 {
            cmd.args(["-p", &format!("127.0.0.1::{cont_p}")]);
        } else {
            cmd.args(["-p", &format!("127.0.0.1:{host_p}:{cont_p}")]);
        }
    }
    for (k, v) in envs {
        cmd.args(["-e", &format!("{k}={v}")]);
    }
    for (vol, target) in volumes {
        cmd.args(["-v", &format!("{vol}:{target}")]);
    }
    cmd.arg(image).args(extra_args);
    let out = cmd.output()?;
    if !out.status.success() {
        return Err(format!(
            "`{rt} run {name}` failed: {}",
            String::from_utf8_lossy(&out.stderr)
        )
        .into());
    }
    Ok(())
}

async fn wait_for_postgres_container(
    rt: &str,
    container_name: &str,
    timeout: Duration,
) -> Result<(), BoxError> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok(inspect) = Command::new(rt)
            .args(["inspect", "--format", "{{.State.Running}}", container_name])
            .output()
            && inspect.status.success()
            && String::from_utf8_lossy(&inspect.stdout).trim() == "false"
        {
            return Err(format!(
                "PostgreSQL container '{container_name}' exited before becoming ready"
            )
            .into());
        }
        let ready = Command::new(rt)
            .args([
                "exec",
                container_name,
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
        if ready {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(
        format!("timed out waiting for PostgreSQL container '{container_name}' to become ready")
            .into(),
    )
}

async fn wait_for_minio_container(
    rt: &str,
    container_name: &str,
    timeout: Duration,
) -> Result<(), BoxError> {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let port_out = Command::new(rt)
        .args(["port", container_name, "9000/tcp"])
        .output()?;
    if !port_out.status.success() {
        return Err(format!(
            "failed to resolve published port for MinIO container '{container_name}': {}",
            String::from_utf8_lossy(&port_out.stderr).trim()
        )
        .into());
    }
    let addr = String::from_utf8_lossy(&port_out.stdout)
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .ok_or_else(|| {
            format!("MinIO container '{container_name}' did not report a mapped port for 9000/tcp")
        })?
        .to_string();

    let deadline = Instant::now() + timeout;
    let mut last_err = String::new();
    while Instant::now() < deadline {
        if let Ok(inspect) = Command::new(rt)
            .args(["inspect", "--format", "{{.State.Running}}", container_name])
            .output()
            && inspect.status.success()
            && String::from_utf8_lossy(&inspect.stdout).trim() == "false"
        {
            return Err(
                format!("MinIO container '{container_name}' exited before becoming ready").into(),
            );
        }
        match tokio::time::timeout(
            Duration::from_secs(2),
            tokio::net::TcpStream::connect(&addr),
        )
        .await
        {
            Ok(Ok(mut stream)) => {
                let req = b"GET /minio/health/live HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
                match stream.write_all(req).await {
                    Ok(()) => {
                        let mut buf = [0u8; 256];
                        match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
                            .await
                        {
                            Ok(Ok(n)) => {
                                let head = String::from_utf8_lossy(&buf[..n]);
                                if head.lines().next().is_some_and(|l| l.contains(" 200 ")) {
                                    return Ok(());
                                }
                                last_err = format!(
                                    "unexpected MinIO health response: {}",
                                    head.lines().next().unwrap_or("<empty>")
                                );
                            }
                            Ok(Err(e)) => last_err = format!("health read error: {e}"),
                            Err(_) => last_err = "health read timed out".to_string(),
                        }
                    }
                    Err(e) => last_err = format!("health request write error: {e}"),
                }
            }
            Ok(Err(e)) => last_err = e.to_string(),
            Err(_) => last_err = "connect timed out".to_string(),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Err(format!(
        "timed out waiting for MinIO container '{container_name}' at {addr} (/minio/health/live): {last_err}"
    )
    .into())
}

pub(super) fn remove_container_resource(rt: &str, args: &[&str]) -> Result<(), String> {
    let out = Command::new(rt)
        .args(args)
        .output()
        .map_err(|e| e.to_string())?;
    let stderr = String::from_utf8_lossy(&out.stderr);
    let lower = stderr.to_ascii_lowercase();
    if out.status.success() || lower.contains("no such") || lower.contains("not found") {
        Ok(())
    } else {
        Err(stderr.trim().to_string())
    }
}
