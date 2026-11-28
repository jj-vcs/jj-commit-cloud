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

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};

pub struct ServerGuard {
    child: Child,
    url: String,
    daemons: DaemonSandbox,
    metrics_addr: Option<std::net::SocketAddr>,
}

impl ServerGuard {
    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn metrics_addr(&self) -> Option<std::net::SocketAddr> {
        self.metrics_addr
    }

    /// A `jj` command whose auto-started daemons live in this server's
    /// sandbox and stop with the server.
    pub fn jj_cmd(&self) -> assert_cmd::Command {
        self.daemons.jj_cmd()
    }

    /// Where this server's auto-started daemons put their sockets.
    pub fn daemon_socket_dir(&self) -> &std::path::Path {
        self.daemons.path()
    }
}

/// Legacy socket-dir override; also kept alongside `JJ_CC_DAEMON_DIR` for sandboxed tests.
const DAEMON_SOCKET_DIR_ENV: &str = "JJ_CC_DAEMON_SOCKET_DIR";

/// Confines auto-started `jj-cc-daemon` processes and daemon config to one test.
///
/// Commands from [`DaemonSandbox::jj_cmd`] isolate daemon discovery (`JJ_CC_DAEMON_DIR`),
/// L2 cache (`JJ_CC_CACHE_DIR`), and daemon enablement config (`JJ_CC_DAEMON_CONFIG`) inside a
/// fresh temporary directory, and dropping the sandbox stops every daemon whose state or socket
/// directory lives inside it.
pub struct DaemonSandbox {
    dir: tempfile::TempDir,
}

impl DaemonSandbox {
    pub fn new() -> Self {
        DaemonSandbox {
            dir: tempfile::tempdir().expect("daemon socket directory should have been created"),
        }
    }

    pub fn path(&self) -> &std::path::Path {
        self.dir.path()
    }

    /// The `jj` binary, with auto-started daemons and daemon config kept in this sandbox.
    pub fn jj_cmd(&self) -> assert_cmd::Command {
        let mut cmd = find_jj_cmd();
        cmd.env(DAEMON_SOCKET_DIR_ENV, self.dir.path());
        cmd.env("JJ_CC_DAEMON_DIR", self.dir.path().join("daemon"));
        cmd.env("JJ_CC_CACHE_DIR", self.dir.path().join("cache"));
        cmd.env("JJ_CC_DAEMON_CONFIG", self.dir.path().join("daemon.toml"));
        cmd
    }
}

impl Default for DaemonSandbox {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for DaemonSandbox {
    fn drop(&mut self) {
        // The daemons were started by `jj`, not by this process, so there is
        // no child handle to kill. Each one has its socket path, and so this
        // unique directory, on its command line.
        let pattern = self.dir.path().to_string_lossy().into_owned();
        let _ = std::process::Command::new("pkill")
            .args(["-f", &pattern])
            .status();
        // Wait until they are gone, so the next test starts clean.
        for _ in 0..100 {
            let alive = std::process::Command::new("pgrep")
                .args(["-f", &pattern])
                .stdout(Stdio::null())
                .status()
                .is_ok_and(|s| s.success());
            if !alive {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

fn extract_listening_address(line: &str) -> Option<String> {
    let marker = "listening on ";
    if let Some(idx) = line.find(marker) {
        let addr_part = &line[idx + marker.len()..];
        let addr = addr_part.trim().to_string();
        if !addr.is_empty() {
            return Some(addr);
        }
    }
    None
}

fn target_profile_dir() -> Option<PathBuf> {
    let current_exe = std::env::current_exe().ok()?;
    Some(current_exe.parent()?.parent()?.to_path_buf())
}

fn find_jj_cmd() -> assert_cmd::Command {
    if let Some(path) = std::env::var_os("CARGO_BIN_EXE_jj")
        && !path.is_empty()
    {
        return assert_cmd::Command::new(path);
    }
    if let Some(target_dir) = target_profile_dir() {
        let direct = target_dir.join(format!("jj{}", std::env::consts::EXE_SUFFIX));
        if direct.is_file() {
            return assert_cmd::Command::new(direct);
        }
    }
    #[allow(deprecated)]
    assert_cmd::Command::cargo_bin("jj").expect("The jj CLI binary should have compiled")
}

fn extract_metrics_address(line: &str) -> Option<std::net::SocketAddr> {
    let marker = "jj-cc-server metrics server bound to ";
    let idx = line.find(marker)?;
    line[idx + marker.len()..].trim().parse().ok()
}
pub fn daemon_binary_path() -> PathBuf {
    PathBuf::from(env!("JJ_CC_DAEMON_BIN"))
}

pub fn server_binary_path() -> PathBuf {
    PathBuf::from(env!("JJ_CC_SERVER_BIN"))
}

pub async fn spawn_server() -> ServerGuard {
    spawn_server_with_args(&[]).await
}

pub async fn spawn_server_with_args(extra_args: &[&str]) -> ServerGuard {
    spawn_server_with_env(&[], extra_args).await
}

pub const SANITIZED_SERVER_ENV_VARS: &[&str] = &[
    "HOST",
    "PORT",
    "STORE_TYPE",
    "BLOB_STORE",
    "INLINE_BLOB_THRESHOLD_BYTES",
    "SQLITE_PATH",
    "BLOB_PATH",
    "BLOB_LOCATION_ID",
    "LOCAL_PEER_BLOB_DIRS",
    "SPANNER_DATABASE",
    "SPANNER_ENDPOINT",
    "SPANNER_EMULATOR",
    "SPANNER_EMULATOR_HOST",
    "SPANNER_EMULATOR_REST_HOST",
    "SPANNER_AUTO_MIGRATE",
    "GCS_BUCKET",
    "GCS_PEER_BUCKETS",
    "STORAGE_EMULATOR_HOST",
    "BLOB_CACHE_BYTES",
    "GCS_REPLICATION_RETRY_MS",
    "GCS_ASYNC_WRITE_FANOUT",
    "JJ_CC_SERVER_AUTH_TOKEN",
    "JJ_CC_SERVER_AUTH_TOKEN_FILE",
    "JJ_CC_ALLOW_UNAUTHENTICATED",
    "METRICS_LISTEN",
];

pub async fn spawn_server_with_env(env_vars: &[(&str, &str)], extra_args: &[&str]) -> ServerGuard {
    let server_binary_path = server_binary_path();

    let mut cmd = Command::new(server_binary_path);
    cmd.arg("--port=0");
    for var in SANITIZED_SERVER_ENV_VARS {
        cmd.env_remove(var);
    }
    for (k, v) in env_vars {
        cmd.env(k, v);
    }
    for arg in extra_args {
        cmd.arg(arg);
    }

    let mut child = cmd
        .kill_on_drop(true)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("The jj-cc-server process should have spawned");

    let stdout = child
        .stdout
        .take()
        .expect("The stdout pipe should have been captured from the child process");
    let mut stderr = child
        .stderr
        .take()
        .expect("The stderr pipe should have been captured from the child process");

    let stderr_buf: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let stderr_buf_bg = Arc::clone(&stderr_buf);
    let stderr_drain = tokio::spawn(async move {
        const MAX_CAPTURED_STDERR_BYTES: usize = 16 * 1024;
        let mut chunk = [0u8; 4096];
        while let Ok(n) = stderr.read(&mut chunk).await {
            if n == 0 {
                break;
            }
            let mut buf = stderr_buf_bg.lock().unwrap_or_else(|e| e.into_inner());
            let remaining = MAX_CAPTURED_STDERR_BYTES.saturating_sub(buf.len());
            if remaining > 0 {
                buf.extend_from_slice(&chunk[..n.min(remaining)]);
            }
        }
    });

    let mut reader = BufReader::new(stdout).lines();

    let timeout = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(timeout);

    let mut metrics_addr = None;
    let server_addr = loop {
        tokio::select! {
            line_result = reader.next_line() => {
                match line_result {
                    Ok(Some(line)) => {
                        if let Some(bound) = extract_metrics_address(&line) {
                            metrics_addr = Some(bound);
                        }
                        if let Some(addr) = extract_listening_address(&line) {
                            break addr;
                        }
                    }
                    Ok(None) => {
                        let _ = tokio::time::timeout(Duration::from_millis(200), stderr_drain).await;
                        let captured = String::from_utf8_lossy(
                            &stderr_buf.lock().unwrap_or_else(|e| e.into_inner()),
                        )
                        .into_owned();
                        panic!(
                            "The stdout pipe closed before the server listening address was printed (stderr: {captured})"
                        );
                    }
                    Err(e) => {
                        let captured = String::from_utf8_lossy(
                            &stderr_buf.lock().unwrap_or_else(|e| e.into_inner()),
                        )
                        .into_owned();
                        panic!(
                            "A line from the server stdout should have been read: {e:?} (stderr: {captured})"
                        );
                    }
                }
            }
            _ = &mut timeout => {
                let captured = String::from_utf8_lossy(
                    &stderr_buf.lock().unwrap_or_else(|e| e.into_inner()),
                )
                .into_owned();
                panic!(
                    "The server listening address should have appeared in stdout before timeout (stderr: {captured})"
                );
            }
        }
    };

    // Keep draining stdout (and stderr via the detached `stderr_drain` task) in the background
    // so the child process never blocks on a full pipe.
    tokio::spawn(async move { while let Ok(Some(_)) = reader.next_line().await {} });

    let url = format!("http://{}", server_addr);

    ServerGuard {
        child,
        url,
        daemons: DaemonSandbox::new(),
        metrics_addr,
    }
}

pub async fn spawn_sqlite_server(db_path: &Path) -> ServerGuard {
    let sqlite_arg = format!("--sqlite-path={}", db_path.display());
    spawn_server_with_args(&["--store-type=sqlite", &sqlite_arg]).await
}

fn allocate_ephemeral_port_pair() -> (u16, u16) {
    let grpc_listener = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("gRPC ephemeral port allocation should succeed");
    let http_listener = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("HTTP ephemeral port allocation should succeed");
    let grpc_port = grpc_listener
        .local_addr()
        .expect("gRPC local_addr should resolve")
        .port();
    let http_port = http_listener
        .local_addr()
        .expect("HTTP local_addr should resolve")
        .port();
    debug_assert_ne!(grpc_port, http_port);
    drop((grpc_listener, http_listener));
    (grpc_port, http_port)
}

fn normalize_emulator_host_port(raw: &str) -> Option<String> {
    let normalized = raw
        .trim()
        .trim_start_matches("http://")
        .trim_end_matches('/');
    if normalized.is_empty() {
        None
    } else {
        Some(normalized.to_string())
    }
}

fn resolve_external_spanner_hosts(
    grpc_env: Option<&str>,
    rest_env: Option<&str>,
) -> Option<(String, String)> {
    let grpc_host_port = grpc_env.and_then(normalize_emulator_host_port)?;
    let rest_host_port = rest_env
        .and_then(normalize_emulator_host_port)
        .unwrap_or_else(|| {
            grpc_host_port
                .strip_suffix(":9010")
                .map(|h| format!("{h}:9020"))
                .unwrap_or_else(|| grpc_host_port.clone())
        });
    Some((grpc_host_port, rest_host_port))
}

#[derive(Debug, PartialEq, Eq)]
enum SpannerEmulatorPlan {
    AttachExternal {
        grpc_host_port: String,
        rest_host_port: String,
        fault_injection_fallback: bool,
    },
    SpawnLocal {
        gateway_bin: PathBuf,
        grpc_bin: PathBuf,
        enable_fault_injection: bool,
    },
}

fn plan_spanner_emulator(
    enable_fault_injection: bool,
    external_hosts: Option<(String, String)>,
    local_bins: Option<(PathBuf, PathBuf)>,
) -> Option<SpannerEmulatorPlan> {
    if enable_fault_injection {
        if let Some((gateway_bin, grpc_bin)) = local_bins {
            return Some(SpannerEmulatorPlan::SpawnLocal {
                gateway_bin,
                grpc_bin,
                enable_fault_injection: true,
            });
        }
        return external_hosts.map(|(grpc_host_port, rest_host_port)| {
            SpannerEmulatorPlan::AttachExternal {
                grpc_host_port,
                rest_host_port,
                fault_injection_fallback: true,
            }
        });
    }

    if let Some((grpc_host_port, rest_host_port)) = external_hosts {
        return Some(SpannerEmulatorPlan::AttachExternal {
            grpc_host_port,
            rest_host_port,
            fault_injection_fallback: false,
        });
    }

    local_bins.map(|(gateway_bin, grpc_bin)| SpannerEmulatorPlan::SpawnLocal {
        gateway_bin,
        grpc_bin,
        enable_fault_injection: false,
    })
}

fn locate_spanner_emulator_binaries() -> Option<(PathBuf, PathBuf)> {
    if let (Ok(gw), Ok(grpc)) = (
        std::env::var("SPANNER_EMULATOR_GATEWAY_BIN"),
        std::env::var("SPANNER_EMULATOR_GRPC_BIN"),
    ) {
        let gw_path = PathBuf::from(gw);
        let grpc_path = PathBuf::from(grpc);
        if gw_path.exists() && grpc_path.exists() {
            return Some((gw_path, grpc_path));
        }
    }

    if let Some(paths) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&paths) {
            let gw = dir.join("gateway_main");
            let grpc = dir.join("emulator_main");
            if gw.exists() && grpc.exists() {
                return Some((gw, grpc));
            }
        }
    }

    if let Ok(emu_dir) = std::env::var("JJ_CC_SPANNER_EMULATOR_DIR")
        && !emu_dir.trim().is_empty()
    {
        let dir = Path::new(emu_dir.trim());
        let gw = dir.join("gateway_main");
        let grpc = dir.join("emulator_main");
        if gw.exists() && grpc.exists() {
            return Some((gw, grpc));
        }
    }

    None
}

const EMULATOR_HTTP_TIMEOUT: Duration = Duration::from_secs(5);

async fn send_http_json(
    host_port: &str,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> std::io::Result<(u16, String)> {
    send_http_json_with_timeout(host_port, method, path, body, EMULATOR_HTTP_TIMEOUT).await
}

async fn send_http_json_with_timeout(
    host_port: &str,
    method: &str,
    path: &str,
    body: Option<&str>,
    timeout_dur: Duration,
) -> std::io::Result<(u16, String)> {
    tokio::time::timeout(timeout_dur, async {
        let mut stream = TcpStream::connect(host_port).await?;
        let payload = body.unwrap_or("");
        let request = format!(
            "{method} {path} HTTP/1.0\r\nHost: {host_port}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{payload}",
            payload.len()
        );
        stream.write_all(request.as_bytes()).await?;
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).await?;
        let response = String::from_utf8_lossy(&raw).into_owned();

        let status_code = response
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse::<u16>().ok())
            .unwrap_or(0);
        Ok((status_code, response))
    })
    .await
    .map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("HTTP {method} {path} to {host_port} timed out after {timeout_dur:?}"),
        )
    })?
}

/// Manages a hermetic Cloud Spanner Emulator (`gateway_main` + `emulator_main`)
/// instance on ephemeral ports (or attaches to an existing `SPANNER_EMULATOR_HOST`).
pub struct SpannerEmulatorGuard {
    child: Option<Child>,
    grpc_host_port: String,
    rest_url: String,
    project_id: String,
    instance_id: String,
}

impl SpannerEmulatorGuard {
    /// Starts an emulator like [`Self::start`], or prints a skip notice and returns `None`
    /// when none is available. With `JJ_CC_REQUIRE_SPANNER=1` a missing emulator fails the
    /// test instead, so CI jobs that provide the emulator cannot pass without running it.
    pub async fn start_or_skip(test_name: &str, enable_fault_injection: bool) -> Option<Self> {
        if let Some(guard) = Self::start(enable_fault_injection).await {
            return Some(guard);
        }
        on_missing_spanner_emulator(
            test_name,
            std::env::var("JJ_CC_REQUIRE_SPANNER").ok().as_deref(),
        );
        None
    }

    pub async fn start(enable_fault_injection: bool) -> Option<Self> {
        let external_hosts = resolve_external_spanner_hosts(
            std::env::var("SPANNER_EMULATOR_HOST").ok().as_deref(),
            std::env::var("SPANNER_EMULATOR_REST_HOST").ok().as_deref(),
        );
        let local_bins = if enable_fault_injection || external_hosts.is_none() {
            locate_spanner_emulator_binaries()
        } else {
            None
        };
        Self::start_with_plan(plan_spanner_emulator(
            enable_fault_injection,
            external_hosts,
            local_bins,
        )?)
        .await
    }

    async fn start_with_plan(plan: SpannerEmulatorPlan) -> Option<Self> {
        let project_id = "test-project".to_string();
        let instance_id = "test-instance".to_string();

        match plan {
            SpannerEmulatorPlan::AttachExternal {
                grpc_host_port,
                rest_host_port,
                fault_injection_fallback,
            } => {
                if fault_injection_fallback {
                    let _ = writeln!(
                        std::io::stderr(),
                        "NOTE: local Cloud Spanner Emulator binaries not found; attaching to \
                         SPANNER_EMULATOR_HOST ({grpc_host_port}) instead of spawning a local \
                         -enable_fault_injection=true emulator"
                    );
                }
                let guard = Self {
                    child: None,
                    grpc_host_port,
                    rest_url: format!("http://{rest_host_port}"),
                    project_id,
                    instance_id,
                };
                guard.ensure_instance(&rest_host_port).await;
                Some(guard)
            }
            SpannerEmulatorPlan::SpawnLocal {
                gateway_bin,
                grpc_bin,
                enable_fault_injection,
            } => {
                let (grpc_port, http_port) = allocate_ephemeral_port_pair();
                let grpc_host_port = format!("127.0.0.1:{grpc_port}");
                let rest_host_port = format!("127.0.0.1:{http_port}");

                let mut cmd = Command::new(gateway_bin);
                cmd.arg(format!("-grpc_binary={}", grpc_bin.display()))
                    .arg("-hostname=127.0.0.1")
                    .arg(format!("-grpc_port={grpc_port}"))
                    .arg(format!("-http_port={http_port}"))
                    .arg("-disable_switch_user_and_group=true");
                if enable_fault_injection {
                    cmd.arg("-enable_fault_injection=true");
                }

                #[cfg(unix)]
                cmd.process_group(0);

                let child = cmd
                    .kill_on_drop(true)
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()
                    .expect("Cloud Spanner emulator gateway_main should spawn");

                let mut guard = Self {
                    child: Some(child),
                    grpc_host_port,
                    rest_url: format!("http://{rest_host_port}"),
                    project_id,
                    instance_id,
                };

                let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
                loop {
                    if let Some(child) = guard.child.as_mut()
                        && let Ok(Some(status)) = child.try_wait()
                    {
                        panic!(
                            "Cloud Spanner Emulator gateway_main exited prematurely with status {status}"
                        );
                    }
                    if let Ok((status, _)) = send_http_json(
                        &rest_host_port,
                        "GET",
                        "/v1/projects/test-project/instances",
                        None,
                    )
                    .await
                        && status == 200
                    {
                        break;
                    }
                    if tokio::time::Instant::now() >= deadline {
                        panic!(
                            "Timed out waiting for Cloud Spanner Emulator REST port {rest_host_port}"
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }

                guard.ensure_instance(&rest_host_port).await;
                Some(guard)
            }
        }
    }

    async fn ensure_instance(&self, rest_host_port: &str) {
        let create_body = format!(
            r#"{{"instanceId":"{inst}","instance":{{"config":"projects/{proj}/instanceConfigs/emulator-config","displayName":"{inst}","nodeCount":1}}}}"#,
            inst = self.instance_id,
            proj = self.project_id,
        );
        let path = format!("/v1/projects/{}/instances", self.project_id);
        let (status, resp) = send_http_json(rest_host_port, "POST", &path, Some(&create_body))
            .await
            .expect("POST /instances to Spanner emulator should succeed");
        // 200 OK (created) or 409 Conflict (already exists)
        assert!(
            status == 200 || status == 409,
            "Unexpected Spanner emulator create_instance status {status}: {resp}"
        );
    }

    pub fn grpc_host_port(&self) -> &str {
        &self.grpc_host_port
    }

    pub fn grpc_endpoint(&self) -> String {
        format!("http://{}", self.grpc_host_port)
    }

    pub fn rest_endpoint(&self) -> &str {
        &self.rest_url
    }

    pub fn database_uri(&self, db_id: &str) -> String {
        format!(
            "projects/{}/instances/{}/databases/{db_id}",
            self.project_id, self.instance_id
        )
    }
}

impl Drop for SpannerEmulatorGuard {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            #[cfg(unix)]
            if let Some(pid) = child.id() {
                let pgid = format!("-{pid}");
                let _ = std::process::Command::new("kill")
                    .args(["-TERM", "--", &pgid])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
                for _ in 0..25 {
                    if matches!(child.try_wait(), Ok(Some(_)) | Err(_)) {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
                let _ = std::process::Command::new("kill")
                    .args(["-KILL", "--", &pgid])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
            }
            let _ = child.start_kill();
            for _ in 0..25 {
                if matches!(child.try_wait(), Ok(Some(_)) | Err(_)) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    }
}

fn on_missing_spanner_emulator(test_name: &str, require_spanner_env: Option<&str>) {
    let hint = "set SPANNER_EMULATOR_HOST, put gateway_main and emulator_main on PATH, or set \
                SPANNER_EMULATOR_GATEWAY_BIN and SPANNER_EMULATOR_GRPC_BIN";
    if require_spanner_env == Some("1") {
        panic!("{test_name}: JJ_CC_REQUIRE_SPANNER=1 but no Cloud Spanner Emulator found; {hint}");
    }
    // Write to the stderr handle: the test harness captures `eprintln!` of passing tests.
    let _ = writeln!(
        std::io::stderr(),
        "SKIPPED {test_name}: no Cloud Spanner Emulator found; {hint}, or set \
         JJ_CC_REQUIRE_SPANNER=1 to fail instead of skipping"
    );
}

/// Struct to hold the jj-cc-server instance and the temporary test directory where changes are made
/// to the working directory for cli integration tests
pub struct TestWorkspace {
    server: ServerGuard,
    temp_dir: tempfile::TempDir,
    _db_dir: Option<tempfile::TempDir>,
    db_path: Option<PathBuf>,
}

impl TestWorkspace {
    /// Spawns a new dynamic-port `jj-cc-server` instance and initializes a temporary
    /// Commit Cloud repository workspace using `jj cc init`.
    pub async fn init() -> Self {
        let server = spawn_server().await;
        Self::init_against_server(server).await
    }

    pub async fn init_sqlite() -> Self {
        let db_dir = tempfile::tempdir()
            .expect("temporary directory should have been created for sqlite db");
        let db_path = db_dir.path().join("commit_cloud.db");
        let mut ws = Self::init_sqlite_at(&db_path).await;
        ws._db_dir = Some(db_dir);
        ws
    }

    pub async fn init_sqlite_at(db_path: &Path) -> Self {
        let server = spawn_sqlite_server(db_path).await;
        let mut ws = Self::init_against_server(server).await;
        ws.db_path = Some(db_path.to_path_buf());
        ws
    }

    pub async fn init_against_server(server: ServerGuard) -> Self {
        let temp_dir =
            tempfile::tempdir().expect("temporary directory should have been created for testing");
        let repo_path = temp_dir.path();

        let mut init_cmd = server.jj_cmd();

        init_cmd
            .current_dir(repo_path)
            .env("JJ_USER", "Test User")
            .env("JJ_EMAIL", "test.user@example.com")
            .args([
                "cc",
                "init",
                "--server",
                server.url(),
                "--project-id",
                "test-project",
                "--create",
                ".",
            ]);

        init_cmd.assert().success();

        TestWorkspace {
            server,
            temp_dir,
            _db_dir: None,
            db_path: None,
        }
    }

    pub fn db_path(&self) -> &Path {
        self.db_path
            .as_deref()
            .expect("db_path should only be called on workspaces initialized with SQLite")
    }

    pub fn repo_path(&self) -> &Path {
        self.temp_dir.path()
    }

    pub fn server_url(&self) -> &str {
        self.server.url()
    }

    /// The workspace's server; its `jj_cmd` runs `jj` outside the workspace
    /// directory with the same daemon sandbox.
    pub fn server(&self) -> &ServerGuard {
        &self.server
    }

    pub fn jj_cmd(&self) -> assert_cmd::Command {
        let mut cmd = self.server.jj_cmd();
        cmd.current_dir(self.repo_path());
        cmd.env("JJ_USER", "Test User");
        cmd.env("JJ_EMAIL", "test.user@example.com");
        cmd
    }

    pub async fn mount_vfs(&self) -> VfsMountGuard {
        spawn_vfs_mount(self.repo_path()).await
    }
}

pub struct VfsMountGuard {
    child: Option<Child>,
    mount_dir: tempfile::TempDir,
}

impl VfsMountGuard {
    pub fn mountpoint(&self) -> &Path {
        self.mount_dir.path()
    }
}

impl Drop for VfsMountGuard {
    fn drop(&mut self) {
        #[cfg(target_os = "linux")]
        {
            let unmounted = std::process::Command::new("fusermount3")
                .arg("-u")
                .arg(self.mount_dir.path())
                .status()
                .is_ok_and(|s| s.success());
            if !unmounted {
                let _ = std::process::Command::new("fusermount")
                    .arg("-u")
                    .arg(self.mount_dir.path())
                    .status();
            }
        }
        if let Some(mut child) = self.child.take() {
            let _ = child.start_kill();
            for _ in 0..20 {
                if matches!(child.try_wait(), Ok(Some(_)) | Err(_)) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
}

/// Locates the `jjfsd` FUSE daemon, which is built from the separate jj-vfs
/// repository and is not part of this workspace.
fn find_jjfsd_bin() -> PathBuf {
    if let Some(path) = std::env::var_os("JJFSD_BIN") {
        return PathBuf::from(path);
    }
    let exe_name = format!("jjfsd{}", std::env::consts::EXE_SUFFIX);
    if let Some(path_var) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&path_var) {
            let candidate = dir.join(&exe_name);
            if candidate.is_file() {
                return candidate;
            }
        }
    }
    target_profile_dir().unwrap_or_default().join(exe_name)
}

pub async fn spawn_vfs_mount(workspace_path: &Path) -> VfsMountGuard {
    let mount_dir = tempfile::tempdir().expect("Failed to create tempdir for vfs mount");
    let vfs_bin = find_jjfsd_bin();
    let mut cmd = Command::new(vfs_bin);
    cmd.arg(mount_dir.path());
    cmd.arg(workspace_path);
    cmd.env("JJ_USER", "Test User");
    cmd.env("JJ_EMAIL", "test.user@example.com");
    let mut child = cmd.spawn().expect("Failed to spawn jjfsd");

    let commits_dir = mount_dir.path().join("commits");
    let mut ready = false;
    for _ in 0..50 {
        if commits_dir.exists() {
            ready = true;
            break;
        }
        if let Ok(Some(status)) = child.try_wait() {
            panic!("jjfsd exited prematurely with status: {:?}", status);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(ready, "VFS mount point did not become ready in time");

    VfsMountGuard {
        child: Some(child),
        mount_dir,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "JJ_CC_REQUIRE_SPANNER=1")]
    fn require_spanner_env_fails_when_emulator_is_missing() {
        on_missing_spanner_emulator("sample_spanner_test", Some("1"));
    }

    #[test]
    fn unset_require_spanner_env_skips_without_panicking() {
        on_missing_spanner_emulator("sample_spanner_test", None);
        on_missing_spanner_emulator("sample_spanner_test", Some("0"));
    }

    #[test]
    fn allocate_ephemeral_port_pair_returns_distinct_ports() {
        let (grpc_port, http_port) = allocate_ephemeral_port_pair();
        assert_ne!(grpc_port, 0);
        assert_ne!(http_port, 0);
        assert_ne!(grpc_port, http_port);
    }

    #[test]
    fn resolve_external_spanner_hosts_trims_whitespace_scheme_and_trailing_slashes() {
        assert_eq!(
            resolve_external_spanner_hosts(Some("  http://127.0.0.1:9010/  "), None),
            Some(("127.0.0.1:9010".to_string(), "127.0.0.1:9020".to_string(),))
        );
        assert_eq!(
            resolve_external_spanner_hosts(
                Some(" http://spanner-emu:9010/// "),
                Some("  http://spanner-emu:9025/  "),
            ),
            Some((
                "spanner-emu:9010".to_string(),
                "spanner-emu:9025".to_string(),
            ))
        );
        assert_eq!(
            resolve_external_spanner_hosts(Some("   "), Some("http://127.0.0.1:9020")),
            None
        );
        assert_eq!(resolve_external_spanner_hosts(Some("http:///"), None), None);
    }

    #[test]
    fn fault_injection_prefers_local_binaries_and_falls_back_to_external_host() {
        let bins = (
            PathBuf::from("/opt/spanner/gateway_main"),
            PathBuf::from("/opt/spanner/emulator_main"),
        );
        let external = ("127.0.0.1:9010".to_string(), "127.0.0.1:9020".to_string());

        assert_eq!(
            plan_spanner_emulator(true, Some(external.clone()), Some(bins.clone())),
            Some(SpannerEmulatorPlan::SpawnLocal {
                gateway_bin: bins.0.clone(),
                grpc_bin: bins.1.clone(),
                enable_fault_injection: true,
            })
        );
        assert_eq!(
            plan_spanner_emulator(true, Some(external.clone()), None),
            Some(SpannerEmulatorPlan::AttachExternal {
                grpc_host_port: external.0.clone(),
                rest_host_port: external.1.clone(),
                fault_injection_fallback: true,
            })
        );
        assert_eq!(
            plan_spanner_emulator(false, Some(external.clone()), Some(bins)),
            Some(SpannerEmulatorPlan::AttachExternal {
                grpc_host_port: external.0,
                rest_host_port: external.1,
                fault_injection_fallback: false,
            })
        );
        assert_eq!(plan_spanner_emulator(true, None, None), None);
    }

    #[tokio::test]
    async fn fault_injection_external_fallback_attaches_without_local_binaries() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rest_addr = listener.local_addr().unwrap().to_string();
        let mock_rest = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 2048];
            let n = stream.read(&mut buf).await.unwrap();
            let req = String::from_utf8_lossy(&buf[..n]).into_owned();
            let resp =
                "HTTP/1.0 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}";
            stream.write_all(resp.as_bytes()).await.unwrap();
            req
        });

        let plan = plan_spanner_emulator(
            true,
            Some(("127.0.0.1:19010".to_string(), rest_addr.clone())),
            None,
        )
        .expect("fault injection should fall back to external SPANNER_EMULATOR_HOST");
        let guard = SpannerEmulatorGuard::start_with_plan(plan)
            .await
            .expect("external fallback should attach cleanly");
        assert!(guard.child.is_none());
        assert_eq!(guard.grpc_host_port(), "127.0.0.1:19010");
        assert_eq!(guard.rest_endpoint(), format!("http://{rest_addr}"));

        let received = mock_rest.await.unwrap();
        assert!(received.starts_with("POST /v1/projects/test-project/instances HTTP/1.0\r\n"));
        assert!(received.contains("Connection: close\r\n"));
    }

    #[test]
    fn sanitized_server_env_vars_includes_spanner_emulator() {
        assert!(SANITIZED_SERVER_ENV_VARS.contains(&"SPANNER_EMULATOR"));
    }

    #[tokio::test]
    async fn send_http_json_times_out_on_hung_port() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let stall = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let err = send_http_json_with_timeout(
            &addr,
            "GET",
            "/v1/projects/test-project/instances",
            None,
            Duration::from_millis(50),
        )
        .await
        .expect_err("hung port should time out");
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
        stall.abort();
    }
}
