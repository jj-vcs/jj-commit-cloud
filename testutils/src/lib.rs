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

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

pub struct ServerGuard {
    child: Child,
    url: String,
}

impl ServerGuard {
    pub fn url(&self) -> &str {
        &self.url
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
    "JJ_CC_SERVER_AUTH_TOKEN",
    "JJ_CC_SERVER_AUTH_TOKEN_FILE",
    "JJ_CC_ALLOW_UNAUTHENTICATED",
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
        .stderr(Stdio::inherit())
        .spawn()
        .expect("The jj-cc-server process should have spawned");

    let stdout = child
        .stdout
        .take()
        .expect("The stdout pipe should have been captured from the child process");

    let mut reader = BufReader::new(stdout).lines();

    let timeout = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(timeout);

    let server_addr = loop {
        tokio::select! {
            line_result = reader.next_line() => {
                match line_result {
                    Ok(Some(line)) => {
                        if let Some(addr) = extract_listening_address(&line) {
                            break addr;
                        }
                    }
                    Ok(None) => panic!("The stdout pipe should not have closed before the server listening address was printed"),
                    Err(e) => panic!("A line from the server stdout should have been read: {:?}", e),
                }
            }
            _ = &mut timeout => {
                panic!("The server listening address should have appeared in stdout before timeout");
            }
        }
    };

    // Keep draining stdout in the background so the child process never blocks on a full pipe.
    tokio::spawn(async move { while let Ok(Some(_)) = reader.next_line().await {} });

    let url = format!("http://{}", server_addr);

    ServerGuard { child, url }
}

pub async fn spawn_sqlite_server(db_path: &Path) -> ServerGuard {
    let sqlite_arg = format!("--sqlite-path={}", db_path.display());
    spawn_server_with_args(&["--store-type=sqlite", &sqlite_arg]).await
}

/// Struct to hold the jj-cc-server instance and the temporary test directory where changes are made
/// to the working directory for cli integration tests
pub struct TestWorkspace {
    server: ServerGuard,
    temp_dir: tempfile::TempDir,
}

impl TestWorkspace {
    /// Spawns a new dynamic-port `jj-cc-server` instance and initializes a temporary
    /// Commit Cloud repository workspace using `jj cc init`.
    pub async fn init() -> Self {
        let server = spawn_server().await;
        Self::init_against_server(server).await
    }

    pub async fn init_sqlite(db_path: &Path) -> Self {
        let server = spawn_sqlite_server(db_path).await;
        Self::init_against_server(server).await
    }

    pub async fn init_against_server(server: ServerGuard) -> Self {
        let temp_dir =
            tempfile::tempdir().expect("temporary directory should have been created for testing");
        let repo_path = temp_dir.path();

        let mut init_cmd =
            assert_cmd::Command::cargo_bin("jj").expect("The jj CLI binary should have compiled");

        init_cmd.current_dir(repo_path).args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--create",
            ".",
        ]);

        init_cmd.assert().success();

        TestWorkspace { server, temp_dir }
    }

    pub fn repo_path(&self) -> &Path {
        self.temp_dir.path()
    }

    pub fn server_url(&self) -> &str {
        self.server.url()
    }

    pub fn jj_cmd(&self) -> assert_cmd::Command {
        let mut cmd =
            assert_cmd::Command::cargo_bin("jj").expect("The jj CLI binary should have compiled");
        cmd.current_dir(self.repo_path());
        cmd
    }
}
