// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fs;
use std::path::{Path, PathBuf};

use assert_cmd::Command;
use cc_lib::daemon_client::read_daemon_stats;
use cc_lib::util::CommitCloudConfig;

fn jj_daemon_cmd(cwd: &Path, xdg: &Path, daemon_dir: &Path, cache_dir: &Path) -> Command {
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("jj");
    cmd.current_dir(cwd)
        .env("XDG_CONFIG_HOME", xdg)
        .env("JJ_CC_DAEMON_DIR", daemon_dir)
        .env("JJ_CC_CACHE_DIR", cache_dir)
        .env("JJ_CC_DAEMON_CONFIG", xdg.join("daemon.toml"))
        .env("JJ_CC_DAEMON_BIN", testutils::daemon_binary_path())
        .env("JJ_CC_DAEMON_IDLE_SECS", "30")
        .env("JJ_USER", "Daemon Tester")
        .env("JJ_EMAIL", "daemon@example.com");
    cmd
}

struct DaemonStopGuard {
    cwd: PathBuf,
    xdg: PathBuf,
    daemon_dir: PathBuf,
    cache_dir: PathBuf,
}

impl DaemonStopGuard {
    fn new(cwd: &Path, xdg: &Path, daemon_dir: &Path, cache_dir: &Path) -> Self {
        Self {
            cwd: cwd.to_path_buf(),
            xdg: xdg.to_path_buf(),
            daemon_dir: daemon_dir.to_path_buf(),
            cache_dir: cache_dir.to_path_buf(),
        }
    }
}

impl Drop for DaemonStopGuard {
    fn drop(&mut self) {
        if self.daemon_dir.join("daemon.json").exists() {
            let _ = jj_daemon_cmd(&self.cwd, &self.xdg, &self.daemon_dir, &self.cache_dir)
                .args(["cc", "daemon", "stop"])
                .ok();
        }
    }
}

#[tokio::test]
async fn test_daemon_autospawn_two_tier_cache_exceeds_80_percent_warm_hit_rate_and_persists_l2() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    let daemon_dir = dir.path().join("daemon_state");
    let cache_dir = dir.path().join("daemon_cache");
    fs::create_dir_all(&xdg).unwrap();
    let _daemon_guard = DaemonStopGuard::new(dir.path(), &xdg, &daemon_dir, &cache_dir);

    let server = testutils::spawn_sqlite_server(&dir.path().join("daemon_test.db")).await;

    jj_daemon_cmd(dir.path(), &xdg, &daemon_dir, &cache_dir)
        .args(["cc", "daemon", "enable"])
        .assert()
        .success();

    // Initializing ws1 auto-spawns `jj-cc-daemon` on demand.
    let ws1 = dir.path().join("ws1");
    jj_daemon_cmd(dir.path(), &xdg, &daemon_dir, &cache_dir)
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            ws1.to_str().unwrap(),
        ])
        .assert()
        .success();
    assert!(
        daemon_dir.join("daemon.json").is_file(),
        "jj-cc-daemon should auto-spawn and write daemon.json"
    );
    assert!(
        daemon_dir.join("daemon.log").is_file(),
        "auto-spawned jj-cc-daemon should redirect logs to daemon.log"
    );

    fs::create_dir_all(ws1.join("src")).unwrap();
    fs::write(ws1.join("src/main.rs"), "fn main() { println!(\"v1\"); }\n").unwrap();
    fs::write(ws1.join("src/util.rs"), "pub fn helper() -> usize { 42 }\n").unwrap();
    let payload: Vec<u8> = (0..(256 * 1024)).map(|i| (i % 251) as u8).collect();
    fs::write(ws1.join("src/data.bin"), &payload).unwrap();

    jj_daemon_cmd(&ws1, &xdg, &daemon_dir, &cache_dir)
        .args(["commit", "-m", "initial commit via daemon"])
        .assert()
        .success();

    let cfg = CommitCloudConfig::load_from_store(&ws1).unwrap();

    // Clone ws2 on the same host and run repeated reads to warm the daemon cache.
    let ws2 = dir.path().join("ws2");
    jj_daemon_cmd(dir.path(), &xdg, &daemon_dir, &cache_dir)
        .args([
            "cc",
            "clone",
            "--server",
            server.url(),
            &cfg.repo_id,
            ws2.to_str().unwrap(),
        ])
        .assert()
        .success();
    assert_eq!(fs::read(ws2.join("src/data.bin")).unwrap(), payload);

    for _ in 0..4 {
        jj_daemon_cmd(&ws2, &xdg, &daemon_dir, &cache_dir)
            .args(["log", "-r", "::@", "-T", "commit_id ++ \"\\n\""])
            .assert()
            .success();
        jj_daemon_cmd(&ws2, &xdg, &daemon_dir, &cache_dir)
            .args(["diff", "-r", "@-"])
            .assert()
            .success();
        jj_daemon_cmd(&ws2, &xdg, &daemon_dir, &cache_dir)
            .args(["file", "show", "-r", "@-", "src/main.rs"])
            .assert()
            .success();
    }

    let status_out = String::from_utf8(
        jj_daemon_cmd(&ws2, &xdg, &daemon_dir, &cache_dir)
            .args(["cc", "daemon", "status"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap();
    assert!(status_out.contains("RUNNING"));

    // Stopping the daemon flushes final stats; restarting it starts with a cold L1 RAM
    // cache over the surviving L2 disk CAS so subsequent reads exercise L2 hits.
    jj_daemon_cmd(dir.path(), &xdg, &daemon_dir, &cache_dir)
        .args(["cc", "daemon", "stop"])
        .assert()
        .success();
    assert!(!daemon_dir.join("daemon.json").exists());

    let stats = read_daemon_stats(&daemon_dir).expect("daemon.stats.json should exist");
    assert!(
        stats.hit_ratio_percent() > 80.0,
        "expected >80% warm cache hit ratio (C1), got {:.1}% ({stats:?})",
        stats.hit_ratio_percent()
    );

    jj_daemon_cmd(dir.path(), &xdg, &daemon_dir, &cache_dir)
        .args(["cc", "daemon", "start", "--idle-timeout-secs", "30"])
        .assert()
        .success();
    jj_daemon_cmd(&ws2, &xdg, &daemon_dir, &cache_dir)
        .args(["diff", "-r", "@-"])
        .assert()
        .success();

    jj_daemon_cmd(dir.path(), &xdg, &daemon_dir, &cache_dir)
        .args(["cc", "daemon", "stop"])
        .assert()
        .success();

    let restarted_stats =
        read_daemon_stats(&daemon_dir).expect("restarted daemon.stats.json should exist");
    assert!(
        restarted_stats.l2_hits > 0,
        "restarted daemon should serve warm objects from surviving L2 disk CAS ({restarted_stats:?})"
    );
}

#[tokio::test]
async fn test_daemon_preserves_repo_path_component_sort_order_and_isolates_unrelated_projects() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    let daemon_dir = dir.path().join("daemon_state");
    let cache_dir = dir.path().join("daemon_cache");
    fs::create_dir_all(&xdg).unwrap();
    let _daemon_guard = DaemonStopGuard::new(dir.path(), &xdg, &daemon_dir, &cache_dir);

    let server = testutils::spawn_sqlite_server(&dir.path().join("daemon_sort_iso.db")).await;

    jj_daemon_cmd(dir.path(), &xdg, &daemon_dir, &cache_dir)
        .args(["cc", "daemon", "enable"])
        .assert()
        .success();

    let ws_a = dir.path().join("ws_a");
    jj_daemon_cmd(dir.path(), &xdg, &daemon_dir, &cache_dir)
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--project-id",
            "proj-alpha",
            "--create",
            ws_a.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Regression: directory `foo/` coexisting with `foo-bar.rs` ('-' = 45) and `foo.rs` ('.' =
    // 46). Git hashes sort `"foo-bar.rs" < "foo.rs" < "foo/"`, whereas
    // `jj_lib::backend::Tree::from_sorted_entries` requires strict UTF-8 component order `"foo" <
    // "foo-bar.rs" < "foo.rs"`.
    fs::create_dir_all(ws_a.join("src/foo")).unwrap();
    fs::write(ws_a.join("src/foo/mod.rs"), "pub fn sub() {}\n").unwrap();
    fs::write(ws_a.join("src/foo-bar.rs"), "pub fn dash() {}\n").unwrap();
    fs::write(ws_a.join("src/foo.rs"), "pub fn dot() {}\n").unwrap();

    jj_daemon_cmd(&ws_a, &xdg, &daemon_dir, &cache_dir)
        .args(["commit", "-m", "tree sort collision via daemon"])
        .assert()
        .success();

    // Reading back from the daemon's write-through cache must preserve `RepoPathComponent` sort
    // order.
    for rel in ["src/foo/mod.rs", "src/foo-bar.rs", "src/foo.rs"] {
        jj_daemon_cmd(&ws_a, &xdg, &daemon_dir, &cache_dir)
            .args(["file", "show", "-r", "@-", rel])
            .assert()
            .success();
    }
    jj_daemon_cmd(&ws_a, &xdg, &daemon_dir, &cache_dir)
        .args(["diff", "-r", "@-"])
        .assert()
        .success();

    // Regression: an unrelated project (`proj-beta`) sharing the same local `jj-cc-daemon`
    // must NOT be able to read `proj-alpha`'s cached file blobs.
    let ws_b = dir.path().join("ws_b");
    jj_daemon_cmd(dir.path(), &xdg, &daemon_dir, &cache_dir)
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--project-id",
            "proj-beta",
            "--create",
            ws_b.to_str().unwrap(),
        ])
        .assert()
        .success();

    let cfg_a = CommitCloudConfig::load_from_store(&ws_a).unwrap();
    let cfg_b = CommitCloudConfig::load_from_store(&ws_b).unwrap();
    let commit_hex = String::from_utf8(
        jj_daemon_cmd(&ws_a, &xdg, &daemon_dir, &cache_dir)
            .args(["log", "--no-graph", "-r", "@-", "-T", "commit_id"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap()
    .trim()
    .to_string();
    let commit_bytes = hex::decode(commit_hex).unwrap();

    let (_, channel) = cc_lib::daemon_client::connect_existing_daemon(&daemon_dir)
        .await
        .unwrap()
        .expect("daemon should be listening");

    let mut daemon_backend =
        cc_common::backend::backend_service_client::BackendServiceClient::new(channel);
    let mut read_commit_req = tonic::Request::new(cc_common::backend::ReadCommitRequest {
        project_id: cfg_a.effective_project_id().to_string(),
        commit_id: commit_bytes,
    });
    read_commit_req
        .metadata_mut()
        .insert("x-jj-cc-upstream", server.url().parse().unwrap());
    let root_tree_id = daemon_backend
        .read_commit(read_commit_req)
        .await
        .unwrap()
        .into_inner()
        .commit
        .unwrap()
        .root_tree_id[0]
        .clone();

    let mut read_root_req = tonic::Request::new(cc_common::backend::ReadTreeRequest {
        project_id: cfg_a.effective_project_id().to_string(),
        path: "src".to_string(),
        tree_id: root_tree_id,
        page_size: 0,
        page_token: String::new(),
    });
    read_root_req
        .metadata_mut()
        .insert("x-jj-cc-upstream", server.url().parse().unwrap());
    let root_entries = daemon_backend
        .read_tree(read_root_req)
        .await
        .unwrap()
        .into_inner()
        .entries;
    let src_tree_id = root_entries
        .iter()
        .find(|e| e.name == "src")
        .and_then(|e| e.value.as_ref())
        .and_then(|v| match v.value.as_ref() {
            Some(cc_common::backend::tree_value::Value::TreeId(id)) => Some(id.clone()),
            _ => None,
        })
        .expect("src tree entry");

    let mut read_src_req = tonic::Request::new(cc_common::backend::ReadTreeRequest {
        project_id: cfg_a.effective_project_id().to_string(),
        path: "src".to_string(),
        tree_id: src_tree_id,
        page_size: 0,
        page_token: String::new(),
    });
    read_src_req
        .metadata_mut()
        .insert("x-jj-cc-upstream", server.url().parse().unwrap());
    let src_entries = daemon_backend
        .read_tree(read_src_req)
        .await
        .unwrap()
        .into_inner()
        .entries;
    let src_names: Vec<&str> = src_entries.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(src_names, vec!["foo", "foo-bar.rs", "foo.rs"]);

    let foo_rs_file = src_entries
        .iter()
        .find(|e| e.name == "foo.rs")
        .and_then(|e| e.value.as_ref())
        .and_then(|v| match v.value.as_ref() {
            Some(cc_common::backend::tree_value::Value::File(f)) => Some(f.clone()),
            _ => None,
        })
        .expect("foo.rs file entry");
    assert_eq!(
        foo_rs_file.size_bytes,
        Some("pub fn dot() {}\n".len() as u64)
    );

    // Probe `foo_rs_file.id` through the daemon using `cfg_b`'s project (`proj-beta`): must be
    // NOT_FOUND.
    let mut unauthorized_read = tonic::Request::new(cc_common::backend::ReadFileRequest {
        project_id: cfg_b.effective_project_id().to_string(),
        file_id: foo_rs_file.id,
        offset: 0,
        max_bytes: 0,
    });
    unauthorized_read
        .metadata_mut()
        .insert("x-jj-cc-upstream", server.url().parse().unwrap());
    let err = daemon_backend
        .read_file(unauthorized_read)
        .await
        .expect_err("proj-beta must not read proj-alpha's cached blob from daemon");
    assert_eq!(err.code(), tonic::Code::NotFound);

    jj_daemon_cmd(dir.path(), &xdg, &daemon_dir, &cache_dir)
        .args(["cc", "daemon", "stop"])
        .assert()
        .success();
}

#[cfg(unix)]
#[tokio::test]
async fn test_daemon_stop_does_not_signal_unrelated_process_from_stale_daemon_json() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    let daemon_dir = dir.path().join("daemon_state");
    let cache_dir = dir.path().join("daemon_cache");
    fs::create_dir_all(&xdg).unwrap();
    cc_lib::daemon_client::ensure_private_state_dir(&daemon_dir).unwrap();

    let mut innocent = std::process::Command::new("sleep")
        .arg("60")
        .spawn()
        .unwrap();
    let stale_json = format!(
        r#"{{"pid":{},"socket_path":"{}","tcp_addr":null,"cache_dir":"{}","stats_path":"{}","default_upstream":null,"started_unix_secs":1}}"#,
        innocent.id(),
        daemon_dir.join("daemon.sock").display(),
        cache_dir.display(),
        daemon_dir.join("daemon.stats.json").display(),
    );
    fs::write(daemon_dir.join("daemon.json"), stale_json).unwrap();

    jj_daemon_cmd(dir.path(), &xdg, &daemon_dir, &cache_dir)
        .args(["cc", "daemon", "stop"])
        .assert()
        .success();

    assert!(
        innocent.try_wait().unwrap().is_none(),
        "jj cc daemon stop must never kill a recycled PID when the daemon socket is dead"
    );
    let _ = innocent.kill();
    let _ = innocent.wait();
    assert!(
        !daemon_dir.join("daemon.json").exists(),
        "stale daemon.json should be cleaned up"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn test_daemon_falls_back_to_direct_connection_when_state_dir_mode_is_unsafe() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    let daemon_dir = dir.path().join("insecure_state");
    let cache_dir = dir.path().join("daemon_cache");
    fs::create_dir_all(&xdg).unwrap();
    fs::create_dir_all(&daemon_dir).unwrap();
    fs::set_permissions(&daemon_dir, fs::Permissions::from_mode(0o755)).unwrap();

    let server = testutils::spawn_sqlite_server(&dir.path().join("daemon_fallback.db")).await;

    jj_daemon_cmd(dir.path(), &xdg, &daemon_dir, &cache_dir)
        .args(["cc", "daemon", "enable"])
        .assert()
        .success();

    let ws = dir.path().join("ws");
    jj_daemon_cmd(dir.path(), &xdg, &daemon_dir, &cache_dir)
        .args(["cc", "init", "--server", server.url(), ws.to_str().unwrap()])
        .assert()
        .success();
    assert!(
        !daemon_dir.join("daemon.json").exists(),
        "daemon mode must be refused on 0755 state_dir while commands fall back directly"
    );
}

#[tokio::test]
async fn test_daemon_rejects_non_loopback_listen_and_authenticates_tcp_with_secret() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    let daemon_dir = dir.path().join("daemon_state");
    let cache_dir = dir.path().join("daemon_cache");
    fs::create_dir_all(&xdg).unwrap();
    let _daemon_guard = DaemonStopGuard::new(dir.path(), &xdg, &daemon_dir, &cache_dir);

    jj_daemon_cmd(dir.path(), &xdg, &daemon_dir, &cache_dir)
        .args(["cc", "daemon", "start", "--listen", "0.0.0.0:0"])
        .assert()
        .failure();

    let server = testutils::spawn_sqlite_server(&dir.path().join("daemon_tcp.db")).await;

    jj_daemon_cmd(dir.path(), &xdg, &daemon_dir, &cache_dir)
        .args(["cc", "daemon", "enable"])
        .assert()
        .success();

    jj_daemon_cmd(dir.path(), &xdg, &daemon_dir, &cache_dir)
        .args([
            "cc",
            "daemon",
            "start",
            "--listen",
            "127.0.0.1:0",
            "--idle-timeout-secs",
            "30",
        ])
        .assert()
        .success();

    let info = cc_lib::daemon_client::read_daemon_runtime_info(&daemon_dir).expect("daemon.json");
    let secret = info.secret.clone().expect("secret");
    assert!(!secret.is_empty());
    let tcp_url = info.tcp_addr.clone().expect("tcp_addr");

    // Unauthenticated TCP callers must be rejected.
    let unauth_ch = tonic::transport::Endpoint::from_shared(tcp_url.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut unauth_client =
        cc_common::backend::backend_service_client::BackendServiceClient::new(unauth_ch);
    let unauth_err = unauth_client
        .read_tree(tonic::Request::new(cc_common::backend::ReadTreeRequest {
            project_id: "r".to_string(),
            path: String::new(),
            tree_id: cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
            page_size: 0,
            page_token: String::new(),
        }))
        .await
        .unwrap_err();
    assert_eq!(unauth_err.code(), tonic::Code::Unauthenticated);

    // Force `cc-lib` onto the TCP path by clearing `socket_path` in `daemon.json`; `cc-lib` must
    // attach `x-jj-cc-daemon-secret` automatically.
    let raw_json = fs::read_to_string(daemon_dir.join("daemon.json")).unwrap();
    let tcp_only_json = format!(
        r#"{{"pid":{},"socket_path":null,"tcp_addr":"{tcp_url}","cache_dir":"{}","stats_path":"{}","default_upstream":null,"started_unix_secs":{},"secret":"{secret}"}}"#,
        info.pid, info.cache_dir, info.stats_path, info.started_unix_secs
    );
    fs::write(daemon_dir.join("daemon.json"), tcp_only_json).unwrap();

    let ws = dir.path().join("ws_tcp");
    jj_daemon_cmd(dir.path(), &xdg, &daemon_dir, &cache_dir)
        .args(["cc", "init", "--server", server.url(), ws.to_str().unwrap()])
        .assert()
        .success();
    fs::write(ws.join("hello.txt"), "tcp secret ok\n").unwrap();
    jj_daemon_cmd(&ws, &xdg, &daemon_dir, &cache_dir)
        .args(["commit", "-m", "commit over authenticated daemon tcp"])
        .assert()
        .success();

    fs::write(daemon_dir.join("daemon.json"), raw_json).unwrap();
    jj_daemon_cmd(dir.path(), &xdg, &daemon_dir, &cache_dir)
        .args(["cc", "daemon", "stop"])
        .assert()
        .success();
}

#[test]
fn test_daemon_rejects_legacy_flag_combined_with_subcommand() {
    let dir = tempfile::tempdir().expect("tempdir");
    let xdg = dir.path().join("xdg");
    let daemon_dir = dir.path().join("daemon_state");
    let cache_dir = dir.path().join("daemon_cache");
    fs::create_dir_all(&xdg).unwrap();

    let out = jj_daemon_cmd(dir.path(), &xdg, &daemon_dir, &cache_dir)
        .args(["cc", "daemon", "--enable", "stop"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("Cannot combine legacy flags"),
        "expected user_error for legacy flag + subcommand, got: {stderr}"
    );
}
