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

//! End-to-end `jj cc` smoke test suite executed against local, Kubernetes, or GCP environments.
//!
//! # Cross-Process and RPC Boundaries
//! Invokes isolated `jj` subprocesses (`JJ_CONFIG=""`, `XDG_CONFIG_HOME` in a temporary directory,
//! and `JJ_CC_USE_DAEMON=0`) to exercise init, streamed >4 MiB blob uploads, cross-workspace clones,
//! historical checkouts, short op prefixes, Git export/re-import parity, and peer-region reads.

use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::local::resolve_bearer_token;
use crate::state::{AuthMode, BoxError, cargo_target_dir, load_state, workspace_root};

pub struct SmokeOptions {
    pub env_name: String,
    pub server_override: Option<String>,
    pub peer_server_override: Option<String>,
    pub jj_bin_override: Option<PathBuf>,
    pub keep_workspaces: bool,
}

pub fn resolve_jj_bin_override(path: &Path) -> Result<PathBuf, BoxError> {
    if path.is_absolute() || path.components().count() <= 1 {
        return Ok(path.to_path_buf());
    }
    Ok(std::env::current_dir()?.join(path))
}

fn short_id(id: &str) -> &str {
    id.get(..12).unwrap_or(id)
}

pub(crate) fn normalize_optional_url(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
}

#[derive(Debug, PartialEq, Eq)]
struct SmokeTarget {
    primary_url: String,
    peer_url: Option<String>,
    auth_mode: AuthMode,
    server_token: Option<String>,
    mode_label: String,
}

fn resolve_smoke_target(root: &Path, opts: &SmokeOptions) -> Result<SmokeTarget, BoxError> {
    let server_override = normalize_optional_url(opts.server_override.as_deref());
    let peer_server_override = normalize_optional_url(opts.peer_server_override.as_deref());
    if let Some(explicit_server) = server_override {
        return Ok(SmokeTarget {
            primary_url: explicit_server,
            peer_url: peer_server_override,
            auth_mode: AuthMode::None,
            server_token: None,
            mode_label: "explicit".to_string(),
        });
    }
    let state = load_state(root, &opts.env_name)?.ok_or_else(|| {
        format!(
            "no active environment state found in .jj-cc for '{}' (start a server first with `cargo xtask up <mode>`)",
            opts.env_name
        )
    })?;
    if state.compute_stopped {
        return Err(format!(
            "environment '{}' compute is stopped (only retained cloud storage is recorded); start a server first with `cargo xtask up <mode>`",
            opts.env_name
        )
        .into());
    }
    Ok(SmokeTarget {
        primary_url: state.endpoints.primary,
        peer_url: peer_server_override.or(state.endpoints.peer),
        auth_mode: state.auth,
        server_token: state.auth_token,
        mode_label: format!("{} ({})", state.mode, state.runtime),
    })
}

pub fn run_smoke(opts: &SmokeOptions) -> Result<(), BoxError> {
    let root = workspace_root();
    let SmokeTarget {
        primary_url,
        peer_url,
        auth_mode,
        server_token,
        mode_label,
    } = resolve_smoke_target(&root, opts)?;

    let auth_token = resolve_bearer_token(auth_mode, server_token.as_deref())?;

    let jj_bin = match &opts.jj_bin_override {
        Some(p) => resolve_jj_bin_override(p)?,
        None => ensure_host_jj_binary(&root)?,
    };

    let temp_holder = tempfile::Builder::new().prefix("jj-cc-smoke-").tempdir()?;
    let base_dir = if opts.keep_workspaces {
        temp_holder.keep()
    } else {
        temp_holder.path().to_path_buf()
    };

    let ws_writer = base_dir.join("ws-writer");
    let ws_reader = base_dir.join("ws-reader");
    let xdg_cfg = base_dir.join("xdg-config");
    fs::create_dir_all(&xdg_cfg)?;
    let run_jj = |jj_bin: &Path, auth_token: Option<&str>, args: &[&OsStr]| {
        run_jj_isolated(jj_bin, &xdg_cfg, auth_token, args)
    };
    let checkout_url = peer_url.as_deref().unwrap_or(&primary_url);

    println!("==> Smoke test against {mode_label}");
    println!("    Writer endpoint:   {primary_url}");
    if peer_url.is_some() {
        println!("    Checkout endpoint: {checkout_url} (cross-region replica)");
    }
    let steps = if peer_url.is_some() { 7 } else { 6 };

    let init_out = run_jj(
        &jj_bin,
        auth_token.as_deref(),
        &[
            OsStr::new("cc"),
            OsStr::new("init"),
            OsStr::new("--server"),
            OsStr::new(&primary_url),
            OsStr::new("--create"),
            ws_writer.as_os_str(),
        ],
    )?;
    let repo_id = extract_repo_id(&ws_writer, &init_out)?;
    println!("    [1/{steps}] Initialized remote repository {repo_id}");

    fs::create_dir_all(ws_writer.join("src"))?;
    let v1_code = "pub fn version() -> &'static str { \"v1.0.0\" }\n";
    fs::write(ws_writer.join("src/lib.rs"), v1_code)?;
    run_jj(
        &jj_bin,
        auth_token.as_deref(),
        &[
            OsStr::new("-R"),
            ws_writer.as_os_str(),
            OsStr::new("describe"),
            OsStr::new("-m"),
            OsStr::new("release v1.0.0"),
        ],
    )?;
    let v1_commit = run_jj(
        &jj_bin,
        auth_token.as_deref(),
        &[
            OsStr::new("-R"),
            ws_writer.as_os_str(),
            OsStr::new("log"),
            OsStr::new("--no-graph"),
            OsStr::new("-r"),
            OsStr::new("@"),
            OsStr::new("-T"),
            OsStr::new("commit_id"),
        ],
    )?
    .trim()
    .to_string();
    println!(
        "    [2/{steps}] Published v1 commit {}",
        short_id(&v1_commit)
    );

    run_jj(
        &jj_bin,
        auth_token.as_deref(),
        &[OsStr::new("-R"), ws_writer.as_os_str(), OsStr::new("new")],
    )?;
    let v2_code = "pub fn version() -> &'static str { \"v2.0.0\" }\n";
    fs::write(ws_writer.join("src/lib.rs"), v2_code)?;
    fs::create_dir_all(ws_writer.join("assets"))?;
    let large_blob: Vec<u8> = (0..(4 * 1024 * 1024 + 512 * 1024))
        .map(|i| ((i * 29 + 7) & 0xff) as u8)
        .collect();
    fs::write(ws_writer.join("assets/weights.bin"), &large_blob)?;
    #[cfg(unix)]
    std::os::unix::fs::symlink("src/lib.rs", ws_writer.join("entry.rs"))?;

    run_jj(
        &jj_bin,
        auth_token.as_deref(),
        &[
            OsStr::new("-R"),
            ws_writer.as_os_str(),
            OsStr::new("describe"),
            OsStr::new("-m"),
            OsStr::new("release v2.0.0 with streamed asset"),
        ],
    )?;
    let v2_commit = run_jj(
        &jj_bin,
        auth_token.as_deref(),
        &[
            OsStr::new("-R"),
            ws_writer.as_os_str(),
            OsStr::new("log"),
            OsStr::new("--no-graph"),
            OsStr::new("-r"),
            OsStr::new("@"),
            OsStr::new("-T"),
            OsStr::new("commit_id"),
        ],
    )?
    .trim()
    .to_string();
    println!(
        "    [3/{steps}] Published v2 commit {} (4.5 MiB streamed blob)",
        short_id(&v2_commit)
    );

    run_jj(
        &jj_bin,
        auth_token.as_deref(),
        &[
            OsStr::new("cc"),
            OsStr::new("clone"),
            OsStr::new("--server"),
            OsStr::new(checkout_url),
            OsStr::new(&repo_id),
            ws_reader.as_os_str(),
        ],
    )?;
    verify_v2_checkout(&ws_reader, v2_code, &large_blob)?;
    println!("    [4/{steps}] Cloned second workspace and verified v2 files on disk");

    run_jj(
        &jj_bin,
        auth_token.as_deref(),
        &[
            OsStr::new("-R"),
            ws_reader.as_os_str(),
            OsStr::new("new"),
            OsStr::new(&v1_commit),
        ],
    )?;
    let checked_out_v1 = fs::read_to_string(ws_reader.join("src/lib.rs"))?;
    if checked_out_v1 != v1_code {
        return Err(format!("expected historical v1 code, got {checked_out_v1:?}").into());
    }
    if ws_reader.join("assets/weights.bin").exists() {
        return Err("assets/weights.bin should not exist when v1 is checked out".into());
    }

    run_jj(
        &jj_bin,
        auth_token.as_deref(),
        &[
            OsStr::new("-R"),
            ws_reader.as_os_str(),
            OsStr::new("new"),
            OsStr::new(&v2_commit),
        ],
    )?;
    let op_prefix = run_jj(
        &jj_bin,
        auth_token.as_deref(),
        &[
            OsStr::new("-R"),
            ws_reader.as_os_str(),
            OsStr::new("op"),
            OsStr::new("log"),
            OsStr::new("--no-graph"),
            OsStr::new("-n"),
            OsStr::new("1"),
            OsStr::new("-T"),
            OsStr::new("id.short(6)"),
        ],
    )?
    .trim()
    .to_string();
    run_jj(
        &jj_bin,
        auth_token.as_deref(),
        &[
            OsStr::new("-R"),
            ws_reader.as_os_str(),
            OsStr::new("op"),
            OsStr::new("show"),
            OsStr::new(&op_prefix),
            OsStr::new("--no-op-diff"),
        ],
    )?;
    println!(
        "    [5/{steps}] Checked out historical v1 -> v2 and verified short op prefix ({op_prefix})"
    );

    run_jj(
        &jj_bin,
        auth_token.as_deref(),
        &[
            OsStr::new("-R"),
            ws_writer.as_os_str(),
            OsStr::new("bookmark"),
            OsStr::new("set"),
            OsStr::new("main"),
            OsStr::new("-r"),
            OsStr::new(&v2_commit),
        ],
    )?;
    let exported_git = base_dir.join("roundtrip.git");
    run_jj(
        &jj_bin,
        auth_token.as_deref(),
        &[
            OsStr::new("cc"),
            OsStr::new("export-git"),
            OsStr::new("--bare"),
            OsStr::new("--server"),
            OsStr::new(checkout_url),
            OsStr::new("--repo-id"),
            OsStr::new(&repo_id),
            OsStr::new("--git-dir"),
            exported_git.as_os_str(),
        ],
    )?;
    let exported_main = fs::read_to_string(exported_git.join("refs/heads/main"))?
        .trim()
        .to_string();
    if exported_main != v2_commit {
        return Err(format!(
            "Git export SHA-1 mismatch: expected {v2_commit}, got {exported_main}"
        )
        .into());
    }
    let reimported_repo_id = format!("smoke-import-{}", uuid::Uuid::new_v4().simple());
    run_jj(
        &jj_bin,
        auth_token.as_deref(),
        &[
            OsStr::new("cc"),
            OsStr::new("import-git"),
            OsStr::new("--server"),
            OsStr::new(&primary_url),
            OsStr::new("--repo-id"),
            OsStr::new(&reimported_repo_id),
            OsStr::new("--create"),
            OsStr::new("--git-dir"),
            exported_git.as_os_str(),
        ],
    )?;
    let ws_reimported = base_dir.join("ws-reimported");
    run_jj(
        &jj_bin,
        auth_token.as_deref(),
        &[
            OsStr::new("cc"),
            OsStr::new("clone"),
            OsStr::new("--server"),
            OsStr::new(&primary_url),
            OsStr::new(&reimported_repo_id),
            ws_reimported.as_os_str(),
        ],
    )?;
    let imported_main = run_jj(
        &jj_bin,
        auth_token.as_deref(),
        &[
            OsStr::new("-R"),
            ws_reimported.as_os_str(),
            OsStr::new("log"),
            OsStr::new("--no-graph"),
            OsStr::new("-r"),
            OsStr::new("main"),
            OsStr::new("-T"),
            OsStr::new("commit_id"),
        ],
    )?
    .trim()
    .to_string();
    if imported_main != v2_commit {
        return Err(format!(
            "Git re-import SHA-1 mismatch: expected {v2_commit}, got {imported_main}"
        )
        .into());
    }
    run_jj(
        &jj_bin,
        auth_token.as_deref(),
        &[
            OsStr::new("-R"),
            ws_reimported.as_os_str(),
            OsStr::new("new"),
            OsStr::new("main"),
        ],
    )?;
    verify_v2_checkout(&ws_reimported, v2_code, &large_blob)?;
    println!(
        "    [6/{steps}] Verified 1:1 Git SHA-1 export, re-import, and cloned tree parity ({})",
        short_id(&exported_main)
    );

    if peer_url.is_some() {
        // Reverse-direction check: publish a new blob on the peer server and verify checkout on
        // the primary server reads it via peer origin fallback.
        let v3_code = "pub fn peer_region() -> &'static str { \"authored-in-peer\" }\n";
        fs::write(ws_reader.join("src/peer.rs"), v3_code)?;
        run_jj(
            &jj_bin,
            auth_token.as_deref(),
            &[
                OsStr::new("-R"),
                ws_reader.as_os_str(),
                OsStr::new("describe"),
                OsStr::new("-m"),
                OsStr::new("peer-authored v3"),
            ],
        )?;
        let v3_commit = run_jj(
            &jj_bin,
            auth_token.as_deref(),
            &[
                OsStr::new("-R"),
                ws_reader.as_os_str(),
                OsStr::new("log"),
                OsStr::new("--no-graph"),
                OsStr::new("-r"),
                OsStr::new("@"),
                OsStr::new("-T"),
                OsStr::new("commit_id"),
            ],
        )?
        .trim()
        .to_string();
        run_jj(
            &jj_bin,
            auth_token.as_deref(),
            &[
                OsStr::new("-R"),
                ws_writer.as_os_str(),
                OsStr::new("new"),
                OsStr::new(&v3_commit),
            ],
        )?;
        let checked_out_peer = fs::read_to_string(ws_writer.join("src/peer.rs"))?;
        if checked_out_peer != v3_code {
            return Err(format!(
                "expected peer-authored file on primary, got {checked_out_peer:?}"
            )
            .into());
        }
        println!(
            "    [7/{steps}] Verified reverse peer -> primary blob read ({})",
            short_id(&v3_commit)
        );
    }

    println!("==> Smoke verification PASSED");
    println!(
        "    Clone this repository interactively:\n      cargo run -p cli -- cc clone --server {checkout_url} {repo_id} ./my-checkout"
    );
    if opts.keep_workspaces {
        println!("    Preserved workspaces at: {}", base_dir.display());
    }
    Ok(())
}

fn verify_v2_checkout(ws_dir: &Path, v2_code: &str, large_blob: &[u8]) -> Result<(), BoxError> {
    let cloned_v2 = fs::read_to_string(ws_dir.join("src/lib.rs"))?;
    if cloned_v2 != v2_code {
        return Err(format!(
            "expected v2 code in {}, got {cloned_v2:?}",
            ws_dir.display()
        )
        .into());
    }
    let cloned_blob = fs::read(ws_dir.join("assets/weights.bin"))?;
    if cloned_blob != large_blob {
        return Err(format!("4.5 MiB binary payload mismatch in {}", ws_dir.display()).into());
    }
    #[cfg(unix)]
    {
        let link_target = fs::read_link(ws_dir.join("entry.rs"))?;
        if link_target.as_os_str() != OsStr::new("src/lib.rs") {
            return Err(format!("unexpected symlink target {link_target:?}").into());
        }
    }
    Ok(())
}

fn ensure_host_jj_binary(root: &Path) -> Result<PathBuf, BoxError> {
    ensure_host_jj_binary_for_profile(root, false)
}

pub fn ensure_host_jj_binary_for_profile(root: &Path, release: bool) -> Result<PathBuf, BoxError> {
    let exe_name = if cfg!(windows) { "jj.exe" } else { "jj" };
    let mut cmd = Command::new("cargo");
    cmd.current_dir(root)
        .args(["build", "--quiet", "-p", "cli", "--bin", "jj"]);
    if release {
        cmd.arg("--release");
    }
    let status = cmd.status()?;
    if !status.success() {
        return Err(if release {
            "`cargo build --release -p cli --bin jj` failed"
        } else {
            "`cargo build -p cli --bin jj` failed"
        }
        .into());
    }
    let profile_dir = if release { "release" } else { "debug" };
    Ok(cargo_target_dir(root).join(profile_dir).join(exe_name))
}

fn build_jj_isolated_command(
    jj_bin: &Path,
    xdg_cfg: &Path,
    auth_token: Option<&str>,
    args: &[&OsStr],
) -> Command {
    let mut cmd = Command::new(jj_bin);
    cmd.arg("--no-pager")
        .arg("--config=user.name=\"Smoke Test\"")
        .arg("--config=user.email=\"smoke@example.com\"")
        .env("JJ_CONFIG", "")
        .env_remove("JJ_CC_SYNC_PROJECT_COMMITS")
        .env("XDG_CONFIG_HOME", xdg_cfg)
        .env("JJ_CC_USE_DAEMON", "0")
        .args(args);
    if let Some(tok) = auth_token {
        cmd.env("JJ_CC_AUTH_TOKEN", tok);
    } else {
        cmd.env_remove("JJ_CC_AUTH_TOKEN");
    }
    cmd
}

fn run_jj_isolated(
    jj_bin: &Path,
    xdg_cfg: &Path,
    auth_token: Option<&str>,
    args: &[&OsStr],
) -> Result<String, BoxError> {
    let out = build_jj_isolated_command(jj_bin, xdg_cfg, auth_token, args).output()?;
    if !out.status.success() {
        return Err(format!(
            "jj {:?} failed ({}):\nstdout: {}\nstderr: {}",
            args,
            out.status,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        )
        .into());
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

fn extract_repo_id(ws_dir: &Path, init_stdout: &str) -> Result<String, BoxError> {
    let cfg_path = ws_dir
        .join(".jj")
        .join("repo")
        .join("store")
        .join("config.toml");
    if cfg_path.exists() {
        let raw = fs::read_to_string(&cfg_path)?;
        for line in raw.lines() {
            if let Some(rest) = line.trim().strip_prefix("repo_id")
                && let Some((_, quoted)) = rest.split_once('"')
                && let Some((id, _)) = quoted.split_once('"')
            {
                return Ok(id.to_string());
            }
        }
    }
    Err(format!("failed to read repo_id after init ({init_stdout})").into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_id_handles_short_and_long_commit_ids_without_panicking() {
        assert_eq!(short_id(""), "");
        assert_eq!(short_id("abc123"), "abc123");
        assert_eq!(short_id("0123456789ab"), "0123456789ab");
        assert_eq!(
            short_id("0123456789abcdef0123456789abcdef01234567"),
            "0123456789ab"
        );
    }

    #[test]
    fn isolated_jj_command_isolates_jj_config_and_unset_auth_token() {
        let cmd = build_jj_isolated_command(Path::new("jj"), Path::new("/tmp/xdg"), None, &[]);
        let envs: std::collections::HashMap<&OsStr, Option<&OsStr>> = cmd.get_envs().collect();
        assert_eq!(
            envs.get(OsStr::new("JJ_CONFIG")),
            Some(&Some(OsStr::new("")))
        );
        assert_eq!(
            envs.get(OsStr::new("XDG_CONFIG_HOME")),
            Some(&Some(OsStr::new("/tmp/xdg")))
        );
        assert_eq!(
            envs.get(OsStr::new("JJ_CC_SYNC_PROJECT_COMMITS")),
            Some(&None)
        );
        assert_eq!(envs.get(OsStr::new("JJ_CC_AUTH_TOKEN")), Some(&None));

        let authed =
            build_jj_isolated_command(Path::new("jj"), Path::new("/tmp/xdg"), Some("tok"), &[]);
        let auth_val = authed
            .get_envs()
            .find(|(k, _)| *k == OsStr::new("JJ_CC_AUTH_TOKEN"))
            .and_then(|(_, v)| v);
        assert_eq!(auth_val, Some(OsStr::new("tok")));
    }

    #[test]
    fn resolve_jj_bin_override_absolutizes_relative_paths_but_preserves_bare_names() {
        assert_eq!(
            resolve_jj_bin_override(Path::new("jj")).unwrap(),
            PathBuf::from("jj")
        );
        let abs = PathBuf::from("/usr/local/bin/jj");
        assert_eq!(resolve_jj_bin_override(&abs).unwrap(), abs);
        let rel = resolve_jj_bin_override(Path::new("./target/release/jj")).unwrap();
        assert!(rel.is_absolute(), "expected absolute path, got {rel:?}");
        assert!(rel.ends_with("target/release/jj"));
    }

    #[test]
    fn resolve_smoke_target_ignores_blank_server_and_peer_overrides() {
        let tmp = tempfile::tempdir().unwrap();
        let st = crate::state::EnvState::new(
            "default",
            "multiregion",
            "docker",
            "http://127.0.0.1:8081",
            Some("http://127.0.0.1:8082".to_string()),
            AuthMode::None,
        );
        crate::state::save_state(tmp.path(), &st).unwrap();

        // Explicit server with empty SMOKE_PEER_SERVER_URL="" must not set checkout_url to "".
        let explicit = resolve_smoke_target(
            tmp.path(),
            &SmokeOptions {
                env_name: "default".to_string(),
                server_override: Some(" http://jj-cc-server:8080 ".to_string()),
                peer_server_override: Some("".to_string()),
                jj_bin_override: None,
                keep_workspaces: false,
            },
        )
        .unwrap();
        assert_eq!(explicit.primary_url, "http://jj-cc-server:8080");
        assert_eq!(explicit.peer_url, None);

        // Blank server and peer env vars fall back to recorded state.
        let from_state = resolve_smoke_target(
            tmp.path(),
            &SmokeOptions {
                env_name: "default".to_string(),
                server_override: Some("   ".to_string()),
                peer_server_override: Some(" \t ".to_string()),
                jj_bin_override: None,
                keep_workspaces: false,
            },
        )
        .unwrap();
        assert_eq!(from_state.primary_url, "http://127.0.0.1:8081");
        assert_eq!(
            from_state.peer_url.as_deref(),
            Some("http://127.0.0.1:8082")
        );
    }
}
