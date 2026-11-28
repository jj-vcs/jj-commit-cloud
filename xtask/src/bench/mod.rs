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

//! Multi-agent concurrency, throughput, and warm-cache hit-ratio benchmark (`cargo xtask bench`).
//!
//! # Cross-Process and RPC Boundaries
//! Spawns `N` concurrent `jj` agent workspaces against an active `cargo xtask` environment or
//! ephemeral SQLite server (with ambient server env variables sanitized), optionally routing
//! through an isolated `jj-cc-daemon` instance guarded by [`BenchDaemonGuard`].

use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command as StdCommand, Stdio};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command as TokioCommand};
use tokio::sync::Barrier;
use tokio::task::JoinSet;

use crate::local::{SERVER_SANITIZED_ENV_VARS, resolve_bearer_token};
use crate::smoke::{
    ensure_host_jj_binary_for_profile, normalize_optional_url, resolve_jj_bin_override,
};
use crate::state::{AuthMode, BoxError, cargo_target_dir, load_state, workspace_root};

/// Ambient `jj` and `jj-cc-daemon` client environment variables cleared so host shell config
/// cannot redirect benchmark subprocesses to an external socket or upstream server. `JJ_CONFIG`
/// is set explicitly to `""` on each command so `jj` skips user and system config files across
/// Linux, macOS, and Windows.
const BENCH_SANITIZED_CLIENT_ENV_VARS: &[&str] = &[
    "JJ_CC_SYNC_PROJECT_COMMITS",
    "JJ_CC_DAEMON_SOCKET",
    "JJ_CC_DAEMON_LISTEN",
    "JJ_CC_DAEMON_CONFIG",
    "JJ_CC_DAEMON_ALLOW_UPSTREAM",
    "JJ_CC_DAEMON_MEMORY_MB",
    "JJ_CC_DAEMON_IDLE_SECS",
    "JJ_CC_UPSTREAM",
];

pub struct BenchOptions {
    pub env_name: String,
    pub server_override: Option<String>,
    pub ephemeral: bool,
    pub agents: usize,
    pub commits_per_agent: usize,
    pub isolated_repos: bool,
    pub use_daemon: bool,
    pub release: bool,
    pub jj_bin_override: Option<PathBuf>,
}

fn validate_bench_target_options(opts: &BenchOptions) -> Result<Option<String>, BoxError> {
    let server_override = normalize_optional_url(opts.server_override.as_deref());
    if opts.ephemeral && server_override.is_some() {
        return Err("`--server` cannot be combined with `--ephemeral`".into());
    }
    Ok(server_override)
}

fn format_bench_build_profile(opts: &BenchOptions) -> String {
    let profile = if opts.release { "release" } else { "debug" };
    if opts.jj_bin_override.is_some() {
        if opts.ephemeral || opts.use_daemon {
            format!("custom --jj-bin ({profile} for managed binaries)")
        } else {
            "custom --jj-bin".to_string()
        }
    } else if opts.release {
        "release".to_string()
    } else {
        "debug (pass --release for optimized builds)".to_string()
    }
}

#[derive(Debug, Default, Deserialize)]
struct DaemonStatsJson {
    l1_hits: u64,
    l2_hits: u64,
    misses: u64,
    singleflight_coalesced: u64,
    upstream_rpcs: u64,
}

struct EphemeralServer {
    child: Child,
    url: String,
}

impl Drop for EphemeralServer {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

struct BenchDaemonGuard {
    jj_bin: PathBuf,
    cwd: PathBuf,
    xdg_dir: PathBuf,
    daemon_bin: PathBuf,
    state_dir: PathBuf,
    cache_dir: PathBuf,
    active: bool,
}

impl BenchDaemonGuard {
    fn disarm(&mut self) {
        self.active = false;
    }

    fn build_stop_command(&self) -> StdCommand {
        let mut cmd = StdCommand::new(&self.jj_bin);
        for var in BENCH_SANITIZED_CLIENT_ENV_VARS {
            cmd.env_remove(var);
        }
        cmd.current_dir(&self.cwd)
            .args(["--no-pager", "cc", "daemon", "stop"])
            .env("JJ_CONFIG", "")
            .env("XDG_CONFIG_HOME", &self.xdg_dir)
            .env("JJ_CC_USE_DAEMON", "1")
            .env("JJ_CC_DAEMON_BIN", &self.daemon_bin)
            .env("JJ_CC_DAEMON_DIR", &self.state_dir)
            .env("JJ_CC_CACHE_DIR", &self.cache_dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        cmd
    }
}

impl Drop for BenchDaemonGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let _ = self.build_stop_command().status();
    }
}

pub async fn run_bench(opts: &BenchOptions) -> Result<(), BoxError> {
    if opts.agents == 0 || opts.commits_per_agent == 0 {
        return Err("`--agents` and `--commits-per-agent` must both be >= 1".into());
    }
    let total_expected_commits = opts
        .agents
        .checked_mul(opts.commits_per_agent)
        .ok_or("`--agents` * `--commits-per-agent` overflows usize")?;
    let server_override = validate_bench_target_options(opts)?;

    let root = workspace_root();
    let temp_dir = tempfile::Builder::new().prefix("jj-cc-bench-").tempdir()?;
    let xdg_dir = temp_dir.path().join("xdg");
    fs::create_dir_all(&xdg_dir)?;

    let mut _ephemeral_guard: Option<EphemeralServer> = None;
    let (target_url, auth_mode, stored_auth_token, mode_label) = if opts.ephemeral {
        let srv =
            spawn_ephemeral_sqlite_server(&root, &temp_dir.path().join("bench.db"), opts.release)
                .await?;
        let url = srv.url.clone();
        _ephemeral_guard = Some(srv);
        (url, AuthMode::None, None, "ephemeral-sqlite".to_string())
    } else if let Some(explicit) = server_override {
        (explicit, AuthMode::None, None, "explicit".to_string())
    } else if let Some(state) = load_state(&root, &opts.env_name)? {
        if state.compute_stopped {
            return Err(format!(
                "environment '{}' compute is stopped (only retained cloud storage is recorded); start a server with `cargo xtask up <mode>` or pass `--ephemeral`",
                opts.env_name
            )
            .into());
        }
        (
            state.endpoints.primary,
            state.auth,
            state.auth_token,
            format!("{} ({})", state.mode, state.runtime),
        )
    } else {
        return Err(format!(
            "no active environment state found for '{}' (start a stack with `cargo xtask up <mode>` or pass `--ephemeral`)",
            opts.env_name
        )
        .into());
    };

    let auth_token = resolve_bearer_token(auth_mode, stored_auth_token.as_deref())?;

    let jj_bin = match &opts.jj_bin_override {
        Some(p) => resolve_jj_bin_override(p)?,
        None => ensure_host_jj_binary_for_profile(&root, opts.release)?,
    };

    let daemon_state_dir = temp_dir.path().join("daemon_state");
    let daemon_cache_dir = temp_dir.path().join("daemon_cache");
    let daemon_bin = if opts.use_daemon {
        Some(ensure_host_daemon_binary(&root, opts.release)?)
    } else {
        None
    };

    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let rand_uuid = uuid::Uuid::new_v4().simple().to_string();
    let rand_suffix = rand_uuid.get(..8).unwrap_or(&rand_uuid);
    let project_id = format!("bench-proj-{ts}-{rand_suffix}");
    let shared_repo_id = format!("bench-repo-{ts}-{rand_suffix}");

    println!("==> Multi-agent scale benchmark against {mode_label}");
    println!("    Endpoint:          {target_url}");
    println!(
        "    Build profile:     {}",
        format_bench_build_profile(opts)
    );
    println!(
        "    Topology:          {} agents x {} commits = {} commits ({})",
        opts.agents,
        opts.commits_per_agent,
        total_expected_commits,
        if opts.isolated_repos {
            "isolated repos sharing project_id"
        } else {
            "shared repo_id with server-side ReconcileOpHeads"
        }
    );
    println!(
        "    Local daemon:      {}",
        if opts.use_daemon {
            "enabled (L1 memory + L2 disk CAS)"
        } else {
            "disabled (direct HTTP/2)"
        }
    );

    let mut daemon_guard = if let Some(dbin) = &daemon_bin {
        run_jj_async(
            &jj_bin,
            temp_dir.path(),
            &xdg_dir,
            auth_token.as_deref(),
            Some((dbin, &daemon_state_dir, &daemon_cache_dir)),
            &[
                OsStr::new("cc"),
                OsStr::new("daemon"),
                OsStr::new("start"),
                OsStr::new("--upstream"),
                OsStr::new(&target_url),
                OsStr::new("--idle-timeout-secs"),
                OsStr::new("60"),
            ],
        )
        .await?;
        Some(BenchDaemonGuard {
            jj_bin: jj_bin.clone(),
            cwd: temp_dir.path().to_path_buf(),
            xdg_dir: xdg_dir.clone(),
            daemon_bin: dbin.clone(),
            state_dir: daemon_state_dir.clone(),
            cache_dir: daemon_cache_dir.clone(),
            active: true,
        })
    } else {
        None
    };

    // Seed project + initial baseline commit so all agents branch from a realistic tree.
    let seed_ws = temp_dir.path().join("seed-ws");
    run_jj_async(
        &jj_bin,
        temp_dir.path(),
        &xdg_dir,
        auth_token.as_deref(),
        daemon_bin
            .as_deref()
            .map(|b| (b, daemon_state_dir.as_path(), daemon_cache_dir.as_path())),
        &[
            OsStr::new("cc"),
            OsStr::new("init"),
            OsStr::new("--server"),
            OsStr::new(&target_url),
            OsStr::new("--project-id"),
            OsStr::new(&project_id),
            OsStr::new("--repo-id"),
            OsStr::new(&shared_repo_id),
            OsStr::new("--create"),
            seed_ws.as_os_str(),
        ],
    )
    .await?;
    fs::create_dir_all(seed_ws.join("src"))?;
    fs::write(
        seed_ws.join("src/common.rs"),
        "pub const BENCH_BASELINE: u64 = 1;\n",
    )?;
    run_jj_async(
        &jj_bin,
        &seed_ws,
        &xdg_dir,
        auth_token.as_deref(),
        daemon_bin
            .as_deref()
            .map(|b| (b, daemon_state_dir.as_path(), daemon_cache_dir.as_path())),
        &[
            OsStr::new("commit"),
            OsStr::new("-m"),
            OsStr::new("benchmark baseline commit"),
        ],
    )
    .await?;

    // Set up all N agent workspaces before starting the throughput clock.
    let shared_jj = Arc::new(jj_bin.clone());
    let shared_url = Arc::new(target_url.clone());
    let shared_proj = Arc::new(project_id.clone());
    let shared_repo = Arc::new(shared_repo_id.clone());
    let shared_xdg = Arc::new(xdg_dir.clone());
    let shared_auth = Arc::new(auth_token.clone());
    let shared_daemon = Arc::new(
        daemon_bin
            .clone()
            .map(|b| (b, daemon_state_dir.clone(), daemon_cache_dir.clone())),
    );

    let mut setup_set = JoinSet::new();
    for agent_idx in 0..opts.agents {
        let ws_dir = temp_dir.path().join(format!("agent-{agent_idx}"));
        let jj = Arc::clone(&shared_jj);
        let url = Arc::clone(&shared_url);
        let proj = Arc::clone(&shared_proj);
        let repo = Arc::clone(&shared_repo);
        let xdg = Arc::clone(&shared_xdg);
        let auth = Arc::clone(&shared_auth);
        let daemon_ctx = Arc::clone(&shared_daemon);
        let isolated = opts.isolated_repos;
        let parent_tmp = temp_dir.path().to_path_buf();

        setup_set.spawn(async move {
            let d_ref = daemon_ctx
                .as_ref()
                .as_ref()
                .map(|(b, s, c)| (b.as_path(), s.as_path(), c.as_path()));

            let agent_repo = if isolated {
                let r = format!("{repo}-a{agent_idx}");
                run_jj_async(
                    &jj,
                    &parent_tmp,
                    &xdg,
                    auth.as_deref(),
                    d_ref,
                    &[
                        OsStr::new("cc"),
                        OsStr::new("init"),
                        OsStr::new("--server"),
                        OsStr::new(url.as_str()),
                        OsStr::new("--project-id"),
                        OsStr::new(proj.as_str()),
                        OsStr::new("--repo-id"),
                        OsStr::new(&r),
                        OsStr::new("--create"),
                        ws_dir.as_os_str(),
                    ],
                )
                .await?;
                r
            } else {
                let ws_name = format!("agent-{agent_idx}");
                run_jj_async(
                    &jj,
                    &parent_tmp,
                    &xdg,
                    auth.as_deref(),
                    d_ref,
                    &[
                        OsStr::new("cc"),
                        OsStr::new("clone"),
                        OsStr::new("--server"),
                        OsStr::new(url.as_str()),
                        OsStr::new("--workspace-name"),
                        OsStr::new(&ws_name),
                        OsStr::new(repo.as_str()),
                        ws_dir.as_os_str(),
                    ],
                )
                .await?;
                (*repo).clone()
            };

            fs::create_dir_all(ws_dir.join("agents")).map_err(|e| e.to_string())?;
            Ok::<(usize, String, PathBuf), String>((agent_idx, agent_repo, ws_dir))
        });
    }

    let mut prepared_agents = Vec::with_capacity(opts.agents);
    while let Some(joined) = setup_set.join_next().await {
        let ready = joined.map_err(|e| format!("agent setup task panicked: {e}"))??;
        prepared_agents.push(ready);
    }

    // Release all prepared agents behind a barrier so wall-clock throughput excludes
    // workspace init/clone setup.
    let barrier = Arc::new(Barrier::new(opts.agents));
    let commit_start = Arc::new(OnceLock::<Instant>::new());
    let mut join_set = JoinSet::new();

    for (agent_idx, agent_repo, ws_dir) in prepared_agents {
        let jj = Arc::clone(&shared_jj);
        let xdg = Arc::clone(&shared_xdg);
        let auth = Arc::clone(&shared_auth);
        let daemon_ctx = Arc::clone(&shared_daemon);
        let commits_k = opts.commits_per_agent;
        let barrier = Arc::clone(&barrier);
        let commit_start = Arc::clone(&commit_start);

        join_set.spawn(async move {
            let d_ref = daemon_ctx
                .as_ref()
                .as_ref()
                .map(|(b, s, c)| (b.as_path(), s.as_path(), c.as_path()));

            barrier.wait().await;
            let _ = commit_start.get_or_init(Instant::now);

            let mut latencies_ms = Vec::with_capacity(commits_k);
            let mut markers = Vec::with_capacity(commits_k);
            let agent_subdir = ws_dir.join("agents");
            for step in 0..commits_k {
                let file_path = agent_subdir.join(format!("agent_{agent_idx}_step_{step}.rs"));
                fs::write(
                    &file_path,
                    format!("pub fn agent_{agent_idx}_step_{step}() -> usize {{ {step} }}\n"),
                )
                .map_err(|e| e.to_string())?;
                let msg = format!("bench-commit:a{agent_idx}:s{step}");
                let t0 = Instant::now();
                run_jj_async(
                    &jj,
                    &ws_dir,
                    &xdg,
                    auth.as_deref(),
                    d_ref,
                    &[OsStr::new("commit"), OsStr::new("-m"), OsStr::new(&msg)],
                )
                .await?;
                latencies_ms.push(t0.elapsed().as_secs_f64() * 1000.0);
                markers.push(msg);
            }
            Ok::<(String, PathBuf, Vec<f64>, Vec<String>), String>((
                agent_repo,
                ws_dir,
                latencies_ms,
                markers,
            ))
        });
    }

    let mut all_latencies_ms = Vec::with_capacity(total_expected_commits);
    let mut agent_workspaces = Vec::with_capacity(opts.agents);
    let mut agent_results = Vec::with_capacity(opts.agents);
    while let Some(joined) = join_set.join_next().await {
        let (agent_repo, ws_dir, lats, markers) =
            joined.map_err(|e| format!("agent task panicked: {e}"))??;
        agent_workspaces.push(ws_dir.clone());
        all_latencies_ms.extend(lats);
        agent_results.push((agent_repo, ws_dir, markers));
    }
    let write_elapsed = commit_start.get().map(Instant::elapsed).unwrap_or_default();

    // Collect each agent's authored commit IDs and verify all commits per repo through fresh
    // direct workspaces (bypassing the local daemon cache) so both shared and isolated topologies
    // prove zero lost commits on the server.
    let mut expected_by_repo: HashMap<String, Vec<(String, String)>> = HashMap::new();
    let mut collect_set = JoinSet::new();
    for (agent_repo, ws_dir, markers) in agent_results {
        let jj = Arc::clone(&shared_jj);
        let xdg = Arc::clone(&shared_xdg);
        let auth = Arc::clone(&shared_auth);
        let daemon_ctx = Arc::clone(&shared_daemon);
        collect_set.spawn(async move {
            let d_ref = daemon_ctx
                .as_ref()
                .as_ref()
                .map(|(b, s, c)| (b.as_path(), s.as_path(), c.as_path()));
            let log_out = run_jj_async(
                &jj,
                &ws_dir,
                &xdg,
                auth.as_deref(),
                d_ref,
                &[
                    OsStr::new("log"),
                    OsStr::new("--no-graph"),
                    OsStr::new("-r"),
                    OsStr::new("all()"),
                    OsStr::new("-T"),
                    OsStr::new("commit_id ++ \"\\t\" ++ description.first_line() ++ \"\\n\""),
                ],
            )
            .await?;
            let collected = extract_agent_commit_ids(&log_out, &markers)?;
            Ok::<(String, Vec<(String, String)>), String>((agent_repo, collected))
        });
    }
    while let Some(joined) = collect_set.join_next().await {
        let (repo_id, pairs) =
            joined.map_err(|e| format!("commit collection task panicked: {e}"))??;
        expected_by_repo.entry(repo_id).or_default().extend(pairs);
    }

    let verify_root = temp_dir.path().join("verify");
    fs::create_dir_all(&verify_root)?;
    let mut verify_set = JoinSet::new();
    for (idx, (repo_id, expected_pairs)) in expected_by_repo.into_iter().enumerate() {
        let jj = Arc::clone(&shared_jj);
        let url = Arc::clone(&shared_url);
        let xdg = Arc::clone(&shared_xdg);
        let auth = Arc::clone(&shared_auth);
        let verify_parent = verify_root.clone();
        let verify_ws = verify_root.join(format!("repo-{idx}"));
        verify_set.spawn(async move {
            run_jj_async(
                &jj,
                &verify_parent,
                &xdg,
                auth.as_deref(),
                None,
                &[
                    OsStr::new("cc"),
                    OsStr::new("clone"),
                    OsStr::new("--server"),
                    OsStr::new(url.as_str()),
                    OsStr::new(&repo_id),
                    verify_ws.as_os_str(),
                ],
            )
            .await?;
            let log_out = run_jj_async(
                &jj,
                &verify_ws,
                &xdg,
                auth.as_deref(),
                None,
                &[
                    OsStr::new("log"),
                    OsStr::new("--no-graph"),
                    OsStr::new("-r"),
                    OsStr::new("all()"),
                    OsStr::new("-T"),
                    OsStr::new("commit_id ++ \"\\t\" ++ description.first_line() ++ \"\\n\""),
                ],
            )
            .await?;
            verify_repo_commits(&repo_id, &log_out, &expected_pairs)
        });
    }

    let mut verified_count = 0usize;
    while let Some(joined) = verify_set.join_next().await {
        let count = joined.map_err(|e| format!("verification task panicked: {e}"))??;
        verified_count += count;
    }
    let verified_commits: Option<usize> = Some(verified_count);

    // Optional warm-read sweep when `--daemon` is enabled.
    let mut daemon_stats: Option<DaemonStatsJson> = None;
    if let Some(dbin) = &daemon_bin {
        for ws in agent_workspaces.iter().take(4) {
            for _ in 0..2 {
                run_jj_async(
                    &jj_bin,
                    ws,
                    &xdg_dir,
                    auth_token.as_deref(),
                    Some((dbin, &daemon_state_dir, &daemon_cache_dir)),
                    &[
                        OsStr::new("log"),
                        OsStr::new("--no-graph"),
                        OsStr::new("-r"),
                        OsStr::new("all()"),
                        OsStr::new("-T"),
                        OsStr::new("commit_id ++ \"\\n\""),
                    ],
                )
                .await?;
                run_jj_async(
                    &jj_bin,
                    ws,
                    &xdg_dir,
                    auth_token.as_deref(),
                    Some((dbin, &daemon_state_dir, &daemon_cache_dir)),
                    &[OsStr::new("diff"), OsStr::new("-r"), OsStr::new("@-")],
                )
                .await?;
            }
        }
        if run_jj_async(
            &jj_bin,
            temp_dir.path(),
            &xdg_dir,
            auth_token.as_deref(),
            Some((dbin, &daemon_state_dir, &daemon_cache_dir)),
            &[OsStr::new("cc"), OsStr::new("daemon"), OsStr::new("stop")],
        )
        .await
        .is_ok()
            && let Some(guard) = &mut daemon_guard
        {
            guard.disarm();
        }
        if let Ok(raw) = fs::read_to_string(daemon_state_dir.join("daemon.stats.json")) {
            daemon_stats = serde_json::from_str(&raw).ok();
        }
    }

    all_latencies_ms.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let throughput = (total_expected_commits as f64) / write_elapsed.as_secs_f64().max(0.001);
    let p50 = percentile(&all_latencies_ms, 0.50);
    let p95 = percentile(&all_latencies_ms, 0.95);
    let p99 = percentile(&all_latencies_ms, 0.99);

    println!("==> Benchmark Results");
    println!(
        "    Commits verified:  {}",
        format_verification_summary(verified_commits, total_expected_commits)
    );
    println!("    Wall-clock time:   {:.2}s", write_elapsed.as_secs_f64());
    println!("    Throughput:        {throughput:.2} commits/s");
    println!("    Commit latency:    p50={p50:.1}ms  p95={p95:.1}ms  p99={p99:.1}ms");
    if let Some(st) = daemon_stats {
        let hits = st.l1_hits + st.l2_hits;
        let denom = (hits + st.misses).max(1);
        let pct = (hits as f64) * 100.0 / (denom as f64);
        println!(
            "    Daemon cache:      {} L1 + {} L2 hits ({pct:.1}% hit rate, {} coalesced, {} upstream RPCs)",
            st.l1_hits, st.l2_hits, st.singleflight_coalesced, st.upstream_rpcs
        );
    }

    if let Some(mut srv) = _ephemeral_guard.take() {
        let _ = srv.child.kill().await;
    }

    Ok(())
}

fn format_verification_summary(verified: Option<usize>, expected: usize) -> String {
    match verified {
        Some(n) => format!(
            "{n}/{expected} ({} lost commits)",
            expected.saturating_sub(n)
        ),
        None => "not verified".to_string(),
    }
}

fn parse_commit_log_lines(log_out: &str) -> HashMap<&str, &str> {
    let mut by_desc = HashMap::new();
    for line in log_out.lines() {
        if let Some((commit_id, desc)) = line.split_once('\t') {
            let cid = commit_id.trim();
            let d = desc.trim();
            if !cid.is_empty() && !d.is_empty() {
                by_desc.insert(d, cid);
            }
        }
    }
    by_desc
}

fn extract_agent_commit_ids(
    log_out: &str,
    expected_markers: &[String],
) -> Result<Vec<(String, String)>, String> {
    let by_desc = parse_commit_log_lines(log_out);
    let mut pairs = Vec::with_capacity(expected_markers.len());
    for marker in expected_markers {
        let Some(&commit_id) = by_desc.get(marker.as_str()) else {
            return Err(format!(
                "agent workspace is missing authored commit '{marker}'"
            ));
        };
        pairs.push((commit_id.to_string(), marker.clone()));
    }
    Ok(pairs)
}

fn verify_repo_commits(
    repo_id: &str,
    log_out: &str,
    expected_pairs: &[(String, String)],
) -> Result<usize, String> {
    let present: HashSet<(&str, &str)> = log_out
        .lines()
        .filter_map(|line| {
            let (cid, desc) = line.split_once('\t')?;
            Some((cid.trim(), desc.trim()))
        })
        .collect();
    for (commit_id, marker) in expected_pairs {
        if !present.contains(&(commit_id.as_str(), marker.as_str())) {
            return Err(format!(
                "repository '{repo_id}' lost commit {commit_id} ('{marker}')"
            ));
        }
    }
    Ok(expected_pairs.len())
}

fn percentile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() - 1) as f64 * q).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn ensure_host_daemon_binary(root: &Path, release: bool) -> Result<PathBuf, BoxError> {
    let exe_name = if cfg!(windows) {
        "jj-cc-daemon.exe"
    } else {
        "jj-cc-daemon"
    };
    let mut cmd = StdCommand::new("cargo");
    cmd.current_dir(root).args([
        "build",
        "--quiet",
        "-p",
        "jj-commit-cloud-daemon",
        "--bin",
        "jj-cc-daemon",
    ]);
    if release {
        cmd.arg("--release");
    }
    let status = cmd.status()?;
    if !status.success() {
        return Err(if release {
            "`cargo build --release -p jj-commit-cloud-daemon` failed"
        } else {
            "`cargo build -p jj-commit-cloud-daemon` failed"
        }
        .into());
    }
    let profile_dir = if release { "release" } else { "debug" };
    Ok(cargo_target_dir(root).join(profile_dir).join(exe_name))
}

async fn spawn_ephemeral_sqlite_server(
    root: &Path,
    db_path: &Path,
    release: bool,
) -> Result<EphemeralServer, BoxError> {
    let exe_name = if cfg!(windows) {
        "jj-cc-server.exe"
    } else {
        "jj-cc-server"
    };
    let mut build_cmd = StdCommand::new("cargo");
    build_cmd.current_dir(root).args([
        "build",
        "--quiet",
        "-p",
        "jj-commit-cloud-server",
        "--bin",
        "jj-cc-server",
    ]);
    if release {
        build_cmd.arg("--release");
    }
    let status = build_cmd.status()?;
    if !status.success() {
        return Err(if release {
            "`cargo build --release -p jj-commit-cloud-server` failed"
        } else {
            "`cargo build -p jj-commit-cloud-server` failed"
        }
        .into());
    }
    let profile_dir = if release { "release" } else { "debug" };
    let server_bin = cargo_target_dir(root).join(profile_dir).join(exe_name);

    let blob_dir = db_path.with_extension("blobs");
    fs::create_dir_all(&blob_dir)?;
    let stderr_path = db_path.with_extension("stderr.log");
    let stderr_file = fs::File::create(&stderr_path)?;
    let mut cmd = TokioCommand::new(&server_bin);
    for var in SERVER_SANITIZED_ENV_VARS {
        cmd.env_remove(var);
    }
    let mut child = cmd
        .kill_on_drop(true)
        .arg("--host=127.0.0.1")
        .arg("--port=0")
        .arg("--store-type=sqlite")
        .arg("--blob-store=local")
        .arg("--sqlite-path")
        .arg(db_path)
        .arg("--blob-path")
        .arg(&blob_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::from(stderr_file))
        .spawn()?;

    let stdout = child
        .stdout
        .take()
        .ok_or("failed to capture jj-cc-server stdout")?;
    let mut lines = BufReader::new(stdout).lines();
    let deadline = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(deadline);

    let bound_addr = loop {
        tokio::select! {
            line = lines.next_line() => {
                match line? {
                    Some(text) => {
                        if let Some((_, rest)) = text.split_once("listening on ") {
                            let addr = rest.trim().to_string();
                            if !addr.is_empty() {
                                break addr;
                            }
                        }
                    }
                    None => {
                        let stderr_out = fs::read_to_string(&stderr_path).unwrap_or_default();
                        return Err(format!(
                            "ephemeral jj-cc-server exited before binding (stderr: {})",
                            stderr_out.trim()
                        )
                        .into());
                    }
                }
            }
            _ = &mut deadline => {
                let stderr_out = fs::read_to_string(&stderr_path).unwrap_or_default();
                return Err(format!(
                    "timed out waiting for ephemeral jj-cc-server (stderr: {})",
                    stderr_out.trim()
                )
                .into());
            }
        }
    };
    tokio::spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });

    Ok(EphemeralServer {
        child,
        url: format!("http://{bound_addr}"),
    })
}

fn build_bench_jj_command(
    jj_bin: &Path,
    cwd: &Path,
    xdg_dir: &Path,
    auth_token: Option<&str>,
    daemon_ctx: Option<(&Path, &Path, &Path)>,
    args: &[&OsStr],
) -> TokioCommand {
    let mut cmd = TokioCommand::new(jj_bin);
    for var in BENCH_SANITIZED_CLIENT_ENV_VARS {
        cmd.env_remove(var);
    }
    cmd.kill_on_drop(true)
        .current_dir(cwd)
        .arg("--no-pager")
        .args(args)
        .env("JJ_CONFIG", "")
        .env("XDG_CONFIG_HOME", xdg_dir)
        .env("JJ_USER", "Benchmark Agent")
        .env("JJ_EMAIL", "bench@example.com");

    if let Some(tok) = auth_token {
        cmd.env("JJ_CC_AUTH_TOKEN", tok);
    } else {
        cmd.env_remove("JJ_CC_AUTH_TOKEN");
    }
    if let Some((daemon_bin, state_dir, cache_dir)) = daemon_ctx {
        cmd.env("JJ_CC_USE_DAEMON", "1")
            .env("JJ_CC_DAEMON_BIN", daemon_bin)
            .env("JJ_CC_DAEMON_DIR", state_dir)
            .env("JJ_CC_CACHE_DIR", cache_dir);
    } else {
        cmd.env("JJ_CC_USE_DAEMON", "0");
    }
    cmd
}

async fn run_jj_async(
    jj_bin: &Path,
    cwd: &Path,
    xdg_dir: &Path,
    auth_token: Option<&str>,
    daemon_ctx: Option<(&Path, &Path, &Path)>,
    args: &[&OsStr],
) -> Result<String, String> {
    let mut cmd = build_bench_jj_command(jj_bin, cwd, xdg_dir, auth_token, daemon_ctx, args);
    let out = cmd
        .output()
        .await
        .map_err(|e| format!("failed to spawn jj {args:?}: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "jj {args:?} failed ({}):\nstdout: {}\nstderr: {}",
            out.status,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

#[cfg(test)]
mod tests;
