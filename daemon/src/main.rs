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

//! CLI entry point and process lifecycle wrapper for the `jj-cc-daemon` binary.
//!
//! # Authority and Crash-Safety Ordering
//! Empty or whitespace-only path environment variables fall back to per-user defaults before
//! resolving `state_dir`, `socket_path`, and `cache_dir`. Legacy invocations passing `--socket`
//! without `--state-dir` or `--listen` disable discovery files so ad-hoc test or per-workspace
//! sockets never acquire or clobber the user's primary `state_dir` lock, `daemon.json`, or
//! `daemon.stats.json`.
//!
//! # Concurrency and Merge Invariants
//! All concurrency, singleflight, and state-directory lock enforcement live in
//! [`jj_commit_cloud_daemon::run_daemon`]; this binary forwards `SIGINT`/`SIGTERM` into a
//! single graceful shutdown signal.
//!
//! # Cross-Process and RPC Boundaries
//! Accepts `--upstream` (legacy `--server`), `--allow-upstream`, `--socket`, `--listen`,
//! `--memory-cache-mb`, `--max-disk-cache-mb`, and `--idle-timeout-secs` (legacy
//! `--idle-timeout`) via CLI flags or `JJ_CC_*` environment vars.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use clap::builder::{OsStringValueParser, TypedValueParser as _};
use jj_commit_cloud_daemon::{
    DEFAULT_MAX_DISK_CACHE_MB, DaemonConfig, default_daemon_cache_dir, default_daemon_state_dir,
    run_daemon,
};

fn permissive_path_parser() -> impl clap::builder::TypedValueParser<Value = PathBuf> {
    OsStringValueParser::new().map(PathBuf::from)
}

#[derive(Parser, Debug)]
#[command(
    name = "jj-cc-daemon",
    about = "Local two-tier caching gRPC proxy daemon for jj-commit-cloud"
)]
struct Args {
    /// Directory storing runtime discovery metadata (`daemon.json` and `daemon.stats.json`)
    #[arg(long, env = "JJ_CC_DAEMON_DIR", value_parser = permissive_path_parser())]
    state_dir: Option<PathBuf>,

    /// Unix domain socket path on Linux and macOS
    #[arg(long, env = "JJ_CC_DAEMON_SOCKET", value_parser = permissive_path_parser())]
    socket: Option<PathBuf>,

    /// Optional loopback TCP bind address (for example `127.0.0.1:0` or `127.0.0.1:50055`);
    /// TCP clients must send the secret recorded in `daemon.json`
    #[arg(long, env = "JJ_CC_DAEMON_LISTEN")]
    listen: Option<SocketAddr>,

    /// Local L2 content-addressed disk cache directory
    #[arg(long, env = "JJ_CC_CACHE_DIR", value_parser = permissive_path_parser())]
    cache_dir: Option<PathBuf>,

    /// Optional default upstream `jj-cc-server` URL when callers omit `x-jj-cc-upstream`
    #[arg(long, alias = "server", env = "JJ_CC_UPSTREAM")]
    upstream: Option<String>,

    /// Optional allowlist of permitted upstream `jj-cc-server` origins (comma-separated or
    /// repeatable; `--upstream` is automatically included when set)
    #[arg(
        long = "allow-upstream",
        env = "JJ_CC_DAEMON_ALLOW_UPSTREAM",
        value_delimiter = ','
    )]
    allow_upstreams: Vec<String>,

    /// Maximum L1 in-memory object cache capacity in MiB
    #[arg(long, default_value_t = 256, env = "JJ_CC_DAEMON_MEMORY_MB")]
    memory_cache_mb: usize,

    /// Maximum L2 disk cache size in MiB before automatic background LRU eviction (`0` disables
    /// automatic L2 disk-quota LRU eviction)
    #[arg(
        long,
        default_value_t = DEFAULT_MAX_DISK_CACHE_MB,
        env = "JJ_CC_DAEMON_MAX_DISK_CACHE_MB"
    )]
    max_disk_cache_mb: u64,

    /// Idle shutdown timeout in seconds (`0` disables automatic idle exit)
    #[arg(
        long,
        alias = "idle-timeout",
        default_value_t = 1800,
        env = "JJ_CC_DAEMON_IDLE_SECS"
    )]
    idle_timeout_secs: u64,
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("SIGTERM signal handler should register");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn non_empty_path(path: Option<PathBuf>) -> Option<PathBuf> {
    path.filter(|p| {
        p.to_str()
            .map(|s| !s.trim().is_empty())
            .unwrap_or(!p.as_os_str().is_empty())
    })
}

fn non_empty_string(val: Option<String>) -> Option<String> {
    val.and_then(|s| {
        let trimmed = s.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
}

fn build_config(args: Args) -> DaemonConfig {
    let explicit_state_dir = non_empty_path(args.state_dir);
    let explicit_socket = non_empty_path(args.socket);
    let cache_dir = non_empty_path(args.cache_dir).unwrap_or_else(default_daemon_cache_dir);
    let listen_addr = if cfg!(unix) {
        args.listen
    } else {
        Some(
            args.listen
                .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 0))),
        )
    };

    // Legacy per-socket invocations (`--socket` without `--state-dir` and without `--listen`)
    // skip discovery files so ad-hoc daemons never collide with or clobber the user's primary
    // `daemon.json`, `daemon.stats.json`, or `daemon.instance.lock`.
    let write_discovery_files =
        explicit_state_dir.is_some() || explicit_socket.is_none() || listen_addr.is_some();
    let state_dir = if write_discovery_files {
        explicit_state_dir.unwrap_or_else(default_daemon_state_dir)
    } else {
        PathBuf::new()
    };
    let socket_path = if cfg!(unix) {
        Some(explicit_socket.unwrap_or_else(|| state_dir.join("daemon.sock")))
    } else {
        explicit_socket
    };

    DaemonConfig {
        state_dir,
        socket_path,
        listen_addr,
        cache_dir,
        default_upstream: non_empty_string(args.upstream),
        allowed_upstreams: args.allow_upstreams,
        memory_cache_mb: args.memory_cache_mb,
        max_disk_cache_mb: args.max_disk_cache_mb,
        idle_timeout_secs: args.idle_timeout_secs,
        write_discovery_files,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = build_config(Args::parse());
    run_daemon(config, shutdown_signal()).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_accepts_legacy_server_and_idle_timeout_aliases() {
        let legacy = Args::try_parse_from([
            "jj-cc-daemon",
            "--server",
            "http://127.0.0.1:8080",
            "--socket",
            "/tmp/test.sock",
            "--idle-timeout",
            "45",
        ])
        .expect("legacy flags should parse");
        assert_eq!(legacy.upstream.as_deref(), Some("http://127.0.0.1:8080"));
        assert_eq!(legacy.socket, Some(PathBuf::from("/tmp/test.sock")));
        assert_eq!(legacy.idle_timeout_secs, 45);

        let modern = Args::try_parse_from([
            "jj-cc-daemon",
            "--upstream",
            "http://127.0.0.1:9090",
            "--idle-timeout-secs",
            "0",
            "--max-disk-cache-mb",
            "0",
        ])
        .expect("modern flags should parse");
        assert_eq!(modern.upstream.as_deref(), Some("http://127.0.0.1:9090"));
        assert_eq!(modern.idle_timeout_secs, 0);
        assert_eq!(modern.max_disk_cache_mb, 0);
        assert_eq!(legacy.max_disk_cache_mb, DEFAULT_MAX_DISK_CACHE_MB);
    }

    #[test]
    fn build_config_filters_empty_paths_and_isolates_explicit_socket_mode() {
        let blank_args = Args::try_parse_from([
            "jj-cc-daemon",
            "--state-dir",
            "   ",
            "--socket",
            "",
            "--cache-dir",
            "\t",
            "--upstream",
            "  ",
        ])
        .expect("blank path arguments should parse");
        let blank_cfg = build_config(blank_args);
        assert!(blank_cfg.write_discovery_files);
        assert_eq!(blank_cfg.state_dir, default_daemon_state_dir());
        assert_eq!(blank_cfg.cache_dir, default_daemon_cache_dir());
        assert_eq!(blank_cfg.default_upstream, None);
        #[cfg(unix)]
        assert_eq!(
            blank_cfg.socket_path,
            Some(default_daemon_state_dir().join("daemon.sock"))
        );

        let legacy_socket_args = Args::try_parse_from([
            "jj-cc-daemon",
            "--state-dir",
            "",
            "--socket",
            "/tmp/adhoc.sock",
        ])
        .expect("explicit socket args should parse");
        let legacy_cfg = build_config(legacy_socket_args);
        #[cfg(unix)]
        {
            assert!(
                !legacy_cfg.write_discovery_files,
                "explicit --socket without --state-dir or --listen must skip discovery files"
            );
            assert!(legacy_cfg.state_dir.as_os_str().is_empty());
            assert_eq!(
                legacy_cfg.socket_path,
                Some(PathBuf::from("/tmp/adhoc.sock"))
            );
        }

        let explicit_both_args = Args::try_parse_from([
            "jj-cc-daemon",
            "--state-dir",
            "/tmp/custom-state",
            "--socket",
            "/tmp/adhoc.sock",
        ])
        .expect("explicit state-dir + socket args should parse");
        let explicit_both = build_config(explicit_both_args);
        assert!(explicit_both.write_discovery_files);
        assert_eq!(explicit_both.state_dir, PathBuf::from("/tmp/custom-state"));
        assert_eq!(
            explicit_both.socket_path,
            Some(PathBuf::from("/tmp/adhoc.sock"))
        );

        let socket_with_tcp_args = Args::try_parse_from([
            "jj-cc-daemon",
            "--socket",
            "/tmp/adhoc.sock",
            "--listen",
            "127.0.0.1:0",
        ])
        .expect("socket + listen args should parse");
        let socket_with_tcp = build_config(socket_with_tcp_args);
        assert!(
            socket_with_tcp.write_discovery_files,
            "TCP listener needs daemon.json to publish its secret"
        );
    }
}
