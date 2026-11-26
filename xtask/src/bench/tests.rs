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

use super::*;

#[test]
fn exact_line_matching_rejects_prefix_collision_between_s1_and_s10() {
    let log_out = "\
aaaaaaaa11111111\tbench-commit:a1:s0\n\
bbbbbbbb22222222\tbench-commit:a1:s10\n";
    let markers = vec![
        "bench-commit:a1:s0".to_string(),
        "bench-commit:a1:s1".to_string(),
    ];
    let err = extract_agent_commit_ids(log_out, &markers).unwrap_err();
    assert!(
        err.contains("bench-commit:a1:s1"),
        "unexpected error: {err}"
    );
}

#[test]
fn verify_repo_commits_checks_both_commit_id_and_exact_marker() {
    let log_out = "\
aaaaaaaa11111111\tbench-commit:a0:s0\n\
cccccccc33333333\tbench-commit:a0:s1\n";
    let pairs = vec![
        (
            "aaaaaaaa11111111".to_string(),
            "bench-commit:a0:s0".to_string(),
        ),
        (
            "cccccccc33333333".to_string(),
            "bench-commit:a0:s1".to_string(),
        ),
    ];
    assert_eq!(verify_repo_commits("repo-a0", log_out, &pairs).unwrap(), 2);

    let wrong_id = vec![(
        "ffffffff99999999".to_string(),
        "bench-commit:a0:s1".to_string(),
    )];
    assert!(verify_repo_commits("repo-a0", log_out, &wrong_id).is_err());
}

#[test]
fn verification_summary_prints_not_verified_when_skipped() {
    assert_eq!(format_verification_summary(None, 36), "not verified");
    assert_eq!(
        format_verification_summary(Some(36), 36),
        "36/36 (0 lost commits)"
    );
    assert_eq!(
        format_verification_summary(Some(35), 36),
        "35/36 (1 lost commits)"
    );
}

#[test]
fn bench_jj_command_isolates_jj_config_and_strips_ambient_daemon_and_unset_auth_vars() {
    let cmd = build_bench_jj_command(
        Path::new("jj"),
        Path::new("/tmp"),
        Path::new("/tmp/xdg"),
        None,
        None,
        &[],
    );
    let envs: HashMap<&OsStr, Option<&OsStr>> = cmd.as_std().get_envs().collect();
    assert_eq!(
        envs.get(OsStr::new("JJ_CONFIG")),
        Some(&Some(OsStr::new("")))
    );
    assert_eq!(
        envs.get(OsStr::new("XDG_CONFIG_HOME")),
        Some(&Some(OsStr::new("/tmp/xdg")))
    );
    for var in [
        "JJ_CC_SYNC_PROJECT_COMMITS",
        "JJ_CC_DAEMON_SOCKET",
        "JJ_CC_DAEMON_LISTEN",
        "JJ_CC_DAEMON_CONFIG",
        "JJ_CC_DAEMON_ALLOW_UPSTREAM",
        "JJ_CC_DAEMON_MEMORY_MB",
        "JJ_CC_DAEMON_IDLE_SECS",
        "JJ_CC_UPSTREAM",
        "JJ_CC_AUTH_TOKEN",
    ] {
        assert_eq!(
            envs.get(OsStr::new(var)),
            Some(&None),
            "expected {var} to be removed"
        );
    }

    let authed = build_bench_jj_command(
        Path::new("jj"),
        Path::new("/tmp"),
        Path::new("/tmp/xdg"),
        Some("secret-tok"),
        None,
        &[],
    );
    let token_val = authed
        .as_std()
        .get_envs()
        .find(|(k, _)| *k == OsStr::new("JJ_CC_AUTH_TOKEN"))
        .and_then(|(_, v)| v);
    assert_eq!(token_val, Some(OsStr::new("secret-tok")));

    let guard = BenchDaemonGuard {
        jj_bin: PathBuf::from("jj"),
        cwd: PathBuf::from("/tmp"),
        xdg_dir: PathBuf::from("/tmp/xdg"),
        daemon_bin: PathBuf::from("/tmp/jj-cc-daemon"),
        state_dir: PathBuf::from("/tmp/state"),
        cache_dir: PathBuf::from("/tmp/cache"),
        active: false,
    };
    let stop_cmd = guard.build_stop_command();
    let stop_envs: HashMap<&OsStr, Option<&OsStr>> = stop_cmd.get_envs().collect();
    assert_eq!(
        stop_envs.get(OsStr::new("JJ_CONFIG")),
        Some(&Some(OsStr::new("")))
    );
    assert_eq!(
        stop_envs.get(OsStr::new("XDG_CONFIG_HOME")),
        Some(&Some(OsStr::new("/tmp/xdg")))
    );
    for var in BENCH_SANITIZED_CLIENT_ENV_VARS {
        assert_eq!(
            stop_envs.get(OsStr::new(var)),
            Some(&None),
            "expected daemon stop command to remove {var}"
        );
    }
}

fn sample_bench_opts() -> BenchOptions {
    BenchOptions {
        env_name: "default".to_string(),
        server_override: None,
        ephemeral: false,
        agents: 4,
        commits_per_agent: 2,
        isolated_repos: false,
        use_daemon: false,
        release: false,
        jj_bin_override: None,
    }
}

#[test]
fn validate_bench_target_options_normalizes_blank_server_and_rejects_conflict() {
    let mut blank_ephemeral = sample_bench_opts();
    blank_ephemeral.server_override = Some("   ".to_string());
    blank_ephemeral.ephemeral = true;
    assert_eq!(
        validate_bench_target_options(&blank_ephemeral).unwrap(),
        None
    );

    let mut blank_state = sample_bench_opts();
    blank_state.server_override = Some("".to_string());
    assert_eq!(validate_bench_target_options(&blank_state).unwrap(), None);

    let mut explicit = sample_bench_opts();
    explicit.server_override = Some(" http://127.0.0.1:8080 ".to_string());
    assert_eq!(
        validate_bench_target_options(&explicit).unwrap().as_deref(),
        Some("http://127.0.0.1:8080")
    );

    let mut conflict = sample_bench_opts();
    conflict.server_override = Some("http://127.0.0.1:8080".to_string());
    conflict.ephemeral = true;
    let err = validate_bench_target_options(&conflict).unwrap_err();
    assert!(
        err.to_string().contains("--server") && err.to_string().contains("--ephemeral"),
        "unexpected conflict error: {err}"
    );
}

#[test]
fn format_bench_build_profile_reports_debug_release_and_custom_jj() {
    let mut opts = sample_bench_opts();
    assert!(format_bench_build_profile(&opts).contains("debug"));

    opts.release = true;
    assert_eq!(format_bench_build_profile(&opts), "release");

    opts.jj_bin_override = Some(PathBuf::from("/usr/local/bin/jj"));
    assert_eq!(format_bench_build_profile(&opts), "custom --jj-bin");

    opts.use_daemon = true;
    assert_eq!(
        format_bench_build_profile(&opts),
        "custom --jj-bin (release for managed binaries)"
    );

    opts.release = false;
    assert_eq!(
        format_bench_build_profile(&opts),
        "custom --jj-bin (debug for managed binaries)"
    );
}
