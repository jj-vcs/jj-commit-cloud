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
fn gce_public_ip_requires_explicit_non_zero_allow_cidr() {
    assert!(
        parse_cli_from([
            "cargo xtask",
            "up",
            "gce",
            "--project",
            "p",
            "--gce-public-ip"
        ])
        .is_err()
    );
    assert!(
        parse_cli_from([
            "cargo xtask",
            "up",
            "gce",
            "--project",
            "p",
            "--allow-cidr",
            "203.0.113.0/24",
        ])
        .is_err()
    );
    assert!(
        parse_cli_from([
            "cargo xtask",
            "up",
            "gce",
            "--project",
            "p",
            "--gce-public-ip",
            "--allow-cidr",
            "0.0.0.0/0",
        ])
        .is_err()
    );
    let parsed = parse_cli_from([
        "cargo xtask",
        "up",
        "gce",
        "--project",
        "p",
        "--gce-public-ip",
        "--allow-cidr",
        "203.0.113.0/24,198.51.100.7/32",
    ])
    .unwrap();
    match parsed.command {
        Commands::Up {
            gce_public_ip,
            allow_cidr,
            ..
        } => {
            assert!(gce_public_ip);
            assert_eq!(allow_cidr, vec!["203.0.113.0/24", "198.51.100.7/32"]);
        }
        other => panic!("unexpected command: {other:?}"),
    }
}

#[test]
fn rejects_inapplicable_gcp_flags_on_local_and_k8s_modes() {
    for bad_args in [
        vec!["cargo xtask", "up", "sqlite", "--project", "p"],
        vec!["cargo xtask", "up", "memory", "--region", "europe-west1"],
        vec!["cargo xtask", "up", "k8s", "--peer-region", "europe-west1"],
        vec!["cargo xtask", "up", "spanner", "--image", "gcr.io/p/img"],
        vec![
            "cargo xtask",
            "up",
            "k8s-sqlite",
            "--spanner-instance",
            "inst",
        ],
        vec![
            "cargo xtask",
            "up",
            "sqlite",
            "--gce-public-ip",
            "--allow-cidr",
            "203.0.113.0/24",
        ],
        vec!["cargo xtask", "up", "cloud-run", "--gke-cluster", "c"],
    ] {
        assert!(
            parse_cli_from(bad_args.clone()).is_err(),
            "expected error for {bad_args:?}"
        );
    }

    assert!(parse_cli_from(["cargo xtask", "up", "sqlite"]).is_ok());
    assert!(parse_cli_from(["cargo xtask", "up", "postgres"]).is_ok());
    assert!(parse_cli_from(["cargo xtask", "up", "k8s-postgres"]).is_ok());
    assert!(parse_cli_from(["cargo xtask", "up", "k8s-spanner"]).is_ok());
    assert!(parse_cli_from(["cargo xtask", "up", "postgres", "--project", "p"]).is_err());
}

#[test]
fn rejects_reserved_state_env_on_cli() {
    assert!(parse_cli_from(["cargo xtask", "up", "sqlite", "--env", "state"]).is_err());
    assert!(parse_cli_from(["cargo xtask", "down", "--env", "state"]).is_err());
}

#[test]
fn rejects_zero_ports_on_up_cli() {
    assert!(parse_cli_from(["cargo xtask", "up", "sqlite", "--port", "0"]).is_err());
    assert!(parse_cli_from(["cargo xtask", "up", "multiregion", "--peer-port", "0"]).is_err());
    assert!(parse_cli_from(["cargo xtask", "up", "k8s", "--port", "0"]).is_err());
    assert!(parse_cli_from(["cargo xtask", "up", "gke", "--project", "p", "--port", "0"]).is_err());
    assert!(
        parse_cli_from([
            "cargo xtask",
            "up",
            "gce",
            "--project",
            "p",
            "--peer-region",
            "europe-west1",
            "--peer-port",
            "0",
        ])
        .is_err()
    );
    assert!(parse_cli_from(["cargo xtask", "up", "sqlite", "--port", "1"]).is_ok());
    assert!(
        parse_cli_from([
            "cargo xtask",
            "up",
            "multiregion",
            "--port",
            "65534",
            "--peer-port",
            "65535",
        ])
        .is_ok()
    );
}

#[test]
fn finalize_replaced_env_state_removes_state_when_gcp_has_no_retained_storage() {
    let tmp = tempfile::tempdir().unwrap();

    // Replaced GCP env with no created cloud storage (compute_stopped=false): remove state file.
    let active_no_storage = EnvState::new(
        "staging",
        "gce",
        "gcloud",
        "http://127.0.0.1:8080",
        None,
        state::AuthMode::None,
    );
    state::save_state(tmp.path(), &active_no_storage).unwrap();
    let carried =
        finalize_replaced_env_state(tmp.path(), "staging", true, active_no_storage).unwrap();
    assert!(carried.is_none());
    assert!(
        load_state(tmp.path(), "staging").unwrap().is_none(),
        "stale active state file must be removed when teardown left no retained storage"
    );

    // Replaced GCP env with created cloud storage retained (compute_stopped=true): carry forward.
    let mut stopped_with_storage = EnvState::new(
        "staging",
        "gce",
        "gcloud",
        "stopped",
        None,
        state::AuthMode::None,
    );
    stopped_with_storage.compute_stopped = true;
    stopped_with_storage.resources.gcs_buckets_created =
        vec!["proj-a-jj-blobs-staging-us-central1".to_string()];
    state::save_state(tmp.path(), &stopped_with_storage).unwrap();
    let carried =
        finalize_replaced_env_state(tmp.path(), "staging", true, stopped_with_storage.clone())
            .unwrap();
    assert_eq!(carried, Some(stopped_with_storage));
    assert!(
        load_state(tmp.path(), "staging").unwrap().is_some(),
        "retained storage-only state file must stay on disk"
    );

    // Replaced non-GCP env (carry_forward_gcp=false): remove state file.
    let local_state = EnvState::new(
        "local",
        "sqlite",
        "docker",
        "http://127.0.0.1:8080",
        None,
        state::AuthMode::None,
    );
    state::save_state(tmp.path(), &local_state).unwrap();
    let carried = finalize_replaced_env_state(tmp.path(), "local", false, local_state).unwrap();
    assert!(carried.is_none());
    assert!(load_state(tmp.path(), "local").unwrap().is_none());
}

#[test]
fn fallback_down_rejects_gcp_modes_without_state_file() {
    let tmp = tempfile::tempdir().unwrap();
    for mode in [DeployMode::CloudRun, DeployMode::Gce, DeployMode::Gke] {
        let err = synthesize_fallback_state(tmp.path(), "default", mode).unwrap_err();
        assert!(
            err.to_string().contains("without a state file"),
            "unexpected error for {mode:?}: {err}"
        );
    }
}

#[test]
fn next_steps_includes_env_flag_for_named_envs() {
    assert_eq!(
        format_next_steps("default"),
        "`cargo xtask smoke` or `cargo xtask down`"
    );
    assert_eq!(
        format_next_steps("staging"),
        "`cargo xtask smoke --env staging` or `cargo xtask down --env staging`"
    );
}

fn sample_gcp_opts(project: &str, peer_region: Option<&str>) -> GcpDeployOptions {
    GcpDeployOptions {
        project_id: project.to_string(),
        region: "us-central1".to_string(),
        peer_region: peer_region.map(ToString::to_string),
        spanner_instance: "jj-cc-dev".to_string(),
        spanner_database: "commit-cloud-gcp".to_string(),
        gke_cluster: "jj-cc-cluster-gcp".to_string(),
        image_override: None,
        local_port: 8080,
        local_peer_port: 8082,
        gce_public_ip: false,
        allow_cidrs: Vec::new(),
    }
}

#[test]
fn cross_mode_replacement_refuses_dropping_created_gcp_storage() {
    let mut gcp_state = EnvState::new(
        "gcp",
        "cloud-run",
        "gcloud",
        "https://jj-cc.a.run.app",
        None,
        state::AuthMode::GcloudIdentityToken,
    );
    gcp_state.project_id = Some("proj-a".to_string());
    gcp_state.resources.spanner_database = Some("commit-cloud-gcp".to_string());
    gcp_state.resources.spanner_database_created = true;
    gcp_state.resources.gcs_buckets_created = vec!["proj-a-jj-blobs-gcp-us-central1".to_string()];

    // Active GCP -> local with `--replace` must refuse before teardown/removal.
    let err = plan_existing_env_transition(&gcp_state, "gcp", DeployMode::Sqlite, true, None)
        .unwrap_err();
    assert!(
        err.to_string().contains("down --env gcp --purge")
            && err.to_string().contains("different `--env`"),
        "unexpected err: {err}"
    );

    // Stopped GCP -> local (both with and without `--replace`) must also refuse.
    gcp_state.compute_stopped = true;
    assert!(
        plan_existing_env_transition(&gcp_state, "gcp", DeployMode::Memory, true, None).is_err()
    );
    assert!(
        plan_existing_env_transition(&gcp_state, "gcp", DeployMode::K8sSpanner, false, None)
            .is_err()
    );

    // When no created cloud storage is tracked, GCP -> local `--replace` is allowed.
    let mut preexisting_only = EnvState::new(
        "gcp",
        "cloud-run",
        "gcloud",
        "https://jj-cc.a.run.app",
        None,
        state::AuthMode::GcloudIdentityToken,
    );
    preexisting_only.project_id = Some("proj-a".to_string());
    assert_eq!(
        plan_existing_env_transition(&preexisting_only, "gcp", DeployMode::Sqlite, true, None)
            .unwrap(),
        ExistingEnvPlan::ReplaceActive {
            carry_forward_gcp: false
        }
    );
}

#[test]
fn gcp_redeploy_refuses_unmatched_created_storage_and_allows_matching() {
    // `up gke` -> `down` -> `up cloud-run` must refuse dropping created GKE cluster.
    let mut stopped_gke = EnvState::new(
        "gcp",
        "gke",
        "gcloud",
        "stopped",
        None,
        state::AuthMode::None,
    );
    stopped_gke.project_id = Some("proj-a".to_string());
    stopped_gke.compute_stopped = true;
    stopped_gke.resources.gke_cluster = Some("jj-cc-cluster-gcp".to_string());
    stopped_gke.resources.gke_region = Some("us-central1".to_string());
    stopped_gke.resources.gke_cluster_created = true;
    stopped_gke.resources.spanner_instance = Some("jj-cc-dev".to_string());
    stopped_gke.resources.spanner_database = Some("commit-cloud-gcp".to_string());
    stopped_gke.resources.spanner_database_created = true;
    stopped_gke.resources.gcs_buckets_created = vec!["proj-a-jj-blobs-gcp-us-central1".to_string()];

    let opts = sample_gcp_opts("proj-a", None);
    let err = plan_existing_env_transition(
        &stopped_gke,
        "gcp",
        DeployMode::CloudRun,
        false,
        Some((GcpMode::CloudRun, &opts)),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("GKE cluster jj-cc-cluster-gcp")
            && err.to_string().contains("down --env gcp --purge"),
        "unexpected err: {err}"
    );
    assert_eq!(
        plan_existing_env_transition(
            &stopped_gke,
            "gcp",
            DeployMode::Gke,
            false,
            Some((GcpMode::Gke, &opts)),
        )
        .unwrap(),
        ExistingEnvPlan::ReuseStoppedGcpState
    );

    // Active multi-region Cloud Run `--replace` dropping `--peer-region` must refuse.
    let mut active_multi = EnvState::new(
        "gcp",
        "cloud-run",
        "gcloud",
        "https://jj-cc.a.run.app",
        Some("https://jj-cc-eu.a.run.app".to_string()),
        state::AuthMode::GcloudIdentityToken,
    );
    active_multi.project_id = Some("proj-a".to_string());
    active_multi.resources.gcs_buckets_created = vec![
        "proj-a-jj-blobs-gcp-us-central1".to_string(),
        "proj-a-jj-blobs-gcp-europe-west1".to_string(),
    ];
    let err = plan_existing_env_transition(
        &active_multi,
        "gcp",
        DeployMode::CloudRun,
        true,
        Some((GcpMode::CloudRun, &opts)),
    )
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("gs://proj-a-jj-blobs-gcp-europe-west1"),
        "unexpected err: {err}"
    );

    let matching_multi = sample_gcp_opts("proj-a", Some("europe-west1"));
    assert_eq!(
        plan_existing_env_transition(
            &active_multi,
            "gcp",
            DeployMode::CloudRun,
            true,
            Some((GcpMode::CloudRun, &matching_multi)),
        )
        .unwrap(),
        ExistingEnvPlan::ReplaceActive {
            carry_forward_gcp: true
        }
    );
}

#[test]
fn detect_kind_cluster_runtime_checks_alternate_runtime_when_primary_misses() {
    let mut checked = Vec::new();
    let found = detect_kind_cluster_runtime("jj-cc-dev", "docker", |_, rt| {
        checked.push(rt.to_string());
        Ok(rt == "podman")
    });
    assert_eq!(found.as_deref(), Some("podman"));
    assert_eq!(checked, vec!["docker", "podman"]);

    let mut checked_rev = Vec::new();
    let found_rev = detect_kind_cluster_runtime("jj-cc-dev", "podman", |_, rt| {
        checked_rev.push(rt.to_string());
        Ok(rt == "docker")
    });
    assert_eq!(found_rev.as_deref(), Some("docker"));
    assert_eq!(checked_rev, vec!["podman", "docker"]);

    let none = detect_kind_cluster_runtime("jj-cc-dev", "docker", |_, _| Ok(false));
    assert_eq!(none, None);
}

#[test]
fn build_gcp_deploy_options_rejects_missing_blank_and_invalid_fields() {
    for bad_project in [None, Some(String::new()), Some("   ".to_string())] {
        let err = build_gcp_deploy_options(
            "alice",
            GcpMode::CloudRun,
            bad_project,
            "us-central1".to_string(),
            None,
            "jj-cc-dev".to_string(),
            None,
            None,
            None,
            8080,
            8082,
            false,
            Vec::new(),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("--project") && err.to_string().contains("PROJECT_ID"),
            "unexpected err: {err}"
        );
    }

    // Valid project with surrounding whitespace is trimmed.
    let opts = build_gcp_deploy_options(
        "alice",
        GcpMode::Gke,
        Some(" proj-a ".to_string()),
        "us-central1".to_string(),
        None,
        "jj-cc-dev".to_string(),
        None,
        None,
        None,
        8080,
        8082,
        false,
        Vec::new(),
    )
    .unwrap();
    assert_eq!(opts.project_id, "proj-a");
    assert_eq!(opts.spanner_database, "commit-cloud-alice");
    assert_eq!(opts.gke_cluster, "jj-cc-cluster-alice");
}

#[test]
fn smoke_and_bench_cli_normalize_blank_urls_handle_release_and_reject_server_with_ephemeral() {
    let smoke = parse_cli_from([
        "cargo xtask",
        "smoke",
        "--server",
        "  ",
        "--peer-server",
        "",
    ])
    .unwrap();
    match smoke.command {
        Commands::Smoke {
            server,
            peer_server,
            ..
        } => {
            assert_eq!(server, None);
            assert_eq!(peer_server, None);
        }
        other => panic!("unexpected command: {other:?}"),
    }

    let err = parse_cli_from([
        "cargo xtask",
        "bench",
        "--server",
        "http://127.0.0.1:8080",
        "--ephemeral",
    ])
    .unwrap_err();
    assert!(
        err.to_string().contains("--server") && err.to_string().contains("--ephemeral"),
        "unexpected conflict err: {err}"
    );

    let bench_release =
        parse_cli_from(["cargo xtask", "bench", "--ephemeral", "--release"]).unwrap();
    match bench_release.command {
        Commands::Bench {
            server,
            ephemeral,
            release,
            ..
        } => {
            assert_eq!(server, None);
            assert!(ephemeral);
            assert!(release);
        }
        other => panic!("unexpected command: {other:?}"),
    }

    // Ambient BENCH_SERVER_URL yields to `--ephemeral`, while blank env values normalize to None.
    assert_eq!(
        resolve_bench_server_override(
            true,
            Some("http://ambient:8080".to_string()),
            Some(ValueSource::EnvVariable),
        )
        .unwrap(),
        None
    );
    assert!(
        resolve_bench_server_override(
            true,
            Some("http://cli:8080".to_string()),
            Some(ValueSource::CommandLine),
        )
        .is_err()
    );
    assert_eq!(
        resolve_bench_server_override(
            false,
            Some("   ".to_string()),
            Some(ValueSource::EnvVariable),
        )
        .unwrap(),
        None
    );
    assert_eq!(
        resolve_bench_server_override(
            false,
            Some(" http://127.0.0.1:8080 ".to_string()),
            Some(ValueSource::EnvVariable),
        )
        .unwrap()
        .as_deref(),
        Some("http://127.0.0.1:8080")
    );
}
