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

use std::fs;
use std::path::Path;
use std::process::Command;

use super::infra::{
    apply_gke_auth_secret, classify_gcloud_exists, ensure_gke_workload_absent,
    gce_zone_list_filter, locate_gcloud_with, locate_kubectl_with, parse_gce_nat_ip,
    pick_zone_from_list,
};
use super::teardown::{down_gcp_resources, down_gcp_with, teardown_result};
use crate::state::{AuthMode, save_state};

use super::*;
use crate::state::{load_state, workspace_root};

#[test]
fn allow_cidr_rejects_open_world_and_ipv6_and_accepts_explicit_ipv4_ranges() {
    assert!(parse_allow_cidr("0.0.0.0/0").is_err());
    assert!(parse_allow_cidr("::/0").is_err());
    assert!(parse_allow_cidr("203.0.113.5").is_err());
    assert!(parse_allow_cidr("203.0.113.0/33").is_err());
    assert!(parse_allow_cidr("2001:db8::/129").is_err());
    assert!(parse_allow_cidr("not-an-ip/24").is_err());

    let v6_err = parse_allow_cidr("2001:db8::/32").unwrap_err();
    assert!(
        v6_err.contains("IPv4-only") && v6_err.contains("xtask up gce"),
        "expected IPv4-only GCE error, got: {v6_err}"
    );

    assert_eq!(
        parse_allow_cidr("203.0.113.0/24").unwrap(),
        "203.0.113.0/24"
    );
    assert_eq!(
        parse_allow_cidr("198.51.100.7/32").unwrap(),
        "198.51.100.7/32"
    );
}

#[test]
fn default_bucket_and_resource_names_include_env_name() {
    assert_eq!(
        blob_bucket_name("example-proj", "default", "us-central1").unwrap(),
        "example-proj-jj-blobs-default-us-central1"
    );
    assert_eq!(
        blob_bucket_name("example-proj", "alice", "europe-west1").unwrap(),
        "example-proj-jj-blobs-alice-europe-west1"
    );
    assert_eq!(env_resource_name("commit-cloud", "default"), "commit-cloud");
    assert_eq!(
        env_resource_name("commit-cloud", "alice"),
        "commit-cloud-alice"
    );

    assert!(validate_gcp_env_name("default").is_ok());
    assert!(validate_gcp_env_name("alice-1").is_ok());
    assert!(validate_gcp_env_name("").is_err());
    assert!(validate_gcp_env_name("Upper").is_err());
    assert!(validate_gcp_env_name("trailing-").is_err());
    assert!(validate_gcp_env_name("way-too-long-environment-name").is_err());

    let t1 = generate_auth_token();
    let t2 = generate_auth_token();
    assert_eq!(t1.len(), 64);
    assert_ne!(t1, t2);
}

#[test]
fn classify_gcloud_exists_propagates_non_not_found_errors() {
    assert!(classify_gcloud_exists(true, "", "spanner instances describe").unwrap());
    assert!(
        !classify_gcloud_exists(
            false,
            "ERROR: (gcloud.spanner.instances.describe) NOT_FOUND: Instance not found",
            "spanner instances describe",
        )
        .unwrap()
    );
    assert!(
        !classify_gcloud_exists(
            false,
            "ERROR: (gcloud.iam.service-accounts.describe) NOT_FOUND: Unknown service account",
            "iam service-accounts describe",
        )
        .unwrap()
    );
    assert!(
        !classify_gcloud_exists(
            false,
            "ERROR: HTTPError 404: The specified bucket does not exist.",
            "storage buckets describe",
        )
        .unwrap()
    );
    let perm_err = classify_gcloud_exists(
        false,
        "ERROR: (gcloud.spanner.instances.describe) PERMISSION_DENIED: caller lacks permission",
        "spanner instances describe",
    )
    .unwrap_err();
    assert!(
        perm_err.to_string().contains("PERMISSION_DENIED"),
        "unexpected err: {perm_err}"
    );
    assert!(
        classify_gcloud_exists(false, "ERROR: network timeout", "storage buckets describe")
            .is_err()
    );
    assert!(classify_gcloud_exists(false, "", "compute instances describe").is_err());
}

#[test]
fn pick_zone_from_list_prefers_zone_a_and_falls_back_cleanly() {
    assert_eq!(
        pick_zone_from_list(
            "us-central1",
            "us-central1-c\nus-central1-a\nus-central1-b\n"
        )
        .unwrap(),
        "us-central1-a"
    );
    assert_eq!(
        pick_zone_from_list(
            "europe-west1",
            "europe-west1-d\neurope-west1-b\neurope-west1-c\n"
        )
        .unwrap(),
        "europe-west1-b"
    );
    assert_eq!(
        pick_zone_from_list(
            "europe-west1",
            "europe-west10-a\neurope-west1-d\neurope-west1-b\n"
        )
        .unwrap(),
        "europe-west1-b"
    );
    assert!(pick_zone_from_list("europe-west1", "europe-west10-a\n").is_err());
    assert!(pick_zone_from_list("europe-west1", "  \n ").is_err());
    assert_eq!(
        gce_zone_list_filter("europe-west1"),
        "name~^europe-west1-[a-z]+$ AND status=UP"
    );
}

fn sample_gcp_opts() -> GcpDeployOptions {
    GcpDeployOptions {
        project_id: "proj-a".to_string(),
        region: "us-central1".to_string(),
        peer_region: None,
        spanner_instance: "jj-cc-dev".to_string(),
        spanner_database: "commit-cloud-alice".to_string(),
        gke_cluster: "jj-cc-cluster-alice".to_string(),
        image_override: None,
        local_port: 8080,
        local_peer_port: 8082,
        gce_public_ip: false,
        allow_cidrs: Vec::new(),
    }
}

#[test]
fn validate_gcp_options_rejects_same_peer_region_and_misplaced_gce_public_ip() {
    let mut same_peer = sample_gcp_opts();
    same_peer.peer_region = Some("us-central1".to_string());
    let err = validate_gcp_options("alice", GcpMode::CloudRun, &same_peer).unwrap_err();
    assert!(err.to_string().contains("--peer-region"));
    assert!(validate_gcp_options("alice", GcpMode::Gce, &same_peer).is_err());

    let mut gke_peer = sample_gcp_opts();
    gke_peer.peer_region = Some("europe-west1".to_string());
    assert!(validate_gcp_options("alice", GcpMode::Gke, &gke_peer).is_err());
    assert!(validate_gcp_options("alice", GcpMode::CloudRun, &gke_peer).is_ok());

    let mut pub_ip = sample_gcp_opts();
    pub_ip.gce_public_ip = true;
    pub_ip.allow_cidrs = vec!["203.0.113.0/24".to_string()];
    assert!(validate_gcp_options("alice", GcpMode::CloudRun, &pub_ip).is_err());
    assert!(validate_gcp_options("alice", GcpMode::Gke, &pub_ip).is_err());
    assert!(validate_gcp_options("alice", GcpMode::Gce, &pub_ip).is_ok());

    let mut pub_ip_no_cidr = sample_gcp_opts();
    pub_ip_no_cidr.gce_public_ip = true;
    assert!(validate_gcp_options("alice", GcpMode::Gce, &pub_ip_no_cidr).is_err());

    let mut iap_port_clash = sample_gcp_opts();
    iap_port_clash.peer_region = Some("europe-west1".to_string());
    iap_port_clash.local_peer_port = 8081;
    assert!(validate_gcp_options("alice", GcpMode::Gce, &iap_port_clash).is_err());

    let mut zero_primary = sample_gcp_opts();
    zero_primary.local_port = 0;
    assert!(validate_gcp_options("alice", GcpMode::Gke, &zero_primary).is_err());
    assert!(validate_gcp_options("alice", GcpMode::Gce, &zero_primary).is_err());

    let mut zero_peer = sample_gcp_opts();
    zero_peer.peer_region = Some("europe-west1".to_string());
    zero_peer.local_peer_port = 0;
    assert!(validate_gcp_options("alice", GcpMode::Gce, &zero_peer).is_err());
}

#[test]
fn validate_gcp_options_rejects_blank_and_malformed_identifiers() {
    for bad_project in [
        "", "   ", "Proj-A", "bad_proj", "-proj", "proj-", "p@bad", "p,bad",
    ] {
        let mut opts = sample_gcp_opts();
        opts.project_id = bad_project.to_string();
        assert!(
            validate_gcp_options("alice", GcpMode::CloudRun, &opts).is_err(),
            "expected project `{bad_project}` to fail"
        );
    }

    for bad_region in [
        "",
        "  ",
        "US-Central1",
        "us_central1",
        "us-central1/",
        "us@1",
    ] {
        let mut opts = sample_gcp_opts();
        opts.region = bad_region.to_string();
        assert!(
            validate_gcp_options("alice", GcpMode::CloudRun, &opts).is_err(),
            "expected region `{bad_region}` to fail"
        );

        let mut peer_opts = sample_gcp_opts();
        peer_opts.peer_region = Some(bad_region.to_string());
        assert!(
            validate_gcp_options("alice", GcpMode::CloudRun, &peer_opts).is_err(),
            "expected peer_region `{bad_region}` to fail"
        );
    }

    for bad_inst in ["", "  ", "Bad_Inst", "inst-", "inst/bad"] {
        let mut opts = sample_gcp_opts();
        opts.spanner_instance = bad_inst.to_string();
        assert!(
            validate_gcp_options("alice", GcpMode::CloudRun, &opts).is_err(),
            "expected spanner_instance `{bad_inst}` to fail"
        );
    }

    for bad_db in ["", "  ", "BadDb", "db-", "db_", "db/bad", &"a".repeat(31)] {
        let mut opts = sample_gcp_opts();
        opts.spanner_database = bad_db.to_string();
        assert!(
            validate_gcp_options("alice", GcpMode::CloudRun, &opts).is_err(),
            "expected spanner_database `{bad_db}` to fail"
        );
    }
    let mut underscore_db = sample_gcp_opts();
    underscore_db.spanner_database = "commit_cloud_1".to_string();
    assert!(validate_gcp_options("alice", GcpMode::CloudRun, &underscore_db).is_ok());

    for bad_cluster in ["", "  ", "Bad_Cluster", "cluster-", &"a".repeat(41)] {
        let mut opts = sample_gcp_opts();
        opts.gke_cluster = bad_cluster.to_string();
        assert!(
            validate_gcp_options("alice", GcpMode::Gke, &opts).is_err(),
            "expected gke_cluster `{bad_cluster}` to fail"
        );
    }

    for bad_image in [
        "",
        "   ",
        "gcr.io/p/img with space",
        "gcr.io/p/\"img\"",
        "-bad",
    ] {
        let mut opts = sample_gcp_opts();
        opts.image_override = Some(bad_image.to_string());
        assert!(
            validate_gcp_options("alice", GcpMode::Gke, &opts).is_err(),
            "expected image `{bad_image}` to fail"
        );
    }
    let mut digest_image = sample_gcp_opts();
    digest_image.image_override = Some(
        "us-central1-docker.pkg.dev/proj-a/jj-commit-cloud/jj-cc-server@sha256:abcdef012345"
            .to_string(),
    );
    assert!(validate_gcp_options("alice", GcpMode::Gke, &digest_image).is_ok());
}

#[test]
fn ensure_gcp_ownership_preserved_refuses_dropping_any_created_cloud_resource() {
    let mut prev_gke = EnvState::new("alice", "gke", "gcloud", "stopped", None, AuthMode::None);
    prev_gke.project_id = Some("proj-a".to_string());
    prev_gke.compute_stopped = true;
    prev_gke.resources.spanner_instance = Some("jj-cc-dev".to_string());
    prev_gke.resources.spanner_instance_created = true;
    prev_gke.resources.spanner_database = Some("commit-cloud-alice".to_string());
    prev_gke.resources.spanner_database_created = true;
    prev_gke.resources.gke_cluster = Some("jj-cc-cluster-alice".to_string());
    prev_gke.resources.gke_region = Some("us-central1".to_string());
    prev_gke.resources.gke_cluster_created = true;
    prev_gke.resources.gcs_buckets_created = vec!["proj-a-jj-blobs-alice-us-central1".to_string()];

    // Matching GKE redeploy keeps all created resources.
    let opts = sample_gcp_opts();
    assert!(ensure_gcp_ownership_preserved(&prev_gke, "alice", GcpMode::Gke, &opts).is_ok());

    // Switching GKE -> Cloud Run would drop tracked created GKE cluster.
    let err =
        ensure_gcp_ownership_preserved(&prev_gke, "alice", GcpMode::CloudRun, &opts).unwrap_err();
    assert!(
        err.to_string().contains("GKE cluster jj-cc-cluster-alice")
            && err.to_string().contains("down --env alice --purge"),
        "unexpected err: {err}"
    );

    // Changing project drops all created storage.
    let mut other_proj = sample_gcp_opts();
    other_proj.project_id = "proj-b".to_string();
    assert!(ensure_gcp_ownership_preserved(&prev_gke, "alice", GcpMode::Gke, &other_proj).is_err());

    // Changing Spanner database or instance drops created Spanner storage.
    let mut other_db = sample_gcp_opts();
    other_db.spanner_database = "commit-cloud-other".to_string();
    let err =
        ensure_gcp_ownership_preserved(&prev_gke, "alice", GcpMode::Gke, &other_db).unwrap_err();
    assert!(err.to_string().contains("Spanner database"));

    let mut other_inst = sample_gcp_opts();
    other_inst.spanner_instance = "other-inst".to_string();
    let err =
        ensure_gcp_ownership_preserved(&prev_gke, "alice", GcpMode::Gke, &other_inst).unwrap_err();
    assert!(err.to_string().contains("Spanner instance"));

    // Dropping --peer-region drops a created peer Cloud Storage bucket.
    let mut prev_multi = EnvState::new(
        "alice",
        "cloud-run",
        "gcloud",
        "stopped",
        None,
        AuthMode::GcloudIdentityToken,
    );
    prev_multi.project_id = Some("proj-a".to_string());
    prev_multi.compute_stopped = true;
    prev_multi.resources.gcs_buckets_created = vec![
        "proj-a-jj-blobs-alice-us-central1".to_string(),
        "proj-a-jj-blobs-alice-europe-west1".to_string(),
    ];
    let err =
        ensure_gcp_ownership_preserved(&prev_multi, "alice", GcpMode::CloudRun, &opts).unwrap_err();
    assert!(
        err.to_string()
            .contains("gs://proj-a-jj-blobs-alice-europe-west1"),
        "unexpected err: {err}"
    );

    let mut matching_multi = sample_gcp_opts();
    matching_multi.peer_region = Some("europe-west1".to_string());
    assert!(
        ensure_gcp_ownership_preserved(&prev_multi, "alice", GcpMode::CloudRun, &matching_multi)
            .is_ok()
    );
}

#[test]
fn parse_gce_nat_ip_validates_non_empty_ip() {
    assert_eq!(
        parse_gce_nat_ip("vm1", "203.0.113.42\n").unwrap(),
        "203.0.113.42"
    );
    assert!(parse_gce_nat_ip("vm1", "").is_err());
    assert!(parse_gce_nat_ip("vm1", "   \n").is_err());
    assert!(parse_gce_nat_ip("vm1", "not-an-ip").is_err());
}

#[test]
fn carry_forward_gcp_ownership_preserves_only_matching_created_resources() {
    let mut prev = EnvState::new("alice", "gke", "gcloud", "stopped", None, AuthMode::None);
    prev.project_id = Some("proj-a".to_string());
    prev.resources.spanner_instance = Some("jj-cc-dev".to_string());
    prev.resources.spanner_instance_created = true;
    prev.resources.spanner_database = Some("commit-cloud-alice".to_string());
    prev.resources.spanner_database_created = true;
    prev.resources.gke_cluster = Some("jj-cc-cluster-alice".to_string());
    prev.resources.gke_region = Some("us-central1".to_string());
    prev.resources.gke_cluster_created = true;
    prev.resources.gcs_buckets_created = vec![
        "proj-a-jj-blobs-alice-us-central1".to_string(),
        "proj-a-jj-blobs-alice-asia-east1".to_string(),
    ];

    let opts = GcpDeployOptions {
        project_id: "proj-a".to_string(),
        region: "us-central1".to_string(),
        peer_region: None,
        spanner_instance: "jj-cc-dev".to_string(),
        spanner_database: "commit-cloud-alice".to_string(),
        gke_cluster: "jj-cc-cluster-alice".to_string(),
        image_override: None,
        local_port: 8080,
        local_peer_port: 8082,
        gce_public_ip: false,
        allow_cidrs: Vec::new(),
    };
    let mut next = EnvState::new("alice", "gke", "gcloud", "pending", None, AuthMode::None);
    next.project_id = Some("proj-a".to_string());
    carry_forward_gcp_ownership(
        Some(&prev),
        &mut next,
        &opts,
        "proj-a-jj-blobs-alice-us-central1",
        None,
        GcpMode::Gke,
    );
    assert!(next.resources.spanner_instance_created);
    assert!(next.resources.spanner_database_created);
    assert!(next.resources.gke_cluster_created);
    assert_eq!(
        next.resources.gcs_buckets_created,
        vec!["proj-a-jj-blobs-alice-us-central1".to_string()]
    );

    // Different project never carries ownership forward.
    let mut other_proj = EnvState::new("alice", "gke", "gcloud", "pending", None, AuthMode::None);
    other_proj.project_id = Some("proj-b".to_string());
    let mut opts_b = opts;
    opts_b.project_id = "proj-b".to_string();
    carry_forward_gcp_ownership(
        Some(&prev),
        &mut other_proj,
        &opts_b,
        "proj-b-jj-blobs-alice-us-central1",
        None,
        GcpMode::Gke,
    );
    assert!(!other_proj.resources.spanner_instance_created);
    assert!(!other_proj.resources.spanner_database_created);
    assert!(!other_proj.resources.gke_cluster_created);
    assert!(other_proj.resources.gcs_buckets_created.is_empty());
}

#[cfg(unix)]
fn write_executable(path: &Path, script: &str) {
    use std::os::unix::fs::PermissionsExt;
    fs::write(path, script).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

#[cfg(unix)]
#[test]
fn apply_gke_auth_secret_pipes_secret_json_without_exposing_token_in_argv() {
    let _proc_guard = crate::lock_test_proc();
    let tmp = tempfile::tempdir().unwrap();
    let argv_log = tmp.path().join("argv.log");
    let stdin_log = tmp.path().join("stdin.json");
    let fake_kubectl = tmp.path().join("fake-kubectl.sh");
    write_executable(
        &fake_kubectl,
        &format!(
            "#!/bin/sh\necho \"$*\" > \"{}\"\ncat > \"{}\"\nexit 0\n",
            argv_log.display(),
            stdin_log.display()
        ),
    );

    apply_gke_auth_secret(&fake_kubectl, "gke_p_r_c", "jj-cc-server-auth", "tok-123").unwrap();

    let argv = fs::read_to_string(&argv_log).unwrap();
    assert!(argv.contains("--context gke_p_r_c --namespace default apply -f -"));
    assert!(!argv.contains("tok-123"), "token must never appear in argv");

    let body: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&stdin_log).unwrap()).unwrap();
    assert_eq!(body["kind"], "Secret");
    assert_eq!(body["metadata"]["name"], "jj-cc-server-auth");
    assert_eq!(body["stringData"]["tokens"], "tok-123\n");
}

#[cfg(unix)]
#[test]
fn down_purge_only_deletes_created_resources_and_passes_kube_context() {
    let _proc_guard = crate::lock_test_proc();
    let tmp = tempfile::tempdir().unwrap();
    let calls_log = tmp.path().join("calls.log");
    let fake_gcloud = tmp.path().join("fake-gcloud.sh");
    let fake_kubectl = tmp.path().join("fake-kubectl.sh");
    write_executable(
        &fake_gcloud,
        &format!(
            "#!/bin/sh\necho \"gcloud $*\" >> \"{}\"\nexit 0\n",
            calls_log.display()
        ),
    );
    write_executable(
        &fake_kubectl,
        &format!(
            "#!/bin/sh\necho \"kubectl $*\" >> \"{}\"\nexit 0\n",
            calls_log.display()
        ),
    );

    let manifest_path = tmp.path().join("rendered-gke.yaml");
    fs::write(&manifest_path, "kind: Service\n").unwrap();

    let mut st = EnvState::new(
        "alice",
        "gke",
        "gcloud",
        "http://127.0.0.1:8080",
        None,
        AuthMode::None,
    );
    st.project_id = Some("example-proj".to_string());
    st.resources.gke_cluster = Some("preexisting-cluster".to_string());
    st.resources.gke_region = Some("us-central1".to_string());
    st.resources.gke_cluster_created = false;
    st.resources.kube_context =
        Some("gke_example-proj_us-central1_preexisting-cluster".to_string());
    st.resources.kube_manifest = Some(manifest_path.display().to_string());
    st.resources.gke_auth_secret = Some("jj-cc-server-auth".to_string());
    st.resources.spanner_instance = Some("preexisting-inst".to_string());
    st.resources.spanner_instance_created = false;
    st.resources.spanner_database = Some("commit-cloud-alice".to_string());
    st.resources.spanner_database_created = true;
    st.resources.gcs_buckets = vec![
        "preexisting-bkt".to_string(),
        "example-proj-jj-blobs-alice-us-central1".to_string(),
    ];
    st.resources.gcs_buckets_created = vec!["example-proj-jj-blobs-alice-us-central1".to_string()];
    save_state(tmp.path(), &st).unwrap();

    down_gcp_resources(
        &fake_gcloud,
        || Ok(fake_kubectl.clone()),
        tmp.path(),
        &mut st,
        true,
        Vec::new(),
    )
    .unwrap();

    let calls = fs::read_to_string(&calls_log).unwrap();
    assert!(
        calls.contains(
            "kubectl --context gke_example-proj_us-central1_preexisting-cluster --namespace default delete --ignore-not-found -f"
        ),
        "expected explicit --context and --namespace in kubectl call, got:\n{calls}"
    );
    assert!(
        calls.contains(
            "kubectl --context gke_example-proj_us-central1_preexisting-cluster --namespace default delete --ignore-not-found secret jj-cc-server-auth"
        ),
        "expected GKE auth secret deletion with --namespace default, got:\n{calls}"
    );
    assert!(
        calls.contains(
            "gcloud spanner databases delete commit-cloud-alice --instance=preexisting-inst"
        ),
        "expected created Spanner database deletion, got:\n{calls}"
    );
    assert!(
        !calls.contains("spanner instances delete"),
        "must never delete pre-existing Spanner instance:\n{calls}"
    );
    assert!(
        !calls.contains("container clusters delete"),
        "must never delete pre-existing GKE cluster:\n{calls}"
    );
    assert!(
        calls.contains("storage rm --recursive gs://example-proj-jj-blobs-alice-us-central1"),
        "expected created bucket deletion, got:\n{calls}"
    );
    assert!(
        !calls.contains("preexisting-bkt"),
        "must never delete pre-existing bucket:\n{calls}"
    );
}

#[cfg(unix)]
#[test]
fn down_without_purge_retains_storage_only_state_when_created_storage_exists() {
    let _proc_guard = crate::lock_test_proc();
    let tmp = tempfile::tempdir().unwrap();
    let fake_gcloud = tmp.path().join("fake-gcloud.sh");
    let fake_kubectl = tmp.path().join("fake-kubectl.sh");
    write_executable(&fake_gcloud, "#!/bin/sh\nexit 0\n");
    write_executable(&fake_kubectl, "#!/bin/sh\nexit 0\n");

    let mut st = EnvState::new(
        "alice",
        "cloud-run",
        "gcloud",
        "https://jj-cc.a.run.app",
        None,
        AuthMode::GcloudIdentityToken,
    );
    st.project_id = Some("example-proj".to_string());
    st.resources.cloud_run_services = vec!["us-central1/jj-cc-server-alice".to_string()];
    st.resources.spanner_instance = Some("jj-cc-dev".to_string());
    st.resources.spanner_instance_created = true;
    st.resources.gcs_buckets = vec!["example-proj-jj-blobs-alice-us-central1".to_string()];
    st.resources.gcs_buckets_created = vec!["example-proj-jj-blobs-alice-us-central1".to_string()];
    save_state(tmp.path(), &st).unwrap();

    down_gcp_resources(
        &fake_gcloud,
        || Ok(fake_kubectl.clone()),
        tmp.path(),
        &mut st,
        false,
        Vec::new(),
    )
    .unwrap();

    assert!(st.compute_stopped);
    assert!(st.resources.cloud_run_services.is_empty());
    assert!(st.resources.has_created_cloud_storage());
    let persisted = load_state(tmp.path(), "alice").unwrap().unwrap();
    assert!(persisted.compute_stopped);
    assert!(persisted.resources.spanner_instance_created);
}

#[cfg(unix)]
#[test]
fn down_purge_deletes_empty_spanner_instance_when_db_was_preexisting_or_uncreated() {
    let _proc_guard = crate::lock_test_proc();
    let tmp = tempfile::tempdir().unwrap();
    let calls_log = tmp.path().join("calls.log");
    let fake_gcloud = tmp.path().join("fake-gcloud.sh");
    let fake_kubectl = tmp.path().join("fake-kubectl.sh");
    write_executable(
        &fake_gcloud,
        &format!(
            "#!/bin/sh\necho \"gcloud $*\" >> \"{}\"\nexit 0\n",
            calls_log.display()
        ),
    );
    write_executable(&fake_kubectl, "#!/bin/sh\nexit 0\n");

    let mut st = EnvState::new(
        "alice",
        "cloud-run",
        "gcloud",
        "pending",
        None,
        AuthMode::None,
    );
    st.project_id = Some("example-proj".to_string());
    st.resources.spanner_instance = Some("jj-cc-dev".to_string());
    st.resources.spanner_instance_created = true;
    st.resources.spanner_database = Some("commit-cloud-alice".to_string());
    st.resources.spanner_database_created = false;
    save_state(tmp.path(), &st).unwrap();

    down_gcp_resources(
        &fake_gcloud,
        || Ok(fake_kubectl.clone()),
        tmp.path(),
        &mut st,
        true,
        Vec::new(),
    )
    .unwrap();

    let calls = fs::read_to_string(&calls_log).unwrap();
    assert!(
        calls.contains("gcloud spanner instances delete jj-cc-dev"),
        "expected empty created instance to be deleted, got:\n{calls}"
    );
}

#[cfg(unix)]
#[test]
fn down_purge_retains_state_when_created_spanner_instance_holds_other_databases() {
    let _proc_guard = crate::lock_test_proc();
    let tmp = tempfile::tempdir().unwrap();
    let calls_log = tmp.path().join("calls.log");
    let fake_gcloud = tmp.path().join("fake-gcloud.sh");
    let fake_kubectl = tmp.path().join("fake-kubectl.sh");
    write_executable(
        &fake_gcloud,
        &format!(
            "#!/bin/sh\necho \"gcloud $*\" >> \"{}\"\ncase \"$*\" in\n  \"spanner databases list \"*)\n    echo \"projects/example-proj/instances/jj-cc-dev/databases/commit-cloud-bob\"\n    exit 0\n    ;;\n  *)\n    exit 0\n    ;;\nesac\n",
            calls_log.display()
        ),
    );
    write_executable(&fake_kubectl, "#!/bin/sh\nexit 0\n");

    let mut st = EnvState::new(
        "alice",
        "cloud-run",
        "gcloud",
        "https://jj-cc.a.run.app",
        None,
        AuthMode::GcloudIdentityToken,
    );
    st.project_id = Some("example-proj".to_string());
    st.resources.cloud_run_services = vec!["us-central1/jj-cc-server-alice".to_string()];
    st.resources.spanner_instance = Some("jj-cc-dev".to_string());
    st.resources.spanner_instance_created = true;
    st.resources.spanner_database = Some("commit-cloud-alice".to_string());
    st.resources.spanner_database_created = true;
    st.resources.gcs_buckets = vec!["example-proj-jj-blobs-alice-us-central1".to_string()];
    st.resources.gcs_buckets_created = vec!["example-proj-jj-blobs-alice-us-central1".to_string()];
    save_state(tmp.path(), &st).unwrap();

    down_gcp_resources(
        &fake_gcloud,
        || Ok(fake_kubectl.clone()),
        tmp.path(),
        &mut st,
        true,
        Vec::new(),
    )
    .unwrap();

    let calls = fs::read_to_string(&calls_log).unwrap();
    assert!(
        calls.contains("gcloud spanner databases delete commit-cloud-alice"),
        "expected alice database to be deleted, got:\n{calls}"
    );
    assert!(
        !calls.contains("gcloud spanner instances delete"),
        "must not delete shared Spanner instance while bob's database exists:\n{calls}"
    );
    assert!(
        st.compute_stopped,
        "state must stay retained with compute_stopped=true while created Spanner instance remains"
    );
    assert_eq!(st.resources.spanner_instance.as_deref(), Some("jj-cc-dev"));
    assert!(st.resources.spanner_instance_created);
    assert!(st.resources.spanner_database.is_none());
    assert!(!st.resources.spanner_database_created);
    assert!(st.resources.gcs_buckets_created.is_empty());

    let persisted = load_state(tmp.path(), "alice")
        .unwrap()
        .expect("state file must be preserved while created Spanner instance remains");
    assert!(persisted.compute_stopped);
    assert_eq!(
        persisted.resources.spanner_instance.as_deref(),
        Some("jj-cc-dev")
    );
    assert!(persisted.resources.spanner_instance_created);

    // Once the other environment's database is gone, a subsequent `down --purge` deletes the
    // instance and clears `compute_stopped` so `main` removes the state file.
    let fake_gcloud_empty = tmp.path().join("fake-gcloud-empty.sh");
    write_executable(
        &fake_gcloud_empty,
        &format!(
            "#!/bin/sh\necho \"gcloud $*\" >> \"{}\"\nexit 0\n",
            calls_log.display()
        ),
    );
    down_gcp_resources(
        &fake_gcloud_empty,
        || Ok(fake_kubectl.clone()),
        tmp.path(),
        &mut st,
        true,
        Vec::new(),
    )
    .unwrap();

    let calls = fs::read_to_string(&calls_log).unwrap();
    assert!(
        calls.contains("gcloud spanner instances delete jj-cc-dev"),
        "expected follow-up purge to delete now-empty Spanner instance, got:\n{calls}"
    );
    assert!(!st.compute_stopped);
    assert!(st.resources.spanner_instance.is_none());
    assert!(!st.resources.spanner_instance_created);
    assert!(!st.resources.has_created_cloud_storage());
}

#[cfg(unix)]
#[test]
fn down_purge_converges_when_created_spanner_instance_was_deleted_out_of_band() {
    let _proc_guard = crate::lock_test_proc();
    let tmp = tempfile::tempdir().unwrap();
    let calls_log = tmp.path().join("calls.log");
    let fake_gcloud = tmp.path().join("fake-gcloud.sh");
    let fake_kubectl = tmp.path().join("fake-kubectl.sh");
    write_executable(
        &fake_gcloud,
        &format!(
            "#!/bin/sh\necho \"gcloud $*\" >> \"{}\"\ncase \"$*\" in\n  \"spanner databases list \"*)\n    echo \"ERROR: (gcloud.spanner.databases.list) NOT_FOUND: Instance not found: projects/example-proj/instances/jj-cc-dev\" >&2\n    exit 1\n    ;;\n  \"spanner databases delete \"*)\n    echo \"ERROR: (gcloud.spanner.databases.delete) NOT_FOUND: Instance not found\" >&2\n    exit 1\n    ;;\n  *)\n    exit 0\n    ;;\nesac\n",
            calls_log.display()
        ),
    );
    write_executable(&fake_kubectl, "#!/bin/sh\nexit 0\n");

    let mut st = EnvState::new(
        "alice",
        "cloud-run",
        "gcloud",
        "stopped",
        None,
        AuthMode::GcloudIdentityToken,
    );
    st.project_id = Some("example-proj".to_string());
    st.compute_stopped = true;
    st.resources.spanner_instance = Some("jj-cc-dev".to_string());
    st.resources.spanner_instance_created = true;
    st.resources.spanner_database = Some("commit-cloud-alice".to_string());
    st.resources.spanner_database_created = true;
    save_state(tmp.path(), &st).unwrap();

    down_gcp_resources(
        &fake_gcloud,
        || Ok(fake_kubectl.clone()),
        tmp.path(),
        &mut st,
        true,
        Vec::new(),
    )
    .unwrap();

    let calls = fs::read_to_string(&calls_log).unwrap();
    assert!(
        !calls.contains("gcloud spanner instances delete"),
        "should not attempt deleting an instance that `databases list` already reported NOT_FOUND:\n{calls}"
    );
    assert!(!st.compute_stopped);
    assert!(st.resources.spanner_instance.is_none());
    assert!(!st.resources.spanner_instance_created);
    assert!(st.resources.spanner_database.is_none());
    assert!(!st.resources.spanner_database_created);
    assert!(!st.resources.has_created_cloud_storage());

    let persisted = load_state(tmp.path(), "alice").unwrap().unwrap();
    assert!(persisted.resources.spanner_instance.is_none());
    assert!(!persisted.resources.spanner_instance_created);

    // Non-NOT_FOUND errors from `spanner databases list` must still surface as teardown failures.
    let fake_gcloud_perm = tmp.path().join("fake-gcloud-perm.sh");
    write_executable(
        &fake_gcloud_perm,
        "#!/bin/sh\necho \"ERROR: (gcloud.spanner.databases.list) PERMISSION_DENIED: caller lacks permission\" >&2\nexit 1\n",
    );
    let mut perm_st = EnvState::new(
        "bob",
        "cloud-run",
        "gcloud",
        "stopped",
        None,
        AuthMode::GcloudIdentityToken,
    );
    perm_st.project_id = Some("example-proj".to_string());
    perm_st.compute_stopped = true;
    perm_st.resources.spanner_instance = Some("jj-cc-dev".to_string());
    perm_st.resources.spanner_instance_created = true;
    save_state(tmp.path(), &perm_st).unwrap();

    let err = down_gcp_resources(
        &fake_gcloud_perm,
        || Ok(fake_kubectl.clone()),
        tmp.path(),
        &mut perm_st,
        true,
        Vec::new(),
    )
    .unwrap_err();
    assert!(err.to_string().contains("PERMISSION_DENIED"), "err: {err}");
    assert!(perm_st.resources.spanner_instance_created);
}

#[cfg(unix)]
#[test]
fn down_preserves_remaining_state_when_delete_fails() {
    let _proc_guard = crate::lock_test_proc();
    let tmp = tempfile::tempdir().unwrap();
    let fake_gcloud = tmp.path().join("fake-gcloud.sh");
    let fake_kubectl = tmp.path().join("fake-kubectl.sh");
    write_executable(
        &fake_gcloud,
        "#!/bin/sh\ncase \"$*\" in\n  *--region=europe-west1*)\n    echo \"permission denied\" >&2\n    exit 1\n    ;;\n  *)\n    exit 0\n    ;;\nesac\n",
    );
    write_executable(&fake_kubectl, "#!/bin/sh\nexit 0\n");

    let mut st = EnvState::new(
        "alice",
        "cloud-run",
        "gcloud",
        "https://jj-cc.a.run.app",
        None,
        AuthMode::GcloudIdentityToken,
    );
    st.project_id = Some("example-proj".to_string());
    st.resources.cloud_run_services = vec![
        "us-central1/jj-cc-server-alice".to_string(),
        "europe-west1/jj-cc-server-alice".to_string(),
    ];
    save_state(tmp.path(), &st).unwrap();

    let err = down_gcp_resources(
        &fake_gcloud,
        || Ok(fake_kubectl.clone()),
        tmp.path(),
        &mut st,
        false,
        Vec::new(),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("europe-west1/jj-cc-server-alice")
            && err.to_string().contains("`cargo xtask down --env alice`"),
        "unexpected err: {err}"
    );

    let persisted = load_state(tmp.path(), "alice")
        .unwrap()
        .expect("state file must remain");
    assert_eq!(
        persisted.resources.cloud_run_services,
        vec!["europe-west1/jj-cc-server-alice".to_string()]
    );
}

#[test]
fn manifests_and_dockerfile_follow_security_and_storage_invariants() {
    let root = workspace_root();

    let gke = fs::read_to_string(root.join("deploy/k8s/gcp/gke.yaml")).unwrap();
    assert!(gke.contains("type: ClusterIP"));
    assert!(!gke.contains("LoadBalancer"));
    assert!(gke.contains(
        "name: JJ_CC_SERVER_AUTH_TOKEN_FILE\n              value: \"/etc/jj-cc/auth/tokens\""
    ));
    assert!(!gke.contains("JJ_CC_ALLOW_UNAUTHENTICATED"));
    assert!(gke.contains("secretName: jj-cc-server-auth"));
    assert!(gke.contains("defaultMode: 0440"));
    assert!(gke.contains(
        "      securityContext:\n        runAsNonRoot: true\n        runAsUser: 65532\n        runAsGroup: 65532\n        fsGroup: 65532"
    ));
    assert!(gke.contains("optional: false"));
    assert!(gke.contains("readOnly: true"));
    assert!(gke.contains("readOnlyRootFilesystem: true"));
    assert!(gke.contains("name: BLOB_STORE\n              value: \"gcs\""));

    let compose = fs::read_to_string(root.join("compose.yaml")).unwrap();
    let mut in_ports = false;
    let mut checked_ports = 0usize;
    for line in compose.lines() {
        let trimmed = line.trim();
        if trimmed == "ports:" {
            in_ports = true;
            continue;
        }
        if in_ports {
            if let Some(item) = trimmed.strip_prefix("- ") {
                let unquoted = item.trim_matches('"');
                assert!(
                    unquoted.starts_with("127.0.0.1:"),
                    "compose port binding must be loopback-bound: {trimmed}"
                );
                checked_ports += 1;
            } else if !trimmed.is_empty() && !trimmed.starts_with('#') {
                in_ports = false;
            }
        }
    }
    assert!(
        checked_ports >= 7,
        "expected at least 7 port bindings, got {checked_ports}"
    );
    assert!(compose.contains("GCS_PEER_BUCKETS: \"gs://blobs-ew1\""));
    assert!(compose.contains("GCS_PEER_BUCKETS: \"gs://blobs-uc1\""));
    assert!(compose.contains("STORE_TYPE: \"postgres\""));
    assert!(compose.contains("BLOB_STORE: \"s3\""));
    assert!(compose.contains("- SMOKE_PEER_SERVER_URL"));
    assert!(compose.contains("- JJ_CC_AUTH_TOKEN"));

    for rel in [
        "deploy/k8s/postgres.yaml",
        "deploy/k8s/spanner.yaml",
        "deploy/k8s/multiregion.yaml",
    ] {
        let body = fs::read_to_string(root.join(rel)).unwrap();
        assert!(
            body.contains("restartPolicy: Always"),
            "{rel} must set restartPolicy: Always"
        );
    }

    for (rel, expected_tmp_limit, expected_grpc_probe_ports) in [
        ("deploy/k8s/memory.yaml", Some("1Gi"), &[8080u16][..]),
        ("deploy/k8s/sqlite.yaml", Some("1Gi"), &[8080u16][..]),
        ("deploy/k8s/postgres.yaml", Some("1Gi"), &[8080u16][..]),
        ("deploy/k8s/spanner.yaml", Some("1Gi"), &[8080u16][..]),
        (
            "deploy/k8s/multiregion.yaml",
            Some("1Gi"),
            &[8081u16, 8082u16][..],
        ),
    ] {
        let body = fs::read_to_string(root.join(rel)).unwrap();
        assert!(
            body.contains("type: ClusterIP"),
            "{rel} must expose ClusterIP only"
        );
        assert!(
            !body.contains("NodePort") && !body.contains("LoadBalancer"),
            "{rel} must not use NodePort or LoadBalancer"
        );
        let host_ports = body
            .lines()
            .filter(|l| l.trim_start().starts_with("hostPort:"))
            .count();
        let loopback_ips = body
            .lines()
            .filter(|l| l.trim() == "hostIP: 127.0.0.1")
            .count();
        assert_eq!(
            host_ports, loopback_ips,
            "{rel} must set hostIP: 127.0.0.1 on every hostPort"
        );
        let readiness_probes = body
            .lines()
            .filter(|l| l.trim() == "readinessProbe:")
            .count();
        assert_eq!(
            readiness_probes,
            expected_grpc_probe_ports.len(),
            "{rel} must define a gRPC readinessProbe on every jj-cc-server container"
        );
        for port in expected_grpc_probe_ports {
            assert!(
                body.contains(&format!(
                    "readinessProbe:\n        grpc:\n          port: {port}"
                )),
                "{rel} missing gRPC readinessProbe on port {port}"
            );
        }
        assert_readonly_containers_have_writable_tmp_emptydir(rel, &body, expected_tmp_limit);
    }

    assert_readonly_containers_have_writable_tmp_emptydir(
        "deploy/k8s/gcp/gke.yaml",
        &gke,
        Some("2Gi"),
    );

    for rel in [
        "deploy/k8s/gcp/gke-substrate.yaml",
        "deploy/k8s/gcp/gke-substrate-postgres.yaml",
    ] {
        let body = fs::read_to_string(root.join(rel)).unwrap();
        assert!(body.contains("type: ClusterIP"), "{rel} must use ClusterIP");
        assert!(
            !body.contains("LoadBalancer") && !body.contains("NodePort"),
            "{rel} must not expose LoadBalancer or NodePort"
        );
        assert!(
            !body.contains("JJ_CC_ALLOW_UNAUTHENTICATED"),
            "{rel} must not enable unauthenticated binds"
        );
        assert!(body.contains("replicas: 4"), "{rel} must run 4 replicas");
        assert!(body.contains("readOnlyRootFilesystem: true"));
        assert!(body.contains("runAsUser: 65532"));
        assert!(body.contains("fsGroup: 65532"));
        assert!(body.contains("METRICS_LISTEN"));
        assert!(body.contains("0.0.0.0:50052"));
        assert!(body.contains("JJ_CC_AUTH_OIDC_ISSUER"));
        assert!(body.contains("JJ_CC_AUTH_K8S_TOKENREVIEW_URL"));
        assert!(body.contains("port: 50051"));
        assert!(body.contains("port: 50052"));
        assert_readonly_containers_have_writable_tmp_emptydir(rel, &body, Some("2Gi"));
    }
    let gke_sub_pg =
        fs::read_to_string(root.join("deploy/k8s/gcp/gke-substrate-postgres.yaml")).unwrap();
    assert!(gke_sub_pg.contains("secretKeyRef:"));
    assert!(gke_sub_pg.contains("name: jj-cc-postgres-app"));
    assert!(gke_sub_pg.contains("name: jj-cc-s3-credentials"));
    let multiregion = fs::read_to_string(root.join("deploy/k8s/multiregion.yaml")).unwrap();
    assert!(multiregion.contains("value: \"gs://blobs-ew1\""));
    assert!(multiregion.contains("name: GCS_PEER_BUCKETS\n          value: \"gs://blobs-ew1\""));

    let dockerfile = fs::read_to_string(root.join("Dockerfile")).unwrap();
    assert!(!dockerfile.contains(".fingerprint/cc-*"));
    assert!(dockerfile.contains(".fingerprint/cc-common*"));
    assert!(dockerfile.contains(".fingerprint/cc-lib*"));
    assert!(dockerfile.contains("ENV STORE_TYPE=sqlite SQLITE_PATH=/data/commit_cloud.db\n"));
    assert!(!dockerfile.contains("BLOB_PATH=/data/blobs"));
}

fn yaml_indent(line: &str) -> usize {
    line.len() - line.trim_start_matches(' ').len()
}

fn assert_readonly_containers_have_writable_tmp_emptydir(
    rel: &str,
    body: &str,
    expected_size_limit: Option<&str>,
) {
    use std::collections::HashMap;

    let lines: Vec<&str> = body.lines().collect();
    let mut volumes: HashMap<String, (bool, Option<String>)> = HashMap::new();
    let mut i = 0;
    while i < lines.len() {
        let trimmed = lines[i].trim();
        if trimmed == "volumes:" {
            let vol_indent = yaml_indent(lines[i]);
            i += 1;
            let mut cur_name: Option<String> = None;
            let mut cur_empty_dir = false;
            let mut cur_limit: Option<String> = None;
            while i < lines.len() {
                let t = lines[i].trim();
                if t.is_empty() || t.starts_with('#') {
                    i += 1;
                    continue;
                }
                let ind = yaml_indent(lines[i]);
                if ind <= vol_indent || t == "---" {
                    break;
                }
                if let Some(name) = t.strip_prefix("- name:") {
                    if let Some(prev) = cur_name.take() {
                        volumes.insert(prev, (cur_empty_dir, cur_limit.take()));
                    }
                    cur_name = Some(name.trim().trim_matches('"').to_string());
                    cur_empty_dir = false;
                    cur_limit = None;
                } else if t.starts_with("emptyDir:") {
                    cur_empty_dir = true;
                } else if let Some(limit) = t.strip_prefix("sizeLimit:") {
                    cur_limit = Some(limit.trim().trim_matches('"').to_string());
                }
                i += 1;
            }
            if let Some(prev) = cur_name.take() {
                volumes.insert(prev, (cur_empty_dir, cur_limit.take()));
            }
            continue;
        }
        i += 1;
    }

    let mut ro_containers = 0usize;
    i = 0;
    while i < lines.len() {
        if lines[i].trim() == "containers:" {
            let c_indent = yaml_indent(lines[i]);
            let item_indent = c_indent + 2;
            i += 1;
            while i < lines.len() {
                let t = lines[i].trim();
                if t.is_empty() || t.starts_with('#') {
                    i += 1;
                    continue;
                }
                let ind = yaml_indent(lines[i]);
                if ind <= c_indent || t == "---" {
                    break;
                }
                if ind == item_indent && t.starts_with("- name:") {
                    let container_name = t
                        .strip_prefix("- name:")
                        .unwrap()
                        .trim()
                        .trim_matches('"')
                        .to_string();
                    let start = i;
                    i += 1;
                    while i < lines.len() {
                        let ct = lines[i].trim();
                        if ct.is_empty() || ct.starts_with('#') {
                            i += 1;
                            continue;
                        }
                        let cind = yaml_indent(lines[i]);
                        if cind <= c_indent
                            || ct == "---"
                            || (cind == item_indent && ct.starts_with("- "))
                        {
                            break;
                        }
                        i += 1;
                    }
                    let block = &lines[start..i];
                    if block
                        .iter()
                        .any(|l| l.trim() == "readOnlyRootFilesystem: true")
                    {
                        ro_containers += 1;
                        let mut tmp_vol_name: Option<String> = None;
                        let mut j = 0;
                        while j < block.len() {
                            if block[j].trim() == "volumeMounts:" {
                                let vm_indent = yaml_indent(block[j]);
                                j += 1;
                                let mut m_name: Option<String> = None;
                                let mut m_path: Option<String> = None;
                                let mut m_ro = false;
                                let flush =
                                    |m_name: &mut Option<String>,
                                     m_path: &mut Option<String>,
                                     m_ro: &mut bool,
                                     out: &mut Option<String>| {
                                        if m_path.as_deref() == Some("/tmp") {
                                            assert!(
                                                !*m_ro,
                                                "{rel} container '{container_name}' mounts /tmp readOnly: true"
                                            );
                                            *out = m_name.clone();
                                        }
                                        *m_name = None;
                                        *m_path = None;
                                        *m_ro = false;
                                    };
                                while j < block.len() {
                                    let mt = block[j].trim();
                                    if mt.is_empty() || mt.starts_with('#') {
                                        j += 1;
                                        continue;
                                    }
                                    if yaml_indent(block[j]) <= vm_indent {
                                        break;
                                    }
                                    if let Some(name) = mt.strip_prefix("- name:") {
                                        flush(
                                            &mut m_name,
                                            &mut m_path,
                                            &mut m_ro,
                                            &mut tmp_vol_name,
                                        );
                                        m_name = Some(name.trim().trim_matches('"').to_string());
                                    } else if let Some(path) = mt.strip_prefix("mountPath:") {
                                        m_path = Some(path.trim().trim_matches('"').to_string());
                                    } else if mt == "readOnly: true" {
                                        m_ro = true;
                                    }
                                    j += 1;
                                }
                                flush(&mut m_name, &mut m_path, &mut m_ro, &mut tmp_vol_name);
                                continue;
                            }
                            j += 1;
                        }
                        let vol_name = tmp_vol_name.unwrap_or_else(|| {
                            panic!(
                                "{rel} container '{container_name}' sets readOnlyRootFilesystem: true without a writable /tmp volumeMount"
                            )
                        });
                        let (is_empty_dir, size_limit) = volumes.get(&vol_name).unwrap_or_else(|| {
                            panic!(
                                "{rel} container '{container_name}' mounts /tmp from undefined volume '{vol_name}'"
                            )
                        });
                        assert!(
                            *is_empty_dir,
                            "{rel} /tmp volume '{vol_name}' must use emptyDir"
                        );
                        if let Some(expected) = expected_size_limit {
                            assert_eq!(
                                size_limit.as_deref(),
                                Some(expected),
                                "{rel} /tmp emptyDir volume '{vol_name}' must declare sizeLimit: {expected}"
                            );
                        }
                    }
                    continue;
                }
                i += 1;
            }
            continue;
        }
        i += 1;
    }

    let total_ro_flags = body
        .lines()
        .filter(|l| l.trim() == "readOnlyRootFilesystem: true")
        .count();
    assert_eq!(
        ro_containers, total_ro_flags,
        "{rel}: parsed {ro_containers} readOnlyRootFilesystem containers but found {total_ro_flags} flags"
    );
    if expected_size_limit.is_some() {
        assert!(
            ro_containers > 0,
            "{rel}: expected at least one readOnlyRootFilesystem: true container"
        );
    }
}

#[cfg(unix)]
#[test]
fn substrate_actor_init_enforces_injective_repo_id_format() {
    let _proc_guard = crate::lock_test_proc();
    let root = workspace_root();
    let script = root.join("deploy/k8s/gcp/substrate-actor-init.sh");

    let syntax = Command::new("bash")
        .arg("-n")
        .arg(&script)
        .status()
        .unwrap();
    assert!(syntax.success(), "bash -n failed on {}", script.display());

    let tmp = tempfile::tempdir().unwrap();
    let args_log = tmp.path().join("jj-args.log");
    let fake_jj = tmp.path().join("fake-jj.sh");
    write_executable(
        &fake_jj,
        &format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"{}\"\nexit 0\n",
            args_log.display()
        ),
    );
    let ws_dir = tmp.path().join("ws");
    let base_commit = "0123456789abcdef0123456789abcdef01234567";

    // Happy path: injective `<8-hex>_<atespace>_<actorName>` with `--flag=value`.
    let ok = Command::new("bash")
        .arg(&script)
        .env("JJ_BIN", &fake_jj)
        .env("WORKSPACE_DIR", &ws_dir)
        .env("JJ_CC_SERVER", "http://jj-cc-server:50051")
        .env("JJ_CC_PROJECT_ID", "my-project.1")
        .env("ATESPACE", "team-alpha.1")
        .env("ACTOR_NAME", "coder-agent-7")
        .env("BASE_COMMIT", base_commit)
        .output()
        .unwrap();
    assert!(
        ok.status.success(),
        "expected happy path to succeed, stderr:\n{}",
        String::from_utf8_lossy(&ok.stderr)
    );
    let stdout = String::from_utf8_lossy(&ok.stdout);
    let repo_line = stdout
        .lines()
        .find_map(|l| l.strip_prefix("export REPO_ID="))
        .expect("stdout must export REPO_ID");
    let parts: Vec<&str> = repo_line.split('_').collect();
    assert_eq!(parts.len(), 3, "unexpected REPO_ID format: {repo_line}");
    assert_eq!(parts[0].len(), 8);
    assert!(parts[0].chars().all(|c| c.is_ascii_hexdigit()));
    assert_eq!(parts[1], "team-alpha.1");
    assert_eq!(parts[2], "coder-agent-7");
    let recorded = fs::read_to_string(&args_log).unwrap();
    assert!(
        recorded.contains(&format!(
            "cc init --server=http://jj-cc-server:50051 --project-id=my-project.1 --repo-id={repo_line}"
        )),
        "expected `=`-separated flags in jj cc init, got:\n{recorded}"
    );
    assert!(
        recorded.contains(&format!(
            "--repository={} new {base_commit}",
            ws_dir.display()
        )),
        "expected substrate-actor-init.sh to check out BASE_COMMIT via `jj new`, got:\n{recorded}"
    );

    // Helper for expected failures.
    let run_expect_fail = |project: &str, atespace: Option<&str>, actor: &str| {
        let mut cmd = Command::new("bash");
        cmd.arg(&script)
            .env("JJ_BIN", &fake_jj)
            .env("WORKSPACE_DIR", &ws_dir)
            .env("JJ_CC_SERVER", "http://jj-cc-server:50051")
            .env("JJ_CC_PROJECT_ID", project)
            .env("ACTOR_NAME", actor)
            .env("BASE_COMMIT", base_commit);
        match atespace {
            Some(v) => {
                cmd.env("ATESPACE", v);
            }
            None => {
                cmd.env_remove("ATESPACE");
            }
        }
        let out = cmd.output().unwrap();
        assert!(
            !out.status.success(),
            "expected failure for project={project:?} atespace={atespace:?} actor={actor:?}"
        );
        String::from_utf8_lossy(&out.stderr).into_owned()
    };

    // Missing or empty ATESPACE fails closed.
    assert!(run_expect_fail("proj", None, "coder-1").contains("ATESPACE"));
    assert!(run_expect_fail("proj", Some(""), "coder-1").contains("ATESPACE"));

    // Underscores or leading punctuation in ATESPACE / ACTOR_NAME rejected.
    assert!(run_expect_fail("proj", Some("bad_space"), "coder-1").contains("ATESPACE"));
    assert!(run_expect_fail("proj", Some("-leading"), "coder-1").contains("ATESPACE"));
    assert!(run_expect_fail("proj", Some("space"), "bad_actor").contains("ACTOR_NAME"));
    assert!(run_expect_fail("proj", Some("space"), ".dot").contains("ACTOR_NAME"));

    // Invalid JJ_CC_PROJECT_ID rejected ('.', '..', or invalid chars).
    assert!(run_expect_fail(".", Some("space"), "coder-1").contains("JJ_CC_PROJECT_ID"));
    assert!(run_expect_fail("..", Some("space"), "coder-1").contains("JJ_CC_PROJECT_ID"));
    assert!(run_expect_fail("bad/proj", Some("space"), "coder-1").contains("JJ_CC_PROJECT_ID"));

    // Non-ASCII characters rejected even under UTF-8 locales (LC_ALL=C validation).
    assert!(run_expect_fail("proj", Some("space"), "café").contains("ACTOR_NAME"));
    assert!(run_expect_fail("proj", Some("späce"), "coder-1").contains("ATESPACE"));

    // Combined REPO_ID exceeding 128 bytes rejected with explicit limit error.
    let long_space = "a".repeat(60);
    let long_actor = "b".repeat(60); // 8 + 1 + 60 + 1 + 60 = 130 > 128
    let err = run_expect_fail("proj", Some(&long_space), &long_actor);
    assert!(
        err.contains("128-byte"),
        "expected explicit 128-byte limit error, got: {err}"
    );
}

#[cfg(unix)]
#[test]
fn locate_kubectl_validates_explicit_and_candidate_binaries() {
    let _proc_guard = crate::lock_test_proc();
    let tmp = tempfile::tempdir().unwrap();
    let fake_kubectl = tmp.path().join("fake-kubectl.sh");
    write_executable(
        &fake_kubectl,
        "#!/bin/sh\n[ \"$1\" = \"version\" ] && [ \"$2\" = \"--client\" ] && exit 0\nexit 1\n",
    );

    let explicit_str = fake_kubectl.to_string_lossy().into_owned();
    assert_eq!(
        locate_kubectl_with(Some(&explicit_str), &[]).unwrap(),
        fake_kubectl
    );
    let missing = tmp.path().join("no-such-kubectl");
    assert!(locate_kubectl_with(Some(&missing.to_string_lossy()), &[]).is_err());

    // Blank explicit env falls through to candidate probing.
    assert_eq!(
        locate_kubectl_with(Some("   "), &[explicit_str.as_str()]).unwrap(),
        fake_kubectl
    );
    assert!(locate_kubectl_with(None, &["definitely-not-a-kubectl-binary-xyz"]).is_err());
}

#[cfg(unix)]
#[test]
fn ensure_gke_workload_absent_blocks_existing_resources_and_fails_closed() {
    let _proc_guard = crate::lock_test_proc();
    let tmp = tempfile::tempdir().unwrap();
    let ctx = "gke_example-proj_us-central1_jj-cc-cluster";

    // Both deployment and auth Secret absent -> passes and invokes expected kubectl args.
    let calls_log = tmp.path().join("absent-calls.log");
    let kubectl_absent = tmp.path().join("kubectl-absent.sh");
    write_executable(
        &kubectl_absent,
        &format!(
            "#!/bin/sh\necho \"$*\" >> \"{}\"\necho 'Error from server (NotFound): resource not found' >&2\nexit 1\n",
            calls_log.display()
        ),
    );
    ensure_gke_workload_absent(&kubectl_absent, ctx).unwrap();
    let calls = fs::read_to_string(&calls_log).unwrap();
    assert!(
        calls.contains(&format!(
            "--context {ctx} --namespace default get deployment jj-cc-server"
        )),
        "expected deployment check, got:\n{calls}"
    );
    assert!(
        calls.contains(&format!(
            "--context {ctx} --namespace default get secret jj-cc-server-auth"
        )),
        "expected secret check, got:\n{calls}"
    );

    // Existing Deployment blocks deploy with actionable delete command.
    let kubectl_has_deploy = tmp.path().join("kubectl-has-deploy.sh");
    write_executable(
        &kubectl_has_deploy,
        "#!/bin/sh\ncase \"$*\" in\n  *\"get deployment jj-cc-server\"*)\n    echo 'NAME READY'\n    exit 0\n    ;;\n  *)\n    echo 'Error from server (NotFound): secrets \"jj-cc-server-auth\" not found' >&2\n    exit 1\n    ;;\nesac\n",
    );
    let err = ensure_gke_workload_absent(&kubectl_has_deploy, ctx).unwrap_err();
    assert!(
        err.to_string().contains("deployment jj-cc-server")
            && err.to_string().contains(&format!(
                "kubectl --context {ctx} --namespace default delete deployment jj-cc-server"
            )),
        "unexpected deployment-exists error: {err}"
    );

    // Absent Deployment but existing Secret blocks deploy so Secret is never silently rotated.
    let kubectl_has_secret = tmp.path().join("kubectl-has-secret.sh");
    write_executable(
        &kubectl_has_secret,
        "#!/bin/sh\ncase \"$*\" in\n  *\"get secret jj-cc-server-auth\"*)\n    echo 'NAME TYPE'\n    exit 0\n    ;;\n  *)\n    echo 'Error from server (NotFound): deployments.apps \"jj-cc-server\" not found' >&2\n    exit 1\n    ;;\nesac\n",
    );
    let err = ensure_gke_workload_absent(&kubectl_has_secret, ctx).unwrap_err();
    assert!(
        err.to_string().contains("secret jj-cc-server-auth")
            && err.to_string().contains(&format!(
                "kubectl --context {ctx} --namespace default delete secret jj-cc-server-auth"
            )),
        "unexpected secret-exists error: {err}"
    );

    // Non-NotFound failures (RBAC Forbidden, missing context, missing auth plugin) fail closed.
    for (idx, stderr_msg) in [
        "Error from server (Forbidden): deployments.apps \"jj-cc-server\" is forbidden",
        "error: context \"gke_example-proj_us-central1_jj-cc-cluster\" does not exist",
        "Unable to connect to the server: getting credentials: exec: executable gke-gcloud-auth-plugin not found",
    ]
    .iter()
    .enumerate()
    {
        let kubectl_err = tmp.path().join(format!("kubectl-err-{idx}.sh"));
        write_executable(
            &kubectl_err,
            &format!("#!/bin/sh\necho '{}' >&2\nexit 1\n", stderr_msg),
        );
        let err = ensure_gke_workload_absent(&kubectl_err, ctx).unwrap_err();
        assert!(
            err.to_string().contains(stderr_msg),
            "expected fail-closed error containing {stderr_msg:?}, got: {err}"
        );
    }
}

#[cfg(unix)]
#[test]
fn down_gcp_honors_kubectl_bin_and_skips_kubectl_for_non_gke() {
    let _proc_guard = crate::lock_test_proc();
    let tmp = tempfile::tempdir().unwrap();
    let calls_log = tmp.path().join("down-calls.log");
    let fake_gcloud = tmp.path().join("fake-gcloud.sh");
    let fake_kubectl = tmp.path().join("custom-kubectl.sh");
    write_executable(
        &fake_gcloud,
        &format!(
            "#!/bin/sh\necho \"gcloud $*\" >> \"{}\"\nexit 0\n",
            calls_log.display()
        ),
    );
    write_executable(
        &fake_kubectl,
        &format!(
            "#!/bin/sh\necho \"custom-kubectl $*\" >> \"{}\"\nexit 0\n",
            calls_log.display()
        ),
    );
    let gcloud_str = fake_gcloud.to_string_lossy().into_owned();
    let kubectl_str = fake_kubectl.to_string_lossy().into_owned();

    // GKE state resolves KUBECTL_BIN and runs the custom kubectl binary.
    let manifest_path = tmp.path().join("rendered-gke.yaml");
    fs::write(&manifest_path, "kind: Deployment\n").unwrap();
    let mut gke_st = EnvState::new(
        "gke-env",
        "gke",
        "gcloud",
        "http://127.0.0.1:8080",
        None,
        AuthMode::None,
    );
    gke_st.project_id = Some("example-proj".to_string());
    gke_st.resources.gke_cluster = Some("preexisting-cluster".to_string());
    gke_st.resources.gke_region = Some("us-central1".to_string());
    gke_st.resources.kube_context =
        Some("gke_example-proj_us-central1_preexisting-cluster".to_string());
    gke_st.resources.kube_manifest = Some(manifest_path.display().to_string());
    gke_st.resources.gke_auth_secret = Some(GKE_AUTH_SECRET_NAME.to_string());
    save_state(tmp.path(), &gke_st).unwrap();

    down_gcp_with(
        tmp.path(),
        &mut gke_st,
        false,
        || locate_gcloud_with(Some(&gcloud_str), &[]),
        || locate_kubectl_with(Some(&kubectl_str), &[]),
    )
    .unwrap();

    let calls = fs::read_to_string(&calls_log).unwrap();
    assert!(
        calls.contains(
            "custom-kubectl --context gke_example-proj_us-central1_preexisting-cluster --namespace default delete --ignore-not-found -f"
        ),
        "expected custom KUBECTL_BIN to delete manifest, got:\n{calls}"
    );
    assert!(
        calls.contains(
            "custom-kubectl --context gke_example-proj_us-central1_preexisting-cluster --namespace default delete --ignore-not-found secret jj-cc-server-auth"
        ),
        "expected custom KUBECTL_BIN to delete auth secret, got:\n{calls}"
    );
    assert!(gke_st.resources.kube_manifest.is_none());
    assert!(gke_st.resources.gke_auth_secret.is_none());

    // Missing KUBECTL_BIN fails when GKE resources are recorded.
    let mut secret_only = EnvState::new(
        "gke-secret-only",
        "gke",
        "gcloud",
        "http://127.0.0.1:8080",
        None,
        AuthMode::None,
    );
    secret_only.project_id = Some("example-proj".to_string());
    secret_only.resources.gke_auth_secret = Some(GKE_AUTH_SECRET_NAME.to_string());
    let missing_kubectl = tmp.path().join("missing-kubectl");
    let missing_kubectl_str = missing_kubectl.to_string_lossy().into_owned();
    let err = down_gcp_with(
        tmp.path(),
        &mut secret_only,
        false,
        || locate_gcloud_with(Some(&gcloud_str), &[]),
        || locate_kubectl_with(Some(&missing_kubectl_str), &[]),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("KUBECTL_BIN"),
        "expected KUBECTL_BIN error, got: {err}"
    );

    // Cloud Run and GCE states never call locate_kubectl.
    let mut cr_st = EnvState::new(
        "cr-env",
        "cloud-run",
        "gcloud",
        "https://jj-cc.a.run.app",
        None,
        AuthMode::GcloudIdentityToken,
    );
    cr_st.project_id = Some("example-proj".to_string());
    cr_st.resources.cloud_run_services = vec!["us-central1/jj-cc-server-cr-env".to_string()];
    save_state(tmp.path(), &cr_st).unwrap();
    down_gcp_with(
        tmp.path(),
        &mut cr_st,
        false,
        || locate_gcloud_with(Some(&gcloud_str), &[]),
        || Err("locate_kubectl must not be called for Cloud Run".into()),
    )
    .unwrap();

    let mut gce_st = EnvState::new(
        "gce-env",
        "gce",
        "gcloud",
        "http://127.0.0.1:8080",
        None,
        AuthMode::None,
    );
    gce_st.project_id = Some("example-proj".to_string());
    gce_st.resources.gce_instances = vec!["us-central1-a/jj-cc-gce-env-us-central1".to_string()];
    save_state(tmp.path(), &gce_st).unwrap();
    down_gcp_with(
        tmp.path(),
        &mut gce_st,
        false,
        || locate_gcloud_with(Some(&gcloud_str), &[]),
        || Err("locate_kubectl must not be called for GCE".into()),
    )
    .unwrap();
}

#[cfg(unix)]
#[test]
fn down_purge_on_created_gke_cluster_succeeds_without_kubectl() {
    let _proc_guard = crate::lock_test_proc();
    let tmp = tempfile::tempdir().unwrap();
    let calls_log = tmp.path().join("purge-gke-no-kubectl.log");
    let fake_gcloud = tmp.path().join("fake-gcloud.sh");
    write_executable(
        &fake_gcloud,
        &format!(
            "#!/bin/sh\necho \"gcloud $*\" >> \"{}\"\nexit 0\n",
            calls_log.display()
        ),
    );
    let gcloud_str = fake_gcloud.to_string_lossy().into_owned();
    let manifest_path = tmp.path().join("rendered-gke.yaml");
    fs::write(&manifest_path, "kind: Deployment\n").unwrap();

    let mut st = EnvState::new(
        "gke-created",
        "gke",
        "gcloud",
        "http://127.0.0.1:8080",
        None,
        AuthMode::None,
    );
    st.project_id = Some("example-proj".to_string());
    st.resources.gke_cluster = Some("jj-cc-cluster-gke-created".to_string());
    st.resources.gke_region = Some("us-central1".to_string());
    st.resources.gke_cluster_created = true;
    st.resources.kube_context =
        Some("gke_example-proj_us-central1_jj-cc-cluster-gke-created".to_string());
    st.resources.kube_manifest = Some(manifest_path.display().to_string());
    st.resources.gke_auth_secret = Some(GKE_AUTH_SECRET_NAME.to_string());
    st.resources.gcs_buckets = vec!["example-proj-jj-blobs-gke-created-us-central1".to_string()];
    st.resources.gcs_buckets_created =
        vec!["example-proj-jj-blobs-gke-created-us-central1".to_string()];
    st.resources.spanner_instance = Some("jj-cc-dev".to_string());
    st.resources.spanner_instance_created = true;
    st.resources.spanner_database = Some("commit-cloud-gke-created".to_string());
    st.resources.spanner_database_created = true;
    save_state(tmp.path(), &st).unwrap();

    down_gcp_with(
        tmp.path(),
        &mut st,
        true,
        || locate_gcloud_with(Some(&gcloud_str), &[]),
        || Err("kubectl is not installed".into()),
    )
    .unwrap();

    let calls = fs::read_to_string(&calls_log).unwrap();
    assert!(
        calls.contains("gcloud container clusters delete jj-cc-cluster-gke-created"),
        "expected GKE cluster deletion, got:\n{calls}"
    );
    assert!(
        calls.contains(
            "gcloud storage rm --recursive gs://example-proj-jj-blobs-gke-created-us-central1"
        ),
        "expected Cloud Storage bucket deletion, got:\n{calls}"
    );
    assert!(
        calls.contains("gcloud spanner databases delete commit-cloud-gke-created"),
        "expected Spanner database deletion, got:\n{calls}"
    );
    assert!(
        calls.contains("gcloud spanner instances delete jj-cc-dev"),
        "expected Spanner instance deletion, got:\n{calls}"
    );
    assert!(st.resources.kube_manifest.is_none());
    assert!(st.resources.gke_auth_secret.is_none());
    assert!(!st.resources.has_created_cloud_storage());
    assert!(!st.compute_stopped);
}

#[cfg(unix)]
#[test]
fn down_purge_still_deletes_storage_when_kubectl_resolution_fails() {
    let _proc_guard = crate::lock_test_proc();
    let tmp = tempfile::tempdir().unwrap();
    let calls_log = tmp.path().join("purge-storage-without-kubectl.log");
    let fake_gcloud = tmp.path().join("fake-gcloud.sh");
    write_executable(
        &fake_gcloud,
        &format!(
            "#!/bin/sh\necho \"gcloud $*\" >> \"{}\"\nexit 0\n",
            calls_log.display()
        ),
    );
    let gcloud_str = fake_gcloud.to_string_lossy().into_owned();
    let manifest_path = tmp.path().join("rendered-gke.yaml");
    fs::write(&manifest_path, "kind: Deployment\n").unwrap();

    let mut st = EnvState::new(
        "gke-preexisting",
        "gke",
        "gcloud",
        "http://127.0.0.1:8080",
        None,
        AuthMode::None,
    );
    st.project_id = Some("example-proj".to_string());
    st.resources.gke_cluster = Some("preexisting-cluster".to_string());
    st.resources.gke_region = Some("us-central1".to_string());
    st.resources.gke_cluster_created = false;
    st.resources.kube_context =
        Some("gke_example-proj_us-central1_preexisting-cluster".to_string());
    st.resources.kube_manifest = Some(manifest_path.display().to_string());
    st.resources.gke_auth_secret = Some(GKE_AUTH_SECRET_NAME.to_string());
    st.resources.gcs_buckets =
        vec!["example-proj-jj-blobs-gke-preexisting-us-central1".to_string()];
    st.resources.gcs_buckets_created =
        vec!["example-proj-jj-blobs-gke-preexisting-us-central1".to_string()];
    st.resources.spanner_instance = Some("jj-cc-dev".to_string());
    st.resources.spanner_database = Some("commit-cloud-gke-preexisting".to_string());
    st.resources.spanner_database_created = true;
    save_state(tmp.path(), &st).unwrap();

    let err = down_gcp_with(
        tmp.path(),
        &mut st,
        true,
        || locate_gcloud_with(Some(&gcloud_str), &[]),
        || Err("kubectl not found on PATH".into()),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("kubectl not found on PATH")
            && err
                .to_string()
                .contains("`cargo xtask down --env gke-preexisting --purge`"),
        "expected kubectl error and env+purge rerun hint, got: {err}"
    );

    let calls = fs::read_to_string(&calls_log).unwrap();
    assert!(
        calls.contains(
            "gcloud storage rm --recursive gs://example-proj-jj-blobs-gke-preexisting-us-central1"
        ),
        "expected Cloud Storage bucket deletion even when kubectl is missing, got:\n{calls}"
    );
    assert!(
        calls.contains("gcloud spanner databases delete commit-cloud-gke-preexisting"),
        "expected Spanner database deletion even when kubectl is missing, got:\n{calls}"
    );
    assert!(st.resources.gcs_buckets_created.is_empty());
    assert!(!st.resources.spanner_database_created);
    assert!(st.resources.kube_manifest.is_some());
    assert!(st.resources.gke_auth_secret.is_some());
}

#[test]
fn teardown_result_formats_rerun_command_with_env_and_purge_flags() {
    let tmp = tempfile::tempdir().unwrap();
    let default_st = EnvState::new(
        "default",
        "cloud-run",
        "gcloud",
        "https://jj-cc.a.run.app",
        None,
        AuthMode::GcloudIdentityToken,
    );
    let staging_st = EnvState::new(
        "staging",
        "cloud-run",
        "gcloud",
        "https://jj-cc.a.run.app",
        None,
        AuthMode::GcloudIdentityToken,
    );

    assert!(teardown_result(tmp.path(), &default_st, false, Vec::new()).is_ok());

    let default_no_purge =
        teardown_result(tmp.path(), &default_st, false, vec!["boom".to_string()]).unwrap_err();
    assert!(
        default_no_purge.to_string().contains("`cargo xtask down`"),
        "unexpected err: {default_no_purge}"
    );

    let default_purge =
        teardown_result(tmp.path(), &default_st, true, vec!["boom".to_string()]).unwrap_err();
    assert!(
        default_purge
            .to_string()
            .contains("`cargo xtask down --purge`"),
        "unexpected err: {default_purge}"
    );

    let staging_no_purge =
        teardown_result(tmp.path(), &staging_st, false, vec!["boom".to_string()]).unwrap_err();
    assert!(
        staging_no_purge
            .to_string()
            .contains("`cargo xtask down --env staging`"),
        "unexpected err: {staging_no_purge}"
    );

    let staging_purge =
        teardown_result(tmp.path(), &staging_st, true, vec!["boom".to_string()]).unwrap_err();
    assert!(
        staging_purge
            .to_string()
            .contains("`cargo xtask down --env staging --purge`"),
        "unexpected err: {staging_purge}"
    );
}
