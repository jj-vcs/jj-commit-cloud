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

use std::ffi::OsStr;

use super::*;
use crate::state::workspace_root;

#[test]
fn kind_command_sets_podman_experimental_provider_only_for_podman() {
    let podman_cmd = kind_command("podman");
    let podman_env = podman_cmd
        .get_envs()
        .find(|(k, _)| *k == OsStr::new("KIND_EXPERIMENTAL_PROVIDER"))
        .and_then(|(_, v)| v);
    assert_eq!(podman_env, Some(OsStr::new("podman")));

    let docker_cmd = kind_command("docker");
    let docker_env = docker_cmd
        .get_envs()
        .find(|(k, _)| *k == OsStr::new("KIND_EXPERIMENTAL_PROVIDER"));
    assert!(docker_env.is_none());
}

#[test]
fn validate_k8s_options_rejects_invalid_env_names_and_port_combinations() {
    assert!(
        validate_k8s_options(
            K8sProvider::PodmanKube,
            "alice",
            K8sMode::Sqlite,
            8080,
            8082
        )
        .is_err()
    );
    assert_eq!(
        validate_k8s_options(
            K8sProvider::PodmanKube,
            "default",
            K8sMode::Sqlite,
            8080,
            8082
        )
        .unwrap(),
        (8080, None)
    );
    assert_eq!(
        validate_k8s_options(
            K8sProvider::PodmanKube,
            "default",
            K8sMode::Multiregion,
            8080,
            8082
        )
        .unwrap(),
        (8081, Some(8082))
    );
    assert!(
        validate_k8s_options(
            K8sProvider::PodmanKube,
            "default",
            K8sMode::Sqlite,
            9090,
            8082
        )
        .is_err()
    );
    assert_eq!(
        validate_k8s_options(K8sProvider::Kind, "alice", K8sMode::Sqlite, 9090, 8082).unwrap(),
        (9090, None)
    );
    assert!(
        validate_k8s_options(K8sProvider::Kind, "alice", K8sMode::Multiregion, 8080, 8081).is_err()
    );
    assert!(validate_k8s_options(K8sProvider::Kind, "alice", K8sMode::Sqlite, 0, 8082).is_err());
    assert!(
        validate_k8s_options(K8sProvider::Kind, "alice", K8sMode::Multiregion, 0, 8082).is_err()
    );
    assert!(
        validate_k8s_options(K8sProvider::Kind, "alice", K8sMode::Multiregion, 8080, 0).is_err()
    );
    assert!(
        validate_k8s_options(K8sProvider::PodmanKube, "default", K8sMode::Sqlite, 0, 8082).is_err()
    );
    assert!(
        validate_k8s_options(
            K8sProvider::PodmanKube,
            "default",
            K8sMode::Multiregion,
            8080,
            0
        )
        .is_err()
    );

    // `kind` cluster names (`jj-cc-<env>`) must be lowercase RFC-1123 labels <= 63 chars.
    assert!(
        validate_k8s_options(K8sProvider::Kind, "My_Env", K8sMode::Sqlite, 8080, 8082).is_err()
    );
    assert!(
        validate_k8s_options(K8sProvider::Kind, "bad_env", K8sMode::Sqlite, 8080, 8082).is_err()
    );
    assert!(
        validate_k8s_options(K8sProvider::Kind, "trailing-", K8sMode::Sqlite, 8080, 8082).is_err()
    );
    assert!(
        validate_k8s_options(K8sProvider::Kind, "-leading", K8sMode::Sqlite, 8080, 8082).is_err()
    );
    let too_long = "a".repeat(58); // "jj-cc-" (6) + 58 = 64 > 63
    assert!(
        validate_k8s_options(K8sProvider::Kind, &too_long, K8sMode::Sqlite, 8080, 8082).is_err()
    );
    let exact_limit = "a".repeat(57); // "jj-cc-" (6) + 57 = 63
    assert_eq!(
        validate_k8s_options(K8sProvider::Kind, &exact_limit, K8sMode::Sqlite, 8080, 8082).unwrap(),
        (8080, None)
    );
}

#[test]
fn prepare_k8s_up_validates_explicit_provider_clis_and_custom_kubectl() {
    let root = workspace_root();
    let custom_kubectl = PathBuf::from("/opt/custom/kubectl");

    // Explicit `kind` rejects missing `kind`, missing `kubectl`, or missing container runtime.
    let err = prepare_k8s_up_with(
        &root,
        "alice",
        K8sMode::Sqlite,
        K8sProvider::Kind,
        ContainerRuntimeChoice::Auto,
        8080,
        8082,
        || false,
        || Ok(custom_kubectl.clone()),
        || true,
        |_| Ok("docker".to_string()),
    )
    .unwrap_err();
    assert!(err.to_string().contains("requires `kind`"), "err: {err}");

    let err = prepare_k8s_up_with(
        &root,
        "alice",
        K8sMode::Sqlite,
        K8sProvider::Kind,
        ContainerRuntimeChoice::Auto,
        8080,
        8082,
        || true,
        || Err("KUBECTL_BIN missing".into()),
        || true,
        |_| Ok("docker".to_string()),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("KUBECTL_BIN missing"),
        "err: {err}"
    );

    let err = prepare_k8s_up_with(
        &root,
        "alice",
        K8sMode::Sqlite,
        K8sProvider::Kind,
        ContainerRuntimeChoice::Auto,
        8080,
        8082,
        || true,
        || Ok(custom_kubectl.clone()),
        || true,
        |_| Err("no running docker/podman".into()),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("no running docker/podman"),
        "err: {err}"
    );

    // Explicit `podman-kube` checks options before CLIs, and rejects missing `podman`.
    let err = prepare_k8s_up_with(
        &root,
        "alice",
        K8sMode::Sqlite,
        K8sProvider::PodmanKube,
        ContainerRuntimeChoice::Auto,
        8080,
        8082,
        || false,
        || Err("unused".into()),
        || false,
        |_| Err("unused".into()),
    )
    .unwrap_err();
    assert!(err.to_string().contains("--env alice"), "err: {err}");

    let err = prepare_k8s_up_with(
        &root,
        "default",
        K8sMode::Sqlite,
        K8sProvider::PodmanKube,
        ContainerRuntimeChoice::Auto,
        8080,
        8082,
        || false,
        || Err("unused".into()),
        || false,
        |_| Err("unused".into()),
    )
    .unwrap_err();
    assert!(err.to_string().contains("requires `podman`"), "err: {err}");

    // `Auto` selects `kind` with custom `KUBECTL_BIN` when both `kind` and `kubectl` resolve.
    let plan = prepare_k8s_up_with(
        &root,
        "dev-1",
        K8sMode::Sqlite,
        K8sProvider::Auto,
        ContainerRuntimeChoice::Auto,
        9090,
        8082,
        || true,
        || Ok(custom_kubectl.clone()),
        || false,
        |_| Ok("podman".to_string()),
    )
    .unwrap();
    assert_eq!(
        plan,
        K8sUpPlan {
            mode: K8sMode::Sqlite,
            primary_port: 9090,
            peer_port: None,
            target: K8sUpTarget::Kind {
                cluster_name: "jj-cc-dev-1".to_string(),
                container_runtime: "podman".to_string(),
                kubectl_bin: custom_kubectl,
            },
        }
    );
}

#[test]
fn k8s_mode_pod_and_service_names_match_clusterip_manifests() {
    let root = workspace_root();
    for mode in [
        K8sMode::Memory,
        K8sMode::Sqlite,
        K8sMode::Postgres,
        K8sMode::Spanner,
        K8sMode::Multiregion,
    ] {
        let body = fs::read_to_string(root.join(mode.manifest_rel_path())).unwrap();
        assert!(
            body.contains(&format!("name: {}", mode.pod_name())),
            "{} missing pod {}",
            mode.manifest_rel_path(),
            mode.pod_name()
        );
        assert!(
            body.contains(&format!("name: {}", mode.service_name())),
            "{} missing service {}",
            mode.manifest_rel_path(),
            mode.service_name()
        );
        assert!(
            body.contains("type: ClusterIP"),
            "{} must use ClusterIP",
            mode.manifest_rel_path()
        );
        assert!(
            !body.contains("NodePort") && !body.contains("LoadBalancer"),
            "{} must not expose NodePort or LoadBalancer",
            mode.manifest_rel_path()
        );
    }
}

#[cfg(unix)]
fn write_executable(path: &Path, script: &str) {
    use std::os::unix::fs::PermissionsExt;
    fs::write(path, script).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

#[cfg(unix)]
#[test]
fn down_k8s_skips_kubectl_delete_when_kind_cluster_created_and_records_failures_otherwise() {
    let _proc_guard = crate::lock_test_proc();
    let tmp = tempfile::tempdir().unwrap();
    let calls_log = tmp.path().join("calls.log");
    let fake_kubectl_fail = tmp.path().join("kubectl-fail.sh");
    let fake_kind = tmp.path().join("kind-ok.sh");
    write_executable(
        &fake_kubectl_fail,
        &format!(
            "#!/bin/sh\necho \"kubectl $*\" >> \"{}\"\necho \"connection refused\" >&2\nexit 1\n",
            calls_log.display()
        ),
    );
    write_executable(
        &fake_kind,
        &format!(
            "#!/bin/sh\necho \"kind $*\" >> \"{}\"\nexit 0\n",
            calls_log.display()
        ),
    );

    let mut created_state = EnvState::new(
        "dev",
        "k8s-sqlite",
        "kind",
        "http://127.0.0.1:8080",
        None,
        AuthMode::None,
    );
    created_state.resources.container_runtime = Some("docker".to_string());
    created_state.resources.kind_cluster = Some("jj-cc-dev".to_string());
    created_state.resources.kind_cluster_created = true;
    created_state.resources.kube_context = Some("kind-jj-cc-dev".to_string());
    created_state.resources.kube_manifest = Some("deploy/k8s/sqlite.yaml".to_string());

    down_k8s_with(
        tmp.path(),
        &created_state,
        false,
        || Err("kubectl must not be resolved when deleting a created kind cluster".into()),
        &fake_kind,
    )
    .unwrap();
    let calls = fs::read_to_string(&calls_log).unwrap();
    assert!(
        !calls.contains("kubectl "),
        "kubectl delete should be skipped when kind cluster is deleted, got:\n{calls}"
    );
    assert!(
        calls.contains("kind delete cluster --name jj-cc-dev"),
        "expected kind cluster deletion, got:\n{calls}"
    );

    fs::write(&calls_log, "").unwrap();
    let mut preexisting_state = created_state;
    preexisting_state.resources.kind_cluster_created = false;
    let err = down_k8s_with(
        tmp.path(),
        &preexisting_state,
        false,
        || Ok(fake_kubectl_fail.clone()),
        &fake_kind,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("connection refused"),
        "expected kubectl delete failure in error, got: {err}"
    );
    let calls = fs::read_to_string(&calls_log).unwrap();
    assert!(
        calls.contains(
            "kubectl --context kind-jj-cc-dev delete --ignore-not-found -f deploy/k8s/sqlite.yaml"
        ),
        "expected kubectl delete for preexisting cluster, got:\n{calls}"
    );
    assert!(
        !calls.contains("kind delete cluster"),
        "must never delete preexisting kind cluster, got:\n{calls}"
    );

    let err = down_k8s_with(
        tmp.path(),
        &preexisting_state,
        false,
        || Err("KUBECTL_BIN points to missing executable".into()),
        &fake_kind,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("KUBECTL_BIN"),
        "expected KUBECTL_BIN failure in error, got: {err}"
    );
}

#[test]
fn build_kind_port_forward_args_includes_metrics_for_postgres_only_when_requested() {
    let with_metrics =
        build_kind_port_forward_args("kind-jj-cc-dev", "service/jj-cc-server", 8080, None, true);
    assert_eq!(
        with_metrics,
        vec![
            "--context",
            "kind-jj-cc-dev",
            "port-forward",
            "service/jj-cc-server",
            "8080:8080",
            "50052:50052",
            "--address=127.0.0.1",
        ]
    );

    let without_metrics =
        build_kind_port_forward_args("kind-jj-cc-dev", "service/jj-cc-server", 9090, None, false);
    assert_eq!(
        without_metrics,
        vec![
            "--context",
            "kind-jj-cc-dev",
            "port-forward",
            "service/jj-cc-server",
            "9090:8080",
            "--address=127.0.0.1",
        ]
    );

    let multi = build_kind_port_forward_args(
        "kind-jj-cc-dev",
        "service/jj-cc-multiregion",
        8081,
        Some(8082),
        false,
    );
    assert_eq!(
        multi,
        vec![
            "--context",
            "kind-jj-cc-dev",
            "port-forward",
            "service/jj-cc-multiregion",
            "8081:8081",
            "8082:8082",
            "--address=127.0.0.1",
        ]
    );
}

#[cfg(unix)]
#[test]
fn ensure_podman_postgres_pod_ready_waits_and_restarts_exited_server() {
    let _proc_guard = crate::lock_test_proc();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let calls_log = tmp.path().join("podman.log");
    let pg_attempts = tmp.path().join("pg-attempts");
    let minio_attempts = tmp.path().join("minio-attempts");
    let server_state = tmp.path().join("server-running");
    fs::write(&server_state, "false\n").unwrap();

    let fake_podman = tmp.path().join("fake-podman.sh");
    write_executable(
        &fake_podman,
        &format!(
            r#"#!/bin/sh
echo "$*" >> "{calls}"
case "$1" in
  inspect)
target="$4"
case "$target" in
  jj-cc-postgres-postgres|jj-cc-postgres-minio)
    echo "true"
    exit 0
    ;;
  jj-cc-postgres-jj-cc-server)
    cat "{srv}"
    exit 0
    ;;
esac
;;
  exec)
if [ "$3" = "pg_isready" ]; then
  n=0
  [ -f "{pg}" ] && n="$(cat "{pg}")"
  n=$((n + 1))
  echo "$n" > "{pg}"
  [ "$n" -ge 2 ] && exit 0 || exit 1
else
  n=0
  [ -f "{minio}" ] && n="$(cat "{minio}")"
  n=$((n + 1))
  echo "$n" > "{minio}"
  [ "$n" -ge 2 ] && exit 0 || exit 1
fi
;;
  start)
echo "true" > "{srv}"
exit 0
;;
esac
exit 1
"#,
            calls = calls_log.display(),
            srv = server_state.display(),
            pg = pg_attempts.display(),
            minio = minio_attempts.display(),
        ),
    );

    rt.block_on(ensure_podman_postgres_pod_ready_with_bin(
        &fake_podman,
        "jj-cc-postgres",
        Duration::from_secs(5),
        Duration::from_millis(10),
    ))
    .unwrap();

    let calls = fs::read_to_string(&calls_log).unwrap();
    assert!(
        calls.contains("exec jj-cc-postgres-postgres pg_isready"),
        "expected pg_isready probe, got:\n{calls}"
    );
    assert!(
        calls.contains("start jj-cc-postgres-jj-cc-server"),
        "expected exited server container to be restarted, got:\n{calls}"
    );

    // When the server container is already running, `podman start` must not be called.
    fs::write(&calls_log, "").unwrap();
    rt.block_on(ensure_podman_postgres_pod_ready_with_bin(
        &fake_podman,
        "jj-cc-postgres",
        Duration::from_secs(5),
        Duration::from_millis(10),
    ))
    .unwrap();
    let second_calls = fs::read_to_string(&calls_log).unwrap();
    assert!(
        !second_calls.contains("start jj-cc-postgres-jj-cc-server"),
        "running server should not be restarted, got:\n{second_calls}"
    );
}

#[cfg(unix)]
#[test]
fn ensure_podman_postgres_pod_ready_fails_fast_when_dependency_exits() {
    let _proc_guard = crate::lock_test_proc();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let fake_pg_dead = tmp.path().join("podman-pg-dead.sh");
    write_executable(
        &fake_pg_dead,
        "#!/bin/sh\nif [ \"$1\" = \"inspect\" ] && [ \"$4\" = \"jj-cc-postgres-postgres\" ]; then echo \"false\"; exit 0; fi\nexit 1\n",
    );
    let err = rt
        .block_on(ensure_podman_postgres_pod_ready_with_bin(
            &fake_pg_dead,
            "jj-cc-postgres",
            Duration::from_secs(5),
            Duration::from_millis(10),
        ))
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("PostgreSQL container 'jj-cc-postgres-postgres' exited"),
        "unexpected err: {err}"
    );

    let fake_minio_dead = tmp.path().join("podman-minio-dead.sh");
    write_executable(
        &fake_minio_dead,
        r#"#!/bin/sh
case "$1" in
  inspect)
if [ "$4" = "jj-cc-postgres-minio" ]; then echo "false"; else echo "true"; fi
exit 0
;;
  exec)
[ "$3" = "pg_isready" ] && exit 0
exit 1
;;
esac
exit 1
"#,
    );
    let err = rt
        .block_on(ensure_podman_postgres_pod_ready_with_bin(
            &fake_minio_dead,
            "jj-cc-postgres",
            Duration::from_secs(5),
            Duration::from_millis(10),
        ))
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("MinIO container 'jj-cc-postgres-minio' exited"),
        "unexpected err: {err}"
    );
}
