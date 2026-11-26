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
use cc_common::EMPTY_TREE_ID_BYTES;
use cc_common::backend::ReadTreeRequest;
use cc_common::backend::backend_service_client::BackendServiceClient;
use tonic::transport::Endpoint;
use tonic::{Code, Request};

#[tokio::test]
async fn run_daemon_rejects_non_loopback_listen_address() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = DaemonConfig {
        state_dir: dir.path().join("state"),
        socket_path: None,
        listen_addr: Some("0.0.0.0:0".parse().unwrap()),
        cache_dir: dir.path().join("cache"),
        default_upstream: None,
        allowed_upstreams: Vec::new(),
        memory_cache_mb: 8,
        idle_timeout_secs: 0,
        write_discovery_files: true,
    };
    let err = run_daemon(cfg, std::future::pending()).await.unwrap_err();
    assert!(
        err.to_string().contains("non-loopback"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn tcp_listener_requires_per_daemon_secret_from_0600_daemon_json() {
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let cache_dir = dir.path().join("cache");
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let cfg = DaemonConfig {
        state_dir: state_dir.clone(),
        socket_path: None,
        listen_addr: Some("127.0.0.1:0".parse().unwrap()),
        cache_dir,
        default_upstream: Some("http://127.0.0.1:1".to_string()),
        allowed_upstreams: Vec::new(),
        memory_cache_mb: 8,
        idle_timeout_secs: 0,
        write_discovery_files: true,
    };
    let handle = tokio::spawn(async move {
        run_daemon(cfg, async move {
            let _ = stop_rx.await;
        })
        .await
    });

    let info_path = state_dir.join("daemon.json");
    let info: DaemonRuntimeInfo = loop {
        if let Ok(raw) = fs::read(&info_path)
            && let Ok(parsed) = serde_json::from_slice(&raw)
        {
            break parsed;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(&info_path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "daemon.json must be mode 0600");
    }

    let tcp_url = info.tcp_addr.expect("tcp_addr recorded");
    let secret = info.secret.expect("secret recorded for TCP listener");
    let channel = Endpoint::from_shared(tcp_url)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = BackendServiceClient::new(channel);

    let empty_tree_req = || ReadTreeRequest {
        project_id: "repo".to_string(),
        path: String::new(),
        tree_id: EMPTY_TREE_ID_BYTES.to_vec(),
        page_size: 0,
        page_token: String::new(),
    };

    let missing_err = client
        .read_tree(Request::new(empty_tree_req()))
        .await
        .unwrap_err();
    assert_eq!(missing_err.code(), Code::Unauthenticated);

    let mut wrong_req = Request::new(empty_tree_req());
    wrong_req
        .metadata_mut()
        .insert(DAEMON_SECRET_HEADER, "wrong-secret".parse().unwrap());
    let wrong_err = client.read_tree(wrong_req).await.unwrap_err();
    assert_eq!(wrong_err.code(), Code::Unauthenticated);

    let mut valid_req = Request::new(empty_tree_req());
    valid_req
        .metadata_mut()
        .insert(DAEMON_SECRET_HEADER, secret.parse().unwrap());
    let resp = client.read_tree(valid_req).await.unwrap().into_inner();
    assert_eq!(resp.tree_id, EMPTY_TREE_ID_BYTES);

    let _ = stop_tx.send(());
    handle.await.unwrap().unwrap();
    assert!(!info_path.exists());
}

#[cfg(unix)]
#[tokio::test]
async fn state_dir_rejects_open_permissions_and_socket_parent_mode_is_preserved() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let open_state_dir = dir.path().join("open_state");
    fs::create_dir_all(&open_state_dir).unwrap();
    fs::set_permissions(&open_state_dir, fs::Permissions::from_mode(0o755)).unwrap();

    let bad_cfg = DaemonConfig {
        state_dir: open_state_dir,
        socket_path: None,
        listen_addr: Some("127.0.0.1:0".parse().unwrap()),
        cache_dir: dir.path().join("cache"),
        default_upstream: None,
        allowed_upstreams: Vec::new(),
        memory_cache_mb: 8,
        idle_timeout_secs: 0,
        write_discovery_files: true,
    };
    let err = run_daemon(bad_cfg, std::future::pending())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("0700"), "unexpected error: {err}");

    let sock_parent = dir.path().join("shared_parent");
    fs::create_dir_all(&sock_parent).unwrap();
    fs::set_permissions(&sock_parent, fs::Permissions::from_mode(0o755)).unwrap();
    let sock_path = sock_parent.join("custom.sock");

    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let ok_cfg = DaemonConfig {
        state_dir: dir.path().join("private_state"),
        socket_path: Some(sock_path.clone()),
        listen_addr: None,
        cache_dir: dir.path().join("cache"),
        default_upstream: None,
        allowed_upstreams: Vec::new(),
        memory_cache_mb: 8,
        idle_timeout_secs: 0,
        write_discovery_files: true,
    };
    let handle = tokio::spawn(async move {
        run_daemon(ok_cfg, async move {
            let _ = stop_rx.await;
        })
        .await
    });
    while !sock_path.exists() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let parent_mode = fs::metadata(&sock_parent).unwrap().permissions().mode() & 0o777;
    assert_eq!(
        parent_mode, 0o755,
        "existing --socket parent mode must not be changed"
    );

    let _ = stop_tx.send(());
    handle.await.unwrap().unwrap();
}

#[test]
fn shutdown_cleanup_preserves_successor_daemon_files() {
    let dir = tempfile::tempdir().unwrap();
    let info_path = dir.path().join("daemon.json");
    let sock_path = dir.path().join("daemon.sock");
    fs::write(&sock_path, b"").unwrap();
    let pid = std::process::id();
    write_runtime_info(
        &info_path,
        &DaemonRuntimeInfo {
            pid: pid.wrapping_add(1),
            socket_path: Some(sock_path.to_string_lossy().into_owned()),
            tcp_addr: None,
            cache_dir: dir.path().to_string_lossy().into_owned(),
            stats_path: dir
                .path()
                .join("daemon.stats.json")
                .to_string_lossy()
                .into_owned(),
            default_upstream: None,
            started_unix_secs: 1,
            secret: None,
        },
    )
    .unwrap();

    remove_runtime_files_if_owned(&info_path, pid, 1, Some(&sock_path), None);
    assert!(info_path.exists(), "must not delete successor daemon.json");
    assert!(sock_path.exists(), "must not delete successor socket");

    // Same PID with a newer `started_unix_secs` must also be treated as a successor.
    write_runtime_info(
        &info_path,
        &DaemonRuntimeInfo {
            pid,
            socket_path: Some(sock_path.to_string_lossy().into_owned()),
            tcp_addr: None,
            cache_dir: dir.path().to_string_lossy().into_owned(),
            stats_path: dir
                .path()
                .join("daemon.stats.json")
                .to_string_lossy()
                .into_owned(),
            default_upstream: None,
            started_unix_secs: 2,
            secret: None,
        },
    )
    .unwrap();
    remove_runtime_files_if_owned(&info_path, pid, 1, Some(&sock_path), None);
    assert!(
        info_path.exists(),
        "must not delete daemon.json with newer started_unix_secs"
    );
    assert!(
        sock_path.exists(),
        "must not delete socket when started_unix_secs differs"
    );

    // Matching PID and timestamp removes `daemon.json`, but preserves `sock_path` if its
    // inode was replaced by another daemon.
    #[cfg(unix)]
    {
        let old_identity = socket_identity(&sock_path);
        let replacement = dir.path().join("replacement.sock");
        fs::write(&replacement, b"new").unwrap();
        fs::rename(&replacement, &sock_path).unwrap();
        remove_runtime_files_if_owned(&info_path, pid, 2, Some(&sock_path), old_identity);
        assert!(!info_path.exists(), "owned daemon.json should be removed");
        assert!(
            sock_path.exists(),
            "must not delete socket replaced with a different inode"
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn cache_dir_rejects_open_permissions_and_symlink() {
    use std::os::unix::fs::{PermissionsExt, symlink};

    let dir = tempfile::tempdir().unwrap();

    let open_cache = dir.path().join("open_cache");
    fs::create_dir_all(&open_cache).unwrap();
    fs::set_permissions(&open_cache, fs::Permissions::from_mode(0o755)).unwrap();
    let open_cfg = DaemonConfig {
        state_dir: dir.path().join("state_open"),
        socket_path: None,
        listen_addr: Some("127.0.0.1:0".parse().unwrap()),
        cache_dir: open_cache,
        default_upstream: None,
        allowed_upstreams: Vec::new(),
        memory_cache_mb: 8,
        idle_timeout_secs: 0,
        write_discovery_files: true,
    };
    let err = run_daemon(open_cfg, std::future::pending())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("0700"), "unexpected error: {err}");

    let target_cache = dir.path().join("real_cache");
    fs::create_dir_all(&target_cache).unwrap();
    fs::set_permissions(&target_cache, fs::Permissions::from_mode(0o700)).unwrap();
    let symlink_cache = dir.path().join("symlink_cache");
    symlink(&target_cache, &symlink_cache).unwrap();
    let sym_cfg = DaemonConfig {
        state_dir: dir.path().join("state_sym"),
        socket_path: None,
        listen_addr: Some("127.0.0.1:0".parse().unwrap()),
        cache_dir: symlink_cache,
        default_upstream: None,
        allowed_upstreams: Vec::new(),
        memory_cache_mb: 8,
        idle_timeout_secs: 0,
        write_discovery_files: true,
    };
    let err = run_daemon(sym_cfg, std::future::pending())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("0700") || err.to_string().contains("directory"),
        "unexpected error for symlinked cache_dir: {err}"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn duplicate_startup_preserves_live_stats_and_dangling_symlink_is_cleaned() {
    use std::os::unix::fs::{PermissionsExt, symlink};

    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let cache_dir = dir.path().join("cache");
    let sock_path = dir.path().join("daemon.sock");

    // Plant a dangling symlink at `sock_path` and verify startup replaces it.
    symlink(dir.path().join("nonexistent-target"), &sock_path).unwrap();
    assert!(fs::symlink_metadata(&sock_path).is_ok());

    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let cfg1 = DaemonConfig {
        state_dir: state_dir.clone(),
        socket_path: Some(sock_path.clone()),
        listen_addr: Some("127.0.0.1:0".parse().unwrap()),
        cache_dir: cache_dir.clone(),
        default_upstream: Some("http://127.0.0.1:1".to_string()),
        allowed_upstreams: Vec::new(),
        memory_cache_mb: 8,
        idle_timeout_secs: 0,
        write_discovery_files: true,
    };
    let handle = tokio::spawn(async move {
        run_daemon(cfg1, async move {
            let _ = stop_rx.await;
        })
        .await
    });

    let info_path = state_dir.join("daemon.json");
    let stats_path = state_dir.join("daemon.stats.json");
    let info: DaemonRuntimeInfo = loop {
        if let Ok(raw) = fs::read(&info_path)
            && let Ok(parsed) = serde_json::from_slice(&raw)
            && stats_path.exists()
        {
            break parsed;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };

    let stats_mode = fs::metadata(&stats_path).unwrap().permissions().mode() & 0o777;
    assert_eq!(stats_mode, 0o600, "daemon.stats.json must be mode 0600");

    let tcp_url = info.tcp_addr.expect("tcp_addr recorded");
    let secret = info.secret.expect("secret recorded");
    let channel = Endpoint::from_shared(tcp_url)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = BackendServiceClient::new(channel);
    let mut req = Request::new(ReadTreeRequest {
        project_id: "repo".to_string(),
        path: String::new(),
        tree_id: EMPTY_TREE_ID_BYTES.to_vec(),
        page_size: 0,
        page_token: String::new(),
    });
    req.metadata_mut()
        .insert(DAEMON_SECRET_HEADER, secret.parse().unwrap());
    client.read_tree(req).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if let Ok(raw) = fs::read(&stats_path)
            && let Ok(snap) = serde_json::from_slice::<crate::cache::CacheStatsSnapshot>(&raw)
            && snap.l1_hits >= 1
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for daemon.stats.json flush"
        );
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    // Racing duplicate daemon must fail without zeroing live `daemon.stats.json`.
    let dup_cfg = DaemonConfig {
        state_dir: state_dir.clone(),
        socket_path: Some(sock_path),
        listen_addr: None,
        cache_dir,
        default_upstream: Some("http://127.0.0.1:1".to_string()),
        allowed_upstreams: Vec::new(),
        memory_cache_mb: 8,
        idle_timeout_secs: 0,
        write_discovery_files: true,
    };
    let err = run_daemon(dup_cfg, std::future::pending())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("already listening"),
        "unexpected duplicate error: {err}"
    );

    let after_dup: crate::cache::CacheStatsSnapshot =
        serde_json::from_slice(&fs::read(&stats_path).unwrap()).unwrap();
    assert!(
        after_dup.l1_hits >= 1,
        "duplicate startup clobbered live daemon.stats.json: {after_dup:?}"
    );

    let _ = stop_tx.send(());
    handle.await.unwrap().unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn startup_failure_cleans_up_newly_bound_socket() {
    let dir = tempfile::tempdir().unwrap();
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let occupied_addr = occupied.local_addr().unwrap();
    let sock_path = dir.path().join("failed.sock");

    let cfg = DaemonConfig {
        state_dir: dir.path().join("state"),
        socket_path: Some(sock_path.clone()),
        listen_addr: Some(occupied_addr),
        cache_dir: dir.path().join("cache"),
        default_upstream: None,
        allowed_upstreams: Vec::new(),
        memory_cache_mb: 8,
        idle_timeout_secs: 0,
        write_discovery_files: true,
    };
    assert!(run_daemon(cfg, std::future::pending()).await.is_err());
    assert!(
        fs::symlink_metadata(&sock_path).is_err(),
        "RAII guard must remove newly bound socket when startup fails"
    );
    drop(occupied);
}

#[tokio::test]
async fn idle_timeout_waits_for_active_rpc_and_then_shuts_down() {
    use cc_common::backend::backend_service_server::BackendService;
    use cc_common::backend::*;
    use tokio::sync::{Mutex, Notify};

    struct BlockingCommitUpstream {
        entered: Arc<Notify>,
        release: Arc<Mutex<Option<tokio::sync::oneshot::Receiver<()>>>>,
    }

    fn unimpl<T>() -> Result<tonic::Response<T>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }

    #[tonic::async_trait]
    impl BackendService for BlockingCommitUpstream {
        async fn register_project(
            &self,
            _: Request<RegisterProjectRequest>,
        ) -> Result<tonic::Response<RegisterProjectResponse>, tonic::Status> {
            unimpl()
        }
        async fn register_repository(
            &self,
            _: Request<RegisterRepositoryRequest>,
        ) -> Result<tonic::Response<RegisterRepositoryResponse>, tonic::Status> {
            unimpl()
        }
        async fn read_commit(
            &self,
            request: Request<ReadCommitRequest>,
        ) -> Result<tonic::Response<ReadCommitResponse>, tonic::Status> {
            self.entered.notify_one();
            let rx = self.release.lock().await.take();
            if let Some(rx) = rx {
                let _ = rx.await;
            }
            Ok(tonic::Response::new(ReadCommitResponse {
                commit: Some(Commit {
                    commit_id: request.into_inner().commit_id,
                    change_id: vec![0x11; 16],
                    parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                    root_tree_id: vec![EMPTY_TREE_ID_BYTES.to_vec()],
                    description: "slow commit".to_string(),
                    ..Default::default()
                }),
            }))
        }
        async fn write_commit(
            &self,
            _: Request<WriteCommitRequest>,
        ) -> Result<tonic::Response<WriteCommitResponse>, tonic::Status> {
            unimpl()
        }
        async fn list_commits(
            &self,
            _: Request<ListCommitsRequest>,
        ) -> Result<tonic::Response<ListCommitsResponse>, tonic::Status> {
            unimpl()
        }
        async fn resolve_commit_prefix(
            &self,
            _: Request<ResolveCommitPrefixRequest>,
        ) -> Result<tonic::Response<ResolveCommitPrefixResponse>, tonic::Status> {
            unimpl()
        }
        async fn resolve_change_prefix(
            &self,
            _: Request<ResolveChangePrefixRequest>,
        ) -> Result<tonic::Response<ResolveChangePrefixResponse>, tonic::Status> {
            unimpl()
        }
        async fn read_tree(
            &self,
            _: Request<ReadTreeRequest>,
        ) -> Result<tonic::Response<ReadTreeResponse>, tonic::Status> {
            unimpl()
        }
        async fn write_tree(
            &self,
            _: Request<WriteTreeRequest>,
        ) -> Result<tonic::Response<WriteTreeResponse>, tonic::Status> {
            unimpl()
        }
        type ReadFileStream = Pin<
            Box<
                dyn futures::Stream<Item = Result<ReadFileResponse, tonic::Status>>
                    + Send
                    + 'static,
            >,
        >;
        async fn read_file(
            &self,
            _: Request<ReadFileRequest>,
        ) -> Result<tonic::Response<Self::ReadFileStream>, tonic::Status> {
            unimpl()
        }
        async fn write_file(
            &self,
            _: Request<WriteFileRequest>,
        ) -> Result<tonic::Response<WriteFileResponse>, tonic::Status> {
            unimpl()
        }
        async fn write_file_stream(
            &self,
            _: Request<tonic::Streaming<WriteFileChunk>>,
        ) -> Result<tonic::Response<WriteFileResponse>, tonic::Status> {
            unimpl()
        }
        async fn read_symlink(
            &self,
            _: Request<ReadSymlinkRequest>,
        ) -> Result<tonic::Response<ReadSymlinkResponse>, tonic::Status> {
            unimpl()
        }
        async fn write_symlink(
            &self,
            _: Request<WriteSymlinkRequest>,
        ) -> Result<tonic::Response<WriteSymlinkResponse>, tonic::Status> {
            unimpl()
        }
    }

    let entered = Arc::new(Notify::new());
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_url = format!("http://{}", listener.local_addr().unwrap());
    let incoming =
        tonic::transport::server::TcpIncoming::from_listener(listener, true, None).unwrap();
    tokio::spawn(
        Server::builder()
            .add_service(BackendServiceServer::new(BlockingCommitUpstream {
                entered: Arc::clone(&entered),
                release: Arc::new(Mutex::new(Some(release_rx))),
            }))
            .serve_with_incoming(incoming),
    );

    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let cfg = DaemonConfig {
        state_dir: state_dir.clone(),
        socket_path: None,
        listen_addr: Some("127.0.0.1:0".parse().unwrap()),
        cache_dir: dir.path().join("cache"),
        default_upstream: Some(upstream_url),
        allowed_upstreams: Vec::new(),
        memory_cache_mb: 8,
        idle_timeout_secs: 1,
        write_discovery_files: true,
    };
    let daemon_handle = tokio::spawn(run_daemon(cfg, std::future::pending()));

    let info_path = state_dir.join("daemon.json");
    let info: DaemonRuntimeInfo = loop {
        if let Ok(raw) = fs::read(&info_path)
            && let Ok(parsed) = serde_json::from_slice(&raw)
        {
            break parsed;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };

    let channel = Endpoint::from_shared(info.tcp_addr.unwrap())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = BackendServiceClient::new(channel);
    let secret = info.secret.unwrap();
    let rpc_task = tokio::spawn(async move {
        let mut req = Request::new(ReadCommitRequest {
            project_id: "proj".to_string(),
            commit_id: vec![0x55; 20],
        });
        req.metadata_mut()
            .insert(DAEMON_SECRET_HEADER, secret.parse().unwrap());
        client.read_commit(req).await
    });

    entered.notified().await;
    // Wait past the 1s idle timeout while the RPC is still active.
    tokio::time::sleep(Duration::from_millis(1400)).await;
    assert!(
        !daemon_handle.is_finished(),
        "daemon must not shut down while an RPC is active"
    );

    let _ = release_tx.send(());
    let resp = rpc_task.await.unwrap().unwrap().into_inner();
    assert_eq!(resp.commit.unwrap().description, "slow commit");

    // Once idle after the RPC finishes, the daemon should exit automatically.
    tokio::time::timeout(Duration::from_secs(4), daemon_handle)
        .await
        .expect("daemon should shut down after idle timeout once RPC completes")
        .unwrap()
        .unwrap();
    assert!(!info_path.exists());
}

#[tokio::test]
async fn tcp_only_duplicate_daemon_fails_without_clobbering_live_discovery_or_stats() {
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let cache_dir = dir.path().join("cache");
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let cfg1 = DaemonConfig {
        state_dir: state_dir.clone(),
        socket_path: None,
        listen_addr: Some("127.0.0.1:0".parse().unwrap()),
        cache_dir: cache_dir.clone(),
        default_upstream: Some("http://127.0.0.1:1".to_string()),
        allowed_upstreams: Vec::new(),
        memory_cache_mb: 8,
        idle_timeout_secs: 0,
        write_discovery_files: true,
    };
    let handle = tokio::spawn(async move {
        run_daemon(cfg1, async move {
            let _ = stop_rx.await;
        })
        .await
    });

    let info_path = state_dir.join("daemon.json");
    let stats_path = state_dir.join("daemon.stats.json");
    let lock_path = state_dir.join("daemon.instance.lock");
    let info: DaemonRuntimeInfo = loop {
        if let Ok(raw) = fs::read(&info_path)
            && let Ok(parsed) = serde_json::from_slice(&raw)
            && stats_path.exists()
        {
            break parsed;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let lock_mode = fs::metadata(&lock_path).unwrap().permissions().mode() & 0o777;
        assert_eq!(lock_mode, 0o600, "daemon.instance.lock must be mode 0600");
    }

    let tcp_url = info.tcp_addr.clone().expect("tcp_addr recorded");
    let secret = info.secret.clone().expect("secret recorded");
    let channel = Endpoint::from_shared(tcp_url)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = BackendServiceClient::new(channel);
    let mut req = Request::new(ReadTreeRequest {
        project_id: "repo".to_string(),
        path: String::new(),
        tree_id: EMPTY_TREE_ID_BYTES.to_vec(),
        page_size: 0,
        page_token: String::new(),
    });
    req.metadata_mut()
        .insert(DAEMON_SECRET_HEADER, secret.parse().unwrap());
    client.read_tree(req).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if let Ok(raw) = fs::read(&stats_path)
            && let Ok(snap) = serde_json::from_slice::<crate::cache::CacheStatsSnapshot>(&raw)
            && snap.l1_hits >= 1
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for daemon.stats.json flush"
        );
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    let info_before = fs::read(&info_path).unwrap();

    // A second TCP-only daemon targeting the same `state_dir` must fail immediately on the
    // exclusive `daemon.instance.lock` without clobbering `daemon.json` or zeroing stats.
    let dup_cfg = DaemonConfig {
        state_dir: state_dir.clone(),
        socket_path: None,
        listen_addr: Some("127.0.0.1:0".parse().unwrap()),
        cache_dir: cache_dir.clone(),
        default_upstream: Some("http://127.0.0.1:2".to_string()),
        allowed_upstreams: Vec::new(),
        memory_cache_mb: 8,
        idle_timeout_secs: 0,
        write_discovery_files: true,
    };
    let err = run_daemon(dup_cfg, std::future::pending())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("already listening"),
        "unexpected TCP-only duplicate error: {err}"
    );

    assert_eq!(
        fs::read(&info_path).unwrap(),
        info_before,
        "duplicate TCP-only daemon must not clobber live daemon.json"
    );
    let after_dup: crate::cache::CacheStatsSnapshot =
        serde_json::from_slice(&fs::read(&stats_path).unwrap()).unwrap();
    assert!(
        after_dup.l1_hits >= 1,
        "duplicate TCP-only daemon must not zero live daemon.stats.json: {after_dup:?}"
    );

    let _ = stop_tx.send(());
    handle.await.unwrap().unwrap();
    assert!(!info_path.exists());

    // After the first daemon shuts down, a fresh TCP-only daemon can acquire the lock again.
    let (stop2_tx, stop2_rx) = tokio::sync::oneshot::channel::<()>();
    let cfg2 = DaemonConfig {
        state_dir: state_dir.clone(),
        socket_path: None,
        listen_addr: Some("127.0.0.1:0".parse().unwrap()),
        cache_dir,
        default_upstream: Some("http://127.0.0.1:1".to_string()),
        allowed_upstreams: Vec::new(),
        memory_cache_mb: 8,
        idle_timeout_secs: 0,
        write_discovery_files: true,
    };
    let handle2 = tokio::spawn(async move {
        run_daemon(cfg2, async move {
            let _ = stop2_rx.await;
        })
        .await
    });
    while !info_path.exists() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let _ = stop2_tx.send(());
    handle2.await.unwrap().unwrap();
    assert!(!info_path.exists());
}

#[cfg(unix)]
#[tokio::test]
async fn explicit_socket_without_discovery_files_leaves_state_dir_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let untouched_state = dir.path().join("untouched_state");
    let sock_path = dir.path().join("adhoc.sock");
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();

    let cfg = DaemonConfig {
        state_dir: untouched_state.clone(),
        socket_path: Some(sock_path.clone()),
        listen_addr: None,
        cache_dir: dir.path().join("cache"),
        default_upstream: Some("http://127.0.0.1:1".to_string()),
        allowed_upstreams: Vec::new(),
        memory_cache_mb: 8,
        idle_timeout_secs: 0,
        write_discovery_files: false,
    };
    let handle = tokio::spawn(async move {
        run_daemon(cfg, async move {
            let _ = stop_rx.await;
        })
        .await
    });

    while !sock_path.exists() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        !untouched_state.exists(),
        "legacy --socket mode without discovery files must not create or touch state_dir"
    );

    let _ = stop_tx.send(());
    handle.await.unwrap().unwrap();
    assert!(
        !sock_path.exists(),
        "owned ad-hoc socket should be cleaned up on graceful shutdown"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn socket_startup_refuses_to_unlink_regular_file_or_directory_but_cleans_stale_socket() {
    let dir = tempfile::tempdir().unwrap();
    let cache_dir = dir.path().join("cache");

    // Regular file at `--socket` must fail and remain untouched.
    let regular_file = dir.path().join("important.txt");
    fs::write(&regular_file, b"keep-me").unwrap();
    let file_cfg = DaemonConfig {
        state_dir: PathBuf::new(),
        socket_path: Some(regular_file.clone()),
        listen_addr: None,
        cache_dir: cache_dir.clone(),
        default_upstream: Some("http://127.0.0.1:1".to_string()),
        allowed_upstreams: Vec::new(),
        memory_cache_mb: 8,
        idle_timeout_secs: 0,
        write_discovery_files: false,
    };
    let err = run_daemon(file_cfg, std::future::pending())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("non-socket"),
        "unexpected regular-file error: {err}"
    );
    assert_eq!(fs::read(&regular_file).unwrap(), b"keep-me");

    // Directory at `--socket` must fail and remain untouched.
    let dir_path = dir.path().join("existing_dir");
    fs::create_dir(&dir_path).unwrap();
    let dir_cfg = DaemonConfig {
        state_dir: PathBuf::new(),
        socket_path: Some(dir_path.clone()),
        listen_addr: None,
        cache_dir: cache_dir.clone(),
        default_upstream: Some("http://127.0.0.1:1".to_string()),
        allowed_upstreams: Vec::new(),
        memory_cache_mb: 8,
        idle_timeout_secs: 0,
        write_discovery_files: false,
    };
    let err = run_daemon(dir_cfg, std::future::pending())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("non-socket"),
        "unexpected directory error: {err}"
    );
    assert!(dir_path.is_dir());

    // Stale Unix socket at `--socket` is still cleaned up and replaced.
    let stale_sock = dir.path().join("stale.sock");
    drop(std::os::unix::net::UnixListener::bind(&stale_sock).unwrap());
    assert!(fs::symlink_metadata(&stale_sock).is_ok());
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let stale_cfg = DaemonConfig {
        state_dir: PathBuf::new(),
        socket_path: Some(stale_sock.clone()),
        listen_addr: None,
        cache_dir,
        default_upstream: Some("http://127.0.0.1:1".to_string()),
        allowed_upstreams: Vec::new(),
        memory_cache_mb: 8,
        idle_timeout_secs: 0,
        write_discovery_files: false,
    };
    let handle = tokio::spawn(async move {
        run_daemon(stale_cfg, async move {
            let _ = stop_rx.await;
        })
        .await
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::net::UnixStream::connect(&stale_sock).await.is_err() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for daemon to bind stale socket"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let _ = stop_tx.send(());
    handle.await.unwrap().unwrap();
    assert!(!stale_sock.exists());
}

#[test]
fn daemon_runtime_info_debug_redacts_secret() {
    let secret = "super-secret-tcp-token-0123456789abcdef";
    let with_secret = DaemonRuntimeInfo {
        pid: 42,
        socket_path: Some("/tmp/daemon.sock".to_string()),
        tcp_addr: Some("http://127.0.0.1:50055".to_string()),
        cache_dir: "/tmp/cache".to_string(),
        stats_path: "/tmp/state/daemon.stats.json".to_string(),
        default_upstream: Some("http://127.0.0.1:8080".to_string()),
        started_unix_secs: 1_700_000_000,
        secret: Some(secret.to_string()),
    };
    let compact = format!("{with_secret:?}");
    let pretty = format!("{with_secret:#?}");
    assert!(
        !compact.contains(secret) && !pretty.contains(secret),
        "Debug formatting must never expose plaintext TCP secret: {compact}"
    );
    assert!(
        compact.contains("<redacted>"),
        "Debug formatting should indicate secret is redacted: {compact}"
    );

    let without_secret = DaemonRuntimeInfo {
        secret: None,
        ..with_secret
    };
    assert!(format!("{without_secret:?}").contains("secret: None"));
}

#[cfg(unix)]
#[test]
fn create_private_dir_all_and_ensure_private_dir_do_not_chmod_symlink_target() {
    use std::os::unix::fs::{PermissionsExt, symlink};

    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("target_dir");
    fs::create_dir(&target).unwrap();
    fs::set_permissions(&target, fs::Permissions::from_mode(0o755)).unwrap();

    let link = dir.path().join("link_dir");
    symlink(&target, &link).unwrap();

    // `create_private_dir_all` leaves existing symlinked parents (such as `/tmp` on macOS)
    // untouched, while `ensure_private_dir` rejects them without changing the target's mode
    // even when a trailing slash is present.
    let link_slash = dir.path().join("link_dir/");
    create_private_dir_all(&link).unwrap();
    create_private_dir_all(&link_slash).unwrap();
    assert_eq!(
        fs::metadata(&target).unwrap().permissions().mode() & 0o777,
        0o755
    );
    for path in [&link, &link_slash] {
        let err = ensure_private_dir(path).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
        assert_eq!(
            fs::metadata(&target).unwrap().permissions().mode() & 0o777,
            0o755,
            "ensure_private_dir must never chmod a symlink target"
        );
    }

    // Even when a symlink points to an owned `0700` directory, a trailing slash must not let
    // `verify_private_dir` / `ensure_private_dir` resolve through the symlink.
    let private_target = dir.path().join("private_target_dir");
    fs::create_dir(&private_target).unwrap();
    fs::set_permissions(&private_target, fs::Permissions::from_mode(0o700)).unwrap();
    let private_link = dir.path().join("private_link_dir");
    symlink(&private_target, &private_link).unwrap();
    let private_link_slash = dir.path().join("private_link_dir/");
    for path in [&private_link, &private_link_slash] {
        let err = ensure_private_dir(path).unwrap_err();
        assert_eq!(
            err.kind(),
            std::io::ErrorKind::PermissionDenied,
            "ensure_private_dir must reject symlink even with trailing slash: {}",
            path.display()
        );
    }

    // Trailing slash on a newly created private dir still creates and secures it.
    let new_dir = dir.path().join("fresh_private/");
    ensure_private_dir(&new_dir).unwrap();
    assert_eq!(
        fs::symlink_metadata(dir.path().join("fresh_private"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
}
