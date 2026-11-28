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
use std::fs;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use jj_lib::config::StackedConfig;
use jj_lib::signing_factory::signer_from_settings;
use jj_lib::simple_backend::SimpleBackend;

struct TempDirGuard(PathBuf);

impl TempDirGuard {
    fn new(prefix: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let path =
            std::env::temp_dir().join(format!("{prefix}-{}-{nanos}-{seq}", std::process::id(),));
        fs::create_dir_all(&path).expect("create temp test dir");
        Self(path)
    }
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn init_local_fixture(
    root: &std::path::Path,
    initial_op: OperationId,
) -> (
    Arc<Store>,
    PathBuf,
    PathBuf,
    UserSettings,
    Box<dyn WorkingCopy>,
) {
    let store_path = root.join("store");
    let wc_path = root.join("wc");
    let state_path = root.join("state");
    fs::create_dir_all(&store_path).unwrap();
    fs::create_dir_all(&wc_path).unwrap();
    fs::create_dir_all(&state_path).unwrap();

    let settings =
        UserSettings::from_config_and_home_dir(StackedConfig::with_defaults(), None).unwrap();
    let merge_options = settings.merge_options().unwrap();
    let signer = signer_from_settings(&settings).unwrap();
    let backend = SimpleBackend::init(&store_path);
    let store = Store::new(Box::new(backend), signer, merge_options);

    let local_wc = LocalWorkingCopyFactory {}
        .init_working_copy(
            store.clone(),
            wc_path.clone(),
            state_path.clone(),
            initial_op,
            WorkspaceNameBuf::from("default"),
            &settings,
        )
        .unwrap();
    (store, wc_path, state_path, settings, local_wc)
}

#[test]
fn test_resolve_user_requires_non_empty_user_email_in_settings() {
    let default_settings =
        UserSettings::from_config_and_home_dir(StackedConfig::with_defaults(), None).unwrap();
    let err =
        resolve_user(&default_settings).expect_err("empty default user.email must fail closed");
    assert!(
        err.message.contains("user.email"),
        "error should guide user to set user.email: {}",
        err.message
    );

    let mut blank_cfg = StackedConfig::with_defaults();
    blank_cfg.add_layer(
        jj_lib::config::ConfigLayer::parse(
            jj_lib::config::ConfigSource::User,
            "user.email = \"   \"\n",
        )
        .unwrap(),
    );
    let blank_settings = UserSettings::from_config_and_home_dir(blank_cfg, None).unwrap();
    assert!(resolve_user(&blank_settings).is_err());

    let mut valid_cfg = StackedConfig::with_defaults();
    valid_cfg.add_layer(
        jj_lib::config::ConfigLayer::parse(
            jj_lib::config::ConfigSource::User,
            "user.email = \"  alice@example.com  \"\n",
        )
        .unwrap(),
    );
    let valid_settings = UserSettings::from_config_and_home_dir(valid_cfg, None).unwrap();
    assert_eq!(resolve_user(&valid_settings).unwrap(), "alice@example.com");
}

#[test]
fn test_finish_persists_locally_when_sync_lock_cannot_be_acquired() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let dir = TempDirGuard::new("jj-cc-wc-lock-fail");
        let initial_op = OperationId::from_bytes(&[0x11; 64]);
        let next_op = OperationId::from_bytes(&[0x22; 64]);
        let (store, wc_path, state_path, settings, local_wc) =
            init_local_fixture(&dir.0, initial_op);

        // Bind a local listener to verify remote sync is skipped when `cc_sync.lock` fails.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let saw_connection = Arc::new(AtomicBool::new(false));
        let saw_connection_bg = Arc::clone(&saw_connection);
        let accept_task = tokio::spawn(async move {
            if listener.accept().await.is_ok() {
                saw_connection_bg.store(true, Ordering::SeqCst);
            }
        });

        // A directory at `cc_sync.lock` deterministically causes `FileLock::lock` to fail.
        fs::create_dir(state_path.join("cc_sync.lock")).unwrap();

        let config = CommitCloudConfig {
            server_url: format!("http://{addr}"),
            project_id: Some("proj-1".to_string()),
            repo_id: "repo-1".to_string(),
        };
        let wc = CommitCloudWorkingCopy::new(
            local_wc,
            config,
            state_path.clone(),
            "alice@example.com".to_string(),
        );

        let locked = wc.start_mutation().await.unwrap();
        let finished = locked
            .finish(next_op.clone())
            .await
            .expect("local finish must succeed even if cc_sync.lock fails");
        assert_eq!(finished.operation_id(), &next_op);

        // Reload from disk to confirm local working copy persistence succeeded.
        let reloaded = LocalWorkingCopy::load(store, wc_path, state_path, &settings).unwrap();
        assert_eq!(reloaded.operation_id(), &next_op);

        // Confirm remote sync was skipped instead of connecting without the lock.
        tokio::time::sleep(Duration::from_millis(50)).await;
        accept_task.abort();
        assert!(
            !saw_connection.load(Ordering::SeqCst),
            "remote sync should be skipped when cc_sync.lock cannot be acquired"
        );
    });
}

#[test]
fn test_finish_times_out_hung_remote_sync_and_releases_sync_lock() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let dir = TempDirGuard::new("jj-cc-wc-timeout");
        let initial_op = OperationId::from_bytes(&[0x33; 64]);
        let next_op = OperationId::from_bytes(&[0x44; 64]);
        let (store, wc_path, state_path, settings, local_wc) =
            init_local_fixture(&dir.0, initial_op);

        // Accept connections and stall forever without writing HTTP/2 frames.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let stall_task = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                held.push(stream);
                std::future::pending::<()>().await;
            }
        });

        let config = CommitCloudConfig {
            server_url: format!("http://{addr}"),
            project_id: Some("proj-1".to_string()),
            repo_id: "repo-1".to_string(),
        };
        let wc = CommitCloudWorkingCopy::new(
            local_wc,
            config,
            state_path.clone(),
            "alice@example.com".to_string(),
        )
        .with_sync_timeout(Duration::from_millis(150));

        let started = Instant::now();
        let locked = wc.start_mutation().await.unwrap();
        let finished = locked
            .finish(next_op.clone())
            .await
            .expect("local finish must succeed even if remote sync times out");
        let elapsed = started.elapsed();

        stall_task.abort();

        assert!(
            elapsed < Duration::from_secs(5),
            "hung remote sync should time out quickly, took {elapsed:?}"
        );
        assert_eq!(finished.operation_id(), &next_op);

        // Local state must be persisted on disk and `cc_sync.lock` released.
        let reloaded =
            LocalWorkingCopy::load(store, wc_path, state_path.clone(), &settings).unwrap();
        assert_eq!(reloaded.operation_id(), &next_op);
        let relock = FileLock::try_lock(state_path.join("cc_sync.lock"))
            .expect("cc_sync.lock should be lockable after timeout");
        assert!(
            relock.is_some(),
            "cc_sync.lock must be released after remote sync times out"
        );
    });
}

struct MockWorkspaceService {
    stall_on_op: Option<Vec<u8>>,
    in_flight_tx: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    release_rx: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    recorded_updates: Arc<std::sync::Mutex<Vec<WorkspaceState>>>,
    recorded_deletes: Arc<std::sync::Mutex<Vec<String>>>,
}

#[tonic::async_trait]
impl cc_common::workspace::workspace_service_server::WorkspaceService for MockWorkspaceService {
    async fn get_workspace(
        &self,
        _request: tonic::Request<cc_common::workspace::GetWorkspaceRequest>,
    ) -> Result<tonic::Response<cc_common::workspace::GetWorkspaceResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not used in unit test"))
    }

    async fn update_workspace(
        &self,
        request: tonic::Request<UpdateWorkspaceRequest>,
    ) -> Result<tonic::Response<cc_common::workspace::UpdateWorkspaceResponse>, tonic::Status> {
        let ws = request
            .into_inner()
            .workspace
            .ok_or_else(|| tonic::Status::invalid_argument("missing workspace"))?;
        if let Some(stall_op) = &self.stall_on_op
            && ws.operation_id == *stall_op
        {
            if let Some(tx) = self.in_flight_tx.lock().unwrap().take() {
                let _ = tx.send(());
            }
            let rx = self.release_rx.lock().unwrap().take();
            if let Some(rx) = rx {
                let _ = rx.await;
            }
        }
        self.recorded_updates.lock().unwrap().push(ws.clone());
        Ok(tonic::Response::new(
            cc_common::workspace::UpdateWorkspaceResponse {
                success: true,
                workspace: Some(ws),
            },
        ))
    }

    async fn list_workspaces(
        &self,
        _request: tonic::Request<cc_common::workspace::ListWorkspacesRequest>,
    ) -> Result<tonic::Response<cc_common::workspace::ListWorkspacesResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not used in unit test"))
    }

    async fn check_working_copy_changes(
        &self,
        _request: tonic::Request<cc_common::workspace::CheckWorkingCopyChangesRequest>,
    ) -> Result<tonic::Response<cc_common::workspace::CheckWorkingCopyChangesResponse>, tonic::Status>
    {
        Err(tonic::Status::unimplemented("not used in unit test"))
    }

    async fn delete_workspace(
        &self,
        request: tonic::Request<DeleteWorkspaceRequest>,
    ) -> Result<tonic::Response<cc_common::workspace::DeleteWorkspaceResponse>, tonic::Status> {
        self.recorded_deletes
            .lock()
            .unwrap()
            .push(request.into_inner().workspace_name);
        Ok(tonic::Response::new(
            cc_common::workspace::DeleteWorkspaceResponse { existed: true },
        ))
    }
}

type MockServerHandle = (
    String,
    Arc<std::sync::Mutex<Vec<WorkspaceState>>>,
    Arc<std::sync::Mutex<Vec<String>>>,
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
);

async fn spawn_mock_workspace_server(stall_on_op: Option<Vec<u8>>) -> MockServerHandle {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let recorded_updates = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded_deletes = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (in_flight_tx, in_flight_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();

    let service = MockWorkspaceService {
        stall_on_op,
        in_flight_tx: std::sync::Mutex::new(Some(in_flight_tx)),
        release_rx: std::sync::Mutex::new(Some(release_rx)),
        recorded_updates: Arc::clone(&recorded_updates),
        recorded_deletes: Arc::clone(&recorded_deletes),
    };
    let incoming = Box::pin(futures::stream::unfold(listener, |listener| async move {
        let item = listener.accept().await.map(|(stream, _)| stream);
        Some((item, listener))
    }));
    let server_task = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(
                cc_common::workspace::workspace_service_server::WorkspaceServiceServer::new(
                    service,
                ),
            )
            .serve_with_incoming(incoming)
            .await;
    });

    (
        format!("http://{addr}"),
        recorded_updates,
        recorded_deletes,
        in_flight_rx,
        release_tx,
        server_task,
    )
}

fn load_commit_cloud_wc(
    store: &Arc<Store>,
    wc_path: &Path,
    state_path: &Path,
    settings: &UserSettings,
    config: &CommitCloudConfig,
    sync_timeout: Duration,
) -> CommitCloudWorkingCopy {
    let local_wc = LocalWorkingCopy::load(
        store.clone(),
        wc_path.to_path_buf(),
        state_path.to_path_buf(),
        settings,
    )
    .unwrap();
    CommitCloudWorkingCopy::with_reload_context(
        Box::new(local_wc),
        config.clone(),
        state_path.to_path_buf(),
        "alice@example.com".to_string(),
        Some(LocalReloadContext {
            store: store.clone(),
            working_copy_path: wc_path.to_path_buf(),
            settings: settings.clone(),
        }),
        sync_timeout,
    )
}

async fn wait_for_local_state_and_unlocked(
    store: &Arc<Store>,
    wc_path: &Path,
    state_path: &Path,
    settings: &UserSettings,
    expected_op: &OperationId,
    expected_sparse: Option<&[&str]>,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    loop {
        let reloaded = LocalWorkingCopy::load(
            store.clone(),
            wc_path.to_path_buf(),
            state_path.to_path_buf(),
            settings,
        )
        .unwrap();
        let op_matches = reloaded.operation_id() == expected_op;
        let sparse_matches = match expected_sparse {
            Some(expected) => {
                let actual = extract_sparse_patterns(&reloaded).unwrap();
                actual
                    .iter()
                    .map(String::as_str)
                    .eq(expected.iter().copied())
            }
            None => true,
        };
        if op_matches
            && sparse_matches
            && let Ok(Some(lock)) = FileLock::try_lock(state_path.join("working_copy.lock"))
        {
            drop(lock);
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for local state {expected_op:?} and working_copy.lock release"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[test]
fn test_overlap_with_hung_remote_sync_releases_working_copy_lock_immediately() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let dir = TempDirGuard::new("jj-cc-wc-overlap-hung");
        let initial_op = OperationId::from_bytes(&[0x50; 64]);
        let op1 = OperationId::from_bytes(&[0x51; 64]);
        let op2 = OperationId::from_bytes(&[0x52; 64]);
        let (store, wc_path, state_path, settings, _local_wc) =
            init_local_fixture(&dir.0, initial_op);

        let (
            server_url,
            recorded_updates,
            _recorded_deletes,
            in_flight_rx,
            release_tx,
            server_task,
        ) = spawn_mock_workspace_server(Some(op1.as_bytes().to_vec())).await;
        let config = CommitCloudConfig {
            server_url,
            project_id: Some("proj-1".to_string()),
            repo_id: "repo-1".to_string(),
        };

        let wc1 = load_commit_cloud_wc(
            &store,
            &wc_path,
            &state_path,
            &settings,
            &config,
            WORKSPACE_SYNC_TIMEOUT,
        );
        let locked1 = wc1.start_mutation().await.unwrap();
        let op1_clone = op1.clone();
        let caller1_task = tokio::spawn(async move { locked1.finish(op1_clone).await });

        // Wait until caller 1 is actively stalled inside remote `UpdateWorkspace(op1)`
        // while holding `cc_sync.lock`.
        tokio::time::timeout(Duration::from_secs(5), in_flight_rx)
            .await
            .expect("caller 1 should reach remote UpdateWorkspace")
            .unwrap();

        // Caller 2 must persist locally and release `working_copy.lock` immediately
        // (well under `WORKSPACE_SYNC_TIMEOUT`) even while caller 1 remains stalled.
        let started = Instant::now();
        let wc2 = load_commit_cloud_wc(
            &store,
            &wc_path,
            &state_path,
            &settings,
            &config,
            WORKSPACE_SYNC_TIMEOUT,
        );
        let locked2 = wc2.start_mutation().await.unwrap();
        let op2_clone = op2.clone();
        let caller2_task = tokio::spawn(async move { locked2.finish(op2_clone).await });

        wait_for_local_state_and_unlocked(
            &store,
            &wc_path,
            &state_path,
            &settings,
            &op2,
            None,
            Duration::from_secs(2),
        )
        .await;
        let local_persist_elapsed = started.elapsed();
        assert!(
            local_persist_elapsed < Duration::from_secs(2),
            "caller 2 must persist locally and unlock immediately, took {local_persist_elapsed:?}"
        );
        assert!(
            !caller1_task.is_finished(),
            "caller 1 should still be stalled in remote sync"
        );

        // Release caller 1 and verify caller 2 publishes the newer `op2` last.
        release_tx.send(()).unwrap();
        let finished1 = caller1_task.await.unwrap().unwrap();
        let finished2 = caller2_task.await.unwrap().unwrap();
        assert_eq!(finished1.operation_id(), &op1);
        assert_eq!(finished2.operation_id(), &op2);

        let recorded_ops: Vec<Vec<u8>> = recorded_updates
            .lock()
            .unwrap()
            .iter()
            .map(|ws| ws.operation_id.clone())
            .collect();
        assert_eq!(
            recorded_ops,
            vec![op1.as_bytes().to_vec(), op2.as_bytes().to_vec()],
            "newer caller 2 must publish after stalled caller 1 finishes"
        );

        let relock = FileLock::try_lock(state_path.join("cc_sync.lock"))
            .expect("cc_sync.lock should be lockable after overlap completes");
        assert!(relock.is_some());
        server_task.abort();
    });
}

#[test]
fn test_superseded_caller_skips_stale_remote_sync() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let dir = TempDirGuard::new("jj-cc-wc-superseded");
        let initial_op = OperationId::from_bytes(&[0x60; 64]);
        let op0 = OperationId::from_bytes(&[0x61; 64]);
        let op1 = OperationId::from_bytes(&[0x62; 64]);
        let op2 = OperationId::from_bytes(&[0x63; 64]);
        let (store, wc_path, state_path, settings, _local_wc) =
            init_local_fixture(&dir.0, initial_op);

        let (
            server_url,
            recorded_updates,
            _recorded_deletes,
            in_flight_rx,
            release_tx,
            server_task,
        ) = spawn_mock_workspace_server(Some(op0.as_bytes().to_vec())).await;
        let config = CommitCloudConfig {
            server_url,
            project_id: Some("proj-1".to_string()),
            repo_id: "repo-1".to_string(),
        };

        // Stall caller 0 inside remote sync so callers 1 and 2 overlap while `cc_sync.lock` is held.
        let wc0 = load_commit_cloud_wc(
            &store,
            &wc_path,
            &state_path,
            &settings,
            &config,
            WORKSPACE_SYNC_TIMEOUT,
        );
        let locked0 = wc0.start_mutation().await.unwrap();
        let op0_clone = op0.clone();
        let caller0_task = tokio::spawn(async move { locked0.finish(op0_clone).await });
        tokio::time::timeout(Duration::from_secs(5), in_flight_rx)
            .await
            .expect("caller 0 should reach remote UpdateWorkspace")
            .unwrap();

        // Caller 1 persists `op1`, releases `working_copy.lock`, and queues on `cc_sync.lock`.
        let wc1 = load_commit_cloud_wc(
            &store,
            &wc_path,
            &state_path,
            &settings,
            &config,
            WORKSPACE_SYNC_TIMEOUT,
        );
        let locked1 = wc1.start_mutation().await.unwrap();
        let op1_clone = op1.clone();
        let caller1_task = tokio::spawn(async move { locked1.finish(op1_clone).await });
        wait_for_local_state_and_unlocked(
            &store,
            &wc_path,
            &state_path,
            &settings,
            &op1,
            None,
            Duration::from_secs(2),
        )
        .await;

        // Caller 2 supersedes `op1` locally with `op2`, releases `working_copy.lock`, and queues on `cc_sync.lock`.
        let wc2 = load_commit_cloud_wc(
            &store,
            &wc_path,
            &state_path,
            &settings,
            &config,
            WORKSPACE_SYNC_TIMEOUT,
        );
        let locked2 = wc2.start_mutation().await.unwrap();
        let op2_clone = op2.clone();
        let caller2_task = tokio::spawn(async move { locked2.finish(op2_clone).await });
        wait_for_local_state_and_unlocked(
            &store,
            &wc_path,
            &state_path,
            &settings,
            &op2,
            None,
            Duration::from_secs(2),
        )
        .await;

        // Unblock caller 0. Regardless of which queued caller acquires `cc_sync.lock` first,
        // stale `op1` must detect `op2` on disk and skip publishing.
        release_tx.send(()).unwrap();
        caller0_task.await.unwrap().unwrap();
        caller1_task.await.unwrap().unwrap();
        caller2_task.await.unwrap().unwrap();

        let recorded_ops: Vec<Vec<u8>> = recorded_updates
            .lock()
            .unwrap()
            .iter()
            .map(|ws| ws.operation_id.clone())
            .collect();
        assert_eq!(
            recorded_ops,
            vec![op0.as_bytes().to_vec(), op2.as_bytes().to_vec()],
            "superseded op1 must be skipped so it never overwrites newer op2"
        );

        server_task.abort();
    });
}

#[test]
fn test_superseded_same_op_sparse_mutation_skips_stale_remote_sync() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let dir = TempDirGuard::new("jj-cc-wc-superseded-sparse");
        let op = OperationId::from_bytes(&[0x70; 64]);
        let (store, wc_path, state_path, settings, _local_wc) =
            init_local_fixture(&dir.0, op.clone());

        let (
            server_url,
            recorded_updates,
            _recorded_deletes,
            _in_flight_rx,
            _release_tx,
            server_task,
        ) = spawn_mock_workspace_server(None).await;
        let config = CommitCloudConfig {
            server_url,
            project_id: Some("proj-1".to_string()),
            repo_id: "repo-1".to_string(),
        };

        let held_sync_lock = FileLock::lock(state_path.join("cc_sync.lock")).unwrap();

        let wc1 = load_commit_cloud_wc(
            &store,
            &wc_path,
            &state_path,
            &settings,
            &config,
            WORKSPACE_SYNC_TIMEOUT,
        );
        let mut locked1 = wc1.start_mutation().await.unwrap();
        locked1
            .set_sparse_patterns(vec![RepoPathBuf::from_internal_string("a").unwrap()])
            .await
            .unwrap();
        let op_clone = op.clone();
        let caller1_task = tokio::spawn(async move { locked1.finish(op_clone).await });
        wait_for_local_state_and_unlocked(
            &store,
            &wc_path,
            &state_path,
            &settings,
            &op,
            Some(&["a"]),
            Duration::from_secs(2),
        )
        .await;

        let wc2 = load_commit_cloud_wc(
            &store,
            &wc_path,
            &state_path,
            &settings,
            &config,
            WORKSPACE_SYNC_TIMEOUT,
        );
        let mut locked2 = wc2.start_mutation().await.unwrap();
        locked2
            .set_sparse_patterns(vec![RepoPathBuf::from_internal_string("b").unwrap()])
            .await
            .unwrap();
        let op_clone = op.clone();
        let caller2_task = tokio::spawn(async move { locked2.finish(op_clone).await });
        wait_for_local_state_and_unlocked(
            &store,
            &wc_path,
            &state_path,
            &settings,
            &op,
            Some(&["b"]),
            Duration::from_secs(2),
        )
        .await;

        drop(held_sync_lock);
        caller1_task.await.unwrap().unwrap();
        caller2_task.await.unwrap().unwrap();

        let recorded_sparse: Vec<Vec<String>> = recorded_updates
            .lock()
            .unwrap()
            .iter()
            .map(|ws| ws.sparse_patterns.clone())
            .collect();
        assert_eq!(
            recorded_sparse,
            vec![vec!["b".to_string()]],
            "superseded same-op sparse mutation must be skipped"
        );

        server_task.abort();
    });
}

fn spawn_mock_workspace_server_on_shared_runtime() -> (
    String,
    Arc<std::sync::Mutex<Vec<WorkspaceState>>>,
    Arc<std::sync::Mutex<Vec<String>>>,
    tokio::sync::oneshot::Sender<()>,
) {
    let recorded_updates = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded_deletes = Arc::new(std::sync::Mutex::new(Vec::new()));
    let updates_bg = Arc::clone(&recorded_updates);
    let deletes_bg = Arc::clone(&recorded_deletes);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

    let server_url = run_async(move || async move {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let service = MockWorkspaceService {
            stall_on_op: None,
            in_flight_tx: std::sync::Mutex::new(None),
            release_rx: std::sync::Mutex::new(None),
            recorded_updates: updates_bg,
            recorded_deletes: deletes_bg,
        };
        let incoming = Box::pin(futures::stream::unfold(listener, |listener| async move {
            let item = listener.accept().await.map(|(stream, _)| stream);
            Some((item, listener))
        }));
        tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(
                    cc_common::workspace::workspace_service_server::WorkspaceServiceServer::new(
                        service,
                    ),
                )
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });
        Ok(format!("http://{addr}"))
    })
    .unwrap();

    (server_url, recorded_updates, recorded_deletes, shutdown_tx)
}

#[test]
fn test_sync_from_ephemeral_tokio_runtime_does_not_poison_pooled_channel() {
    let dir = TempDirGuard::new("jj-cc-wc-ephemeral-rt");
    let initial_op = OperationId::from_bytes(&[0x80; 64]);
    let op1 = OperationId::from_bytes(&[0x81; 64]);
    let op2 = OperationId::from_bytes(&[0x82; 64]);
    let (store, wc_path, state_path, settings, _local_wc) = init_local_fixture(&dir.0, initial_op);

    let (server_url, recorded_updates, recorded_deletes, shutdown_tx) =
        spawn_mock_workspace_server_on_shared_runtime();
    let config = CommitCloudConfig {
        server_url: server_url.clone(),
        project_id: Some("proj-ephemeral".to_string()),
        repo_id: "repo-ephemeral".to_string(),
    };

    // Sync first mutation from a temporary Tokio runtime, then drop that runtime.
    let ephemeral_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    ephemeral_rt.block_on(async {
        let wc1 = load_commit_cloud_wc(
            &store,
            &wc_path,
            &state_path,
            &settings,
            &config,
            WORKSPACE_SYNC_TIMEOUT,
        );
        let locked1 = wc1.start_mutation().await.unwrap();
        let finished1 = locked1.finish(op1.clone()).await.unwrap();
        assert_eq!(finished1.operation_id(), &op1);
    });
    drop(ephemeral_rt);

    // Subsequent RPCs to the same endpoint (both via `finish` and directly on `SHARED_RUNTIME`)
    // must succeed without hitting a closed channel bound to the dropped runtime.
    let second_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    second_rt.block_on(async {
        let wc2 = load_commit_cloud_wc(
            &store,
            &wc_path,
            &state_path,
            &settings,
            &config,
            WORKSPACE_SYNC_TIMEOUT,
        );
        let locked2 = wc2.start_mutation().await.unwrap();
        let finished2 = locked2.finish(op2.clone()).await.unwrap();
        assert_eq!(finished2.operation_id(), &op2);

        let url_for_probe = server_url.clone();
        run_on_tokio(async move {
            let mut client = connect_workspace_client(url_for_probe).await?;
            client
                .delete_workspace(DeleteWorkspaceRequest {
                    repo_id: "repo-ephemeral".to_string(),
                    user: "alice@example.com".to_string(),
                    workspace_name: "probe-ws".to_string(),
                })
                .await?;
            Ok(())
        })
        .await
        .expect("pooled channel must remain healthy after ephemeral runtime drops");
    });
    drop(second_rt);

    let recorded_ops: Vec<Vec<u8>> = recorded_updates
        .lock()
        .unwrap()
        .iter()
        .map(|ws| ws.operation_id.clone())
        .collect();
    assert_eq!(
        recorded_ops,
        vec![op1.as_bytes().to_vec(), op2.as_bytes().to_vec()]
    );
    assert_eq!(*recorded_deletes.lock().unwrap(), vec!["probe-ws"]);

    crate::util::evict_cached_channel(&server_url);
    let _ = shutdown_tx.send(());
}

#[test]
fn test_superseded_rename_deletes_stale_record_unless_reverted() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let dir = TempDirGuard::new("jj-cc-wc-superseded-rename");
        let initial_op = OperationId::from_bytes(&[0x90; 64]);
        let op1 = OperationId::from_bytes(&[0x91; 64]);
        let op2 = OperationId::from_bytes(&[0x92; 64]);
        let op3 = OperationId::from_bytes(&[0x93; 64]);
        let op4 = OperationId::from_bytes(&[0x94; 64]);
        let (store, wc_path, state_path, settings, _local_wc) =
            init_local_fixture(&dir.0, initial_op);

        let (server_url, recorded_updates, recorded_deletes, shutdown_tx) =
            spawn_mock_workspace_server_on_shared_runtime();
        let config = CommitCloudConfig {
            server_url: server_url.clone(),
            project_id: Some("proj-rename".to_string()),
            repo_id: "repo-rename".to_string(),
        };

        // Caller 1 renames `default` -> `ws-a` and is superseded by caller 2 updating
        // `ws-a`. Caller 1 must skip publishing stale `op1` yet still delete `default`.
        let held_lock = FileLock::lock(state_path.join("cc_sync.lock")).unwrap();
        let wc1 = load_commit_cloud_wc(
            &store,
            &wc_path,
            &state_path,
            &settings,
            &config,
            WORKSPACE_SYNC_TIMEOUT,
        );
        let mut locked1 = wc1.start_mutation().await.unwrap();
        locked1.rename_workspace(WorkspaceNameBuf::from("ws-a"));
        let op1_clone = op1.clone();
        let caller1_task = tokio::spawn(async move { locked1.finish(op1_clone).await });
        wait_for_local_state_and_unlocked(
            &store,
            &wc_path,
            &state_path,
            &settings,
            &op1,
            None,
            Duration::from_secs(2),
        )
        .await;

        let wc2 = load_commit_cloud_wc(
            &store,
            &wc_path,
            &state_path,
            &settings,
            &config,
            WORKSPACE_SYNC_TIMEOUT,
        );
        let locked2 = wc2.start_mutation().await.unwrap();
        let op2_clone = op2.clone();
        let caller2_task = tokio::spawn(async move { locked2.finish(op2_clone).await });
        wait_for_local_state_and_unlocked(
            &store,
            &wc_path,
            &state_path,
            &settings,
            &op2,
            None,
            Duration::from_secs(2),
        )
        .await;

        drop(held_lock);
        caller1_task.await.unwrap().unwrap();
        caller2_task.await.unwrap().unwrap();

        let recorded_ops: Vec<(String, Vec<u8>)> = recorded_updates
            .lock()
            .unwrap()
            .iter()
            .map(|ws| (ws.workspace_name.clone(), ws.operation_id.clone()))
            .collect();
        assert_eq!(
            recorded_ops,
            vec![("ws-a".to_string(), op2.as_bytes().to_vec())],
            "only latest non-superseded state should be published"
        );
        assert_eq!(
            *recorded_deletes.lock().unwrap(),
            vec!["default".to_string()],
            "superseded rename must still delete orphaned old workspace name"
        );

        // Caller 3 renames `ws-a` -> `ws-b`, then caller 4 renames back `ws-b` -> `ws-a`.
        // Because on-disk workspace name is `ws-a` again, caller 3 must NOT delete `ws-a`.
        recorded_updates.lock().unwrap().clear();
        recorded_deletes.lock().unwrap().clear();
        let held_lock = FileLock::lock(state_path.join("cc_sync.lock")).unwrap();

        let wc3 = load_commit_cloud_wc(
            &store,
            &wc_path,
            &state_path,
            &settings,
            &config,
            WORKSPACE_SYNC_TIMEOUT,
        );
        let mut locked3 = wc3.start_mutation().await.unwrap();
        locked3.rename_workspace(WorkspaceNameBuf::from("ws-b"));
        let op3_clone = op3.clone();
        let caller3_task = tokio::spawn(async move { locked3.finish(op3_clone).await });
        wait_for_local_state_and_unlocked(
            &store,
            &wc_path,
            &state_path,
            &settings,
            &op3,
            None,
            Duration::from_secs(2),
        )
        .await;

        let wc4 = load_commit_cloud_wc(
            &store,
            &wc_path,
            &state_path,
            &settings,
            &config,
            WORKSPACE_SYNC_TIMEOUT,
        );
        let mut locked4 = wc4.start_mutation().await.unwrap();
        locked4.rename_workspace(WorkspaceNameBuf::from("ws-a"));
        let op4_clone = op4.clone();
        let caller4_task = tokio::spawn(async move { locked4.finish(op4_clone).await });
        wait_for_local_state_and_unlocked(
            &store,
            &wc_path,
            &state_path,
            &settings,
            &op4,
            None,
            Duration::from_secs(2),
        )
        .await;

        drop(held_lock);
        caller3_task.await.unwrap().unwrap();
        caller4_task.await.unwrap().unwrap();

        assert_eq!(
            *recorded_deletes.lock().unwrap(),
            vec!["ws-b".to_string()],
            "superseded rename must not delete old name when disk reverted to it"
        );

        crate::util::evict_cached_channel(&server_url);
        let _ = shutdown_tx.send(());
    });
}
