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

//! `jj_lib::working_copy::WorkingCopy` wrapper synchronizing checkout metadata to
//! `WorkspaceService` while preserving full `Merge<TreeId>` conflict sides, conflict
//! labels, and sparse patterns.
//!
//! # Authority and Crash-Safety Ordering
//! Delegates local disk snapshotting and checkout mutations to `LocalWorkingCopy`, then
//! publishes the resulting `WorkspaceState` to `WorkspaceService` in `finish()`. Tracks
//! `explicit_commit_id` only in memory on `LockedCommitCloudWorkingCopy` (never in a
//! sidecar file on disk) so dropped locks leave zero stale state and same-operation
//! `finish()` calls without checkout/reset/recover derive `@` from `View.wc_commit_ids`.
//! Renaming a workspace upserts the new name before deleting the previous record.
//!
//! # Concurrency and Merge Invariants
//! `start_mutation()` reloads the persisted workspace name only after `working_copy.lock`
//! is acquired. `finish()` acquires `cc_sync.lock` best-effort hand-over-hand while
//! `working_copy.lock` is still held (`working_copy.lock -> cc_sync.lock`), then releases
//! `working_copy.lock` and holds `cc_sync.lock` across bounded (`WORKSPACE_SYNC_TIMEOUT`)
//! remote `UpdateWorkspace` and rename `DeleteWorkspace` RPCs. Lock failures or RPC
//! timeouts log a warning without aborting local working-copy persistence or starving
//! `working_copy.lock`.

use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use cc_common::workspace::{DeleteWorkspaceRequest, UpdateWorkspaceRequest, WorkspaceState};
use jj_lib::commit::Commit;
use jj_lib::local_working_copy::{LocalWorkingCopy, LocalWorkingCopyFactory};
use jj_lib::lock::FileLock;
use jj_lib::merged_tree::MergedTree;
use jj_lib::object_id::ObjectId;
use jj_lib::op_store::OperationId;
use jj_lib::ref_name::{WorkspaceName, WorkspaceNameBuf};
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::settings::UserSettings;
use jj_lib::store::Store;
use jj_lib::working_copy::{
    CheckoutError, CheckoutStats, LockedWorkingCopy, ResetError, SnapshotError, SnapshotOptions,
    SnapshotStats, WorkingCopy, WorkingCopyFactory, WorkingCopyStateError,
};

use crate::client::connect_workspace_client;
use crate::util::{CommitCloudConfig, run_async};

pub const WORKSPACE_SYNC_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct LocalReloadContext {
    store: Arc<Store>,
    working_copy_path: PathBuf,
    settings: UserSettings,
}

pub struct CommitCloudWorkingCopy {
    inner: Box<dyn WorkingCopy>,
    config: CommitCloudConfig,
    state_path: PathBuf,
    user: String,
    reload_context: Option<LocalReloadContext>,
    sync_timeout: Duration,
}

impl CommitCloudWorkingCopy {
    pub fn new(
        inner: Box<dyn WorkingCopy>,
        config: CommitCloudConfig,
        state_path: PathBuf,
        user: String,
    ) -> Self {
        Self {
            inner,
            config,
            state_path,
            user,
            reload_context: None,
            sync_timeout: WORKSPACE_SYNC_TIMEOUT,
        }
    }

    fn with_reload_context(
        inner: Box<dyn WorkingCopy>,
        config: CommitCloudConfig,
        state_path: PathBuf,
        user: String,
        reload_context: Option<LocalReloadContext>,
        sync_timeout: Duration,
    ) -> Self {
        Self {
            inner,
            config,
            state_path,
            user,
            reload_context,
            sync_timeout,
        }
    }

    #[cfg(test)]
    fn with_sync_timeout(mut self, timeout: Duration) -> Self {
        self.sync_timeout = timeout;
        self
    }

    pub fn config(&self) -> &CommitCloudConfig {
        &self.config
    }

    pub fn user(&self) -> &str {
        &self.user
    }
}

#[async_trait(?Send)]
impl WorkingCopy for CommitCloudWorkingCopy {
    fn name(&self) -> &str {
        "commit_cloud"
    }

    fn workspace_name(&self) -> &WorkspaceName {
        self.inner.workspace_name()
    }

    fn operation_id(&self) -> &OperationId {
        self.inner.operation_id()
    }

    fn tree(&self) -> Result<&MergedTree, WorkingCopyStateError> {
        self.inner.tree()
    }

    fn sparse_patterns(&self) -> Result<&[RepoPathBuf], WorkingCopyStateError> {
        self.inner.sparse_patterns()
    }

    async fn start_mutation(&self) -> Result<Box<dyn LockedWorkingCopy>, WorkingCopyStateError> {
        let locked_inner = self.inner.start_mutation().await?;
        let initial_workspace_name = match &self.reload_context {
            Some(ctx) => LocalWorkingCopy::load(
                ctx.store.clone(),
                ctx.working_copy_path.clone(),
                self.state_path.clone(),
                &ctx.settings,
            )?
            .workspace_name()
            .as_str()
            .to_string(),
            None => self.inner.workspace_name().as_str().to_string(),
        };
        Ok(Box::new(LockedCommitCloudWorkingCopy {
            locked_inner,
            config: self.config.clone(),
            state_path: self.state_path.clone(),
            user: self.user.clone(),
            reload_context: self.reload_context.clone(),
            sync_timeout: self.sync_timeout,
            explicit_commit_id: None,
            initial_workspace_name,
            renamed_from: None,
        }))
    }
}

pub struct LockedCommitCloudWorkingCopy {
    locked_inner: Box<dyn LockedWorkingCopy>,
    config: CommitCloudConfig,
    state_path: PathBuf,
    user: String,
    reload_context: Option<LocalReloadContext>,
    sync_timeout: Duration,
    explicit_commit_id: Option<Vec<u8>>,
    initial_workspace_name: String,
    renamed_from: Option<String>,
}

#[async_trait]
impl LockedWorkingCopy for LockedCommitCloudWorkingCopy {
    fn old_operation_id(&self) -> &OperationId {
        self.locked_inner.old_operation_id()
    }

    fn old_tree(&self) -> &MergedTree {
        self.locked_inner.old_tree()
    }

    async fn snapshot(
        &mut self,
        options: &SnapshotOptions,
    ) -> Result<(MergedTree, SnapshotStats), SnapshotError> {
        self.locked_inner.snapshot(options).await
    }

    async fn check_out(&mut self, commit: &Commit) -> Result<CheckoutStats, CheckoutError> {
        let stats = self.locked_inner.check_out(commit).await?;
        self.explicit_commit_id = Some(commit.id().as_bytes().to_vec());
        Ok(stats)
    }

    fn rename_workspace(&mut self, new_workspace_name: WorkspaceNameBuf) {
        if new_workspace_name.as_str() != self.initial_workspace_name {
            self.renamed_from = Some(self.initial_workspace_name.clone());
        }
        self.locked_inner.rename_workspace(new_workspace_name);
    }

    async fn reset(&mut self, commit: &Commit) -> Result<(), ResetError> {
        self.locked_inner.reset(commit).await?;
        self.explicit_commit_id = Some(commit.id().as_bytes().to_vec());
        Ok(())
    }

    async fn recover(&mut self, commit: &Commit) -> Result<(), ResetError> {
        self.locked_inner.recover(commit).await?;
        self.explicit_commit_id = Some(commit.id().as_bytes().to_vec());
        Ok(())
    }

    fn sparse_patterns(&self) -> Result<&[RepoPathBuf], WorkingCopyStateError> {
        self.locked_inner.sparse_patterns()
    }

    async fn set_sparse_patterns(
        &mut self,
        new_sparse_patterns: Vec<RepoPathBuf>,
    ) -> Result<CheckoutStats, CheckoutError> {
        self.locked_inner
            .set_sparse_patterns(new_sparse_patterns)
            .await
    }

    async fn finish(
        self: Box<Self>,
        new_operation_id: OperationId,
    ) -> Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
        let config = self.config.clone();
        let state_path = self.state_path.clone();
        let user = self.user.clone();
        let reload_context = self.reload_context.clone();
        let sync_timeout = self.sync_timeout;
        let old_operation_id = self.locked_inner.old_operation_id().clone();
        let explicit_commit_id = self.explicit_commit_id;
        let renamed_from = self.renamed_from;
        // Acquire `cc_sync.lock` best-effort while `working_copy.lock` is still held so
        // overlapping local commands execute remote updates and rename deletes in local
        // commit order without ever aborting local persistence on sync-lock errors.
        let sync_lock = match FileLock::lock(state_path.join("cc_sync.lock")) {
            Ok(lock) => Some(lock),
            Err(err) => {
                eprintln!(
                    "Warning: failed to lock Commit Cloud working copy sync ({err}); skipping remote WorkspaceService sync"
                );
                None
            }
        };
        let finished_inner = self.locked_inner.finish(new_operation_id.clone()).await?;

        if let Some(_sync_lock) = sync_lock {
            let ws_name = finished_inner.workspace_name().as_str().to_string();
            let op_id_bytes = new_operation_id.as_bytes().to_vec();
            let (tree_id_bytes, tree_ids_bytes, conflict_labels) =
                extract_tree_state(finished_inner.as_ref())?;
            let sparse_patterns = extract_sparse_patterns(finished_inner.as_ref())?;

            // Pass an explicit commit ID only on same-operation checkout/reset/recover mutations;
            // otherwise leave it empty so `UpdateWorkspace` derives `@` from `View.wc_commit_ids`.
            let commit_id_bytes = if new_operation_id == old_operation_id {
                explicit_commit_id.unwrap_or_default()
            } else {
                Vec::new()
            };

            let config_clone = config.clone();
            let repo_id = config.repo_id.clone();
            let user_clone = user.clone();
            if let Err(err) = run_async(move || async move {
                tokio::time::timeout(sync_timeout, async move {
                    let mut client = connect_workspace_client(&config_clone).await?;
                    #[allow(deprecated)]
                    let req = UpdateWorkspaceRequest {
                        workspace: Some(WorkspaceState {
                            repo_id: repo_id.clone(),
                            user: user_clone.clone(),
                            workspace_name: ws_name.clone(),
                            commit_id: commit_id_bytes,
                            operation_id: op_id_bytes,
                            tree_id: tree_id_bytes,
                            sparse_patterns,
                            tree_ids: tree_ids_bytes,
                            conflict_labels,
                            sparse_patterns_set: true,
                        }),
                        expected_operation_id: None,
                    };
                    client.update_workspace(req).await?;
                    if let Some(old_name) = renamed_from
                        && old_name != ws_name
                        && let Err(del_err) = client
                            .delete_workspace(DeleteWorkspaceRequest {
                                repo_id,
                                user: user_clone,
                                workspace_name: old_name.clone(),
                            })
                            .await
                    {
                        eprintln!(
                            "Warning: failed to remove renamed workspace '{old_name}' from WorkspaceService: {del_err}"
                        );
                    }
                    Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
                })
                .await
                .map_err(|_| {
                    Box::<dyn std::error::Error + Send + Sync>::from(format!(
                        "WorkspaceService sync timed out after {sync_timeout:?}"
                    ))
                })?
            }) {
                eprintln!("Warning: failed to sync working copy state to WorkspaceService: {err}");
            }
            drop(_sync_lock);
        }

        Ok(Box::new(CommitCloudWorkingCopy::with_reload_context(
            finished_inner,
            config,
            state_path,
            user,
            reload_context,
            sync_timeout,
        )))
    }
}

pub struct CommitCloudWorkingCopyFactory;

impl Default for CommitCloudWorkingCopyFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl CommitCloudWorkingCopyFactory {
    pub fn new() -> Self {
        Self
    }
}

fn extract_tree_state(
    wc: &dyn WorkingCopy,
) -> Result<(Vec<u8>, Vec<Vec<u8>>, Vec<String>), WorkingCopyStateError> {
    let tree = wc.tree()?;
    let all_ids: Vec<Vec<u8>> = tree
        .tree_ids()
        .iter()
        .map(|id| id.as_bytes().to_vec())
        .collect();
    if all_ids.is_empty() {
        return Err(WorkingCopyStateError {
            message: "Working copy tree contains no tree IDs".to_string(),
            err: "empty Merge<TreeId>".into(),
        });
    }
    let primary = all_ids[0].clone();
    let labels = tree.labels().as_slice().to_vec();
    Ok((primary, all_ids, labels))
}

fn extract_sparse_patterns(wc: &dyn WorkingCopy) -> Result<Vec<String>, WorkingCopyStateError> {
    Ok(wc
        .sparse_patterns()?
        .iter()
        .map(|p| p.as_internal_file_string().to_string())
        .collect())
}

fn resolve_user(settings: &UserSettings) -> Result<String, WorkingCopyStateError> {
    let email = settings.user_email();
    if !email.is_empty() {
        Ok(email.to_string())
    } else {
        Err(WorkingCopyStateError {
            message: "User email (user.email) is not configured in user settings. Run `jj config set --user user.email <email>`".to_string(),
            err: "User email missing".into(),
        })
    }
}

impl WorkingCopyFactory for CommitCloudWorkingCopyFactory {
    fn init_working_copy(
        &self,
        store: Arc<Store>,
        working_copy_path: PathBuf,
        state_path: PathBuf,
        operation_id: OperationId,
        workspace_name: WorkspaceNameBuf,
        settings: &UserSettings,
    ) -> Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
        let user = resolve_user(settings)?;
        let reload_context = Some(LocalReloadContext {
            store: store.clone(),
            working_copy_path: working_copy_path.clone(),
            settings: settings.clone(),
        });
        let local_factory = LocalWorkingCopyFactory {};
        let local_wc = local_factory.init_working_copy(
            store,
            working_copy_path,
            state_path.clone(),
            operation_id.clone(),
            workspace_name.clone(),
            settings,
        )?;

        let config =
            CommitCloudConfig::load_from_store(&state_path).map_err(|e| WorkingCopyStateError {
                message: format!("Failed to load config: {}", e),
                err: e,
            })?;

        let ws_name = workspace_name.as_str().to_string();
        let op_id_bytes = operation_id.as_bytes().to_vec();
        let (tree_id_bytes, tree_ids_bytes, conflict_labels) =
            extract_tree_state(local_wc.as_ref())?;
        let sparse_patterns = extract_sparse_patterns(local_wc.as_ref())?;

        let config_clone = config.clone();
        let repo_id = config.repo_id.clone();
        let user_clone = user.clone();
        if let Err(err) = run_async(move || async move {
            tokio::time::timeout(WORKSPACE_SYNC_TIMEOUT, async move {
                let mut client = connect_workspace_client(&config_clone).await?;
                #[allow(deprecated)]
                let req = UpdateWorkspaceRequest {
                    workspace: Some(WorkspaceState {
                        repo_id,
                        user: user_clone,
                        workspace_name: ws_name,
                        commit_id: Vec::new(),
                        operation_id: op_id_bytes,
                        tree_id: tree_id_bytes,
                        sparse_patterns,
                        tree_ids: tree_ids_bytes,
                        conflict_labels,
                        sparse_patterns_set: true,
                    }),
                    expected_operation_id: None,
                };
                client.update_workspace(req).await?;
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            })
            .await
            .map_err(|_| {
                Box::<dyn std::error::Error + Send + Sync>::from(format!(
                    "WorkspaceService sync timed out after {WORKSPACE_SYNC_TIMEOUT:?}"
                ))
            })?
        }) {
            eprintln!(
                "Warning: failed to sync initial working copy state to WorkspaceService: {err}"
            );
        }

        Ok(Box::new(CommitCloudWorkingCopy::with_reload_context(
            local_wc,
            config,
            state_path,
            user,
            reload_context,
            WORKSPACE_SYNC_TIMEOUT,
        )))
    }

    fn load_working_copy(
        &self,
        store: Arc<Store>,
        working_copy_path: PathBuf,
        state_path: PathBuf,
        settings: &UserSettings,
    ) -> Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
        let user = resolve_user(settings)?;
        let reload_context = Some(LocalReloadContext {
            store: store.clone(),
            working_copy_path: working_copy_path.clone(),
            settings: settings.clone(),
        });
        let local_factory = LocalWorkingCopyFactory {};
        let local_wc = local_factory.load_working_copy(
            store,
            working_copy_path,
            state_path.clone(),
            settings,
        )?;

        let config =
            CommitCloudConfig::load_from_store(&state_path).map_err(|e| WorkingCopyStateError {
                message: format!("Failed to load config: {}", e),
                err: e,
            })?;

        Ok(Box::new(CommitCloudWorkingCopy::with_reload_context(
            local_wc,
            config,
            state_path,
            user,
            reload_context,
            WORKSPACE_SYNC_TIMEOUT,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::{Instant, SystemTime, UNIX_EPOCH};

    use jj_lib::config::StackedConfig;
    use jj_lib::signing::Signer;
    use jj_lib::simple_backend::SimpleBackend;
    use jj_lib::tree_merge::MergeOptions;

    struct TempDirGuard(PathBuf);

    impl TempDirGuard {
        fn new(prefix: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos());
            let path = std::env::temp_dir()
                .join(format!("{prefix}-{}-{nanos}-{seq}", std::process::id(),));
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

        let settings = UserSettings::from_config(StackedConfig::with_defaults()).unwrap();
        let merge_options = MergeOptions::from_settings(&settings).unwrap();
        let signer = Signer::from_settings(&settings).unwrap();
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
                project_id: "proj-1".to_string(),
                repo_id: "repo-1".to_string(),
                use_daemon: false,
                daemon_socket: None,
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
                project_id: "proj-1".to_string(),
                repo_id: "repo-1".to_string(),
                use_daemon: false,
                daemon_socket: None,
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
}
