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
//! is acquired. `finish()` always persists local state and releases `working_copy.lock`
//! first, then acquires `cc_sync.lock` off-executor (best-effort) and reloads on-disk
//! checkout state before issuing bounded (`WORKSPACE_SYNC_TIMEOUT`) remote
//! `UpdateWorkspace` and rename `DeleteWorkspace` RPCs. If another command already
//! persisted newer local state, the stale `UpdateWorkspace` is skipped while any
//! superseded rename still deletes its previous name (unless renamed back); lock, reload,
//! or RPC failures log a warning without aborting local persistence.

use async_trait::async_trait;
use std::path::{Path, PathBuf};
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
        self.renamed_from = (new_workspace_name.as_str() != self.initial_workspace_name)
            .then(|| self.initial_workspace_name.clone());
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

        // Persist local `.jj/working_copy/tree_state` + `checkout` and release
        // `working_copy.lock` before waiting on `cc_sync.lock` or any remote RPC.
        let finished_inner = self.locked_inner.finish(new_operation_id.clone()).await?;

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

        let expected_tree_state = (
            tree_id_bytes.clone(),
            tree_ids_bytes.clone(),
            conflict_labels.clone(),
        );
        let sync_plan = acquire_sync_plan(
            state_path.clone(),
            reload_context.clone(),
            new_operation_id,
            ws_name.clone(),
            expected_tree_state,
            sparse_patterns.clone(),
            renamed_from.clone(),
        )
        .await;

        match sync_plan {
            SyncPlan::Full(_sync_lock) => {
                let payload = WorkspaceSyncPayload {
                    config: config.clone(),
                    user: user.clone(),
                    ws_name,
                    commit_id_bytes,
                    op_id_bytes,
                    tree_id_bytes,
                    tree_ids_bytes,
                    conflict_labels,
                    sparse_patterns,
                    renamed_from,
                    sync_timeout,
                };
                if let Err(err) = sync_workspace_state(payload).await {
                    eprintln!(
                        "Warning: failed to sync working copy state to WorkspaceService: {err}"
                    );
                }
                drop(_sync_lock);
            }
            SyncPlan::DeleteOldOnly(_sync_lock, old_name) => {
                if let Err(err) = sync_delete_renamed_workspace(
                    config.clone(),
                    user.clone(),
                    old_name.clone(),
                    sync_timeout,
                )
                .await
                {
                    eprintln!(
                        "Warning: failed to remove renamed workspace '{old_name}' from WorkspaceService: {err}"
                    );
                }
                drop(_sync_lock);
            }
            SyncPlan::Skip => {}
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

struct WorkspaceSyncPayload {
    config: CommitCloudConfig,
    user: String,
    ws_name: String,
    commit_id_bytes: Vec<u8>,
    op_id_bytes: Vec<u8>,
    tree_id_bytes: Vec<u8>,
    tree_ids_bytes: Vec<Vec<u8>>,
    conflict_labels: Vec<String>,
    sparse_patterns: Vec<String>,
    renamed_from: Option<String>,
    sync_timeout: Duration,
}

type TreeStateSnapshot = (Vec<u8>, Vec<Vec<u8>>, Vec<String>);

enum DiskStateCheck {
    Current,
    Superseded { current_workspace_name: String },
}

enum SyncPlan {
    Full(FileLock),
    DeleteOldOnly(FileLock, String),
    Skip,
}

fn inspect_on_disk_state(
    ctx: &LocalReloadContext,
    state_path: &Path,
    expected_op_id: &OperationId,
    expected_ws_name: &str,
    expected_tree_state: &TreeStateSnapshot,
    expected_sparse_patterns: &[String],
) -> Result<DiskStateCheck, WorkingCopyStateError> {
    let reloaded = LocalWorkingCopy::load(
        ctx.store.clone(),
        ctx.working_copy_path.clone(),
        state_path.to_path_buf(),
        &ctx.settings,
    )?;
    let current_workspace_name = reloaded.workspace_name().as_str().to_string();
    if reloaded.operation_id() != expected_op_id
        || current_workspace_name != expected_ws_name
        || &extract_tree_state(&reloaded)? != expected_tree_state
        || extract_sparse_patterns(&reloaded)?.as_slice() != expected_sparse_patterns
    {
        return Ok(DiskStateCheck::Superseded {
            current_workspace_name,
        });
    }
    Ok(DiskStateCheck::Current)
}

fn lock_and_plan_sync(
    state_path: &Path,
    reload_context: Option<&LocalReloadContext>,
    expected_op_id: &OperationId,
    expected_ws_name: &str,
    expected_tree_state: &TreeStateSnapshot,
    expected_sparse_patterns: &[String],
    renamed_from: Option<&str>,
) -> SyncPlan {
    let sync_lock = match FileLock::lock(state_path.join("cc_sync.lock")) {
        Ok(lock) => lock,
        Err(err) => {
            eprintln!(
                "Warning: failed to lock Commit Cloud working copy sync ({err}); skipping remote WorkspaceService sync"
            );
            return SyncPlan::Skip;
        }
    };

    if let Some(ctx) = reload_context {
        match inspect_on_disk_state(
            ctx,
            state_path,
            expected_op_id,
            expected_ws_name,
            expected_tree_state,
            expected_sparse_patterns,
        ) {
            Ok(DiskStateCheck::Current) => {}
            Ok(DiskStateCheck::Superseded {
                current_workspace_name,
            }) => {
                // Another command already persisted newer local checkout state; skip publishing
                // this superseded snapshot so remote WorkspaceService never regresses, while
                // still cleaning up a superseded rename source unless renamed back to it.
                return match renamed_from {
                    Some(old_name)
                        if old_name != expected_ws_name && old_name != current_workspace_name =>
                    {
                        SyncPlan::DeleteOldOnly(sync_lock, old_name.to_string())
                    }
                    _ => SyncPlan::Skip,
                };
            }
            Err(err) => {
                eprintln!(
                    "Warning: failed to reload on-disk working copy state before sync ({err}); skipping remote WorkspaceService sync"
                );
                return SyncPlan::Skip;
            }
        }
    }

    SyncPlan::Full(sync_lock)
}

async fn acquire_sync_plan(
    state_path: PathBuf,
    reload_context: Option<LocalReloadContext>,
    expected_op_id: OperationId,
    expected_ws_name: String,
    expected_tree_state: TreeStateSnapshot,
    expected_sparse_patterns: Vec<String>,
    renamed_from: Option<String>,
) -> SyncPlan {
    if tokio::runtime::Handle::try_current().is_ok() {
        match tokio::task::spawn_blocking(move || {
            lock_and_plan_sync(
                &state_path,
                reload_context.as_ref(),
                &expected_op_id,
                &expected_ws_name,
                &expected_tree_state,
                &expected_sparse_patterns,
                renamed_from.as_deref(),
            )
        })
        .await
        {
            Ok(plan) => plan,
            Err(err) => {
                eprintln!(
                    "Warning: Commit Cloud working copy sync lock task failed ({err}); skipping remote WorkspaceService sync"
                );
                SyncPlan::Skip
            }
        }
    } else {
        lock_and_plan_sync(
            &state_path,
            reload_context.as_ref(),
            &expected_op_id,
            &expected_ws_name,
            &expected_tree_state,
            &expected_sparse_patterns,
            renamed_from.as_deref(),
        )
    }
}

async fn delete_renamed_workspace(
    config: CommitCloudConfig,
    user: String,
    old_name: String,
    sync_timeout: Duration,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let repo_id = config.repo_id.clone();
    tokio::time::timeout(sync_timeout, async move {
        let mut client = connect_workspace_client(&config).await?;
        client
            .delete_workspace(DeleteWorkspaceRequest {
                repo_id,
                user,
                workspace_name: old_name,
            })
            .await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await
    .map_err(|_| {
        Box::<dyn std::error::Error + Send + Sync>::from(format!(
            "WorkspaceService delete timed out after {sync_timeout:?}"
        ))
    })?
}

async fn sync_delete_renamed_workspace(
    config: CommitCloudConfig,
    user: String,
    old_name: String,
    sync_timeout: Duration,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if tokio::runtime::Handle::try_current().is_ok() {
        delete_renamed_workspace(config, user, old_name, sync_timeout).await
    } else {
        run_async(move || delete_renamed_workspace(config, user, old_name, sync_timeout))
    }
}

async fn publish_workspace_state(
    payload: WorkspaceSyncPayload,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let WorkspaceSyncPayload {
        config,
        user,
        ws_name,
        commit_id_bytes,
        op_id_bytes,
        tree_id_bytes,
        tree_ids_bytes,
        conflict_labels,
        sparse_patterns,
        renamed_from,
        sync_timeout,
    } = payload;
    let repo_id = config.repo_id.clone();

    tokio::time::timeout(sync_timeout, async move {
        let mut client = connect_workspace_client(&config).await?;
        #[allow(deprecated)]
        let req = UpdateWorkspaceRequest {
            workspace: Some(WorkspaceState {
                repo_id: repo_id.clone(),
                user: user.clone(),
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
                    user,
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
}

async fn sync_workspace_state(
    payload: WorkspaceSyncPayload,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if tokio::runtime::Handle::try_current().is_ok() {
        publish_workspace_state(payload).await
    } else {
        run_async(move || publish_workspace_state(payload))
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

fn extract_tree_state(wc: &dyn WorkingCopy) -> Result<TreeStateSnapshot, WorkingCopyStateError> {
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

        let payload = WorkspaceSyncPayload {
            config: config.clone(),
            user: user.clone(),
            ws_name,
            commit_id_bytes: Vec::new(),
            op_id_bytes,
            tree_id_bytes,
            tree_ids_bytes,
            conflict_labels,
            sparse_patterns,
            renamed_from: None,
            sync_timeout: WORKSPACE_SYNC_TIMEOUT,
        };
        if let Err(err) = run_async(move || publish_workspace_state(payload)) {
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
        ) -> Result<tonic::Response<cc_common::workspace::GetWorkspaceResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("not used in unit test"))
        }

        async fn update_workspace(
            &self,
            request: tonic::Request<UpdateWorkspaceRequest>,
        ) -> Result<tonic::Response<cc_common::workspace::UpdateWorkspaceResponse>, tonic::Status>
        {
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
        ) -> Result<tonic::Response<cc_common::workspace::ListWorkspacesResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("not used in unit test"))
        }

        async fn check_working_copy_changes(
            &self,
            _request: tonic::Request<cc_common::workspace::CheckWorkingCopyChangesRequest>,
        ) -> Result<
            tonic::Response<cc_common::workspace::CheckWorkingCopyChangesResponse>,
            tonic::Status,
        > {
            Err(tonic::Status::unimplemented("not used in unit test"))
        }

        async fn delete_workspace(
            &self,
            request: tonic::Request<DeleteWorkspaceRequest>,
        ) -> Result<tonic::Response<cc_common::workspace::DeleteWorkspaceResponse>, tonic::Status>
        {
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

            let (server_url, recorded_updates, _recorded_deletes, in_flight_rx, release_tx, server_task) =
                spawn_mock_workspace_server(Some(op1.as_bytes().to_vec())).await;
            let config = CommitCloudConfig {
                server_url,
                project_id: "proj-1".to_string(),
                repo_id: "repo-1".to_string(),
                use_daemon: false,
                daemon_socket: None,
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
                project_id: "proj-1".to_string(),
                repo_id: "repo-1".to_string(),
                use_daemon: false,
                daemon_socket: None,
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
                project_id: "proj-1".to_string(),
                repo_id: "repo-1".to_string(),
                use_daemon: false,
                daemon_socket: None,
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

    #[test]
    fn test_superseded_rename_still_deletes_old_workspace() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let dir = TempDirGuard::new("jj-cc-wc-superseded-rename");
            let initial_op = OperationId::from_bytes(&[0x80; 64]);
            let op1 = OperationId::from_bytes(&[0x81; 64]);
            let op2 = OperationId::from_bytes(&[0x82; 64]);
            let (store, wc_path, state_path, settings, _local_wc) =
                init_local_fixture(&dir.0, initial_op);

            let (
                server_url,
                recorded_updates,
                recorded_deletes,
                _in_flight_rx,
                _release_tx,
                server_task,
            ) = spawn_mock_workspace_server(None).await;
            let config = CommitCloudConfig {
                server_url,
                project_id: "proj-1".to_string(),
                repo_id: "repo-1".to_string(),
                use_daemon: false,
                daemon_socket: None,
            };

            let held_sync_lock = FileLock::lock(state_path.join("cc_sync.lock")).unwrap();

            // Caller 1 renames `default -> renamed-ws` and queues on `cc_sync.lock`.
            let wc1 = load_commit_cloud_wc(
                &store,
                &wc_path,
                &state_path,
                &settings,
                &config,
                WORKSPACE_SYNC_TIMEOUT,
            );
            let mut locked1 = wc1.start_mutation().await.unwrap();
            locked1.rename_workspace(WorkspaceNameBuf::from("renamed-ws"));
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

            // Caller 2 advances `renamed-ws` to `op2` (no rename) and queues on `cc_sync.lock`.
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

            drop(held_sync_lock);
            caller1_task.await.unwrap().unwrap();
            caller2_task.await.unwrap().unwrap();

            let updates: Vec<(String, Vec<u8>)> = recorded_updates
                .lock()
                .unwrap()
                .iter()
                .map(|ws| (ws.workspace_name.clone(), ws.operation_id.clone()))
                .collect();
            assert_eq!(
                updates,
                vec![("renamed-ws".to_string(), op2.as_bytes().to_vec())],
                "stale op1 UpdateWorkspace must be skipped while newer op2 publishes"
            );
            let deletes = recorded_deletes.lock().unwrap().clone();
            assert_eq!(
                deletes,
                vec!["default".to_string()],
                "superseded rename must still delete the orphaned old workspace name"
            );

            server_task.abort();
        });
    }

    #[test]
    fn test_superseded_rename_back_skips_deleting_restored_workspace() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let dir = TempDirGuard::new("jj-cc-wc-superseded-rename-back");
            let initial_op = OperationId::from_bytes(&[0x90; 64]);
            let op1 = OperationId::from_bytes(&[0x91; 64]);
            let op2 = OperationId::from_bytes(&[0x92; 64]);
            let (store, wc_path, state_path, settings, _local_wc) =
                init_local_fixture(&dir.0, initial_op);

            let (
                server_url,
                recorded_updates,
                recorded_deletes,
                _in_flight_rx,
                _release_tx,
                server_task,
            ) = spawn_mock_workspace_server(None).await;
            let config = CommitCloudConfig {
                server_url,
                project_id: "proj-1".to_string(),
                repo_id: "repo-1".to_string(),
                use_daemon: false,
                daemon_socket: None,
            };

            let held_sync_lock = FileLock::lock(state_path.join("cc_sync.lock")).unwrap();

            // Caller 1 renames `default -> temp-ws`.
            let wc1 = load_commit_cloud_wc(
                &store,
                &wc_path,
                &state_path,
                &settings,
                &config,
                WORKSPACE_SYNC_TIMEOUT,
            );
            let mut locked1 = wc1.start_mutation().await.unwrap();
            locked1.rename_workspace(WorkspaceNameBuf::from("temp-ws"));
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

            // Caller 2 renames `temp-ws -> default` before caller 1 acquires `cc_sync.lock`.
            let wc2 = load_commit_cloud_wc(
                &store,
                &wc_path,
                &state_path,
                &settings,
                &config,
                WORKSPACE_SYNC_TIMEOUT,
            );
            let mut locked2 = wc2.start_mutation().await.unwrap();
            locked2.rename_workspace(WorkspaceNameBuf::from("default"));
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

            drop(held_sync_lock);
            caller1_task.await.unwrap().unwrap();
            caller2_task.await.unwrap().unwrap();

            let updates: Vec<(String, Vec<u8>)> = recorded_updates
                .lock()
                .unwrap()
                .iter()
                .map(|ws| (ws.workspace_name.clone(), ws.operation_id.clone()))
                .collect();
            assert_eq!(
                updates,
                vec![("default".to_string(), op2.as_bytes().to_vec())]
            );
            let deletes = recorded_deletes.lock().unwrap().clone();
            assert!(
                !deletes.contains(&"default".to_string()),
                "superseded rename must not delete `default` after workspace was renamed back: {deletes:?}"
            );
            assert_eq!(deletes, vec!["temp-ws".to_string()]);

            server_task.abort();
        });
    }
}
