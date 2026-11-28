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

//! `jj_lib::working_copy::WorkingCopy` wrapper synchronizing local checkout state with
//! remote `WorkspaceService`.
//!
//! # Authority and Crash-Safety Ordering
//! `LockedCommitCloudWorkingCopy::finish` completes `LocalWorkingCopy::finish` before
//! publishing `WorkspaceState` (`commit_id`, `operation_id`, `tree_id`/`tree_ids`, and
//! `sparse_patterns`) to `WorkspaceService`. Explicit commit IDs live only in memory on
//! the locked handle so dropped locks leave zero stale state; when `operation_id` advances,
//! the server derives `@` from `View.wc_commit_ids[workspace_name]`. Renaming upserts the
//! new workspace name before deleting the previous record.
//!
//! # Concurrency and Merge Invariants
//! Keyed by `(repo_id, workspace_name)` with `user` stored as a mutable non-key attribute.
//! `start_mutation()` reloads the workspace name after `working_copy.lock` is acquired.
//! `finish()` persists local state and unlocks `working_copy.lock` first, then acquires
//! `cc_sync.lock` off-executor and reloads on-disk state before issuing bounded
//! (`WORKSPACE_SYNC_TIMEOUT`) `UpdateWorkspace` and rename `DeleteWorkspace` RPCs. Superseded
//! publishes are skipped (while still deleting stale renamed names absent from disk); lock,
//! reload, or RPC errors warn without aborting local persistence. Conflicted checkouts preserve
//! all `Merge<TreeId>` sides in `tree_ids`.
//!
//! # Cross-Process and RPC Boundaries
//! Always dispatches RPCs onto `SHARED_RUNTIME` via [`crate::util::run_on_tokio`] so pooled
//! HTTP/2 channels never bind to ephemeral caller runtimes, logging `tracing::warn!` on
//! unreachable remotes so local working-copy durability is never lost.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
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

use crate::util::{CommitCloudConfig, connect_workspace_client, run_async, run_on_tokio};

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
    pub fn name() -> &'static str {
        "commit_cloud"
    }

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
        Self::name()
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
            current_commit_id: None,
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
    current_commit_id: Option<Vec<u8>>,
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
        self.current_commit_id = Some(commit.id().as_bytes().to_vec());
        Ok(stats)
    }

    fn rename_workspace(&mut self, new_workspace_name: WorkspaceNameBuf) {
        if new_workspace_name.as_str() != self.initial_workspace_name {
            self.renamed_from = Some(self.initial_workspace_name.clone());
        } else {
            self.renamed_from = None;
        }
        self.locked_inner.rename_workspace(new_workspace_name);
    }

    async fn reset(&mut self, commit: &Commit) -> Result<(), ResetError> {
        self.locked_inner.reset(commit).await?;
        self.current_commit_id = Some(commit.id().as_bytes().to_vec());
        Ok(())
    }

    async fn recover(&mut self, commit: &Commit) -> Result<(), ResetError> {
        self.locked_inner.recover(commit).await?;
        self.current_commit_id = Some(commit.id().as_bytes().to_vec());
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
        let config = self.config;
        let state_path = self.state_path;
        let user = self.user;
        let reload_context = self.reload_context;
        let sync_timeout = self.sync_timeout;
        let old_operation_id = self.locked_inner.old_operation_id().clone();
        let current_commit_id = self.current_commit_id;
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
            current_commit_id.unwrap_or_default()
        } else {
            Vec::new()
        };

        let expected_tree_state = (
            tree_id_bytes.clone(),
            tree_ids_bytes.clone(),
            conflict_labels.clone(),
        );
        match acquire_sync_plan(
            state_path.clone(),
            reload_context.clone(),
            new_operation_id,
            ws_name.clone(),
            expected_tree_state,
            sparse_patterns.clone(),
            renamed_from.clone(),
        )
        .await
        {
            SyncLockDecision::Publish(_sync_lock) => {
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
                    tracing::warn!("Failed to sync working copy state to WorkspaceService: {err}");
                }
                drop(_sync_lock);
            }
            SyncLockDecision::DeleteStaleRenamed {
                _sync_lock,
                old_name,
            } => {
                if let Err(err) =
                    delete_stale_renamed_workspace(config.clone(), old_name.clone(), sync_timeout)
                        .await
                {
                    tracing::warn!(
                        "Failed to remove renamed workspace '{old_name}' from WorkspaceService: {err}"
                    );
                }
                drop(_sync_lock);
            }
            SyncLockDecision::Skip => {}
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

enum DiskSyncState {
    Current,
    Superseded { current_ws_name: String },
}

enum SyncLockDecision {
    Publish(FileLock),
    DeleteStaleRenamed {
        _sync_lock: FileLock,
        old_name: String,
    },
    Skip,
}

fn check_disk_state(
    ctx: &LocalReloadContext,
    state_path: &Path,
    expected_op_id: &OperationId,
    expected_ws_name: &str,
    expected_tree_state: &TreeStateSnapshot,
    expected_sparse_patterns: &[String],
) -> Result<DiskSyncState, WorkingCopyStateError> {
    let reloaded = LocalWorkingCopy::load(
        ctx.store.clone(),
        ctx.working_copy_path.clone(),
        state_path.to_path_buf(),
        &ctx.settings,
    )?;
    let current_ws_name = reloaded.workspace_name().as_str().to_string();
    if reloaded.operation_id() != expected_op_id
        || current_ws_name != expected_ws_name
        || &extract_tree_state(&reloaded)? != expected_tree_state
        || extract_sparse_patterns(&reloaded)?.as_slice() != expected_sparse_patterns
    {
        return Ok(DiskSyncState::Superseded { current_ws_name });
    }
    Ok(DiskSyncState::Current)
}

fn lock_and_plan_sync(
    state_path: &Path,
    reload_context: Option<&LocalReloadContext>,
    expected_op_id: &OperationId,
    expected_ws_name: &str,
    expected_tree_state: &TreeStateSnapshot,
    expected_sparse_patterns: &[String],
    renamed_from: Option<&str>,
) -> SyncLockDecision {
    let sync_lock = match FileLock::lock(state_path.join("cc_sync.lock")) {
        Ok(lock) => lock,
        Err(err) => {
            tracing::warn!(
                "Failed to lock Commit Cloud working copy sync ({err}); skipping remote WorkspaceService sync"
            );
            return SyncLockDecision::Skip;
        }
    };

    if let Some(ctx) = reload_context {
        match check_disk_state(
            ctx,
            state_path,
            expected_op_id,
            expected_ws_name,
            expected_tree_state,
            expected_sparse_patterns,
        ) {
            Ok(DiskSyncState::Superseded { current_ws_name }) => {
                // Another command already persisted newer local checkout state; skip publishing
                // this superseded snapshot so remote WorkspaceService never regresses. If this
                // mutation renamed away from `old_name` and disk did not switch back to it,
                // still remove the stale remote record under `cc_sync.lock`.
                if let Some(old_name) = renamed_from
                    && old_name != expected_ws_name
                    && current_ws_name != old_name
                {
                    return SyncLockDecision::DeleteStaleRenamed {
                        _sync_lock: sync_lock,
                        old_name: old_name.to_string(),
                    };
                }
                return SyncLockDecision::Skip;
            }
            Ok(DiskSyncState::Current) => {}
            Err(err) => {
                tracing::warn!(
                    "Failed to reload on-disk working copy state before sync ({err}); skipping remote WorkspaceService sync"
                );
                return SyncLockDecision::Skip;
            }
        }
    }

    SyncLockDecision::Publish(sync_lock)
}

async fn acquire_sync_plan(
    state_path: PathBuf,
    reload_context: Option<LocalReloadContext>,
    expected_op_id: OperationId,
    expected_ws_name: String,
    expected_tree_state: TreeStateSnapshot,
    expected_sparse_patterns: Vec<String>,
    renamed_from: Option<String>,
) -> SyncLockDecision {
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
            Ok(decision) => decision,
            Err(err) => {
                tracing::warn!(
                    "Commit Cloud working copy sync lock task failed ({err}); skipping remote WorkspaceService sync"
                );
                SyncLockDecision::Skip
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
    let server_url = config.server_url;
    let repo_id = config.repo_id;

    tokio::time::timeout(sync_timeout, async move {
        let mut client = connect_workspace_client(server_url).await?;
        // Crash-safe rename ordering: publish the new workspace record before removing the
        // old name so an interrupted rename never leaves zero records on WorkspaceService.
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
                    workspace_name: old_name.clone(),
                })
                .await
        {
            tracing::warn!(
                "Failed to remove renamed workspace '{old_name}' from WorkspaceService: {del_err}"
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

async fn delete_stale_renamed_workspace(
    config: CommitCloudConfig,
    old_name: String,
    sync_timeout: Duration,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    run_on_tokio(async move {
        tokio::time::timeout(sync_timeout, async move {
            let mut client = connect_workspace_client(config.server_url).await?;
            client
                .delete_workspace(DeleteWorkspaceRequest {
                    repo_id: config.repo_id,
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
    })
    .await
}

async fn sync_workspace_state(
    payload: WorkspaceSyncPayload,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Always execute on `SHARED_RUNTIME` so pooled HTTP/2 channels never bind to an
    // ephemeral caller Tokio runtime.
    run_on_tokio(publish_workspace_state(payload)).await
}

#[derive(Default)]
pub struct CommitCloudWorkingCopyFactory;

impl CommitCloudWorkingCopyFactory {
    pub fn new() -> Self {
        Self
    }
}

/// Resolves the non-prescriptive mutable `WorkspaceState.user` attribute from user settings:
/// returns `user.email` if non-empty, else falls back to `user.name`, else empty string `""`.
pub fn resolve_user(settings: &UserSettings) -> String {
    let email = settings.user_email().trim();
    if !email.is_empty() {
        return email.to_string();
    }
    let name = settings.user_name().trim();
    if !name.is_empty() {
        return name.to_string();
    }
    String::new()
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
        let user = resolve_user(settings);
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
                message: format!("Failed to load Commit Cloud config: {e}"),
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
            // Derived by the server from the operation that added this workspace.
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
            tracing::warn!("Failed to register initial workspace in WorkspaceService: {err}");
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
        let user = resolve_user(settings);
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
                message: format!("Failed to load Commit Cloud config: {e}"),
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
mod tests;
