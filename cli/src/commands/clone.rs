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

//! `jj cc clone` command for attaching a new local workspace to a remote Commit Cloud repository.
//!
//! # Authority and Crash-Safety Ordering
//! Validates CLI arguments, optional `--project-id` binding, and workspace-name uniqueness
//! against remote operation heads before creating local directories or mutating remote state.
//! Initializes `.jj` metadata pointing at the remote `repo_id`, reconciles active operation
//! heads, creates a working-copy commit on top of the selected revision, and checks out its
//! tree. If any step after workspace creation fails, this command forgets and deletes only
//! the workspace created by this invocation and removes newly created directories.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use cc_lib::cc_backend::CommitCloudBackend;
use cc_lib::cc_index_store::CommitCloudIndexStore;
use cc_lib::cc_op_heads_store::CommitCloudOpHeadsStore;
use cc_lib::cc_op_store::CommitCloudOpStore;
use cc_lib::cc_working_copy::{
    CommitCloudWorkingCopy, CommitCloudWorkingCopyFactory, resolve_user,
};
use clap::Parser;
use jj_cli::cli_util::{CommandHelper, RevisionArg};
use jj_cli::command_error::{CommandError, cli_error, user_error};
use jj_cli::ui::Ui;
use jj_lib::backend::BackendInitError;
use jj_lib::commit::Commit;
use jj_lib::default_backend_factories::{
    default_submodule_store_initializer, default_workspace_store_initializer,
};
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_store::{OpStore as _, RootOperationData};
use jj_lib::ref_name::{WorkspaceName, WorkspaceNameBuf};
use jj_lib::repo::{ReadonlyRepo, Repo as _};
use jj_lib::settings::UserSettings;
use jj_lib::signing_factory::signer_from_settings;
use jj_lib::workspace::Workspace;
use uuid::Uuid;

#[derive(Parser, Clone, Debug)]
pub struct CcCloneArgs {
    /// Remote gRPC server URL (e.g. http://127.0.0.1:8080)
    #[arg(long)]
    pub server: String,

    /// Existing remote repository ID to clone
    pub repo_id: String,

    /// Destination directory for the cloned workspace (defaults to <repo_id>)
    pub destination: Option<String>,

    /// Optional shared project ID to verify against the remote repository (pass when cloning a
    /// multi-repo project to share local `jj-cc-daemon` L1/L2 cache entries across repositories)
    #[arg(long)]
    pub project_id: Option<String>,

    /// Optional workspace identifier (defaults to a unique ws-<short_id>)
    #[arg(long)]
    pub workspace_name: Option<String>,

    /// Revision (commit ID, change ID, or revset) to check out on clone
    #[arg(short = 'r', long)]
    pub revision: Option<String>,

    /// Working copy implementation ("commit_cloud" syncs state to WorkspaceService, or "local")
    #[arg(long, default_value = "commit_cloud")]
    pub working_copy_type: String,
}

pub(crate) fn find_first_missing_ancestor(path: &Path) -> Option<PathBuf> {
    if path.exists() {
        return None;
    }
    let mut curr = path.to_path_buf();
    while let Some(parent) = curr.parent() {
        if parent.as_os_str().is_empty() || parent.exists() {
            break;
        }
        curr = parent.to_path_buf();
    }
    Some(curr)
}

/// Cleans up directories created by a failed `jj cc init` or `jj cc clone` without recursively
/// removing shared parent directories that a concurrent process may now occupy.
pub(crate) fn cleanup_failed_workspace_dirs(
    dest_path: &Path,
    dest_existed: bool,
    dot_jj_existed: bool,
    first_created_dir: Option<&Path>,
) {
    if !dest_existed {
        let _ = fs::remove_dir_all(dest_path);
        if let Some(stop_at) = first_created_dir {
            let mut curr = dest_path.parent();
            while let Some(dir) = curr {
                if dir.as_os_str().is_empty() || !dir.starts_with(stop_at) {
                    break;
                }
                if fs::remove_dir(dir).is_err() {
                    break;
                }
                if dir == stop_at {
                    break;
                }
                curr = dir.parent();
            }
        }
    } else if !dot_jj_existed {
        let _ = fs::remove_dir_all(dest_path.join(".jj"));
    }
}

pub(crate) async fn remote_workspace_exists(
    server_url: &str,
    repo_id: &str,
    ws_name: &str,
    allow_missing_repo: bool,
) -> Result<bool, CommandError> {
    let op_heads_store = CommitCloudOpHeadsStore::new(repo_id.to_string(), server_url.to_string());
    let op_store = CommitCloudOpStore::new(repo_id.to_string(), server_url.to_string());
    let heads = match op_heads_store.get_op_heads_without_reconciliation().await {
        Ok(heads) => heads,
        Err(jj_lib::op_heads_store::OpHeadsStoreError::Read(ref source))
            if allow_missing_repo && cc_lib::util::is_grpc_not_found(source.as_ref()) =>
        {
            return Ok(false);
        }
        Err(e) => {
            return Err(user_error(format!(
                "Failed to inspect remote repository '{repo_id}' on {server_url}: {e}"
            )));
        }
    };
    let ws_ref = WorkspaceName::new(ws_name);
    for head_id in heads {
        if head_id.as_bytes() == cc_common::ROOT_OPERATION_ID_BYTES {
            continue;
        }
        let op = op_store.read_operation(&head_id).await.map_err(|e| {
            user_error(format!(
                "Failed to read remote operation {} for repository '{repo_id}': {e}",
                head_id.hex()
            ))
        })?;
        let view = op_store.read_view(&op.view_id).await.map_err(|e| {
            user_error(format!(
                "Failed to read remote view {} for repository '{repo_id}': {e}",
                op.view_id.hex()
            ))
        })?;
        if view.wc_commit_ids.contains_key(ws_ref) {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn delete_remote_workspace_records(
    server_url: &str,
    repo_id: &str,
    ws_name: &str,
    working_copy_type: &str,
    users: &[String],
) {
    if working_copy_type == CommitCloudWorkingCopy::name() {
        let mut seen = std::collections::HashSet::new();
        for user in users {
            if !seen.insert(user.as_str()) {
                continue;
            }
            let server_url = server_url.to_string();
            let repo_id = repo_id.to_string();
            let user = user.clone();
            let ws_name_del = ws_name.to_string();
            let _ = cc_lib::util::run_on_tokio(async move {
                let mut client = cc_lib::util::connect_workspace_client(server_url).await?;
                let _ = client
                    .delete_workspace(cc_common::workspace::DeleteWorkspaceRequest {
                        repo_id,
                        user,
                        workspace_name: ws_name_del,
                    })
                    .await;
                Ok(())
            })
            .await;
        }
    }
}

pub(crate) async fn rollback_created_remote_workspace(
    repo: &Arc<ReadonlyRepo>,
    server_url: &str,
    repo_id: &str,
    ws_name: &str,
    working_copy_type: &str,
    users: &[String],
    reason: &str,
) {
    let ws_ref = WorkspaceName::new(ws_name);
    if repo.view().get_wc_commit_id(ws_ref).is_some() {
        let mut forget_tx = repo.start_transaction();
        if forget_tx.repo_mut().remove_workspace(ws_ref).await.is_ok()
            && forget_tx.repo_mut().rebase_descendants().await.is_ok()
        {
            let _ = forget_tx.commit(format!("{reason} {ws_name}")).await;
        }
    }
    delete_remote_workspace_records(server_url, repo_id, ws_name, working_copy_type, users).await;
}

struct RollbackTempDirGuard(PathBuf);

impl Drop for RollbackTempDirGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn create_private_temp_dir(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new().mode(0o700).create(path)
    }
    #[cfg(not(unix))]
    {
        fs::create_dir(path)
    }
}

/// Best-effort cleanup when `Workspace::init_with_factories` fails after attaching to or creating
/// a remote repository (at which point `jj_lib` has already deleted `.jj` locally, but an
/// `add workspace` op and/or `WorkspaceService` record may already have been published remotely).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn rollback_failed_init_remote_workspace(
    user_settings: &UserSettings,
    server_url: &str,
    repo_id: &str,
    project_id: &str,
    ws_name: &str,
    working_copy_type: &str,
    users: &[String],
    reason: &str,
) {
    let temp_root = std::env::temp_dir().join(format!("jj-cc-rollback-{}", Uuid::new_v4()));
    let _guard = RollbackTempDirGuard(temp_root.clone());
    let index_dir = temp_root.join("index");
    let submod_dir = temp_root.join("submodules");
    let ws_store_dir = temp_root.join("workspace_store");
    let loaded_repo = if create_private_temp_dir(&temp_root).is_ok()
        && fs::create_dir(&index_dir).is_ok()
        && fs::create_dir(&submod_dir).is_ok()
        && fs::create_dir(&ws_store_dir).is_ok()
        && let Ok(index_store) = jj_lib::default_index::DefaultIndexStore::init(&index_dir)
        && let Ok(ws_store) = default_workspace_store_initializer()(user_settings, &ws_store_dir)
        && let Ok(signer) = signer_from_settings(user_settings)
        && let Ok(merge_options) = user_settings.merge_options()
    {
        let backend = CommitCloudBackend::from_config(cc_lib::util::CommitCloudConfig {
            server_url: server_url.to_string(),
            repo_id: repo_id.to_string(),
            project_id: (!project_id.is_empty()).then(|| project_id.to_string()),
        });
        let store = jj_lib::store::Store::new(Box::new(backend), signer, merge_options);
        let op_store = Arc::new(CommitCloudOpStore::new(
            repo_id.to_string(),
            server_url.to_string(),
        ));
        let op_heads_store = Arc::new(CommitCloudOpHeadsStore::new(
            repo_id.to_string(),
            server_url.to_string(),
        ));
        let submodule_store =
            Arc::new(jj_lib::default_submodule_store::DefaultSubmoduleStore::init(&submod_dir));
        let loader = jj_lib::repo::RepoLoader::new(
            user_settings.clone(),
            store,
            Arc::from(ws_store),
            op_store,
            op_heads_store,
            Arc::new(index_store),
            submodule_store,
        );
        loader.load_at_head(None).await.ok()
    } else {
        None
    };

    if let Some(repo) = loaded_repo {
        rollback_created_remote_workspace(
            &repo,
            server_url,
            repo_id,
            ws_name,
            working_copy_type,
            users,
            reason,
        )
        .await;
    } else {
        delete_remote_workspace_records(server_url, repo_id, ws_name, working_copy_type, users)
            .await;
    }
}

pub async fn cmd_cc_clone(
    ui: &mut Ui,
    command_helper: &CommandHelper,
    args: &CcCloneArgs,
) -> Result<(), CommandError> {
    if command_helper.global_args().at_operation.is_some() {
        return Err(cli_error("--at-op is not respected"));
    }
    if args.repo_id.trim().is_empty() {
        return Err(user_error("Repository ID must not be empty or whitespace"));
    }
    if let Some(proj) = args.project_id.as_deref()
        && proj.trim().is_empty()
    {
        return Err(user_error("--project-id must not be empty or whitespace"));
    }
    if let Some(ws) = args.workspace_name.as_deref()
        && ws.trim().is_empty()
    {
        return Err(user_error(
            "--workspace-name must not be empty or whitespace",
        ));
    }

    let working_copy_factory: Box<dyn jj_lib::working_copy::WorkingCopyFactory> =
        match args.working_copy_type.as_str() {
            "commit_cloud" => Box::new(CommitCloudWorkingCopyFactory::new()),
            "local" => Box::new(jj_lib::local_working_copy::LocalWorkingCopyFactory {}),
            other => {
                return Err(user_error(format!(
                    "Unsupported working copy type '{other}' (expected 'commit_cloud' or 'local')"
                )));
            }
        };

    let dest_str = match args.destination.as_deref() {
        Some(dest) => dest,
        None => {
            let mut comps = Path::new(&args.repo_id).components();
            match (comps.next(), comps.next()) {
                (Some(std::path::Component::Normal(_)), None) => args.repo_id.as_str(),
                _ => {
                    return Err(user_error(format!(
                        "Repository ID '{}' cannot be used as a default destination directory; pass an explicit destination path",
                        args.repo_id
                    )));
                }
            }
        }
    };
    let dest_path = Path::new(dest_str);
    let dest_existed = dest_path.exists();
    if dest_existed {
        let is_non_empty = fs::read_dir(dest_path)
            .map_err(|e| {
                user_error(format!(
                    "Failed to inspect clone directory {}: {e}",
                    dest_path.display()
                ))
            })?
            .next()
            .is_some();
        if is_non_empty {
            return Err(user_error(
                "Destination path exists and is not an empty directory",
            ));
        }
    }

    let ws_name = args.workspace_name.clone().unwrap_or_else(|| {
        let short = &Uuid::new_v4().simple().to_string()[..8];
        format!("ws-{short}")
    });

    // Validate local settings, signer, and remote workspace availability before creating any
    // local directories or mutating remote state.
    let (init_settings, _) = command_helper.settings_for_new_workspace(ui, dest_path)?;
    let signer = signer_from_settings(&init_settings)
        .map_err(|e| user_error(format!("Failed to initialize signature signer: {e:?}")))?;
    let init_user = if args.working_copy_type == CommitCloudWorkingCopy::name() {
        Some(resolve_user(&init_settings).map_err(|e| user_error(e.message))?)
    } else {
        None
    };

    if remote_workspace_exists(&args.server, &args.repo_id, &ws_name, false).await? {
        return Err(user_error(format!(
            "Workspace '{ws_name}' already exists in repository '{}'. Pass a unique `--workspace-name <name>` (or omit `--workspace-name` to generate one) instead of reusing an existing workspace name.",
            args.repo_id
        )));
    }

    let first_created_dir = find_first_missing_ancestor(dest_path);
    let dot_jj_existed = dest_path.join(".jj").exists();
    if !dest_existed {
        fs::create_dir_all(dest_path).map_err(|e| {
            user_error(format!(
                "Failed to create clone directory {}: {e}",
                dest_path.display()
            ))
        })?;
    }

    let attached_ids = Arc::new(Mutex::new((String::new(), String::new())));
    let ids_slot = Arc::clone(&attached_ids);
    let backend_initializer = move |_settings: &UserSettings, store_path: &Path| {
        let backend = CommitCloudBackend::init_in_project(
            store_path,
            &args.server,
            None,
            args.project_id.as_deref(),
            Some(&args.repo_id),
            false,
        )
        .map_err(BackendInitError)?;
        *ids_slot.lock().unwrap() = (
            backend.repo_id().to_string(),
            backend.project_id().to_string(),
        );
        Ok(Box::new(backend) as Box<dyn jj_lib::backend::Backend>)
    };

    let op_store_initializer =
        |_settings: &UserSettings, store_path: &Path, _root_op_data: RootOperationData| {
            let op_store = CommitCloudOpStore::load(store_path).map_err(BackendInitError)?;
            Ok(Box::new(op_store) as Box<dyn jj_lib::op_store::OpStore>)
        };

    let op_heads_store_initializer =
        |_settings: &UserSettings,
         store_path: &Path,
         _root_op_id: &jj_lib::op_store::OperationId| {
            let op_heads_store =
                CommitCloudOpHeadsStore::load(store_path).map_err(BackendInitError)?;
            Ok(Box::new(op_heads_store) as Box<dyn jj_lib::op_heads_store::OpHeadsStore>)
        };

    let index_store_initializer = |settings: &UserSettings, store_path: &Path| {
        let index_store =
            CommitCloudIndexStore::init(settings, store_path).map_err(BackendInitError)?;
        Ok(Box::new(index_store) as Box<dyn jj_lib::index_store::IndexStore>)
    };

    let clone_res: Result<(), CommandError> = async {
        let workspace_name = WorkspaceNameBuf::from(ws_name.clone());
        let (init_ws, init_repo) = match Workspace::init_with_factories(
            &init_settings,
            dest_path,
            &workspace_name,
            &backend_initializer,
            signer,
            default_workspace_store_initializer(),
            &op_store_initializer,
            &op_heads_store_initializer,
            &index_store_initializer,
            default_submodule_store_initializer(),
            working_copy_factory.as_ref(),
        )
        .await
        {
            Ok(pair) => pair,
            Err(e) => {
                let (repo_id, project_id) = attached_ids.lock().unwrap().clone();
                if !repo_id.is_empty() {
                    let rollback_users: Vec<String> = init_user.into_iter().collect();
                    rollback_failed_init_remote_workspace(
                        &init_settings,
                        &args.server,
                        &repo_id,
                        &project_id,
                        &ws_name,
                        &args.working_copy_type,
                        &rollback_users,
                        "forget failed clone workspace",
                    )
                    .await;
                }
                return Err(user_error(format!(
                    "Failed to initialize cloned workspace: {e:?}"
                )));
            }
        };

        let mut rollback_repo = init_repo;
        let mut rollback_settings = init_settings.clone();
        let mut rollback_users: Vec<String> = init_user.into_iter().collect();

        let post_init_res: Result<(), CommandError> = async {
            // Reload settings and workspace at dest_path so CWD's repository config never leaks
            // into the clone.
            let (clone_settings, _) = command_helper.settings_for_new_workspace(ui, dest_path)?;
            rollback_settings = clone_settings.clone();
            if args.working_copy_type == CommitCloudWorkingCopy::name() {
                rollback_users
                    .push(resolve_user(&clone_settings).map_err(|e| user_error(e.message))?);
            }
            let workspace = command_helper.load_workspace_at(dest_path, &clone_settings)?;
            let repo = workspace.repo_loader().load_at_head(None).await?;
            rollback_repo = repo.clone();
            let mut workspace_command = command_helper.for_workable_repo(ui, workspace, repo)?;

            let target_commit = if let Some(rev_str) = &args.revision {
                let rev_arg = RevisionArg::from(rev_str.clone());
                workspace_command
                    .resolve_single_rev(ui, &rev_arg)
                    .await
                    .map(Some)?
            } else {
                select_default_clone_head(
                    workspace_command.repo(),
                    workspace_command.get_wc_commit_id(),
                )
                .await?
            };

            if let Some(commit) = target_commit {
                let mut tx = workspace_command.start_transaction();
                tx.check_out(&commit)
                    .map_err(|e| user_error(format!("Failed to check out cloned commit: {e:?}")))?;
                tx.finish(
                    ui,
                    format!("check out cloned Commit Cloud commit {}", commit.id().hex()),
                )
                .await?;
            }
            Ok(())
        }
        .await;

        if let Err(err) = post_init_res {
            let reload_ws = command_helper
                .load_workspace_at(dest_path, &rollback_settings)
                .ok();
            let rollback_loader = reload_ws.as_ref().unwrap_or(&init_ws).repo_loader();
            let latest_rollback_repo = rollback_loader
                .load_at_head(None)
                .await
                .unwrap_or(rollback_repo);
            rollback_created_remote_workspace(
                &latest_rollback_repo,
                &args.server,
                &args.repo_id,
                &ws_name,
                &args.working_copy_type,
                &rollback_users,
                "forget failed clone workspace",
            )
            .await;
            return Err(err);
        }
        Ok(())
    }
    .await;

    if let Err(err) = clone_res {
        cleanup_failed_workspace_dirs(
            dest_path,
            dest_existed,
            dot_jj_existed,
            first_created_dir.as_deref(),
        );
        return Err(err);
    }

    println!(
        "Cloned Commit Cloud repository {} into {}",
        args.repo_id,
        dest_path.display()
    );
    Ok(())
}

async fn select_default_clone_head(
    repo: &ReadonlyRepo,
    own_wc_id: Option<&jj_lib::backend::CommitId>,
) -> Result<Option<Commit>, CommandError> {
    let root_id = repo.store().root_commit_id();
    let wc_commit_ids: HashSet<&jj_lib::backend::CommitId> =
        repo.view().wc_commit_ids().values().collect();
    let mut candidates = Vec::new();
    for head_id in repo.view().heads() {
        if head_id == root_id || Some(head_id) == own_wc_id {
            continue;
        }
        let commit = repo
            .store()
            .get_commit_async(head_id)
            .await
            .map_err(|e| user_error(format!("Failed to read remote head commit: {e:?}")))?;
        if !commit.description().is_empty() {
            candidates.push((true, commit.committer().timestamp.timestamp.0, commit));
            continue;
        }
        let is_non_empty = !commit
            .is_empty(repo.index())
            .await
            .map_err(|e| user_error(format!("Failed to inspect head tree: {e:?}")))?;
        // Only unwrap or skip an empty, undescribed head when it is a peer workspace's active `@`:
        // skip peer `@` commits directly atop `root()` (freshly initialized repos) and unwrap peer
        // `@` commits atop a single non-root parent so clones never stack directly on another
        // workspace's empty `@`. Leave intentional non-WC empty commits unwrapped.
        if !is_non_empty && wc_commit_ids.contains(head_id) {
            if commit.parent_ids().len() == 1 {
                let parent_id = &commit.parent_ids()[0];
                if parent_id == root_id {
                    continue;
                }
                let parent = repo
                    .store()
                    .get_commit_async(parent_id)
                    .await
                    .map_err(|e| user_error(format!("Failed to read parent commit: {e:?}")))?;
                let parent_is_interesting = !parent.description().is_empty()
                    || !parent
                        .is_empty(repo.index())
                        .await
                        .map_err(|e| user_error(format!("Failed to inspect parent tree: {e:?}")))?;
                if parent_is_interesting {
                    candidates.push((true, parent.committer().timestamp.timestamp.0, parent));
                } else if !wc_commit_ids.contains(parent_id) {
                    candidates.push((false, parent.committer().timestamp.timestamp.0, parent));
                }
            } else {
                candidates.push((false, commit.committer().timestamp.timestamp.0, commit));
            }
        } else {
            candidates.push((
                is_non_empty,
                commit.committer().timestamp.timestamp.0,
                commit,
            ));
        }
    }

    candidates.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then_with(|| a.1.cmp(&b.1))
            .then_with(|| a.2.id().cmp(b.2.id()))
    });
    Ok(candidates.pop().map(|(_, _, c)| c))
}

#[cfg(test)]
mod tests {
    use super::*;
    use jj_lib::config::StackedConfig;

    #[tokio::test]
    async fn rollback_failed_init_remote_workspace_cleans_published_workspace_and_preserves_peers()
    {
        let server = testutils::spawn_server().await;
        let dir = tempfile::tempdir().unwrap();
        let store_dir = dir.path().join("store");
        fs::create_dir_all(&store_dir).unwrap();

        let backend = CommitCloudBackend::init_in_project(
            &store_dir,
            server.url(),
            None,
            Some("proj-rb"),
            Some("repo-rb"),
            true,
        )
        .unwrap();
        let repo_id = backend.repo_id().to_string();
        let project_id = backend.project_id().to_string();

        let settings =
            UserSettings::from_config_and_home_dir(StackedConfig::with_defaults(), None).unwrap();
        let index_dir = dir.path().join("idx");
        let submod_dir = dir.path().join("submod");
        let ws_store_dir = dir.path().join("ws_store");
        fs::create_dir_all(&index_dir).unwrap();
        fs::create_dir_all(&submod_dir).unwrap();
        fs::create_dir_all(&ws_store_dir).unwrap();
        let store = jj_lib::store::Store::new(
            Box::new(backend),
            signer_from_settings(&settings).unwrap(),
            settings.merge_options().unwrap(),
        );
        let ws_store = default_workspace_store_initializer()(&settings, &ws_store_dir).unwrap();
        let op_store = Arc::new(CommitCloudOpStore::new(
            repo_id.clone(),
            server.url().to_string(),
        ));
        let op_heads_store = Arc::new(CommitCloudOpHeadsStore::new(
            repo_id.clone(),
            server.url().to_string(),
        ));
        let loader = jj_lib::repo::RepoLoader::new(
            settings.clone(),
            store,
            Arc::from(ws_store),
            op_store,
            op_heads_store.clone(),
            Arc::new(jj_lib::default_index::DefaultIndexStore::init(&index_dir).unwrap()),
            Arc::new(jj_lib::default_submodule_store::DefaultSubmoduleStore::init(&submod_dir)),
        );

        // Publish an existing "default" workspace and a leaked "failed-ws" workspace op +
        // WorkspaceService record (simulating a failure inside `Workspace::init_with_factories`
        // after `init_working_copy` already published remotely).
        let repo = loader.load_at_head(None).await.unwrap();
        let mut tx = repo.start_transaction();
        tx.repo_mut()
            .check_out(
                WorkspaceNameBuf::from("default"),
                &repo.store().root_commit(),
            )
            .await
            .unwrap();
        let repo = tx.commit("add default workspace").await.unwrap();

        let mut tx2 = repo.start_transaction();
        tx2.repo_mut()
            .check_out(
                WorkspaceNameBuf::from("failed-ws"),
                &repo.store().root_commit(),
            )
            .await
            .unwrap();
        let repo2 = tx2.commit("add failed-ws workspace").await.unwrap();

        let server_url_owned = server.url().to_string();
        let repo_id_owned = repo_id.clone();
        let op_bytes = repo2.op_id().as_bytes().to_vec();
        #[allow(deprecated)]
        cc_lib::util::run_on_tokio(async move {
            let mut client = cc_lib::util::connect_workspace_client(server_url_owned).await?;
            client
                .update_workspace(cc_common::workspace::UpdateWorkspaceRequest {
                    workspace: Some(cc_common::workspace::WorkspaceState {
                        repo_id: repo_id_owned,
                        user: "alice@example.com".to_string(),
                        workspace_name: "failed-ws".to_string(),
                        commit_id: Vec::new(),
                        operation_id: op_bytes,
                        tree_id: cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                        sparse_patterns: vec![String::new()],
                        tree_ids: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
                        conflict_labels: Vec::new(),
                        sparse_patterns_set: true,
                    }),
                    expected_operation_id: None,
                })
                .await?;
            Ok(())
        })
        .await
        .unwrap();

        assert!(
            remote_workspace_exists(server.url(), &repo_id, "failed-ws", false)
                .await
                .unwrap()
        );

        rollback_failed_init_remote_workspace(
            &settings,
            server.url(),
            &repo_id,
            &project_id,
            "failed-ws",
            CommitCloudWorkingCopy::name(),
            &["alice@example.com".to_string()],
            "forget failed init workspace",
        )
        .await;

        assert!(
            !remote_workspace_exists(server.url(), &repo_id, "failed-ws", false)
                .await
                .unwrap(),
            "failed-ws must be forgotten from remote view"
        );
        assert!(
            remote_workspace_exists(server.url(), &repo_id, "default", false)
                .await
                .unwrap(),
            "existing peer workspace 'default' must remain intact"
        );

        let server_url_check = server.url().to_string();
        let repo_id_check = repo_id.clone();
        let leaked_ws = cc_lib::util::run_on_tokio(async move {
            let mut client = cc_lib::util::connect_workspace_client(server_url_check).await?;
            Ok(client
                .get_workspace(cc_common::workspace::GetWorkspaceRequest {
                    repo_id: repo_id_check,
                    user: "alice@example.com".to_string(),
                    workspace_name: "failed-ws".to_string(),
                })
                .await?
                .into_inner()
                .workspace)
        })
        .await
        .unwrap();
        assert!(
            leaked_ws.is_none(),
            "WorkspaceService record for failed-ws must be deleted"
        );

        // Calling rollback for a workspace that was never published must not create a new op.
        let heads_before = op_heads_store
            .get_op_heads_without_reconciliation()
            .await
            .unwrap();
        rollback_failed_init_remote_workspace(
            &settings,
            server.url(),
            &repo_id,
            &project_id,
            "never-published-ws",
            CommitCloudWorkingCopy::name(),
            &["alice@example.com".to_string()],
            "forget failed init workspace",
        )
        .await;
        let heads_after = op_heads_store
            .get_op_heads_without_reconciliation()
            .await
            .unwrap();
        assert_eq!(
            heads_before, heads_after,
            "rollback for unpublished workspace must not emit a spurious op"
        );
    }

    #[test]
    fn cleanup_failed_workspace_dirs_preserves_concurrent_sibling_and_preexisting_dest() {
        let root = tempfile::tempdir().unwrap();

        // Nested missing ancestors with a concurrent sibling created under `first_created_dir`:
        // removes `dest` and empty intermediate dirs, but preserves `work/ws2`.
        let dest = root.path().join("work/nested/ws1");
        let first_created = find_first_missing_ancestor(&dest);
        assert_eq!(
            first_created.as_deref(),
            Some(root.path().join("work").as_path())
        );
        fs::create_dir_all(dest.join(".jj/repo")).unwrap();
        let sibling_file = root.path().join("work/ws2/keep.txt");
        fs::create_dir_all(sibling_file.parent().unwrap()).unwrap();
        fs::write(&sibling_file, "concurrent sibling").unwrap();

        cleanup_failed_workspace_dirs(&dest, false, false, first_created.as_deref());
        assert!(!dest.exists(), "failed destination dir must be removed");
        assert!(
            !root.path().join("work/nested").exists(),
            "empty created intermediate dir should be removed"
        );
        assert_eq!(
            fs::read_to_string(&sibling_file).unwrap(),
            "concurrent sibling",
            "concurrent sibling under first_created_dir must never be deleted"
        );

        // When no sibling exists, all newly created empty ancestors up to `first_created_dir`
        // are removed.
        let dest_empty = root.path().join("empty_chain/a/b/ws");
        let first_empty = find_first_missing_ancestor(&dest_empty);
        fs::create_dir_all(dest_empty.join(".jj")).unwrap();
        cleanup_failed_workspace_dirs(&dest_empty, false, false, first_empty.as_deref());
        assert!(
            !root.path().join("empty_chain").exists(),
            "all empty newly created ancestor dirs should be cleaned up"
        );

        // When `dest` already existed, only newly created `.jj` is removed.
        let existing_dest = root.path().join("existing_dest");
        fs::create_dir_all(&existing_dest).unwrap();
        let existing_file = existing_dest.join("user_file.txt");
        fs::write(&existing_file, "keep me").unwrap();
        fs::create_dir_all(existing_dest.join(".jj/repo")).unwrap();
        cleanup_failed_workspace_dirs(&existing_dest, true, false, None);
        assert!(!existing_dest.join(".jj").exists());
        assert_eq!(fs::read_to_string(&existing_file).unwrap(), "keep me");
    }

    #[tokio::test]
    async fn select_default_clone_head_skips_peer_empty_wc_on_root_and_preserves_non_wc_empty_head()
    {
        let server = testutils::spawn_server().await;
        let dir = tempfile::tempdir().unwrap();
        let store_dir = dir.path().join("store");
        fs::create_dir_all(&store_dir).unwrap();

        let backend = CommitCloudBackend::init_in_project(
            &store_dir,
            server.url(),
            None,
            Some("proj-head-sel"),
            Some("repo-head-sel"),
            true,
        )
        .unwrap();
        let repo_id = backend.repo_id().to_string();

        let settings =
            UserSettings::from_config_and_home_dir(StackedConfig::with_defaults(), None).unwrap();
        let index_dir = dir.path().join("idx");
        let submod_dir = dir.path().join("submod");
        let ws_store_dir = dir.path().join("ws_store");
        fs::create_dir_all(&index_dir).unwrap();
        fs::create_dir_all(&submod_dir).unwrap();
        fs::create_dir_all(&ws_store_dir).unwrap();
        let store = jj_lib::store::Store::new(
            Box::new(backend),
            signer_from_settings(&settings).unwrap(),
            settings.merge_options().unwrap(),
        );
        let ws_store = default_workspace_store_initializer()(&settings, &ws_store_dir).unwrap();
        let op_store = Arc::new(CommitCloudOpStore::new(
            repo_id.clone(),
            server.url().to_string(),
        ));
        let op_heads_store = Arc::new(CommitCloudOpHeadsStore::new(
            repo_id,
            server.url().to_string(),
        ));
        let loader = jj_lib::repo::RepoLoader::new(
            settings,
            store,
            Arc::from(ws_store),
            op_store,
            op_heads_store,
            Arc::new(jj_lib::default_index::DefaultIndexStore::init(&index_dir).unwrap()),
            Arc::new(jj_lib::default_submodule_store::DefaultSubmoduleStore::init(&submod_dir)),
        );

        // Freshly initialized repo with only a peer workspace's empty WC commit on `root()`:
        // must return `None` so the clone stays on `root()` instead of stacking on `@`.
        let repo = loader.load_at_head(None).await.unwrap();
        let root_commit = repo.store().root_commit();
        let mut tx = repo.start_transaction();
        let _peer_wc = tx
            .repo_mut()
            .check_out(WorkspaceNameBuf::from("default"), &root_commit)
            .await
            .unwrap();
        let own_wc = tx
            .repo_mut()
            .check_out(WorkspaceNameBuf::from("ws-clone"), &root_commit)
            .await
            .unwrap();
        let repo = tx.commit("add peer and clone workspaces").await.unwrap();

        let selected_fresh = select_default_clone_head(&repo, Some(own_wc.id()))
            .await
            .unwrap();
        assert!(
            selected_fresh.is_none(),
            "freshly initialized repo with only empty peer WC on root() must return None, got: {:?}",
            selected_fresh.map(|c| c.id().hex())
        );

        // An intentional empty, undescribed non-WC head atop a described parent must NOT be
        // unwrapped to its parent.
        let mut tx2 = repo.start_transaction();
        let c1 = tx2
            .repo_mut()
            .new_commit(vec![root_commit.id().clone()], root_commit.tree())
            .set_description("release v1")
            .write()
            .await
            .unwrap();
        let non_wc_empty = tx2
            .repo_mut()
            .new_commit(vec![c1.id().clone()], c1.tree())
            .write()
            .await
            .unwrap();
        let repo = tx2.commit("add non-WC empty head").await.unwrap();

        let selected_non_wc = select_default_clone_head(&repo, Some(own_wc.id()))
            .await
            .unwrap()
            .expect("non-WC empty head should be selected");
        assert_eq!(
            selected_non_wc.id(),
            non_wc_empty.id(),
            "non-WC empty head must stay unwrapped instead of falling back to its parent"
        );

        // A peer workspace's empty, undescribed WC commit atop `c1` unwraps to `c1`.
        let mut tx3 = repo.start_transaction();
        let _peer_wc_on_c1 = tx3
            .repo_mut()
            .check_out(WorkspaceNameBuf::from("default"), &c1)
            .await
            .unwrap();
        tx3.repo_mut().rebase_descendants().await.unwrap();
        let repo = tx3.commit("peer workspace on c1").await.unwrap();

        let selected_unwrapped = select_default_clone_head(&repo, Some(own_wc.id()))
            .await
            .unwrap()
            .expect("peer empty WC atop described commit should unwrap to c1");
        assert_eq!(selected_unwrapped.id(), c1.id());
    }
}
