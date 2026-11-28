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

//! `jj cc init` command for creating or attaching a local Commit Cloud workspace.
//!
//! # Authority and Crash-Safety Ordering
//! Validates CLI arguments, settings, and signing configuration before creating local
//! directories. Registers or verifies remote `repo_id` and `project_id` before writing
//! `.jj/repo/store/config.toml`; if post-init indexing (`--index-commit`) fails, rolls
//! back any newly created remote workspace entry and removes newly created directories.

use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};

use cc_lib::cc_backend::CommitCloudBackend;
use cc_lib::cc_index_store::CommitCloudIndexStore;
use cc_lib::cc_op_heads_store::CommitCloudOpHeadsStore;
use cc_lib::cc_op_store::CommitCloudOpStore;
use cc_lib::cc_working_copy::CommitCloudWorkingCopyFactory;
use clap::Parser;
use jj_cli::cli_util::CommandHelper;
use jj_cli::command_error::{CommandError, user_error};
use jj_cli::ui::Ui;
use jj_lib::backend::{BackendInitError, CommitId};
use jj_lib::default_backend_factories::{
    default_submodule_store_initializer, default_workspace_store_initializer,
};
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_store::RootOperationData;
use jj_lib::ref_name::WorkspaceNameBuf;
use jj_lib::settings::UserSettings;
use jj_lib::signing_factory::signer_from_settings;
use jj_lib::workspace::Workspace;

use crate::commands::clone::{
    cleanup_failed_workspace_dirs, find_first_missing_ancestor, remote_workspace_exists,
    rollback_created_remote_workspace, rollback_failed_init_remote_workspace,
};

#[derive(Parser, Clone, Debug)]
pub struct CcInitArgs {
    /// Remote gRPC server URL (e.g. http://127.0.0.1:8080)
    #[arg(long)]
    pub server: String,

    /// Create and register a new remote repository (default when --repo-id is omitted)
    #[arg(long)]
    pub create: bool,

    /// Existing remote repository ID to attach to (or explicit ID when paired with --create)
    #[arg(long)]
    pub repo_id: Option<String>,

    /// Optional shared project ID owning commits, trees, symlinks, and blobs across repositories
    /// (pass when attaching with `--repo-id` to share local `jj-cc-daemon` L1/L2 cache entries)
    #[arg(long)]
    pub project_id: Option<String>,

    /// Optional human-readable repository name when creating a new repository
    #[arg(long)]
    pub name: Option<String>,

    /// Local workspace identifier recorded in repository views
    #[arg(long, default_value = "default")]
    pub workspace_name: String,

    /// Working copy implementation ("commit_cloud" syncs state to WorkspaceService, or "local")
    #[arg(long, default_value = "commit_cloud")]
    pub working_copy_type: String,

    /// Full commit ID to index with its ancestors so it resolves when
    /// commit-cloud.sync-project-commits is false (repeatable)
    #[arg(long = "index-commit", value_name = "COMMIT_ID")]
    pub index_commits: Vec<String>,

    /// Destination directory for the local workspace
    #[arg(default_value = ".")]
    pub destination: String,
}

pub async fn cmd_cc_init(
    ui: &mut Ui,
    command_helper: &CommandHelper,
    args: &CcInitArgs,
) -> Result<(), CommandError> {
    let index_commit_ids = args
        .index_commits
        .iter()
        .map(|hex| {
            CommitId::try_from_hex(hex)
                .filter(|id| id.as_bytes().len() == cc_common::COMMIT_ID_LENGTH)
                .ok_or_else(|| {
                    user_error(format!("--index-commit expects a full commit ID: {hex}"))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;

    if args.workspace_name.trim().is_empty() {
        return Err(user_error(
            "--workspace-name must not be empty or whitespace",
        ));
    }
    if let Some(repo_id) = args.repo_id.as_deref()
        && repo_id.trim().is_empty()
    {
        return Err(user_error("--repo-id must not be empty or whitespace"));
    }
    if let Some(project_id) = args.project_id.as_deref()
        && project_id.trim().is_empty()
    {
        return Err(user_error("--project-id must not be empty or whitespace"));
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

    let dest_path = Path::new(&args.destination);
    let (user_settings, _) = command_helper.settings_for_new_workspace(ui, dest_path)?;
    let signer = signer_from_settings(&user_settings)
        .map_err(|e| user_error(format!("Failed to initialize signature signer: {e:?}")))?;

    let should_create = args.create || args.repo_id.is_none();
    if let Some(existing_repo_id) = args.repo_id.as_deref()
        && remote_workspace_exists(
            &args.server,
            existing_repo_id,
            &args.workspace_name,
            should_create,
        )
        .await?
    {
        return Err(user_error(format!(
            "Workspace '{}' already exists in repository '{existing_repo_id}'. Pass a unique `--workspace-name <name>` (or use `jj cc clone`) instead of reusing an existing workspace name.",
            args.workspace_name
        )));
    }

    let dest_existed = dest_path.exists();
    let first_created_dir = find_first_missing_ancestor(dest_path);
    let dot_jj_existed = dest_path.join(".jj").exists();
    if !dest_existed {
        fs::create_dir_all(dest_path).map_err(|e| {
            user_error(format!(
                "Failed to create destination directory {}: {e}",
                dest_path.display()
            ))
        })?;
    }

    let allocated_ids = Arc::new(Mutex::new((String::new(), String::new())));
    let ids_slot = Arc::clone(&allocated_ids);

    let backend_initializer = move |_settings: &UserSettings, store_path: &Path| {
        let backend = CommitCloudBackend::init_in_project(
            store_path,
            &args.server,
            args.name.as_deref(),
            args.project_id.as_deref(),
            args.repo_id.as_deref(),
            should_create,
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

    let init_res: Result<(), CommandError> = async {
        let workspace_name = WorkspaceNameBuf::from(args.workspace_name.clone());
        let (workspace, repo) = match Workspace::init_with_factories(
            &user_settings,
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
                let (repo_id, project_id) = allocated_ids.lock().unwrap().clone();
                if !repo_id.is_empty() {
                    rollback_failed_init_remote_workspace(
                        &user_settings,
                        &args.server,
                        &repo_id,
                        &project_id,
                        &args.workspace_name,
                        &args.working_copy_type,
                        "forget failed init workspace",
                    )
                    .await;
                }
                return Err(user_error(format!("Failed to initialize workspace: {e:?}")));
            }
        };

        if let Err(err) = cc_lib::cc_index_store::index_commits(&repo, &index_commit_ids)
            .await
            .map_err(|e| user_error(format!("Failed to index --index-commit commits: {e:?}")))
        {
            let repo_id = allocated_ids.lock().unwrap().0.clone();
            if !repo_id.is_empty() {
                let latest_rollback_repo = workspace
                    .repo_loader()
                    .load_at_head(None)
                    .await
                    .unwrap_or(repo);
                rollback_created_remote_workspace(
                    &latest_rollback_repo,
                    &args.server,
                    &repo_id,
                    &args.workspace_name,
                    &args.working_copy_type,
                    "forget failed init workspace",
                )
                .await;
            }
            return Err(err);
        }
        Ok(())
    }
    .await;

    if let Err(err) = init_res {
        cleanup_failed_workspace_dirs(
            dest_path,
            dest_existed,
            dot_jj_existed,
            first_created_dir.as_deref(),
        );
        return Err(err);
    }

    let (repo_id, project_id) = allocated_ids.lock().unwrap().clone();
    println!(
        "Initialized remote Commit Cloud repository (repo_id: {repo_id}, project_id: {project_id})"
    );
    Ok(())
}
