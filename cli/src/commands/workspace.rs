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

//! `jj cc workspace` subcommands for inspecting and removing cloud-tracked workspaces.
//!
//! # Cross-Process and RPC Boundaries
//! Queries `WorkspaceService` (`ListWorkspaces`, `GetWorkspace`, `CheckWorkingCopyChanges`,
//! `DeleteWorkspace`) using the repository configuration in `.jj/repo/store/config.toml`.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use cc_common::workspace::{
    CheckWorkingCopyChangesRequest, DeleteWorkspaceRequest, GetWorkspaceRequest,
    ListWorkspacesRequest,
};
use cc_lib::StoreFactoriesExt as _;
use cc_lib::cc_working_copy::CommitCloudWorkingCopy;
use cc_lib::util::{CommitCloudConfig, connect_workspace_client, run_on_tokio};
use clap::{Parser, Subcommand};
use jj_cli::cli_util::{CommandHelper, find_workspace_dir};
use jj_cli::command_error::{CommandError, user_error};
use jj_cli::ui::Ui;
use jj_lib::default_backend_factories::default_backend_factories;
use jj_lib::local_working_copy::{LocalWorkingCopy, LocalWorkingCopyFactory};
use jj_lib::repo::StoreLoadError;
use jj_lib::settings::UserSettings;
use jj_lib::workspace::{
    DefaultWorkspaceLoaderFactory, WorkingCopyFactories, WorkspaceLoadError,
    WorkspaceLoaderFactory as _,
};

#[derive(Parser, Clone, Debug)]
pub struct WorkspaceArgs {
    #[command(subcommand)]
    pub subcommand: WorkspaceSubcommands,
}

#[derive(Subcommand, Clone, Debug)]
pub enum WorkspaceSubcommands {
    /// List cloud-tracked workspaces registered in this repository
    List {
        /// Optional workspace directory (defaults to current directory)
        #[arg(long)]
        repo_dir: Option<PathBuf>,
    },
    /// Show cloud-tracked checkout and working-copy dirty status for a workspace
    Status {
        /// Workspace name to inspect (defaults to active workspace name)
        #[arg(long)]
        workspace_name: Option<String>,
        /// Optional workspace directory (defaults to current directory)
        #[arg(long)]
        repo_dir: Option<PathBuf>,
    },
    /// Remove a cloud-tracked workspace metadata entry from `WorkspaceService`
    /// (does not remove the workspace from repository history; run `jj workspace forget <name>` to forget it from views)
    Delete {
        /// Workspace name to remove
        workspace_name: String,
        /// Optional workspace directory (defaults to current directory)
        #[arg(long)]
        repo_dir: Option<PathBuf>,
    },
}

fn resolve_repo_dir(
    command_helper: &CommandHelper,
    repo_dir: &Path,
) -> Result<PathBuf, CommandError> {
    let joined = command_helper.cwd().join(repo_dir);
    let canonical = std::fs::canonicalize(&joined).map_err(|e| {
        user_error(format!(
            "Failed to access repository directory '{}': {e}",
            repo_dir.display()
        ))
    })?;
    Ok(find_workspace_dir(&canonical).to_path_buf())
}

fn load_repo_config(
    command_helper: &CommandHelper,
    ws_root: Option<&Path>,
) -> Result<CommitCloudConfig, CommandError> {
    let base = match ws_root {
        Some(p) => p.to_path_buf(),
        None => command_helper
            .workspace_loader()
            .map(|w| w.repo_path().to_path_buf())
            .unwrap_or_else(|_| command_helper.cwd().to_path_buf()),
    };
    CommitCloudConfig::load_from_store(&base)
        .map_err(|e| user_error(format!("Failed to load Commit Cloud config: {e}")))
}

fn resolve_target_settings(
    ui: &Ui,
    command_helper: &CommandHelper,
    ws_root: Option<&Path>,
) -> Result<Option<UserSettings>, CommandError> {
    match ws_root {
        Some(ws_root) => command_helper
            .settings_for_new_workspace(ui, ws_root)
            .map(|(s, _)| Some(s)),
        None => Ok(None),
    }
}

/// Reads the local workspace name from `.jj/working_copy` metadata without invoking
/// `CommitCloudWorkingCopyFactory` (so `jj cc workspace status` works inside non-default
/// workspaces without touching remote sync).
fn load_local_workspace_name(
    command_helper: &CommandHelper,
    ws_root: Option<&Path>,
    settings: &UserSettings,
) -> Result<String, CommandError> {
    let target_root = match ws_root {
        Some(p) => p.to_path_buf(),
        None => command_helper
            .workspace_loader()?
            .workspace_root()
            .to_path_buf(),
    };
    let mut store_factories = default_backend_factories();
    store_factories.add_commit_cloud();
    let mut wc_factories = WorkingCopyFactories::new();
    wc_factories.insert(
        LocalWorkingCopy::NAME.to_owned(),
        Box::new(LocalWorkingCopyFactory {}),
    );
    wc_factories.insert(
        CommitCloudWorkingCopy::name().to_owned(),
        Box::new(LocalWorkingCopyFactory {}),
    );
    let load_res = DefaultWorkspaceLoaderFactory
        .create(&target_root)
        .and_then(|loader| loader.load(settings, &store_factories, &wc_factories));
    match load_res {
        Ok(ws) => Ok(ws.workspace_name().as_str().to_string()),
        Err(WorkspaceLoadError::StoreLoadError(StoreLoadError::UnsupportedType { .. })) => {
            let ws = command_helper.load_workspace_at(&target_root, settings)?;
            Ok(ws.workspace_name().as_str().to_string())
        }
        Err(err) => Err(user_error(format!(
            "Failed to read local workspace metadata at {} (pass `--workspace-name <name>` to specify a workspace): {err}",
            target_root.display()
        ))),
    }
}

const MAX_LIST_WORKSPACES_PAGES: usize = 10_000;

async fn list_all_workspaces(
    server_url: String,
    repo_id: String,
) -> Result<Vec<cc_common::workspace::WorkspaceState>, Box<dyn std::error::Error + Send + Sync>> {
    list_all_workspaces_with_max_pages(server_url, repo_id, MAX_LIST_WORKSPACES_PAGES).await
}

async fn list_all_workspaces_with_max_pages(
    server_url: String,
    repo_id: String,
    max_pages: usize,
) -> Result<Vec<cc_common::workspace::WorkspaceState>, Box<dyn std::error::Error + Send + Sync>> {
    run_on_tokio(async move {
        let mut client = connect_workspace_client(server_url).await?;
        let mut all_workspaces = Vec::new();
        let mut page_token = String::new();
        let mut seen_page_tokens: HashSet<String> = HashSet::new();
        let mut pages_fetched: usize = 0;
        loop {
            let res = client
                .list_workspaces(ListWorkspacesRequest {
                    repo_id: repo_id.clone(),
                    page_size: 0,
                    page_token,
                })
                .await?
                .into_inner();
            pages_fetched += 1;
            if res.workspaces.is_empty() && !res.next_page_token.is_empty() {
                return Err(format!(
                    "ListWorkspaces returned empty page with non-empty next_page_token for repo {repo_id}"
                )
                .into());
            }
            all_workspaces.extend(res.workspaces);
            if res.next_page_token.is_empty() {
                break;
            }
            if pages_fetched >= max_pages {
                return Err(format!(
                    "ListWorkspaces exceeded maximum of {max_pages} pages for repo {repo_id}"
                )
                .into());
            }
            if !seen_page_tokens.insert(res.next_page_token.clone()) {
                return Err(format!(
                    "ListWorkspaces pagination cursor repeated or failed to advance for repo {repo_id}"
                )
                .into());
            }
            page_token = res.next_page_token;
        }
        Ok(all_workspaces)
    })
    .await
}

fn validate_workspace_name(name: &str, label: &str) -> Result<String, CommandError> {
    if name.trim().is_empty() {
        return Err(user_error(format!(
            "{label} must not be empty or whitespace"
        )));
    }
    Ok(name.to_string())
}

pub async fn cmd_cc_workspace(
    ui: &mut Ui,
    command_helper: &CommandHelper,
    args: &WorkspaceArgs,
) -> Result<(), CommandError> {
    match &args.subcommand {
        WorkspaceSubcommands::List { repo_dir } => {
            let ws_root = repo_dir
                .as_deref()
                .map(|dir| resolve_repo_dir(command_helper, dir))
                .transpose()?;
            let cfg = load_repo_config(command_helper, ws_root.as_deref())?;
            let workspaces = list_all_workspaces(cfg.server_url, cfg.repo_id)
                .await
                .map_err(|e| user_error(format!("Failed to list cloud workspaces: {e}")))?;

            if workspaces.is_empty() {
                println!("No cloud-tracked workspaces registered.");
            } else {
                for ws in workspaces {
                    let conflict_suffix = if ws.tree_ids.len() > 1 {
                        " (conflicted)"
                    } else {
                        ""
                    };
                    if ws.user.is_empty() {
                        println!(
                            "{}: commit={} op={} tree={}{}",
                            ws.workspace_name,
                            short_hex(&ws.commit_id, 12),
                            short_hex(&ws.operation_id, 12),
                            short_hex(primary_tree_id(&ws), 12),
                            conflict_suffix,
                        );
                    } else {
                        println!(
                            "{} ({}): commit={} op={} tree={}{}",
                            ws.workspace_name,
                            ws.user,
                            short_hex(&ws.commit_id, 12),
                            short_hex(&ws.operation_id, 12),
                            short_hex(primary_tree_id(&ws), 12),
                            conflict_suffix,
                        );
                    }
                }
            }
            Ok(())
        }
        WorkspaceSubcommands::Status {
            workspace_name,
            repo_dir,
        } => {
            let ws_root = repo_dir
                .as_deref()
                .map(|dir| resolve_repo_dir(command_helper, dir))
                .transpose()?;
            let cfg = load_repo_config(command_helper, ws_root.as_deref())?;
            let resolved_ws_name = match workspace_name {
                Some(name) => validate_workspace_name(name, "--workspace-name")?,
                None => {
                    let target_settings =
                        resolve_target_settings(ui, command_helper, ws_root.as_deref())?;
                    let effective_settings = target_settings
                        .as_ref()
                        .unwrap_or_else(|| command_helper.settings());
                    load_local_workspace_name(
                        command_helper,
                        ws_root.as_deref(),
                        effective_settings,
                    )?
                }
            };
            let ws_name = resolved_ws_name.clone();
            let server_url = cfg.server_url;
            let repo_id = cfg.repo_id;

            let (ws_opt, changes_opt) = run_on_tokio(async move {
                let mut client = connect_workspace_client(server_url).await?;
                let ws = client
                    .get_workspace(GetWorkspaceRequest {
                        repo_id: repo_id.clone(),
                        workspace_name: ws_name.clone(),
                    })
                    .await?
                    .into_inner()
                    .workspace;
                let changes = if ws.is_some() {
                    client
                        .check_working_copy_changes(CheckWorkingCopyChangesRequest {
                            repo_id,
                            workspace_name: ws_name,
                        })
                        .await
                        .map(|r| Some(r.into_inner()))?
                } else {
                    None
                };
                Ok((ws, changes))
            })
            .await
            .map_err(|e| user_error(format!("Failed to query cloud workspace status: {e}")))?;

            let Some(ws) = ws_opt else {
                return Err(user_error(format!(
                    "Cloud workspace '{resolved_ws_name}' is not registered"
                )));
            };
            println!("Workspace: {}", ws.workspace_name);
            if !ws.user.is_empty() {
                println!("  User:      {}", ws.user);
            }
            println!("  Commit:    {}", hex::encode(&ws.commit_id));
            println!("  Operation: {}", hex::encode(&ws.operation_id));
            if ws.tree_ids.len() > 1 {
                let terms: Vec<String> = ws.tree_ids.iter().map(hex::encode).collect();
                println!("  Trees:     {} (conflicted)", terms.join(", "));
            } else {
                println!("  Tree:      {}", hex::encode(primary_tree_id(&ws)));
            }
            println!(
                "  Sparse:    {}",
                format_sparse_patterns(&ws.sparse_patterns)
            );
            if let Some(ch) = changes_opt {
                println!("  Modified:  {}", ch.has_changes);
            }
            Ok(())
        }
        WorkspaceSubcommands::Delete {
            workspace_name,
            repo_dir,
        } => {
            let ws_name = validate_workspace_name(workspace_name, "Workspace name")?;
            let ws_root = repo_dir
                .as_deref()
                .map(|dir| resolve_repo_dir(command_helper, dir))
                .transpose()?;
            let cfg = load_repo_config(command_helper, ws_root.as_deref())?;
            let ws_name_rpc = ws_name.clone();
            let server_url = cfg.server_url;
            let repo_id = cfg.repo_id;

            let deleted = run_on_tokio(async move {
                let mut client = connect_workspace_client(server_url).await?;
                let res = client
                    .delete_workspace(DeleteWorkspaceRequest {
                        repo_id,
                        workspace_name: ws_name_rpc,
                    })
                    .await?
                    .into_inner();
                Ok(res.existed)
            })
            .await
            .map_err(|e| user_error(format!("Failed to delete cloud workspace: {e}")))?;

            if deleted {
                println!(
                    "Deleted cloud workspace '{ws_name}' from WorkspaceService.\n\
                     Note: this does not remove '{ws_name}' from repository history; run `jj workspace forget {ws_name}` to remove it from `jj workspace list` and allow reusing the name."
                );
            } else {
                println!("Cloud workspace '{ws_name}' was not present in WorkspaceService");
            }
            Ok(())
        }
    }
}

fn format_sparse_patterns(patterns: &[String]) -> String {
    if patterns.is_empty() {
        "(none)".to_string()
    } else if patterns.len() == 1 && patterns[0].is_empty() {
        "(all)".to_string()
    } else {
        patterns
            .iter()
            .map(|p| if p.is_empty() { "(all)" } else { p.as_str() })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[allow(deprecated)]
fn primary_tree_id(ws: &cc_common::workspace::WorkspaceState) -> &[u8] {
    ws.tree_ids
        .first()
        .map(Vec::as_slice)
        .unwrap_or(&ws.tree_id)
}

fn short_hex(bytes: &[u8], max_chars: usize) -> String {
    let full = hex::encode(bytes);
    if full.len() > max_chars {
        full[..max_chars].to_string()
    } else {
        full
    }
}

#[cfg(test)]
mod tests {
    use cc_common::workspace::workspace_service_server::{
        WorkspaceService, WorkspaceServiceServer,
    };
    use cc_common::workspace::{
        CheckWorkingCopyChangesResponse, DeleteWorkspaceResponse, GetWorkspaceResponse,
        ListWorkspacesResponse, UpdateWorkspaceRequest, UpdateWorkspaceResponse,
    };
    use tonic::{Request, Response, Status};

    use super::*;

    struct EmptyPageWorkspaceServer;

    #[tonic::async_trait]
    impl WorkspaceService for EmptyPageWorkspaceServer {
        async fn get_workspace(
            &self,
            _: Request<GetWorkspaceRequest>,
        ) -> Result<Response<GetWorkspaceResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }
        async fn update_workspace(
            &self,
            _: Request<UpdateWorkspaceRequest>,
        ) -> Result<Response<UpdateWorkspaceResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }
        async fn list_workspaces(
            &self,
            _: Request<ListWorkspacesRequest>,
        ) -> Result<Response<ListWorkspacesResponse>, Status> {
            Ok(Response::new(ListWorkspacesResponse {
                workspaces: Vec::new(),
                next_page_token: "next-cursor".to_string(),
            }))
        }
        async fn check_working_copy_changes(
            &self,
            _: Request<CheckWorkingCopyChangesRequest>,
        ) -> Result<Response<CheckWorkingCopyChangesResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }
        async fn delete_workspace(
            &self,
            _: Request<DeleteWorkspaceRequest>,
        ) -> Result<Response<DeleteWorkspaceResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }
    }

    #[test]
    fn format_sparse_patterns_handles_empty_all_and_paths() {
        assert_eq!(format_sparse_patterns(&[]), "(none)");
        assert_eq!(format_sparse_patterns(&[String::new()]), "(all)");
        assert_eq!(
            format_sparse_patterns(&["src".to_string(), "docs".to_string()]),
            "src, docs"
        );
    }

    #[test]
    fn validate_workspace_name_rejects_empty_or_whitespace() {
        assert_eq!(
            validate_workspace_name("ws-1", "--workspace-name").unwrap(),
            "ws-1"
        );
        assert!(validate_workspace_name("", "--workspace-name").is_err());
        assert!(validate_workspace_name("   \t\n", "Workspace name").is_err());
    }

    #[tokio::test]
    async fn list_all_workspaces_rejects_empty_page_with_next_page_token() {
        let url = run_on_tokio(async move {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let addr = listener.local_addr()?;
            let incoming =
                tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
                    .map_err(|e| format!("incoming error: {e}"))?;
            tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(WorkspaceServiceServer::new(EmptyPageWorkspaceServer))
                    .serve_with_incoming(incoming),
            );
            Ok(format!("http://{addr}"))
        })
        .await
        .unwrap();

        let err = list_all_workspaces(url, "repo-1".to_string())
            .await
            .expect_err("empty page with non-empty next_page_token must fail");
        assert!(
            err.to_string().contains("empty page"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn list_all_workspaces_enforces_max_page_cap() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct InfinitePagesWorkspaceServer {
            pages_served: Arc<AtomicUsize>,
        }

        #[tonic::async_trait]
        impl WorkspaceService for InfinitePagesWorkspaceServer {
            async fn get_workspace(
                &self,
                _: Request<GetWorkspaceRequest>,
            ) -> Result<Response<GetWorkspaceResponse>, Status> {
                Err(Status::unimplemented("unused"))
            }
            async fn update_workspace(
                &self,
                _: Request<UpdateWorkspaceRequest>,
            ) -> Result<Response<UpdateWorkspaceResponse>, Status> {
                Err(Status::unimplemented("unused"))
            }
            #[allow(deprecated)]
            async fn list_workspaces(
                &self,
                _: Request<ListWorkspacesRequest>,
            ) -> Result<Response<ListWorkspacesResponse>, Status> {
                let page = self.pages_served.fetch_add(1, Ordering::SeqCst) + 1;
                Ok(Response::new(ListWorkspacesResponse {
                    workspaces: vec![cc_common::workspace::WorkspaceState {
                        repo_id: "repo-cap".to_string(),
                        user: "alice@example.com".to_string(),
                        workspace_name: format!("ws-{page}"),
                        commit_id: Vec::new(),
                        operation_id: Vec::new(),
                        tree_id: Vec::new(),
                        sparse_patterns: Vec::new(),
                        tree_ids: Vec::new(),
                        conflict_labels: Vec::new(),
                        sparse_patterns_set: false,
                    }],
                    next_page_token: format!("cursor-{page}"),
                }))
            }
            async fn check_working_copy_changes(
                &self,
                _: Request<CheckWorkingCopyChangesRequest>,
            ) -> Result<Response<CheckWorkingCopyChangesResponse>, Status> {
                Err(Status::unimplemented("unused"))
            }
            async fn delete_workspace(
                &self,
                _: Request<DeleteWorkspaceRequest>,
            ) -> Result<Response<DeleteWorkspaceResponse>, Status> {
                Err(Status::unimplemented("unused"))
            }
        }

        let pages_served = Arc::new(AtomicUsize::new(0));
        let pages_bg = Arc::clone(&pages_served);
        let url = run_on_tokio(async move {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let addr = listener.local_addr()?;
            let incoming =
                tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
                    .map_err(|e| format!("incoming error: {e}"))?;
            tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(WorkspaceServiceServer::new(InfinitePagesWorkspaceServer {
                        pages_served: pages_bg,
                    }))
                    .serve_with_incoming(incoming),
            );
            Ok(format!("http://{addr}"))
        })
        .await
        .unwrap();

        let err = list_all_workspaces_with_max_pages(url, "repo-cap".to_string(), 3)
            .await
            .expect_err("unbounded distinct pagination tokens must hit max page cap");
        assert!(
            err.to_string().contains("exceeded maximum of 3 pages"),
            "unexpected error: {err}"
        );
        assert_eq!(pages_served.load(Ordering::SeqCst), 3);
    }
}
