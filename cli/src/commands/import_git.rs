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

//! `jj cc import-git` command wrapping [`GitImporter`] to stream local Git history to Commit Cloud.

use std::path::PathBuf;

use cc_lib::git_bridge::{GitImportOptions, GitImporter};
use cc_lib::util::run_on_tokio;
use clap::Parser;
use jj_cli::command_error::{CommandError, user_error};

#[derive(Parser, Clone, Debug)]
pub struct ImportGitArgs {
    /// Path to the local Git repository (working tree root or `.git` directory)
    #[arg(long)]
    pub git_dir: PathBuf,

    /// Remote Commit Cloud gRPC server URL (e.g. http://127.0.0.1:8080)
    #[arg(long)]
    pub server: String,

    /// Optional shared project ID owning commits, trees, symlinks, and blobs
    #[arg(long)]
    pub project_id: Option<String>,

    /// Optional target repository ID (allocates a new repository when omitted)
    #[arg(long)]
    pub repo_id: Option<String>,

    /// Register the target `--repo-id` if it does not exist yet
    #[arg(long)]
    pub create: bool,
}

pub async fn cmd_import_git(args: &ImportGitArgs) -> Result<(), CommandError> {
    if args.repo_id.as_deref().is_some_and(|s| s.trim().is_empty()) {
        return Err(user_error("--repo-id must not be empty or whitespace"));
    }
    if args
        .project_id
        .as_deref()
        .is_some_and(|s| s.trim().is_empty())
    {
        return Err(user_error("--project-id must not be empty or whitespace"));
    }

    let opts = GitImportOptions {
        git_dir: args.git_dir.clone(),
        server_url: args.server.clone(),
        project_id: args.project_id.clone(),
        repo_id: args.repo_id.clone(),
        create_repo: args.create || args.repo_id.is_none(),
    };

    let report = run_on_tokio(async move { GitImporter::new(opts).run().await })
        .await
        .map_err(|e| user_error(format!("Git import failed: {e}")))?;

    println!(
        "Imported Git repository into Commit Cloud (repo_id: {}, project_id: {})",
        report.repo_id, report.project_id
    );
    println!(
        "  Commits: {}, Trees: {}, Blobs: {}, Symlinks: {}, Bookmarks: {}",
        report.commits_imported,
        report.trees_imported,
        report.blobs_imported,
        report.symlinks_imported,
        report.bookmarks_imported
    );
    println!(
        "Clone with:\n  jj cc clone --server {} {} ./checkout",
        args.server, report.repo_id
    );
    Ok(())
}
