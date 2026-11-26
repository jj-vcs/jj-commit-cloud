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

use cc_lib::git_importer::GitImporter;
use clap::Parser;
use jj_cli::command_error::{CommandError, user_error};
use std::path::PathBuf;

#[derive(Parser, Clone, Debug)]
pub struct ImportGitArgs {
    /// Path to the existing .git directory (or project root containing .git)
    #[arg(long)]
    pub git_dir: PathBuf,

    /// Target Project ID in Commit Cloud (owns shared commit/tree/file storage)
    #[arg(long)]
    pub project_id: String,

    /// Target Repository ID in Commit Cloud (owns operation log)
    #[arg(long)]
    pub repo_id: String,

    /// Remote Commit Cloud server URL (e.g. http://localhost:8080)
    #[arg(long)]
    pub server: String,
}

pub async fn cmd_import_git(args: &ImportGitArgs) -> Result<(), CommandError> {
    let work_dir = if args.git_dir.file_name() == Some(std::ffi::OsStr::new(".git")) {
        args.git_dir.parent().unwrap_or(&args.git_dir).to_path_buf()
    } else {
        args.git_dir.clone()
    };

    println!(
        "Opening Git repository at {:?} for Target Project ID '{}' / Repo ID '{}'...",
        work_dir, args.project_id, args.repo_id
    );
    let importer = GitImporter::new(
        work_dir,
        args.project_id.clone(),
        args.repo_id.clone(),
        args.server.clone(),
    );

    let repo_id = cc_lib::util::run_async(move || async move { importer.run().await })
        .map_err(|e| user_error(format!("Git import failed: {e}")))?;

    println!("\n✅ Git repository imported successfully!");
    println!("Cloud Repository ID: {}", repo_id);
    println!("\nTo connect a workspace to this cloud repository, run:");
    println!(
        "jj cc init --server {} --project-id {} --repo-id {} .",
        args.server, args.project_id, repo_id
    );

    Ok(())
}
