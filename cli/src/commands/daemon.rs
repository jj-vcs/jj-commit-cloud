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

use cc_lib::util::CommitCloudConfig;
use clap::Parser;
use jj_cli::command_error::{CommandError, user_error};

#[derive(Parser, Clone, Debug)]
pub struct DaemonArgs {
    /// Enable daemon usage for this workspace
    #[arg(long, conflicts_with = "disable", conflicts_with = "status")]
    pub enable: bool,

    /// Disable daemon usage for this workspace (fallback to direct gRPC)
    #[arg(long, conflicts_with = "enable", conflicts_with = "status")]
    pub disable: bool,

    /// Check daemon status and configuration
    #[arg(long, conflicts_with = "enable", conflicts_with = "disable")]
    pub status: bool,
}

pub async fn cmd_daemon(args: &DaemonArgs) -> Result<(), CommandError> {
    let cwd = std::env::current_dir()
        .map_err(|e| user_error(format!("Failed to get current directory: {e}")))?;
    let mut config = CommitCloudConfig::load_from_store(&cwd)
        .map_err(|e| user_error(format!("Failed to load Commit Cloud config: {e}")))?;

    if args.enable {
        config.use_daemon = true;
        config
            .save_to_store(&cwd)
            .map_err(|e| user_error(format!("Failed to save config: {e}")))?;
        println!("Commit Cloud daemon mode enabled.");
    } else if args.disable {
        config.use_daemon = false;
        config
            .save_to_store(&cwd)
            .map_err(|e| user_error(format!("Failed to save config: {e}")))?;
        println!("Commit Cloud daemon mode disabled (using direct gRPC).");
    } else {
        println!("Commit Cloud Daemon Status:");
        println!("  Enabled: {}", config.use_daemon);
        println!("  Server URL: {}", config.server_url);
        println!("  Repo ID: {}", config.repo_id);
        if let Some(sock) = &config.daemon_socket {
            println!("  Custom Socket: {}", sock);
        }
    }

    Ok(())
}
