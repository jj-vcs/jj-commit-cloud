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

use std::collections::HashMap;
use std::process::ExitCode;

use cc_lib::StoreFactoriesExt;
use cc_lib::cc_working_copy::{CommitCloudWorkingCopy, CommitCloudWorkingCopyFactory};
use jj_cli::cli_util::{CliRunner, CommandHelper};
use jj_cli::command_error::CommandError;
use jj_lib::config::{ConfigLayer, ConfigSource};
use jj_lib::repo::StoreFactories;
use jj_lib::workspace::WorkingCopyFactories;

mod commands;

#[derive(clap::Subcommand, Clone, Debug)]
enum CustomCommands {
    /// Manage Commit Cloud repositories and workspaces
    Cc {
        #[command(subcommand)]
        subcommand: CcCommands,
    },
}

#[derive(clap::Subcommand, Clone, Debug)]
enum CcCommands {
    /// Initialize a remote Commit Cloud repository and local working copy
    Init(commands::init::CcInitArgs),
    /// Clone an existing remote Commit Cloud repository and check out a revision
    Clone(commands::clone::CcCloneArgs),
    /// Import commits, trees, blobs, symlinks, and branches from a Git repository
    ImportGit(commands::import_git::ImportGitArgs),
    /// Export commits, trees, blobs, symlinks, and bookmarks to a Git repository
    ExportGit(commands::export_git::ExportGitArgs),
    /// Inspect or manage cloud-tracked workspace states on the server
    Workspace(commands::workspace::WorkspaceArgs),
    /// Manage the local two-tier caching `jj-cc-daemon` gRPC proxy
    Daemon(commands::daemon::DaemonArgs),
}

fn create_store_factories() -> StoreFactories {
    let mut factories = StoreFactories::empty();
    factories.add_commit_cloud();
    factories
}

fn create_working_copy_factories() -> WorkingCopyFactories {
    let mut factories = HashMap::new();
    factories.insert(
        CommitCloudWorkingCopy::name().to_string(),
        Box::new(CommitCloudWorkingCopyFactory::new())
            as Box<dyn jj_lib::working_copy::WorkingCopyFactory>,
    );
    factories
}

fn default_commit_cloud_config() -> ConfigLayer {
    ConfigLayer::parse(
        ConfigSource::Default,
        "snapshot.max-new-file-size = \"64MiB\"\n",
    )
    .expect("default Commit Cloud config layer should parse")
}

async fn run_custom_command(
    ui: &mut jj_cli::ui::Ui,
    command_helper: &CommandHelper,
    command: CustomCommands,
) -> Result<(), CommandError> {
    match command {
        CustomCommands::Cc { subcommand } => match subcommand {
            CcCommands::Init(args) => commands::init::cmd_cc_init(ui, command_helper, &args).await,
            CcCommands::Clone(args) => {
                commands::clone::cmd_cc_clone(ui, command_helper, &args).await
            }
            CcCommands::ImportGit(args) => commands::import_git::cmd_import_git(&args).await,
            CcCommands::ExportGit(args) => {
                commands::export_git::cmd_export_git(command_helper, &args).await
            }
            CcCommands::Workspace(args) => {
                commands::workspace::cmd_cc_workspace(ui, command_helper, &args).await
            }
            CcCommands::Daemon(args) => commands::daemon::cmd_cc_daemon(&args).await,
        },
    }
}

fn main() -> ExitCode {
    cc_lib::cc_index_store::register_cli_commit_hints(std::env::args_os());
    CliRunner::init()
        .add_extra_config(default_commit_cloud_config())
        .add_store_factories(create_store_factories())
        .add_working_copy_factories(create_working_copy_factories())
        .add_subcommand(run_custom_command)
        .run()
        .into()
}
