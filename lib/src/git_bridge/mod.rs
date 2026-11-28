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

//! Bidirectional Git history bridge (`GitImporter` and `GitExporter`) preserving 1:1
//! 20-byte Git SHA-1 commit, tree, symlink, and blob digests.
//!
//! # Authority and Crash-Safety Ordering
//! - [`GitImporter`] uploads file blobs (`WriteFileStream` in 64 KiB chunks) and symlinks
//!   (`WriteSymlink`) first, then bottom-up directory trees (`WriteTree`), then commits
//!   (`WriteCommit`) in strict post-order topological order (parents before children), and finally
//!   publishes the updated `View`, `Operation`, and `OpHeads` in one atomic `PublishOperation`
//!   call on top of the current `View`; failing to read that `View` aborts the import. Like
//!   `jj git import`, it reads only `HEAD`, `refs/heads/*`, `refs/remotes/*`, and `refs/tags/*`,
//!   and it never writes to the source repository.
//! - [`GitExporter`] verifies every blob, symlink, tree, and commit SHA-1 digest before writing
//!   into the target `.git/objects/` store and updating `refs/heads/*` and `HEAD` through one
//!   atomic gix ref transaction (lock files, then rename). Newly created non-bare repos check out
//!   `HEAD` and roll back on failure; pre-existing repos refuse updating any branch checked out in
//!   the main or any linked worktree. Invalid, conflicted, or root-target bookmarks are skipped.
//!
//! # Concurrency and Merge Invariants
//! File and symlink uploads pipeline up to `CONCURRENT_UPLOADS` (16) streams over the
//! shared pooled HTTP/2 channel, slicing each in-flight `Bytes` blob lazily into 64 KiB frames.
//! Imported commits without an existing `change-id` header get the synthetic `ChangeId` that jj's
//! `GitBackend` derives from the Git SHA-1, and preserve `raw_git_commit` bytes so round-trips
//! back to Git reproduce non-standard Git headers, encodings, and signatures bit-for-bit.

mod export;
mod git_repo;
mod import;
mod objects;
mod publish;
mod refs;

#[cfg(test)]
mod tests;

use std::path::PathBuf;

pub use export::GitExporter;
pub use import::GitImporter;

const CONCURRENT_UPLOADS: usize = 16;

const EMPTY_STRING_PLACEHOLDER: &str = "JJ_EMPTY_STRING";

/// Maximum total entries accumulated when assembling a paginated `ReadTree` response,
/// matching `CommitCloudBackend` and `jj-cc-daemon`.
const MAX_TREE_ENTRIES: usize = 500_000;

/// Maximum total bytes buffered when assembling a streamed `ReadFile` response,
/// matching `CommitCloudBackend` and the server's 512 MiB `MAX_STREAM_FILE_BYTES` limit.
const MAX_READ_FILE_BYTES: usize = 512 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct GitImportOptions {
    pub git_dir: PathBuf,
    pub server_url: String,
    pub project_id: Option<String>,
    pub repo_id: Option<String>,
    pub create_repo: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GitImportReport {
    pub repo_id: String,
    /// Project namespace used for object writes. When `--project-id` is omitted while importing
    /// into an existing repository, this defaults to `repo_id` (the server resolves the
    /// repository's bound project namespace on write).
    pub project_id: String,
    pub commits_imported: usize,
    pub trees_imported: usize,
    pub blobs_imported: usize,
    pub symlinks_imported: usize,
    pub bookmarks_imported: usize,
}

#[derive(Clone, Debug)]
pub struct GitExportOptions {
    pub git_dir: PathBuf,
    pub server_url: String,
    pub repo_id: String,
    pub project_id: Option<String>,
    pub bare: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GitExportReport {
    pub commits_exported: usize,
    pub trees_exported: usize,
    pub blobs_exported: usize,
    pub bookmarks_exported: usize,
    /// Bookmarks left out because their names are not valid Git branch names or they target the
    /// virtual root commit.
    pub skipped_bookmarks: Vec<String>,
    /// Bookmarks left out because they have unresolved conflicts in the `View`.
    pub skipped_conflicted_bookmarks: Vec<String>,
}

fn validate_optional_id(
    value: Option<&str>,
    flag: &str,
) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
    match value {
        None => Ok(None),
        Some(s) if s.trim().is_empty() => {
            Err(format!("{flag} must not be empty or whitespace").into())
        }
        Some(s) => Ok(Some(s.to_string())),
    }
}

fn validate_required_id(
    value: &str,
    flag: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if value.trim().is_empty() {
        return Err(format!("{flag} must not be empty or whitespace").into());
    }
    Ok(value.to_string())
}
