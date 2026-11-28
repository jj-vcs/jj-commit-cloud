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

//! Local Git repository discovery, reftable compatibility, and target repository
//! lifecycle management for [`super::GitImporter`] and [`super::GitExporter`].
//!
//! # Authority and Crash-Safety Ordering
//! `ReftableObjectsDir` creates an isolated temporary bare directory referencing
//! the source object database via `objects/info/alternates` and never mutates the
//! source repository. `TargetGitRepoCleanup` tracks newly initialized export
//! targets and removes created directories on drop unless committed after ref
//! updates and optional worktree checkout succeed.
//!
//! # Cross-Process and RPC Boundaries
//! All `git` subprocesses run through `git_command`, which clears ambient
//! `GIT_*` environment variables so host repository context cannot leak into
//! reftable checks, ref enumeration, or worktree checkouts.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Component, Path, PathBuf};

pub(super) fn git_command() -> tokio::process::Command {
    let mut cmd = tokio::process::Command::new("git");
    for var in [
        "JJ_CC_AUTH_TOKEN",
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_COMMON_DIR",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_NAMESPACE",
        "GIT_SHALLOW_FILE",
        "GIT_REPLACE_REF_BASE",
        "GIT_NO_REPLACE_OBJECTS",
        "GIT_CONFIG_PARAMETERS",
        "GIT_CONFIG_COUNT",
        "GIT_CONFIG_SYSTEM",
    ] {
        cmd.env_remove(var);
    }
    // Explicitly disable global and system Git config so `git` never falls back to
    // `~/.gitconfig`, `$XDG_CONFIG_HOME/git/config`, or `/etc/gitconfig` (which could inject
    // `core.hooksPath`, `core.fsmonitor`, or smudge/clean filters).
    cmd.env(
        "GIT_CONFIG_GLOBAL",
        if cfg!(windows) { "NUL" } else { "/dev/null" },
    );
    cmd.env("GIT_CONFIG_NOSYSTEM", "1");
    cmd
}

pub(super) async fn verify_reftable_sha1(
    dot_git: &Path,
    work_dir: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let canonical_dot_git = fs::canonicalize(dot_git).map_err(|e| {
        format!(
            "Failed to canonicalize Git directory {}: {e}",
            dot_git.display()
        )
    })?;
    let cwd = if work_dir.is_dir() {
        work_dir
    } else {
        canonical_dot_git.as_path()
    };
    let out = git_command()
        .arg("--git-dir")
        .arg(&canonical_dot_git)
        .args(["rev-parse", "--show-object-format"])
        .current_dir(cwd)
        .output()
        .await
        .map_err(|e| {
            format!(
                "Failed to run `git rev-parse --show-object-format` for reftable repository {}: {e}",
                canonical_dot_git.display()
            )
        })?;
    if !out.status.success() {
        return Err(format!(
            "`git rev-parse --show-object-format` failed for {} ({}): {}",
            canonical_dot_git.display(),
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        )
        .into());
    }
    let format = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if format != "sha1" {
        return Err(format!(
            "Unsupported Git object hash format {format:?} at {}: Commit Cloud supports SHA-1 Git repositories only",
            canonical_dot_git.display()
        )
        .into());
    }
    Ok(())
}

pub(super) fn resolve_dot_git_dir(git_dir: &Path) -> PathBuf {
    let dot_git = git_dir.join(".git");
    let candidate = if dot_git.exists() {
        dot_git
    } else if git_dir.is_file()
        || git_dir.file_name() == Some(std::ffi::OsStr::new(".git"))
        || git_dir.join("HEAD").is_file()
    {
        git_dir.to_path_buf()
    } else {
        dot_git
    };
    resolve_gitdir_file(candidate)
}

/// Resolves a `.git` pointer file (`gitdir: <path>`) used by linked Git worktrees and submodules.
fn resolve_gitdir_file(mut path: PathBuf) -> PathBuf {
    for _ in 0..8 {
        if !path.is_file() {
            break;
        }
        let Ok(content) = fs::read_to_string(&path) else {
            break;
        };
        let Some(rest) = content
            .trim_end_matches(['\r', '\n'])
            .strip_prefix("gitdir:")
        else {
            break;
        };
        let target = Path::new(rest.trim());
        if target.as_os_str().is_empty() {
            break;
        }
        path = if target.is_absolute() {
            target.to_path_buf()
        } else {
            let base = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            base.join(target)
        };
    }
    path
}

/// Resolves the shared Git common directory (`commondir`) for linked worktrees, falling back to
/// `dot_git` for normal repositories and submodules.
pub(super) fn resolve_git_common_dir(dot_git: &Path) -> PathBuf {
    if let Ok(content) = fs::read_to_string(dot_git.join("commondir")) {
        let raw = content.trim();
        if !raw.is_empty() {
            let target = Path::new(raw);
            return if target.is_absolute() {
                target.to_path_buf()
            } else {
                dot_git.join(target)
            };
        }
    }
    dot_git.to_path_buf()
}

fn is_reftable_git_dir(dot_git: &Path) -> bool {
    let common_dir = resolve_git_common_dir(dot_git);
    dot_git.join("reftable").is_dir()
        || common_dir.join("reftable").is_dir()
        || fs::read_to_string(dot_git.join("HEAD"))
            .is_ok_and(|head| head.trim_end() == "ref: refs/heads/.invalid")
}

/// Temporary bare git dir that exposes a reftable repository's objects to `gix`, which
/// rejects the `ref: refs/heads/.invalid` HEAD stub that reftable repositories keep for older
/// clients. Objects are borrowed through `objects/info/alternates` and refs are listed with
/// `git for-each-ref` (see [`super::refs::discover_git_refs`]), so the source repository is never written.
pub(super) struct ReftableObjectsDir {
    pub(super) path: PathBuf,
}

impl ReftableObjectsDir {
    pub(super) fn create_if_reftable(dot_git: &Path) -> std::io::Result<Option<Self>> {
        if !is_reftable_git_dir(dot_git) {
            return Ok(None);
        }
        let path =
            std::env::temp_dir().join(format!("jj-cc-import-git-{}.git", uuid::Uuid::new_v4()));
        Self::create_at(dot_git, path).map(Some)
    }

    pub(super) fn create_at(dot_git: &Path, path: PathBuf) -> std::io::Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            fs::DirBuilder::new().mode(0o700).create(&path)?;
        }
        #[cfg(not(unix))]
        {
            fs::create_dir(&path)?;
        }
        // Construct the RAII guard only after exclusive directory creation succeeds so `Drop`
        // can never remove a pre-existing path if `create` fails with `AlreadyExists`.
        let dir = Self { path };
        let common_dir = resolve_git_common_dir(dot_git);
        fs::create_dir_all(dir.path.join("objects").join("info"))?;
        fs::create_dir(dir.path.join("refs"))?;
        fs::write(dir.path.join("HEAD"), "ref: refs/heads/main\n")?;
        let objects_dir = if common_dir.join("objects").is_dir() {
            common_dir.join("objects")
        } else {
            dot_git.join("objects")
        };
        let mut alternates = fs::canonicalize(&objects_dir)?
            .into_os_string()
            .into_encoded_bytes();
        alternates.push(b'\n');
        fs::write(
            dir.path.join("objects").join("info").join("alternates"),
            alternates,
        )?;
        Ok(dir)
    }
}

impl Drop for ReftableObjectsDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

struct TargetGitRepoCleanup {
    git_dir: PathBuf,
    first_created_dir: Option<PathBuf>,
    newly_created: bool,
    committed: bool,
}

impl TargetGitRepoCleanup {
    fn existing(git_dir: PathBuf) -> Self {
        Self {
            git_dir,
            first_created_dir: None,
            newly_created: false,
            committed: true,
        }
    }

    fn newly_created(git_dir: PathBuf, first_created_dir: Option<PathBuf>) -> Self {
        Self {
            git_dir,
            first_created_dir,
            newly_created: true,
            committed: false,
        }
    }

    fn commit(&mut self) {
        self.committed = true;
    }

    fn rollback(&mut self) {
        if self.committed || !self.newly_created {
            return;
        }
        self.committed = true;
        if let Some(root) = &self.first_created_dir {
            // Remove only the target repository directory recursively, then prune newly created
            // parent directories non-recursively so concurrent sibling directories survive.
            let _ = fs::remove_dir_all(&self.git_dir);
            let mut curr = self.git_dir.parent();
            while let Some(dir) = curr {
                if dir.as_os_str().is_empty() || !dir.starts_with(root) {
                    break;
                }
                match fs::remove_dir(dir) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(_) => break,
                }
                if dir == root {
                    break;
                }
                curr = dir.parent();
            }
        } else if let Ok(entries) = fs::read_dir(&self.git_dir) {
            // `git_dir` existed as an empty directory before export: remove only the entries
            // created inside it without removing the directory itself or following symlinks.
            for entry in entries.flatten() {
                let path = entry.path();
                if entry.file_type().is_ok_and(|ft| ft.is_dir()) {
                    let _ = fs::remove_dir_all(path);
                } else {
                    let _ = fs::remove_file(path);
                }
            }
        }
    }
}

impl Drop for TargetGitRepoCleanup {
    fn drop(&mut self) {
        self.rollback();
    }
}

fn first_nonexistent_ancestor(path: &Path) -> Option<PathBuf> {
    let mut candidate = None;
    for ancestor in path.ancestors() {
        if ancestor.as_os_str().is_empty() {
            break;
        }
        match fs::symlink_metadata(ancestor) {
            Ok(_) => break,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                candidate = Some(ancestor.to_path_buf());
            }
            Err(_) => break,
        }
    }
    candidate
}

fn normalize_git_dir(git_dir: &Path) -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    if git_dir.as_os_str().is_empty() {
        return Err("Destination Git directory path must not be empty".into());
    }
    let anchored = if git_dir.is_absolute() {
        git_dir.to_path_buf()
    } else {
        std::env::current_dir()?.join(git_dir)
    };
    let mut normalized = PathBuf::new();
    for component in anchored.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::Normal(part) => normalized.push(part),
            Component::ParentDir => {
                // Reject `..` after nonexistent or non-directory segments so paths like
                // `<existing_non_empty_dir>/missing/..` cannot bypass the non-empty-directory
                // guard or break `first_nonexistent_ancestor` rollback cleanup, and resolve
                // symlinks before popping so `symlink/..` matches OS path resolution.
                let meta = fs::symlink_metadata(&normalized).map_err(|e| {
                    format!(
                        "Invalid destination Git path {}: cannot traverse '..' through {}: {e}",
                        git_dir.display(),
                        normalized.display()
                    )
                })?;
                if meta.file_type().is_symlink() {
                    normalized = fs::canonicalize(&normalized).map_err(|e| {
                        format!(
                            "Invalid destination Git path {}: failed to resolve symlink {}: {e}",
                            git_dir.display(),
                            normalized.display()
                        )
                    })?;
                    if !normalized.is_dir() {
                        return Err(format!(
                            "Invalid destination Git path {}: cannot traverse '..' through non-directory {}",
                            git_dir.display(),
                            normalized.display()
                        )
                        .into());
                    }
                } else if !meta.is_dir() {
                    return Err(format!(
                        "Invalid destination Git path {}: cannot traverse '..' through non-directory {}",
                        git_dir.display(),
                        normalized.display()
                    )
                    .into());
                }
                if matches!(
                    normalized.components().next_back(),
                    Some(Component::Normal(_))
                ) {
                    normalized.pop();
                }
            }
        }
    }
    Ok(normalized)
}

pub(super) struct TargetGitRepo {
    pub(super) git_dir: PathBuf,
    pub(super) repo: gix::ThreadSafeRepository,
    pub(super) newly_created: bool,
    pub(super) is_bare: bool,
    cleanup: TargetGitRepoCleanup,
}

impl TargetGitRepo {
    pub(super) fn commit(&mut self) {
        self.cleanup.commit();
    }
}

fn same_dir_path(a: &Path, b: &Path) -> bool {
    match (fs::canonicalize(a), fs::canonicalize(b)) {
        (Ok(ca), Ok(cb)) => ca == cb,
        _ => a == b,
    }
}

/// Returns `true` when `repo` is a linked Git worktree (including linked worktrees of bare
/// repositories and `--separate-git-dir` repositories, where `gix::Repository::kind()` only
/// recognizes paths under a literal `.git` directory).
fn is_linked_worktree(repo: &gix::Repository) -> bool {
    repo.git_dir().join("commondir").is_file() && !same_dir_path(repo.git_dir(), repo.common_dir())
}

pub(super) fn open_or_init_git_repo(
    git_dir: &Path,
    bare: bool,
) -> Result<TargetGitRepo, Box<dyn std::error::Error + Send + Sync>> {
    let git_dir = normalize_git_dir(git_dir)?;
    let existed =
        git_dir.exists() && (git_dir.join(".git").exists() || git_dir.join("HEAD").exists());
    let (repo, cleanup) = if existed {
        if is_reftable_git_dir(&resolve_dot_git_dir(&git_dir)) {
            return Err(format!(
                "Unsupported Git reftable ref storage at {}: Commit Cloud export supports the files ref backend only. Export to a new directory or a repository initialized with `--ref-format=files`.",
                git_dir.display()
            )
            .into());
        }
        let repo = gix::open_opts(&git_dir, gix::open::Options::isolated()).map_err(|e| {
            format!(
                "Failed to open existing Git repository at {}: {e}",
                git_dir.display()
            )
        })?;
        (repo, TargetGitRepoCleanup::existing(git_dir.clone()))
    } else {
        let (git_dir_existed_empty, first_created_dir) = if git_dir.exists()
            || fs::symlink_metadata(&git_dir).is_ok()
        {
            if !git_dir.is_dir() {
                return Err(format!(
                    "Destination path {} exists and is not an empty directory",
                    git_dir.display()
                )
                .into());
            }
            if let Some(entry) = fs::read_dir(&git_dir)?.next() {
                entry?;
                return Err(format!(
                        "Destination path {} exists and is not an empty directory. Export to a new or empty directory, or an existing Git repository.",
                        git_dir.display()
                    )
                    .into());
            }
            (true, None)
        } else {
            (false, first_nonexistent_ancestor(&git_dir))
        };
        let cleanup = if git_dir_existed_empty || first_created_dir.is_some() {
            TargetGitRepoCleanup::newly_created(git_dir.clone(), first_created_dir)
        } else {
            TargetGitRepoCleanup::existing(git_dir.clone())
        };
        fs::create_dir_all(&git_dir)?;
        let kind = if bare {
            gix::create::Kind::Bare
        } else {
            gix::create::Kind::WithWorktree
        };
        let repo = gix::ThreadSafeRepository::init_opts(
            &git_dir,
            kind,
            gix::create::Options::default(),
            gix::open::Options::isolated(),
        )?
        .to_thread_local();
        (repo, cleanup)
    };
    let object_hash = repo.object_hash();
    let is_bare = repo.is_bare() && !is_linked_worktree(&repo) && repo.workdir().is_none();
    let target = TargetGitRepo {
        git_dir: git_dir.clone(),
        repo: repo.into_sync(),
        newly_created: !existed,
        is_bare,
        cleanup,
    };
    if object_hash != gix::hash::Kind::Sha1 {
        return Err(format!(
            "Unsupported Git object hash algorithm {object_hash:?} at {}: Commit Cloud export supports SHA-1 Git repositories only",
            git_dir.display()
        )
        .into());
    }
    Ok(target)
}

fn check_head_branch_conflict(
    head_repo: &gix::Repository,
    worktree_path: &Path,
    exported_branches: &BTreeMap<String, Vec<u8>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let head = head_repo
        .head()
        .map_err(|e| format!("Failed to inspect HEAD at {}: {e}", worktree_path.display()))?;
    let Some(referent) = head.referent_name() else {
        return Ok(());
    };
    let referent_str = referent.as_bstr().to_string();
    let Some(checked_out_branch) = referent_str.strip_prefix("refs/heads/") else {
        return Ok(());
    };
    if exported_branches.contains_key(checked_out_branch) {
        return Err(format!(
            "Refusing to update checked-out branch {checked_out_branch:?} in existing Git worktree {}: \
             updating a checked-out branch would leave the working tree and index out of sync. \
             Pass --bare with a bare repository that has no linked worktrees on that branch, \
             switch branches in the worktree (for example `git -C {} checkout --detach`), \
             or export to a new directory.",
            worktree_path.display(),
            worktree_path.display()
        )
        .into());
    }
    Ok(())
}

/// Refuses updating any branch currently checked out by `HEAD` in the target repository's main
/// worktree or any attached linked worktree (matching Git's `receive.denyCurrentBranch` across
/// worktrees) so no working tree and index are left out of sync with `HEAD`.
pub(super) fn ensure_no_checked_out_branch_conflict(
    repo: &gix::Repository,
    git_dir: &Path,
    exported_branches: &BTreeMap<String, Vec<u8>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if exported_branches.is_empty() {
        return Ok(());
    }
    if is_linked_worktree(repo) {
        let own_path = repo.workdir().unwrap_or(git_dir);
        check_head_branch_conflict(repo, own_path, exported_branches)?;
        let main_repo = repo.main_repo().map_err(|e| {
            format!(
                "Failed to inspect main Git repository for linked worktree {}: {e}",
                own_path.display()
            )
        })?;
        if !main_repo.is_bare() {
            let main_path = main_repo.workdir().unwrap_or_else(|| main_repo.git_dir());
            check_head_branch_conflict(&main_repo, main_path, exported_branches)?;
        }
    } else if !repo.is_bare() {
        let own_path = repo.workdir().unwrap_or(git_dir);
        check_head_branch_conflict(repo, own_path, exported_branches)?;
    }

    let linked_worktrees = repo.worktrees().map_err(|e| {
        format!(
            "Failed to enumerate linked Git worktrees for {}: {e}",
            git_dir.display()
        )
    })?;
    for proxy in linked_worktrees {
        if same_dir_path(proxy.git_dir(), repo.git_dir()) {
            continue;
        }
        let wt_path = proxy
            .base()
            .unwrap_or_else(|_| proxy.git_dir().to_path_buf());
        let wt_repo = proxy
            .into_repo_with_possibly_inaccessible_worktree()
            .map_err(|e| {
                format!(
                    "Failed to open linked Git worktree {}: {e}",
                    wt_path.display()
                )
            })?;
        check_head_branch_conflict(&wt_repo, &wt_path, exported_branches)?;
    }
    Ok(())
}

/// Materializes `HEAD`'s tree and index in a newly initialized non-bare Git repository so tracked
/// files exist on disk and `git status` is clean.
pub(super) async fn checkout_git_worktree(
    git_dir: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let out = git_command()
        .arg("-C")
        .arg(git_dir)
        .args(["checkout", "--force", "HEAD"])
        .output()
        .await
        .map_err(|e| {
            format!(
                "Failed to run `git checkout --force HEAD` in {}: {e}",
                git_dir.display()
            )
        })?;
    if !out.status.success() {
        return Err(format!(
            "`git checkout --force HEAD` failed in {} ({}): {}",
            git_dir.display(),
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        )
        .into());
    }
    Ok(())
}
