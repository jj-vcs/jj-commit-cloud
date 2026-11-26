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

//! Git reference discovery, import filtering, directory/file conflict validation,
//! and atomic Git ref updates.
//!
//! # Authority and Crash-Safety Ordering
//! `discover_git_refs` reads only `HEAD`, `refs/heads/*`, `refs/remotes/*`, and
//! `refs/tags/*` without mutating the source repository, using `git for-each-ref`
//! for reftable backends and `gix` otherwise. `write_git_refs` checks batch and
//! on-disk D/F conflicts before committing one atomic `gix` ref transaction.
//!
//! # Cross-Process and RPC Boundaries
//! Reftable ref discovery invokes `git for-each-ref` through a sanitized
//! subprocess environment and fails closed on non-UTF-8 importable ref names or
//! dangling ref targets.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs;
use std::path::Path;

use super::git_repo::git_command;

pub(super) type DiscoveredRefs = (
    Vec<Vec<u8>>,
    BTreeMap<String, Vec<u8>>,
    BTreeMap<String, Vec<u8>>,
);

pub(super) async fn discover_git_refs(
    thread_safe_repo: &gix::ThreadSafeRepository,
    dot_git: &Path,
    work_dir: &Path,
    is_reftable: bool,
) -> Result<DiscoveredRefs, Box<dyn std::error::Error + Send + Sync>> {
    let mut heads: Vec<Vec<u8>> = Vec::new();
    let mut local_branches: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    let mut remote_branches: BTreeMap<String, Vec<u8>> = BTreeMap::new();

    if !is_reftable {
        let repo = thread_safe_repo.to_thread_local();
        let platform = repo.references()?;
        let iter = platform.all()?;
        for ref_res in iter {
            let mut r = ref_res.map_err(|e| format!("Failed to read Git reference: {e}"))?;
            let Some(full_name) =
                decode_importable_git_ref_name(r.name().as_bstr().as_ref())?.map(ToOwned::to_owned)
            else {
                continue;
            };
            let id = r
                .peel_to_id()
                .map_err(|e| format!("Failed to peel Git reference '{full_name}': {e}"))?;
            register_git_ref(
                &repo,
                &full_name,
                id.detach(),
                &mut heads,
                &mut local_branches,
                &mut remote_branches,
            )?;
        }

        let mut head = repo
            .head()
            .map_err(|e| format!("Failed to read Git HEAD: {e}"))?;
        if !head.is_unborn() {
            let head_commit = head
                .peel_to_commit()
                .map_err(|e| format!("Failed to resolve Git HEAD to a commit: {e}"))?;
            let bytes = head_commit.id.as_bytes().to_vec();
            if !heads.contains(&bytes) {
                heads.push(bytes);
            }
        }
    } else {
        // `gix` reads the files ref backend, so reftable repositories query `git`
        // read-only for refs and `HEAD` while reading objects through `ReftableObjectsDir`.
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
            .args(["for-each-ref", "--format=%(refname)%00%(objectname)"])
            .current_dir(cwd)
            .output()
            .await
            .map_err(|e| {
                format!(
                    "Failed to run `git for-each-ref` for reftable repository {}: {e}",
                    canonical_dot_git.display()
                )
            })?;
        if !out.status.success() {
            return Err(format!(
                "`git for-each-ref` failed for {} ({}): {}",
                canonical_dot_git.display(),
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            )
            .into());
        }
        let head_out = git_command()
            .arg("--git-dir")
            .arg(&canonical_dot_git)
            .args(["rev-parse", "--verify", "--quiet", "HEAD^{commit}"])
            .current_dir(cwd)
            .output()
            .await
            .map_err(|e| {
                format!(
                    "Failed to run `git rev-parse` for reftable repository {}: {e}",
                    canonical_dot_git.display()
                )
            })?;

        let repo = thread_safe_repo.to_thread_local();
        for (name, oid) in parse_reftable_for_each_ref_output(&out.stdout, &canonical_dot_git)? {
            register_git_ref(
                &repo,
                &name,
                oid,
                &mut heads,
                &mut local_branches,
                &mut remote_branches,
            )?;
        }
        if head_out.status.success() {
            let oid = gix::hash::ObjectId::from_hex(head_out.stdout.trim_ascii()).map_err(|e| {
                format!(
                    "Invalid commit object ID {:?} returned by `git rev-parse HEAD^{{commit}}` for {}: {e}",
                    gix::bstr::BStr::new(head_out.stdout.trim_ascii()),
                    canonical_dot_git.display()
                )
            })?;
            let bytes = oid.as_bytes().to_vec();
            if !heads.contains(&bytes) {
                heads.push(bytes);
            }
        } else {
            // Only an unborn symbolic `HEAD` (pointing at a branch ref that does not yet exist)
            // may be skipped; corrupt or dangling detached `HEAD` must surface as an error.
            let sym_out = git_command()
                .arg("--git-dir")
                .arg(&canonical_dot_git)
                .args(["symbolic-ref", "--quiet", "HEAD"])
                .current_dir(cwd)
                .output()
                .await
                .map_err(|e| {
                    format!(
                        "Failed to run `git symbolic-ref` for reftable repository {}: {e}",
                        canonical_dot_git.display()
                    )
                })?;
            let is_unborn = if sym_out.status.success() {
                let target_ref = std::str::from_utf8(sym_out.stdout.trim_ascii()).map_err(|e| {
                    format!(
                        "Non-UTF-8 symbolic HEAD target {:?} in reftable repository {}: {e}",
                        gix::bstr::BStr::new(sym_out.stdout.trim_ascii()),
                        canonical_dot_git.display()
                    )
                })?;
                if target_ref.is_empty() {
                    false
                } else {
                    let show_out = git_command()
                        .arg("--git-dir")
                        .arg(&canonical_dot_git)
                        .args(["show-ref", "--verify", "--quiet", "--", target_ref])
                        .current_dir(cwd)
                        .output()
                        .await
                        .map_err(|e| {
                            format!(
                                "Failed to run `git show-ref` for reftable repository {}: {e}",
                                canonical_dot_git.display()
                            )
                        })?;
                    show_out.status.code() == Some(1)
                }
            } else {
                false
            };
            if !is_unborn {
                return Err(format!(
                    "Failed to resolve Git HEAD to a commit in reftable repository {}: {}",
                    canonical_dot_git.display(),
                    String::from_utf8_lossy(&head_out.stderr).trim()
                )
                .into());
            }
        }
    }

    Ok((heads, local_branches, remote_branches))
}

pub(super) fn parse_reftable_for_each_ref_output(
    stdout: &[u8],
    canonical_dot_git: &Path,
) -> Result<Vec<(String, gix::hash::ObjectId)>, Box<dyn std::error::Error + Send + Sync>> {
    let mut parsed = Vec::new();
    for line in stdout.split(|&b| b == b'\n') {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.is_empty() {
            continue;
        }
        let Some(nul_idx) = line.iter().position(|&b| b == 0) else {
            return Err(format!(
                "Unexpected `git for-each-ref` output line {:?} in reftable repository {}",
                gix::bstr::BStr::new(line),
                canonical_dot_git.display()
            )
            .into());
        };
        let raw_name = &line[..nul_idx];
        let raw_hex = &line[nul_idx + 1..];
        let Some(name) = decode_importable_git_ref_name(raw_name)? else {
            continue;
        };
        if raw_hex.is_empty() {
            return Err(format!(
                "Failed to resolve dangling Git reference '{name}' in reftable repository {}",
                canonical_dot_git.display()
            )
            .into());
        }
        let hex_str = std::str::from_utf8(raw_hex).map_err(|e| {
            format!(
                "Invalid non-UTF-8 object ID {:?} for Git reference '{name}': {e}",
                gix::bstr::BStr::new(raw_hex)
            )
        })?;
        let oid = gix::hash::ObjectId::from_hex(hex_str.as_bytes()).map_err(|e| {
            format!("Invalid object ID {hex_str:?} for Git reference '{name}': {e}")
        })?;
        if oid.kind() != gix::hash::Kind::Sha1 {
            return Err(format!(
                "Unsupported object ID {hex_str:?} ({:?}) for Git reference '{name}': expected SHA-1",
                oid.kind()
            )
            .into());
        }
        parsed.push((name.to_string(), oid));
    }
    Ok(parsed)
}

fn register_git_ref(
    repo: &gix::Repository,
    full_name: &str,
    oid: gix::hash::ObjectId,
    heads: &mut Vec<Vec<u8>>,
    local_branches: &mut BTreeMap<String, Vec<u8>>,
    remote_branches: &mut BTreeMap<String, Vec<u8>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if !is_importable_git_ref(full_name) {
        return Ok(());
    }
    let obj = repo
        .find_object(oid)
        .map_err(|e| format!("Failed to read object {oid} for Git reference '{full_name}': {e}"))?;
    let peeled = obj
        .peel_tags_to_end()
        .map_err(|e| format!("Failed to peel Git reference '{full_name}' ({oid}): {e}"))?;
    if peeled.kind != gix::object::Kind::Commit {
        if full_name.starts_with("refs/tags/") {
            return Ok(());
        }
        return Err(format!(
            "Git reference '{full_name}' points to {:?} {oid}, expected commit",
            peeled.kind
        )
        .into());
    }
    let bytes = peeled.id.as_bytes().to_vec();
    if !heads.contains(&bytes) {
        heads.push(bytes.clone());
    }
    if let Some(branch) = full_name.strip_prefix("refs/heads/")
        && !branch.is_empty()
        && branch != ".invalid"
        && branch != "HEAD"
    {
        local_branches.insert(branch.to_string(), bytes);
    } else if let Some(rem) = full_name.strip_prefix("refs/remotes/")
        && let Some((remote_name, branch_name)) = rem.split_once('/')
        && !remote_name.is_empty()
        && remote_name != "git"
        && !remote_name.contains('@')
        && !branch_name.is_empty()
        && branch_name != "HEAD"
    {
        remote_branches.insert(format!("{branch_name}@{remote_name}"), bytes);
    }
    Ok(())
}

/// Mirrors `jj git import`: only branches, remote-tracking branches (except `<remote>/HEAD` and
/// remotes containing `'@'`, which cannot be encoded unambiguously in `<bookmark>@<remote>`), and
/// tags make commits visible. `refs/jj/keep/*`, `refs/stash`, `refs/notes/*`, `refs/pull/*`, and
/// other namespaces hold commits that jj does not show. Tags only keep their commits visible
/// because the cloud `View` has no tags yet.
fn is_importable_git_ref_bytes(full_name: &[u8]) -> bool {
    if let Some(remote_ref) = full_name.strip_prefix(b"refs/remotes/") {
        return remote_ref
            .iter()
            .position(|&b| b == b'/')
            .is_some_and(|idx| {
                let remote = &remote_ref[..idx];
                let branch = &remote_ref[idx + 1..];
                !remote.is_empty()
                    && remote != b"git"
                    && !remote.contains(&b'@')
                    && !branch.is_empty()
                    && branch != b"HEAD"
            });
    }
    full_name
        .strip_prefix(b"refs/heads/")
        .is_some_and(|b| !b.is_empty() && b != b".invalid" && b != b"HEAD")
        || full_name
            .strip_prefix(b"refs/tags/")
            .is_some_and(|t| !t.is_empty())
}

pub(super) fn is_importable_git_ref(full_name: &str) -> bool {
    is_importable_git_ref_bytes(full_name.as_bytes())
}

pub(super) fn decode_importable_git_ref_name(
    raw_name: &[u8],
) -> Result<Option<&str>, Box<dyn std::error::Error + Send + Sync>> {
    if !is_importable_git_ref_bytes(raw_name) {
        return Ok(None);
    }
    std::str::from_utf8(raw_name).map(Some).map_err(|e| {
        format!(
            "Non-UTF-8 Git reference name {:?}: {e}",
            gix::bstr::BStr::new(raw_name)
        )
        .into()
    })
}

/// Returns `refs/heads/<name>` if `name` is a valid Git branch name. Bookmark names in the `View`
/// are arbitrary strings, so this check keeps names such as `../config`, `/home/user/file`,
/// `-flag`, or `HEAD` from reaching the ref store.
pub(super) fn git_branch_ref_name(name: &str) -> Option<gix::refs::FullName> {
    if name == "HEAD" || name.starts_with('-') {
        return None;
    }
    gix::refs::FullName::try_from(format!("refs/heads/{name}")).ok()
}

pub(super) fn check_batch_df_ref_conflicts(
    ref_names: &BTreeSet<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for name in ref_names {
        for (slash_idx, _) in name.match_indices('/') {
            let prefix = &name[..slash_idx];
            if ref_names.contains(prefix) {
                return Err(format!(
                    "Git ref directory/file conflict between '{prefix}' and '{name}'"
                )
                .into());
            }
        }
    }
    Ok(())
}

pub(super) fn check_existing_df_ref_conflicts(
    repo: &gix::Repository,
    update_names: &BTreeSet<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut existing_refs: HashSet<String> = HashSet::new();
    for ref_res in repo.references()?.all()? {
        let r = ref_res.map_err(|e| format!("Failed to read existing Git reference: {e}"))?;
        existing_refs.insert(r.name().as_bstr().to_string());
    }
    for name in update_names {
        for (slash_idx, _) in name.match_indices('/') {
            let prefix = &name[..slash_idx];
            if existing_refs.contains(prefix) {
                return Err(format!(
                    "Git ref directory/file conflict between existing '{prefix}' and '{name}'"
                )
                .into());
            }
        }
    }
    for existing in &existing_refs {
        for (slash_idx, _) in existing.match_indices('/') {
            let prefix = &existing[..slash_idx];
            if update_names.contains(prefix) {
                return Err(format!(
                    "Git ref directory/file conflict between '{prefix}' and existing '{existing}'"
                )
                .into());
            }
        }
    }
    Ok(())
}

/// Applies all exported branch and `HEAD` updates in one atomic gix ref transaction so a lock or
/// directory/file ref conflict never leaves partially updated refs on disk.
pub(super) fn write_git_refs(
    repo: &gix::Repository,
    updates: Vec<(gix::refs::FullName, gix::refs::Target)>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use gix::refs::transaction::{Change, LogChange, PreviousValue, RefEdit, RefLog};

    // Check directory/file ref conflicts (e.g. `refs/heads/foo` alongside `refs/heads/foo/bar`)
    // before opening the gix files transaction: lockfiles (`foo.lock` vs `foo/bar.lock`) do not
    // collide during `prepare()`, so a conflict would otherwise fail mid-`commit()` after earlier
    // refs in the batch were already renamed into place.
    let mut update_names: BTreeSet<String> = BTreeSet::new();
    for (name, _) in &updates {
        update_names.insert(name.as_bstr().to_string());
    }
    check_batch_df_ref_conflicts(&update_names)?;
    check_existing_df_ref_conflicts(repo, &update_names)?;

    let edits: Vec<RefEdit> = updates
        .into_iter()
        .map(|(name, new)| RefEdit {
            change: Change::Update {
                log: LogChange {
                    mode: RefLog::AndReference,
                    force_create_reflog: false,
                    message: "jj cc export-git".into(),
                },
                expected: PreviousValue::Any,
                new,
            },
            name,
            deref: false,
        })
        .collect();
    // Reflog entries need a committer, and Git may have no identity configured.
    let mut fallback_time_buf = gix::date::parse::TimeBuf::default();
    let fallback_time_str = gix::date::Time::now_utc().to_str(&mut fallback_time_buf);
    let committer = repo
        .committer()
        .transpose()?
        .unwrap_or(gix::actor::SignatureRef {
            name: "jj cc export-git".into(),
            email: "".into(),
            time: fallback_time_str,
        });
    let fail_mode = gix::lock::acquire::Fail::Immediately;
    repo.refs
        .transaction()
        .prepare(edits, fail_mode, fail_mode)?
        .commit(Some(committer))?;
    Ok(())
}
