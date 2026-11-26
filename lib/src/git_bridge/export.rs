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

//! Commit-Cloud-to-Git export orchestration and bounded recursive tree and file
//! materialization into a local Git object database.
//!
//! # Authority and Crash-Safety Ordering
//! `GitExporter` preflights commits, branch ref names, target worktrees, and D/F
//! ref conflicts before fetching objects, verifies every blob, symlink, tree, and
//! commit SHA-1 digest before writing to `.git/objects/`, writes Git refs
//! atomically, checks out newly created non-bare repos, and commits cleanup last.
//!
//! # Cross-Process and RPC Boundaries
//! Paginated `ReadTree` responses are bounded by `MAX_TREE_ENTRIES` and streamed
//! `ReadFile` payloads are bounded by `MAX_READ_FILE_BYTES`, with every page and
//! entry validated before recursing into child trees.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use cc_common::backend::{
    Commit, ReadCommitRequest, ReadFileRequest, ReadSymlinkRequest, ReadTreeRequest,
    RegisterRepositoryRequest, tree_value,
};
use cc_common::op_store::RefTarget;
use cc_common::{
    COMMIT_ID_LENGTH, EMPTY_TREE_ID_BYTES, FILE_ID_LENGTH, ROOT_COMMIT_ID_BYTES, SYMLINK_ID_LENGTH,
    TREE_ID_LENGTH,
};
use futures::StreamExt as _;
use gix::objs::WriteTo as _;
use gix::prelude::Write as _;

use super::git_repo::{
    checkout_git_worktree, ensure_no_checked_out_branch_conflict, open_or_init_git_repo,
};
use super::objects::{
    VerifiedExportCommit, prepare_export_commit, validate_export_commit_structure,
    write_verified_commit,
};
use super::publish::{read_operation_and_view, resolve_single_op_head};
use super::refs::{
    check_batch_df_ref_conflicts, check_existing_df_ref_conflicts, git_branch_ref_name,
    write_git_refs,
};
use super::{
    GitExportOptions, GitExportReport, MAX_READ_FILE_BYTES, MAX_TREE_ENTRIES, validate_optional_id,
    validate_required_id,
};
use crate::util::{AuthBackendClient, connect_backend_client, connect_op_store_client};

pub struct GitExporter {
    opts: GitExportOptions,
}

impl GitExporter {
    pub fn new(opts: GitExportOptions) -> Self {
        Self { opts }
    }

    pub async fn run(&self) -> Result<GitExportReport, Box<dyn std::error::Error + Send + Sync>> {
        let repo_id = validate_required_id(&self.opts.repo_id, "--repo-id")?;
        let explicit_project_id =
            validate_optional_id(self.opts.project_id.as_deref(), "--project-id")?;

        let mut backend_client = connect_backend_client(self.opts.server_url.clone()).await?;
        let mut op_client = connect_op_store_client(self.opts.server_url.clone()).await?;

        let op_head = resolve_single_op_head(&mut op_client, &repo_id).await?;

        // `BackendService` has no read-only `GetRepository` RPC yet, so verify an explicit
        // `--project-id` binding through the idempotent `RegisterRepository` check.
        let object_project_id = if let Some(proj) = explicit_project_id {
            backend_client
                .register_repository(RegisterRepositoryRequest {
                    project_id: proj,
                    repo_id: Some(repo_id.clone()),
                    name: None,
                    hash_algorithm: cc_common::backend::HashAlgorithm::Sha1 as i32,
                })
                .await
                .map_err(|e| {
                    format!("Failed to verify project binding for repository '{repo_id}': {e}")
                })?
                .into_inner()
                .project_id
        } else {
            repo_id.clone()
        };

        let (_op, view) =
            read_operation_and_view(&mut op_client, &repo_id, &op_head, "export-git").await?;

        // Seed roots from all resolved bookmarks and non-root view heads in deterministic order.
        let wc_set: HashSet<Vec<u8>> = view.wc_commit_ids.values().cloned().collect();
        let mut root_targets: Vec<Vec<u8>> = Vec::new();
        let mut exported_branches: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        let mut skipped_bookmarks: Vec<String> = Vec::new();
        let mut skipped_conflicted_bookmarks: Vec<String> = Vec::new();

        let mut sorted_bookmarks: Vec<(&String, &RefTarget)> =
            view.local_bookmarks.iter().collect();
        sorted_bookmarks.sort_by_key(|(a, _)| *a);
        for (bm_name, target) in sorted_bookmarks {
            if !target.removes.is_empty() || target.adds.len() > 1 {
                skipped_conflicted_bookmarks.push(bm_name.clone());
                continue;
            }
            if let [first_add] = target.adds.as_slice() {
                if first_add.commit_id.as_slice() == ROOT_COMMIT_ID_BYTES
                    || git_branch_ref_name(bm_name).is_none()
                {
                    skipped_bookmarks.push(bm_name.clone());
                    continue;
                }
                exported_branches.insert(bm_name.clone(), first_add.commit_id.clone());
                if !root_targets.contains(&first_add.commit_id) {
                    root_targets.push(first_add.commit_id.clone());
                }
            }
        }
        let mut sorted_head_ids = view.head_ids.clone();
        sorted_head_ids.sort();
        let mut prefetched_commits: HashMap<Vec<u8>, Commit> = HashMap::new();
        for head_id in &sorted_head_ids {
            if head_id.as_slice() == ROOT_COMMIT_ID_BYTES {
                continue;
            }
            // Skip synthetic empty working-copy commits unless explicitly pointed to by a bookmark.
            if wc_set.contains(head_id) && !exported_branches.values().any(|b| b == head_id) {
                if !prefetched_commits.contains_key(head_id) {
                    let proto_c = backend_client
                        .read_commit(ReadCommitRequest {
                            project_id: object_project_id.clone(),
                            commit_id: head_id.clone(),
                        })
                        .await?
                        .into_inner()
                        .commit
                        .ok_or("missing commit payload during export-git")?;
                    prefetched_commits.insert(head_id.clone(), proto_c);
                }
                let maybe_only_parent = {
                    let proto_c = &prefetched_commits[head_id];
                    if proto_c.description.is_empty()
                        && let [only_parent] = proto_c.parent_commit_ids.as_slice()
                    {
                        Some((only_parent.clone(), proto_c.root_tree_id.clone()))
                    } else {
                        None
                    }
                };
                if let Some((only_parent, head_trees)) = maybe_only_parent {
                    let parent_trees = if only_parent.as_slice() == ROOT_COMMIT_ID_BYTES {
                        vec![EMPTY_TREE_ID_BYTES.to_vec()]
                    } else {
                        if !prefetched_commits.contains_key(&only_parent) {
                            let parent_commit = backend_client
                                .read_commit(ReadCommitRequest {
                                    project_id: object_project_id.clone(),
                                    commit_id: only_parent.clone(),
                                })
                                .await?
                                .into_inner()
                                .commit
                                .ok_or("missing parent commit payload during export-git")?;
                            prefetched_commits.insert(only_parent.clone(), parent_commit);
                        }
                        prefetched_commits[&only_parent].root_tree_id.clone()
                    };
                    if parent_trees == head_trees {
                        if only_parent.as_slice() != ROOT_COMMIT_ID_BYTES
                            && !root_targets.contains(&only_parent)
                        {
                            root_targets.push(only_parent);
                        }
                        continue;
                    }
                }
            }
            if !root_targets.contains(head_id) {
                root_targets.push(head_id.clone());
            }
        }

        // Fetch reachable commit graph from Commit Cloud topological oldest-first.
        let mut commit_map: HashMap<Vec<u8>, Commit> = HashMap::new();
        let mut topo_commits: Vec<Vec<u8>> = Vec::new();
        let mut state: HashMap<Vec<u8>, u8> = HashMap::new();
        for target in &root_targets {
            let mut stack = vec![(target.clone(), false)];
            while let Some((cid, expanded)) = stack.pop() {
                if cid.as_slice() == ROOT_COMMIT_ID_BYTES {
                    continue;
                }
                if expanded {
                    if state.get(&cid) != Some(&2) {
                        state.insert(cid.clone(), 2);
                        topo_commits.push(cid);
                    }
                    continue;
                }
                match state.get(&cid) {
                    Some(&2) => continue,
                    Some(&1) => {
                        return Err(format!(
                            "Cycle detected in Commit Cloud commit graph at commit {}",
                            hex::encode(&cid)
                        )
                        .into());
                    }
                    _ => {}
                }
                state.insert(cid.clone(), 1);
                let commit = match prefetched_commits.remove(&cid) {
                    Some(cached) => cached,
                    None => backend_client
                        .read_commit(ReadCommitRequest {
                            project_id: object_project_id.clone(),
                            commit_id: cid.clone(),
                        })
                        .await?
                        .into_inner()
                        .commit
                        .ok_or("missing commit payload during export-git")?,
                };
                validate_export_commit_structure(&cid, &commit)?;
                stack.push((cid.clone(), true));
                for p in commit.parent_commit_ids.iter().rev() {
                    if p.as_slice() == ROOT_COMMIT_ID_BYTES {
                        continue;
                    }
                    match state.get(p) {
                        Some(&2) => {}
                        Some(&1) => {
                            return Err(format!(
                                "Cycle detected in Commit Cloud commit graph at commit {}",
                                hex::encode(p)
                            )
                            .into());
                        }
                        _ => stack.push((p.clone(), false)),
                    }
                }
                commit_map.insert(cid, commit);
            }
        }

        // Only synthesize a fallback `main` branch when the repository had zero local bookmarks
        // (never when bookmarks existed and were all skipped as conflicted or invalid).
        if view.local_bookmarks.is_empty()
            && exported_branches.is_empty()
            && let Some(last_head) = topo_commits.last()
        {
            exported_branches.insert("main".to_string(), last_head.clone());
        }

        // Preflight branch ref names, commit raw_git_commit integrity, and batch-internal
        // directory/file ref conflicts before initializing or mutating the destination Git dir.
        let mut branch_ref_updates: Vec<(gix::refs::FullName, gix::refs::Target)> =
            Vec::with_capacity(exported_branches.len() + 1);
        let mut branch_ref_names: BTreeSet<String> = BTreeSet::new();
        for (branch, cid) in &exported_branches {
            if cid.len() != COMMIT_ID_LENGTH {
                return Err(format!("invalid commit ID length {}", cid.len()).into());
            }
            let name = git_branch_ref_name(branch)
                .ok_or_else(|| format!("bookmark {branch:?} is not a valid Git branch name"))?;
            branch_ref_names.insert(name.as_bstr().to_string());
            let target = gix::refs::Target::Object(gix::hash::ObjectId::try_from(cid.as_slice())?);
            branch_ref_updates.push((name, target));
        }
        check_batch_df_ref_conflicts(&branch_ref_names)?;

        let mut prepared_commits: HashMap<Vec<u8>, VerifiedExportCommit> =
            HashMap::with_capacity(topo_commits.len());
        for cid in &topo_commits {
            let commit = commit_map.get(cid).expect("commit loaded");
            let verified = prepare_export_commit(cid, commit)?;
            prepared_commits.insert(cid.clone(), verified);
        }

        let mut target_repo = open_or_init_git_repo(&self.opts.git_dir, self.opts.bare)?;
        if !target_repo.newly_created {
            let git_repo = target_repo.repo.to_thread_local();
            ensure_no_checked_out_branch_conflict(
                &git_repo,
                &target_repo.git_dir,
                &exported_branches,
            )?;
            if !branch_ref_names.is_empty() {
                check_existing_df_ref_conflicts(&git_repo, &branch_ref_names)?;
            }
        }

        let mut tree_state = ExportTreeState::default();

        for cid in &topo_commits {
            let commit = commit_map.get(cid).expect("commit loaded");
            let verified = prepared_commits.get(cid).expect("commit prepared");
            for tree_id in &commit.root_tree_id {
                export_tree_recursive(
                    &target_repo.repo,
                    &mut backend_client,
                    &object_project_id,
                    tree_id,
                    &mut tree_state,
                )
                .await?;
            }
            if let Some(raw_tree_id) = &verified.raw_tree_id
                && !commit.root_tree_id.iter().any(|t| t == raw_tree_id)
            {
                export_tree_recursive(
                    &target_repo.repo,
                    &mut backend_client,
                    &object_project_id,
                    raw_tree_id,
                    &mut tree_state,
                )
                .await?;
            }
            write_verified_commit(&target_repo.repo, cid, &verified.git_commit_bytes)?;
        }

        // Batch `refs/heads/<bookmark>` (and `HEAD` for newly created or bare repos) into one
        // atomic ref transaction.
        {
            let git_repo = target_repo.repo.to_thread_local();
            let mut ref_updates = branch_ref_updates;
            if (target_repo.newly_created || target_repo.is_bare)
                && let Some(default_branch) = exported_branches
                    .contains_key("main")
                    .then_some("main")
                    .or_else(|| exported_branches.keys().next().map(String::as_str))
            {
                let head_target =
                    git_branch_ref_name(default_branch).ok_or("invalid default branch")?;
                ref_updates.push(("HEAD".try_into()?, gix::refs::Target::Symbolic(head_target)));
            }
            if !ref_updates.is_empty() {
                write_git_refs(&git_repo, ref_updates)?;
            }
        }

        if target_repo.newly_created && !target_repo.is_bare && !exported_branches.is_empty() {
            checkout_git_worktree(&target_repo.git_dir).await?;
        }
        target_repo.commit();

        skipped_bookmarks.sort();
        skipped_conflicted_bookmarks.sort();

        Ok(GitExportReport {
            commits_exported: topo_commits.len(),
            trees_exported: tree_state.written_trees.len(),
            blobs_exported: tree_state.written_blobs.len(),
            bookmarks_exported: exported_branches.len(),
            skipped_bookmarks,
            skipped_conflicted_bookmarks,
        })
    }
}

async fn read_file_with_max_bytes(
    backend_client: &mut AuthBackendClient,
    project_id: &str,
    file_id: &[u8],
    max_bytes: usize,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let mut stream = backend_client
        .read_file(ReadFileRequest {
            project_id: project_id.to_string(),
            file_id: file_id.to_vec(),
            offset: 0,
            max_bytes: 0,
        })
        .await?
        .into_inner();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if bytes
            .len()
            .checked_add(chunk.chunk.len())
            .is_none_or(|n| n > max_bytes)
        {
            return Err(format!(
                "ReadFile exceeded maximum allowed size of {max_bytes} bytes for file {}",
                hex::encode(file_id)
            )
            .into());
        }
        bytes.extend_from_slice(&chunk.chunk);
    }
    Ok(bytes)
}

#[derive(Default)]
pub(super) struct ExportTreeState {
    pub(super) written_blobs: HashSet<Vec<u8>>,
    pub(super) validated_symlinks: HashSet<Vec<u8>>,
    pub(super) written_trees: HashSet<Vec<u8>>,
    pub(super) in_progress: HashSet<Vec<u8>>,
}

pub(super) async fn export_tree_recursive(
    thread_safe_repo: &gix::ThreadSafeRepository,
    backend_client: &mut AuthBackendClient,
    project_id: &str,
    tree_id: &[u8],
    state: &mut ExportTreeState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    export_tree_recursive_with_max_file_bytes(
        thread_safe_repo,
        backend_client,
        project_id,
        tree_id,
        state,
        MAX_READ_FILE_BYTES,
    )
    .await
}

pub(super) async fn export_tree_recursive_with_max_file_bytes(
    thread_safe_repo: &gix::ThreadSafeRepository,
    backend_client: &mut AuthBackendClient,
    project_id: &str,
    tree_id: &[u8],
    state: &mut ExportTreeState,
    max_file_bytes: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if state.written_trees.contains(tree_id) {
        return Ok(());
    }
    if tree_id == EMPTY_TREE_ID_BYTES {
        let computed = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Tree, &[])?;
        if computed.as_bytes() != EMPTY_TREE_ID_BYTES {
            return Err(format!(
                "Exported empty Git tree SHA-1 mismatch: expected {}, computed {computed}",
                hex::encode(EMPTY_TREE_ID_BYTES)
            )
            .into());
        }
        let written_tree = {
            let git_repo = thread_safe_repo.to_thread_local();
            git_repo.objects.write_buf(gix::objs::Kind::Tree, &[])?
        };
        if written_tree.as_bytes() != EMPTY_TREE_ID_BYTES {
            return Err(format!(
                "Exported empty Git tree SHA-1 mismatch: expected {}, wrote {written_tree}",
                hex::encode(EMPTY_TREE_ID_BYTES)
            )
            .into());
        }
        state.written_trees.insert(tree_id.to_vec());
        return Ok(());
    }
    if !state.in_progress.insert(tree_id.to_vec()) {
        return Err(format!(
            "Cycle detected in Commit Cloud tree graph at tree {}",
            hex::encode(tree_id)
        )
        .into());
    }
    let result = export_tree_recursive_inner(
        thread_safe_repo,
        backend_client,
        project_id,
        tree_id,
        state,
        max_file_bytes,
    )
    .await;
    state.in_progress.remove(tree_id);
    result
}

async fn export_tree_recursive_inner(
    thread_safe_repo: &gix::ThreadSafeRepository,
    backend_client: &mut AuthBackendClient,
    project_id: &str,
    tree_id: &[u8],
    state: &mut ExportTreeState,
    max_file_bytes: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let tree_id_hex = hex::encode(tree_id);
    let mut entries = Vec::new();
    let mut page_token = String::new();
    loop {
        let resp = backend_client
            .read_tree(ReadTreeRequest {
                project_id: project_id.to_string(),
                tree_id: tree_id.to_vec(),
                path: String::new(),
                page_size: 0,
                page_token: page_token.clone(),
            })
            .await?
            .into_inner();
        if !resp.tree_id.is_empty() && resp.tree_id.as_slice() != tree_id {
            return Err(format!(
                "ReadTree response tree_id {} did not match requested {tree_id_hex}",
                hex::encode(&resp.tree_id)
            )
            .into());
        }
        if resp.entries.is_empty() && !resp.next_page_token.is_empty() {
            return Err(format!(
                "ReadTree returned empty page with non-empty next_page_token for tree {tree_id_hex}"
            )
            .into());
        }
        if entries.len().saturating_add(resp.entries.len()) > MAX_TREE_ENTRIES {
            return Err(format!(
                "ReadTree exceeded maximum of {MAX_TREE_ENTRIES} entries for tree {tree_id_hex}"
            )
            .into());
        }
        entries.extend(resp.entries);
        if resp.next_page_token.is_empty() {
            break;
        }
        if !page_token.is_empty() && resp.next_page_token.as_bytes() <= page_token.as_bytes() {
            return Err(format!(
                "read_tree pagination cursor did not advance for tree {tree_id_hex}"
            )
            .into());
        }
        page_token = resp.next_page_token;
    }

    // Validate every entry name, uniqueness, value presence, and digest length before fetching
    // any child blobs, symlinks, or subtrees.
    {
        let mut seen_names: HashSet<&str> = HashSet::with_capacity(entries.len());
        for entry in &entries {
            let name = entry.name.as_str();
            if name.is_empty()
                || name == "."
                || name == ".."
                || name.eq_ignore_ascii_case(".git")
                || name.contains(['/', '\0'])
            {
                return Err(format!(
                    "ReadTree returned invalid tree entry name {name:?} in tree {tree_id_hex}"
                )
                .into());
            }
            if !seen_names.insert(name) {
                return Err(format!(
                    "ReadTree returned duplicate tree entry name {name:?} in tree {tree_id_hex}"
                )
                .into());
            }
            let val = entry
                .value
                .as_ref()
                .and_then(|v| v.value.as_ref())
                .ok_or_else(|| {
                    format!("ReadTree entry {name:?} in tree {tree_id_hex} is missing TreeValue")
                })?;
            match val {
                tree_value::Value::File(f) => {
                    if f.id.len() != FILE_ID_LENGTH {
                        return Err(format!(
                            "ReadTree file entry {name:?} in tree {tree_id_hex} must have {FILE_ID_LENGTH}-byte id (got {} bytes)",
                            f.id.len()
                        )
                        .into());
                    }
                    if !f.copy_id.is_empty() {
                        return Err(format!(
                            "ReadTree file entry {name:?} in tree {tree_id_hex} has unsupported non-empty copy_id ({} bytes)",
                            f.copy_id.len()
                        )
                        .into());
                    }
                }
                tree_value::Value::SymlinkId(id) => {
                    if id.len() != SYMLINK_ID_LENGTH {
                        return Err(format!(
                            "ReadTree symlink entry {name:?} in tree {tree_id_hex} must have {SYMLINK_ID_LENGTH}-byte id (got {} bytes)",
                            id.len()
                        )
                        .into());
                    }
                }
                tree_value::Value::TreeId(id) => {
                    if id.len() != TREE_ID_LENGTH {
                        return Err(format!(
                            "ReadTree subdir entry {name:?} in tree {tree_id_hex} must have {TREE_ID_LENGTH}-byte id (got {} bytes)",
                            id.len()
                        )
                        .into());
                    }
                }
                tree_value::Value::GitSubmoduleId(id) => {
                    if id.len() != COMMIT_ID_LENGTH {
                        return Err(format!(
                            "ReadTree submodule entry {name:?} in tree {tree_id_hex} must have {COMMIT_ID_LENGTH}-byte id (got {} bytes)",
                            id.len()
                        )
                        .into());
                    }
                }
                tree_value::Value::ConflictId(_) => {
                    return Err("Cannot export unresolved conflict entry to native Git tree".into());
                }
            }
        }
    }

    let mut gix_entries = Vec::with_capacity(entries.len());
    for entry in entries {
        let Some(val) = entry.value.as_ref().and_then(|v| v.value.as_ref()) else {
            return Err(format!(
                "ReadTree entry {:?} in tree {tree_id_hex} is missing TreeValue",
                entry.name
            )
            .into());
        };
        let (kind, oid_bytes) = match val {
            tree_value::Value::File(f) => {
                if !state.written_blobs.contains(&f.id) {
                    let bytes =
                        read_file_with_max_bytes(backend_client, project_id, &f.id, max_file_bytes)
                            .await?;
                    let computed = gix::objs::compute_hash(
                        gix::hash::Kind::Sha1,
                        gix::objs::Kind::Blob,
                        &bytes,
                    )?;
                    if computed.as_bytes() != f.id.as_slice() {
                        return Err(format!(
                            "Exported Git blob SHA-1 mismatch: expected {}, computed {computed}",
                            hex::encode(&f.id)
                        )
                        .into());
                    }
                    let written = {
                        let git_repo = thread_safe_repo.to_thread_local();
                        git_repo.objects.write_buf(gix::objs::Kind::Blob, &bytes)?
                    };
                    if written.as_bytes() != f.id.as_slice() {
                        return Err(format!(
                            "Exported Git blob SHA-1 mismatch: expected {}, wrote {written}",
                            hex::encode(&f.id)
                        )
                        .into());
                    }
                    state.written_blobs.insert(f.id.clone());
                }
                let mode = if f.executable {
                    gix::objs::tree::EntryKind::BlobExecutable
                } else {
                    gix::objs::tree::EntryKind::Blob
                };
                (mode, f.id.clone())
            }
            tree_value::Value::SymlinkId(sym_id) => {
                if !state.validated_symlinks.contains(sym_id) {
                    let target = backend_client
                        .read_symlink(ReadSymlinkRequest {
                            project_id: project_id.to_string(),
                            symlink_id: sym_id.clone(),
                        })
                        .await?
                        .into_inner()
                        .target;
                    if target.as_bytes().contains(&0) {
                        return Err(format!(
                            "ReadSymlink returned symlink target containing NUL byte for {}",
                            hex::encode(sym_id)
                        )
                        .into());
                    }
                    let computed = gix::objs::compute_hash(
                        gix::hash::Kind::Sha1,
                        gix::objs::Kind::Blob,
                        target.as_bytes(),
                    )?;
                    if computed.as_bytes() != sym_id.as_slice() {
                        return Err(format!(
                            "Exported Git symlink SHA-1 mismatch: expected {}, computed {computed}",
                            hex::encode(sym_id)
                        )
                        .into());
                    }
                    if !state.written_blobs.contains(sym_id) {
                        let written = {
                            let git_repo = thread_safe_repo.to_thread_local();
                            git_repo
                                .objects
                                .write_buf(gix::objs::Kind::Blob, target.as_bytes())?
                        };
                        if written.as_bytes() != sym_id.as_slice() {
                            return Err(format!(
                                "Exported Git symlink SHA-1 mismatch: expected {}, wrote {written}",
                                hex::encode(sym_id)
                            )
                            .into());
                        }
                        state.written_blobs.insert(sym_id.clone());
                    }
                    state.validated_symlinks.insert(sym_id.clone());
                }
                (gix::objs::tree::EntryKind::Link, sym_id.clone())
            }
            tree_value::Value::TreeId(sub_id) => {
                Box::pin(export_tree_recursive_with_max_file_bytes(
                    thread_safe_repo,
                    backend_client,
                    project_id,
                    sub_id,
                    state,
                    max_file_bytes,
                ))
                .await?;
                (gix::objs::tree::EntryKind::Tree, sub_id.clone())
            }
            tree_value::Value::GitSubmoduleId(submod_cid) => {
                (gix::objs::tree::EntryKind::Commit, submod_cid.clone())
            }
            tree_value::Value::ConflictId(_) => {
                return Err("Cannot export unresolved conflict entry to native Git tree".into());
            }
        };

        let oid = gix::hash::ObjectId::try_from(oid_bytes.as_slice())?;
        gix_entries.push(gix::objs::tree::Entry {
            mode: kind.into(),
            filename: entry.name.as_bytes().into(),
            oid,
        });
    }

    gix_entries.sort_unstable();
    let gix_tree = gix::objs::Tree {
        entries: gix_entries,
    };
    let mut buf = Vec::new();
    gix_tree.write_to(&mut buf)?;
    let computed_tree =
        gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Tree, &buf)?;
    if computed_tree.as_bytes() != tree_id {
        return Err(format!(
            "Exported Git tree SHA-1 mismatch: expected {}, computed {computed_tree}",
            hex::encode(tree_id)
        )
        .into());
    }
    let written_tree = {
        let git_repo = thread_safe_repo.to_thread_local();
        git_repo.objects.write_buf(gix::objs::Kind::Tree, &buf)?
    };
    if written_tree.as_bytes() != tree_id {
        return Err(format!(
            "Exported Git tree SHA-1 mismatch: expected {}, wrote {written_tree}",
            hex::encode(tree_id)
        )
        .into());
    }
    state.written_trees.insert(tree_id.to_vec());
    Ok(())
}
