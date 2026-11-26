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

//! Git-to-Commit-Cloud import orchestration, commit-graph traversal, and
//! concurrent blob, symlink, tree, and commit uploads.
//!
//! # Authority and Crash-Safety Ordering
//! `GitImporter` preflights reachable refs and commits in post-order topological
//! order (rejecting cycles), uploads blobs and symlinks first, writes directory
//! trees bottom-up, writes commits parents-before-children, and delegates final
//! `View` and `Operation` publication to `publish_imported_refs`.
//!
//! # Cross-Process and RPC Boundaries
//! Blob and symlink uploads pipeline up to `CONCURRENT_UPLOADS` streams over the
//! shared gRPC channel, streaming file blobs in `FILE_STREAM_CHUNK_SIZE` frames
//! with fallback to unary `WriteFile` when streaming is unimplemented.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::Ordering as AtomicOrdering;

use bytes::Bytes;
use cc_common::backend::{
    Commit, File, RegisterRepositoryRequest, TreeEntry, TreeValue, WriteCommitRequest,
    WriteFileChunk, WriteFileRequest, WriteSymlinkRequest, WriteTreeRequest, tree_value,
};
use cc_common::{EMPTY_TREE_ID_BYTES, FILE_STREAM_CHUNK_SIZE, ROOT_COMMIT_ID_BYTES};
use futures::StreamExt as _;
use jj_lib::backend::CommitId;
use jj_lib::git_backend::synthetic_change_id_from_git_commit_id;
use jj_lib::object_id::ObjectId as _;

use super::git_repo::{ReftableObjectsDir, resolve_dot_git_dir, verify_reftable_sha1};
use super::objects::{
    GitTreeEntryKind, ParsedGitCommit, parse_git_commit, validate_git_tree_mode,
    validate_tree_entry_name,
};
use super::publish::{publish_imported_refs, resolve_single_op_head};
use super::refs::discover_git_refs;
use super::{CONCURRENT_UPLOADS, GitImportOptions, GitImportReport, validate_optional_id};
use crate::util::{AuthBackendClient, connect_backend_client, connect_op_store_client};

pub struct GitImporter {
    opts: GitImportOptions,
}

impl GitImporter {
    pub fn new(opts: GitImportOptions) -> Self {
        Self { opts }
    }

    pub async fn run(&self) -> Result<GitImportReport, Box<dyn std::error::Error + Send + Sync>> {
        let requested_repo_id = validate_optional_id(self.opts.repo_id.as_deref(), "--repo-id")?;
        let requested_project_id =
            validate_optional_id(self.opts.project_id.as_deref(), "--project-id")?;

        let dot_git = resolve_dot_git_dir(&self.opts.git_dir);
        let reftable_objects = ReftableObjectsDir::create_if_reftable(&dot_git)?;
        if reftable_objects.is_some() {
            verify_reftable_sha1(&dot_git, &self.opts.git_dir).await?;
        }
        let open_dir = reftable_objects
            .as_ref()
            .map_or(dot_git.as_path(), |dir| dir.path.as_path());

        let open_opts = gix::open::Options::isolated();
        let thread_safe_repo = gix::ThreadSafeRepository::open_opts(open_dir, open_opts.clone())
            .or_else(|_| gix::ThreadSafeRepository::open_opts(&self.opts.git_dir, open_opts))
            .map_err(|e| {
                format!(
                    "Failed to open Git repository at {}: {e}",
                    self.opts.git_dir.display()
                )
            })?;
        let object_hash = thread_safe_repo.to_thread_local().object_hash();
        if object_hash != gix::hash::Kind::Sha1 {
            return Err(format!(
                "Unsupported Git object hash algorithm {object_hash:?} at {}: Commit Cloud supports SHA-1 Git repositories only",
                self.opts.git_dir.display()
            )
            .into());
        }

        // Collect and validate the full Git graph (refs, commit metadata/signatures, conflict
        // headers, trees, modes, object kinds, strict UTF-8 filenames, symlinks) before issuing
        // any write RPCs.
        let collected = collect_git_graph(
            &thread_safe_repo,
            &dot_git,
            &self.opts.git_dir,
            reftable_objects.is_some(),
        )
        .await?;

        let mut backend_client = connect_backend_client(self.opts.server_url.clone()).await?;
        let mut op_client = connect_op_store_client(self.opts.server_url.clone()).await?;

        let should_register = self.opts.create_repo || requested_repo_id.is_none();
        let (repo_id, project_id) = if should_register {
            let effective_repo_id =
                requested_repo_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let effective_project_id =
                requested_project_id.unwrap_or_else(|| effective_repo_id.clone());
            let res = backend_client
                .register_repository(RegisterRepositoryRequest {
                    project_id: effective_project_id,
                    repo_id: Some(effective_repo_id),
                    name: None,
                    hash_algorithm: cc_common::backend::HashAlgorithm::Sha1 as i32,
                })
                .await?
                .into_inner();
            (res.repo_id, res.project_id)
        } else {
            let existing_repo =
                requested_repo_id.expect("repo_id must be present when create_repo is false");
            if let Err(e) = resolve_single_op_head(&mut op_client, &existing_repo).await {
                if e.downcast_ref::<tonic::Status>()
                    .is_some_and(|s| s.code() == tonic::Code::NotFound)
                {
                    return Err(format!(
                        "Target repository '{existing_repo}' is not registered on {} (pass --create to register it): {e}",
                        self.opts.server_url
                    )
                    .into());
                }
                return Err(format!(
                    "Failed to resolve target repository '{existing_repo}' on {}: {e}",
                    self.opts.server_url
                )
                .into());
            }
            // `BackendService` has no read-only `GetRepository` RPC yet, so verify an explicit
            // `--project-id` binding through the idempotent `RegisterRepository` check; when
            // omitted, pass `repo_id` and let the server resolve its bound project namespace.
            let proj = if let Some(proj) = requested_project_id {
                backend_client
                    .register_repository(RegisterRepositoryRequest {
                        project_id: proj,
                        repo_id: Some(existing_repo.clone()),
                        name: None,
                        hash_algorithm: cc_common::backend::HashAlgorithm::Sha1 as i32,
                    })
                    .await
                    .map_err(|e| {
                        format!(
                            "Failed to verify project binding for repository '{existing_repo}': {e}"
                        )
                    })?
                    .into_inner()
                    .project_id
            } else {
                existing_repo.clone()
            };
            (existing_repo, proj)
        };

        // Upload blobs and symlinks before trees and commits so referenced objects exist before
        // publication.
        let blobs_imported = collected.blob_oids.len();
        upload_blobs_streaming(
            &thread_safe_repo,
            &backend_client,
            &project_id,
            collected.blob_oids,
        )
        .await?;

        let symlinks_imported = collected.symlinks.len();
        upload_symlinks(&backend_client, &project_id, collected.symlinks).await?;

        let mut tree_cache: HashSet<gix::hash::ObjectId> = HashSet::new();
        for commit_oid in &collected.topo_commits {
            import_single_commit(
                &thread_safe_repo,
                *commit_oid,
                &mut backend_client,
                &project_id,
                &mut tree_cache,
            )
            .await?;
        }

        let bookmarks_imported = if collected.topo_commits.is_empty()
            && collected.head_commit_ids.is_empty()
            && collected.local_branches.is_empty()
            && collected.remote_branches.is_empty()
        {
            0
        } else {
            publish_imported_refs(
                &mut op_client,
                Some((&mut backend_client, &project_id)),
                &repo_id,
                &collected.head_commit_ids,
                &collected.ancestor_commit_ids,
                &collected.commit_parents,
                &collected.local_branches,
                &collected.remote_branches,
            )
            .await?
        };

        Ok(GitImportReport {
            repo_id,
            project_id,
            commits_imported: collected.topo_commits.len(),
            trees_imported: tree_cache.len(),
            blobs_imported,
            symlinks_imported,
            bookmarks_imported,
        })
    }
}

pub(super) struct CollectedGitGraph {
    pub(super) head_commit_ids: Vec<Vec<u8>>,
    pub(super) ancestor_commit_ids: HashSet<Vec<u8>>,
    pub(super) commit_parents: HashMap<Vec<u8>, Vec<Vec<u8>>>,
    pub(super) local_branches: BTreeMap<String, Vec<u8>>,
    pub(super) remote_branches: BTreeMap<String, Vec<u8>>,
    pub(super) topo_commits: Vec<gix::hash::ObjectId>,
    pub(super) blob_oids: Vec<gix::hash::ObjectId>,
    pub(super) symlinks: Vec<(gix::hash::ObjectId, String)>,
}

pub(super) async fn collect_git_graph(
    thread_safe_repo: &gix::ThreadSafeRepository,
    dot_git: &Path,
    work_dir: &Path,
    is_reftable: bool,
) -> Result<CollectedGitGraph, Box<dyn std::error::Error + Send + Sync>> {
    let (head_commit_ids, local_branches, remote_branches) =
        discover_git_refs(thread_safe_repo, dot_git, work_dir, is_reftable).await?;
    let repo = thread_safe_repo.to_thread_local();

    // Post-order DFS so every parent strictly precedes all of its descendants, even on diamond
    // DAGs. Every parent seen here is a strict ancestor of at least one discovered head.
    let mut topo_commits = Vec::new();
    let mut visit_state: HashMap<gix::hash::ObjectId, u8> = HashMap::new();
    let mut non_head_commits: HashSet<gix::hash::ObjectId> = HashSet::new();
    let mut commit_parents: HashMap<Vec<u8>, Vec<Vec<u8>>> = HashMap::new();
    for head in &head_commit_ids {
        let head_oid = gix::hash::ObjectId::try_from(head.as_slice())?;
        let mut stack = vec![(head_oid, false)];
        while let Some((oid, expanded)) = stack.pop() {
            if expanded {
                if visit_state.get(&oid) != Some(&2) {
                    visit_state.insert(oid, 2);
                    topo_commits.push(oid);
                }
                continue;
            }
            match visit_state.get(&oid) {
                Some(&2) => continue,
                Some(&1) => {
                    return Err(
                        format!("Cycle detected in Git commit graph at commit {oid}").into(),
                    );
                }
                _ => {}
            }
            visit_state.insert(oid, 1);
            let obj = repo.find_object(oid)?;
            let commit_ref = obj
                .try_to_commit_ref()
                .map_err(|e| format!("Failed to decode Git commit {oid}: {e}"))?;
            stack.push((oid, true));
            let parents: Vec<gix::hash::ObjectId> = commit_ref.parents().collect();
            let parent_bytes: Vec<Vec<u8>> = if parents.is_empty() {
                vec![ROOT_COMMIT_ID_BYTES.to_vec()]
            } else {
                parents.iter().map(|p| p.as_bytes().to_vec()).collect()
            };
            commit_parents.insert(oid.as_bytes().to_vec(), parent_bytes);
            for p in parents.into_iter().rev() {
                non_head_commits.insert(p);
                match visit_state.get(&p) {
                    Some(&2) => {}
                    Some(&1) => {
                        return Err(
                            format!("Cycle detected in Git commit graph at commit {p}").into()
                        );
                    }
                    _ => stack.push((p, false)),
                }
            }
        }
    }

    // Preserve jj_lib's minimal-heads View invariant by pruning ancestors of other discovered heads.
    let head_commit_ids: Vec<Vec<u8>> = head_commit_ids
        .into_iter()
        .filter(|head| {
            gix::hash::ObjectId::try_from(head.as_slice())
                .is_ok_and(|oid| !non_head_commits.contains(&oid))
        })
        .collect();
    let ancestor_commit_ids: HashSet<Vec<u8>> = non_head_commits
        .into_iter()
        .map(|oid| oid.as_bytes().to_vec())
        .collect();

    let mut seen_blobs = HashSet::new();
    let mut seen_symlinks = HashSet::new();
    let mut tree_state: HashMap<gix::hash::ObjectId, u8> = HashMap::new();
    let mut blob_oids = Vec::new();
    let mut symlinks = Vec::new();

    for commit_oid in &topo_commits {
        let parsed = parse_git_commit(&repo, *commit_oid)?;
        let mut tree_stack = vec![(parsed.tree_oid, false)];
        tree_stack.extend(parsed.extra_tree_ids.into_iter().map(|oid| (oid, false)));
        while let Some((tree_oid, expanded)) = tree_stack.pop() {
            if expanded {
                tree_state.insert(tree_oid, 2);
                continue;
            }
            if tree_oid.as_bytes() == EMPTY_TREE_ID_BYTES {
                continue;
            }
            match tree_state.get(&tree_oid) {
                Some(&2) => continue,
                Some(&1) => {
                    return Err(
                        format!("Cycle detected in Git tree graph at tree {tree_oid}").into(),
                    );
                }
                _ => {}
            }
            tree_state.insert(tree_oid, 1);
            tree_stack.push((tree_oid, true));
            let tree_obj = repo.find_object(tree_oid)?;
            if tree_obj.kind != gix::object::Kind::Tree {
                return Err(format!(
                    "Expected Git tree for {tree_oid}, found {:?}",
                    tree_obj.kind
                )
                .into());
            }
            let tree_ref = gix::objs::TreeRef::from_bytes(&tree_obj.data, gix::hash::Kind::Sha1)
                .map_err(|e| format!("Failed to parse Git tree {tree_oid}: {e}"))?;
            let mut seen_names: HashSet<&str> = HashSet::with_capacity(tree_ref.entries.len());
            for entry in tree_ref.entries {
                let name = validate_tree_entry_name(tree_oid, entry.filename.as_ref())?;
                if !seen_names.insert(name) {
                    return Err(
                        format!("Duplicate filename {name:?} in Git tree {tree_oid}").into(),
                    );
                }
                let entry_oid = entry.oid.to_owned();
                match validate_git_tree_mode(tree_oid, name, entry.mode)? {
                    GitTreeEntryKind::Tree => {
                        tree_stack.push((entry_oid, false));
                    }
                    GitTreeEntryKind::Link => {
                        if seen_symlinks.insert(entry_oid) {
                            let link_obj = repo.find_object(entry_oid)?;
                            if link_obj.kind != gix::object::Kind::Blob {
                                return Err(format!(
                                    "Expected Git blob for symlink {entry_oid}, found {:?}",
                                    link_obj.kind
                                )
                                .into());
                            }
                            let target = std::str::from_utf8(&link_obj.data)
                                .map_err(|e| format!("Non-UTF8 Git symlink {entry_oid}: {e}"))?;
                            if target.as_bytes().contains(&0) {
                                return Err(format!(
                                    "Git symlink {entry_oid} target must not contain NUL bytes"
                                )
                                .into());
                            }
                            symlinks.push((entry_oid, target.to_string()));
                        }
                    }
                    GitTreeEntryKind::Blob { .. } => {
                        if seen_blobs.insert(entry_oid) {
                            let header = repo.find_header(entry_oid)?;
                            if header.kind() != gix::object::Kind::Blob {
                                return Err(format!(
                                    "Expected Git blob for {entry_oid}, found {:?}",
                                    header.kind()
                                )
                                .into());
                            }
                            blob_oids.push(entry_oid);
                        }
                    }
                    GitTreeEntryKind::Submodule => {}
                }
            }
        }
    }

    Ok(CollectedGitGraph {
        head_commit_ids,
        ancestor_commit_ids,
        commit_parents,
        local_branches,
        remote_branches,
        topo_commits,
        blob_oids,
        symlinks,
    })
}

async fn upload_blobs_streaming(
    thread_safe_repo: &gix::ThreadSafeRepository,
    backend_client: &AuthBackendClient,
    project_id: &str,
    blob_oids: Vec<gix::hash::ObjectId>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let prefer_unary = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut stream = futures::stream::iter(blob_oids.into_iter().map(|oid| {
        let mut client = backend_client.clone();
        let p_id = project_id.to_string();
        let ts_repo = thread_safe_repo.clone();
        let prefer_unary = std::sync::Arc::clone(&prefer_unary);
        async move {
            let data = tokio::task::spawn_blocking(
                move || -> Result<Bytes, Box<dyn std::error::Error + Send + Sync>> {
                    let repo = ts_repo.to_thread_local();
                    let obj = repo.find_object(oid)?;
                    if obj.kind != gix::object::Kind::Blob {
                        return Err(
                            format!("Expected Git blob for {oid}, found {:?}", obj.kind).into()
                        );
                    }
                    Ok(Bytes::from(obj.detach().data))
                },
            )
            .await
            .map_err(|e| format!("Failed to join Git blob reader task for {oid}: {e}"))??;
            let res = if prefer_unary.load(AtomicOrdering::Relaxed) {
                client
                    .write_file(WriteFileRequest {
                        project_id: p_id,
                        content: data.into(),
                    })
                    .await?
                    .into_inner()
            } else {
                let total_len = data.len();
                let chunk_count = if total_len == 0 {
                    1
                } else {
                    total_len.div_ceil(FILE_STREAM_CHUNK_SIZE)
                };
                let stream_data = data.clone();
                let stream_project_id = p_id.clone();
                let expected_oid = oid.as_bytes().to_vec();
                let frames = futures::stream::iter(0..chunk_count).map(move |idx| {
                    let start = idx * FILE_STREAM_CHUNK_SIZE;
                    let end = (start + FILE_STREAM_CHUNK_SIZE).min(total_len);
                    WriteFileChunk {
                        project_id: if idx == 0 {
                            stream_project_id.clone()
                        } else {
                            String::new()
                        },
                        chunk: stream_data.slice(start..end).to_vec(),
                        total_size: (idx == 0).then_some(total_len as u64),
                        expected_file_id: (idx == 0).then(|| expected_oid.clone()),
                    }
                });
                match client.write_file_stream(frames).await {
                    Ok(r) => r.into_inner(),
                    Err(status) if status.code() == tonic::Code::Unimplemented => {
                        prefer_unary.store(true, AtomicOrdering::Relaxed);
                        client
                            .write_file(WriteFileRequest {
                                project_id: p_id,
                                content: data.into(),
                            })
                            .await?
                            .into_inner()
                    }
                    Err(status) => return Err(status.into()),
                }
            };
            if res.file_id.as_slice() != oid.as_bytes() {
                return Err::<(), Box<dyn std::error::Error + Send + Sync>>(
                    format!(
                        "Git blob SHA-1 mismatch: expected {oid}, server computed {}",
                        hex::encode(&res.file_id)
                    )
                    .into(),
                );
            }
            Ok(())
        }
    }))
    .buffer_unordered(CONCURRENT_UPLOADS);

    while let Some(res) = stream.next().await {
        res?;
    }
    Ok(())
}

async fn upload_symlinks(
    backend_client: &AuthBackendClient,
    project_id: &str,
    symlinks: Vec<(gix::hash::ObjectId, String)>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut stream = futures::stream::iter(symlinks.into_iter().map(|(oid, target)| {
        let mut client = backend_client.clone();
        let p_id = project_id.to_string();
        async move {
            let res = client
                .write_symlink(WriteSymlinkRequest {
                    project_id: p_id,
                    target,
                })
                .await?
                .into_inner();
            if res.symlink_id.as_slice() != oid.as_bytes() {
                return Err::<(), Box<dyn std::error::Error + Send + Sync>>(
                    format!(
                        "Git symlink SHA-1 mismatch: expected {oid}, server computed {}",
                        hex::encode(&res.symlink_id)
                    )
                    .into(),
                );
            }
            Ok(())
        }
    }))
    .buffer_unordered(CONCURRENT_UPLOADS);

    while let Some(res) = stream.next().await {
        res?;
    }
    Ok(())
}

pub(super) async fn import_tree_recursive(
    thread_safe_repo: &gix::ThreadSafeRepository,
    tree_oid: gix::hash::ObjectId,
    backend_client: &mut AuthBackendClient,
    project_id: &str,
    tree_cache: &mut HashSet<gix::hash::ObjectId>,
    in_progress: &mut HashSet<gix::hash::ObjectId>,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    if tree_oid.as_bytes() == EMPTY_TREE_ID_BYTES {
        return Ok(EMPTY_TREE_ID_BYTES.to_vec());
    }
    if tree_cache.contains(&tree_oid) {
        return Ok(tree_oid.as_bytes().to_vec());
    }
    if !in_progress.insert(tree_oid) {
        return Err(format!("Cycle detected in Git tree graph at tree {tree_oid}").into());
    }
    let result = import_tree_recursive_inner(
        thread_safe_repo,
        tree_oid,
        backend_client,
        project_id,
        tree_cache,
        in_progress,
    )
    .await;
    in_progress.remove(&tree_oid);
    result
}

async fn import_tree_recursive_inner(
    thread_safe_repo: &gix::ThreadSafeRepository,
    tree_oid: gix::hash::ObjectId,
    backend_client: &mut AuthBackendClient,
    project_id: &str,
    tree_cache: &mut HashSet<gix::hash::ObjectId>,
    in_progress: &mut HashSet<gix::hash::ObjectId>,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let raw_entries = {
        let repo = thread_safe_repo.to_thread_local();
        let tree_obj = repo.find_object(tree_oid)?;
        if tree_obj.kind != gix::object::Kind::Tree {
            return Err(format!(
                "Expected Git tree for {tree_oid}, found {:?}",
                tree_obj.kind
            )
            .into());
        }
        let tree_ref = gix::objs::TreeRef::from_bytes(&tree_obj.data, gix::hash::Kind::Sha1)
            .map_err(|e| format!("Failed to parse Git tree {tree_oid}: {e}"))?;
        let mut seen_names: HashSet<&str> = HashSet::with_capacity(tree_ref.entries.len());
        tree_ref
            .entries
            .into_iter()
            .map(|e| {
                let name = validate_tree_entry_name(tree_oid, e.filename.as_ref())?;
                if !seen_names.insert(name) {
                    return Err(
                        format!("Duplicate filename {name:?} in Git tree {tree_oid}").into(),
                    );
                }
                let kind = validate_git_tree_mode(tree_oid, name, e.mode)?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
                    name.to_string(),
                    e.oid.to_owned(),
                    kind,
                ))
            })
            .collect::<Result<Vec<_>, _>>()?
    };

    let mut proto_entries = Vec::with_capacity(raw_entries.len());
    for (name, child_oid, kind) in raw_entries {
        let val = match kind {
            GitTreeEntryKind::Tree => {
                let child_bytes = Box::pin(import_tree_recursive(
                    thread_safe_repo,
                    child_oid,
                    backend_client,
                    project_id,
                    tree_cache,
                    in_progress,
                ))
                .await?;
                tree_value::Value::TreeId(child_bytes)
            }
            GitTreeEntryKind::Link => tree_value::Value::SymlinkId(child_oid.as_bytes().to_vec()),
            GitTreeEntryKind::Submodule => {
                tree_value::Value::GitSubmoduleId(child_oid.as_bytes().to_vec())
            }
            GitTreeEntryKind::Blob { executable } => tree_value::Value::File(File {
                id: child_oid.as_bytes().to_vec(),
                executable,
                copy_id: Vec::new(),
                size_bytes: None,
            }),
        };
        proto_entries.push(TreeEntry {
            name,
            value: Some(TreeValue { value: Some(val) }),
        });
    }

    let res = backend_client
        .write_tree(WriteTreeRequest {
            project_id: project_id.to_string(),
            path: String::new(),
            entries: proto_entries,
        })
        .await?
        .into_inner();

    if res.tree_id.as_slice() != tree_oid.as_bytes() {
        return Err(format!(
            "Git tree SHA-1 mismatch: expected {tree_oid}, server computed {}",
            hex::encode(&res.tree_id)
        )
        .into());
    }
    tree_cache.insert(tree_oid);
    Ok(res.tree_id)
}

async fn import_single_commit(
    thread_safe_repo: &gix::ThreadSafeRepository,
    commit_oid: gix::hash::ObjectId,
    backend_client: &mut AuthBackendClient,
    project_id: &str,
    tree_cache: &mut HashSet<gix::hash::ObjectId>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ParsedGitCommit {
        raw_commit_bytes,
        tree_oid,
        mut parent_ids,
        author,
        committer,
        message,
        parsed_change_id,
        extra_tree_ids,
        conflict_labels,
        secure_sig,
    } = {
        let repo = thread_safe_repo.to_thread_local();
        parse_git_commit(&repo, commit_oid)?
    };

    if parent_ids.is_empty() {
        parent_ids.push(ROOT_COMMIT_ID_BYTES.to_vec());
    }

    let mut in_progress_trees: HashSet<gix::hash::ObjectId> = HashSet::new();
    let root_tree_ids = if extra_tree_ids.len() > 1 {
        let mut ids = Vec::with_capacity(extra_tree_ids.len());
        for tid in &extra_tree_ids {
            ids.push(
                import_tree_recursive(
                    thread_safe_repo,
                    *tid,
                    backend_client,
                    project_id,
                    tree_cache,
                    &mut in_progress_trees,
                )
                .await?,
            );
        }
        // Upload the synthetic `.jjconflict` tree pointed to by the Git commit's `tree` header
        // so round-tripping `raw_git_commit` back to Git passes `git fsck --full`.
        if !extra_tree_ids.contains(&tree_oid) {
            import_tree_recursive(
                thread_safe_repo,
                tree_oid,
                backend_client,
                project_id,
                tree_cache,
                &mut in_progress_trees,
            )
            .await?;
        }
        ids
    } else {
        vec![
            import_tree_recursive(
                thread_safe_repo,
                tree_oid,
                backend_client,
                project_id,
                tree_cache,
                &mut in_progress_trees,
            )
            .await?,
        ]
    };

    let change_id = parsed_change_id.unwrap_or_else(|| {
        synthetic_change_id_from_git_commit_id(&CommitId::from_bytes(commit_oid.as_bytes()))
            .to_bytes()
    });

    let mut external_ids = HashMap::new();
    external_ids.insert("git-sha1".to_string(), commit_oid.as_bytes().to_vec());

    let proto_commit = Commit {
        commit_id: commit_oid.as_bytes().to_vec(),
        change_id,
        parent_commit_ids: parent_ids,
        root_tree_id: root_tree_ids,
        description: message,
        author: Some(author),
        committer: Some(committer),
        predecessors: Vec::new(),
        conflict_labels,
        secure_sig,
        raw_git_commit: raw_commit_bytes,
        external_ids,
    };

    let res = backend_client
        .write_commit(WriteCommitRequest {
            project_id: project_id.to_string(),
            commit: Some(proto_commit),
        })
        .await?
        .into_inner();
    if res.commit_id.as_slice() != commit_oid.as_bytes() {
        return Err(format!(
            "Git commit SHA-1 mismatch: expected {commit_oid}, server returned {}",
            hex::encode(&res.commit_id)
        )
        .into());
    }
    Ok(())
}
