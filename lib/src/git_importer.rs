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

use cc_common::ROOT_COMMIT_ID_BYTES;
use cc_common::backend::backend_service_client::BackendServiceClient;
use cc_common::backend::{
    Commit, File, Signature, Timestamp, TreeEntry, TreeValue, WriteCommitRequest, WriteFileRequest,
    WriteSymlinkRequest, WriteTreeRequest, tree_value,
};
use cc_common::op_store::op_store_service_client::OpStoreServiceClient;
use cc_common::op_store::{
    GetOpHeadsRequest, Operation, OperationMetadata, ReadOperationRequest, ReadViewRequest,
    RefTarget, RefTargetTerm, UpdateOpHeadsRequest, View, WriteOperationRequest, WriteViewRequest,
};
use futures::StreamExt;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

const CONCURRENT_BLOB_UPLOADS: usize = 32;
const EMPTY_STRING_PLACEHOLDER: &str = "JJ_EMPTY_STRING";

#[derive(Default)]
struct ImportCache {
    blobs: HashMap<gix::hash::ObjectId, Vec<u8>>,
    trees: HashMap<gix::hash::ObjectId, Vec<u8>>,
    symlinks: HashMap<gix::hash::ObjectId, Vec<u8>>,
}

fn normalize_git_signature_field(raw: &gix::bstr::BStr) -> String {
    let text = raw.to_string();
    if text == EMPTY_STRING_PLACEHOLDER {
        String::new()
    } else {
        text
    }
}

fn make_request<T>(payload: T) -> tonic::Request<T> {
    let mut req = tonic::Request::new(payload);
    if let Ok(token) = std::env::var("JJ_CC_AUTH_TOKEN") {
        let token = token.trim();
        if !token.is_empty() {
            if let Ok(val) = format!("Bearer {}", token).parse() {
                req.metadata_mut().insert("authorization", val);
            }
        }
    }
    req
}

pub struct GitImporter {
    pub git_dir: PathBuf,
    pub project_id: String,
    pub repo_id: String,
    pub server_url: String,
}

impl GitImporter {
    pub fn new(git_dir: PathBuf, project_id: String, repo_id: String, server_url: String) -> Self {
        Self {
            git_dir,
            project_id,
            repo_id,
            server_url,
        }
    }

    pub async fn run(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let dot_git = if self.git_dir.file_name() == Some(std::ffi::OsStr::new(".git")) {
            self.git_dir.clone()
        } else {
            self.git_dir.join(".git")
        };

        Self::normalize_head_branch(&dot_git);

        let opts = gix::open::Options::isolated();
        let thread_safe_repo = gix::ThreadSafeRepository::open_opts(&dot_git, opts.clone())
            .or_else(|_| gix::ThreadSafeRepository::open_opts(&self.git_dir, opts.clone()))
            .or_else(|_| gix::ThreadSafeRepository::open(&dot_git))
            .or_else(|_| gix::ThreadSafeRepository::open(&self.git_dir))
            .or_else(|_| gix::ThreadSafeRepository::discover(&self.git_dir))
            .map_err(|e| format!("Failed to open Git repository at {:?}: {e}", self.git_dir))?;

        let mut backend_client = BackendServiceClient::connect(self.server_url.clone())
            .await
            .map_err(|e| format!("Failed to connect to backend service: {e}"))?;

        let mut op_client = OpStoreServiceClient::connect(self.server_url.clone())
            .await
            .map_err(|e| format!("Failed to connect to op store service: {e}"))?;

        let project_id = self.project_id.clone();
        let repo_id = self.repo_id.clone();

        // Verify that target repo_id is already registered on the server
        op_client
            .get_op_heads(make_request(GetOpHeadsRequest {
                repo_id: repo_id.clone(),
            }))
            .await
            .map_err(|e| {
                format!(
                    "Target repository '{}' is not registered on the server: {e}",
                    repo_id
                )
            })?;

        // Ensure Jujutsu Root Commit exists on server in project CAS
        // Validate repository and project exist before importing any Git objects.
        let _ = backend_client
            .read_commit(make_request(cc_common::backend::ReadCommitRequest {
                project_id: project_id.clone(),
                commit_id: ROOT_COMMIT_ID_BYTES.to_vec(),
            }))
            .await?;

        // Discover HEAD commit IDs and collect commits & blobs to upload
        let mut cache = ImportCache::default();
        let (head_commit_ids, commits_to_process, blobs_to_upload) =
            self.collect_git_objects(&thread_safe_repo, &dot_git, &mut cache.blobs)?;

        // Concurrently upload file blobs to project CAS
        self.upload_blobs(
            &mut backend_client,
            &project_id,
            blobs_to_upload,
            &mut cache.blobs,
        )
        .await?;

        // Convert and upload commit graph to project CAS
        for commit_oid in commits_to_process {
            self.import_commit(
                &thread_safe_repo,
                commit_oid,
                &mut backend_client,
                &project_id,
                &mut cache,
            )
            .await?;
        }

        // Update Op Store View and Operations for repo_id
        self.publish_op_head(&mut op_client, &repo_id, &head_commit_ids)
            .await?;

        Ok(repo_id)
    }

    /// Normalizes HEAD ref if it points to an invalid branch name (.invalid from templates)
    fn normalize_head_branch(dot_git: &Path) {
        let head_path = dot_git.join("HEAD");
        if let Ok(head_str) = std::fs::read_to_string(&head_path) {
            if head_str.contains(".invalid") {
                let invalid_ref = dot_git.join("refs/heads/.invalid");
                let main_ref = dot_git.join("refs/heads/main");
                if invalid_ref.exists() && !main_ref.exists() {
                    let _ = std::fs::rename(&invalid_ref, &main_ref);
                }
                let _ = std::fs::write(&head_path, "ref: refs/heads/main\n");
            }
        }
    }

    /// Resolves head commit IDs and collects commits & blobs to process
    fn collect_git_objects(
        &self,
        thread_safe_repo: &gix::ThreadSafeRepository,
        dot_git: &Path,
        blob_map: &mut HashMap<gix::hash::ObjectId, Vec<u8>>,
    ) -> Result<
        (
            Vec<Vec<u8>>,
            Vec<gix::hash::ObjectId>,
            Vec<(gix::hash::ObjectId, Vec<u8>)>,
        ),
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let repo = thread_safe_repo.to_thread_local();
        let head_commit_ids = self.discover_head_commit_ids(&repo, dot_git);

        // Collect all reachable commits in post-order topological order (parents before children)
        let mut commits_to_process = Vec::new();
        let mut visit_state: HashMap<gix::hash::ObjectId, bool> = HashMap::new();

        for head in &head_commit_ids {
            let head_oid = gix::hash::ObjectId::try_from(head.as_slice())?;
            if visit_state.contains_key(&head_oid) {
                continue;
            }
            let mut stack = vec![(head_oid, false)];

            while let Some((oid, expanded)) = stack.pop() {
                if expanded {
                    if visit_state.insert(oid, true) != Some(true) {
                        commits_to_process.push(oid);
                    }
                    continue;
                }
                if visit_state.contains_key(&oid) {
                    continue;
                }
                visit_state.insert(oid, false);
                stack.push((oid, true));
                if let Ok(obj) = repo.find_object(oid) {
                    let commit_ref = obj.to_commit_ref();
                    let parents: Vec<_> = commit_ref.parents().collect();
                    for parent in parents.into_iter().rev() {
                        if !visit_state.contains_key(&parent) {
                            stack.push((parent, false));
                        }
                    }
                }
            }
        }

        // Collect all file blobs for parallel upload
        let mut blobs_to_upload = Vec::new();
        let mut seen_trees = HashSet::new();
        for commit_oid in &commits_to_process {
            let obj = repo.find_object(*commit_oid)?;
            let commit_ref = obj.to_commit_ref();
            let mut tree_stack = vec![commit_ref.tree()];
            if let Some(trees_hdr) = commit_ref.extra_headers().find("jj:trees") {
                for hex_id in trees_hdr.split(|b| *b == b' ') {
                    if let Some(raw_id) = jj_lib::hex_util::decode_hex(hex_id)
                        && let Ok(oid) = gix::hash::ObjectId::try_from(raw_id.as_slice())
                    {
                        tree_stack.push(oid);
                    }
                }
            }

            while let Some(tree_oid) = tree_stack.pop() {
                if !seen_trees.insert(tree_oid) {
                    continue;
                }
                if let Ok(tree_obj) = repo.find_object(tree_oid) {
                    if let Ok(tree_ref) =
                        gix::objs::TreeRef::from_bytes(&tree_obj.data, repo.object_hash())
                    {
                        for entry in tree_ref.entries {
                            if entry.mode.is_tree() {
                                tree_stack.push(entry.oid.to_owned());
                            } else if !entry.mode.is_link() && !entry.mode.is_commit() {
                                if !blob_map.contains_key(&entry.oid.to_owned()) {
                                    if let Ok(blob_obj) = repo.find_object(entry.oid) {
                                        blobs_to_upload
                                            .push((entry.oid.to_owned(), blob_obj.data.to_vec()));
                                        blob_map.insert(entry.oid.to_owned(), Vec::new());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok((head_commit_ids, commits_to_process, blobs_to_upload))
    }

    /// Discovers all head commit IDs across loose refs, packed refs, HEAD, and git CLI
    fn discover_head_commit_ids(&self, repo: &gix::Repository, dot_git: &Path) -> Vec<Vec<u8>> {
        let mut head_commit_ids = Vec::new();

        fn walk_refs(dir: &Path, out: &mut Vec<Vec<u8>>) {
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        walk_refs(&path, out);
                    } else if path.is_file() {
                        if let Ok(content) = std::fs::read_to_string(&path) {
                            if let Ok(oid) =
                                gix::hash::ObjectId::from_hex(content.trim().as_bytes())
                            {
                                let bytes = oid.as_bytes().to_vec();
                                if !out.contains(&bytes) {
                                    out.push(bytes);
                                }
                            }
                        }
                    }
                }
            }
        }

        walk_refs(&dot_git.join("refs"), &mut head_commit_ids);

        if let Ok(packed) = std::fs::read_to_string(dot_git.join("packed-refs")) {
            for line in packed.lines() {
                let line = line.trim();
                if !line.starts_with('#') && !line.starts_with('^') {
                    if let Some((hash, _name)) = line.split_once(' ') {
                        if let Ok(oid) = gix::hash::ObjectId::from_hex(hash.trim().as_bytes()) {
                            let bytes = oid.as_bytes().to_vec();
                            if !head_commit_ids.contains(&bytes) {
                                head_commit_ids.push(bytes);
                            }
                        }
                    }
                }
            }
        }

        if let Ok(head_str) = std::fs::read_to_string(dot_git.join("HEAD")) {
            let head_str = head_str.trim();
            if let Some(target_ref) = head_str.strip_prefix("ref: ") {
                let ref_path = dot_git.join(target_ref.trim());
                if let Ok(ref_str) = std::fs::read_to_string(ref_path) {
                    if let Ok(oid) = gix::hash::ObjectId::from_hex(ref_str.trim().as_bytes()) {
                        let bytes = oid.as_bytes().to_vec();
                        if !head_commit_ids.contains(&bytes) {
                            head_commit_ids.push(bytes);
                        }
                    }
                }
            } else if let Ok(oid) = gix::hash::ObjectId::from_hex(head_str.as_bytes()) {
                let bytes = oid.as_bytes().to_vec();
                if !head_commit_ids.contains(&bytes) {
                    head_commit_ids.push(bytes);
                }
            }
        }

        if head_commit_ids.is_empty() {
            if let Ok(head_commit) = repo.head_commit() {
                let bytes = head_commit.id.as_bytes().to_vec();
                if !head_commit_ids.contains(&bytes) {
                    head_commit_ids.push(bytes);
                }
            }
        }

        // Git CLI fallback (for reftable or custom backends)
        if head_commit_ids.is_empty() {
            if let Ok(output) = std::process::Command::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(&self.git_dir)
                .output()
            {
                if output.status.success() {
                    let hash_str = String::from_utf8_lossy(&output.stdout).trim().to_string();
                    if let Ok(oid) = gix::hash::ObjectId::from_hex(hash_str.as_bytes()) {
                        let bytes = oid.as_bytes().to_vec();
                        if !head_commit_ids.contains(&bytes) {
                            head_commit_ids.push(bytes);
                        }
                    }
                }
            }
        }

        if head_commit_ids.is_empty() {
            if let Ok(output) = std::process::Command::new("git")
                .args(["for-each-ref", "--format=%(objectname)"])
                .current_dir(&self.git_dir)
                .output()
            {
                if output.status.success() {
                    for line in String::from_utf8_lossy(&output.stdout).lines() {
                        let hash_str = line.trim();
                        if let Ok(oid) = gix::hash::ObjectId::from_hex(hash_str.as_bytes()) {
                            let bytes = oid.as_bytes().to_vec();
                            if !head_commit_ids.contains(&bytes) {
                                head_commit_ids.push(bytes);
                            }
                        }
                    }
                }
            }
        }

        head_commit_ids
    }

    /// Uploads file blobs concurrently using parallel workers over multiplexed HTTP/2 channel
    async fn upload_blobs(
        &self,
        backend_client: &mut BackendServiceClient<tonic::transport::Channel>,
        project_id: &str,
        blobs_to_upload: Vec<(gix::hash::ObjectId, Vec<u8>)>,
        blob_map: &mut HashMap<gix::hash::ObjectId, Vec<u8>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let upload_stream =
            futures::stream::iter(blobs_to_upload.into_iter().map(|(blob_oid, content)| {
                let mut client = backend_client.clone();
                let p_id = project_id.to_string();
                async move {
                    let write_res = client
                        .write_file(make_request(WriteFileRequest {
                            project_id: p_id,
                            content,
                        }))
                        .await?
                        .into_inner();
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>((blob_oid, write_res.file_id))
                }
            }))
            .buffer_unordered(CONCURRENT_BLOB_UPLOADS);

        let uploaded_blobs: Vec<Result<(gix::hash::ObjectId, Vec<u8>), _>> =
            upload_stream.collect().await;
        for res in uploaded_blobs {
            let (blob_oid, file_id) = res?;
            blob_map.insert(blob_oid, file_id);
        }
        Ok(())
    }

    /// Imports a single commit into Jujutsu Commit Cloud format
    async fn import_commit(
        &self,
        thread_safe_repo: &gix::ThreadSafeRepository,
        commit_oid: gix::hash::ObjectId,
        backend_client: &mut BackendServiceClient<tonic::transport::Channel>,
        project_id: &str,
        cache: &mut ImportCache,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (
            root_tree_oids,
            change_id,
            conflict_labels,
            author_sig_name,
            author_sig_email,
            author_time_millis,
            author_tz_offset,
            committer_sig_name,
            committer_sig_email,
            committer_time_millis,
            committer_tz_offset,
            mut parent_ids,
            message,
            raw_git_commit,
            secure_sig,
        ) = {
            let repo = thread_safe_repo.to_thread_local();
            let object = repo.find_object(commit_oid)?;
            let raw_bytes = object.data.to_vec();
            let commit_ref = object.to_commit_ref();
            let parsed_sig = gix::objs::CommitRefIter::signature(&raw_bytes, gix::hash::Kind::Sha1)
                .map_err(|e| format!("commit {commit_oid}: invalid signature: {e}"))?
                .map(|(sig, data)| cc_common::backend::SecureSig {
                    data: Vec::from(data.to_bstring()),
                    sig: sig.as_ref().to_vec(),
                });

            let change_id = commit_ref
                .extra_headers()
                .find("change-id")
                .and_then(jj_lib::hex_util::decode_reverse_hex)
                .filter(|id| id.len() == cc_common::CHANGE_ID_LENGTH)
                .unwrap_or_else(|| {
                    use jj_lib::object_id::ObjectId as _;
                    let jj_commit_id = jj_lib::backend::CommitId::from_bytes(commit_oid.as_bytes());
                    jj_lib::git_backend::synthetic_change_id_from_git_commit_id(&jj_commit_id)
                        .as_bytes()
                        .to_vec()
                });

            let root_tree_oids = match commit_ref.extra_headers().find("jj:trees") {
                Some(value) => value
                    .split(|b| *b == b' ')
                    .map(|hex| {
                        jj_lib::hex_util::decode_hex(hex)
                            .filter(|id| id.len() == cc_common::TREE_ID_LENGTH)
                            .and_then(|id| gix::hash::ObjectId::try_from(id.as_slice()).ok())
                    })
                    .collect::<Option<Vec<_>>>()
                    .filter(|ids| ids.len() > 1 && ids.len() % 2 == 1)
                    .ok_or_else(|| format!("commit {commit_oid}: invalid jj:trees header"))?,
                None => vec![commit_ref.tree()],
            };

            let conflict_labels = match commit_ref.extra_headers().find("jj:conflict-labels") {
                Some(value) => std::str::from_utf8(value)
                    .map_err(|_| format!("commit {commit_oid}: jj:conflict-labels is not UTF-8"))?
                    .split_terminator('\n')
                    .map(str::to_string)
                    .collect(),
                None => Vec::new(),
            };

            let author_sig = commit_ref
                .author()
                .map_err(|e| format!("commit {commit_oid}: invalid author: {e}"))?;
            let committer_sig = commit_ref
                .committer()
                .map_err(|e| format!("commit {commit_oid}: invalid committer: {e}"))?;
            let author_time = author_sig
                .time()
                .map_err(|e| format!("commit {commit_oid}: invalid author time: {e}"))?;
            let committer_time = committer_sig
                .time()
                .map_err(|e| format!("commit {commit_oid}: invalid committer time: {e}"))?;

            let parent_ids: Vec<Vec<u8>> = commit_ref
                .parents()
                .map(|p| p.as_bytes().to_vec())
                .collect();

            let author_time_millis = author_time
                .seconds
                .checked_mul(1000)
                .ok_or_else(|| format!("commit {commit_oid}: author timestamp overflow"))?;
            let author_tz_offset = author_time.offset.div_euclid(60);
            let committer_time_millis = committer_time
                .seconds
                .checked_mul(1000)
                .ok_or_else(|| format!("commit {commit_oid}: committer timestamp overflow"))?;
            let committer_tz_offset = committer_time.offset.div_euclid(60);

            (
                root_tree_oids,
                change_id,
                conflict_labels,
                normalize_git_signature_field(author_sig.name),
                normalize_git_signature_field(author_sig.email),
                author_time_millis,
                author_tz_offset,
                normalize_git_signature_field(committer_sig.name),
                normalize_git_signature_field(committer_sig.email),
                committer_time_millis,
                committer_tz_offset,
                parent_ids,
                commit_ref.message.to_string(),
                raw_bytes,
                parsed_sig,
            )
        };

        // Git root commits must attach to JJ's universal root commit
        if parent_ids.is_empty() {
            parent_ids.push(ROOT_COMMIT_ID_BYTES.to_vec());
        }

        if parent_ids.len() > 1
            && parent_ids
                .iter()
                .any(|p| p.as_slice() == ROOT_COMMIT_ID_BYTES)
        {
            return Err(
                "The Commit Cloud backend does not support creating merge commits with the root commit as one of the parents."
                    .into(),
            );
        }

        // Process all root tree terms (single tree or jj:trees conflict terms)
        let mut root_tree_ids = Vec::with_capacity(root_tree_oids.len());
        for tree_oid in root_tree_oids {
            let tid = self
                .process_tree(
                    thread_safe_repo,
                    tree_oid,
                    backend_client,
                    project_id,
                    cache,
                )
                .await?;
            root_tree_ids.push(tid);
        }

        let jj_commit = Commit {
            commit_id: commit_oid.as_bytes().to_vec(),
            change_id,
            parent_commit_ids: parent_ids,
            root_tree_id: root_tree_ids,
            description: message,
            author: Some(Signature {
                name: author_sig_name,
                email: author_sig_email,
                timestamp: Some(Timestamp {
                    millis_since_epoch: author_time_millis,
                    tz_offset: author_tz_offset,
                }),
            }),
            committer: Some(Signature {
                name: committer_sig_name,
                email: committer_sig_email,
                timestamp: Some(Timestamp {
                    millis_since_epoch: committer_time_millis,
                    tz_offset: committer_tz_offset,
                }),
            }),
            predecessors: Vec::new(),
            conflict_labels,
            secure_sig,
            raw_git_commit,
            external_ids: Default::default(),
        };

        backend_client
            .write_commit(make_request(WriteCommitRequest {
                project_id: project_id.to_string(),
                commit: Some(jj_commit),
            }))
            .await?;

        Ok(())
    }

    /// Recursively processes Git tree objects into Commit Cloud tree protos
    async fn process_tree(
        &self,
        thread_safe_repo: &gix::ThreadSafeRepository,
        tree_oid: gix::hash::ObjectId,
        backend_client: &mut BackendServiceClient<tonic::transport::Channel>,
        project_id: &str,
        cache: &mut ImportCache,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(existing) = cache.trees.get(&tree_oid) {
            return Ok(existing.clone());
        }

        let raw_entries = {
            let repo = thread_safe_repo.to_thread_local();
            let tree_obj = repo.find_object(tree_oid)?;
            let tree_ref = gix::objs::TreeRef::from_bytes(&tree_obj.data, repo.object_hash())
                .map_err(|e| format!("Failed to parse tree: {e}"))?;

            let mut raw = Vec::new();
            for entry in tree_ref.entries {
                let name = std::str::from_utf8(entry.filename.as_ref())
                    .map_err(|_| {
                        format!(
                            "tree {tree_oid}: entry filename {:?} is not valid UTF-8",
                            entry.filename
                        )
                    })?
                    .to_string();
                let entry_oid = entry.oid.to_owned();
                let is_tree = entry.mode.is_tree();
                let is_link = entry.mode.is_link();
                let is_commit = entry.mode.is_commit();
                let is_exec = entry.mode.is_executable();
                let needs_blob = (is_link
                    && cache.symlinks.get(&entry_oid).is_none_or(Vec::is_empty))
                    || (!is_tree
                        && !is_link
                        && !is_commit
                        && cache.blobs.get(&entry_oid).is_none_or(Vec::is_empty));
                let blob_data = if needs_blob {
                    let blob_obj = repo.find_object(entry_oid)?;
                    Some(blob_obj.data.to_vec())
                } else {
                    None
                };
                raw.push((
                    name, entry_oid, is_tree, is_link, is_commit, is_exec, blob_data,
                ));
            }
            raw
        };

        let mut entries = Vec::new();

        for (name, entry_oid, is_tree, is_link, is_commit, is_exec, blob_data) in raw_entries {
            let tree_value = if is_tree {
                let child_tree_id = Box::pin(self.process_tree(
                    thread_safe_repo,
                    entry_oid,
                    backend_client,
                    project_id,
                    cache,
                ))
                .await?;
                TreeValue {
                    value: Some(tree_value::Value::TreeId(child_tree_id)),
                }
            } else if is_commit {
                TreeValue {
                    value: Some(tree_value::Value::GitSubmoduleId(
                        entry_oid.as_bytes().to_vec(),
                    )),
                }
            } else if is_link {
                let symlink_id = if let Some(existing) =
                    cache.symlinks.get(&entry_oid).filter(|id| !id.is_empty())
                {
                    existing.clone()
                } else {
                    let raw_target = blob_data.ok_or_else(|| {
                        format!("missing symlink target blob for '{name}' ({entry_oid})")
                    })?;
                    let target = String::from_utf8(raw_target).map_err(|_| {
                        format!("symlink target for '{name}' ({entry_oid}) is not valid UTF-8")
                    })?;
                    let write_res = backend_client
                        .write_symlink(make_request(WriteSymlinkRequest {
                            project_id: project_id.to_string(),
                            target,
                        }))
                        .await?
                        .into_inner();
                    let written_id = write_res.symlink_id;
                    cache.symlinks.insert(entry_oid, written_id.clone());
                    written_id
                };
                TreeValue {
                    value: Some(tree_value::Value::SymlinkId(symlink_id)),
                }
            } else {
                let blob_id = if let Some(existing) =
                    cache.blobs.get(&entry_oid).filter(|id| !id.is_empty())
                {
                    existing.clone()
                } else {
                    let content = blob_data
                        .ok_or_else(|| format!("missing file blob for '{name}' ({entry_oid})"))?;
                    let write_res = backend_client
                        .write_file(make_request(WriteFileRequest {
                            project_id: project_id.to_string(),
                            content,
                        }))
                        .await?
                        .into_inner();
                    let written_id = write_res.file_id;
                    cache.blobs.insert(entry_oid, written_id.clone());
                    written_id
                };
                TreeValue {
                    value: Some(tree_value::Value::File(File {
                        id: blob_id,
                        executable: is_exec,
                        copy_id: Vec::new(),
                        size_bytes: None,
                    })),
                }
            };

            entries.push(TreeEntry {
                name,
                value: Some(tree_value),
            });
        }

        let write_tree_res = backend_client
            .write_tree(make_request(WriteTreeRequest {
                project_id: project_id.to_string(),
                path: "".to_string(),
                entries,
            }))
            .await?
            .into_inner();

        let written_tree_id = write_tree_res.tree_id;
        cache.trees.insert(tree_oid, written_tree_id.clone());

        Ok(written_tree_id)
    }

    /// Chains the imported commit history into the Op Store View and Operations
    async fn publish_op_head(
        &self,
        op_client: &mut OpStoreServiceClient<tonic::transport::Channel>,
        repo_id: &str,
        head_commit_ids: &[Vec<u8>],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Fetch existing op heads from server so we chain operations
        let op_heads_res = op_client
            .get_op_heads(make_request(GetOpHeadsRequest {
                repo_id: repo_id.to_string(),
            }))
            .await?
            .into_inner();
        let old_op_head_ids = op_heads_res.op_head_ids;

        let mut view = View {
            head_ids: Vec::new(),
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: HashMap::new(),
        };

        // If an operation head exists, inherit its existing view state
        for op_head in &old_op_head_ids {
            if let Ok(op_res) = op_client
                .read_operation(make_request(ReadOperationRequest {
                    repo_id: repo_id.to_string(),
                    operation_id: op_head.clone(),
                }))
                .await
            {
                if let Some(op) = op_res.into_inner().operation {
                    if let Ok(view_res) = op_client
                        .read_view(make_request(ReadViewRequest {
                            repo_id: repo_id.to_string(),
                            view_id: op.view_id,
                        }))
                        .await
                    {
                        if let Some(v) = view_res.into_inner().view {
                            view = v;
                            break;
                        }
                    }
                }
            }
        }

        // Add imported head commit IDs
        for head in head_commit_ids {
            if !view.head_ids.contains(head) {
                view.head_ids.push(head.clone());
            }
        }

        // Point 'main' bookmark to the imported primary head
        if let Some(primary_head) = head_commit_ids.first() {
            view.local_bookmarks.insert(
                "main".to_string(),
                RefTarget {
                    adds: vec![RefTargetTerm {
                        commit_id: primary_head.clone(),
                    }],
                    removes: Vec::new(),
                },
            );
        }

        let view_res = op_client
            .write_view(make_request(WriteViewRequest {
                repo_id: repo_id.to_string(),
                view: Some(view),
            }))
            .await?
            .into_inner();

        let operation = Operation {
            view_id: view_res.view_id,
            parents: old_op_head_ids.clone(),
            metadata: Some(OperationMetadata {
                start_time_millis: 0,
                end_time_millis: 0,
                description: "Imported from Git repository".to_string(),
                is_snapshot: false,
                workspace_name: None,
                hostname: "localhost".to_string(),
                username: "git-importer".to_string(),
                attributes: HashMap::new(),
            }),
            commit_predecessors: Vec::new(),
            commit_predecessors_set: false,
        };

        let op_res = op_client
            .write_operation(make_request(WriteOperationRequest {
                repo_id: repo_id.to_string(),
                operation: Some(operation),
            }))
            .await?
            .into_inner();

        let _ = op_client
            .update_op_heads(make_request(UpdateOpHeadsRequest {
                repo_id: repo_id.to_string(),
                old_op_head_ids,
                new_op_head_id: op_res.operation_id,
            }))
            .await?;

        Ok(())
    }
}
