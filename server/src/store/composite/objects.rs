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

//! Object persistence methods on `CompositeStore` for files, symlinks, trees, and commits.
//!
//! # Authority and Crash-Safety Ordering
//! `put_file` and `put_file_from_temp_file` persist out-of-line payloads to `BlobStore`
//! before writing `BlobRecord` rows into `MetadataStore`. `put_tree` and `put_commit`
//! verify referenced blobs, symlinks, subtrees, and parent commits before publishing.
//!
//! # Concurrency and Merge Invariants
//! Content-addressed objects are immutable and idempotent; tree entries are enriched with
//! authoritative blob sizes and stored in deterministic UTF-8 name order.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use cc_common::backend::*;
use tokio::sync::mpsc;

use super::{CompositeStore, MAX_TREE_PAGE_SIZE};
use crate::store::{
    BlobRecord, ChangePrefixResolution, CommitId, IdPrefixResolution, MAX_WATERMARK_BYTES,
    StoreError, StoreResult, expected_range_len, hex_prefix_byte_bounds,
    normalize_change_id_prefix_to_hex, read_bounded_file, slice_blob_range, stream_inline_blob,
};

impl CompositeStore {
    pub async fn put_file(
        &self,
        namespace_id: &str,
        file_id: &[u8],
        content: Bytes,
    ) -> StoreResult<()> {
        let project_id = self
            .resolve_project_namespace(namespace_id, "writing files")
            .await?;

        let content_size = content.len() as u64;
        if content.is_empty() || content.len() <= self.config.inline_blob_threshold {
            let record = BlobRecord {
                blob_id: file_id.to_vec(),
                content_size,
                inline_content: Some(content),
                primary_location: None,
                replicated_locations: Vec::new(),
            };
            self.metadata.put_blob_record(&project_id, &record).await?;
            return Ok(());
        }

        let local_location = self.blobs.location_id().to_string();
        // Write-before-publish: persist bytes in BlobStore before exposing BlobRecord in
        // MetadataStore. `MetadataStore::put_blob_record` atomically merges `primary_location`
        // and `replicated_locations` with any existing global row.
        self.blobs.put_blob(file_id, content.clone()).await?;

        let record = BlobRecord {
            blob_id: file_id.to_vec(),
            content_size,
            inline_content: None,
            primary_location: Some(local_location.clone()),
            replicated_locations: vec![local_location],
        };
        self.metadata.put_blob_record(&project_id, &record).await?;
        self.blobs.notify_blob_published(file_id, content).await;
        Ok(())
    }

    pub async fn put_file_from_temp_file(
        &self,
        namespace_id: &str,
        file_id: &[u8],
        temp_path: &Path,
        content_size: u64,
    ) -> StoreResult<()> {
        let project_id = self
            .resolve_project_namespace(namespace_id, "writing files")
            .await?;

        if content_size == 0 {
            let content = read_bounded_file(temp_path, 0).await?;
            let record = BlobRecord {
                blob_id: file_id.to_vec(),
                content_size: 0,
                inline_content: Some(content),
                primary_location: None,
                replicated_locations: Vec::new(),
            };
            self.metadata.put_blob_record(&project_id, &record).await?;
            return Ok(());
        }
        if content_size <= self.config.inline_blob_threshold as u64
            && content_size <= self.stream_memory_budget_bytes as u64
            && let Ok(permits) = u32::try_from(content_size)
        {
            // Guard materializing spilled inline-eligible files against the same global memory
            // budget that triggered spilling; payloads larger than the total budget fall through
            // to external BlobStore streaming instead of blocking indefinitely.
            let _permit = self
                .stream_memory_budget
                .clone()
                .acquire_many_owned(permits)
                .await
                .map_err(|_| {
                    StoreError::ResourceExhausted("stream memory budget closed".to_string())
                })?;
            let content = read_bounded_file(temp_path, content_size).await?;
            let record = BlobRecord {
                blob_id: file_id.to_vec(),
                content_size,
                inline_content: Some(content),
                primary_location: None,
                replicated_locations: Vec::new(),
            };
            self.metadata.put_blob_record(&project_id, &record).await?;
            return Ok(());
        }

        let local_location = self.blobs.location_id().to_string();
        // Write-before-publish: persist bytes in BlobStore before exposing BlobRecord in
        // MetadataStore. `MetadataStore::put_blob_record` atomically merges `primary_location`
        // and `replicated_locations` with any existing global row.
        self.blobs
            .put_blob_from_file(file_id, temp_path, content_size)
            .await?;

        let record = BlobRecord {
            blob_id: file_id.to_vec(),
            content_size,
            inline_content: None,
            primary_location: Some(local_location.clone()),
            replicated_locations: vec![local_location],
        };
        self.metadata.put_blob_record(&project_id, &record).await?;
        self.blobs
            .notify_blob_file_published(file_id, content_size)
            .await;
        Ok(())
    }

    pub async fn get_file_range(
        &self,
        namespace_id: &str,
        file_id: &[u8],
        offset: u64,
        max_bytes: Option<u64>,
    ) -> StoreResult<Bytes> {
        let project_id = self
            .resolve_project_namespace(namespace_id, "reading files")
            .await?;

        let record = self
            .metadata
            .get_blob_record(&project_id, file_id)
            .await?
            .ok_or_else(|| StoreError::ObjectNotFound {
                message: "file should have been present in cloud database".to_string(),
            })?;

        if offset > record.content_size {
            return Err(StoreError::OutOfRange(format!(
                "requested read at offset {offset}, file size is {}",
                record.content_size
            )));
        }
        let expected_len = expected_range_len(record.content_size, offset, max_bytes);

        if let Some(inline) = &record.inline_content {
            if inline.len() as u64 != record.content_size {
                return Err(StoreError::Corruption(format!(
                    "inline blob payload length {} does not match metadata content_size {} for file {}",
                    inline.len(),
                    record.content_size,
                    hex::encode(file_id)
                )));
            }
            return slice_blob_range(inline, offset, max_bytes);
        }

        let bytes = match self
            .blobs
            .get_blob_range_with_record(&record, offset, max_bytes)
            .await
        {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                return Err(StoreError::Corruption(format!(
                    "blob payload missing in object store for file {}",
                    hex::encode(file_id)
                )));
            }
            Err(StoreError::OutOfRange(msg)) => {
                return Err(StoreError::Corruption(format!(
                    "blob payload truncated in object store for file {}: {msg}",
                    hex::encode(file_id)
                )));
            }
            Err(err) => return Err(err),
        };
        if bytes.len() as u64 != expected_len {
            return Err(StoreError::Corruption(format!(
                "blob payload length mismatch for file {}: expected {expected_len} bytes at offset {offset}, got {} bytes",
                hex::encode(file_id),
                bytes.len()
            )));
        }
        Ok(bytes)
    }

    pub async fn get_file_range_stream(
        &self,
        namespace_id: &str,
        file_id: &[u8],
        offset: u64,
        max_bytes: Option<u64>,
    ) -> StoreResult<mpsc::Receiver<StoreResult<Bytes>>> {
        let project_id = self
            .resolve_project_namespace(namespace_id, "reading files")
            .await?;

        let record = self
            .metadata
            .get_blob_record(&project_id, file_id)
            .await?
            .ok_or_else(|| StoreError::ObjectNotFound {
                message: "file should have been present in cloud database".to_string(),
            })?;

        if offset > record.content_size {
            return Err(StoreError::OutOfRange(format!(
                "requested read at offset {offset}, file size is {}",
                record.content_size
            )));
        }
        if let Some(inline) = record.inline_content {
            return stream_inline_blob(file_id, record.content_size, inline, offset, max_bytes)?
                .ok_or_else(|| {
                    StoreError::Corruption(format!(
                        "inline blob stream missing for file {}",
                        hex::encode(file_id)
                    ))
                });
        }

        Arc::clone(&self.blobs)
            .get_blob_range_stream(record, offset, max_bytes)
            .await?
            .ok_or_else(|| {
                StoreError::Corruption(format!(
                    "blob payload missing in object store for file {}",
                    hex::encode(file_id)
                ))
            })
    }

    pub async fn put_symlink(
        &self,
        namespace_id: &str,
        symlink_id: &[u8],
        target: &str,
    ) -> StoreResult<()> {
        if target.as_bytes().contains(&0) {
            return Err(StoreError::InvalidArgument(
                "symlink target must not contain NUL bytes".to_string(),
            ));
        }
        let project_id = self
            .resolve_project_namespace(namespace_id, "writing symlinks")
            .await?;
        self.metadata
            .put_symlink(&project_id, symlink_id, target)
            .await
    }

    pub async fn get_symlink(&self, namespace_id: &str, symlink_id: &[u8]) -> StoreResult<String> {
        let project_id = self
            .resolve_project_namespace(namespace_id, "reading symlinks")
            .await?;
        let target = self
            .metadata
            .get_symlink(&project_id, symlink_id)
            .await?
            .ok_or_else(|| StoreError::ObjectNotFound {
                message: "symlink should have been present in cloud database".to_string(),
            })?;
        if target.as_bytes().contains(&0) {
            return Err(StoreError::Corruption(
                "stored symlink target contains NUL byte".to_string(),
            ));
        }
        Ok(target)
    }

    pub async fn put_tree(
        &self,
        namespace_id: &str,
        tree_id: &[u8],
        entries: &[TreeEntry],
    ) -> StoreResult<()> {
        let project_id = self
            .resolve_project_namespace(namespace_id, "writing trees")
            .await?;

        let mut unique_file_ids: Vec<Vec<u8>> = Vec::new();
        let mut seen_files: HashSet<&[u8]> = HashSet::new();
        let mut unique_symlink_ids: Vec<Vec<u8>> = Vec::new();
        let mut seen_symlinks: HashSet<&[u8]> = HashSet::new();
        let mut unique_subtree_ids: Vec<Vec<u8>> = Vec::new();
        let mut seen_subtrees: HashSet<&[u8]> = HashSet::new();

        for entry in entries {
            if let Some(val) = entry.value.as_ref().and_then(|v| v.value.as_ref()) {
                match val {
                    tree_value::Value::File(f) => {
                        if seen_files.insert(f.id.as_slice()) {
                            unique_file_ids.push(f.id.clone());
                        }
                    }
                    tree_value::Value::SymlinkId(id) => {
                        if seen_symlinks.insert(id.as_slice()) {
                            unique_symlink_ids.push(id.clone());
                        }
                    }
                    tree_value::Value::TreeId(id) => {
                        if id.as_slice() != cc_common::EMPTY_TREE_ID_BYTES
                            && seen_subtrees.insert(id.as_slice())
                        {
                            unique_subtree_ids.push(id.clone());
                        }
                    }
                    tree_value::Value::ConflictId(_) | tree_value::Value::GitSubmoduleId(_) => {}
                }
            }
        }

        // Trees are immutable: keep the entries of an existing `tree_id` instead of rewriting
        // them. The same batched lookup checks subtree dependencies.
        let mut lookup_tree_ids = vec![tree_id.to_vec()];
        if self.config.validate_dependencies {
            lookup_tree_ids.extend(unique_subtree_ids.iter().cloned());
        }
        let existing_trees = self
            .metadata
            .find_existing_trees(&project_id, &lookup_tree_ids)
            .await?;
        if existing_trees.contains(tree_id) {
            return Ok(());
        }

        let blob_sizes = if unique_file_ids.is_empty() {
            HashMap::new()
        } else {
            self.metadata
                .get_blob_sizes_batch(&project_id, &unique_file_ids)
                .await?
        };

        if self.config.validate_dependencies {
            for fid in &unique_file_ids {
                if !blob_sizes.contains_key(fid.as_slice()) {
                    return Err(StoreError::MissingDependency {
                        parent_kind: "tree",
                        missing_kind: "file",
                        id_hex: hex::encode(fid),
                    });
                }
            }
            if !unique_symlink_ids.is_empty() {
                let found = self
                    .metadata
                    .find_existing_symlinks(&project_id, &unique_symlink_ids)
                    .await?;
                for sid in &unique_symlink_ids {
                    if !found.contains(sid.as_slice()) {
                        return Err(StoreError::MissingDependency {
                            parent_kind: "tree",
                            missing_kind: "symlink",
                            id_hex: hex::encode(sid),
                        });
                    }
                }
            }
            for tid in &unique_subtree_ids {
                if !existing_trees.contains(tid.as_slice()) {
                    return Err(StoreError::MissingDependency {
                        parent_kind: "tree",
                        missing_kind: "subtree",
                        id_hex: hex::encode(tid),
                    });
                }
            }
        }

        // Enrich File entries with uncompressed byte lengths and sort strictly in UTF-8
        // RepoPathComponent order (`"foo" < "foo.rs"`) so `jj_lib::Tree::from_sorted_entries`
        // binary searches never encounter Git's trailing-slash directory order.
        let mut normalized: Vec<TreeEntry> = entries
            .iter()
            .map(|e| {
                let mut cloned = e.clone();
                if let Some(tree_value::Value::File(f)) =
                    cloned.value.as_mut().and_then(|v| v.value.as_mut())
                    && let Some(&sz) = blob_sizes.get(f.id.as_slice())
                {
                    f.size_bytes = Some(sz);
                }
                cloned
            })
            .collect();
        normalized.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));

        self.metadata
            .put_tree(&project_id, tree_id, &normalized)
            .await
    }

    pub async fn find_existing_trees(
        &self,
        namespace_id: &str,
        tree_ids: &[Vec<u8>],
    ) -> StoreResult<HashSet<Vec<u8>>> {
        let project_id = self
            .resolve_project_namespace(namespace_id, "checking trees")
            .await?;
        let mut non_empty = Vec::with_capacity(tree_ids.len());
        let mut has_empty = false;
        for tid in tree_ids {
            if tid.as_slice() == cc_common::EMPTY_TREE_ID_BYTES {
                has_empty = true;
            } else {
                non_empty.push(tid.clone());
            }
        }
        let mut found = if non_empty.is_empty() {
            HashSet::new()
        } else {
            self.metadata
                .find_existing_trees(&project_id, &non_empty)
                .await?
        };
        if has_empty {
            found.insert(cc_common::EMPTY_TREE_ID_BYTES.to_vec());
        }
        Ok(found)
    }

    pub async fn get_tree(
        &self,
        namespace_id: &str,
        tree_id: &[u8],
    ) -> StoreResult<Vec<TreeEntry>> {
        let project_id = self
            .resolve_project_namespace(namespace_id, "requesting trees")
            .await?;
        if tree_id == cc_common::EMPTY_TREE_ID_BYTES {
            return Ok(Vec::new());
        }
        self.metadata
            .get_tree(&project_id, tree_id)
            .await?
            .ok_or_else(|| StoreError::ObjectNotFound {
                message: "tree should have been present in cloud database".to_string(),
            })
    }

    pub async fn get_tree_page(
        &self,
        namespace_id: &str,
        tree_id: &[u8],
        page_token: &str,
        page_size: u32,
    ) -> StoreResult<(Vec<TreeEntry>, String)> {
        self.get_tree_page_with_cap(
            namespace_id,
            tree_id,
            page_token,
            page_size,
            MAX_TREE_PAGE_SIZE,
        )
        .await
    }

    pub(in crate::store) async fn get_tree_page_with_cap(
        &self,
        namespace_id: &str,
        tree_id: &[u8],
        page_token: &str,
        page_size: u32,
        max_page_size: usize,
    ) -> StoreResult<(Vec<TreeEntry>, String)> {
        let project_id = self
            .resolve_project_namespace(namespace_id, "requesting trees")
            .await?;
        if tree_id == cc_common::EMPTY_TREE_ID_BYTES {
            return Ok((Vec::new(), String::new()));
        }

        let max_cap = max_page_size.max(1);
        let effective_limit = if page_size == 0 {
            max_cap
        } else {
            (page_size as usize).min(max_cap)
        };
        let after = (!page_token.is_empty()).then_some(page_token);
        let mut items = self
            .metadata
            .get_tree_page(
                &project_id,
                tree_id,
                after,
                effective_limit.saturating_add(1),
            )
            .await?
            .ok_or_else(|| StoreError::ObjectNotFound {
                message: "tree should have been present in cloud database".to_string(),
            })?;

        if items.len() > effective_limit {
            items.truncate(effective_limit);
            let next_token = items.last().map(|e| e.name.clone()).unwrap_or_default();
            Ok((items, next_token))
        } else {
            Ok((items, String::new()))
        }
    }

    pub async fn put_commit(
        &self,
        namespace_id: &str,
        commit_id: &[u8],
        commit: &Commit,
    ) -> StoreResult<()> {
        let project_id = self
            .resolve_project_namespace(namespace_id, "requesting commits")
            .await?;

        let mut seen_parents: HashSet<&[u8]> = HashSet::new();
        let parent_ids: Vec<Vec<u8>> = commit
            .parent_commit_ids
            .iter()
            .filter(|id| {
                id.as_slice() != cc_common::ROOT_COMMIT_ID_BYTES
                    && seen_parents.insert(id.as_slice())
            })
            .cloned()
            .collect();
        let parent_generations = if parent_ids.is_empty() {
            HashMap::new()
        } else {
            self.metadata
                .get_commit_generations(&project_id, &parent_ids)
                .await?
        };
        let mut max_parent_gen = 0u64;
        for parent_id in &parent_ids {
            match parent_generations.get(parent_id.as_slice()) {
                Some(&gen_num) => {
                    max_parent_gen = max_parent_gen.max(gen_num);
                }
                None if self.config.validate_dependencies => {
                    return Err(StoreError::MissingDependency {
                        parent_kind: "commit",
                        missing_kind: "parent commit",
                        id_hex: hex::encode(parent_id),
                    });
                }
                None => {}
            }
        }

        if self.config.validate_dependencies {
            let non_empty_trees: Vec<Vec<u8>> = commit
                .root_tree_id
                .iter()
                .filter(|t| t.as_slice() != cc_common::EMPTY_TREE_ID_BYTES)
                .cloned()
                .collect();
            if !non_empty_trees.is_empty() {
                let found = self
                    .metadata
                    .find_existing_trees(&project_id, &non_empty_trees)
                    .await?;
                for tree_id in &non_empty_trees {
                    if !found.contains(tree_id.as_slice()) {
                        return Err(StoreError::MissingDependency {
                            parent_kind: "commit",
                            missing_kind: "root tree",
                            id_hex: hex::encode(tree_id),
                        });
                    }
                }
            }
        }

        let generation = max_parent_gen.saturating_add(1);
        self.metadata
            .put_commit(&project_id, commit_id, commit, generation)
            .await
    }

    pub async fn get_commit(&self, namespace_id: &str, commit_id: &[u8]) -> StoreResult<Commit> {
        let project_id = self
            .resolve_project_namespace(namespace_id, "requesting commits")
            .await?;
        self.metadata
            .get_commit(&project_id, commit_id)
            .await?
            .ok_or_else(|| StoreError::ObjectNotFound {
                message: "commit should have been present in cloud database".to_string(),
            })
    }

    pub async fn has_commit(&self, namespace_id: &str, commit_id: &[u8]) -> StoreResult<bool> {
        let project_id = self
            .resolve_project_namespace(namespace_id, "checking commits")
            .await?;
        self.metadata.has_commit(&project_id, commit_id).await
    }

    pub async fn list_commits(
        &self,
        namespace_id: &str,
        page_token: &[u8],
        page_size: u32,
    ) -> StoreResult<(Vec<CommitId>, Vec<u8>)> {
        let (items, next_token, _) = self
            .list_commits_with_sync(namespace_id, page_token, page_size, &[])
            .await?;
        Ok((items, next_token))
    }

    pub async fn list_commits_with_sync(
        &self,
        namespace_id: &str,
        page_token: &[u8],
        page_size: u32,
        since_token: &[u8],
    ) -> StoreResult<(Vec<CommitId>, Vec<u8>, Vec<u8>)> {
        const DEFAULT_PAGE_SIZE: usize = 5_000;
        const MAX_PAGE_SIZE: usize = 20_000;

        let project_id = self
            .resolve_project_namespace(namespace_id, "listing project commits")
            .await?;
        let effective_size = if page_size == 0 {
            DEFAULT_PAGE_SIZE
        } else {
            (page_size as usize).min(MAX_PAGE_SIZE)
        };

        if !since_token.is_empty() {
            let cursor = if page_token.is_empty() {
                since_token
            } else {
                page_token
            };
            let (items, new_sync, has_more) = self
                .metadata
                .list_commit_ids_since(&project_id, cursor, effective_size)
                .await?;
            let next_page = if has_more {
                new_sync.clone()
            } else {
                Vec::new()
            };
            return Ok((items, next_page, new_sync));
        }

        // Capture watermark before page 1 so any concurrent commit inserted during a full
        // scan is guaranteed to have watermark > captured_watermark. Page tokens carry it
        // (`len as u32 BE || watermark || last commit id`) so the last page returns it too: a
        // watermark read at the end of the scan would skip commits inserted mid-scan whose ids
        // sort before the cursor.
        let (watermark, after) = if page_token.is_empty() {
            (
                self.metadata.current_commit_watermark(&project_id).await?,
                None,
            )
        } else {
            let malformed =
                || StoreError::InvalidArgument("malformed ListCommits page_token".to_string());
            let (len, rest) = page_token.split_first_chunk::<4>().ok_or_else(malformed)?;
            let len = u32::from_be_bytes(*len) as usize;
            if len == 0
                || len > MAX_WATERMARK_BYTES
                || rest.len() != len.saturating_add(cc_common::COMMIT_ID_LENGTH)
            {
                return Err(malformed());
            }
            let (watermark, after) = rest.split_at(len);
            (watermark.to_vec(), Some(after))
        };

        let mut items = self
            .metadata
            .list_commit_ids(&project_id, after, effective_size + 1)
            .await?;

        if items.len() > effective_size {
            items.truncate(effective_size);
            let last = items.last().map(Vec::as_slice).unwrap_or_default();
            let mut next_token = Vec::with_capacity(4 + watermark.len() + last.len());
            next_token.extend_from_slice(&(watermark.len() as u32).to_be_bytes());
            next_token.extend_from_slice(&watermark);
            next_token.extend_from_slice(last);
            Ok((items, next_token, watermark))
        } else {
            Ok((items, Vec::new(), watermark))
        }
    }

    pub async fn resolve_commit_prefix(
        &self,
        namespace_id: &str,
        hex_prefix: &str,
    ) -> StoreResult<IdPrefixResolution> {
        let project_id = self
            .resolve_project_namespace(namespace_id, "resolving commit prefixes")
            .await?;

        let normalized = hex_prefix.to_ascii_lowercase();
        let Some((min_id, max_id)) =
            hex_prefix_byte_bounds(&normalized, cc_common::COMMIT_ID_LENGTH)?
        else {
            return Ok(IdPrefixResolution::NoMatch);
        };

        let mut matches = self
            .metadata
            .resolve_commit_id_prefix(&project_id, &min_id, &max_id)
            .await?;

        let root_id = cc_common::ROOT_COMMIT_ID_BYTES.to_vec();
        if root_id.as_slice() >= min_id.as_slice()
            && root_id.as_slice() <= max_id.as_slice()
            && !matches.contains(&root_id)
        {
            matches.push(root_id);
        }

        match matches.len() {
            0 => Ok(IdPrefixResolution::NoMatch),
            1 => Ok(IdPrefixResolution::SingleMatch(matches.swap_remove(0))),
            _ => Ok(IdPrefixResolution::AmbiguousMatch),
        }
    }

    pub async fn resolve_change_prefix(
        &self,
        namespace_id: &str,
        prefix: &str,
        limit: usize,
    ) -> StoreResult<ChangePrefixResolution> {
        let project_id = self
            .resolve_project_namespace(namespace_id, "resolving change prefixes")
            .await?;

        let hex_prefix = normalize_change_id_prefix_to_hex(prefix)?;
        let Some((min_id, max_id)) =
            hex_prefix_byte_bounds(&hex_prefix, cc_common::CHANGE_ID_LENGTH)?
        else {
            return Ok(ChangePrefixResolution::NoMatch);
        };

        let mut matches = self
            .metadata
            .resolve_change_id_prefix(&project_id, &min_id, &max_id, limit)
            .await?;

        let root_change = cc_common::ROOT_CHANGE_ID_BYTES.to_vec();
        if root_change.as_slice() >= min_id.as_slice()
            && root_change.as_slice() <= max_id.as_slice()
            && !matches.iter().any(|(ch, _)| ch == &root_change)
        {
            matches.push((root_change, vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()]));
        }

        match matches.len() {
            0 => Ok(ChangePrefixResolution::NoMatch),
            1 => {
                let (change_id, commit_ids) = matches.swap_remove(0);
                Ok(ChangePrefixResolution::SingleMatch {
                    change_id,
                    commit_ids,
                })
            }
            _ => Ok(ChangePrefixResolution::AmbiguousMatch),
        }
    }
}
