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

//! Paginated `ReadTree` and `WriteTree` proxy handlers, entry validation, and tree page slicing.
//!
//! # Authority and Crash-Safety Ordering
//! `WriteTree` writes through to local cache only after jj-cc-server accepts the tree and
//! only when entries pass validation and size bounds. Entries persist sorted in UTF-8 byte
//! order (`"foo" < "foo.rs"`).
//!
//! # Concurrency and Merge Invariants
//! Trees exceeding cache bounds are recorded in `giant_trees` so subsequent paginated callers
//! bypass full-tree assembly and forward directly to upstream. Origins rejecting `page_size`
//! with `UNIMPLEMENTED` are negatively cached with a bounded TTL.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use cc_common::EMPTY_TREE_ID_BYTES;
use cc_common::backend::{
    ReadTreeRequest, ReadTreeResponse, TreeEntry, WriteTreeRequest, WriteTreeResponse, tree_value,
};
use prost::Message;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

use super::{
    DaemonProxy, MAX_TREE_PAGE_SIZE, backend_client, validate_identifier, validate_optional_path,
};
use crate::cache::ObjectKind;

pub(super) fn is_valid_tree_entry_name(name: &str) -> bool {
    !name.is_empty() && name != "." && name != ".." && !name.contains(['/', '\0'])
}

pub(super) fn validate_sorted_tree_entries(entries: &[TreeEntry]) -> Result<(), Status> {
    let mut prev_name: Option<&str> = None;
    for entry in entries {
        if !is_valid_tree_entry_name(&entry.name) {
            return Err(Status::internal(format!(
                "upstream ReadTree returned invalid tree entry name {:?}",
                entry.name
            )));
        }
        if prev_name == Some(entry.name.as_str()) {
            return Err(Status::internal(format!(
                "upstream ReadTree returned duplicate tree entry name {:?}",
                entry.name
            )));
        }
        prev_name = Some(entry.name.as_str());
    }
    Ok(())
}

pub(super) fn slice_tree_page(
    mut full: ReadTreeResponse,
    page_size: u32,
    page_token: &str,
) -> ReadTreeResponse {
    if page_size == 0 && page_token.is_empty() {
        full.next_page_token.clear();
        return full;
    }
    let start_idx = if page_token.is_empty() {
        0
    } else {
        full.entries
            .partition_point(|e| e.name.as_bytes() <= page_token.as_bytes())
    };
    let slice = &full.entries[start_idx..];
    let requested_limit = if page_size == 0 {
        MAX_TREE_PAGE_SIZE
    } else {
        (page_size as usize).min(MAX_TREE_PAGE_SIZE)
    };
    let limit = requested_limit.min(slice.len());
    let page_entries = slice[..limit].to_vec();
    let next_page_token = if limit < slice.len() {
        page_entries
            .last()
            .map(|e| e.name.clone())
            .unwrap_or_default()
    } else {
        String::new()
    };
    ReadTreeResponse {
        tree_id: full.tree_id,
        entries: page_entries,
        next_page_token,
    }
}

impl DaemonProxy {
    async fn forward_paginated_read_tree(
        &self,
        meta: &MetadataMap,
        req: ReadTreeRequest,
    ) -> Result<Response<ReadTreeResponse>, Status> {
        let expected_tree_id = req.tree_id.clone();
        let out = self.inner.upstream.prepare_outbound(meta, req)?;
        let channel = self.inner.upstream.channel_for(meta).await?;
        self.inner.cache.record_upstream_rpc();
        let mut client = backend_client(channel);
        let resp = client.read_tree(out).await?.into_inner();
        if !resp.tree_id.is_empty() && resp.tree_id != expected_tree_id {
            return Err(Status::internal(format!(
                "upstream ReadTree returned mismatched tree_id {} (expected {})",
                hex::encode(&resp.tree_id),
                hex::encode(&expected_tree_id),
            )));
        }
        self.maybe_flush_stats();
        Ok(Response::new(resp))
    }

    pub(super) async fn handle_read_tree(
        &self,
        request: Request<ReadTreeRequest>,
    ) -> Result<Response<ReadTreeResponse>, Status> {
        let _active = self.begin_rpc(request.metadata())?;
        let meta = request.metadata().clone();
        let req = request.into_inner();
        let scope = self.scope_for(&meta, &req.project_id)?;
        validate_optional_path(&req.path)?;
        if req.tree_id.as_slice() == EMPTY_TREE_ID_BYTES {
            validate_identifier(&req.project_id, "project_id")?;
            self.inner.cache.touch_activity();
            self.inner.cache.record_l1_hit();
            self.maybe_flush_stats();
            return Ok(Response::new(ReadTreeResponse {
                tree_id: req.tree_id,
                entries: Vec::new(),
                next_page_token: String::new(),
            }));
        }
        let is_paginated = req.page_size > 0 || !req.page_token.is_empty();
        let giant_key = (
            scope.namespace_hex().to_string(),
            scope.scope_hex().to_string(),
            req.tree_id.clone(),
        );
        if self.is_giant_tree(&giant_key) {
            self.inner.cache.record_miss();
            if is_paginated {
                return self.forward_paginated_read_tree(&meta, req).await;
            }
            self.maybe_flush_stats();
            return Err(Status::resource_exhausted(
                "upstream ReadTree exceeded daemon full-tree cache limits",
            ));
        }

        let tree_id = req.tree_id.clone();
        let page_size = req.page_size;
        let page_token = req.page_token.clone();
        let origin = scope.upstream_url().to_string();
        let tree_too_large = Arc::new(AtomicBool::new(false));
        let mut fetch_res = Err(Status::internal("unreachable"));
        for attempt in 0..2 {
            let fetch_tree_id = req.tree_id.clone();
            let project_id = req.project_id.clone();
            let tree_path = req.path.clone();
            let meta_for_fetch = meta.clone();
            let proxy_for_fetch = self.clone();
            let upstream = Arc::clone(&self.inner.upstream);
            let cache_for_fetch = Arc::clone(&self.inner.cache);
            let max_cached_tree_entries = self.inner.max_cached_tree_entries;
            let max_cached_tree_bytes = self.inner.max_cached_tree_bytes;
            let giant_key_for_fetch = giant_key.clone();
            let origin_for_fetch = origin.clone();
            let tree_too_large_for_fetch = Arc::clone(&tree_too_large);
            let attempted_unpaginated = Arc::new(AtomicBool::new(false));
            let attempted_unpaginated_for_fetch = Arc::clone(&attempted_unpaginated);

            fetch_res = self
                .inner
                .cache
                .get_or_fetch(&scope, ObjectKind::Tree, &tree_id, || async move {
                    let giant_epoch = cache_for_fetch.purge_epoch();
                    if proxy_for_fetch.is_giant_tree(&giant_key_for_fetch) {
                        tree_too_large_for_fetch.store(true, Ordering::Release);
                        return Err(Status::resource_exhausted(
                            "upstream ReadTree exceeded daemon full-tree cache limits",
                        ));
                    }
                    let channel = upstream.channel_for(&meta_for_fetch).await?;
                    let mut client = backend_client(channel);
                    let mut all_entries = Vec::new();
                    let mut cursor = String::new();
                    let mut seen_cursors = HashSet::new();
                    let mut page_idx = 0usize;
                    let mut request_page_size = if !is_paginated
                        && proxy_for_fetch.is_unpaginated_tree_origin(&origin_for_fetch)
                    {
                        0
                    } else {
                        MAX_TREE_PAGE_SIZE as u32
                    };
                    let mut encoded_bytes = if fetch_tree_id.is_empty() {
                        0
                    } else {
                        1usize
                            .saturating_add(prost::length_delimiter_len(fetch_tree_id.len()))
                            .saturating_add(fetch_tree_id.len())
                    };
                    loop {
                        cache_for_fetch.touch_activity();
                        let out = upstream.prepare_coalesced_outbound(
                            &meta_for_fetch,
                            ReadTreeRequest {
                                project_id: project_id.clone(),
                                path: tree_path.clone(),
                                tree_id: fetch_tree_id.clone(),
                                page_size: request_page_size,
                                page_token: cursor.clone(),
                            },
                        )?;
                        if request_page_size == 0 {
                            attempted_unpaginated_for_fetch.store(true, Ordering::Release);
                        }
                        cache_for_fetch.record_upstream_rpc();
                        page_idx += 1;
                        let resp = match client.read_tree(out).await {
                            Ok(resp) => {
                                if page_idx == 1 && request_page_size > 0 {
                                    proxy_for_fetch
                                        .clear_unpaginated_tree_origin(&origin_for_fetch);
                                }
                                resp.into_inner()
                            }
                            Err(status)
                                if status.code() == tonic::Code::Unimplemented
                                    && page_idx == 1
                                    && cursor.is_empty()
                                    && request_page_size > 0 =>
                            {
                                // Remember that this origin rejects paginated `ReadTree` so
                                // subsequent cold unpaginated reads avoid paying two sequential
                                // round-trips, and coalesced unpaginated waiters on a paginated
                                // leader know to retry with `page_size: 0`.
                                proxy_for_fetch
                                    .record_unpaginated_tree_origin(origin_for_fetch.clone());
                                if is_paginated {
                                    return Err(status);
                                }
                                // Servers that do not yet implement paginated `ReadTree` reject
                                // non-zero `page_size` with `UNIMPLEMENTED`; retry page 1 with
                                // `page_size: 0` per `common/proto/backend.proto`.
                                request_page_size = 0;
                                attempted_unpaginated_for_fetch.store(true, Ordering::Release);
                                cache_for_fetch.touch_activity();
                                let retry_out = upstream.prepare_coalesced_outbound(
                                    &meta_for_fetch,
                                    ReadTreeRequest {
                                        project_id: project_id.clone(),
                                        path: tree_path.clone(),
                                        tree_id: fetch_tree_id.clone(),
                                        page_size: 0,
                                        page_token: String::new(),
                                    },
                                )?;
                                cache_for_fetch.record_upstream_rpc();
                                match client.read_tree(retry_out).await {
                                    Ok(resp) => resp.into_inner(),
                                    Err(retry_err) => {
                                        if retry_err.code() == tonic::Code::Unimplemented {
                                            proxy_for_fetch
                                                .clear_unpaginated_tree_origin(&origin_for_fetch);
                                        }
                                        return Err(retry_err);
                                    }
                                }
                            }
                            Err(status) => {
                                if status.code() == tonic::Code::Unimplemented
                                    && page_idx == 1
                                    && cursor.is_empty()
                                    && request_page_size == 0
                                {
                                    proxy_for_fetch
                                        .clear_unpaginated_tree_origin(&origin_for_fetch);
                                }
                                return Err(status);
                            }
                        };
                        if !resp.tree_id.is_empty() && resp.tree_id != fetch_tree_id {
                            return Err(Status::internal(format!(
                                "upstream ReadTree returned mismatched tree_id {} (expected {})",
                                hex::encode(&resp.tree_id),
                                hex::encode(&fetch_tree_id),
                            )));
                        }
                        if resp.entries.is_empty() && !resp.next_page_token.is_empty() {
                            return Err(Status::internal(
                                "upstream ReadTree returned empty page with non-empty next_page_token",
                            ));
                        }
                        if all_entries.len().saturating_add(resp.entries.len())
                            > max_cached_tree_entries
                        {
                            proxy_for_fetch
                                .record_giant_tree(giant_key_for_fetch.clone(), giant_epoch);
                            tree_too_large_for_fetch.store(true, Ordering::Release);
                            return Err(Status::resource_exhausted(format!(
                                "upstream ReadTree exceeded maximum of {max_cached_tree_entries} entries"
                            )));
                        }
                        for entry in &resp.entries {
                            let entry_len = entry.encoded_len();
                            let field_len = 1usize
                                .saturating_add(prost::length_delimiter_len(entry_len))
                                .saturating_add(entry_len);
                            encoded_bytes = encoded_bytes.saturating_add(field_len);
                            if encoded_bytes > max_cached_tree_bytes {
                                proxy_for_fetch
                                    .record_giant_tree(giant_key_for_fetch.clone(), giant_epoch);
                                tree_too_large_for_fetch.store(true, Ordering::Release);
                                return Err(Status::resource_exhausted(format!(
                                    "upstream ReadTree exceeded maximum serialized size of {max_cached_tree_bytes} bytes"
                                )));
                            }
                        }
                        all_entries.extend(resp.entries);
                        if resp.next_page_token.is_empty() {
                            break;
                        }
                        // Track seen cursors with a set rather than lexicographical comparison so
                        // opaque, numeric (`"9"` -> `"10"`), and Git trailing-slash cursors advance
                        // while cycles are still rejected.
                        if !seen_cursors.insert(resp.next_page_token.clone()) {
                            return Err(Status::internal(
                                "upstream ReadTree pagination repeated a cursor token",
                            ));
                        }
                        cursor = resp.next_page_token;
                    }
                    all_entries.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));
                    validate_sorted_tree_entries(&all_entries)?;
                    let full = ReadTreeResponse {
                        tree_id: fetch_tree_id,
                        entries: all_entries,
                        next_page_token: String::new(),
                    };
                    if full.encoded_len() > max_cached_tree_bytes {
                        proxy_for_fetch.record_giant_tree(giant_key_for_fetch, giant_epoch);
                        tree_too_large_for_fetch.store(true, Ordering::Release);
                        return Err(Status::resource_exhausted(format!(
                            "upstream ReadTree exceeded maximum serialized size of {max_cached_tree_bytes} bytes"
                        )));
                    }
                    Ok(Bytes::from(full.encode_to_vec()))
                })
                .await;

            match &fetch_res {
                Err(err)
                    if attempt == 0
                        && !is_paginated
                        && !attempted_unpaginated.load(Ordering::Acquire)
                        && err.code() == tonic::Code::Unimplemented
                        && self.is_unpaginated_tree_origin(&origin) =>
                {
                    continue;
                }
                _ => break,
            }
        }

        let raw = match fetch_res {
            Ok(raw) => raw,
            Err(err) => {
                let ran_fetch_as_giant = tree_too_large.load(Ordering::Acquire);
                if is_paginated
                    && (ran_fetch_as_giant
                        || (err.code() == tonic::Code::ResourceExhausted
                            && self.is_giant_tree(&giant_key)))
                {
                    if !ran_fetch_as_giant {
                        self.inner.cache.record_miss();
                    }
                    return self.forward_paginated_read_tree(&meta, req).await;
                }
                return Err(err);
            }
        };
        let decoded = ReadTreeResponse::decode(raw)
            .map_err(|e| Status::internal(format!("cached tree decode failed: {e}")))?;
        let sliced = slice_tree_page(decoded, page_size, &page_token);
        self.maybe_flush_stats();
        Ok(Response::new(sliced))
    }

    pub(super) async fn handle_write_tree(
        &self,
        request: Request<WriteTreeRequest>,
    ) -> Result<Response<WriteTreeResponse>, Status> {
        let _active = self.begin_rpc(request.metadata())?;
        let meta = request.metadata().clone();
        let req = request.into_inner();
        let scope = self.scope_for(&meta, &req.project_id)?;
        validate_optional_path(&req.path)?;
        let cache_epoch = self.inner.cache.purge_epoch();
        let entries_for_cache =
            (req.entries.len() <= self.inner.max_cached_tree_entries).then(|| req.entries.clone());
        let out = self.inner.upstream.prepare_outbound(&meta, req)?;
        let channel = self.inner.upstream.channel_for(&meta).await?;
        self.inner.cache.record_upstream_rpc();
        let mut client = backend_client(channel);
        let res = client.write_tree(out).await?.into_inner();
        if res.tree_id.as_slice() != EMPTY_TREE_ID_BYTES
            && let Some(mut sorted_entries) = entries_for_cache
        {
            // Must store entries in strict `RepoPathComponent` UTF-8 byte order
            // (`"foo" < "foo.rs"`) expected by `jj_lib::backend::Tree::from_sorted_entries`.
            sorted_entries.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));
            if validate_sorted_tree_entries(&sorted_entries).is_ok() {
                let file_ids: Vec<Vec<u8>> = sorted_entries
                    .iter()
                    .filter_map(
                        |entry| match entry.value.as_ref().and_then(|v| v.value.as_ref()) {
                            Some(tree_value::Value::File(f)) => Some(f.id.clone()),
                            _ => None,
                        },
                    )
                    .collect();
                let sizes = self
                    .inner
                    .cache
                    .blob_sizes_async(scope.clone(), file_ids)
                    .await;
                let mut size_iter = sizes.into_iter();
                let mut all_file_sizes_known = true;
                for entry in &mut sorted_entries {
                    if let Some(tree_value::Value::File(f)) =
                        entry.value.as_mut().and_then(|v| v.value.as_mut())
                    {
                        if let Some(sz) = size_iter.next().flatten() {
                            f.size_bytes = Some(sz);
                        } else {
                            all_file_sizes_known = false;
                        }
                    }
                }
                if all_file_sizes_known
                    && sorted_entries.len() <= self.inner.max_cached_tree_entries
                {
                    let cached_tree = ReadTreeResponse {
                        tree_id: res.tree_id.clone(),
                        entries: sorted_entries,
                        next_page_token: String::new(),
                    };
                    if cached_tree.encoded_len() <= self.inner.max_cached_tree_bytes {
                        self.inner
                            .cache
                            .put_write_through_if_epoch_async(
                                scope,
                                ObjectKind::Tree,
                                res.tree_id.clone(),
                                Bytes::from(cached_tree.encode_to_vec()),
                                Some(cache_epoch),
                            )
                            .await;
                    }
                }
            }
        }
        self.maybe_flush_stats();
        Ok(Response::new(res))
    }
}
