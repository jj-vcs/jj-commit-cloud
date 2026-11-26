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

//! `jj_lib::index::IndexStore` wrapper synchronizing shared project commits into the
//! local revset index using per-operation incremental server watermarks.
//!
//! # Authority and Crash-Safety Ordering
//! `CommitCloudIndexStore` wraps `jj_lib::default_index::DefaultIndexStore`. On
//! `get_index_at_op`, it reads the operation's recorded `sync_token` from
//! `.jj/repo/index/cc_sync_cursor.json` before loading the index (falling back to a full
//! scan if absent, legacy, or its anchor commit is missing), fetches missing commits and
//! ancestors once, inserts parents before children, persists the index segment, and then
//! atomically updates only that operation's watermark. `write_index` propagates a parent
//! watermark only when all parent operations share the exact same verified watermark.
//!
//! # Concurrency and Merge Invariants
//! Commits written in sibling repositories sharing the same `project_id` (or imported
//! from Git) become resolvable in local revsets (`jj log <id>`, `jj new <id>`) in
//! O(delta commits) without advancing watermarks of other operations. Setting
//! `commit-cloud.sync-project-commits = false` or `JJ_CC_SYNC_PROJECT_COMMITS=0` skips
//! project-wide sync and indexes only view-reachable commits plus CLI commit ID hints.
//!
//! # Cross-Process and RPC Boundaries
//! Uses cursor-paginated `ListCommits` frames (`LIST_COMMITS_PAGE_SIZE = 5,000`) and
//! 16-way pipelined `get_commit_async` reads over the pooled HTTP/2 channel. When the
//! server returns `UNIMPLEMENTED`, `UNAVAILABLE`, or `DEADLINE_EXCEEDED`, the sync logs a
//! warning and returns the local index.

use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::StreamExt as _;
use jj_lib::backend::{BackendError, CommitId};
use jj_lib::commit::Commit;
use jj_lib::config::{ConfigGetError, ConfigGetResultExt as _};
use jj_lib::dag_walk;
use jj_lib::default_index::DefaultIndexStore;
use jj_lib::index::{Index, MutableIndex, ReadonlyIndex};
use jj_lib::index_store::{IndexStore, IndexStoreError, IndexStoreResult};
use jj_lib::lock::FileLock;
use jj_lib::object_id::ObjectId as _;
use jj_lib::operation::Operation;
use jj_lib::repo::{ReadonlyRepo, Repo as _};
use jj_lib::settings::UserSettings;
use jj_lib::store::Store;
use serde::{Deserialize, Serialize};

use crate::cc_backend::CLIENT_BACKEND_CONCURRENCY;
use crate::util::{CommitCloudConfig, connect_backend_client, run_on_tokio};

const LIST_COMMITS_PAGE_SIZE: u32 = 5_000;
const MAX_LIST_COMMITS_PAGES: usize = 10_000;
const SYNC_CURSOR_FILENAME: &str = "cc_sync_cursor.json";
const SYNC_CURSOR_LOCK_FILENAME: &str = "cc_sync_cursor.lock";
const SYNC_CURSOR_SCHEMA_VERSION: u32 = 2;
const MAX_OP_WATERMARKS: usize = 256;

/// jj config key (boolean, default `true`) that syncs commits written by other
/// repositories in the same project into the local index on every repository load.
pub const SYNC_PROJECT_COMMITS_CONFIG_KEY: &str = "commit-cloud.sync-project-commits";

/// Environment variable (`0`/`false` disables, `1`/`true` enables) overriding the default
/// project-wide commit sync when `commit-cloud.sync-project-commits` is unset in jj config.
pub const SYNC_PROJECT_COMMITS_ENV: &str = "JJ_CC_SYNC_PROJECT_COMMITS";

static CLI_COMMIT_HINTS: Mutex<Vec<CommitId>> = Mutex::new(Vec::new());

/// Records any full 40-character hexadecimal commit IDs appearing in process CLI arguments
/// so `CommitCloudIndexStore` can fetch and index them on demand (`O(depth)`) when
/// project-wide commit sync is disabled.
pub fn register_cli_commit_hints<I, S>(args: I)
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let hints = extract_cli_commit_hints(args);
    *CLI_COMMIT_HINTS.lock().unwrap() = hints;
}

fn extract_cli_commit_hints<I, S>(args: I) -> Vec<CommitId>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    const HEX_LEN: usize = cc_common::COMMIT_ID_LENGTH * 2;
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for arg in args.into_iter().skip(1) {
        let Some(s) = arg.as_ref().to_str() else {
            continue;
        };
        let bytes = s.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i].is_ascii_hexdigit() {
                let start = i;
                while i < bytes.len() && bytes[i].is_ascii_hexdigit() {
                    i += 1;
                }
                if i - start == HEX_LEN
                    && let Some(cid) = CommitId::try_from_hex(&s[start..i])
                    && cid.as_bytes().len() == cc_common::COMMIT_ID_LENGTH
                    && seen.insert(cid.clone())
                {
                    out.push(cid);
                }
            } else {
                i += 1;
            }
        }
    }
    out
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct OpSyncWatermark {
    op_id_hex: String,
    sync_token_hex: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    anchor_commit_hex: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexSyncCursor {
    version: u32,
    server_url: String,
    project_id: String,
    op_watermarks: Vec<OpSyncWatermark>,
}

#[derive(Debug)]
pub struct CommitCloudIndexStore {
    inner: DefaultIndexStore,
    config: CommitCloudConfig,
    index_dir: PathBuf,
    sync_project_commits: bool,
    loaded_watermarks: Mutex<HashMap<String, Option<OpSyncWatermark>>>,
    cursor_lock: Arc<Mutex<()>>,
}

impl CommitCloudIndexStore {
    pub fn name() -> &'static str {
        "commit_cloud"
    }

    pub fn init(
        settings: &UserSettings,
        store_path: &Path,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let inner = DefaultIndexStore::init(store_path)?;
        let config = CommitCloudConfig::load_from_store(store_path)?;
        let sync_project_commits = sync_project_commits_setting(settings)?;
        Ok(Self {
            inner,
            config,
            index_dir: store_path.to_path_buf(),
            sync_project_commits,
            loaded_watermarks: Mutex::new(HashMap::new()),
            cursor_lock: Arc::new(Mutex::new(())),
        })
    }

    pub fn load(
        settings: &UserSettings,
        store_path: &Path,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let inner = DefaultIndexStore::load(store_path);
        let config = CommitCloudConfig::load_from_store(store_path)?;
        let sync_project_commits = sync_project_commits_setting(settings)?;
        Ok(Self {
            inner,
            config,
            index_dir: store_path.to_path_buf(),
            sync_project_commits,
            loaded_watermarks: Mutex::new(HashMap::new()),
            cursor_lock: Arc::new(Mutex::new(())),
        })
    }

    fn load_cursor_file(&self) -> Option<IndexSyncCursor> {
        let raw = fs::read_to_string(self.index_dir.join(SYNC_CURSOR_FILENAME)).ok()?;
        let cursor: IndexSyncCursor = serde_json::from_str(&raw).ok()?;
        if cursor.version != SYNC_CURSOR_SCHEMA_VERSION
            || cursor.server_url != self.config.server_url
            || cursor.project_id != self.config.effective_project_id()
        {
            return None;
        }
        Some(cursor)
    }

    fn load_op_watermark(&self, op_id: &jj_lib::op_store::OperationId) -> Option<OpSyncWatermark> {
        let op_hex = op_id.hex();
        self.load_cursor_file()?
            .op_watermarks
            .into_iter()
            .find(|entry| entry.op_id_hex == op_hex)
    }

    fn write_cursor_file(&self, cursor: &IndexSyncCursor) {
        if let Ok(bytes) = serde_json::to_vec(cursor) {
            static TMP_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let seq = TMP_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let tmp = self.index_dir.join(format!(
                "{SYNC_CURSOR_FILENAME}.tmp.{}.{seq}",
                std::process::id()
            ));
            let written = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp)
                .and_then(|mut file| {
                    use std::io::Write as _;
                    file.write_all(&bytes)?;
                    file.sync_all()
                });
            if written.is_ok() {
                let dst = self.index_dir.join(SYNC_CURSOR_FILENAME);
                if fs::rename(&tmp, &dst).is_err() {
                    let _ = fs::remove_file(&tmp);
                }
            } else {
                let _ = fs::remove_file(&tmp);
            }
        }
    }

    fn save_op_watermark(&self, wm: OpSyncWatermark) {
        if wm.sync_token_hex.is_empty() {
            return;
        }
        let cursor_path = self.index_dir.join(SYNC_CURSOR_FILENAME);
        let Some(_guard) = lock_cursor_file(&self.cursor_lock, &cursor_path) else {
            return;
        };
        let mut cursor = self.load_cursor_file().unwrap_or_else(|| IndexSyncCursor {
            version: SYNC_CURSOR_SCHEMA_VERSION,
            server_url: self.config.server_url.clone(),
            project_id: self.config.effective_project_id().to_string(),
            op_watermarks: Vec::new(),
        });
        if cursor.op_watermarks.iter().any(|existing| existing == &wm) {
            return;
        }
        cursor
            .op_watermarks
            .retain(|existing| existing.op_id_hex != wm.op_id_hex);
        cursor.op_watermarks.push(wm);
        if cursor.op_watermarks.len() > MAX_OP_WATERMARKS {
            let excess = cursor.op_watermarks.len() - MAX_OP_WATERMARKS;
            cursor.op_watermarks.drain(..excess);
        }
        self.write_cursor_file(&cursor);
    }

    fn remove_op_watermark(&self, op_id_hex: &str) {
        let cursor_path = self.index_dir.join(SYNC_CURSOR_FILENAME);
        let Some(_guard) = lock_cursor_file(&self.cursor_lock, &cursor_path) else {
            return;
        };
        let Some(mut cursor) = self.load_cursor_file() else {
            return;
        };
        let before = cursor.op_watermarks.len();
        cursor
            .op_watermarks
            .retain(|existing| existing.op_id_hex != op_id_hex);
        if cursor.op_watermarks.len() != before {
            self.write_cursor_file(&cursor);
        }
    }

    fn compute_inherited_watermark(
        &self,
        op: &Operation,
        written: &dyn ReadonlyIndex,
    ) -> Option<OpSyncWatermark> {
        let parent_ids = op.parent_ids();
        if parent_ids.is_empty() {
            return None;
        }
        let mut disk_cursor: Option<Option<IndexSyncCursor>> = None;
        let mut common_token_hex: Option<String> = None;
        let mut chosen_anchor_hex: Option<String> = None;

        for parent_id in parent_ids {
            let parent_hex = parent_id.hex();
            let parent_wm = {
                let loaded = self.loaded_watermarks.lock().unwrap();
                if let Some(cached) = loaded.get(&parent_hex) {
                    cached.clone()
                } else {
                    drop(loaded);
                    let cursor_ref = disk_cursor.get_or_insert_with(|| self.load_cursor_file());
                    cursor_ref.as_ref().and_then(|c| {
                        c.op_watermarks
                            .iter()
                            .find(|entry| entry.op_id_hex == parent_hex)
                            .cloned()
                    })
                }
            }?;
            if parent_wm.sync_token_hex.is_empty()
                || hex::decode(&parent_wm.sync_token_hex).is_err()
            {
                return None;
            }
            if let Some(anchor_hex) = &parent_wm.anchor_commit_hex {
                let anchor_id = CommitId::try_from_hex(anchor_hex)?;
                if !futures::executor::block_on(written.as_index().has_id(&anchor_id)).ok()? {
                    return None;
                }
                if chosen_anchor_hex.is_none() {
                    chosen_anchor_hex = Some(anchor_hex.clone());
                }
            }
            match &common_token_hex {
                None => common_token_hex = Some(parent_wm.sync_token_hex),
                Some(existing) if existing == &parent_wm.sync_token_hex => {}
                Some(_) => return None,
            }
        }

        Some(OpSyncWatermark {
            op_id_hex: op.id().hex(),
            sync_token_hex: common_token_hex?,
            anchor_commit_hex: chosen_anchor_hex,
        })
    }
}

async fn validate_watermark(
    wm: Option<OpSyncWatermark>,
    index: &dyn ReadonlyIndex,
) -> Option<(OpSyncWatermark, Vec<u8>)> {
    let wm = wm?;
    if wm.sync_token_hex.is_empty() {
        return None;
    }
    if let Some(anchor_hex) = &wm.anchor_commit_hex {
        let anchor_id = CommitId::try_from_hex(anchor_hex)?;
        if !index.as_index().has_id(&anchor_id).await.ok()? {
            return None;
        }
    }
    let token = hex::decode(&wm.sync_token_hex).ok()?;
    if token.is_empty() {
        return None;
    }
    Some((wm, token))
}

#[async_trait(?Send)]
impl IndexStore for CommitCloudIndexStore {
    fn name(&self) -> &str {
        Self::name()
    }

    async fn get_index_at_op(
        &self,
        op: &Operation,
        store: &Arc<Store>,
    ) -> IndexStoreResult<Box<dyn ReadonlyIndex>> {
        // Read the operation's watermark BEFORE loading the index segment so any watermark
        // observed on disk corresponds to an index already published before this load began.
        let prior_wm = self.load_op_watermark(op.id());
        let index = self.inner.get_index_at_op(op, store).await?;
        let valid_prior = validate_watermark(prior_wm, index.as_ref()).await;
        let (valid_prior_wm, since_token) = match valid_prior {
            Some((wm, token)) => (Some(wm), token),
            None => (None, Vec::new()),
        };

        let op_hex = op.id().hex();
        self.loaded_watermarks
            .lock()
            .unwrap()
            .insert(op_hex.clone(), valid_prior_wm.clone());

        if !self.sync_project_commits {
            return self.index_cli_hinted_commits(index, op, store).await;
        }

        let server_url = self.config.server_url.clone();
        let project_id = self.config.effective_project_id().to_string();
        let cursor_path = self.index_dir.join(SYNC_CURSOR_FILENAME);
        let listed = list_project_commits(
            server_url,
            project_id,
            since_token.clone(),
            cursor_path,
            Arc::clone(&self.cursor_lock),
        )
        .await;
        let (remote_commit_ids, new_sync_token, cursor_reset) = match listed {
            Ok(listed) => listed,
            Err(err) => return keep_local_index_or_fail(index, err),
        };
        if cursor_reset {
            self.loaded_watermarks.lock().unwrap().clear();
        }
        let prev_anchor = if cursor_reset {
            None
        } else {
            valid_prior_wm
                .as_ref()
                .and_then(|wm| wm.anchor_commit_hex.clone())
        };

        let mut missing_ids = Vec::new();
        for cid in &remote_commit_ids {
            if !index
                .as_index()
                .has_id(cid)
                .await
                .map_err(|e| IndexStoreError::Read(e.into()))?
            {
                missing_ids.push(cid.clone());
            }
        }

        // Prefer an anchor proven absent before this sync (`missing_ids.last()`). When
        // `missing_ids` is empty, keep `prev_anchor` rather than downgrading to a local commit in
        // `remote_commit_ids.last()`, so rebuilding this op's view-only index still invalidates
        // the watermark and re-syncs previously indexed non-view sibling commits.
        let anchor = missing_ids
            .last()
            .map(|c| c.hex())
            .or(prev_anchor)
            .or_else(|| remote_commit_ids.last().map(|c| c.hex()));
        let new_wm = if new_sync_token.is_empty() {
            None
        } else {
            Some(OpSyncWatermark {
                op_id_hex: op_hex.clone(),
                sync_token_hex: hex::encode(&new_sync_token),
                anchor_commit_hex: anchor,
            })
        };
        let cursor_unchanged = !cursor_reset && valid_prior_wm == new_wm;

        if missing_ids.is_empty() {
            if !cursor_unchanged && let Some(wm) = new_wm.clone() {
                self.save_op_watermark(wm);
            }
            self.loaded_watermarks
                .lock()
                .unwrap()
                .insert(op_hex, new_wm);
            return Ok(index);
        }

        let fetched = match fetch_missing_commits(store, index.as_index(), missing_ids).await {
            Ok(fetched) => fetched,
            Err(err) => return keep_local_index_or_fail(index, err),
        };

        let mut mut_index = index.start_modification();
        add_commits_parents_first(&mut *mut_index, &fetched).await?;

        let written = self.inner.write_index(mut_index, op)?;
        if !cursor_unchanged && let Some(wm) = new_wm.clone() {
            self.save_op_watermark(wm);
        }
        self.loaded_watermarks
            .lock()
            .unwrap()
            .insert(op_hex, new_wm);
        Ok(written)
    }

    fn write_index(
        &self,
        index: Box<dyn MutableIndex>,
        op: &Operation,
    ) -> IndexStoreResult<Box<dyn ReadonlyIndex>> {
        let written = self.inner.write_index(index, op)?;
        let inherited = self.compute_inherited_watermark(op, written.as_ref());
        let op_hex = op.id().hex();
        self.loaded_watermarks
            .lock()
            .unwrap()
            .insert(op_hex.clone(), inherited.clone());
        match inherited {
            Some(wm) => self.save_op_watermark(wm),
            None => self.remove_op_watermark(&op_hex),
        }
        Ok(written)
    }
}

struct CursorFileGuard<'a> {
    // Drop the on-disk lock before releasing the in-process mutex (reverse of lock order).
    _file_lock: FileLock,
    _mem_guard: std::sync::MutexGuard<'a, ()>,
}

fn lock_cursor_file<'a>(
    cursor_lock: &'a Mutex<()>,
    cursor_path: &Path,
) -> Option<CursorFileGuard<'a>> {
    let mem_guard = cursor_lock.lock().unwrap();
    let lock_path = cursor_path.with_file_name(SYNC_CURSOR_LOCK_FILENAME);
    match FileLock::lock(lock_path) {
        Ok(file_lock) => Some(CursorFileGuard {
            _file_lock: file_lock,
            _mem_guard: mem_guard,
        }),
        Err(err) => {
            tracing::warn!(
                "Failed to acquire Commit Cloud sync cursor lock ({err}); skipping cursor update"
            );
            None
        }
    }
}

fn clear_cursor_file_locked(cursor_lock: &Mutex<()>, cursor_path: &Path) {
    let Some(_guard) = lock_cursor_file(cursor_lock, cursor_path) else {
        return;
    };
    let _ = fs::remove_file(cursor_path);
}

async fn list_project_commits(
    server_url: String,
    project_id: String,
    since_token: Vec<u8>,
    cursor_path: PathBuf,
    cursor_lock: Arc<Mutex<()>>,
) -> Result<(Vec<CommitId>, Vec<u8>, bool), Box<dyn std::error::Error + Send + Sync>> {
    list_project_commits_with_max_pages(
        server_url,
        project_id,
        since_token,
        cursor_path,
        cursor_lock,
        MAX_LIST_COMMITS_PAGES,
    )
    .await
}

async fn list_project_commits_with_max_pages(
    server_url: String,
    project_id: String,
    since_token: Vec<u8>,
    cursor_path: PathBuf,
    cursor_lock: Arc<Mutex<()>>,
    max_pages: usize,
) -> Result<(Vec<CommitId>, Vec<u8>, bool), Box<dyn std::error::Error + Send + Sync>> {
    run_on_tokio(async move {
        let mut client = connect_backend_client(server_url).await?;
        let mut active_since = since_token;
        let mut cursor_reset = false;
        let mut all_ids = Vec::new();
        let mut page_token = Vec::new();
        let mut seen_page_tokens: HashSet<Vec<u8>> = HashSet::new();
        let mut final_sync_token = Vec::new();
        let mut pages_fetched: usize = 0;
        loop {
            match client
                .list_commits(tonic::Request::new(
                    cc_common::backend::ListCommitsRequest {
                        project_id: project_id.clone(),
                        page_token: page_token.clone(),
                        page_size: LIST_COMMITS_PAGE_SIZE,
                        since_token: active_since.clone(),
                    },
                ))
                .await
            {
                Ok(resp) => {
                    pages_fetched += 1;
                    let res = resp.into_inner();
                    if res.commit_ids.is_empty() && !res.next_page_token.is_empty() {
                        return Err(format!(
                            "ListCommits returned empty page with non-empty next_page_token for project {project_id}"
                        )
                        .into());
                    }
                    // A full scan's watermark is read before its first page, so keep the first
                    // one; each incremental page advances the watermark, so keep the latest.
                    if !res.sync_token.is_empty()
                        && (final_sync_token.is_empty() || !active_since.is_empty())
                    {
                        final_sync_token = res.sync_token;
                    }
                    for b in res.commit_ids {
                        if b.len() != cc_common::COMMIT_ID_LENGTH {
                            return Err(format!(
                                "ListCommits returned invalid commit_id length {} (expected {})",
                                b.len(),
                                cc_common::COMMIT_ID_LENGTH
                            )
                            .into());
                        }
                        all_ids.push(CommitId::from_bytes(&b));
                    }
                    if res.next_page_token.is_empty() {
                        break;
                    }
                    if pages_fetched >= max_pages {
                        return Err(format!(
                            "ListCommits exceeded maximum of {max_pages} pages for project {project_id}"
                        )
                        .into());
                    }
                    if !seen_page_tokens.insert(res.next_page_token.clone()) {
                        return Err(format!(
                            "ListCommits pagination cursor repeated or failed to advance for project {project_id}"
                        )
                        .into());
                    }
                    page_token = res.next_page_token;
                }
                Err(status)
                    if matches!(
                        status.code(),
                        tonic::Code::InvalidArgument | tonic::Code::Unimplemented
                    ) && !active_since.is_empty() =>
                {
                    tracing::warn!(
                        project_id = %project_id,
                        error = %status,
                        "Server rejected saved CommitCloudIndexStore sync_token; clearing cursor and falling back to full sync"
                    );
                    let lock = Arc::clone(&cursor_lock);
                    let path = cursor_path.clone();
                    let _ =
                        tokio::task::spawn_blocking(move || clear_cursor_file_locked(&lock, &path))
                            .await;
                    active_since.clear();
                    cursor_reset = true;
                    all_ids.clear();
                    page_token.clear();
                    seen_page_tokens.clear();
                    final_sync_token.clear();
                    pages_fetched = 0;
                }
                Err(status) => return Err(status.into()),
            }
        }
        Ok((all_ids, final_sync_token, cursor_reset))
    })
    .await
}

/// Indexes `commit_ids` and their ancestors that are missing from the index at `repo`'s
/// operation, so they resolve in revsets when `commit-cloud.sync-project-commits` is off.
/// Indexes of later operations build on this one, so the commits stay resolvable.
pub async fn index_commits(repo: &ReadonlyRepo, commit_ids: &[CommitId]) -> IndexStoreResult<()> {
    let index = repo.readonly_index();
    let mut missing_ids = Vec::new();
    for cid in commit_ids {
        if !index
            .as_index()
            .has_id(cid)
            .await
            .map_err(|e| IndexStoreError::Read(e.into()))?
        {
            missing_ids.push(cid.clone());
        }
    }
    if missing_ids.is_empty() {
        return Ok(());
    }
    let fetched = fetch_missing_commits(repo.store(), index.as_index(), missing_ids)
        .await
        .map_err(IndexStoreError::Read)?;
    let mut mut_index = index.start_modification();
    add_commits_parents_first(&mut *mut_index, &fetched).await?;
    repo.loader()
        .index_store()
        .write_index(mut_index, repo.operation())?;
    Ok(())
}

impl CommitCloudIndexStore {
    async fn index_cli_hinted_commits(
        &self,
        index: Box<dyn ReadonlyIndex>,
        op: &Operation,
        store: &Arc<Store>,
    ) -> IndexStoreResult<Box<dyn ReadonlyIndex>> {
        let hints = CLI_COMMIT_HINTS.lock().unwrap().clone();
        self.index_hinted_commits(index, op, store, hints).await
    }

    async fn index_hinted_commits(
        &self,
        index: Box<dyn ReadonlyIndex>,
        op: &Operation,
        store: &Arc<Store>,
        hints: Vec<CommitId>,
    ) -> IndexStoreResult<Box<dyn ReadonlyIndex>> {
        if hints.is_empty() {
            return Ok(index);
        }
        let mut seen = HashSet::with_capacity(hints.len());
        let mut candidates = Vec::new();
        for cid in hints {
            if seen.insert(cid.clone())
                && !index
                    .as_index()
                    .has_id(&cid)
                    .await
                    .map_err(|e| IndexStoreError::Read(e.into()))?
            {
                candidates.push(cid);
            }
        }
        if candidates.is_empty() {
            return Ok(index);
        }

        // Probe hinted full commit IDs concurrently, ignoring `ObjectNotFound` so
        // `present(<id>)` and cross-project isolation behave identically to a local index miss.
        // Retain fetched `Commit` objects so `fetch_missing_commits_with_seed` only walks their
        // unindexed ancestors rather than re-fetching the hinted roots.
        let mut probes = futures::stream::iter(candidates.into_iter().map(|cid| {
            let store = store.clone();
            async move { store.get_commit_async(&cid).await }
        }))
        .buffer_unordered(CLIENT_BACKEND_CONCURRENCY);

        let mut seeded_commits = HashMap::new();
        while let Some(res) = probes.next().await {
            match res {
                Ok(commit) => {
                    seeded_commits.insert(commit.id().clone(), commit);
                }
                Err(BackendError::ObjectNotFound { .. }) => {}
                Err(err) => return keep_local_index_or_fail(index, err.into()),
            }
        }
        if seeded_commits.is_empty() {
            return Ok(index);
        }

        let fetched = match fetch_missing_commits_with_seed(
            store,
            index.as_index(),
            Vec::new(),
            seeded_commits,
        )
        .await
        {
            Ok(fetched) => fetched,
            Err(err) => return keep_local_index_or_fail(index, err),
        };
        let mut mut_index = index.start_modification();
        add_commits_parents_first(&mut *mut_index, &fetched).await?;
        self.inner.write_index(mut_index, op)
    }
}

fn sync_project_commits_setting(settings: &UserSettings) -> Result<bool, ConfigGetError> {
    if let Some(configured) = settings
        .get_bool(SYNC_PROJECT_COMMITS_CONFIG_KEY)
        .optional()?
    {
        return Ok(configured);
    }
    if let Ok(raw) = std::env::var(SYNC_PROJECT_COMMITS_ENV)
        && let Some(parsed) = parse_env_bool(&raw)
    {
        return Ok(parsed);
    }
    Ok(true)
}

fn parse_env_bool(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// Returns `index` without the synced commits when `err` shows that the server cannot sync
/// right now, so read-only commands keep working with the commits already indexed locally.
fn keep_local_index_or_fail(
    index: Box<dyn ReadonlyIndex>,
    err: Box<dyn std::error::Error + Send + Sync>,
) -> IndexStoreResult<Box<dyn ReadonlyIndex>> {
    if is_sync_unavailable(err.as_ref()) {
        tracing::warn!("Skipping Commit Cloud project commit sync: {err}");
        Ok(index)
    } else {
        Err(IndexStoreError::Read(err))
    }
}

/// Returns whether `err` carries a gRPC status meaning the server lacks `ListCommits` or is
/// unreachable or too slow, as opposed to a failure that the user should see.
fn is_sync_unavailable(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut next = Some(err);
    while let Some(err) = next {
        if err.downcast_ref::<tonic::transport::Error>().is_some() {
            return true;
        }
        if let Some(status) = err.downcast_ref::<tonic::Status>() {
            return matches!(
                status.code(),
                tonic::Code::Unimplemented
                    | tonic::Code::Unavailable
                    | tonic::Code::DeadlineExceeded
            );
        }
        // `BackendError::Other` is `#[error(transparent)]`, so `source()` would skip the status.
        next = match err.downcast_ref::<BackendError>() {
            Some(BackendError::Other(inner)) => Some(inner.as_ref()),
            _ => err.source(),
        };
    }
    false
}

/// Fetches `missing_ids` and their ancestors that `index` lacks, each once and at most
/// `CLIENT_BACKEND_CONCURRENCY` at a time. jj-lib's commit cache holds only 100 commits, so
/// the commits are kept here until they are indexed.
async fn fetch_missing_commits(
    store: &Arc<Store>,
    index: &dyn Index,
    missing_ids: Vec<CommitId>,
) -> Result<HashMap<CommitId, Commit>, Box<dyn std::error::Error + Send + Sync>> {
    fetch_missing_commits_with_seed(store, index, missing_ids, HashMap::new()).await
}

async fn fetch_missing_commits_with_seed(
    store: &Arc<Store>,
    index: &dyn Index,
    missing_ids: Vec<CommitId>,
    mut fetched: HashMap<CommitId, Commit>,
) -> Result<HashMap<CommitId, Commit>, Box<dyn std::error::Error + Send + Sync>> {
    let mut queued = HashSet::with_capacity(missing_ids.len() + fetched.len());
    let mut frontier = Vec::with_capacity(missing_ids.len());
    for cid in fetched.keys() {
        queued.insert(cid.clone());
    }
    for commit in fetched.values() {
        for parent_id in commit.parent_ids() {
            if !queued.contains(parent_id) && !index.has_id(parent_id).await? {
                queued.insert(parent_id.clone());
                frontier.push(parent_id.clone());
            }
        }
    }
    for cid in missing_ids {
        if queued.insert(cid.clone()) {
            frontier.push(cid);
        }
    }
    while !frontier.is_empty() {
        let mut commits =
            futures::stream::iter(std::mem::take(&mut frontier).into_iter().map(|cid| {
                let store = store.clone();
                async move { store.get_commit_async(&cid).await }
            }))
            .buffer_unordered(CLIENT_BACKEND_CONCURRENCY);
        while let Some(commit) = commits.next().await {
            let commit = commit?;
            for parent_id in commit.parent_ids() {
                if !queued.contains(parent_id) && !index.has_id(parent_id).await? {
                    queued.insert(parent_id.clone());
                    frontier.push(parent_id.clone());
                }
            }
            fetched.insert(commit.id().clone(), commit);
        }
    }
    Ok(fetched)
}

/// Adds `commits` to `mut_index`, each after its parents, whose other ancestors must
/// already be indexed.
async fn add_commits_parents_first(
    mut_index: &mut dyn MutableIndex,
    commits: &HashMap<CommitId, Commit>,
) -> IndexStoreResult<()> {
    let parents: HashMap<CommitId, Vec<CommitId>> = commits
        .iter()
        .map(|(id, commit)| (id.clone(), commit.parent_ids().to_vec()))
        .collect();
    for id in parents_first_order(&parents)? {
        mut_index
            .add_commit(&commits[&id])
            .await
            .map_err(|e| IndexStoreError::Write(e.into()))?;
    }
    Ok(())
}

/// Orders the keys of `parents` (commit id to parent ids) so each commit follows its parents
/// that are also keys. The walk keeps an explicit stack, so deep histories cannot overflow
/// the thread stack.
fn parents_first_order(
    parents: &HashMap<CommitId, Vec<CommitId>>,
) -> IndexStoreResult<Vec<CommitId>> {
    dag_walk::topo_order_forward(
        parents.keys().cloned(),
        |id: &CommitId| id.clone(),
        |id: &CommitId| {
            parents[id]
                .iter()
                .filter(|parent| parents.contains_key(*parent))
                .cloned()
                .collect::<Vec<_>>()
        },
        |id: CommitId| {
            IndexStoreError::Read(format!("commit graph has a cycle at {}", id.hex()).into())
        },
    )
}

#[cfg(test)]
mod tests;
