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

//! Ephemeral in-memory `MetadataStore` and `BlobStore` implementations for unit tests and
//! volatile local development stacks (`--store-type=memory`).
//!
//! # Authority and Crash-Safety Ordering
//! `MemoryMetadataStore` mirrors the two-scope `Projects` / `Repositories` relational
//! boundary of the SQLite and Cloud Spanner backends in `Mutex`-guarded maps, keeping
//! immutable content-addressed commits (`or_insert`) and UTF-8 name-sorted directory
//! entries consistent with persistent backends.
//!
//! # Concurrency and Merge Invariants
//! `update_op_heads`, `compare_and_swap_op_heads`, and `publish_operation` execute under
//! serialized per-map mutexes so concurrent sibling operation heads are preserved without
//! read-modify-write races.
//!
//! # Cross-Process and RPC Boundaries
//! State lives strictly inside the `jj-cc-server` process address space and resets on
//! process exit.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;

use cc_common::backend::*;
use cc_common::op_store::*;
use cc_common::workspace::WorkspaceState;

use super::BlobRecord;

mod blobs;
mod metadata;
#[cfg(test)]
mod tests;

pub use blobs::MemoryBlobStore;

pub type ProjectId = String;

pub type RepoId = String;

pub type CommitId = Vec<u8>;

pub type TreeId = Vec<u8>;

pub type FileId = Vec<u8>;

pub type SymlinkId = Vec<u8>;

pub type OpId = Vec<u8>;

pub type ViewId = Vec<u8>;

#[derive(Clone, Debug)]
struct StoredCommit {
    commit: Commit,
    generation: u64,
    seq: u64,
}

#[derive(Clone, Debug)]
struct RepoMeta {
    project_id: ProjectId,
    name: Option<String>,
}

#[derive(Clone, Debug)]
struct StoredOperation {
    op: Operation,
    published: bool,
}

#[derive(Debug, Default)]
pub struct MemoryMetadataStore {
    next_commit_seq: AtomicU64,
    // Methods that hold multiple namespace locks acquire `projects` before `repos` before
    // `project_names` so concurrent registrations cannot deadlock.
    projects: Mutex<HashMap<ProjectId, Option<String>>>,
    project_names: Mutex<HashMap<String, ProjectId>>,
    repos: Mutex<HashMap<RepoId, RepoMeta>>,
    commits: Mutex<HashMap<ProjectId, BTreeMap<CommitId, StoredCommit>>>,
    trees: Mutex<HashMap<ProjectId, HashMap<TreeId, Vec<TreeEntry>>>>,
    symlinks: Mutex<HashMap<ProjectId, HashMap<SymlinkId, String>>>,
    // Methods that hold both blob locks take `project_blobs` before `blobs`, so concurrent
    // blob writes and tree or file reads cannot deadlock.
    blobs: Mutex<HashMap<FileId, BlobRecord>>,
    project_blobs: Mutex<HashMap<ProjectId, HashSet<FileId>>>,
    // Methods that hold both operation locks acquire `ops` before `op_heads` so concurrent
    // head updates and publishes cannot deadlock.
    ops: Mutex<HashMap<RepoId, BTreeMap<OpId, StoredOperation>>>,
    views: Mutex<HashMap<RepoId, HashMap<ViewId, View>>>,
    op_heads: Mutex<HashMap<RepoId, Vec<OpId>>>,
    workspaces: Mutex<HashMap<RepoId, BTreeMap<(String, String), WorkspaceState>>>,
}

impl MemoryMetadataStore {
    pub fn new() -> Self {
        Self::default()
    }
}
