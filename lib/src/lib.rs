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

//! Client-side `jj_lib` storage and working-copy trait implementations backed by
//! remote jj-commit-cloud gRPC services (`CommitCloudBackend`, `CommitCloudOpStore`,
//! `CommitCloudOpHeadsStore`, `CommitCloudIndexStore`, and `CommitCloudWorkingCopy`)
//! with optional local caching proxy routing via [`daemon_client`].
//!
//! # Authority and Crash-Safety Ordering
//! `jj_lib` transactions persist file blobs, symlinks, trees, and commits through
//! [`cc_backend::CommitCloudBackend`] before writing `View` and `Operation` nodes via
//! [`cc_op_store::CommitCloudOpStore`], advancing heads in
//! [`cc_op_heads_store::CommitCloudOpHeadsStore`], updating the local index in
//! [`cc_index_store::CommitCloudIndexStore`], and syncing checkout state in
//! [`cc_working_copy::CommitCloudWorkingCopy`].
//!
//! # Concurrency and Merge Invariants
//! `CommitCloudOpHeadsStore` uses lock-free optimistic updates (`UpdateOpHeads`),
//! letting the server preserve sibling heads for 3-way view reconciliation on load.
//! `CommitCloudWorkingCopy` serializes remote workspace updates with `cc_sync.lock`.
//!
//! # Cross-Process and RPC Boundaries
//! Each store reads `.jj/repo/store/config.toml` (`server_url`, `repo_id`, optional
//! `project_id`), routes through [`daemon_client`] when enabled or pooled HTTP/2 gRPC
//! channels otherwise, and bridges `jj_lib`'s async traits via [`util::run_on_tokio`].

#![allow(
    clippy::large_enum_variant,
    clippy::result_large_err,
    clippy::type_complexity
)]

pub mod cc_backend;
pub mod cc_index_store;
pub mod cc_op_heads_store;
pub mod cc_op_store;
pub mod cc_working_copy;
pub mod daemon_client;
pub mod git_importer;
pub mod repo;
pub mod util;

pub use repo::StoreFactoriesExt;
