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

//! jj-commit-cloud server library exposing gRPC services and pluggable storage backends.
//!
//! # Authority and Crash-Safety Ordering
//! `CompositeStore` enforces write-before-publish ordering across [`store::BlobStore`]
//! and [`store::MetadataStore`]: file bytes persist to `BlobStore` before `BlobRecord`
//! and `ProjectBlobs` rows commit in `MetadataStore`, and child objects are verified before
//! parent trees, commits, operations, or operation heads are published.
//!
//! # Concurrency and Merge Invariants
//! Content-addressed entities are immutable and idempotent across all backends
//! (`MemoryMetadataStore`, `SqliteMetadataStore`, and `SpannerMetadataStore`).
//! `MetadataStore::update_op_heads` serializes per-repository head transitions while
//! retaining concurrent sibling heads for lock-free client reconciliation.
//!
//! # Cross-Process and RPC Boundaries
//! [`backend::CommitCloudBackendService`], [`op_store::CommitCloudOpStoreService`], and
//! [`workspace::CommitCloudWorkspaceService`] translate [`store::StoreError`] variants
//! into deterministic gRPC status codes.

#![allow(clippy::result_large_err, clippy::type_complexity)]

pub mod auth;
pub mod backend;
pub mod hash_utils;
pub mod jj_lib_adapters;
pub mod metrics;
pub mod op_store;
pub mod reconcile_op_heads;
pub mod store;
pub mod workspace;
