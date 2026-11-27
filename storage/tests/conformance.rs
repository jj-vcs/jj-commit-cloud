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

//! Runs the shared suites against every implementation in this crate.

use cc_storage::conformance::{blob, metadata};
use cc_storage::digest::compute;
use cc_storage::local_disk::LocalDiskBlobStore;
use cc_storage::memory::{MemoryBlobStore, MemoryMetadataStore};
use cc_storage::{BlobLimits, HashAlgorithm, MetadataLimits, ObjectKind, ObjectRef};
use std::sync::Arc;

fn small_blob_limits() -> BlobLimits {
    BlobLimits {
        max_blob_bytes: 2 * 1024 * 1024,
        max_read_bytes: 64 * 1024,
        max_batch: 100,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_metadata_store_conforms() {
    metadata::run_all(Arc::new(MemoryMetadataStore::default())).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_metadata_store_conforms_with_small_limits() {
    let store = MemoryMetadataStore::new(MetadataLimits {
        max_batch: 50,
        max_object_bytes: 1024,
        max_batch_bytes: 4096,
        max_missing_reported: 5,
    });
    metadata::run_all(Arc::new(store)).await;
}

/// jj-like setup: the root operation, root commit and empty tree are never
/// stored. (Real ids come from jj; these stand-ins exercise the same path.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_metadata_store_conforms_with_virtual_roots() {
    let root = |kind, name: &str| ObjectRef {
        kind,
        id: compute(HashAlgorithm::Sha256, name.as_bytes()),
    };
    let roots = vec![
        root(ObjectKind::Operation, "root operation"),
        root(ObjectKind::Commit, "root commit"),
        root(ObjectKind::Tree, "empty tree"),
    ];
    let store =
        MemoryMetadataStore::with_virtual_roots(MemoryMetadataStore::default_limits(), roots);
    metadata::run_all(Arc::new(store)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_blob_store_conforms() {
    let store = MemoryBlobStore::new(small_blob_limits());
    blob::run_all(
        Arc::new(store),
        blob::Options {
            supports_delete: true,
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_disk_blob_store_conforms() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalDiskBlobStore::open_with_limits(dir.path(), small_blob_limits()).unwrap();
    blob::run_all(
        Arc::new(store),
        blob::Options {
            supports_delete: true,
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_metadata_store_conforms() {
    let dir = tempfile::tempdir().unwrap();
    let store = cc_storage::sqlite::SqliteMetadataStore::open(
        dir.path().join("meta.db"),
        MemoryMetadataStore::default_limits(),
        Vec::new(),
    )
    .unwrap();
    metadata::run_all(Arc::new(store)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_metadata_store_conforms_with_virtual_roots_and_small_limits() {
    let root = |kind, name: &str| ObjectRef {
        kind,
        id: compute(HashAlgorithm::Sha256, name.as_bytes()),
    };
    let store = cc_storage::sqlite::SqliteMetadataStore::open_in_memory(
        MetadataLimits {
            max_batch: 50,
            max_object_bytes: 1024,
            max_batch_bytes: 4096,
            max_missing_reported: 5,
        },
        vec![
            root(ObjectKind::Operation, "root operation"),
            root(ObjectKind::Commit, "root commit"),
        ],
    )
    .unwrap();
    metadata::run_all(Arc::new(store)).await;
}
