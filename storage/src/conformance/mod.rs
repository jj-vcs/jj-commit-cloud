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

//! Shared test suites for any implementation of the storage traits.
//!
//! This is the seed of DEV-93 (storage test suite on three backends). Each
//! suite takes one store and uses fresh, unique project and workspace names
//! in every case, so it runs against a shared database (Spanner emulator,
//! real Spanner) as well as against a fresh in-memory store.
//!
//! ```ignore
//! #[tokio::test(flavor = "multi_thread")]
//! async fn spanner_store_conforms() {
//!     let store = std::sync::Arc::new(SpannerStore::connect(...).await.unwrap());
//!     // Use a throwaway database on real Spanner: the trait has no delete.
//!     cc_storage::conformance::metadata::run_all(store).await;
//! }
//! ```
//!
//! A failing case panics with the case name and what was expected.

pub mod blob;
pub mod metadata;
pub mod objects;

use std::sync::atomic::{AtomicU64, Ordering};

/// A name no other case, and no earlier run, has used.
pub(crate) fn unique(prefix: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{prefix}-{nanos:x}-{}", N.fetch_add(1, Ordering::Relaxed))
}
