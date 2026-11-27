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

//! Backend-neutral storage traits for the Commit Cloud server.
//!
//! [`ObjectStore`] holds immutable metadata objects: commits, trees, file
//! manifests, symlinks, operations and views. A cache implements only this.
//! [`MetadataStore`] extends it with workspaces and their heads; only the
//! server, the one head authority, implements it. [`BlobStore`] holds file
//! contents. Nothing here names a backend: metadata may live in Spanner,
//! SQLite or memory, and blobs in GCS, S3 or on local disk.
//!
//! Every implementation follows four rules:
//!
//! 1. Objects and blobs are named by a [`Digest`] of their canonical bytes.
//!    Writing the same name and bytes again is a no-op; different bytes fail
//!    and leave the stored ones unchanged.
//! 2. Objects first, head last: clients write blobs, then objects, then
//!    advance the head. Until then new objects are unreachable, so batch
//!    writes need not be atomic and a whole batch can always be retried.
//! 3. Nothing reachable is deleted, so a closure check made before the head
//!    transaction stays valid during it.
//! 4. [`MetadataStore::advance_head`] is the only compare-and-swap, with one
//!    head and one sequence per workspace.
//!
//! `README.md` records the design decisions and the ones still open.

#![warn(missing_docs)]

mod blob;
pub mod digest;
mod error;
mod ids;
pub mod memory;
mod metadata;

#[cfg(feature = "conformance")]
pub mod conformance;
#[cfg(feature = "local-disk")]
pub mod local_disk;
#[cfg(feature = "sqlite")]
pub mod sqlite;

pub use blob::{BlobInfo, BlobKey, BlobLimits, BlobRead, BlobStore, ByteRange, PutOutcome};
pub use error::{AdvanceHeadError, ErrorKind, StoreError, StoreResult};
pub use ids::{
    Digest, HashAlgorithm, MAX_PROJECT_ID_BYTES, MAX_REQUEST_ID_BYTES, ObjectKind, ObjectRef,
    OperationId, ProjectId, RequestId, WorkspaceId, WorkspaceName,
};
pub use metadata::{
    AdvanceHead, AdvanceMode, HeadEvent, HeadState, MetadataLimits, MetadataStore, ObjectRecord,
    ObjectStore, Scope, WorkspaceRecord,
};

// The three traits must stay usable as `Arc<dyn ...>` so the server can pick a
// backend at start-up. This fails to compile if a change breaks that.
#[allow(dead_code)]
fn _assert_dyn_compatible(
    _: std::sync::Arc<dyn ObjectStore>,
    _: std::sync::Arc<dyn MetadataStore>,
    _: std::sync::Arc<dyn BlobStore>,
) {
}
