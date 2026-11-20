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

//! Shared gRPC service contracts and object-identifier constants for jj-commit-cloud.
//!
//! # Authority and Crash-Safety Ordering
//! Callers writing repository state over `BackendService` and `OpStoreService` must
//! persist dependencies bottom-up before publishing heads:
//! 1. Upload file blobs (`WriteFile` / `WriteFileStream`) and symlinks (`WriteSymlink`).
//! 2. Write directory trees (`WriteTree`) referencing those blobs, symlinks, and subtrees.
//! 3. Write commits (`WriteCommit`) referencing persisted trees and parent commits.
//! 4. Write views (`WriteView`) and operations (`WriteOperation`), then advance `UpdateOpHeads`.
//!
//! # Concurrency and Merge Invariants
//! Object writes (`Commit`, `Tree`, `File`, `Symlink`, `View`, `Operation`) are immutable
//! and idempotent because identifiers derive from canonical content hashes. `UpdateOpHeads`
//! is the sole mutable coordination point per repository and preserves concurrent sibling
//! operation heads so `jj_lib` can reconcile divergent views without repository locks.
//!
//! # Cross-Process and RPC Boundaries
//! Defines shared framing constants ([`FILE_STREAM_CHUNK_SIZE`] for 64 KiB `ReadFile` /
//! `WriteFileStream` chunks and [`MAX_GRPC_MESSAGE_BYTES`] as the 64 MiB ceiling for
//! wide-tree and multi-agent view transport endpoints that opt in above Tonic's 4 MiB
//! default).

#[allow(clippy::result_large_err)]
pub mod backend {
    tonic::include_proto!("commit_cloud.backend");
}

#[allow(clippy::result_large_err)]
pub mod op_store {
    tonic::include_proto!("commit_cloud.op_store");
}

pub const COMMIT_ID_LENGTH: usize = 20;
pub const TREE_ID_LENGTH: usize = 20;
pub const FILE_ID_LENGTH: usize = 20;
pub const SYMLINK_ID_LENGTH: usize = 20;
pub const CHANGE_ID_LENGTH: usize = 16;
pub const OPERATION_ID_LENGTH: usize = 16;
pub const VIEW_ID_LENGTH: usize = 16;
pub const FILE_STREAM_CHUNK_SIZE: usize = 64 * 1024;
pub const MAX_GRPC_MESSAGE_BYTES: usize = 64 * 1024 * 1024;
pub const ROOT_COMMIT_ID_BYTES: [u8; COMMIT_ID_LENGTH] = [0u8; COMMIT_ID_LENGTH];
pub const ROOT_CHANGE_ID_BYTES: [u8; CHANGE_ID_LENGTH] = [0u8; CHANGE_ID_LENGTH];
pub const ROOT_OPERATION_ID_BYTES: [u8; OPERATION_ID_LENGTH] = [0u8; OPERATION_ID_LENGTH];
pub const ROOT_VIEW_ID_BYTES: [u8; VIEW_ID_LENGTH] = [0u8; VIEW_ID_LENGTH];
pub const EMPTY_TREE_ID_HEX: &str = "4b825dc642cb6eb9a060e54bf8d69288fbee4904";
pub const EMPTY_TREE_ID_BYTES: [u8; TREE_ID_LENGTH] = [
    0x4b, 0x82, 0x5d, 0xc6, 0x42, 0xcb, 0x6e, 0xb9, 0xa0, 0x60, 0xe5, 0x4b, 0xf8, 0xd6, 0x92, 0x88,
    0xfb, 0xee, 0x49, 0x04,
];
