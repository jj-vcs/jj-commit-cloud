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

//! Shared gRPC service contracts and object-identifier constants for jj-commit-cloud.
//!
//! # Authority and Crash-Safety Ordering
//! Callers writing repository and workspace state over `BackendService`, `OpStoreService`,
//! and `WorkspaceService` must persist dependencies bottom-up before publishing heads:
//! 1. Upload file blobs (`WriteFile` / `WriteFileStream`) and symlinks (`WriteSymlink`).
//! 2. Write directory trees (`WriteTree`) referencing those blobs, symlinks, and subtrees.
//! 3. Write commits (`WriteCommit`) referencing persisted trees and parent commits.
//! 4. Write views (`WriteView`) and operations (`WriteOperation`), then advance `UpdateOpHeads`
//!    and `UpdateWorkspace`.
//!
//! # Concurrency and Merge Invariants
//! Object writes (`Commit`, `Tree`, `File`, `Symlink`, `View`, `Operation`) are immutable
//! and idempotent because identifiers derive from canonical content hashes. `UpdateOpHeads`
//! preserves concurrent sibling operation heads, and `UpdateWorkspace` is an idempotent
//! `(repo_id, workspace_name)` upsert (with mutable non-key `user`) preserving full
//! `Merge<TreeId>` conflict terms.
//!
//! # Cross-Process and RPC Boundaries
//! [`FILE_STREAM_CHUNK_SIZE`] (64 KiB) defines the shared streaming chunk target for file
//! transfers, [`MAX_GRPC_MESSAGE_BYTES`] (64 MiB) defines the message size bound for
//! `tonic` clients and servers, and [`validate_optional_repo_path`] enforces canonical
//! repository-relative path rules across tree, file, and symlink RPCs.

#[allow(clippy::result_large_err)]
pub mod backend {
    tonic::include_proto!("commit_cloud.backend");
}

#[allow(clippy::result_large_err)]
pub mod op_store {
    tonic::include_proto!("commit_cloud.op_store");
}

#[allow(clippy::result_large_err)]
pub mod workspace {
    tonic::include_proto!("commit_cloud.workspace");
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
pub const MAX_REPO_PATH_BYTES: usize = 4096;
pub const ROOT_COMMIT_ID_BYTES: [u8; COMMIT_ID_LENGTH] = [0u8; COMMIT_ID_LENGTH];
pub const ROOT_CHANGE_ID_BYTES: [u8; CHANGE_ID_LENGTH] = [0u8; CHANGE_ID_LENGTH];
pub const ROOT_OPERATION_ID_BYTES: [u8; OPERATION_ID_LENGTH] = [0u8; OPERATION_ID_LENGTH];
pub const ROOT_VIEW_ID_BYTES: [u8; VIEW_ID_LENGTH] = [0u8; VIEW_ID_LENGTH];
pub const EMPTY_TREE_ID_HEX: &str = "4b825dc642cb6eb9a060e54bf8d69288fbee4904";
pub const EMPTY_TREE_ID_BYTES: [u8; TREE_ID_LENGTH] = [
    0x4b, 0x82, 0x5d, 0xc6, 0x42, 0xcb, 0x6e, 0xb9, 0xa0, 0x60, 0xe5, 0x4b, 0xf8, 0xd6, 0x92, 0x88,
    0xfb, 0xee, 0x49, 0x04,
];

/// Validates an optional canonical repository-relative path carried on tree,
/// file, and symlink RPCs.
///
/// Empty paths (`""`) are accepted (representing the repository root on tree
/// RPCs or an omitted path). Non-empty paths must be at most
/// [`MAX_REPO_PATH_BYTES`] bytes and must not contain NUL bytes (`\0`),
/// leading or trailing `/`, empty segments (`//`), or `.` / `..` segments.
pub fn validate_optional_repo_path(path: &str) -> Result<(), &'static str> {
    if path.is_empty() {
        return Ok(());
    }
    if path.len() > MAX_REPO_PATH_BYTES {
        return Err("path exceeds maximum length of 4096 bytes");
    }
    if path.as_bytes().contains(&0) {
        return Err("path must not contain NUL bytes");
    }
    if path.starts_with('/') || path.ends_with('/') {
        return Err("path must not start or end with '/'");
    }
    for segment in path.split('/') {
        if segment.is_empty() {
            return Err("path must not contain empty segments ('//')");
        }
        if segment == "." || segment == ".." {
            return Err("path must not contain '.' or '..' segments");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message as _;

    #[test]
    fn validate_optional_repo_path_accepts_empty_and_canonical_paths() {
        for valid in [
            "",
            "a",
            "src/lib.rs",
            "a/.hidden",
            "a/foo..bar",
            "a/..hidden",
            "a/foo.",
            "docs/日本語.md",
        ] {
            assert!(
                validate_optional_repo_path(valid).is_ok(),
                "expected valid path {valid:?}"
            );
        }

        let max_len = "a".repeat(MAX_REPO_PATH_BYTES);
        assert!(validate_optional_repo_path(&max_len).is_ok());
    }

    #[test]
    fn validate_optional_repo_path_rejects_malformed_and_oversized_paths() {
        for invalid in [
            "/", "/abs", "dir/", "a//b", ".", "..", "./a", "a/.", "a/./b", "../a", "a/..",
            "a/../b", "a\0b", "\0",
        ] {
            assert!(
                validate_optional_repo_path(invalid).is_err(),
                "expected invalid path {invalid:?}"
            );
        }

        let too_long = "a".repeat(MAX_REPO_PATH_BYTES + 1);
        assert!(validate_optional_repo_path(&too_long).is_err());
    }

    #[test]
    fn file_and_symlink_rpc_messages_round_trip_optional_path() {
        let read_file = backend::ReadFileRequest {
            project_id: "proj-1".to_string(),
            file_id: vec![0x11; FILE_ID_LENGTH],
            offset: 10,
            max_bytes: 20,
            path: "src/main.rs".to_string(),
        };
        assert_eq!(
            backend::ReadFileRequest::decode(read_file.encode_to_vec().as_slice()).unwrap(),
            read_file
        );

        let write_file = backend::WriteFileRequest {
            project_id: "proj-1".to_string(),
            content: b"hello".to_vec(),
            path: "src/main.rs".to_string(),
        };
        assert_eq!(
            backend::WriteFileRequest::decode(write_file.encode_to_vec().as_slice()).unwrap(),
            write_file
        );

        let write_chunk = backend::WriteFileChunk {
            project_id: "proj-1".to_string(),
            chunk: b"hello".to_vec(),
            total_size: Some(5),
            expected_file_id: Some(vec![0x22; FILE_ID_LENGTH]),
            path: "src/main.rs".to_string(),
        };
        assert_eq!(
            backend::WriteFileChunk::decode(write_chunk.encode_to_vec().as_slice()).unwrap(),
            write_chunk
        );

        let read_symlink = backend::ReadSymlinkRequest {
            project_id: "proj-1".to_string(),
            symlink_id: vec![0x33; SYMLINK_ID_LENGTH],
            path: "docs/latest".to_string(),
        };
        assert_eq!(
            backend::ReadSymlinkRequest::decode(read_symlink.encode_to_vec().as_slice()).unwrap(),
            read_symlink
        );

        let write_symlink = backend::WriteSymlinkRequest {
            project_id: "proj-1".to_string(),
            target: "../v2".to_string(),
            path: "docs/latest".to_string(),
        };
        assert_eq!(
            backend::WriteSymlinkRequest::decode(write_symlink.encode_to_vec().as_slice()).unwrap(),
            write_symlink
        );
    }

    #[test]
    #[allow(deprecated)]
    fn workspace_proto_messages_round_trip_with_conflict_terms_and_presence() {
        let state = workspace::WorkspaceState {
            repo_id: "repo-1".to_string(),
            user: "alice@example.com".to_string(),
            workspace_name: "default".to_string(),
            commit_id: vec![0x11; COMMIT_ID_LENGTH],
            operation_id: vec![0x22; OPERATION_ID_LENGTH],
            tree_id: vec![0x33; TREE_ID_LENGTH],
            sparse_patterns: Vec::new(),
            tree_ids: vec![
                vec![0x33; TREE_ID_LENGTH],
                vec![0x44; TREE_ID_LENGTH],
                vec![0x55; TREE_ID_LENGTH],
            ],
            conflict_labels: vec!["side-a".into(), "base".into(), "side-b".into()],
            sparse_patterns_set: true,
        };
        let encoded = workspace::GetWorkspaceResponse {
            workspace: Some(state.clone()),
        }
        .encode_to_vec();
        let decoded = workspace::GetWorkspaceResponse::decode(encoded.as_slice()).unwrap();
        assert_eq!(decoded.workspace, Some(state.clone()));

        for expected_op in [
            None,
            Some(Vec::new()),
            Some(vec![0x22; OPERATION_ID_LENGTH]),
        ] {
            let cas_req = workspace::UpdateWorkspaceRequest {
                workspace: Some(state.clone()),
                expected_operation_id: expected_op,
            };
            let cas_bytes = cas_req.encode_to_vec();
            let cas_decoded =
                workspace::UpdateWorkspaceRequest::decode(cas_bytes.as_slice()).unwrap();
            assert_eq!(cas_decoded, cas_req);
        }

        let absent_bytes = workspace::GetWorkspaceResponse { workspace: None }.encode_to_vec();
        let absent = workspace::GetWorkspaceResponse::decode(absent_bytes.as_slice()).unwrap();
        assert!(absent.workspace.is_none());
    }
}
