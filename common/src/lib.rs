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
//! [`FILE_STREAM_CHUNK_SIZE`] (64 KiB) defines the shared streaming chunk target,
//! [`MAX_GRPC_MESSAGE_BYTES`] (64 MiB) bounds `tonic` message sizes,
//! [`validate_optional_repo_path`] enforces canonical repository paths across RPCs,
//! [`DAEMON_WIRE_VERSION`] governs CLI-to-daemon wire negotiation, and
//! [`resolve_unix_socket_path`] enforces platform POSIX `sun_path` byte limits.

use std::io;
use std::path::Path;
#[cfg(unix)]
use std::path::PathBuf;

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

/// Metadata header sent by `cc-lib` clients to negotiate the CLI/daemon wire protocol version.
pub const DAEMON_WIRE_VERSION_HEADER: &str = "x-jj-cc-daemon-wire-version";
/// Current CLI-to-daemon wire protocol version.
pub const DAEMON_WIRE_VERSION: u32 = 1;
/// ASCII string form of [`DAEMON_WIRE_VERSION`] for gRPC metadata headers.
pub const DAEMON_WIRE_VERSION_STR: &str = "1";

/// Linux `sockaddr_un.sun_path` buffer length in bytes (including trailing NUL byte).
pub const LINUX_UNIX_SOCKET_PATH_BYTES: usize = 108;
/// macOS / BSD `sockaddr_un.sun_path` buffer length in bytes (including trailing NUL byte).
pub const BSD_UNIX_SOCKET_PATH_BYTES: usize = 104;

/// Platform `sockaddr_un.sun_path` buffer length in bytes (including trailing NUL byte).
/// Usable socket path length is `MAX_UNIX_SOCKET_PATH_BYTES - 1` bytes.
#[cfg(unix)]
pub const MAX_UNIX_SOCKET_PATH_BYTES: usize = {
    #[cfg(any(
        target_os = "macos",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly",
        target_os = "ios",
    ))]
    {
        BSD_UNIX_SOCKET_PATH_BYTES
    }
    #[cfg(not(any(
        target_os = "macos",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly",
        target_os = "ios",
    )))]
    {
        LINUX_UNIX_SOCKET_PATH_BYTES
    }
};

/// Validates that `path` fits within a POSIX `sockaddr_un.sun_path` buffer of `max_sun_path_bytes`
/// bytes (reserving 1 byte for the trailing NUL terminator) and contains no embedded NUL bytes.
pub fn validate_unix_socket_path_len(path: &Path, max_sun_path_bytes: usize) -> io::Result<()> {
    #[cfg(unix)]
    let raw_bytes = {
        use std::os::unix::ffi::OsStrExt;
        path.as_os_str().as_bytes()
    };
    #[cfg(not(unix))]
    let raw_bytes = path.as_os_str().as_encoded_bytes();

    if raw_bytes.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Unix domain socket path must not be empty; set JJ_CC_DAEMON_SOCKET or JJ_CC_DAEMON_DIR",
        ));
    }
    if raw_bytes.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "Unix domain socket path '{}' contains an embedded NUL byte",
                path.display()
            ),
        ));
    }
    let max_usable = max_sun_path_bytes.saturating_sub(1);
    if raw_bytes.len() > max_usable {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "Unix domain socket path '{}' is {} bytes, which exceeds this platform's limit of {} usable bytes (sun_path buffer {} bytes including NUL terminator); set JJ_CC_DAEMON_SOCKET to a shorter path or use a shorter JJ_CC_DAEMON_DIR / XDG_CACHE_HOME",
                path.display(),
                raw_bytes.len(),
                max_usable,
                max_sun_path_bytes,
            ),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn extract_unix_socket_file_name(path: &Path) -> io::Result<&std::ffi::OsStr> {
    use std::os::unix::ffi::OsStrExt;

    let raw = path.as_os_str().as_bytes();
    if raw.is_empty()
        || raw.ends_with(b"/")
        || raw == b"."
        || raw == b".."
        || raw.ends_with(b"/.")
        || raw.ends_with(b"/..")
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "Unix domain socket path '{}' must end in a socket file name",
                path.display()
            ),
        ));
    }
    path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "Unix domain socket path '{}' must end in a socket file name",
                path.display()
            ),
        )
    })
}

/// Canonicalizes the existing parent directory of a Unix domain socket path without following a
/// symlink at the socket filename itself, then enforces [`MAX_UNIX_SOCKET_PATH_BYTES`].
#[cfg(unix)]
pub fn resolve_unix_socket_path(path: &Path) -> io::Result<PathBuf> {
    let file_name = extract_unix_socket_file_name(path)?;
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let canonical_parent = std::fs::canonicalize(parent)?;
    let resolved = canonical_parent.join(file_name);
    validate_unix_socket_path_len(&resolved, MAX_UNIX_SOCKET_PATH_BYTES)?;
    Ok(resolved)
}

/// Like [`resolve_unix_socket_path`], but when the immediate parent directory does not exist yet
/// it canonicalizes the deepest existing ancestor and appends the remaining relative components
/// so callers can fail fast on overlong socket paths before creating directories or spawning.
#[cfg(unix)]
pub fn resolve_unix_socket_path_or_ancestors(path: &Path) -> io::Result<PathBuf> {
    let file_name = extract_unix_socket_file_name(path)?;
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let resolved_parent = match std::fs::canonicalize(parent) {
        Ok(canonical) => canonical,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            let mut missing_tail = Vec::new();
            let mut cur = parent;
            loop {
                match std::fs::canonicalize(cur) {
                    Ok(mut base) => {
                        for comp in missing_tail.into_iter().rev() {
                            base.push(comp);
                        }
                        break base;
                    }
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {
                        if let Some(comp) = cur.file_name() {
                            missing_tail.push(comp.to_os_string());
                            match cur.parent() {
                                Some(next) if !next.as_os_str().is_empty() => cur = next,
                                _ => cur = Path::new("."),
                            }
                        } else {
                            return Err(err);
                        }
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        Err(err) => return Err(err),
    };
    let resolved = resolved_parent.join(file_name);
    validate_unix_socket_path_len(&resolved, MAX_UNIX_SOCKET_PATH_BYTES)?;
    Ok(resolved)
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

    #[test]
    fn daemon_wire_version_constants_are_consistent() {
        assert_eq!(DAEMON_WIRE_VERSION.to_string(), DAEMON_WIRE_VERSION_STR);
        assert_eq!(DAEMON_WIRE_VERSION_HEADER, "x-jj-cc-daemon-wire-version");
    }

    #[cfg(unix)]
    #[test]
    fn max_unix_socket_path_bytes_matches_libc_sockaddr_un() {
        // SAFETY: `libc::sockaddr_un` is a plain C struct; zero-initialization is valid.
        let addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        assert_eq!(
            MAX_UNIX_SOCKET_PATH_BYTES,
            addr.sun_path.len(),
            "MAX_UNIX_SOCKET_PATH_BYTES must match platform sockaddr_un.sun_path length"
        );
    }

    #[test]
    fn validate_unix_socket_path_len_enforces_linux_and_bsd_boundaries() {
        for (platform_max, usable_max) in [
            (LINUX_UNIX_SOCKET_PATH_BYTES, 107usize),
            (BSD_UNIX_SOCKET_PATH_BYTES, 103usize),
        ] {
            let ok_path = format!("/{}", "a".repeat(usable_max - 1));
            assert_eq!(ok_path.len(), usable_max);
            validate_unix_socket_path_len(Path::new(&ok_path), platform_max)
                .expect("path of exact usable max length (sun_path - 1) must succeed");

            let too_long = format!("/{}", "a".repeat(usable_max));
            assert_eq!(too_long.len(), platform_max);
            let err = validate_unix_socket_path_len(Path::new(&too_long), platform_max)
                .expect_err("path of length == sun_path (no room for NUL) must fail");
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
            let msg = err.to_string();
            assert!(
                msg.contains("JJ_CC_DAEMON_SOCKET")
                    && msg.contains("JJ_CC_DAEMON_DIR")
                    && msg.contains("XDG_CACHE_HOME"),
                "error should give actionable env var guidance: {msg}"
            );
        }

        assert!(
            validate_unix_socket_path_len(Path::new(""), LINUX_UNIX_SOCKET_PATH_BYTES).is_err()
        );
        assert!(validate_unix_socket_path_len(
            Path::new("/tmp/bad\0.sock"),
            LINUX_UNIX_SOCKET_PATH_BYTES
        )
        .is_err());
    }

    #[cfg(unix)]
    #[test]
    fn resolve_unix_socket_path_canonicalizes_ancestor_symlinks_and_guards_length() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let real_dir = dir.path().join("real");
        std::fs::create_dir_all(&real_dir).unwrap();
        let link_dir = dir.path().join("link");
        symlink(&real_dir, &link_dir).unwrap();

        let canonical_real = std::fs::canonicalize(&real_dir).unwrap();
        let resolved = resolve_unix_socket_path(&link_dir.join("daemon.sock")).unwrap();
        assert_eq!(resolved, canonical_real.join("daemon.sock"));

        // A dangling symlink at the socket filename itself is NOT followed, so the daemon can
        // inspect and unlink it safely.
        let dangling_sock = link_dir.join("dangling.sock");
        symlink(dir.path().join("nonexistent-target"), &dangling_sock).unwrap();
        let resolved_dangling = resolve_unix_socket_path(&dangling_sock).unwrap();
        assert_eq!(resolved_dangling, canonical_real.join("dangling.sock"));

        // An ancestor symlink that expands to a path exceeding the platform sun_path limit must
        // fail fast even when the uncanonicalized symlink path looked short.
        let long_target = dir.path().join("d".repeat(MAX_UNIX_SOCKET_PATH_BYTES));
        std::fs::create_dir_all(&long_target).unwrap();
        let short_alias = dir.path().join("s");
        symlink(&long_target, &short_alias).unwrap();
        let err = resolve_unix_socket_path(&short_alias.join("a.sock"))
            .expect_err("canonical path exceeding sun_path must fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(
            err.to_string().contains("JJ_CC_DAEMON_SOCKET"),
            "unexpected error: {err}"
        );

        // `resolve_unix_socket_path_or_ancestors` also catches overlong paths before parent dir
        // creation.
        let err_missing =
            resolve_unix_socket_path_or_ancestors(&short_alias.join("nested").join("a.sock"))
                .expect_err("overlong path with missing child dir must fail fast");
        assert_eq!(err_missing.kind(), io::ErrorKind::InvalidInput);
    }

    #[cfg(unix)]
    #[test]
    fn resolve_unix_socket_path_rejects_trailing_slash_and_dot_segments() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path();

        for invalid in [
            PathBuf::from(""),
            PathBuf::from("/"),
            PathBuf::from("."),
            PathBuf::from(".."),
            PathBuf::from(format!("{}/", base.display())),
            PathBuf::from(format!("{}///", base.display())),
            PathBuf::from(format!("{}/daemon.sock/", base.display())),
            PathBuf::from(format!("{}/daemon.sock/.", base.display())),
            PathBuf::from(format!("{}/daemon.sock/./", base.display())),
            PathBuf::from(format!("{}/daemon.sock/..", base.display())),
            PathBuf::from(format!("{}/.", base.display())),
            PathBuf::from(format!("{}/..", base.display())),
            PathBuf::from("daemon.sock/"),
            PathBuf::from("daemon.sock/."),
        ] {
            let err = resolve_unix_socket_path(&invalid)
                .expect_err(&format!("resolve_unix_socket_path must reject {invalid:?}"));
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

            let err_anc = resolve_unix_socket_path_or_ancestors(&invalid).expect_err(&format!(
                "resolve_unix_socket_path_or_ancestors must reject {invalid:?}"
            ));
            assert_eq!(err_anc.kind(), io::ErrorKind::InvalidInput);
        }

        // Valid filenames containing dots (e.g. leading or trailing dot) remain accepted.
        let canonical_base = std::fs::canonicalize(base).unwrap();
        for valid_name in ["daemon.sock", ".hidden.sock", "foo..sock", "foo."] {
            let candidate = base.join(valid_name);
            assert_eq!(
                resolve_unix_socket_path(&candidate).unwrap(),
                canonical_base.join(valid_name)
            );
            assert_eq!(
                resolve_unix_socket_path_or_ancestors(&candidate).unwrap(),
                canonical_base.join(valid_name)
            );
        }
    }
}
