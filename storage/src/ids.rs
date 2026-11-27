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

//! Names used by both stores. All of them are plain values: no backend types.

/// Hash algorithm a [`Digest`] was computed with.
///
/// Digests always name their algorithm, so two algorithms can coexist during a
/// transition (for example SHA-1 for Git import next to the native hash).
/// Which algorithm is native is still open in DEV-79, hence `non_exhaustive`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[non_exhaustive]
pub enum HashAlgorithm {
    /// SHA-256: 32 bytes.
    Sha256,
    /// SHA-1: 20 bytes, as Git uses.
    Sha1,
}

/// A content address: algorithm plus raw digest bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Digest {
    /// The hash function that produced `bytes`.
    pub algorithm: HashAlgorithm,
    /// The raw digest. Stores refuse one that is not
    /// [`HashAlgorithm::digest_len`] bytes long.
    pub bytes: Vec<u8>,
}

/// A project: the namespace that shares commits, trees, file manifests,
/// symlinks and blobs across its workspaces.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProjectId(pub String);

/// Longest project id, in bytes (schema v0: `STRING(128)`).
pub const MAX_PROJECT_ID_BYTES: usize = 128;

/// Stable id of a workspace. Never reused, even if the name is later freed.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WorkspaceId(pub Vec<u8>);

/// Human name of a workspace, `<username>/<workspace>` (DEV-92).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WorkspaceName(pub String);

/// Id of an operation, used for workspace heads.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct OperationId(pub Digest);

/// Longest request id, in bytes (schema v0: `BYTES(64)`).
pub const MAX_REQUEST_ID_BYTES: usize = 64;

/// Client-chosen id of one logical write, used to recognise retries.
/// 1 to [`MAX_REQUEST_ID_BYTES`] bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RequestId(pub Vec<u8>);

/// What kind of metadata object a record holds.
///
/// Commits, trees, file manifests and symlinks are shared by a project.
/// Operations and views belong to one workspace's operation log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[non_exhaustive]
pub enum ObjectKind {
    /// A jj commit. Project scope.
    Commit,
    /// A directory listing. Project scope.
    Tree,
    /// A file's metadata; its contents are blobs. Project scope.
    FileManifest,
    /// A symlink target. Project scope.
    Symlink,
    /// An entry in a workspace's operation log. Workspace scope.
    Operation,
    /// The repository view an operation points to. Workspace scope.
    View,
}

impl HashAlgorithm {
    /// Digest length in bytes.
    pub fn digest_len(self) -> usize {
        match self {
            HashAlgorithm::Sha256 => 32,
            HashAlgorithm::Sha1 => 20,
        }
    }
}

impl ObjectKind {
    /// True for kinds stored per project; false for kinds stored per workspace.
    pub fn is_project_scoped(self) -> bool {
        matches!(
            self,
            ObjectKind::Commit | ObjectKind::Tree | ObjectKind::FileManifest | ObjectKind::Symlink
        )
    }
}

/// A typed reference to a metadata object.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectRef {
    /// The object's kind, which also gives its scope.
    pub kind: ObjectKind,
    /// The digest of the object's canonical bytes.
    pub id: Digest,
}
