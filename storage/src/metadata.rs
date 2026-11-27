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

//! Metadata store contract (DEV-192).

use crate::error::{AdvanceHeadError, StoreResult};
use crate::ids::{
    Digest, ObjectKind, ObjectRef, OperationId, ProjectId, RequestId, WorkspaceId, WorkspaceName,
};
use async_trait::async_trait;
use bytes::Bytes;

/// Where an object lives: shared by a project, or private to one workspace's
/// operation log. Must match [`ObjectKind::is_project_scoped`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Scope {
    /// Commits, trees, file manifests and symlinks of one project.
    Project(ProjectId),
    /// Operations and views of one workspace.
    Workspace(WorkspaceId),
}

/// One metadata object as stored.
///
/// The store keeps `data` (the canonical bytes) and never parses it, so it does
/// not depend on the object encoding (DEV-79). The caller, which does parse,
/// supplies `refs`: the objects this one points to (a commit's parents and
/// root tree, a tree's subtrees, file manifests and symlinks, an operation's
/// view and parents, a view's commits). The store uses `refs` only for the
/// closure check in [`MetadataStore::advance_head`].
///
/// The caller also supplies `blob_refs`: the blobs this object names, in
/// the same project (a file manifest's contents). The store keeps and
/// compares them like `refs`, so a client can find an object's blobs
/// without parsing it, but does not check that they exist: blobs live in a
/// separate [`crate::BlobStore`], and writers upload them first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectRecord {
    /// What the object is; must match the scope it is written to.
    pub kind: ObjectKind,
    /// Digest of `data`.
    pub id: Digest,
    /// The canonical bytes, kept as given.
    pub data: Bytes,
    /// The objects this one points to, for the closure check.
    pub refs: Vec<ObjectRef>,
    /// The blobs this object names, in the same project.
    pub blob_refs: Vec<Digest>,
}

impl ObjectRecord {
    /// Fails with `InvalidArgument` if the id, a ref or a blob ref is not as
    /// long as its algorithm's output.
    pub fn check_digests(&self) -> StoreResult<()> {
        self.id.check_len()?;
        for r in &self.refs {
            r.id.check_len()?;
        }
        for d in &self.blob_refs {
            d.check_len()?;
        }
        Ok(())
    }
}

/// A workspace row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceRecord {
    /// Assigned by the store; never reused.
    pub id: WorkspaceId,
    /// The alias, unique per server.
    pub name: WorkspaceName,
    /// The project whose objects the workspace shares.
    pub project: ProjectId,
    /// The current head.
    pub head: HeadState,
}

/// A workspace's head and its position in that workspace's operation log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadState {
    /// The operation at the head of the workspace's log.
    pub head: OperationId,
    /// Starts at 0 when the workspace is created and goes up by exactly 1 on
    /// every successful advance.
    pub sequence: u64,
    /// True if the store merged racing writers to produce this head. Always
    /// false in `Reject` mode.
    pub merged: bool,
}

/// How [`MetadataStore::advance_head`] treats a head that has moved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AdvanceMode {
    /// Fail with `HeadMoved` and write nothing. Implemented in v0.
    Reject,
    /// Reserved (DEV-147). Implementations return `Unsupported` until the team
    /// decides between server-side merge and divergence.
    Merge,
}

/// Arguments of [`MetadataStore::advance_head`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdvanceHead {
    /// The workspace to advance.
    pub workspace: WorkspaceId,
    /// The head the caller built on; the swap happens only if it is current.
    pub expected: OperationId,
    /// The operation to make the head. Everything it reaches must be stored.
    pub new_head: OperationId,
    /// What to do if the head has moved.
    pub mode: AdvanceMode,
    /// Chosen by the caller; a retry with the same id is recognised.
    pub request_id: RequestId,
}

/// One row of a workspace's change feed, written in the same transaction as
/// the head it records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadEvent {
    /// The workspace whose head moved.
    pub workspace: WorkspaceId,
    /// The head it moved to, with its sequence.
    pub state: HeadState,
    /// The request that moved it.
    pub request_id: RequestId,
}

/// Limits a client must respect. Reported through `GetServerInfo`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataLimits {
    /// Most objects in one `get_objects`, `put_objects` or `find_missing` call.
    pub max_batch: usize,
    /// Largest `data` of one object, in bytes (the tree size guard of DEV-89).
    pub max_object_bytes: usize,
    /// Largest total `data` in one `put_objects` call, in bytes. Spanner caps
    /// one commit at 100 MB, so callers split batches by bytes as well as by
    /// count.
    pub max_batch_bytes: usize,
    /// Most missing objects listed in one `MissingObjects` error.
    pub max_missing_reported: usize,
}

/// Immutable metadata objects: commits, trees, file manifests, symlinks,
/// operations and views, without heads.
///
/// A cache (the Commit Cloud daemon, or a relay server) implements only
/// this trait: it holds objects but never decides a workspace head. The
/// authoritative server implements [`MetadataStore`], which extends it.
///
/// All methods are safe to call concurrently. All writes are idempotent, so
/// every call may be retried after an error with
/// [`crate::StoreError::is_retryable`], or after a lost reply.
///
/// The store does not check that an object's id matches its bytes, because
/// ids depend on the object encoding (DEV-79). Project objects are shared
/// between workspaces, so the first writer of an id decides its bytes for
/// everyone. The content service (DEV-90) must verify ids before calling
/// `put_objects`.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Limits of this implementation. Constant for the life of the store.
    fn limits(&self) -> MetadataLimits;

    /// Well-known objects jj never stores but that every history ends at: the
    /// root operation, the root commit, the empty tree. The closure check and
    /// `find_missing` treat these as present. Configured when the store is
    /// created (the server passes jj's root ids). Constant for the life of
    /// the store.
    fn virtual_roots(&self) -> Vec<ObjectRef>;

    /// Read objects by reference.
    ///
    /// Returns one entry per input, in input order; `None` where the object is
    /// not stored. An absent object is not an error. Read only, so a retry is
    /// always safe and a crash leaves nothing behind.
    ///
    /// Errors: `LimitExceeded` above `max_batch`; `InvalidArgument` if a
    /// reference's kind does not match `scope`; `Unavailable`; `Corrupt`.
    async fn get_objects(
        &self,
        scope: &Scope,
        refs: &[ObjectRef],
    ) -> StoreResult<Vec<Option<ObjectRecord>>>;

    /// Store objects. Insert only: objects are never replaced.
    ///
    /// - Idempotent: an object already stored with identical `data`, `refs`
    ///   and `blob_refs` is a no-op. An empty batch is a no-op.
    /// - An object already stored with different `data`, `refs` or
    ///   `blob_refs` fails with
    ///   `ObjectMismatch`; its stored bytes are unchanged. The same applies to
    ///   two different versions of one id inside the batch.
    /// - Not atomic across the batch. Each object is written whole or not at
    ///   all, but a failure part-way may leave some objects of the batch
    ///   stored. This is safe because they are unreachable until a head
    ///   advance names them; retry the whole batch.
    /// - The store does not check that `refs` exist at write time; that is the
    ///   closure check's job.
    ///
    /// Errors: `ObjectMismatch`; `LimitExceeded` (batch count, batch bytes or
    /// object size); `InvalidArgument` (kind does not match `scope`, or a
    /// digest is not as long as its algorithm's output);
    /// `NotFound` (unknown project or workspace; only from implementations
    /// that track projects and workspaces, which a cache does not);
    /// `Unavailable`.
    async fn put_objects(&self, scope: &Scope, objects: Vec<ObjectRecord>) -> StoreResult<()>;

    /// Return the references from `refs` that are not stored, in input order.
    ///
    /// One batched read where the backend allows it (DEV-89 expects a single
    /// Spanner read for 1,000 ids). Virtual roots are never missing. Read
    /// only, so a retry is always safe and a crash leaves nothing behind.
    ///
    /// Errors: `LimitExceeded` above `max_batch`; `InvalidArgument` if a
    /// reference's kind does not match `scope`; `Unavailable`.
    async fn find_missing(&self, scope: &Scope, refs: &[ObjectRef]) -> StoreResult<Vec<ObjectRef>>;
}

/// The authority for workspace heads, on top of the objects of
/// [`ObjectStore`].
///
/// Exactly one `MetadataStore` decides the head of a given workspace: the
/// upstream server. Daemons and relays never implement this trait; they hold
/// an [`ObjectStore`] and forward head changes to the authority.
#[async_trait]
pub trait MetadataStore: ObjectStore {
    /// Create a project.
    ///
    /// Atomic. A retry after a lost reply gets `AlreadyExists`, which a
    /// caller that only needs the project to exist treats as success.
    ///
    /// Errors: `AlreadyExists`; `Unavailable`.
    async fn create_project(&self, project: &ProjectId) -> StoreResult<()>;

    /// Create a workspace whose head is `initial_head`, at sequence 0.
    ///
    /// Atomic: the workspace either exists afterwards with sequence 0 or was
    /// not created. Retrying after a lost reply returns `AlreadyExists`; the
    /// caller then looks the name up and treats a workspace with the same
    /// project at sequence 0 as its own. (The initial head alone does not
    /// identify the creator: jj's root operation is the same for everyone.)
    /// The initial head must be stored or a virtual root; it is not checked
    /// here.
    ///
    /// Errors: `AlreadyExists`; `NotFound` (unknown project);
    /// `InvalidArgument` (`initial_head` is not as long as its algorithm's
    /// output); `Unavailable`.
    async fn create_workspace(
        &self,
        project: &ProjectId,
        name: &WorkspaceName,
        initial_head: OperationId,
    ) -> StoreResult<WorkspaceRecord>;

    /// Look up a workspace by name. `None` if it does not exist.
    ///
    /// Read only, so a retry is always safe and a crash leaves nothing
    /// behind. Errors: `Unavailable`; `Corrupt`.
    async fn get_workspace_by_name(
        &self,
        name: &WorkspaceName,
    ) -> StoreResult<Option<WorkspaceRecord>>;

    /// Look up a workspace by id. `None` if it does not exist.
    ///
    /// Read only, so a retry is always safe and a crash leaves nothing
    /// behind. Errors: `Unavailable`; `Corrupt`.
    async fn get_workspace(&self, id: &WorkspaceId) -> StoreResult<Option<WorkspaceRecord>>;

    /// Move a workspace head: the one compare-and-swap in this contract.
    ///
    /// In one atomic step: read the workspace row; if its head is
    /// `req.expected`, set the head to `req.new_head`, set the sequence to the
    /// old sequence + 1, and insert a [`HeadEvent`] with that sequence.
    ///
    /// - Stale head: if the head is not `req.expected`, return
    ///   `HeadMoved` with the current state and write nothing. A head conflict
    ///   is returned to the caller, never retried inside the store.
    /// - Closure: before moving, check that every object reachable from
    ///   `req.new_head` through `refs` is stored (operations and views in the
    ///   workspace scope; commits, trees, file manifests and symlinks in the
    ///   project scope), or is a virtual root. If not, return `MissingObjects`
    ///   and write nothing. Objects of another project never satisfy the check.
    ///   Implementations may check before the transaction, because stored
    ///   objects are never deleted.
    /// - Closure cost: walking all history on every advance is too slow
    ///   for real repositories. Implementations should remember which objects
    ///   have a verified closure (all refs present, recursively) and stop
    ///   walking there. Marking is sound as soon as the walk proves an object
    ///   closed, even if the head advance then fails, because objects are
    ///   never deleted. Reachability from the current head is not a safe
    ///   substitute: a crashed half-batch leaves stored, unreachable objects.
    /// - Retries: a `request_id` names one logical write. If it is the
    ///   request that produced the current head, return the current state as
    ///   success, so a retry after a lost reply does not report `HeadMoved`
    ///   against itself. Reusing that `request_id` with a different
    ///   `new_head` is `InvalidArgument`. Only the latest request is
    ///   remembered: if another writer has advanced since, the retry gets
    ///   `HeadMoved`, and the caller finds its own write by looking for its
    ///   `request_id` in [`MetadataStore::read_events`].
    /// - Crash: a crash leaves either the old head with no new event, or
    ///   the new head with its event. Never one without the other.
    /// - Modes: `Reject` only in v0; `Merge` returns `Unsupported`.
    ///
    /// Errors: `HeadMoved`; `MissingObjects`; `Store` with `NotFound`
    /// (unknown workspace), `InvalidArgument` (a reused `request_id`, or a
    /// head that is not as long as its algorithm's output), `Unsupported`,
    /// `Unavailable`.
    async fn advance_head(&self, req: AdvanceHead) -> Result<HeadState, AdvanceHeadError>;

    /// Read a workspace's change feed: events with sequence greater than
    /// `after_sequence`, in sequence order, at most `limit` of them.
    ///
    /// Used by subscriptions to catch up after a gap (DEV-145). Read only, so
    /// a retry is always safe. Any `after_sequence` is valid: one past the
    /// last event returns nothing. An empty result means the caller is up to
    /// date.
    ///
    /// Errors: `NotFound` (unknown workspace); `InvalidArgument` (`limit` is
    /// 0); `Unavailable`.
    async fn read_events(
        &self,
        workspace: &WorkspaceId,
        after_sequence: u64,
        limit: usize,
    ) -> StoreResult<Vec<HeadEvent>>;
}
