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

//! SQLite [`MetadataStore`]: a persistent, single-node fallback for Demo 2.
//!
//! Same contract as the Spanner store (DEV-86, DEV-89), passes the same
//! conformance suite, survives restarts (so DEV-202's restart tests can run
//! against it), and needs no cloud account. Not meant for production: one
//! writer at a time, one machine.
//!
//! Every call runs on a blocking thread and holds one connection lock, so
//! each call is atomic. `advance_head` runs in one SQLite transaction.

use crate::error::{AdvanceHeadError, ErrorKind, StoreError, StoreResult};
use crate::ids::{
    Digest, HashAlgorithm, ObjectKind, ObjectRef, OperationId, ProjectId, RequestId, WorkspaceId,
    WorkspaceName,
};
use crate::metadata::{
    AdvanceHead, AdvanceMode, HeadEvent, HeadState, MetadataLimits, MetadataStore, ObjectRecord,
    ObjectStore, Scope, WorkspaceRecord,
};
use async_trait::async_trait;
use bytes::Bytes;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use std::collections::{HashSet, VecDeque};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const SCHEMA: &str = "
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
CREATE TABLE IF NOT EXISTS projects (id TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS workspaces (
  id BLOB PRIMARY KEY, name TEXT NOT NULL UNIQUE, project TEXT NOT NULL,
  head BLOB NOT NULL, sequence INTEGER NOT NULL, last_request BLOB);
CREATE TABLE IF NOT EXISTS events (
  workspace BLOB NOT NULL, sequence INTEGER NOT NULL, head BLOB NOT NULL,
  request_id BLOB NOT NULL, merged INTEGER NOT NULL,
  PRIMARY KEY (workspace, sequence));
CREATE TABLE IF NOT EXISTS objects (
  scope BLOB NOT NULL, kind INTEGER NOT NULL, id BLOB NOT NULL,
  data BLOB NOT NULL, refs BLOB NOT NULL, blob_refs BLOB NOT NULL,
  verified INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (scope, kind, id));
";

/// How long a call waits for another process's write lock.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// SQLite-backed metadata store.
pub struct SqliteMetadataStore {
    limits: MetadataLimits,
    virtual_roots: Arc<HashSet<ObjectRef>>,
    conn: Arc<Mutex<Connection>>,
}

impl SqliteMetadataStore {
    /// Open (or create) a database file.
    pub fn open(
        path: impl AsRef<Path>,
        limits: MetadataLimits,
        virtual_roots: Vec<ObjectRef>,
    ) -> StoreResult<Self> {
        let conn = Connection::open(path).map_err(db_err)?;
        Self::init(conn, limits, virtual_roots)
    }

    /// An in-memory database (tests).
    pub fn open_in_memory(
        limits: MetadataLimits,
        virtual_roots: Vec<ObjectRef>,
    ) -> StoreResult<Self> {
        Self::init(
            Connection::open_in_memory().map_err(db_err)?,
            limits,
            virtual_roots,
        )
    }

    fn init(conn: Connection, limits: MetadataLimits, roots: Vec<ObjectRef>) -> StoreResult<Self> {
        // Another process may hold the write lock briefly; wait for it
        // rather than fail at once.
        conn.busy_timeout(BUSY_TIMEOUT).map_err(db_err)?;
        conn.execute_batch(SCHEMA).map_err(db_err)?;
        Ok(Self {
            limits,
            virtual_roots: Arc::new(roots.into_iter().collect()),
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Runs `f` on a blocking thread, holding the connection.
    async fn with<T, E>(
        &self,
        f: impl FnOnce(&mut Connection) -> Result<T, E> + Send + 'static,
    ) -> Result<T, E>
    where
        T: Send + 'static,
        E: From<StoreError> + Send + 'static,
    {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let mut c = conn
                .lock()
                .map_err(|_| err(ErrorKind::Internal, "connection lock poisoned"))?;
            f(&mut c)
        })
        .await
        .map_err(|e| err(ErrorKind::Internal, format!("blocking task: {e}")))?
    }
}

// ---- encoding ---------------------------------------------------------------

fn alg_code(a: HashAlgorithm) -> u8 {
    match a {
        HashAlgorithm::Sha256 => 1,
        HashAlgorithm::Sha1 => 2,
    }
}

fn alg_from(c: u8) -> StoreResult<HashAlgorithm> {
    match c {
        1 => Ok(HashAlgorithm::Sha256),
        2 => Ok(HashAlgorithm::Sha1),
        _ => Err(StoreError::new(
            ErrorKind::Corrupt,
            format!("unknown algorithm {c}"),
        )),
    }
}

fn kind_code(k: ObjectKind) -> i64 {
    match k {
        ObjectKind::Commit => 0,
        ObjectKind::Tree => 1,
        ObjectKind::FileManifest => 2,
        ObjectKind::Symlink => 3,
        ObjectKind::Operation => 4,
        ObjectKind::View => 5,
    }
}

fn kind_from(c: i64) -> StoreResult<ObjectKind> {
    Ok(match c {
        0 => ObjectKind::Commit,
        1 => ObjectKind::Tree,
        2 => ObjectKind::FileManifest,
        3 => ObjectKind::Symlink,
        4 => ObjectKind::Operation,
        5 => ObjectKind::View,
        _ => {
            return Err(StoreError::new(
                ErrorKind::Corrupt,
                format!("unknown kind {c}"),
            ));
        }
    })
}

fn enc_digest(d: &Digest) -> Vec<u8> {
    let mut v = vec![alg_code(d.algorithm)];
    v.extend_from_slice(&d.bytes);
    v
}

fn dec_digest(b: &[u8]) -> StoreResult<Digest> {
    let (&a, rest) = b
        .split_first()
        .ok_or_else(|| StoreError::new(ErrorKind::Corrupt, "empty digest"))?;
    Ok(Digest {
        algorithm: alg_from(a)?,
        bytes: rest.to_vec(),
    })
}

fn enc_scope(s: &Scope) -> Vec<u8> {
    match s {
        Scope::Project(p) => [b"p:".as_slice(), p.0.as_bytes()].concat(),
        Scope::Workspace(w) => [b"w:".as_slice(), &w.0].concat(),
    }
}

/// One-byte length prefix for an encoded digest. `put_objects` checks
/// digest lengths first, so this fails only on a bug.
fn len_byte(encoded: &[u8]) -> StoreResult<u8> {
    u8::try_from(encoded.len())
        .map_err(|_| StoreError::new(ErrorKind::Internal, "digest too long to encode"))
}

/// A transaction that takes the write lock at once. A deferred one reads
/// first and may fail with `SQLITE_BUSY` when it upgrades to write, after
/// another process wrote in between; this one waits up to `BUSY_TIMEOUT`
/// instead, then sees that process's writes.
fn write_transaction(c: &mut Connection) -> StoreResult<Transaction<'_>> {
    c.transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(db_err)
}

/// Refs as: kind (1 byte), digest length (1 byte), digest (alg byte + bytes).
fn enc_refs(refs: &[ObjectRef]) -> StoreResult<Vec<u8>> {
    let mut v = Vec::new();
    for r in refs {
        let d = enc_digest(&r.id);
        v.push(kind_code(r.kind) as u8);
        v.push(len_byte(&d)?);
        v.extend_from_slice(&d);
    }
    Ok(v)
}

/// Digests as length-prefixed `enc_digest` values.
fn enc_digests(digests: &[Digest]) -> StoreResult<Vec<u8>> {
    let mut v = Vec::new();
    for d in digests {
        let d = enc_digest(d);
        v.push(len_byte(&d)?);
        v.extend_from_slice(&d);
    }
    Ok(v)
}
fn dec_digests(mut b: &[u8]) -> StoreResult<Vec<Digest>> {
    let mut out = Vec::new();
    while let Some((&len, rest)) = b.split_first() {
        let len = len as usize;
        if rest.len() < len {
            return Err(StoreError::new(ErrorKind::Corrupt, "truncated blob refs"));
        }
        out.push(dec_digest(&rest[..len])?);
        b = &rest[len..];
    }
    Ok(out)
}
fn dec_refs(mut b: &[u8]) -> StoreResult<Vec<ObjectRef>> {
    let mut out = Vec::new();
    while !b.is_empty() {
        if b.len() < 2 || b.len() < 2 + b[1] as usize {
            return Err(StoreError::new(ErrorKind::Corrupt, "truncated refs"));
        }
        let kind = kind_from(b[0] as i64)?;
        let len = b[1] as usize;
        out.push(ObjectRef {
            kind,
            id: dec_digest(&b[2..2 + len])?,
        });
        b = &b[2 + len..];
    }
    Ok(out)
}

fn db_err(e: rusqlite::Error) -> StoreError {
    let kind = match &e {
        rusqlite::Error::SqliteFailure(f, _)
            if matches!(
                f.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
            ) =>
        {
            ErrorKind::Unavailable
        }
        _ => ErrorKind::Internal,
    };
    StoreError {
        kind,
        message: e.to_string(),
        source: Some(Box::new(e)),
    }
}

fn err(kind: ErrorKind, msg: impl Into<String>) -> StoreError {
    StoreError::new(kind, msg)
}

fn check_scope(scope: &Scope, kind: ObjectKind) -> StoreResult<()> {
    let ok = match scope {
        Scope::Project(_) => kind.is_project_scoped(),
        Scope::Workspace(_) => !kind.is_project_scoped(),
    };
    if ok {
        Ok(())
    } else {
        Err(err(
            ErrorKind::InvalidArgument,
            format!("{kind:?} not in {scope:?}"),
        ))
    }
}

/// `data`, `refs`, `blob_refs` and `verified` of one stored object.
type Row = (Vec<u8>, Vec<u8>, Vec<u8>, bool);

fn get_one(
    c: &Connection,
    scope: &[u8],
    kind: ObjectKind,
    id: &Digest,
) -> StoreResult<Option<Row>> {
    c.query_row(
        "SELECT data, refs, blob_refs, verified FROM objects WHERE scope=?1 AND kind=?2 AND id=?3",
        params![scope, kind_code(kind), enc_digest(id)],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get::<_, i64>(3)? != 0,
            ))
        },
    )
    .optional()
    .map_err(db_err)
}

fn scope_exists(c: &Connection, scope: &Scope) -> StoreResult<bool> {
    let n: i64 = match scope {
        Scope::Project(p) => c.query_row(
            "SELECT COUNT(*) FROM projects WHERE id=?1",
            params![p.0],
            |r| r.get(0),
        ),
        Scope::Workspace(w) => c.query_row(
            "SELECT COUNT(*) FROM workspaces WHERE id=?1",
            params![w.0],
            |r| r.get(0),
        ),
    }
    .map_err(db_err)?;
    Ok(n > 0)
}

/// `advance_head` in one immediate transaction: the retry check, the
/// compare-and-swap, the closure check (stopping at virtual roots and
/// verified objects), then the new head and its event.
fn advance_head_tx(
    c: &mut Connection,
    roots: &HashSet<ObjectRef>,
    limit: usize,
    req: &AdvanceHead,
) -> Result<HeadState, AdvanceHeadError> {
    let tx = write_transaction(c)?;
    let row = workspace_row(&tx, &req.workspace)?
        .ok_or_else(|| err(ErrorKind::NotFound, "unknown workspace"))?;
    let last: Option<Vec<u8>> = tx
        .query_row(
            "SELECT last_request FROM workspaces WHERE id=?1",
            params![req.workspace.0],
            |r| r.get(0),
        )
        .map_err(db_err)?;
    let current = row.head.clone();
    if last.as_deref() == Some(&req.request_id.0[..]) {
        if current.head == req.new_head {
            return Ok(current);
        }
        return Err(err(
            ErrorKind::InvalidArgument,
            "request id reused for another head",
        )
        .into());
    }
    if current.head != req.expected {
        return Err(AdvanceHeadError::HeadMoved { current });
    }
    // Closure check, stopping at virtual roots and verified objects.
    let ws_scope = enc_scope(&Scope::Workspace(req.workspace.clone()));
    let pj_scope = enc_scope(&Scope::Project(row.project.clone()));
    let mut seen = HashSet::new();
    let mut walked = Vec::new();
    let mut missing = Vec::new();
    let mut queue = VecDeque::from([ObjectRef {
        kind: ObjectKind::Operation,
        id: req.new_head.0.clone(),
    }]);
    while let Some(r) = queue.pop_front() {
        if roots.contains(&r) || !seen.insert(r.clone()) {
            continue;
        }
        let scope = if r.kind.is_project_scoped() {
            &pj_scope
        } else {
            &ws_scope
        };
        match get_one(&tx, scope, r.kind, &r.id)? {
            Some((_, _, _, true)) => {}
            Some((_, refs, _, false)) => {
                queue.extend(dec_refs(&refs)?);
                walked.push((scope.clone(), r));
            }
            None => {
                missing.push(r);
                if missing.len() >= limit {
                    break;
                }
            }
        }
    }
    if !missing.is_empty() {
        return Err(AdvanceHeadError::MissingObjects { missing });
    }
    for (scope, r) in &walked {
        tx.execute(
            "UPDATE objects SET verified=1 WHERE scope=?1 AND kind=?2 AND id=?3",
            params![scope, kind_code(r.kind), enc_digest(&r.id)],
        )
        .map_err(db_err)?;
    }
    let next = HeadState {
        head: req.new_head.clone(),
        sequence: current.sequence + 1,
        merged: false,
    };
    tx.execute(
        "UPDATE workspaces SET head=?1, sequence=?2, last_request=?3 WHERE id=?4",
        params![
            enc_digest(&next.head.0),
            next.sequence as i64,
            req.request_id.0,
            req.workspace.0
        ],
    )
    .map_err(db_err)?;
    tx.execute(
        "INSERT INTO events (workspace, sequence, head, request_id, merged) VALUES (?1,?2,?3,?4,0)",
        params![
            req.workspace.0,
            next.sequence as i64,
            enc_digest(&next.head.0),
            req.request_id.0
        ],
    )
    .map_err(db_err)?;
    tx.commit().map_err(db_err)?;
    Ok(next)
}

fn workspace_row(c: &Connection, id: &WorkspaceId) -> StoreResult<Option<WorkspaceRecord>> {
    c.query_row(
        "SELECT name, project, head, sequence FROM workspaces WHERE id=?1",
        params![id.0],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, i64>(3)?,
            ))
        },
    )
    .optional()
    .map_err(db_err)?
    .map(|(name, project, head, seq)| {
        Ok(WorkspaceRecord {
            id: id.clone(),
            name: WorkspaceName(name),
            project: ProjectId(project),
            head: HeadState {
                head: OperationId(dec_digest(&head)?),
                sequence: seq as u64,
                merged: false,
            },
        })
    })
    .transpose()
}

// ---- trait ------------------------------------------------------------------

#[async_trait]
impl ObjectStore for SqliteMetadataStore {
    fn limits(&self) -> MetadataLimits {
        self.limits
    }

    fn virtual_roots(&self) -> Vec<ObjectRef> {
        self.virtual_roots.iter().cloned().collect()
    }

    async fn get_objects(
        &self,
        scope: &Scope,
        refs: &[ObjectRef],
    ) -> StoreResult<Vec<Option<ObjectRecord>>> {
        if refs.len() > self.limits.max_batch {
            return Err(err(ErrorKind::LimitExceeded, "batch too large"));
        }
        for r in refs {
            check_scope(scope, r.kind)?;
        }
        let (scope, refs) = (enc_scope(scope), refs.to_vec());
        self.with(move |c| {
            refs.iter()
                .map(|r| {
                    get_one(c, &scope, r.kind, &r.id)?
                        .map(|(data, refs, blob_refs, _)| {
                            Ok(ObjectRecord {
                                kind: r.kind,
                                id: r.id.clone(),
                                data: Bytes::from(data),
                                refs: dec_refs(&refs)?,
                                blob_refs: dec_digests(&blob_refs)?,
                            })
                        })
                        .transpose()
                })
                .collect()
        })
        .await
    }

    async fn put_objects(&self, scope: &Scope, objects: Vec<ObjectRecord>) -> StoreResult<()> {
        if objects.len() > self.limits.max_batch {
            return Err(err(ErrorKind::LimitExceeded, "batch too large"));
        }
        let mut total = 0;
        for o in &objects {
            check_scope(scope, o.kind)?;
            o.check_digests()?;
            if o.data.len() > self.limits.max_object_bytes {
                return Err(err(ErrorKind::LimitExceeded, "object too large"));
            }
            total += o.data.len();
        }
        if total > self.limits.max_batch_bytes {
            return Err(err(
                ErrorKind::LimitExceeded,
                "batch bytes above max_batch_bytes",
            ));
        }
        let scope_val = scope.clone();
        self.with(move |c| {
            let tx = write_transaction(c)?;
            if !scope_exists(&tx, &scope_val)? {
                return Err(err(ErrorKind::NotFound, format!("{scope_val:?}")));
            }
            let scope = enc_scope(&scope_val);
            let mut in_batch: std::collections::HashMap<(ObjectKind, &Digest), &ObjectRecord> =
                Default::default();
            for o in &objects {
                let refs = enc_refs(&o.refs)?;
                let blob_refs = enc_digests(&o.blob_refs)?;
                let earlier = in_batch.insert((o.kind, &o.id), o);
                let stored = get_one(&tx, &scope, o.kind, &o.id)?;
                let clash_stored = stored
                    .is_some_and(|(d, r, b, _)| d != o.data[..] || r != refs || b != blob_refs);
                let clash_batch = earlier.is_some_and(|e| {
                    e.data != o.data || e.refs != o.refs || e.blob_refs != o.blob_refs
                });
                if clash_stored || clash_batch {
                    return Err(err(
                        ErrorKind::ObjectMismatch,
                        format!("{:?} {}", o.kind, o.id.to_hex()),
                    ));
                }
                tx.execute(
                    "INSERT OR IGNORE INTO objects (scope, kind, id, data, refs, blob_refs)
                     VALUES (?1,?2,?3,?4,?5,?6)",
                    params![
                        scope,
                        kind_code(o.kind),
                        enc_digest(&o.id),
                        &o.data[..],
                        refs,
                        blob_refs
                    ],
                )
                .map_err(db_err)?;
            }
            tx.commit().map_err(db_err)
        })
        .await
    }

    async fn find_missing(&self, scope: &Scope, refs: &[ObjectRef]) -> StoreResult<Vec<ObjectRef>> {
        if refs.len() > self.limits.max_batch {
            return Err(err(ErrorKind::LimitExceeded, "batch too large"));
        }
        for r in refs {
            check_scope(scope, r.kind)?;
        }
        let (scope, refs, roots) = (enc_scope(scope), refs.to_vec(), self.virtual_roots.clone());
        self.with(move |c| {
            let mut out = Vec::new();
            for r in refs {
                if !roots.contains(&r) && get_one(c, &scope, r.kind, &r.id)?.is_none() {
                    out.push(r);
                }
            }
            Ok(out)
        })
        .await
    }
}

#[async_trait]
impl MetadataStore for SqliteMetadataStore {
    async fn create_project(&self, project: &ProjectId) -> StoreResult<()> {
        let p = project.0.clone();
        self.with(move |c| {
            let inserted = c
                .execute(
                    "INSERT OR IGNORE INTO projects (id) VALUES (?1)",
                    params![p],
                )
                .map_err(db_err)?;
            if inserted == 0 {
                return Err(err(ErrorKind::AlreadyExists, format!("project {p}")));
            }
            Ok(())
        })
        .await
    }

    async fn create_workspace(
        &self,
        project: &ProjectId,
        name: &WorkspaceName,
        initial_head: OperationId,
    ) -> StoreResult<WorkspaceRecord> {
        initial_head.0.check_len()?;
        let (project, name) = (project.clone(), name.clone());
        self.with(move |c| {
            let tx = write_transaction(c)?;
            if !scope_exists(&tx, &Scope::Project(project.clone()))? {
                return Err(err(ErrorKind::NotFound, format!("project {project:?}")));
            }
            let taken: i64 = tx
                .query_row(
                    "SELECT COUNT(*) FROM workspaces WHERE name=?1",
                    params![name.0],
                    |r| r.get(0),
                )
                .map_err(db_err)?;
            if taken > 0 {
                return Err(err(ErrorKind::AlreadyExists, format!("workspace {name:?}")));
            }
            // Workspaces are never deleted, so the largest rowid only grows
            // and an id is never reused. Big-endian, as the memory store.
            let n: i64 = tx
                .query_row(
                    "SELECT COALESCE(MAX(rowid), 0) + 1 FROM workspaces",
                    [],
                    |r| r.get(0),
                )
                .map_err(db_err)?;
            let id = WorkspaceId(n.to_be_bytes().to_vec());
            tx.execute(
                "INSERT INTO workspaces (id, name, project, head, sequence) VALUES (?1,?2,?3,?4,0)",
                params![id.0, name.0, project.0, enc_digest(&initial_head.0)],
            )
            .map_err(db_err)?;
            tx.commit().map_err(db_err)?;
            Ok(WorkspaceRecord {
                id,
                name,
                project,
                head: HeadState {
                    head: initial_head,
                    sequence: 0,
                    merged: false,
                },
            })
        })
        .await
    }

    async fn get_workspace_by_name(
        &self,
        name: &WorkspaceName,
    ) -> StoreResult<Option<WorkspaceRecord>> {
        let name = name.0.clone();
        self.with(move |c| {
            let id: Option<Vec<u8>> = c
                .query_row(
                    "SELECT id FROM workspaces WHERE name=?1",
                    params![name],
                    |r| r.get(0),
                )
                .optional()
                .map_err(db_err)?;
            match id {
                Some(id) => workspace_row(c, &WorkspaceId(id)),
                None => Ok(None),
            }
        })
        .await
    }

    async fn get_workspace(&self, id: &WorkspaceId) -> StoreResult<Option<WorkspaceRecord>> {
        let id = id.clone();
        self.with(move |c| workspace_row(c, &id)).await
    }

    async fn advance_head(&self, req: AdvanceHead) -> Result<HeadState, AdvanceHeadError> {
        if req.mode != AdvanceMode::Reject {
            return Err(err(ErrorKind::Unsupported, "only Reject mode is implemented").into());
        }
        req.expected.0.check_len()?;
        req.new_head.0.check_len()?;
        let roots = self.virtual_roots.clone();
        let limit = self.limits.max_missing_reported;
        self.with(move |c| advance_head_tx(c, &roots, limit, &req))
            .await
    }

    async fn read_events(
        &self,
        workspace: &WorkspaceId,
        after_sequence: u64,
        limit: usize,
    ) -> StoreResult<Vec<HeadEvent>> {
        if limit == 0 {
            return Err(err(ErrorKind::InvalidArgument, "limit must be at least 1"));
        }
        // SQLite integers are i64. No stored sequence is above i64::MAX, so
        // clamping a larger cursor gives the right answer: no events. A
        // larger limit means "all of them".
        let after = i64::try_from(after_sequence).unwrap_or(i64::MAX);
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let ws = workspace.clone();
        self.with(move |c| {
            if !scope_exists(c, &Scope::Workspace(ws.clone()))? {
                return Err(err(ErrorKind::NotFound, "unknown workspace"));
            }
            let mut stmt = c
                .prepare(
                    "SELECT sequence, head, request_id, merged FROM events
                     WHERE workspace=?1 AND sequence>?2 ORDER BY sequence LIMIT ?3",
                )
                .map_err(db_err)?;
            let rows = stmt
                .query_map(params![ws.0, after, limit], |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, Vec<u8>>(1)?,
                        r.get::<_, Vec<u8>>(2)?,
                        r.get::<_, i64>(3)?,
                    ))
                })
                .map_err(db_err)?;
            let mut out = Vec::new();
            for row in rows {
                let (seq, head, req, merged) = row.map_err(db_err)?;
                out.push(HeadEvent {
                    workspace: ws.clone(),
                    state: HeadState {
                        head: OperationId(dec_digest(&head)?),
                        sequence: seq as u64,
                        merged: merged != 0,
                    },
                    request_id: RequestId(req),
                });
            }
            Ok(out)
        })
        .await
    }
}
