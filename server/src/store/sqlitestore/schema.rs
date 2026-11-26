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

//! SQLite connection setup, WAL pragma configuration, file permissions, and schema migration.
//!
//! # Authority and Crash-Safety Ordering
//! `configure_writer_connection` applies schema DDL and column migrations inside explicit
//! transactions while validating against incompatible legacy database layouts.
//!
//! # Concurrency and Merge Invariants
//! Pragma and schema initialization retry on transient `SQLITE_BUSY` or `SQLITE_LOCKED`
//! errors so concurrent processes opening the same database converge.

use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, ErrorCode, TransactionBehavior};

use crate::store::{StoreError, StoreResult};

pub(super) const SQLITE_SCHEMA: &str = include_str!("../../../sql/sqlite_schema.sql");

const SQLITE_PRAGMA_MAX_RETRIES: usize = 40;

const SQLITE_PRAGMA_RETRY_DELAY: Duration = Duration::from_millis(25);

#[cfg(unix)]
pub(super) fn create_dir_all_private(dir: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt as _;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
}

#[cfg(not(unix))]
pub(super) fn create_dir_all_private(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)
}

/// Atomically creates a missing database file with mode `0o600` before `rusqlite` opens it,
/// avoiding a world-readable window and allowing SQLite to inherit `0o600` on WAL/SHM files.
#[cfg(unix)]
pub(super) fn precreate_private_file(path: &Path) -> std::io::Result<bool> {
    use std::os::unix::fs::OpenOptionsExt as _;
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
    {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(e) => Err(e),
    }
}

#[cfg(not(unix))]
pub(super) fn precreate_private_file(_path: &Path) -> std::io::Result<bool> {
    Ok(false)
}

#[cfg(unix)]
pub(super) fn restrict_sqlite_sidecar_permissions(db_path: &Path) {
    use std::os::unix::fs::PermissionsExt as _;
    for suffix in ["-wal", "-shm"] {
        let mut sidecar = db_path.as_os_str().to_os_string();
        sidecar.push(suffix);
        let _ = std::fs::set_permissions(sidecar, std::fs::Permissions::from_mode(0o600));
    }
}

#[cfg(not(unix))]
pub(super) fn restrict_sqlite_sidecar_permissions(_db_path: &Path) {}

fn is_sqlite_busy_or_locked(err: &rusqlite::Error) -> bool {
    matches!(
        err,
        rusqlite::Error::SqliteFailure(ffi_err, _)
            if matches!(
                ffi_err.code,
                ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked
            )
    )
}

fn retry_on_sqlite_busy<T, F>(mut op: F) -> Result<T, rusqlite::Error>
where
    F: FnMut() -> Result<T, rusqlite::Error>,
{
    let mut attempt = 0;
    loop {
        match op() {
            Ok(val) => return Ok(val),
            Err(err) if is_sqlite_busy_or_locked(&err) && attempt < SQLITE_PRAGMA_MAX_RETRIES => {
                attempt += 1;
                std::thread::sleep(SQLITE_PRAGMA_RETRY_DELAY);
            }
            Err(err) => return Err(err),
        }
    }
}

pub(super) fn configure_reader_connection(conn: &Connection) -> StoreResult<()> {
    conn.execute_batch("PRAGMA busy_timeout = 5000;")
        .map_err(|e| {
            StoreError::Backend(format!("failed to set SQLite reader busy_timeout: {e}"))
        })?;
    retry_on_sqlite_busy(|| conn.execute_batch("PRAGMA query_only = ON;")).map_err(|e| {
        StoreError::Backend(format!("failed to configure SQLite reader pragmas: {e}"))
    })?;
    Ok(())
}

pub(super) fn configure_writer_connection(conn: &mut Connection) -> StoreResult<()> {
    conn.execute_batch("PRAGMA busy_timeout = 5000;")
        .map_err(|e| StoreError::Backend(format!("failed to set SQLite busy_timeout: {e}")))?;
    // Transitioning a fresh or legacy rollback-journal DB into WAL mode can return
    // SQLITE_BUSY/SQLITE_LOCKED immediately despite busy_timeout when concurrent openers
    // race to upgrade SHARED locks to EXCLUSIVE on the database header.
    retry_on_sqlite_busy(|| conn.execute_batch("PRAGMA journal_mode = WAL;")).map_err(|e| {
        StoreError::Backend(format!("failed to enable SQLite WAL journal_mode: {e}"))
    })?;
    retry_on_sqlite_busy(|| {
        conn.execute_batch(
            "PRAGMA synchronous = NORMAL;
             PRAGMA foreign_keys = ON;",
        )
    })
    .map_err(|e| StoreError::Backend(format!("failed to configure SQLite pragmas: {e}")))?;

    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| StoreError::Backend(format!("failed to begin SQLite schema tx: {e}")))?;

    // Reject pre-split PoC databases before mutating any schema objects so legacy files are
    // neither partially migrated nor left in a half-upgraded state.
    let legacy_repos_exists: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'repos')",
            [],
            |row| row.get(0),
        )
        .map_err(|e| StoreError::Backend(format!("failed to inspect SQLite schema: {e}")))?;
    if legacy_repos_exists {
        return Err(StoreError::InvalidArgument(
            "detected incompatible pre-split PoC SQLite database (found legacy 'repos' table); \
             use a new --sqlite-path or remove the legacy database before starting jj-cc-server"
                .to_string(),
        ));
    }

    // Pre-migrate existing `commits` tables that predate `seq` before executing
    // `SQLITE_SCHEMA`, which creates `idx_commits_project_seq ON commits(project_id, seq)`.
    let commits_table_exists: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'commits')",
            [],
            |row| row.get(0),
        )
        .map_err(|e| StoreError::Backend(format!("failed to inspect SQLite schema: {e}")))?;
    if commits_table_exists {
        let has_proto_bytes: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_table_info('commits') WHERE name = 'proto_bytes')",
                [],
                |row| row.get(0),
            )
            .map_err(|e| {
                StoreError::Backend(format!("failed to inspect SQLite commits columns: {e}"))
            })?;
        if !has_proto_bytes {
            return Err(StoreError::InvalidArgument(
                "detected incompatible pre-split PoC SQLite database ('commits' table lacks \
                 'proto_bytes' column); use a new --sqlite-path or remove the legacy database \
                 before starting jj-cc-server"
                    .to_string(),
            ));
        }
        match tx.execute(
            "ALTER TABLE commits ADD COLUMN seq INTEGER NOT NULL DEFAULT 0",
            [],
        ) {
            Ok(_) => {}
            Err(rusqlite::Error::SqliteFailure(_, Some(ref msg)))
                if msg.to_ascii_lowercase().contains("duplicate column name") => {}
            Err(e) => {
                return Err(StoreError::Backend(format!(
                    "failed to migrate legacy commits table (seq): {e}"
                )));
            }
        }
    }

    tx.execute_batch(SQLITE_SCHEMA)
        .map_err(|e| StoreError::Backend(format!("failed to initialize SQLite schema: {e}")))?;

    // Add columns missing from older SQLite schemas if needed.
    for alter_sql in [
        "ALTER TABLE workspaces ADD COLUMN tree_ids_json TEXT NOT NULL DEFAULT '[]'",
        "ALTER TABLE workspaces ADD COLUMN conflict_labels_json TEXT NOT NULL DEFAULT '[]'",
        "ALTER TABLE operations ADD COLUMN published INTEGER NOT NULL DEFAULT 0",
    ] {
        match tx.execute(alter_sql, []) {
            Ok(_) => {}
            Err(rusqlite::Error::SqliteFailure(_, Some(ref msg)))
                if msg.to_ascii_lowercase().contains("duplicate column name") => {}
            Err(e) => {
                return Err(StoreError::Backend(format!(
                    "failed to migrate SQLite schema ({alter_sql}): {e}"
                )));
            }
        }
    }
    tx.execute("UPDATE commits SET seq = rowid WHERE seq = 0", [])
        .map_err(|e| StoreError::Backend(format!("failed to backfill commit seq column: {e}")))?;
    tx.commit()
        .map_err(|e| StoreError::Backend(format!("failed to commit SQLite schema tx: {e}")))?;
    Ok(())
}
