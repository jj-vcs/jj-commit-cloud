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

//! PostgreSQL row decoders, advisory lock key hashing, watermarks, and error mapping.
//!
//! # Cross-Process and RPC Boundaries
//! Maps `tokio-postgres` `SQLSTATE` codes and `deadpool-postgres` pool errors deterministically
//! into [`StoreError`] variants, encodes big-endian commit watermarks, and decodes blob and
//! workspace rows while rejecting negative integer lengths as corruption.

use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use cc_common::workspace::WorkspaceState;
use deadpool_postgres::PoolError;
use sha2::{Digest as _, Sha256};
use tokio_postgres::error::SqlState;

use crate::store::{BlobRecord, StoreError, StoreResult};

const NAMESPACE_LOCK_DOMAIN: &[u8] = b"jj-commit-cloud:namespace:";

pub(super) fn namespace_lock_key(id: &str) -> i64 {
    let mut hasher = Sha256::new();
    hasher.update(NAMESPACE_LOCK_DOMAIN);
    hasher.update(id.as_bytes());
    let digest = hasher.finalize();
    let mut key_bytes = [0u8; 8];
    key_bytes.copy_from_slice(&digest[..8]);
    i64::from_be_bytes(key_bytes)
}

pub(super) fn encode_commit_watermark(seq: i64) -> Vec<u8> {
    seq.to_be_bytes().to_vec()
}

pub(super) fn decode_commit_watermark(token: &[u8]) -> StoreResult<i64> {
    let bytes: [u8; 8] = token.try_into().map_err(|_| {
        StoreError::InvalidArgument("invalid PostgreSQL commit sync token".to_string())
    })?;
    let seq = i64::from_be_bytes(bytes);
    if seq < 0 {
        return Err(StoreError::InvalidArgument(
            "invalid PostgreSQL commit sync token (negative sequence)".to_string(),
        ));
    }
    Ok(seq)
}

pub(super) fn classify_query_canceled(msg: String, rpc_deadline_expired: bool) -> StoreError {
    if rpc_deadline_expired || msg.to_ascii_lowercase().contains("statement timeout") {
        StoreError::DeadlineExceeded(msg)
    } else {
        StoreError::Unavailable(msg)
    }
}

pub(super) fn is_retryable_sqlstate(code: &SqlState) -> bool {
    *code == SqlState::T_R_SERIALIZATION_FAILURE || *code == SqlState::T_R_DEADLOCK_DETECTED
}

pub(super) fn is_retryable_pg_error(err: &tokio_postgres::Error) -> bool {
    err.code().is_some_and(is_retryable_sqlstate)
}

pub(super) fn map_pool_err(err: PoolError) -> StoreError {
    match err {
        PoolError::Backend(pg_err) => map_pg_err(pg_err),
        PoolError::Timeout(kind) => {
            StoreError::Unavailable(format!("PostgreSQL connection pool timed out ({kind:?})"))
        }
        PoolError::Closed => StoreError::Unavailable("PostgreSQL connection pool closed".into()),
        other => StoreError::Unavailable(format!("PostgreSQL connection pool error: {other}")),
    }
}

pub(super) fn map_pool_err_with_context(err: PoolError, context: &str) -> StoreError {
    match err {
        PoolError::Backend(pg_err) => map_pg_err_with_context(pg_err, context),
        other => match map_pool_err(other) {
            StoreError::Unavailable(msg) => StoreError::Unavailable(format!("{context}: {msg}")),
            StoreError::ResourceExhausted(msg) => {
                StoreError::ResourceExhausted(format!("{context}: {msg}"))
            }
            other_err => other_err,
        },
    }
}

pub(super) fn map_pg_err(err: tokio_postgres::Error) -> StoreError {
    map_pg_err_inner(err, None)
}

pub(super) fn map_pg_err_with_context(err: tokio_postgres::Error, context: &str) -> StoreError {
    map_pg_err_inner(err, Some(context))
}

fn map_pg_err_inner(err: tokio_postgres::Error, context: Option<&str>) -> StoreError {
    let msg = match context {
        Some(ctx) => format!("{ctx}: {err}"),
        None => err.to_string(),
    };
    if let Some(code) = err.code() {
        if *code == SqlState::QUERY_CANCELED {
            return classify_query_canceled(
                msg,
                crate::resilience::remaining_rpc_deadline().is_some_and(|rem| rem.is_zero()),
            );
        }
        if *code == SqlState::INVALID_PASSWORD
            || *code == SqlState::INVALID_AUTHORIZATION_SPECIFICATION
        {
            return StoreError::Unauthenticated(msg);
        }
        if *code == SqlState::INSUFFICIENT_PRIVILEGE {
            return StoreError::PermissionDenied(msg);
        }
        if *code == SqlState::TOO_MANY_CONNECTIONS
            || *code == SqlState::CONFIGURATION_LIMIT_EXCEEDED
            || *code == SqlState::DISK_FULL
            || *code == SqlState::OUT_OF_MEMORY
        {
            return StoreError::ResourceExhausted(msg);
        }
        if is_retryable_sqlstate(code)
            || *code == SqlState::ADMIN_SHUTDOWN
            || *code == SqlState::CRASH_SHUTDOWN
            || *code == SqlState::CANNOT_CONNECT_NOW
            || *code == SqlState::CONNECTION_EXCEPTION
            || *code == SqlState::CONNECTION_FAILURE
            || *code == SqlState::SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION
            || *code == SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION
        {
            return StoreError::Unavailable(msg);
        }
        if *code == SqlState::INVALID_TEXT_REPRESENTATION
            || *code == SqlState::CHARACTER_NOT_IN_REPERTOIRE
            || *code == SqlState::STRING_DATA_RIGHT_TRUNCATION
        {
            tracing::warn!(
                sqlstate = code.code(),
                error = %msg,
                "PostgreSQL rejected request argument"
            );
            return StoreError::InvalidArgument(
                "invalid request argument rejected by storage backend".to_string(),
            );
        }
    }
    if err.is_closed() {
        return StoreError::Unavailable(msg);
    }
    if let Some(source) = std::error::Error::source(&err)
        && source.is::<std::io::Error>()
    {
        return StoreError::Unavailable(msg);
    }
    StoreError::Backend(msg)
}

pub(super) fn current_unix_millis() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

pub(super) fn decode_blob_row(row: &tokio_postgres::Row) -> StoreResult<BlobRecord> {
    let blob_id: Vec<u8> = row.get(0);
    let size_i64: i64 = row.get(1);
    let content_size = u64::try_from(size_i64).map_err(|_| {
        StoreError::Corruption(format!("negative PostgreSQL blob content_size {size_i64}"))
    })?;
    let inline_content: Option<Vec<u8>> = row.get(2);
    let primary_location: Option<String> = row.get(3);
    let replicated_locations: Vec<String> = row.get(4);
    Ok(BlobRecord {
        blob_id,
        content_size,
        inline_content: inline_content.map(Bytes::from),
        primary_location,
        replicated_locations,
    })
}

pub(super) fn decode_workspace_row(
    repo_id: String,
    user: String,
    workspace_name: String,
    row: &tokio_postgres::Row,
    col_offset: usize,
) -> WorkspaceState {
    let operation_id: Vec<u8> = row.get(col_offset);
    let commit_id: Vec<u8> = row.get(col_offset + 1);
    let tree_ids: Vec<Vec<u8>> = row.get(col_offset + 2);
    let conflict_labels: Vec<String> = row.get(col_offset + 3);
    let sparse_patterns: Vec<String> = row.get(col_offset + 4);
    let sparse_patterns_set: bool = row.get(col_offset + 5);
    let tree_id = tree_ids.first().cloned().unwrap_or_default();
    #[allow(deprecated)]
    WorkspaceState {
        repo_id,
        user,
        workspace_name,
        commit_id,
        operation_id,
        tree_id,
        sparse_patterns,
        tree_ids,
        conflict_labels,
        sparse_patterns_set,
    }
}
