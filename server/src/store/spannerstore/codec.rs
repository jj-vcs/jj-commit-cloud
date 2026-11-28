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

//! Spanner row decoders, mutation builders, watermark encoding, and error mapping.
//!
//! # Cross-Process and RPC Boundaries
//! Maps Cloud Spanner and GAX status codes into typed `StoreError` variants,
//! drains single-row result sets to completion so trailing abort tokens are not
//! dropped, and validates RFC3339 commit sync watermarks before SQL parsing.

use bytes::Bytes;
use cc_common::backend::{Commit, TreeEntry};
use cc_common::workspace::WorkspaceState;
use google_cloud_gax::error::rpc::Code as RpcCode;
use google_cloud_spanner::key;
use google_cloud_spanner::key::KeySet;
use google_cloud_spanner::mutation::Mutation;
use google_cloud_spanner::result::{ResultSet, Row};
use google_cloud_spanner::statement::{Statement, StatementBuilder};
use google_cloud_spanner::types as spanner_types;
use prost::Message;
use std::time::Duration;

use super::RW_TRANSACTION_TIMEOUT;
use super::schema::commit_inserted_at_shards;
use crate::store::{BlobRecord, StoreError, StoreResult};

pub(super) const EPOCH_RFC3339_NANOS: &str = "1970-01-01T00:00:00.000000000Z";

/// Drains a Spanner `ResultSet` to `None` while returning its first row, so
/// trailing precommit tokens and gRPC errors (including `ABORTED`) are never dropped.
pub(super) async fn drain_first_row(
    mut rs: ResultSet,
) -> google_cloud_spanner::Result<Option<Row>> {
    let mut first = None;
    while let Some(row_res) = rs.next().await {
        let row = row_res?;
        if first.is_none() {
            first = Some(row);
        }
    }
    Ok(first)
}

pub(super) fn is_not_found(err: &google_cloud_gax::error::Error) -> bool {
    err.status().is_some_and(|s| s.code == RpcCode::NotFound)
        || err.http_status_code().is_some_and(|c| c == 404)
}

pub(super) fn is_already_exists(err: &google_cloud_gax::error::Error) -> bool {
    err.status()
        .is_some_and(|s| s.code == RpcCode::AlreadyExists)
        || err.http_status_code().is_some_and(|c| c == 409)
}

pub(super) fn build_statement_with_deadline(builder: StatementBuilder) -> StoreResult<Statement> {
    match crate::resilience::remaining_rpc_deadline() {
        Some(rem) if rem.is_zero() => Err(StoreError::DeadlineExceeded(
            "gRPC deadline expired before building Spanner statement".to_string(),
        )),
        Some(rem) => Ok(builder.with_attempt_timeout(rem).build()),
        None => Ok(builder.build()),
    }
}

pub(super) fn effective_rw_tx_timeout(remaining: Option<Duration>) -> StoreResult<Duration> {
    match remaining {
        Some(rem) if rem.is_zero() => Err(StoreError::DeadlineExceeded(
            "gRPC deadline expired before starting Spanner read-write transaction".to_string(),
        )),
        Some(rem) => Ok(RW_TRANSACTION_TIMEOUT.min(rem)),
        None => Ok(RW_TRANSACTION_TIMEOUT),
    }
}

pub(super) fn map_spanner_admin_err(err: google_cloud_gax::error::Error) -> StoreError {
    map_spanner_err(err)
}

pub(super) fn map_spanner_err(err: google_cloud_spanner::Error) -> StoreError {
    if err.is_timeout() {
        return StoreError::DeadlineExceeded(err.to_string());
    }
    if let Some(status) = err.status() {
        match status.code {
            RpcCode::InvalidArgument => {
                tracing::warn!(error = %err, "Spanner rejected request with INVALID_ARGUMENT");
                StoreError::InvalidArgument(
                    "invalid request argument rejected by storage backend".to_string(),
                )
            }
            RpcCode::OutOfRange => {
                tracing::warn!(error = %err, "Spanner rejected request with OUT_OF_RANGE");
                StoreError::OutOfRange(
                    "requested range out of bounds in storage backend".to_string(),
                )
            }
            RpcCode::DeadlineExceeded => StoreError::DeadlineExceeded(err.to_string()),
            // An ABORTED transaction that reaches this point can be retried as a whole call,
            // which is what UNAVAILABLE tells gRPC clients.
            RpcCode::Unavailable | RpcCode::Aborted => StoreError::Unavailable(err.to_string()),
            RpcCode::ResourceExhausted => StoreError::ResourceExhausted(err.to_string()),
            RpcCode::PermissionDenied => StoreError::PermissionDenied(err.to_string()),
            RpcCode::Unauthenticated => StoreError::Unauthenticated(err.to_string()),
            _ => StoreError::Backend(err.to_string()),
        }
    } else {
        StoreError::Backend(err.to_string())
    }
}

pub(super) fn encode_spanner_watermark(ts_rfc3339: &str, commit_id: &[u8]) -> Vec<u8> {
    let ts_bytes = ts_rfc3339.as_bytes();
    let mut out = Vec::with_capacity(4 + ts_bytes.len() + commit_id.len());
    out.extend_from_slice(&(ts_bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(ts_bytes);
    out.extend_from_slice(commit_id);
    out
}

fn parse_ascii_digits(bytes: &[u8]) -> Option<u32> {
    if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let mut val = 0u32;
    for &b in bytes {
        val = val * 10 + u32::from(b - b'0');
    }
    Some(val)
}

/// Validates that `ts` matches Spanner's `%Y-%m-%dT%H:%M:%E9SZ` UTC RFC3339 format
/// (year `0001..=9999`, valid calendar date including leap years, `00..=23`:`00..=59`:`00..=59`,
/// optional `.1..=9` fractional digits, and trailing `Z`).
fn is_valid_spanner_rfc3339_utc(ts: &str) -> bool {
    let bytes = ts.as_bytes();
    if bytes.len() < 20 || bytes.last() != Some(&b'Z') {
        return false;
    }
    if bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return false;
    }
    let (Some(year), Some(month), Some(day), Some(hour), Some(minute), Some(second)) = (
        parse_ascii_digits(&bytes[0..4]),
        parse_ascii_digits(&bytes[5..7]),
        parse_ascii_digits(&bytes[8..10]),
        parse_ascii_digits(&bytes[11..13]),
        parse_ascii_digits(&bytes[14..16]),
        parse_ascii_digits(&bytes[17..19]),
    ) else {
        return false;
    };
    if year == 0 || !(1..=12).contains(&month) || hour > 23 || minute > 59 || second > 59 {
        return false;
    }
    let is_leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap => 29,
        2 => 28,
        _ => return false,
    };
    if day == 0 || day > max_day {
        return false;
    }
    let frac = &bytes[19..bytes.len() - 1];
    if frac.is_empty() {
        return true;
    }
    frac[0] == b'.'
        && (1..=9).contains(&(frac.len() - 1))
        && frac[1..].iter().all(u8::is_ascii_digit)
}

pub(super) fn decode_spanner_watermark(token: &[u8]) -> StoreResult<(String, Vec<u8>)> {
    if token.len() < 4 {
        return Err(StoreError::InvalidArgument(
            "invalid Spanner commit sync token".to_string(),
        ));
    }
    let ts_len = u32::from_be_bytes(token[..4].try_into().unwrap()) as usize;
    if ts_len == 0 || ts_len > 64 || token.len() - 4 < ts_len {
        return Err(StoreError::InvalidArgument(
            "invalid Spanner commit sync token".to_string(),
        ));
    }
    let ts = std::str::from_utf8(&token[4..4 + ts_len])
        .map_err(|_| StoreError::InvalidArgument("invalid Spanner commit sync token".to_string()))?
        .to_string();
    if !is_valid_spanner_rfc3339_utc(&ts) {
        return Err(StoreError::InvalidArgument(
            "invalid Spanner commit sync token".to_string(),
        ));
    }
    let after_id = &token[4 + ts_len..];
    if !after_id.is_empty() && after_id.len() != cc_common::COMMIT_ID_LENGTH {
        return Err(StoreError::InvalidArgument(
            "invalid Spanner commit sync token".to_string(),
        ));
    }
    Ok((ts, after_id.to_vec()))
}

pub(super) fn decode_workspace_row(repo_id: &str, row: &Row) -> StoreResult<WorkspaceState> {
    let user: String = row.get("User").map_err(map_spanner_err)?;
    let workspace_name: String = row.get("WorkspaceName").map_err(map_spanner_err)?;
    let commit_id: Vec<u8> = row.get("CommitId").map_err(map_spanner_err)?;
    let operation_id: Vec<u8> = row.get("OperationId").map_err(map_spanner_err)?;
    let tree_id: Vec<u8> = row.get("TreeId").map_err(map_spanner_err)?;
    let mut tree_ids: Vec<Vec<u8>> = row.get("TreeIds").map_err(map_spanner_err)?;
    if tree_ids.is_empty() && !tree_id.is_empty() {
        tree_ids.push(tree_id.clone());
    }
    let conflict_labels: Vec<String> = row.get("ConflictLabels").map_err(map_spanner_err)?;
    let sparse_patterns: Vec<String> = row.get("SparsePatterns").map_err(map_spanner_err)?;
    #[allow(deprecated)]
    Ok(WorkspaceState {
        repo_id: repo_id.to_string(),
        user,
        workspace_name,
        commit_id,
        operation_id,
        tree_id,
        sparse_patterns,
        tree_ids,
        conflict_labels,
        sparse_patterns_set: true,
    })
}

pub(super) fn decode_blob_row(row: &Row) -> StoreResult<Option<BlobRecord>> {
    let blob_id: Vec<u8> = row.get("BlobId").map_err(map_spanner_err)?;
    let content_size: i64 = row.get("ContentSize").map_err(map_spanner_err)?;
    let content_size = u64::try_from(content_size).map_err(|_| {
        StoreError::Corruption(format!("negative Spanner blob content_size {content_size}"))
    })?;
    let inline_content: Option<Vec<u8>> = row.get("InlineContent").map_err(map_spanner_err)?;
    let primary_location: Option<String> = row.get("PrimaryLocation").map_err(map_spanner_err)?;
    let replicated_locations: Vec<String> =
        row.get("ReplicatedLocations").map_err(map_spanner_err)?;

    Ok(Some(BlobRecord {
        blob_id,
        content_size,
        inline_content: inline_content.map(Bytes::from),
        primary_location,
        replicated_locations,
    }))
}

pub(super) fn decode_commit_proto(row: &Row) -> StoreResult<Commit> {
    let proto_bytes: Vec<u8> = row.get("ProtoBytes").map_err(map_spanner_err)?;
    Commit::decode(proto_bytes.as_slice())
        .map_err(|e| StoreError::Corruption(format!("failed to decode Spanner Commit: {e}")))
}

pub(super) fn decode_commit_generation(row: &Row) -> StoreResult<u64> {
    let gen_i64: i64 = row.get("Generation").map_err(map_spanner_err)?;
    u64::try_from(gen_i64).map_err(|_| {
        StoreError::Corruption(format!("negative Spanner commit generation {gen_i64}"))
    })
}

fn decode_tree_entry_row(row: &Row) -> StoreResult<TreeEntry> {
    let bytes: Vec<u8> = row.get("EntryProto").map_err(map_spanner_err)?;
    TreeEntry::decode(bytes.as_slice())
        .map_err(|e| StoreError::Corruption(format!("failed to decode Spanner TreeEntry: {e}")))
}

pub(super) async fn collect_tree_entries(mut rs: ResultSet) -> StoreResult<Vec<TreeEntry>> {
    let mut entries = Vec::new();
    while let Some(row) = rs.next().await.transpose().map_err(map_spanner_err)? {
        entries.push(decode_tree_entry_row(&row)?);
    }
    Ok(entries)
}

pub(super) fn decode_blob_content_size(row: &Row) -> StoreResult<u64> {
    let sz: i64 = row.get("ContentSize").map_err(map_spanner_err)?;
    u64::try_from(sz)
        .map_err(|_| StoreError::Corruption(format!("negative Spanner blob content_size {sz}")))
}

pub(super) fn build_commit_mutation(
    project_id: &str,
    commit_id: &[u8],
    commit: &Commit,
    generation: i64,
    inserted_at_shard: i64,
) -> Mutation {
    let proto_bytes = commit.encode_to_vec();
    Mutation::new_insert_builder("Commits")
        .set("ProjectId")
        .to(project_id)
        .set("CommitId")
        .to(commit_id)
        .set("ChangeId")
        .to(&commit.change_id)
        .set("Generation")
        .to(generation)
        .set("ProtoBytes")
        .to(&proto_bytes)
        .set("InsertedAtShard")
        .to(inserted_at_shard)
        .set("InsertedAt")
        .to("spanner.commit_timestamp()")
        .build()
}

pub(super) fn build_current_commit_watermark_stmt(project_id: &str) -> StatementBuilder {
    // Fan out 1-row reverse index-only seeks across all 16 shards so watermark capture
    // stays O(COMMIT_INSERTED_AT_SHARDS) instead of scanning all commits in the project.
    Statement::builder(
        "SELECT
           FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E9SZ', latest.InsertedAt) AS TsRfc3339,
           latest.CommitId
         FROM UNNEST(@shards) AS shard
         CROSS JOIN UNNEST(
           ARRAY(
             SELECT AS STRUCT c.InsertedAt, c.CommitId
             FROM Commits@{FORCE_INDEX=CommitsByInsertedAt} AS c
             WHERE c.ProjectId = @project_id
               AND c.InsertedAtShard = shard
               AND c.InsertedAt IS NOT NULL
             ORDER BY c.InsertedAt DESC, c.CommitId DESC
             LIMIT 1
           )
         ) AS latest
         ORDER BY latest.InsertedAt DESC, latest.CommitId DESC
         LIMIT 1",
    )
    .add_param("project_id", project_id)
    .add_typed_param(
        "shards",
        commit_inserted_at_shards(),
        spanner_types::array(spanner_types::int64()),
    )
}

pub(super) fn build_list_commit_ids_since_stmt(
    project_id: &str,
    ts_rfc3339: &str,
    after_id: &[u8],
    fetch_limit: i64,
) -> StatementBuilder {
    // Bound each shard's index-only scan to `@limit` rows before merging globally on
    // `(InsertedAt ASC, CommitId ASC)` so large catch-up deltas never scan unbounded shards.
    Statement::builder(
        "SELECT
           FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E9SZ', row_entry.InsertedAt) AS TsRfc3339,
           row_entry.CommitId
         FROM UNNEST(@shards) AS shard
         CROSS JOIN UNNEST(
           ARRAY(
             SELECT AS STRUCT c.InsertedAt, c.CommitId
             FROM Commits@{FORCE_INDEX=CommitsByInsertedAt} AS c
             WHERE c.ProjectId = @project_id
               AND c.InsertedAtShard = shard
               AND c.InsertedAt IS NOT NULL
               AND c.InsertedAt >= PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E9SZ', @ts_rfc3339)
               AND (
                 c.InsertedAt > PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E9SZ', @ts_rfc3339)
                 OR (
                   c.InsertedAt = PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E9SZ', @ts_rfc3339)
                   AND c.CommitId > @after_id
                 )
               )
             ORDER BY c.InsertedAt ASC, c.CommitId ASC
             LIMIT @limit
           )
         ) AS row_entry
         ORDER BY row_entry.InsertedAt ASC, row_entry.CommitId ASC
         LIMIT @limit",
    )
    .add_param("project_id", project_id)
    .add_typed_param(
        "shards",
        commit_inserted_at_shards(),
        spanner_types::array(spanner_types::int64()),
    )
    .add_param("ts_rfc3339", ts_rfc3339)
    .add_param("after_id", after_id)
    .add_param("limit", fetch_limit)
}

pub(super) fn build_tree_upsert_mutation(
    project_id: &str,
    tree_id: &[u8],
    ready: bool,
) -> Mutation {
    Mutation::new_insert_or_update_builder("Trees")
        .set("ProjectId")
        .to(project_id)
        .set("TreeId")
        .to(tree_id)
        .set("Ready")
        .to(ready)
        .build()
}

pub(super) fn build_tree_stage_insert_mutation(project_id: &str, tree_id: &[u8]) -> Mutation {
    Mutation::new_insert_builder("Trees")
        .set("ProjectId")
        .to(project_id)
        .set("TreeId")
        .to(tree_id)
        .set("Ready")
        .to(false)
        .build()
}

pub(super) fn build_tree_entry_mutation(
    project_id: &str,
    tree_id: &[u8],
    entry: &TreeEntry,
) -> Mutation {
    let entry_proto = entry.encode_to_vec();
    Mutation::new_insert_or_update_builder("TreeEntries")
        .set("ProjectId")
        .to(project_id)
        .set("TreeId")
        .to(tree_id)
        .set("Name")
        .to(&entry.name)
        .set("EntryProto")
        .to(&entry_proto)
        .build()
}

pub(super) fn build_symlink_mutation(
    project_id: &str,
    symlink_id: &[u8],
    target: &str,
) -> Mutation {
    Mutation::new_insert_or_update_builder("Symlinks")
        .set("ProjectId")
        .to(project_id)
        .set("SymlinkId")
        .to(symlink_id)
        .set("Target")
        .to(target)
        .build()
}

#[allow(clippy::ptr_arg)]
pub(super) fn build_blob_upsert_mutation(
    blob_id: &[u8],
    content_size: i64,
    inline_content: &Option<Vec<u8>>,
    primary_location: &Option<String>,
    replicated_locations: &Vec<String>,
) -> Mutation {
    Mutation::new_insert_or_update_builder("Blobs")
        .set("BlobId")
        .to(blob_id)
        .set("ContentSize")
        .to(content_size)
        .set("InlineContent")
        .to(inline_content)
        .set("PrimaryLocation")
        .to(primary_location)
        .set("ReplicatedLocations")
        .to(replicated_locations)
        .build()
}

pub(super) fn build_project_blob_upsert_mutation(project_id: &str, blob_id: &[u8]) -> Mutation {
    Mutation::new_insert_or_update_builder("ProjectBlobs")
        .set("ProjectId")
        .to(project_id)
        .set("BlobId")
        .to(blob_id)
        .build()
}

pub(super) fn build_workspace_delete_mutation(
    repo_id: &str,
    workspace_names: &[String],
) -> Mutation {
    let mut builder = KeySet::builder();
    for workspace_name in workspace_names {
        builder = builder.add_key(key![repo_id, workspace_name.as_str()]);
    }
    Mutation::delete("Workspaces", builder.build())
}

pub(super) fn build_binary_child_delete_mutation(
    table: &str,
    repo_id: &str,
    child_ids: &[Vec<u8>],
) -> Mutation {
    let mut builder = KeySet::builder();
    for child_id in child_ids {
        builder = builder.add_key(key![repo_id, child_id.as_slice()]);
    }
    Mutation::delete(table, builder.build())
}
