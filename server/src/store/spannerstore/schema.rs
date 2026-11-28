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

//! Cloud Spanner DDL parsing, schema compatibility guards, and auto-migration.
//!
//! # Authority and Crash-Safety Ordering
//! Auto-migration validates existing database DDL before running idempotent
//! `CREATE ... IF NOT EXISTS` statements, rejecting incompatible pre-split or
//! unmigrated schemas instead of layering incompatible columns onto live tables.
//!
//! # Cross-Process and RPC Boundaries
//! Routes emulator administration over HTTP/1.0 REST when explicitly enabled or
//! configured through emulator environment variables, and uses the Spanner
//! `DatabaseAdmin` gRPC API for Cloud Spanner and Spanner Omni.

use std::collections::HashMap;

use google_cloud_gax::error::rpc::Status as RpcStatus;
use google_cloud_spanner::client::Spanner;
use tracing::{info, warn};

use super::codec::{is_already_exists, is_not_found, map_spanner_admin_err};
use super::{SpannerConnectConfig, normalize_endpoint, split_database_resource};
use crate::store::{StoreError, StoreResult};

pub(super) const SPANNER_SCHEMA: &str = include_str!("../../../sql/spanner_schema.sql");

const MAX_EMULATOR_ADMIN_RESPONSE_BYTES: u64 = 1024 * 1024;

/// Number of hash shards prefixed ahead of `InsertedAt` per project in `CommitsByInsertedAt`
/// to distribute monotonically increasing commit timestamps across Cloud Spanner splits.
pub const COMMIT_INSERTED_AT_SHARDS: i64 = 16;

/// Maps `commit_id` deterministically into `0..COMMIT_INSERTED_AT_SHARDS`.
///
/// Uses 64-bit FNV-1a with a MurmurHash3 finalizer so both cryptographic digests and
/// synthetic commit IDs that differ only in trailing or upper-nibble bytes spread evenly
/// across shards without depending on Rust `RandomState` or `DefaultHasher` stability.
pub fn commit_inserted_at_shard(commit_id: &[u8]) -> i64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &byte in commit_id {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    hash ^= hash >> 33;
    (hash % (COMMIT_INSERTED_AT_SHARDS as u64)) as i64
}

pub(super) fn commit_inserted_at_shards() -> Vec<i64> {
    (0..COMMIT_INSERTED_AT_SHARDS).collect()
}

pub(super) fn parse_ddl_statements(sql: &str) -> Vec<String> {
    sql.split(';')
        .map(|stmt| {
            stmt.lines()
                .filter(|line| !line.trim_start().starts_with("--"))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn trim_leading_ws_and_comments(mut s: &str) -> &str {
    loop {
        s = s.trim_start();
        if let Some(after_dash) = s.strip_prefix("--") {
            s = after_dash.find('\n').map_or("", |nl| &after_dash[nl + 1..]);
        } else if let Some(after_open) = s.strip_prefix("/*") {
            s = after_open
                .find("*/")
                .map_or("", |end| &after_open[end + 2..]);
        } else {
            return s;
        }
    }
}

fn strip_ascii_keyword_prefix<'a>(s: &'a str, kw: &str) -> Option<&'a str> {
    let trimmed = trim_leading_ws_and_comments(s);
    if trimmed.len() < kw.len() || !trimmed.is_char_boundary(kw.len()) {
        return None;
    }
    let (head, rest) = trimmed.split_at(kw.len());
    if !head.eq_ignore_ascii_case(kw) {
        return None;
    }
    if rest.is_empty()
        || rest
            .as_bytes()
            .first()
            .is_some_and(|b| !(b.is_ascii_alphanumeric() || *b == b'_'))
    {
        Some(rest)
    } else {
        None
    }
}

fn take_sql_identifier(s: &str) -> Option<(String, bool, &str)> {
    let trimmed = trim_leading_ws_and_comments(s);
    if let Some(after_open) = trimmed.strip_prefix('`') {
        let mut chars = after_open.char_indices();
        while let Some((idx, ch)) = chars.next() {
            if ch == '\\' {
                let _ = chars.next();
            } else if ch == '`' {
                let ident = after_open[..idx].to_ascii_lowercase();
                if ident.is_empty() {
                    return None;
                }
                return Some((ident, true, &after_open[idx + 1..]));
            }
        }
        return None;
    }
    let end = trimmed
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(trimmed.len());
    if end == 0 {
        return None;
    }
    Some((trimmed[..end].to_ascii_lowercase(), false, &trimmed[end..]))
}

fn take_qualified_sql_identifier(s: &str) -> Option<(String, &str)> {
    let (mut ident, _, mut rest) = take_sql_identifier(s)?;
    while let Some(after_dot) = trim_leading_ws_and_comments(rest).strip_prefix('.') {
        let (segment, _, next_rest) = take_sql_identifier(after_dot)?;
        ident.push('.');
        ident.push_str(&segment);
        rest = next_rest;
    }
    Some((ident, rest))
}

fn find_matching_close_paren(body: &str) -> Option<usize> {
    let bytes = body.as_bytes();
    let len = body.len();
    let mut i = 0usize;
    let mut depth = 1usize;
    while i < len {
        match bytes[i] {
            b'-' if i + 1 < len && bytes[i + 1] == b'-' => {
                i += 2;
                while i < len && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < len && bytes[i + 1] == b'*' => {
                i += 2;
                while i + 1 < len && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                i = (i + 2).min(len);
            }
            b'\'' | b'"' | b'`' => {
                let quote = bytes[i];
                i += 1;
                while i < len {
                    if bytes[i] == b'\\' {
                        i += 1;
                        if i < len {
                            i += body[i..].chars().next().map_or(1, char::len_utf8);
                        }
                    } else if bytes[i] == quote {
                        i += 1;
                        break;
                    } else {
                        i += body[i..].chars().next().map_or(1, char::len_utf8);
                    }
                }
            }
            b'(' => {
                depth += 1;
                i += 1;
            }
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
                i += 1;
            }
            _ => {
                i += body[i..].chars().next().map_or(1, char::len_utf8);
            }
        }
    }
    None
}

fn split_top_level_comma_items(s: &str) -> Vec<&str> {
    let bytes = s.as_bytes();
    let len = s.len();
    let mut items = Vec::new();
    let mut item_start = 0usize;
    let mut paren_depth = 0usize;
    let mut angle_depth = 0usize;
    let mut i = 0usize;
    while i < len {
        match bytes[i] {
            b'-' if i + 1 < len && bytes[i + 1] == b'-' => {
                i += 2;
                while i < len && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < len && bytes[i + 1] == b'*' => {
                i += 2;
                while i + 1 < len && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                i = (i + 2).min(len);
            }
            b'\'' | b'"' | b'`' => {
                let quote = bytes[i];
                i += 1;
                while i < len {
                    if bytes[i] == b'\\' {
                        i += 1;
                        if i < len {
                            i += s[i..].chars().next().map_or(1, char::len_utf8);
                        }
                    } else if bytes[i] == quote {
                        i += 1;
                        break;
                    } else {
                        i += s[i..].chars().next().map_or(1, char::len_utf8);
                    }
                }
            }
            b'(' => {
                paren_depth += 1;
                i += 1;
            }
            b')' => {
                paren_depth = paren_depth.saturating_sub(1);
                i += 1;
            }
            b'<' if paren_depth == 0 => {
                angle_depth += 1;
                i += 1;
            }
            b'>' if paren_depth == 0 => {
                angle_depth = angle_depth.saturating_sub(1);
                i += 1;
            }
            b',' if paren_depth == 0 && angle_depth == 0 => {
                items.push(&s[item_start..i]);
                i += 1;
                item_start = i;
            }
            _ => {
                i += s[i..].chars().next().map_or(1, char::len_utf8);
            }
        }
    }
    items.push(&s[item_start..]);
    items
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SqlToken<'a> {
    Ident { text: &'a str, is_quoted: bool },
    Parens,
    StringLit,
    Punct(char),
}

fn is_unquoted_keyword(tok: &SqlToken<'_>, kw: &str) -> bool {
    matches!(tok, SqlToken::Ident { text, is_quoted: false } if text.eq_ignore_ascii_case(kw))
}

fn tokenize_top_level(input: &str) -> Vec<SqlToken<'_>> {
    let bytes = input.as_bytes();
    let len = input.len();
    let mut tokens = Vec::new();
    let mut i = 0usize;
    while i < len {
        match bytes[i] {
            b if b.is_ascii_whitespace() => {
                i += 1;
            }
            b'-' if i + 1 < len && bytes[i + 1] == b'-' => {
                i += 2;
                while i < len && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < len && bytes[i + 1] == b'*' => {
                i += 2;
                while i + 1 < len && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                i = (i + 2).min(len);
            }
            b'`' => {
                let start = i + 1;
                i += 1;
                let mut closed = false;
                while i < len {
                    if bytes[i] == b'\\' {
                        i += 1;
                        if i < len {
                            i += input[i..].chars().next().map_or(1, char::len_utf8);
                        }
                    } else if bytes[i] == b'`' {
                        tokens.push(SqlToken::Ident {
                            text: &input[start..i],
                            is_quoted: true,
                        });
                        i += 1;
                        closed = true;
                        break;
                    } else {
                        i += input[i..].chars().next().map_or(1, char::len_utf8);
                    }
                }
                if !closed {
                    tokens.push(SqlToken::Ident {
                        text: &input[start..len],
                        is_quoted: true,
                    });
                }
            }
            b'\'' | b'"' => {
                let quote = bytes[i];
                i += 1;
                while i < len {
                    if bytes[i] == b'\\' {
                        i += 1;
                        if i < len {
                            i += input[i..].chars().next().map_or(1, char::len_utf8);
                        }
                    } else if bytes[i] == quote {
                        i += 1;
                        break;
                    } else {
                        i += input[i..].chars().next().map_or(1, char::len_utf8);
                    }
                }
                tokens.push(SqlToken::StringLit);
            }
            b'(' => {
                if let Some(rel_close) = find_matching_close_paren(&input[i + 1..]) {
                    i = i + 1 + rel_close + 1;
                } else {
                    i = len;
                }
                tokens.push(SqlToken::Parens);
            }
            b if b.is_ascii_alphanumeric() || b == b'_' => {
                let start = i;
                i += 1;
                while i < len && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                tokens.push(SqlToken::Ident {
                    text: &input[start..i],
                    is_quoted: false,
                });
            }
            _ => {
                let ch = input[i..].chars().next().unwrap();
                tokens.push(SqlToken::Punct(ch));
                i += ch.len_utf8();
            }
        }
    }
    tokens
}

fn parse_create_table_ddl(stmt: &str) -> Option<(String, HashMap<String, String>, Vec<String>)> {
    let rest = strip_ascii_keyword_prefix(stmt, "CREATE")?;
    let mut rest = strip_ascii_keyword_prefix(rest, "TABLE")?;
    if let Some(after_if) = strip_ascii_keyword_prefix(rest, "IF")
        && let Some(after_not) = strip_ascii_keyword_prefix(after_if, "NOT")
        && let Some(after_exists) = strip_ascii_keyword_prefix(after_not, "EXISTS")
    {
        rest = after_exists;
    }
    let (table_name, after_name) = take_qualified_sql_identifier(rest)?;
    let body = trim_leading_ws_and_comments(after_name).strip_prefix('(')?;
    let close_pos = find_matching_close_paren(body)?;
    let cols_block = &body[..close_pos];
    let tail = &body[close_pos + 1..];

    let mut columns = HashMap::new();
    for item in split_top_level_comma_items(cols_block) {
        let Some((tok, is_quoted, def_rest)) = take_sql_identifier(item) else {
            continue;
        };
        if !is_quoted
            && matches!(
                tok.as_str(),
                "constraint" | "foreign" | "primary" | "check" | "synonym" | "row"
            )
        {
            continue;
        }
        columns.insert(tok, def_rest.trim().to_string());
    }

    let pk_cols = parse_primary_key_cols(tail);
    Some((table_name, columns, pk_cols))
}

fn parse_primary_key_cols(tail: &str) -> Vec<String> {
    let Some(after_primary) = strip_ascii_keyword_prefix(tail, "PRIMARY") else {
        return Vec::new();
    };
    let Some(after_key) = strip_ascii_keyword_prefix(after_primary, "KEY") else {
        return Vec::new();
    };
    let Some(rest) = after_key.trim_start().strip_prefix('(') else {
        return Vec::new();
    };
    let Some(close) = rest.find(')') else {
        return Vec::new();
    };
    rest[..close]
        .split(',')
        .filter_map(|part| {
            let trimmed = part.trim();
            if trimmed.is_empty() {
                return None;
            }
            let first = trimmed.split_ascii_whitespace().next().unwrap_or("");
            let col = first.trim_matches('`').to_ascii_lowercase();
            (!col.is_empty()).then_some(col)
        })
        .collect()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ParsedCreateIndex {
    pub(super) index_name: String,
    pub(super) table_name: String,
    pub(super) is_null_filtered: bool,
    pub(super) has_where_clause: bool,
    /// Ordered index key columns as `(lowercase_column_name, is_desc)`.
    pub(super) key_cols: Vec<(String, bool)>,
}

/// Parses `CREATE [UNIQUE] [NULL_FILTERED] INDEX [IF NOT EXISTS] <name> ON <table>(<cols...>)`
/// along with null-filtering, `WHERE` filter presence, and per-column sort direction.
pub(super) fn parse_create_index_ddl(stmt: &str) -> Option<ParsedCreateIndex> {
    let mut rest = strip_ascii_keyword_prefix(stmt, "CREATE")?;
    if let Some(after_unique) = strip_ascii_keyword_prefix(rest, "UNIQUE") {
        rest = after_unique;
    }
    let is_null_filtered = if let Some(after_nf) = strip_ascii_keyword_prefix(rest, "NULL_FILTERED")
    {
        rest = after_nf;
        true
    } else {
        false
    };
    let mut rest = strip_ascii_keyword_prefix(rest, "INDEX")?;
    if let Some(after_if) = strip_ascii_keyword_prefix(rest, "IF")
        && let Some(after_not) = strip_ascii_keyword_prefix(after_if, "NOT")
        && let Some(after_exists) = strip_ascii_keyword_prefix(after_not, "EXISTS")
    {
        rest = after_exists;
    }
    let (index_name, rest) = take_qualified_sql_identifier(rest)?;
    let after_on = strip_ascii_keyword_prefix(rest, "ON")?;
    let (table_name, after_table) = take_qualified_sql_identifier(after_on)?;
    let body = trim_leading_ws_and_comments(after_table).strip_prefix('(')?;
    let close_idx = find_matching_close_paren(body)?;
    let cols_block = &body[..close_idx];
    let tail = &body[close_idx + 1..];

    let mut key_cols = Vec::new();
    for piece_tokens in
        tokenize_top_level(cols_block).split(|tok| matches!(tok, SqlToken::Punct(',')))
    {
        let Some(SqlToken::Ident { text, .. }) = piece_tokens.first() else {
            continue;
        };
        let col = text.to_ascii_lowercase();
        if col.is_empty() {
            continue;
        }
        let is_desc = piece_tokens[1..]
            .iter()
            .any(|tok| is_unquoted_keyword(tok, "DESC"));
        key_cols.push((col, is_desc));
    }

    let has_where_clause = tokenize_top_level(tail)
        .iter()
        .any(|tok| is_unquoted_keyword(tok, "WHERE"));

    Some(ParsedCreateIndex {
        index_name,
        table_name,
        is_null_filtered,
        has_where_clause,
        key_cols,
    })
}

fn validate_inserted_at_shard_column_def(col_def: &str) -> StoreResult<()> {
    let tokens = tokenize_top_level(col_def);
    let is_int64 = tokens
        .first()
        .is_some_and(|tok| is_unquoted_keyword(tok, "INT64"))
        && !matches!(tokens.get(1), Some(SqlToken::Parens | SqlToken::Punct('<')));
    if !is_int64 {
        return Err(StoreError::InvalidArgument(format!(
            "detected incompatible Spanner database schema ('Commits.InsertedAtShard' has definition '{col_def}', \
             expected non-generated 'INT64 NOT NULL'); \
             migrate Commits.InsertedAtShard to INT64, backfill every row, and run \
             ALTER TABLE Commits ALTER COLUMN InsertedAtShard INT64 NOT NULL \
             (or provision a fresh Spanner database) before starting jj-cc-server with --spanner-auto-migrate"
        )));
    }
    let is_generated = tokens
        .iter()
        .any(|tok| is_unquoted_keyword(tok, "AS") || is_unquoted_keyword(tok, "GENERATED"));
    if is_generated {
        return Err(StoreError::InvalidArgument(format!(
            "detected incompatible Spanner database schema ('Commits.InsertedAtShard' is a generated column ('{col_def}'), \
             expected a writable non-generated 'INT64 NOT NULL' column); \
             replace it with a writable INT64 column, backfill every row, and run \
             ALTER TABLE Commits ALTER COLUMN InsertedAtShard INT64 NOT NULL \
             (or provision a fresh Spanner database) before starting jj-cc-server with --spanner-auto-migrate"
        )));
    }
    let is_not_null = tokens
        .windows(2)
        .any(|pair| is_unquoted_keyword(&pair[0], "NOT") && is_unquoted_keyword(&pair[1], "NULL"));
    if !is_not_null {
        return Err(StoreError::InvalidArgument(format!(
            "detected incompatible Spanner database schema ('Commits.InsertedAtShard' is nullable ('{col_def}'), \
             expected 'INT64 NOT NULL'); \
             backfill Commits.InsertedAtShard for every row and run \
             ALTER TABLE Commits ALTER COLUMN InsertedAtShard INT64 NOT NULL \
             before starting jj-cc-server with --spanner-auto-migrate"
        )));
    }
    Ok(())
}

/// Rejects pre-split PoC, pre-migration `Workspaces`, and unsharded `CommitsByInsertedAt` Spanner
/// databases before `--spanner-auto-migrate` runs `CREATE ... IF NOT EXISTS`, matching
/// [`crate::store::SqliteMetadataStore`]'s fail-fast guard.
pub(super) fn validate_existing_spanner_ddl(existing_statements: &[String]) -> StoreResult<()> {
    let mut tables: HashMap<String, (HashMap<String, String>, Vec<String>)> = HashMap::new();
    let mut indexes: HashMap<String, ParsedCreateIndex> = HashMap::new();
    for stmt in existing_statements {
        if let Some((table_name, cols, pk_cols)) = parse_create_table_ddl(stmt) {
            tables.insert(table_name, (cols, pk_cols));
        } else if let Some(parsed_index) = parse_create_index_ddl(stmt) {
            indexes.insert(parsed_index.index_name.clone(), parsed_index);
        }
    }

    for legacy_table in ["repos", "op_heads", "files"] {
        if tables.contains_key(legacy_table) {
            return Err(StoreError::InvalidArgument(format!(
                "detected incompatible pre-split PoC Spanner database (found legacy '{legacy_table}' table); \
                 provision a fresh Spanner database before starting jj-cc-server with --spanner-auto-migrate"
            )));
        }
    }

    if let Some((ws_cols, _)) = tables.get("workspaces")
        && ws_cols.contains_key("useremail")
    {
        return Err(StoreError::InvalidArgument(
            "detected incompatible Spanner database ('Workspaces' table contains legacy 'UserEmail' column); \
             migrate 'Workspaces' to PRIMARY KEY(RepoId, WorkspaceName) with mutable non-key 'User' before starting jj-cc-server"
                .to_string(),
        ));
    }

    const REQUIRED_COLUMNS: &[(&str, &[&str])] = &[
        ("projects", &["projectid"]),
        (
            "commits",
            &[
                "projectid",
                "commitid",
                "changeid",
                "generation",
                "protobytes",
                "insertedatshard",
                "insertedat",
            ],
        ),
        ("trees", &["projectid", "treeid", "ready"]),
        ("operations", &["repoid", "operationid", "protobytes"]),
        ("views", &["repoid", "viewid", "protobytes"]),
        ("workspaces", &["repoid", "workspacename", "user"]),
    ];
    for (table, required_cols) in REQUIRED_COLUMNS {
        if let Some((existing_cols, _)) = tables.get(*table) {
            for col in *required_cols {
                if !existing_cols.contains_key(*col) {
                    return Err(StoreError::InvalidArgument(format!(
                        "detected incompatible Spanner database schema ('{table}' table lacks '{col}' column); \
                         migrate the schema or provision a fresh Spanner database before starting jj-cc-server with --spanner-auto-migrate"
                    )));
                }
            }
        }
    }

    if let Some((commits_cols, _)) = tables.get("commits")
        && let Some(shard_def) = commits_cols.get("insertedatshard")
    {
        validate_inserted_at_shard_column_def(shard_def)?;
    }

    if let Some((_, ws_pk)) = tables.get("workspaces")
        && ws_pk.as_slice() != ["repoid", "workspacename"]
    {
        return Err(StoreError::InvalidArgument(format!(
            "detected incompatible Spanner database ('Workspaces' table has PRIMARY KEY({}), expected PRIMARY KEY(RepoId, WorkspaceName)); \
             migrate 'Workspaces' before starting jj-cc-server",
            ws_pk.join(", ")
        )));
    }

    const EXPECTED_COMMITS_BY_INSERTED_AT: &[(&str, bool)] = &[
        ("projectid", false),
        ("insertedatshard", false),
        ("insertedat", false),
        ("commitid", false),
    ];
    if let Some(index) = indexes.get("commitsbyinsertedat") {
        if index.table_name != "commits" {
            return Err(StoreError::InvalidArgument(format!(
                "detected incompatible Spanner database schema ('CommitsByInsertedAt' index targets table '{}' \
                 instead of 'Commits'); \
                 drop and recreate the index on Commits(ProjectId, InsertedAtShard, InsertedAt, CommitId), \
                 or provision a fresh Spanner database before starting jj-cc-server with --spanner-auto-migrate",
                index.table_name
            )));
        }
        if index.is_null_filtered || index.has_where_clause {
            let filter_kind = match (index.is_null_filtered, index.has_where_clause) {
                (true, true) => "NULL_FILTERED with a WHERE clause",
                (true, false) => "NULL_FILTERED",
                (false, true) => "filtered with a WHERE clause",
                (false, false) => unreachable!(),
            };
            return Err(StoreError::InvalidArgument(format!(
                "detected incompatible Spanner database schema ('CommitsByInsertedAt' index is {filter_kind}, \
                 expected an unfiltered index on Commits(ProjectId, InsertedAtShard, InsertedAt, CommitId)); \
                 drop and recreate the index without NULL_FILTERED or WHERE, or provision a fresh Spanner database \
                 before starting jj-cc-server with --spanner-auto-migrate"
            )));
        }
        let actual: Vec<(&str, bool)> = index
            .key_cols
            .iter()
            .map(|(col, desc)| (col.as_str(), *desc))
            .collect();
        if actual != EXPECTED_COMMITS_BY_INSERTED_AT {
            let formatted = index
                .key_cols
                .iter()
                .map(|(col, desc)| {
                    if *desc {
                        format!("{col} DESC")
                    } else {
                        col.clone()
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            return Err(StoreError::InvalidArgument(format!(
                "detected incompatible Spanner database schema ('CommitsByInsertedAt' index has key columns ({formatted}) \
                 instead of ascending (ProjectId, InsertedAtShard, InsertedAt, CommitId)); \
                 drop and recreate the index after backfilling Commits.InsertedAtShard, or provision a fresh Spanner database \
                 before starting jj-cc-server with --spanner-auto-migrate"
            )));
        }
    }
    Ok(())
}

pub(super) fn parse_emulator_ddl_statements(http_resp: &str) -> StoreResult<Vec<String>> {
    let body = http_resp
        .split_once("\r\n\r\n")
        .or_else(|| http_resp.split_once("\n\n"))
        .map_or(http_resp, |(_, b)| b)
        .trim();
    let json: serde_json::Value = serde_json::from_str(body).map_err(|e| {
        StoreError::Backend(format!(
            "Spanner emulator get_database_ddl returned non-JSON response: {e}"
        ))
    })?;
    let Some(stmts_val) = json.get("statements").filter(|v| !v.is_null()) else {
        return Ok(Vec::new());
    };
    let arr = stmts_val.as_array().ok_or_else(|| {
        StoreError::Backend(format!(
            "Spanner emulator get_database_ddl returned non-array 'statements': {json}"
        ))
    })?;
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        let s = item.as_str().ok_or_else(|| {
            StoreError::Backend(format!(
                "Spanner emulator get_database_ddl returned non-string statement: {item}"
            ))
        })?;
        out.push(s.to_string());
    }
    Ok(out)
}

pub(super) fn derive_emulator_rest_endpoint(emulator_host: &str) -> StoreResult<String> {
    let normalized = normalize_endpoint(emulator_host, false)?;
    let clean = normalized.strip_prefix("http://").ok_or_else(|| {
        StoreError::InvalidArgument(format!(
            "Spanner emulator endpoint must use http:// or omit scheme (got '{emulator_host}')"
        ))
    })?;
    Ok(clean
        .strip_suffix(":9010")
        .map(|host| format!("http://{host}:9020"))
        .unwrap_or_else(|| format!("http://{clean}")))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum AdminEndpointTarget {
    DatabaseAdmin { endpoint: Option<String> },
    EmulatorHttp10 { host_port: String },
}

pub(super) fn resolve_admin_endpoint(
    config: &SpannerConnectConfig,
    emulator_host_env: Option<&str>,
    emulator_rest_host_env: Option<&str>,
) -> StoreResult<AdminEndpointTarget> {
    let emulator_host = emulator_host_env.map(str::trim).filter(|s| !s.is_empty());
    let emulator_rest_host = emulator_rest_host_env
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let explicit_admin = config
        .admin_endpoint
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let config_endpoint = config
        .endpoint
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let tls_or_omni = config.omni
        || config.omni_ca_cert.is_some()
        || config.omni_client_cert.is_some()
        || config.omni_client_key.is_some();
    let is_emulator = !tls_or_omni
        && (config.emulator || emulator_host.is_some() || emulator_rest_host.is_some());

    if is_emulator {
        let ep = if let Some(explicit) = explicit_admin {
            normalize_endpoint(explicit, false)?
        } else if let Some(rest_host) = emulator_rest_host {
            normalize_endpoint(rest_host, false)?
        } else if let Some(emu_host) = emulator_host {
            derive_emulator_rest_endpoint(emu_host)?
        } else if let Some(cfg_ep) = config_endpoint {
            derive_emulator_rest_endpoint(cfg_ep)?
        } else {
            return Ok(AdminEndpointTarget::DatabaseAdmin { endpoint: None });
        };
        if let Some(host_port) = ep.strip_prefix("http://") {
            return Ok(AdminEndpointTarget::EmulatorHttp10 {
                host_port: host_port.to_string(),
            });
        }
        return Ok(AdminEndpointTarget::DatabaseAdmin { endpoint: Some(ep) });
    }

    if let Some(explicit) = explicit_admin {
        let normalized = normalize_endpoint(explicit, true)?;
        if normalized.starts_with("http://") {
            return Err(StoreError::InvalidArgument(
                "plaintext http:// Spanner admin_endpoint is only permitted when targeting the Spanner emulator (--spanner-emulator, SPANNER_EMULATOR_HOST, or SPANNER_EMULATOR_REST_HOST)".to_string(),
            ));
        }
        return Ok(AdminEndpointTarget::DatabaseAdmin {
            endpoint: Some(normalized),
        });
    }

    Ok(AdminEndpointTarget::DatabaseAdmin { endpoint: None })
}

pub(super) async fn ensure_database_and_schema(
    spanner: &Spanner,
    config: &SpannerConnectConfig,
) -> StoreResult<()> {
    let (instance_path, db_id) = split_database_resource(&config.database)?;
    let ddl_statements = parse_ddl_statements(SPANNER_SCHEMA);

    let emulator_host_env = std::env::var("SPANNER_EMULATOR_HOST").ok();
    let emulator_rest_host_env = std::env::var("SPANNER_EMULATOR_REST_HOST").ok();
    let admin_ep = match resolve_admin_endpoint(
        config,
        emulator_host_env.as_deref(),
        emulator_rest_host_env.as_deref(),
    )? {
        AdminEndpointTarget::EmulatorHttp10 { host_port } => {
            return ensure_database_and_schema_emulator_http10(
                &host_port,
                instance_path,
                db_id,
                &config.database,
                &ddl_statements,
            )
            .await;
        }
        AdminEndpointTarget::DatabaseAdmin { endpoint } => endpoint,
    };

    let mut admin_builder = spanner.database_admin_builder();
    if let Some(ep) = admin_ep {
        admin_builder = admin_builder.with_endpoint(ep);
    }

    let admin_client = admin_builder.build().await.map_err(|e| {
        StoreError::Unavailable(format!("failed to build Spanner DatabaseAdmin client: {e}"))
    })?;

    match admin_client
        .get_database()
        .set_name(&config.database)
        .send()
        .await
    {
        Ok(_) => {
            let existing_ddl = admin_client
                .get_database_ddl()
                .set_database(&config.database)
                .send()
                .await
                .map_err(map_spanner_admin_err)?;
            validate_existing_spanner_ddl(&existing_ddl.statements)?;
            if !ddl_statements.is_empty() {
                info!("Applying idempotent Spanner DDL to {}", config.database);
                let op = admin_client
                    .update_database_ddl()
                    .set_database(&config.database)
                    .set_statements(ddl_statements)
                    .send()
                    .await
                    .map_err(map_spanner_admin_err)?;
                let op_error = op.error().map(|s| RpcStatus::from(&**s));
                wait_admin_op(&admin_client, op.name, op.done, op_error).await?;
            }
        }
        Err(err) if is_not_found(&err) => {
            info!(
                "Creating Spanner database {} under {}",
                db_id, instance_path
            );
            let create_res = admin_client
                .create_database()
                .set_parent(instance_path)
                .set_create_statement(format!("CREATE DATABASE `{db_id}`"))
                .set_extra_statements(ddl_statements.clone())
                .send()
                .await;
            match create_res {
                Ok(op) => {
                    let op_error = op.error().map(|s| RpcStatus::from(&**s));
                    wait_admin_op(&admin_client, op.name, op.done, op_error).await?;
                }
                Err(e) if is_already_exists(&e) => {
                    warn!(
                        "Database {} was created concurrently; ensuring DDL",
                        config.database
                    );
                    let existing_ddl = admin_client
                        .get_database_ddl()
                        .set_database(&config.database)
                        .send()
                        .await
                        .map_err(map_spanner_admin_err)?;
                    validate_existing_spanner_ddl(&existing_ddl.statements)?;
                    let op = admin_client
                        .update_database_ddl()
                        .set_database(&config.database)
                        .set_statements(ddl_statements)
                        .send()
                        .await
                        .map_err(map_spanner_admin_err)?;
                    let op_error = op.error().map(|s| RpcStatus::from(&**s));
                    wait_admin_op(&admin_client, op.name, op.done, op_error).await?;
                }
                Err(e) => return Err(map_spanner_admin_err(e)),
            }
        }
        Err(err) => return Err(map_spanner_admin_err(err)),
    }

    Ok(())
}

async fn ensure_database_and_schema_emulator_http10(
    host_port: &str,
    instance_path: &str,
    db_id: &str,
    database_path: &str,
    ddl_statements: &[String],
) -> StoreResult<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn http10_json(
        host_port: &str,
        method: &str,
        path: &str,
        body: &str,
    ) -> StoreResult<(u16, String)> {
        if host_port.is_empty()
            || host_port
                .bytes()
                .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
        {
            return Err(StoreError::InvalidArgument(format!(
                "invalid Spanner emulator host '{host_port}'"
            )));
        }
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
        let mut stream = loop {
            match tokio::net::TcpStream::connect(host_port).await {
                Ok(s) => break s,
                Err(_e) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
                }
                Err(e) => {
                    return Err(StoreError::Unavailable(format!(
                        "failed to connect to Spanner emulator admin {host_port}: {e}"
                    )));
                }
            }
        };
        // HTTP/1.0 avoids chunked binary gRPC server-stats trailers emitted by gateway_main.
        let req = if body.is_empty() {
            format!("{method} {path} HTTP/1.0\r\nHost: {host_port}\r\n\r\n")
        } else {
            format!(
                "{method} {path} HTTP/1.0\r\nHost: {host_port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            )
        };
        let raw = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            stream.write_all(req.as_bytes()).await?;
            let mut buf = Vec::new();
            stream
                .take(MAX_EMULATOR_ADMIN_RESPONSE_BYTES + 1)
                .read_to_end(&mut buf)
                .await?;
            if (buf.len() as u64) > MAX_EMULATOR_ADMIN_RESPONSE_BYTES {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "Spanner emulator admin response exceeded {MAX_EMULATOR_ADMIN_RESPONSE_BYTES} bytes"
                    ),
                ));
            }
            Ok::<Vec<u8>, std::io::Error>(buf)
        })
        .await
        .map_err(|_| {
            StoreError::Unavailable(format!(
                "timed out talking to Spanner emulator admin {host_port}"
            ))
        })?
        .map_err(|e| {
            StoreError::Unavailable(format!(
                "failed talking to Spanner emulator admin {host_port}: {e}"
            ))
        })?;
        let resp = String::from_utf8_lossy(&raw).into_owned();
        let code = resp
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|c| c.parse::<u16>().ok())
            .unwrap_or(0);
        Ok((code, resp))
    }

    if let Some((project_path, instance_id)) = instance_path.rsplit_once("/instances/") {
        let inst_payload = serde_json::json!({
            "instanceId": instance_id,
            "instance": {
                "config": format!("{project_path}/instanceConfigs/emulator-config"),
                "displayName": instance_id,
                "nodeCount": 1,
            }
        })
        .to_string();
        let inst_url_path = format!("/v1/{project_path}/instances");
        let (inst_status, inst_resp) =
            http10_json(host_port, "POST", &inst_url_path, &inst_payload).await?;
        if inst_status != 200 && inst_status != 409 {
            return Err(StoreError::Backend(format!(
                "Spanner emulator create_instance failed (HTTP {inst_status}): {inst_resp}"
            )));
        }
    }

    let create_payload = serde_json::json!({
        "createStatement": format!("CREATE DATABASE `{db_id}`"),
        "extraStatements": ddl_statements,
    })
    .to_string();
    let create_path = format!("/v1/{instance_path}/databases");
    let (status, resp) = http10_json(host_port, "POST", &create_path, &create_payload).await?;
    if status == 200 {
        return check_emulator_lro_json(&resp);
    }
    if status == 409 {
        let ddl_path = format!("/v1/{database_path}/ddl");
        let (get_status, get_resp) = http10_json(host_port, "GET", &ddl_path, "").await?;
        if get_status != 200 {
            return Err(StoreError::Backend(format!(
                "Spanner emulator get_database_ddl failed (HTTP {get_status}): {get_resp}"
            )));
        }
        let existing_ddl = parse_emulator_ddl_statements(&get_resp)?;
        validate_existing_spanner_ddl(&existing_ddl)?;
        if ddl_statements.is_empty() {
            return Ok(());
        }
        let patch_payload = serde_json::json!({ "statements": ddl_statements }).to_string();
        let (ddl_status, ddl_resp) =
            http10_json(host_port, "PATCH", &ddl_path, &patch_payload).await?;
        if ddl_status == 200 {
            return check_emulator_lro_json(&ddl_resp);
        }
        return Err(StoreError::Backend(format!(
            "Spanner emulator update_database_ddl failed (HTTP {ddl_status}): {ddl_resp}"
        )));
    }
    Err(StoreError::Backend(format!(
        "Spanner emulator create_database failed (HTTP {status}): {resp}"
    )))
}

pub(super) fn check_emulator_lro_json(http_resp: &str) -> StoreResult<()> {
    let body = http_resp
        .split_once("\r\n\r\n")
        .or_else(|| http_resp.split_once("\n\n"))
        .map_or(http_resp, |(_, b)| b)
        .trim();
    let json: serde_json::Value = serde_json::from_str(body).map_err(|e| {
        StoreError::Backend(format!(
            "Spanner emulator DDL returned non-JSON response: {e}"
        ))
    })?;
    if let Some(op_err) = json.get("error").filter(|v| !v.is_null()) {
        return Err(StoreError::Backend(format!(
            "Spanner emulator DDL operation failed: {op_err}"
        )));
    }
    if json.get("done").and_then(serde_json::Value::as_bool) != Some(true) {
        return Err(StoreError::Backend(format!(
            "Spanner emulator DDL operation did not complete: {json}"
        )));
    }
    Ok(())
}

async fn wait_admin_op(
    admin_client: &google_cloud_spanner::client::DatabaseAdmin,
    op_name: String,
    already_done: bool,
    op_error: Option<RpcStatus>,
) -> StoreResult<()> {
    if let Some(outcome) = finished_admin_op(already_done, op_error) {
        return outcome;
    }
    if op_name.is_empty() {
        return Ok(());
    }
    const MAX_CONSECUTIVE_POLL_ERRORS: u32 = 5;
    let mut consecutive_errors = 0u32;
    for _ in 0..1200 {
        let op = match admin_client.get_operation().set_name(&op_name).send().await {
            Ok(op) => {
                consecutive_errors = 0;
                op
            }
            Err(err) => {
                consecutive_errors += 1;
                if consecutive_errors >= MAX_CONSECUTIVE_POLL_ERRORS {
                    return Err(map_spanner_admin_err(err));
                }
                tracing::warn!(
                    "Transient error polling Spanner DDL operation '{op_name}' ({consecutive_errors}/{MAX_CONSECUTIVE_POLL_ERRORS}): {err}"
                );
                let backoff_ms = 250u64.saturating_mul(1u64 << (consecutive_errors - 1));
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms.min(2_000))).await;
                continue;
            }
        };
        if let Some(outcome) = finished_admin_op(op.done, op.error().map(|s| RpcStatus::from(&**s)))
        {
            return outcome;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    Err(StoreError::Unavailable(format!(
        "timed out waiting for Spanner DDL operation '{op_name}'"
    )))
}

/// Returns the outcome of a long-running admin operation once it has finished. DDL statements
/// that fail while they run, rather than during validation, are reported only through the
/// operation's error result.
pub(super) fn finished_admin_op(
    done: bool,
    op_error: Option<RpcStatus>,
) -> Option<StoreResult<()>> {
    match op_error {
        Some(status) => Some(Err(map_spanner_admin_err(
            google_cloud_gax::error::Error::service(status),
        ))),
        None if done => Some(Ok(())),
        None => None,
    }
}
