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

use std::collections::{HashMap, HashSet};

use google_cloud_gax::error::rpc::Status as RpcStatus;
use google_cloud_spanner::client::Spanner;
use tracing::{info, warn};

use super::codec::{is_already_exists, is_not_found, map_spanner_admin_err};
use super::{SpannerConnectConfig, normalize_endpoint, split_database_resource};
use crate::store::{StoreError, StoreResult};

pub(super) const SPANNER_SCHEMA: &str = include_str!("../../../sql/spanner_schema.sql");

const MAX_EMULATOR_ADMIN_RESPONSE_BYTES: u64 = 1024 * 1024;

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

fn strip_ascii_keyword_prefix<'a>(s: &'a str, kw: &str) -> Option<&'a str> {
    let trimmed = s.trim_start();
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
            .is_some_and(|b| b.is_ascii_whitespace() || *b == b'(' || *b == b'`')
    {
        Some(rest)
    } else {
        None
    }
}

fn parse_create_table_ddl(stmt: &str) -> Option<(String, HashSet<String>, Vec<String>)> {
    let rest = strip_ascii_keyword_prefix(stmt, "CREATE")?;
    let mut rest = strip_ascii_keyword_prefix(rest, "TABLE")?;
    if let Some(after_if) = strip_ascii_keyword_prefix(rest, "IF")
        && let Some(after_not) = strip_ascii_keyword_prefix(after_if, "NOT")
        && let Some(after_exists) = strip_ascii_keyword_prefix(after_not, "EXISTS")
    {
        rest = after_exists;
    }
    let rest = rest.trim_start();
    let name_end = rest.find(|c: char| c.is_ascii_whitespace() || c == '(')?;
    let raw_table = rest[..name_end].trim().trim_matches('`');
    if raw_table.is_empty() {
        return None;
    }
    let table_name = raw_table.to_ascii_lowercase();

    let after_name = &rest[name_end..];
    let open_idx = after_name.find('(')?;
    let body = &after_name[open_idx + 1..];

    // Match the closing `)` of the column definition block, ignoring nested type/option parens
    // and quoted literals.
    let mut depth = 1usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut close_idx = None;
    let mut chars = body.char_indices().peekable();
    while let Some((idx, ch)) = chars.next() {
        if in_single {
            if ch == '\\' {
                let _ = chars.next();
            } else if ch == '\'' {
                in_single = false;
            }
            continue;
        }
        if in_double {
            if ch == '\\' {
                let _ = chars.next();
            } else if ch == '"' {
                in_double = false;
            }
            continue;
        }
        match ch {
            '\'' => in_single = true,
            '"' => in_double = true,
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    close_idx = Some(idx);
                    break;
                }
            }
            _ => {}
        }
    }
    let close_pos = close_idx?;
    let cols_block = &body[..close_pos];
    let tail = &body[close_pos + 1..];

    let mut columns = HashSet::new();
    let mut paren_depth = 0usize;
    let mut angle_depth = 0usize;
    let mut in_sq = false;
    let mut in_dq = false;
    let mut item_start = 0usize;
    let mut scan = cols_block.char_indices().peekable();
    let mut push_item = |slice: &str| {
        let trimmed = slice.trim();
        if trimmed.is_empty() {
            return;
        }
        let tok_end = trimmed
            .find(|c: char| c.is_ascii_whitespace() || c == '(')
            .unwrap_or(trimmed.len());
        let tok = trimmed[..tok_end].trim_matches('`').to_ascii_lowercase();
        if !tok.is_empty()
            && !matches!(
                tok.as_str(),
                "constraint" | "foreign" | "primary" | "check" | "synonym" | "row"
            )
        {
            columns.insert(tok);
        }
    };
    while let Some((idx, ch)) = scan.next() {
        if in_sq {
            if ch == '\\' {
                let _ = scan.next();
            } else if ch == '\'' {
                in_sq = false;
            }
            continue;
        }
        if in_dq {
            if ch == '\\' {
                let _ = scan.next();
            } else if ch == '"' {
                in_dq = false;
            }
            continue;
        }
        match ch {
            '\'' => in_sq = true,
            '"' => in_dq = true,
            '(' => paren_depth += 1,
            ')' => paren_depth = paren_depth.saturating_sub(1),
            '<' => angle_depth += 1,
            '>' => angle_depth = angle_depth.saturating_sub(1),
            ',' if paren_depth == 0 && angle_depth == 0 => {
                push_item(&cols_block[item_start..idx]);
                item_start = idx + 1;
            }
            _ => {}
        }
    }
    push_item(&cols_block[item_start..]);

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

/// Rejects pre-split PoC Spanner databases and pre-migration `Workspaces` schemas before
/// `--spanner-auto-migrate` runs `CREATE TABLE IF NOT EXISTS`, matching
/// [`crate::store::SqliteMetadataStore`]'s fail-fast guard.
pub(super) fn validate_existing_spanner_ddl(existing_statements: &[String]) -> StoreResult<()> {
    let mut tables: HashMap<String, (HashSet<String>, Vec<String>)> = HashMap::new();
    for stmt in existing_statements {
        if let Some((table_name, cols, pk_cols)) = parse_create_table_ddl(stmt) {
            tables.insert(table_name, (cols, pk_cols));
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
        && ws_cols.contains("useremail")
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
                if !existing_cols.contains(*col) {
                    return Err(StoreError::InvalidArgument(format!(
                        "detected incompatible Spanner database ('{table}' table lacks '{col}' column); \
                         provision a fresh Spanner database or migrate the schema before starting jj-cc-server with --spanner-auto-migrate"
                    )));
                }
            }
        }
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
