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

//! PostgreSQL connection URI and libpq DSN credential redaction and TLS option parsing.
//!
//! # Cross-Process and RPC Boundaries
//! Redacts passwords, unknown keys, and ambiguous URI userinfo segments fail-closed before
//! connection strings enter logs or errors, and extracts `sslmode` and `sslrootcert` using
//! `tokio-postgres` parsing boundaries so TLS enforcement cannot diverge from connection setup.

use super::tls::{ParsedPgTlsOptions, ParsedSslMode};
use crate::store::{StoreError, StoreResult};

fn is_postgres_uri(raw: &str) -> bool {
    let lower = raw.trim_start();
    lower
        .get(..11)
        .is_some_and(|p| p.eq_ignore_ascii_case("postgres://"))
        || lower
            .get(..13)
            .is_some_and(|p| p.eq_ignore_ascii_case("postgresql://"))
}

/// Redacts credentials from both PostgreSQL URIs (`postgres://user:pass@host/db`,
/// `?password=...`) and libpq key-value DSNs (`password=...`) before logging or error
/// formatting.
pub(super) fn redact_postgres_url(raw: &str) -> String {
    let trimmed = raw.trim();
    if is_postgres_uri(trimmed) {
        redact_postgres_uri(trimmed)
    } else {
        redact_libpq_dsn(trimmed)
    }
}

fn is_secret_pg_param(key: &str) -> bool {
    key.eq_ignore_ascii_case("password") || key.eq_ignore_ascii_case("sslpassword")
}

/// Allowlist of non-secret connection parameters recognized by `tokio-postgres` and this store.
/// Unknown keys and bare tokens are masked fail-closed so unquoted whitespace, Base64 `=`, or
/// unencoded `&` inside passwords cannot leak fragments into error messages or logs.
fn is_known_safe_pg_param(key: &str) -> bool {
    matches!(
        key.to_ascii_lowercase().as_str(),
        "user"
            | "dbname"
            | "options"
            | "application_name"
            | "sslmode"
            | "sslrootcert"
            | "sslnegotiation"
            | "host"
            | "hostaddr"
            | "port"
            | "connect_timeout"
            | "tcp_user_timeout"
            | "keepalives"
            | "keepalives_idle"
            | "keepalives_interval"
            | "keepalives_retries"
            | "target_session_attrs"
            | "channel_binding"
            | "load_balance_hosts"
    )
}

/// Returns `true` if a parameter value or query segment embeds a `password=` or `sslpassword=`
/// assignment (for example, from a missing `&` in a URI, percent-encoded `=`, an empty DSN value
/// like `port= password=...`, or a mismatched quote that absorbed a later `password=` token).
fn value_embeds_secret_param(value: &str, decode_percent: bool) -> bool {
    let decoded_buf;
    let text = if decode_percent {
        decoded_buf = percent_decode_lossy_key(value);
        decoded_buf.as_str()
    } else {
        value
    };
    for (eq_idx, _) in text.match_indices('=') {
        let prefix = text[..eq_idx].trim_end();
        let bytes = prefix.as_bytes();
        if bytes.len() >= 8 && bytes[bytes.len() - 8..].eq_ignore_ascii_case(b"password") {
            return true;
        }
    }
    false
}

fn redact_query_param_pair(pair: &str) -> String {
    if pair.is_empty() {
        return String::new();
    }
    if let Some((k, v)) = pair.split_once('=') {
        let decoded = percent_decode_lossy_key(k);
        if is_secret_pg_param(&decoded) {
            format!("{k}=***")
        } else if is_known_safe_pg_param(&decoded) {
            if value_embeds_secret_param(v, true) {
                format!("{k}=***")
            } else {
                pair.to_string()
            }
        } else {
            "***".to_string()
        }
    } else {
        "***".to_string()
    }
}

fn redact_query_params(query: &str) -> String {
    query
        .split('&')
        .map(redact_query_param_pair)
        .collect::<Vec<_>>()
        .join("&")
}

/// Redacts `&`-delimited parameters across a URI tail that has already crossed `?`:
/// - Segment 0 (immediately after `@`) may be a host/path (e.g. `db.example.com:5432/jj?sslmode=require`)
///   or a parameter pair/suffix; any `?`-delimited query part or `key=value` pair in segment 0 is
///   redacted with `redact_query_param_pair`.
/// - Every subsequent `&`-delimited segment (`idx > 0`) is strictly a query parameter and is
///   redacted via `redact_query_param_pair` (allowlist + secret masking).
fn redact_post_query_tail(tail: &str) -> String {
    let mut segments = tail.split('&');
    let Some(first) = segments.next() else {
        return String::new();
    };
    let redacted_first = if let Some((before_q, after_q)) = first.split_once('?') {
        if before_q
            .split_once('=')
            .is_some_and(|(k, _)| !k.contains('/'))
            || value_embeds_secret_param(before_q, true)
        {
            format!(
                "{}?{}",
                redact_query_param_pair(before_q),
                redact_query_param_pair(after_q)
            )
        } else {
            let auth_end = before_q.find('/').unwrap_or(before_q.len());
            if let Some(redacted_auth) = redact_atless_authority(&before_q[..auth_end]) {
                // Fail closed through EOF when segment 0 is a truncated `user:pass` tail.
                return redacted_auth;
            }
            format!("{before_q}?{}", redact_query_param_pair(after_q))
        }
    } else if first.contains('=') || value_embeds_secret_param(first, true) {
        redact_query_param_pair(first)
    } else {
        let auth_end = first.find('/').unwrap_or(first.len());
        if let Some(redacted_auth) = redact_atless_authority(&first[..auth_end]) {
            // Fail closed through EOF when segment 0 is a truncated `user:pass` tail.
            return redacted_auth;
        }
        first.to_string()
    };

    let mut out = vec![redacted_first];
    out.extend(segments.map(redact_query_param_pair));
    out.join("&")
}

fn is_valid_bracketed_ipv6(interior: &str) -> bool {
    let (addr_part, zone_part) = match interior.split_once('%') {
        Some((addr, zone)) => (addr, Some(zone)),
        None => (interior, None),
    };
    addr_part.parse::<std::net::Ipv6Addr>().is_ok()
        && zone_part.is_none_or(|z| !z.is_empty() && !z.contains(':'))
}

fn is_valid_port_tail(s: &str) -> bool {
    s.is_empty()
        || (s.len() <= 5
            && !s.starts_with('0')
            && s.bytes().all(|b| b.is_ascii_digit())
            && s.parse::<u16>().is_ok())
}

/// Detects truncated `@`-less `user:pass` authorities (where any `:` outside a valid bracketed
/// IPv6 literal is followed by something other than an empty or `u16` port) and returns the masked
/// prefix up to that `:`.
fn redact_atless_authority(authority: &str) -> Option<String> {
    let mut offset = 0;
    for chunk in authority.split(',') {
        let bad_colon = if chunk.starts_with('[')
            && let Some(close) = chunk.find(']')
            && is_valid_bracketed_ipv6(&chunk[1..close])
            && (chunk[close + 1..].is_empty() || chunk[close + 1..].starts_with(':'))
        {
            let rest = &chunk[close + 1..];
            rest.strip_prefix(':')
                .filter(|port| !is_valid_port_tail(port))
                .map(|_| offset + close + 1)
        } else {
            chunk.find(':').and_then(|colon| {
                let tail = &chunk[colon + 1..];
                (!is_valid_port_tail(tail)).then_some(offset + colon)
            })
        };
        if let Some(colon_pos) = bad_colon {
            return Some(format!("{}:{}", &authority[..colon_pos], "***"));
        }
        offset += chunk.len() + 1;
    }
    None
}

fn redact_userinfo_prefix(prefix: &str) -> String {
    if let Some((before_q, after_q)) = prefix.split_once('?') {
        format!("{before_q}?{}", redact_query_params(after_q))
    } else {
        prefix.to_string()
    }
}

fn redact_postgres_uri(uri: &str) -> String {
    let Some(scheme_end) = uri.find("://") else {
        return uri.to_string();
    };
    let after_scheme = &uri[scheme_end + 3..];
    let first_delim = after_scheme.find(['/', '?']).unwrap_or(after_scheme.len());
    let first_q = after_scheme.find('?').unwrap_or(after_scheme.len());
    let before_q = &after_scheme[..first_q];

    // Any `@` strictly before `?` is an unconditional redaction floor. Whenever any `:` precedes a
    // post-`?` `@` (covering `user:pass`, `user@domain:pass`, `user@domain?part:pass`, and
    // malformed host/port ambiguities), extend fail-closed to the rightmost post-`?` `@` with no
    // skip heuristics so syntactically ambiguous credentials crossing `@` and `?` can never leak.
    let at_before_q = before_q.rfind('@');
    let at_after_q = (first_q < after_scheme.len())
        .then(|| {
            after_scheme[first_q + 1..]
                .rfind('@')
                .map(|rel| first_q + 1 + rel)
        })
        .flatten();
    let extend_past_query = at_after_q.is_some_and(|at| after_scheme[..at].contains(':'));
    let userinfo_at = if extend_past_query {
        at_after_q
    } else {
        at_before_q
    };

    if let Some(at_pos) = userinfo_at {
        let userinfo = &after_scheme[..at_pos];
        let in_secret_param = if at_pos > first_q {
            let query_before_at = &after_scheme[first_q + 1..at_pos];
            let mut in_secret = false;
            for seg in query_before_at.split('&') {
                if value_embeds_secret_param(seg, true) {
                    in_secret = true;
                } else if seg.split_once('=').is_some_and(|(k, _)| {
                    !k.contains('?') && is_known_safe_pg_param(&percent_decode_lossy_key(k))
                }) {
                    in_secret = false;
                }
            }
            in_secret
        } else {
            false
        };

        if at_pos > first_q && in_secret_param {
            // Mask through the end of the secret parameter value (next `&` or EOF) so the
            // secret's post-`@` suffix is never exposed as a hostname.
            let colon_pos = userinfo.find(':').unwrap_or(userinfo.len());
            let prefix = redact_userinfo_prefix(&userinfo[..colon_pos]);
            let value_end = after_scheme[at_pos + 1..]
                .find('&')
                .map_or(after_scheme.len(), |i| at_pos + 1 + i);
            let redacted_tail = redact_post_query_tail(&after_scheme[value_end..]);
            return format!("{}://{prefix}:***{redacted_tail}", &uri[..scheme_end]);
        }

        let redacted_user = if let Some(colon_pos) = userinfo.find(':') {
            format!("{}:***", redact_userinfo_prefix(&userinfo[..colon_pos]))
        } else {
            userinfo.to_string()
        };
        let tail = &after_scheme[at_pos + 1..];
        let redacted_tail = if at_pos > first_q {
            redact_post_query_tail(tail)
        } else {
            let tail_delim = tail.find(['/', '?']).unwrap_or(tail.len());
            if let Some(redacted_auth) = redact_atless_authority(&tail[..tail_delim]) {
                // Fail closed through EOF when an internal `@` (e.g. IAM username or `@`-password)
                // leaves a truncated `:password` in the post-`@` tail.
                return format!("{}://{redacted_user}@{redacted_auth}", &uri[..scheme_end]);
            }
            if let Some(q_pos) = tail.find('?') {
                format!(
                    "{}?{}",
                    &tail[..q_pos],
                    redact_query_params(&tail[q_pos + 1..])
                )
            } else {
                tail.to_string()
            }
        };
        return format!("{}://{redacted_user}@{redacted_tail}", &uri[..scheme_end]);
    }

    // Neither libpq nor `tokio-postgres` treats `#` as a fragment delimiter inside PostgreSQL
    // URI query strings, so redact every `&`-delimited parameter to the end of the URI.
    let authority = &after_scheme[..first_delim];
    if let Some(redacted_authority) = redact_atless_authority(authority) {
        // Fail closed through EOF so `?`-delimited fragments of a truncated password cannot leak.
        return format!("{}://{redacted_authority}", &uri[..scheme_end]);
    }
    let rest = &after_scheme[first_delim..];
    let redacted_rest = if let Some(q_pos) = rest.find('?') {
        format!(
            "{}?{}",
            &rest[..q_pos],
            redact_query_params(&rest[q_pos + 1..])
        )
    } else {
        rest.to_string()
    };
    format!("{}://{authority}{redacted_rest}", &uri[..scheme_end])
}

fn percent_decode_lossy_key(encoded: &str) -> String {
    let bytes = encoded.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(hi), Some(lo)) = (
                (bytes[i + 1] as char).to_digit(16),
                (bytes[i + 2] as char).to_digit(16),
            )
        {
            out.push(((hi << 4) | lo) as u8);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn redact_libpq_dsn(dsn: &str) -> String {
    let tokens = tokenize_libpq_dsn(dsn);
    let mut out = Vec::with_capacity(tokens.len());
    for (key, value, raw_pair, unterminated_quote) in tokens {
        if is_secret_pg_param(&key) {
            if value.is_some() {
                out.push(format!("{key}=***"));
            } else {
                out.push("***".to_string());
            }
        } else if !is_known_safe_pg_param(&key) || value.is_none() {
            // Bare tokens and unknown keys are invalid in `tokio-postgres` and commonly arise
            // from unquoted whitespace, Base64 `=`, or doubled quotes inside a password.
            out.push("***".to_string());
        } else if unterminated_quote
            || value
                .as_deref()
                .is_some_and(|v| value_embeds_secret_param(v, false))
        {
            out.push(format!("{key}=***"));
        } else {
            out.push(raw_pair);
        }
    }
    out.join(" ")
}

fn percent_decode_uri_param(raw_url: &str, encoded: &str) -> StoreResult<String> {
    let bytes = encoded.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return Err(StoreError::InvalidArgument(format!(
                    "malformed percent-encoding in PostgreSQL URL '{}'",
                    redact_postgres_url(raw_url)
                )));
            }
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            let (Some(hi), Some(lo)) = (hi, lo) else {
                return Err(StoreError::InvalidArgument(format!(
                    "malformed percent-encoding in PostgreSQL URL '{}'",
                    redact_postgres_url(raw_url)
                )));
            };
            out.push(((hi << 4) | lo) as u8);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| {
        StoreError::InvalidArgument(format!(
            "invalid UTF-8 in percent-decoded parameter in PostgreSQL URL '{}'",
            redact_postgres_url(raw_url)
        ))
    })
}

/// Parses `sslmode` and `sslrootcert` from a PostgreSQL URI or libpq key-value DSN and returns a
/// normalized DSN compatible with `tokio_postgres::Config::from_str` (rewriting `verify-ca` and
/// `verify-full` to `require` and stripping `sslrootcert`, which we enforce via `rustls`).
pub(super) fn parse_postgres_tls_options(raw: &str) -> StoreResult<ParsedPgTlsOptions> {
    let trimmed = raw.trim();
    if is_postgres_uri(trimmed) {
        parse_uri_tls_options(trimmed)
    } else {
        parse_dsn_tls_options(trimmed)
    }
}

fn parse_ssl_mode_value(raw_url: &str, value: &str) -> StoreResult<ParsedSslMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "disable" => Ok(ParsedSslMode::Disable),
        "prefer" => Ok(ParsedSslMode::Prefer),
        "require" => Ok(ParsedSslMode::Require),
        "verify-ca" => Ok(ParsedSslMode::VerifyCa),
        "verify-full" => Ok(ParsedSslMode::VerifyFull),
        _ => Err(StoreError::InvalidArgument(format!(
            "unsupported PostgreSQL sslmode in '{}' \
             (supported: disable, prefer, require, verify-ca, verify-full)",
            redact_postgres_url(raw_url)
        ))),
    }
}

fn parse_ssl_root_cert_value(raw_url: &str, value: &str) -> StoreResult<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(StoreError::InvalidArgument(format!(
            "empty PostgreSQL sslrootcert parameter in '{}'",
            redact_postgres_url(raw_url)
        )));
    }
    Ok(trimmed.to_string())
}

fn parse_uri_tls_options(uri: &str) -> StoreResult<ParsedPgTlsOptions> {
    // Follow `tokio_postgres::Config`'s exact `UrlParser` boundary semantics:
    // credentials consume up to the first `@` after `scheme://`, then host/path run until `?`,
    // and query keys/values are percent-decoded.
    let after_scheme_start = uri.find("://").map_or(0, |i| i + 3);
    let after_scheme = &uri[after_scheme_start..];
    let after_creds_start = match after_scheme.find('@') {
        Some(at_pos) => after_scheme_start + at_pos + 1,
        None => after_scheme_start,
    };
    let Some(rel_q_pos) = uri[after_creds_start..].find('?') else {
        return Ok(ParsedPgTlsOptions {
            ssl_mode: ParsedSslMode::Unspecified,
            ssl_root_cert: None,
            normalized_dsn: uri.to_string(),
        });
    };
    let q_pos = after_creds_start + rel_q_pos;
    let before_q = &uri[..q_pos];
    let query = &uri[q_pos + 1..];

    let mut ssl_mode = ParsedSslMode::Unspecified;
    let mut ssl_root_cert = None;
    let mut normalized_params = Vec::new();
    for pair in query.split('&') {
        if let Some((raw_k, raw_v)) = pair.split_once('=') {
            let decoded_k = percent_decode_uri_param(uri, raw_k)?;
            if decoded_k.eq_ignore_ascii_case("sslmode") {
                let decoded_v = percent_decode_uri_param(uri, raw_v)?;
                let parsed = parse_ssl_mode_value(uri, &decoded_v)?;
                ssl_mode = parsed;
                let rewritten_val = match parsed {
                    ParsedSslMode::VerifyCa | ParsedSslMode::VerifyFull => "require",
                    ParsedSslMode::Disable => "disable",
                    ParsedSslMode::Prefer => "prefer",
                    ParsedSslMode::Require => "require",
                    ParsedSslMode::Unspecified => "prefer",
                };
                normalized_params.push(format!("sslmode={rewritten_val}"));
                continue;
            }
            if decoded_k.eq_ignore_ascii_case("sslrootcert") {
                let decoded = percent_decode_uri_param(uri, raw_v)?;
                ssl_root_cert = Some(parse_ssl_root_cert_value(uri, &decoded)?);
                continue;
            }
        }
        if !pair.is_empty() {
            normalized_params.push(pair.to_string());
        }
    }

    let normalized_dsn = if normalized_params.is_empty() {
        before_q.to_string()
    } else {
        format!("{before_q}?{}", normalized_params.join("&"))
    };
    Ok(ParsedPgTlsOptions {
        ssl_mode,
        ssl_root_cert,
        normalized_dsn,
    })
}

fn parse_dsn_tls_options(dsn: &str) -> StoreResult<ParsedPgTlsOptions> {
    let tokens = tokenize_libpq_dsn(dsn);
    let mut ssl_mode = ParsedSslMode::Unspecified;
    let mut ssl_root_cert = None;
    let mut normalized = Vec::with_capacity(tokens.len());
    for (key, value, raw_pair, unterminated_quote) in tokens {
        if unterminated_quote {
            return Err(StoreError::InvalidArgument(format!(
                "unterminated quoted parameter in PostgreSQL connection config '{}'",
                redact_postgres_url(dsn)
            )));
        }
        if key.is_empty() {
            return Err(StoreError::InvalidArgument(format!(
                "invalid PostgreSQL connection config '{}': empty parameter name",
                redact_postgres_url(dsn)
            )));
        }
        if key.eq_ignore_ascii_case("sslmode")
            && let Some(val) = value
        {
            let parsed = parse_ssl_mode_value(dsn, &val)?;
            ssl_mode = parsed;
            let rewritten_val = match parsed {
                ParsedSslMode::VerifyCa | ParsedSslMode::VerifyFull => "require",
                ParsedSslMode::Disable => "disable",
                ParsedSslMode::Prefer => "prefer",
                ParsedSslMode::Require => "require",
                ParsedSslMode::Unspecified => "prefer",
            };
            normalized.push(format!("sslmode={rewritten_val}"));
        } else if key.eq_ignore_ascii_case("sslrootcert")
            && let Some(val) = value
        {
            ssl_root_cert = Some(parse_ssl_root_cert_value(dsn, &val)?);
        } else {
            normalized.push(raw_pair);
        }
    }
    Ok(ParsedPgTlsOptions {
        ssl_mode,
        ssl_root_cert,
        normalized_dsn: normalized.join(" "),
    })
}

/// Tokenizes a libpq key-value connection string into `(key, unquoted_value, raw_token,
/// unterminated_quote)` tuples while respecting Unicode whitespace (matching `tokio-postgres`'s
/// `Parser`), single-quoted values, and backslash escapes.
fn tokenize_libpq_dsn(dsn: &str) -> Vec<(String, Option<String>, String, bool)> {
    let mut tokens = Vec::new();
    let len = dsn.len();
    let mut i = 0;

    let next_char = |idx: usize| dsn[idx..].chars().next();

    while i < len {
        while let Some(ch) = next_char(i)
            && ch.is_whitespace()
        {
            i += ch.len_utf8();
        }
        if i >= len {
            break;
        }
        let start = i;
        while let Some(ch) = next_char(i)
            && ch != '='
            && !ch.is_whitespace()
        {
            i += ch.len_utf8();
        }
        let key = dsn[start..i].to_string();
        while let Some(ch) = next_char(i)
            && ch.is_whitespace()
        {
            i += ch.len_utf8();
        }
        if next_char(i) != Some('=') {
            tokens.push((key.clone(), None, dsn[start..i].trim().to_string(), false));
            continue;
        }
        i += 1; // skip '='
        while let Some(ch) = next_char(i)
            && ch.is_whitespace()
        {
            i += ch.len_utf8();
        }
        let mut unquoted = String::new();
        let mut unterminated_quote = false;
        if next_char(i) == Some('\'') {
            i += 1; // skip opening quote
            let mut closed = false;
            while let Some(ch) = next_char(i) {
                i += ch.len_utf8();
                if ch == '\\'
                    && let Some(esc) = next_char(i)
                {
                    i += esc.len_utf8();
                    unquoted.push(esc);
                } else if ch == '\'' {
                    closed = true;
                    break;
                } else {
                    unquoted.push(ch);
                }
            }
            unterminated_quote = !closed;
        } else {
            while let Some(ch) = next_char(i)
                && !ch.is_whitespace()
            {
                i += ch.len_utf8();
                if ch == '\\'
                    && let Some(esc) = next_char(i)
                {
                    i += esc.len_utf8();
                    unquoted.push(esc);
                } else {
                    unquoted.push(ch);
                }
            }
        }
        tokens.push((
            key,
            Some(unquoted),
            dsn[start..i].to_string(),
            unterminated_quote,
        ));
    }
    tokens
}
