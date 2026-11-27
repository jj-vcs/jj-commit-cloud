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

//! Pure-Rust AWS Signature Version 4 signing, RFC 3986 URI encoding, and UTC date formatting.
//!
//! # Cross-Process and RPC Boundaries
//! Produces deterministic canonical requests, signed header lists, and `AWS4-HMAC-SHA256`
//! authorization headers with `x-amz-date`, `x-amz-content-sha256`, and optional
//! `x-amz-security-token` headers without shelling out or relying on external date crates.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, KeyInit as _, Mac as _};
use sha2::{Digest as _, Sha256};

use super::config::S3Credentials;
use crate::store::{StoreError, StoreResult};

pub(super) const EMPTY_PAYLOAD_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

type HmacSha256 = Hmac<Sha256>;

/// Pure-Rust RFC 3986 percent-encoding for AWS Signature Version 4.
pub(super) fn uri_encode(input: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(input.len());
    for &b in input.as_bytes() {
        let keep = matches!(
            b,
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~'
        ) || (!encode_slash && b == b'/');
        if keep {
            out.push(b as char);
        } else {
            const HEX: &[u8; 16] = b"0123456789ABCDEF";
            out.push('%');
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0x0f) as usize] as char);
        }
    }
    out
}

/// Formats `SystemTime` into AWS SigV4 `(YYYYMMDD, YYYYMMDDTHHMMSSZ)` UTC timestamps using
/// Howard Hinnant's civil-from-days algorithm without external date/time crates.
pub(super) fn format_amz_timestamps(now: SystemTime) -> StoreResult<(String, String)> {
    let dur = now
        .duration_since(UNIX_EPOCH)
        .map_err(|_| StoreError::Backend("system clock is earlier than UNIX_EPOCH".to_string()))?;
    Ok(format_unix_secs_amz(dur.as_secs()))
}

pub(super) fn format_unix_secs_amz(unix_secs: u64) -> (String, String) {
    let days = (unix_secs / 86_400) as i64;
    let secs_of_day = unix_secs % 86_400;
    let hour = secs_of_day / 3600;
    let minute = (secs_of_day % 3600) / 60;
    let second = secs_of_day % 60;

    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = y + i64::from(month <= 2);

    let date_stamp = format!("{year:04}{month:02}{day:02}");
    let amz_date = format!("{date_stamp}T{hour:02}{minute:02}{second:02}Z");
    (date_stamp, amz_date)
}

pub(super) fn sha256_hex(data: &[u8]) -> String {
    if data.is_empty() {
        return EMPTY_PAYLOAD_SHA256.to_string();
    }
    hex::encode(Sha256::digest(data))
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC-SHA256 accepts keys of any size");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

fn normalize_header_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut in_ws = false;
    for ch in value.trim().chars() {
        if ch == ' ' || ch == '\t' {
            if !in_ws {
                out.push(' ');
                in_ws = true;
            }
        } else {
            out.push(ch);
            in_ws = false;
        }
    }
    out
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct SigV4Output {
    pub(super) payload_hash: String,
    pub(super) canonical_request: String,
    pub(super) string_to_sign: String,
    pub(super) signed_headers: String,
    pub(super) signature: String,
    pub(super) authorization: String,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn sign_v4_encoded_path_with_hash(
    method: &str,
    canonical_uri: &str,
    query: &[(&str, &str)],
    headers: &[(&str, &str)],
    payload_hash: &str,
    date_stamp: &str,
    amz_date: &str,
    region: &str,
    creds: &S3Credentials,
) -> SigV4Output {
    let payload_hash = payload_hash.to_string();
    let canonical_uri = if canonical_uri.is_empty() {
        "/"
    } else {
        canonical_uri
    };

    let mut encoded_query: Vec<(String, String)> = query
        .iter()
        .map(|(k, v)| (uri_encode(k, true), uri_encode(v, true)))
        .collect();
    encoded_query.sort();
    let canonical_query = encoded_query
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");

    let mut header_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (k, v) in headers {
        let lower = k.to_ascii_lowercase();
        header_map
            .entry(lower)
            .or_default()
            .push(normalize_header_value(v));
    }
    header_map
        .entry("x-amz-date".to_string())
        .or_insert_with(|| vec![amz_date.to_string()]);
    header_map
        .entry("x-amz-content-sha256".to_string())
        .or_insert_with(|| vec![payload_hash.clone()]);
    if let Some(token) = &creds.session_token {
        header_map
            .entry("x-amz-security-token".to_string())
            .or_insert_with(|| vec![normalize_header_value(token)]);
    }

    let mut canonical_headers = String::new();
    let mut signed_header_names: Vec<&str> = Vec::with_capacity(header_map.len());
    for (name, values) in &header_map {
        canonical_headers.push_str(name);
        canonical_headers.push(':');
        canonical_headers.push_str(&values.join(","));
        canonical_headers.push('\n');
        signed_header_names.push(name.as_str());
    }
    let signed_headers = signed_header_names.join(";");

    let canonical_request = format!(
        "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    );
    let scope = format!("{date_stamp}/{region}/s3/aws4_request");
    let canonical_request_hash = sha256_hex(canonical_request.as_bytes());
    let string_to_sign = format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{canonical_request_hash}");

    let mut signing_secret = Vec::with_capacity(4 + creds.secret_key.len());
    signing_secret.extend_from_slice(b"AWS4");
    signing_secret.extend_from_slice(creds.secret_key.as_bytes());
    let k_date = hmac_sha256(&signing_secret, date_stamp.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, b"s3");
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature = hex::encode(hmac_sha256(&k_signing, string_to_sign.as_bytes()));

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        creds.access_key
    );

    SigV4Output {
        payload_hash,
        canonical_request,
        string_to_sign,
        signed_headers,
        signature,
        authorization,
    }
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(super) fn sign_v4_encoded_path(
    method: &str,
    canonical_uri: &str,
    query: &[(&str, &str)],
    headers: &[(&str, &str)],
    payload: &[u8],
    date_stamp: &str,
    amz_date: &str,
    region: &str,
    creds: &S3Credentials,
) -> SigV4Output {
    let payload_hash = sha256_hex(payload);
    sign_v4_encoded_path_with_hash(
        method,
        canonical_uri,
        query,
        headers,
        &payload_hash,
        date_stamp,
        amz_date,
        region,
        creds,
    )
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(super) fn sign_v4(
    method: &str,
    raw_path: &str,
    query: &[(&str, &str)],
    headers: &[(&str, &str)],
    payload: &[u8],
    date_stamp: &str,
    amz_date: &str,
    region: &str,
    creds: &S3Credentials,
) -> SigV4Output {
    let canonical_uri = if raw_path.is_empty() {
        "/".to_string()
    } else {
        uri_encode(raw_path, false)
    };
    sign_v4_encoded_path(
        method,
        &canonical_uri,
        query,
        headers,
        payload,
        date_stamp,
        amz_date,
        region,
        creds,
    )
}
