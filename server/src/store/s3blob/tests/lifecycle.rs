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

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use super::super::config::*;
use super::super::sigv4::*;
use super::super::*;
use crate::store::gcsblob::IMMUTABLE_CACHE_CONTROL;
use crate::store::{
    BlobRecord, BlobStore, CompositeStore, MemoryMetadataStore, ReplicatedBlobStore,
    ReplicatedBlobStoreConfig, StoreError, StoreResult,
};

#[derive(Clone, Debug)]
struct RecordedRequest {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

enum MockOverrideAction {
    Status(u16, String),
    DropConnection,
}

struct MockOverride {
    method: Option<&'static str>,
    action: MockOverrideAction,
}

struct MockS3State {
    buckets: Mutex<HashMap<String, HashMap<String, Vec<u8>>>>,
    requests: Mutex<Vec<RecordedRequest>>,
    expected_creds: Option<S3Credentials>,
    region: String,
    ignore_range: AtomicBool,
    corrupt_206_start: AtomicBool,
    omit_416_content_range: AtomicBool,
    bucket_create_status: Mutex<Option<(u16, String, bool)>>,
    next_put_status: Mutex<Option<(u16, String)>>,
    scripted_overrides: Mutex<VecDeque<MockOverride>>,
    redirect_all: AtomicBool,
    oversized_error: AtomicBool,
    put_count: AtomicUsize,
}

impl MockS3State {
    fn new(creds: Option<S3Credentials>, region: &str) -> Arc<Self> {
        Arc::new(Self {
            buckets: Mutex::new(HashMap::new()),
            requests: Mutex::new(Vec::new()),
            expected_creds: creds,
            region: region.to_string(),
            ignore_range: AtomicBool::new(false),
            corrupt_206_start: AtomicBool::new(false),
            omit_416_content_range: AtomicBool::new(false),
            bucket_create_status: Mutex::new(None),
            next_put_status: Mutex::new(None),
            scripted_overrides: Mutex::new(VecDeque::new()),
            redirect_all: AtomicBool::new(false),
            oversized_error: AtomicBool::new(false),
            put_count: AtomicUsize::new(0),
        })
    }

    fn push_status(&self, method: Option<&'static str>, code: u16, body: impl Into<String>) {
        self.scripted_overrides
            .lock()
            .unwrap()
            .push_back(MockOverride {
                method,
                action: MockOverrideAction::Status(code, body.into()),
            });
    }

    fn push_drop(&self, method: Option<&'static str>) {
        self.scripted_overrides
            .lock()
            .unwrap()
            .push_back(MockOverride {
                method,
                action: MockOverrideAction::DropConnection,
            });
    }

    fn take_matching_override(&self, method: &str) -> Option<MockOverrideAction> {
        let mut queue = self.scripted_overrides.lock().unwrap();
        if queue
            .front()
            .is_some_and(|entry| entry.method.is_none_or(|m| m == method))
        {
            queue.pop_front().map(|entry| entry.action)
        } else {
            None
        }
    }

    fn insert_bucket(&self, bucket: &str) {
        self.buckets
            .lock()
            .unwrap()
            .entry(bucket.to_string())
            .or_default();
    }
}

fn verify_mock_sigv4(
    state: &MockS3State,
    method: &str,
    path: &str,
    headers: &HashMap<String, String>,
    body: &[u8],
) -> Result<(), String> {
    let Some(creds) = &state.expected_creds else {
        if headers.contains_key("authorization") {
            return Err("unexpected Authorization header on anonymous endpoint".to_string());
        }
        return Ok(());
    };

    let auth = headers
        .get("authorization")
        .ok_or("missing Authorization header")?;
    let amz_date = headers
        .get("x-amz-date")
        .ok_or("missing x-amz-date header")?;
    let payload_hash = headers
        .get("x-amz-content-sha256")
        .ok_or("missing x-amz-content-sha256 header")?;
    if payload_hash != &sha256_hex(body) {
        return Err("x-amz-content-sha256 mismatch".to_string());
    }
    if let Some(expected_tok) = &creds.session_token
        && headers.get("x-amz-security-token") != Some(expected_tok)
    {
        return Err("x-amz-security-token mismatch".to_string());
    }

    // Extract SignedHeaders from Authorization header and verify signature matches.
    let signed_part = auth
        .split("SignedHeaders=")
        .nth(1)
        .and_then(|s| s.split(',').next())
        .ok_or("malformed SignedHeaders")?;
    let mut signed_pairs: Vec<(&str, &str)> = Vec::new();
    for name in signed_part.split(';') {
        let val = headers
            .get(name)
            .ok_or_else(|| format!("signed header '{name}' missing on wire"))?;
        signed_pairs.push((name, val.as_str()));
    }
    let date_stamp = amz_date.get(..8).ok_or("short x-amz-date")?;
    let expected = sign_v4_encoded_path(
        method,
        path,
        &[],
        &signed_pairs,
        body,
        date_stamp,
        amz_date,
        &state.region,
        creds,
    );
    if auth != &expected.authorization {
        return Err(format!(
            "SignatureDoesNotMatch: got {auth}, expected {}",
            expected.authorization
        ));
    }
    Ok(())
}

async fn spawn_mock_s3(state: Arc<MockS3State>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let st = state.clone();
            tokio::spawn(async move {
                let mut reader = BufReader::new(stream);
                loop {
                    let mut request_line = String::new();
                    match reader.read_line(&mut request_line).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                    let parts: Vec<&str> = request_line.split_whitespace().collect();
                    if parts.len() < 2 {
                        break;
                    }
                    let method = parts[0].to_string();
                    let path = parts[1].to_string();

                    let mut headers = HashMap::new();
                    let mut content_length: usize = 0;
                    loop {
                        let mut line = String::new();
                        if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                            break;
                        }
                        let trimmed = line.trim_end_matches(['\r', '\n']);
                        if trimmed.is_empty() {
                            break;
                        }
                        if let Some((k, v)) = trimmed.split_once(':') {
                            let key = k.trim().to_ascii_lowercase();
                            let val = v.trim().to_string();
                            if key == "content-length" {
                                content_length = val.parse().unwrap_or(0);
                            }
                            headers.insert(key, val);
                        }
                    }

                    let mut body = vec![0u8; content_length];
                    if content_length > 0 && reader.read_exact(&mut body).await.is_err() {
                        break;
                    }

                    st.requests.lock().unwrap().push(RecordedRequest {
                        method: method.clone(),
                        path: path.clone(),
                        headers: headers.clone(),
                        body: body.clone(),
                    });

                    let (status, reason, extra_hdrs, resp_body) =
                        match handle_mock_s3_request(&st, &method, &path, &headers, &body) {
                            Some(resp) => resp,
                            None => break,
                        };

                    let has_content_length = extra_hdrs
                        .iter()
                        .any(|(k, _)| k.eq_ignore_ascii_case("content-length"));
                    let mut raw = format!("HTTP/1.1 {status} {reason}\r\n");
                    if !has_content_length {
                        raw.push_str(&format!("Content-Length: {}\r\n", resp_body.len()));
                    }
                    for (k, v) in extra_hdrs {
                        raw.push_str(&format!("{k}: {v}\r\n"));
                    }
                    raw.push_str("\r\n");

                    let stream = reader.get_mut();
                    if stream.write_all(raw.as_bytes()).await.is_err() {
                        break;
                    }
                    if method != "HEAD"
                        && !resp_body.is_empty()
                        && stream.write_all(&resp_body).await.is_err()
                    {
                        break;
                    }
                    let _ = stream.flush().await;
                }
            });
        }
    });
    format!("http://{addr}")
}

fn handle_mock_s3_request(
    state: &MockS3State,
    method: &str,
    path: &str,
    headers: &HashMap<String, String>,
    body: &[u8],
) -> Option<(u16, &'static str, Vec<(String, String)>, Vec<u8>)> {
    if state.redirect_all.load(Ordering::Relaxed) {
        return Some((
            307,
            "Temporary Redirect",
            vec![(
                "Location".to_string(),
                "http://evil.invalid/leak".to_string(),
            )],
            Vec::new(),
        ));
    }
    if state.oversized_error.load(Ordering::Relaxed) {
        return Some((
            500,
            "Internal Server Error",
            Vec::new(),
            vec![b'E'; 128 * 1024],
        ));
    }
    if let Err(msg) = verify_mock_sigv4(state, method, path, headers, body) {
        let xml =
            format!("<Error><Code>SignatureDoesNotMatch</Code><Message>{msg}</Message></Error>");
        return Some((403, "Forbidden", Vec::new(), xml.into_bytes()));
    }
    if let Some(action) = state.take_matching_override(method) {
        return match action {
            MockOverrideAction::DropConnection => None,
            MockOverrideAction::Status(code, xml) => {
                let reason = match code {
                    400 => "Bad Request",
                    401 => "Unauthorized",
                    403 => "Forbidden",
                    404 => "Not Found",
                    408 => "Request Timeout",
                    409 => "Conflict",
                    429 => "Too Many Requests",
                    500 => "Internal Server Error",
                    502 => "Bad Gateway",
                    503 => "Service Unavailable",
                    504 => "Gateway Timeout",
                    _ => "Error",
                };
                Some((code, reason, Vec::new(), xml.into_bytes()))
            }
        };
    }

    let trimmed = path.strip_prefix('/').unwrap_or(path);
    let (bucket, key_opt) = match trimmed.split_once('/') {
        Some((b, k)) if !k.is_empty() => (b, Some(k)),
        Some((b, _)) => (b, None),
        None => (trimmed, None),
    };

    let mut buckets = state.buckets.lock().unwrap();
    Some(match (method, key_opt) {
        ("HEAD", None) => {
            if buckets.contains_key(bucket) {
                (200, "OK", Vec::new(), Vec::new())
            } else {
                (404, "Not Found", Vec::new(), Vec::new())
            }
        }
        ("PUT", None) => {
            if let Some((code, xml, create_after)) =
                state.bucket_create_status.lock().unwrap().take()
            {
                if create_after {
                    buckets.entry(bucket.to_string()).or_default();
                }
                let reason = if code == 409 { "Conflict" } else { "Error" };
                return Some((code, reason, Vec::new(), xml.into_bytes()));
            }
            if buckets.contains_key(bucket) {
                (
                    409,
                    "Conflict",
                    Vec::new(),
                    b"<Error><Code>BucketAlreadyOwnedByYou</Code></Error>".to_vec(),
                )
            } else {
                buckets.insert(bucket.to_string(), HashMap::new());
                (200, "OK", Vec::new(), Vec::new())
            }
        }
        ("PUT", Some(key)) => {
            state.put_count.fetch_add(1, Ordering::Relaxed);
            if let Some((code, xml)) = state.next_put_status.lock().unwrap().take() {
                let reason = if code == 409 { "Conflict" } else { "Error" };
                return Some((code, reason, Vec::new(), xml.into_bytes()));
            }
            let Some(objects) = buckets.get_mut(bucket) else {
                return Some((
                    404,
                    "Not Found",
                    Vec::new(),
                    b"<Error><Code>NoSuchBucket</Code></Error>".to_vec(),
                ));
            };
            if headers.get("if-none-match").map(String::as_str) == Some("*")
                && objects.contains_key(key)
            {
                return Some((412, "Precondition Failed", Vec::new(), Vec::new()));
            }
            objects.insert(key.to_string(), body.to_vec());
            (200, "OK", Vec::new(), Vec::new())
        }
        ("HEAD", Some(key)) => {
            let Some(obj) = buckets.get(bucket).and_then(|b| b.get(key)) else {
                return Some((404, "Not Found", Vec::new(), Vec::new()));
            };
            (
                200,
                "OK",
                vec![("Content-Length".to_string(), obj.len().to_string())],
                Vec::new(),
            )
        }
        ("GET", Some(key)) => {
            let Some(objects) = buckets.get(bucket) else {
                return Some((
                    404,
                    "Not Found",
                    Vec::new(),
                    b"<Error><Code>NoSuchBucket</Code></Error>".to_vec(),
                ));
            };
            let Some(obj) = objects.get(key) else {
                return Some((
                    404,
                    "Not Found",
                    Vec::new(),
                    b"<Error><Code>NoSuchKey</Code></Error>".to_vec(),
                ));
            };
            if let Some(range_hdr) = headers.get("range")
                && !state.ignore_range.load(Ordering::Relaxed)
            {
                let total = obj.len() as u64;
                let spec = range_hdr.strip_prefix("bytes=").unwrap_or("");
                let (start_s, end_s) = spec.split_once('-').unwrap_or(("", ""));
                let start: u64 = start_s.parse().unwrap_or(u64::MAX);
                if start >= total {
                    let extra = if state.omit_416_content_range.load(Ordering::Relaxed) {
                        Vec::new()
                    } else {
                        vec![("Content-Range".to_string(), format!("bytes */{total}"))]
                    };
                    return Some((416, "Range Not Satisfiable", extra, Vec::new()));
                }
                let end = if end_s.is_empty() {
                    total - 1
                } else {
                    end_s.parse::<u64>().unwrap_or(total - 1).min(total - 1)
                };
                let reported_start = if state.corrupt_206_start.load(Ordering::Relaxed) {
                    0
                } else {
                    start
                };
                let slice = obj[(reported_start as usize)..=(end as usize)].to_vec();
                return Some((
                    206,
                    "Partial Content",
                    vec![(
                        "Content-Range".to_string(),
                        format!("bytes {reported_start}-{end}/{total}"),
                    )],
                    slice,
                ));
            }
            (200, "OK", Vec::new(), obj.clone())
        }
        _ => (405, "Method Not Allowed", Vec::new(), Vec::new()),
    })
}

async fn collect_stream(
    mut rx: mpsc::Receiver<StoreResult<Bytes>>,
) -> StoreResult<(Vec<u8>, usize)> {
    let mut out = Vec::new();
    let mut frames = 0usize;
    while let Some(item) = rx.recv().await {
        let chunk = item?;
        assert!(
            chunk.len() <= cc_common::FILE_STREAM_CHUNK_SIZE,
            "stream frame {} exceeded FILE_STREAM_CHUNK_SIZE",
            chunk.len()
        );
        out.extend_from_slice(&chunk);
        frames += 1;
    }
    Ok((out, frames))
}

#[tokio::test]
async fn s3_blob_store_full_lifecycle_sigv4_headers_ranges_and_idempotency() {
    let creds = S3Credentials {
        access_key: "test-access-key".to_string(),
        secret_key: "test-secret-key-do-not-leak".to_string(),
        session_token: Some("test-session-token-do-not-leak".to_string()),
    };
    let state = MockS3State::new(Some(creds.clone()), "us-west-2");
    let endpoint = spawn_mock_s3(state.clone()).await;

    let cfg = S3ConnectConfig {
        bucket: "s3://jj-blobs-demo".to_string(),
        endpoint: Some(endpoint),
        region: "us-west-2".to_string(),
        access_key: Some(creds.access_key.clone()),
        secret_key: Some(creds.secret_key.clone()),
        session_token: creds.session_token.clone(),
        path_style: None,
        auto_create_bucket: None,
    };
    let store = S3BlobStore::connect(&cfg).await.unwrap();
    assert_eq!(store.location_id(), "s3://jj-blobs-demo");
    let store_dbg = format!("{store:?}");
    assert!(!store_dbg.contains("test-secret-key-do-not-leak"));
    assert!(!store_dbg.contains("test-session-token-do-not-leak"));

    // Auto-create ran HEAD (404) -> PUT (200) on bucket with non-us-east-1 LocationConstraint.
    {
        let reqs = state.requests.lock().unwrap();
        assert_eq!(reqs.len(), 2);
        assert_eq!(reqs[0].method, "HEAD");
        assert_eq!(reqs[0].path, "/jj-blobs-demo");
        assert_eq!(reqs[1].method, "PUT");
        assert_eq!(reqs[1].path, "/jj-blobs-demo");
        assert_eq!(
            reqs[1].headers.get("content-type").map(String::as_str),
            Some("application/xml")
        );
        assert!(
            String::from_utf8_lossy(&reqs[1].body)
                .contains("<LocationConstraint>us-west-2</LocationConstraint>")
        );
    }

    let blob_id = [0xab, 0xcd, 0xef, 0x01, 0x23];
    let payload = Bytes::from_static(b"0123456789abcdef");

    assert!(!store.has_blob(&blob_id).await.unwrap());
    assert_eq!(store.get_blob(&blob_id).await.unwrap(), None);

    // First put_blob -> 200 OK with immutable Cache-Control and If-None-Match: *.
    store.put_blob(&blob_id, payload.clone()).await.unwrap();
    {
        let reqs = state.requests.lock().unwrap();
        let put_req = reqs.last().unwrap();
        assert_eq!(put_req.method, "PUT");
        assert_eq!(put_req.path, "/jj-blobs-demo/blobs/ab/cd/abcdef0123");
        assert_eq!(
            put_req.headers.get("cache-control").map(String::as_str),
            Some(IMMUTABLE_CACHE_CONTROL)
        );
        assert_eq!(
            put_req.headers.get("if-none-match").map(String::as_str),
            Some("*")
        );
        assert!(put_req.headers.contains_key("authorization"));
        assert!(put_req.headers.contains_key("x-amz-date"));
        assert!(put_req.headers.contains_key("x-amz-content-sha256"));
        assert_eq!(
            put_req
                .headers
                .get("x-amz-security-token")
                .map(String::as_str),
            Some("test-session-token-do-not-leak")
        );
    }

    // Duplicate put_blob hits 412 Precondition Failed and succeeds idempotently.
    store.put_blob(&blob_id, payload.clone()).await.unwrap();

    // Concurrent 409 ConditionalRequestConflict succeeds when object exists.
    *state.next_put_status.lock().unwrap() = Some((
        409,
        "<Error><Code>ConditionalRequestConflict</Code></Error>".to_string(),
    ));
    store.put_blob(&blob_id, payload.clone()).await.unwrap();

    // has_blob + full get_blob.
    assert!(store.has_blob(&blob_id).await.unwrap());
    assert_eq!(
        store.get_blob(&blob_id).await.unwrap(),
        Some(payload.clone())
    );

    // Range reads (206 Partial Content).
    assert_eq!(
        store.get_blob_range(&blob_id, 4, Some(6)).await.unwrap(),
        Some(Bytes::from_static(b"456789"))
    );
    assert_eq!(
        store.get_blob_range(&blob_id, 12, None).await.unwrap(),
        Some(Bytes::from_static(b"cdef"))
    );
    assert_eq!(
        store.get_blob_range(&blob_id, 12, Some(100)).await.unwrap(),
        Some(Bytes::from_static(b"cdef"))
    );
    // offset == total_len -> empty Bytes (with Content-Range on 416).
    assert_eq!(
        store.get_blob_range(&blob_id, 16, Some(10)).await.unwrap(),
        Some(Bytes::new())
    );
    // offset > total_len -> OutOfRange.
    assert!(matches!(
        store.get_blob_range(&blob_id, 17, Some(10)).await,
        Err(StoreError::OutOfRange(_))
    ));

    // 416 without Content-Range falls back to HEAD Content-Length.
    state.omit_416_content_range.store(true, Ordering::Relaxed);
    assert_eq!(
        store.get_blob_range(&blob_id, 16, Some(4)).await.unwrap(),
        Some(Bytes::new())
    );
    assert!(matches!(
        store.get_blob_range(&blob_id, 17, Some(4)).await,
        Err(StoreError::OutOfRange(_))
    ));
    state.omit_416_content_range.store(false, Ordering::Relaxed);

    // Server ignoring Range (returning 200 OK) slices locally.
    state.ignore_range.store(true, Ordering::Relaxed);
    assert_eq!(
        store.get_blob_range(&blob_id, 2, Some(4)).await.unwrap(),
        Some(Bytes::from_static(b"2345"))
    );
    assert_eq!(
        store.get_blob_range(&blob_id, 16, Some(4)).await.unwrap(),
        Some(Bytes::new())
    );
    assert!(matches!(
        store.get_blob_range(&blob_id, 17, Some(4)).await,
        Err(StoreError::OutOfRange(_))
    ));
    state.ignore_range.store(false, Ordering::Relaxed);

    // get_blob_range_with_record: full read bypasses Range, offset == content_size checks
    // existence (existing -> empty Bytes, missing -> None so CompositeStore surfaces DATA_LOSS).
    let existing_record = BlobRecord {
        blob_id: blob_id.to_vec(),
        content_size: payload.len() as u64,
        inline_content: None,
        primary_location: Some("s3://jj-blobs-demo".to_string()),
        replicated_locations: vec!["s3://jj-blobs-demo".to_string()],
    };
    assert_eq!(
        store
            .get_blob_range_with_record(&existing_record, 0, Some(100))
            .await
            .unwrap(),
        Some(payload.clone())
    );
    assert!(
        !state
            .requests
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .headers
            .contains_key("range")
    );
    assert_eq!(
        store
            .get_blob_range_with_record(&existing_record, 16, Some(4))
            .await
            .unwrap(),
        Some(Bytes::new())
    );

    let missing_record = BlobRecord {
        blob_id: vec![0x99, 0x88, 0x77],
        content_size: 16,
        inline_content: None,
        primary_location: Some("s3://jj-blobs-demo".to_string()),
        replicated_locations: vec!["s3://jj-blobs-demo".to_string()],
    };
    assert_eq!(
        store
            .get_blob_range_with_record(&missing_record, 16, Some(4))
            .await
            .unwrap(),
        None
    );
    assert!(matches!(
        store
            .get_blob_range_with_record(&missing_record, 17, Some(4))
            .await,
        Err(StoreError::OutOfRange(_))
    ));

    // 0-byte object roundtrip and 416 handling at offset 0 vs offset 1.
    let empty_id = [0x00, 0x11, 0x22];
    store.put_blob(&empty_id, Bytes::new()).await.unwrap();
    assert!(store.has_blob(&empty_id).await.unwrap());
    assert_eq!(store.get_blob(&empty_id).await.unwrap(), Some(Bytes::new()));
    assert_eq!(
        store.get_blob_range(&empty_id, 0, Some(10)).await.unwrap(),
        Some(Bytes::new())
    );
    assert!(matches!(
        store.get_blob_range(&empty_id, 1, Some(10)).await,
        Err(StoreError::OutOfRange(_))
    ));

    // Bad secret key produces 403 SignatureDoesNotMatch -> StoreError::Unauthenticated.
    let bad_creds_cfg = S3ConnectConfig {
        bucket: "jj-blobs-demo".to_string(),
        endpoint: cfg.endpoint.clone(),
        region: "us-west-2".to_string(),
        access_key: Some(creds.access_key),
        secret_key: Some("wrong-secret-key".to_string()),
        session_token: creds.session_token,
        path_style: None,
        auto_create_bucket: Some(false),
    };
    let bad_store = S3BlobStore::connect(&bad_creds_cfg).await.unwrap();
    assert!(matches!(
        bad_store.get_blob(&blob_id).await,
        Err(StoreError::Unauthenticated(_))
    ));
}

#[tokio::test]
async fn s3_auto_create_409_already_exists_anonymous_redirects_and_bounded_errors() {
    // 409 BucketAlreadyOwnedByYou succeeds immediately.
    {
        let state = MockS3State::new(None, DEFAULT_S3_REGION);
        *state.bucket_create_status.lock().unwrap() = Some((
            409,
            "<Error><Code>BucketAlreadyOwnedByYou</Code></Error>".to_string(),
            true,
        ));
        let endpoint = spawn_mock_s3(state.clone()).await;
        let cfg = S3ConnectConfig {
            bucket: "anon-bucket".to_string(),
            endpoint: Some(endpoint),
            ..Default::default()
        };
        let store = S3BlobStore::connect(&cfg).await.unwrap();
        store
            .put_blob(&[0x11, 0x22], Bytes::from_static(b"hello"))
            .await
            .unwrap();
        assert_eq!(
            store.get_blob(&[0x11, 0x22]).await.unwrap(),
            Some(Bytes::from_static(b"hello"))
        );
    }

    // 409 BucketAlreadyExists / empty body verifies via follow-up HEAD: succeeds when
    // accessible (concurrent creation race) and fails when the bucket is not accessible.
    for body in ["<Error><Code>BucketAlreadyExists</Code></Error>", ""] {
        let state_ok = MockS3State::new(None, DEFAULT_S3_REGION);
        *state_ok.bucket_create_status.lock().unwrap() = Some((409, body.to_string(), true));
        let ep_ok = spawn_mock_s3(state_ok.clone()).await;
        let store = S3BlobStore::connect(&S3ConnectConfig {
            bucket: "race-bucket".to_string(),
            endpoint: Some(ep_ok),
            ..Default::default()
        })
        .await
        .unwrap();
        store
            .put_blob(&[0x11, 0x22], Bytes::from_static(b"hello"))
            .await
            .unwrap();

        let state_denied = MockS3State::new(None, DEFAULT_S3_REGION);
        *state_denied.bucket_create_status.lock().unwrap() = Some((409, body.to_string(), false));
        let ep_denied = spawn_mock_s3(state_denied).await;
        let err = S3BlobStore::connect(&S3ConnectConfig {
            bucket: "foreign-bucket".to_string(),
            endpoint: Some(ep_denied),
            ..Default::default()
        })
        .await
        .unwrap_err();
        assert!(
            matches!(
                err,
                StoreError::Backend(_) | StoreError::PermissionDenied(_)
            ),
            "409 {body:?} on inaccessible bucket must fail, got {err:?}"
        );
    }

    // Redirects are refused (never followed) and oversized error bodies are bounded.
    let state = MockS3State::new(None, DEFAULT_S3_REGION);
    state.insert_bucket("guard-bucket");
    let endpoint = spawn_mock_s3(state.clone()).await;
    let cfg = S3ConnectConfig {
        bucket: "guard-bucket".to_string(),
        endpoint: Some(endpoint),
        auto_create_bucket: Some(false),
        ..Default::default()
    };
    let store = S3BlobStore::connect(&cfg).await.unwrap();

    state.redirect_all.store(true, Ordering::Relaxed);
    let redir_err = store.get_blob(&[0xaa, 0xbb]).await.unwrap_err();
    assert!(matches!(redir_err, StoreError::Backend(_)));
    assert!(redir_err.to_string().contains("redirect"));
    state.redirect_all.store(false, Ordering::Relaxed);

    state.oversized_error.store(true, Ordering::Relaxed);
    let big_err = store.get_blob(&[0xaa, 0xbb]).await.unwrap_err();
    assert!(matches!(big_err, StoreError::Unavailable(_)));
    assert!(big_err.to_string().len() < 1024);
    state.oversized_error.store(false, Ordering::Relaxed);

    // Missing bucket on GET returns an error (not Ok(None) / DATA_LOSS) rather than hiding
    // misconfiguration, while missing key in an existing bucket returns Ok(None).
    assert_eq!(store.get_blob(&[0xaa, 0xbb]).await.unwrap(), None);
    assert_eq!(
        store
            .get_blob_range(&[0xaa, 0xbb], 0, Some(4))
            .await
            .unwrap(),
        None
    );
    state.buckets.lock().unwrap().remove("guard-bucket");
    let missing_bucket_get = store.get_blob(&[0xaa, 0xbb]).await.unwrap_err();
    assert!(matches!(missing_bucket_get, StoreError::Backend(_)));
    assert!(missing_bucket_get.to_string().contains("NoSuchBucket"));
    let missing_bucket_range = store
        .get_blob_range(&[0xaa, 0xbb], 0, Some(4))
        .await
        .unwrap_err();
    assert!(matches!(missing_bucket_range, StoreError::Backend(_)));
    assert!(missing_bucket_range.to_string().contains("NoSuchBucket"));
}

#[tokio::test]
async fn s3_blob_store_integrates_with_replicated_blob_store_and_composite_store() {
    let creds = S3Credentials {
        access_key: "repl-ak".to_string(),
        secret_key: "repl-sk".to_string(),
        session_token: None,
    };
    let primary_state = MockS3State::new(Some(creds.clone()), "us-east-1");
    let peer_state = MockS3State::new(Some(creds.clone()), "eu-west-1");
    let primary_ep = spawn_mock_s3(primary_state.clone()).await;
    let peer_ep = spawn_mock_s3(peer_state.clone()).await;

    let primary_cfg = S3ConnectConfig {
        bucket: "s3://primary-region".to_string(),
        endpoint: Some(primary_ep),
        region: "us-east-1".to_string(),
        access_key: Some(creds.access_key.clone()),
        secret_key: Some(creds.secret_key.clone()),
        ..Default::default()
    };
    let primary = Arc::new(S3BlobStore::connect(&primary_cfg).await.unwrap());
    // Cross-region peer spec signs requests with `eu-west-1` while inheriting primary creds.
    let peer_cfg = parse_s3_peer_spec(
        &format!("s3://peer-region:eu-west-1={peer_ep}"),
        &primary_cfg,
    )
    .unwrap();
    let peer = Arc::new(S3BlobStore::connect(&peer_cfg).await.unwrap());

    let metadata = Arc::new(MemoryMetadataStore::new());
    let peer_composite = CompositeStore::new(metadata.clone(), peer.clone());
    peer_composite
        .register_repo("demo-repo", None)
        .await
        .unwrap();

    let file_id = [0x42u8; 20];
    let content = Bytes::from_static(b"replicated-s3-blob-content");
    // Write only to peer bucket + shared metadata (simulating cross-region replication lag).
    peer_composite
        .put_file("demo-repo", &file_id, content.clone())
        .await
        .unwrap();
    assert!(!primary.has_blob(&file_id).await.unwrap());
    assert!(peer.has_blob(&file_id).await.unwrap());

    let replicated = Arc::new(ReplicatedBlobStore::new(
        primary.clone(),
        vec![peer.clone()],
        metadata.clone(),
        ReplicatedBlobStoreConfig::default(),
    ));
    let primary_composite = CompositeStore::new(metadata.clone(), replicated.clone());

    // Reading via primary CompositeStore falls back to peer S3 and triggers background repair.
    let got = primary_composite
        .get_file_range("demo-repo", &file_id, 0, None)
        .await
        .unwrap();
    assert_eq!(got, content);
    assert_eq!(
        replicated.stats().origin_fallbacks.load(Ordering::Relaxed),
        1
    );

    // Wait briefly for background read-repair to copy the blob into the primary S3 bucket.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !primary.has_blob(&file_id).await.unwrap() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for S3 read-repair into primary bucket"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(primary.get_blob(&file_id).await.unwrap(), Some(content));
    assert_eq!(
        replicated
            .stats()
            .read_repairs_completed
            .load(Ordering::Relaxed),
        1
    );
}

#[tokio::test]
async fn s3_get_blob_range_with_record_and_stream_detect_truncation_and_oversized_objects() {
    let state = MockS3State::new(None, DEFAULT_S3_REGION);
    state.insert_bucket("corrupt-bucket");
    let endpoint = spawn_mock_s3(state.clone()).await;
    let store = Arc::new(
        S3BlobStore::connect(&S3ConnectConfig {
            bucket: "corrupt-bucket".to_string(),
            endpoint: Some(endpoint),
            auto_create_bucket: Some(false),
            ..Default::default()
        })
        .await
        .unwrap(),
    );

    let blob_id = vec![0xde, 0xad, 0xbe, 0xef];
    let key = S3BlobStore::object_key(&blob_id).unwrap();
    let record = BlobRecord {
        blob_id: blob_id.clone(),
        content_size: 16,
        inline_content: None,
        primary_location: Some("s3://corrupt-bucket".to_string()),
        replicated_locations: vec!["s3://corrupt-bucket".to_string()],
    };

    // Truncated object on S3 (10 bytes stored, metadata says 16 bytes).
    state
        .buckets
        .lock()
        .unwrap()
        .get_mut("corrupt-bucket")
        .unwrap()
        .insert(key.clone(), b"0123456789".to_vec());

    // EOF check (`offset == record.content_size`) must surface Corruption, not Ok(empty).
    assert!(matches!(
        store.get_blob_range_with_record(&record, 16, Some(4)).await,
        Err(StoreError::Corruption(_))
    ));
    // Full read (`offset == 0`) must surface Corruption.
    assert!(matches!(
        store.get_blob_range_with_record(&record, 0, None).await,
        Err(StoreError::Corruption(_))
    ));
    // Ranged reads spanning past actual EOF or starting past actual EOF surface Corruption.
    assert!(matches!(
        store.get_blob_range_with_record(&record, 6, Some(8)).await,
        Err(StoreError::Corruption(_))
    ));
    assert!(matches!(
        store.get_blob_range_with_record(&record, 12, Some(4)).await,
        Err(StoreError::Corruption(_))
    ));
    assert!(matches!(
        Arc::clone(&store)
            .get_blob_range_stream(record.clone(), 0, None)
            .await,
        Err(StoreError::Corruption(_))
    ));

    // Oversized object on S3 (20 bytes stored, metadata says 16 bytes).
    state
        .buckets
        .lock()
        .unwrap()
        .get_mut("corrupt-bucket")
        .unwrap()
        .insert(key.clone(), b"0123456789abcdefXXXX".to_vec());

    // EOF check (`offset == record.content_size`) detects oversized length via HEAD.
    assert!(matches!(
        store.get_blob_range_with_record(&record, 16, Some(4)).await,
        Err(StoreError::Corruption(_))
    ));
    // Full read (`offset == 0`) detects extra trailing bytes.
    assert!(matches!(
        store.get_blob_range_with_record(&record, 0, None).await,
        Err(StoreError::Corruption(_))
    ));
    // Ranged read through EOF probes +1 byte and detects oversized trailing bytes.
    assert!(matches!(
        store.get_blob_range_with_record(&record, 4, None).await,
        Err(StoreError::Corruption(_))
    ));
    assert!(matches!(
        store.get_blob_range_with_record(&record, 4, Some(12)).await,
        Err(StoreError::Corruption(_))
    ));
    assert!(matches!(
        store.get_blob_range_with_record(&record, 4, Some(13)).await,
        Err(StoreError::Corruption(_))
    ));
    // Streaming range read through EOF detects oversized object via +1 probe.
    assert!(matches!(
        Arc::clone(&store)
            .get_blob_range_stream(record.clone(), 4, None)
            .await,
        Err(StoreError::Corruption(_))
    ));
    assert!(matches!(
        Arc::clone(&store)
            .get_blob_range_stream(record.clone(), 0, None)
            .await,
        Err(StoreError::Corruption(_))
    ));

    // Oversized 0-byte record (metadata says 0 bytes, S3 has 4 bytes).
    let zero_record = BlobRecord {
        blob_id: blob_id.clone(),
        content_size: 0,
        inline_content: None,
        primary_location: Some("s3://corrupt-bucket".to_string()),
        replicated_locations: vec!["s3://corrupt-bucket".to_string()],
    };
    assert!(matches!(
        store
            .get_blob_range_with_record(&zero_record, 0, Some(1))
            .await,
        Err(StoreError::Corruption(_))
    ));

    // Mismatched `Content-Range` start offset on 206 Partial Content surfaces Corruption
    // across get_blob_range, get_blob_range_with_record, and get_blob_range_stream.
    state
        .buckets
        .lock()
        .unwrap()
        .get_mut("corrupt-bucket")
        .unwrap()
        .insert(key, b"0123456789abcdef".to_vec());
    state.corrupt_206_start.store(true, Ordering::Relaxed);
    assert!(matches!(
        store.get_blob_range(&blob_id, 4, Some(6)).await,
        Err(StoreError::Corruption(_))
    ));
    assert!(matches!(
        store.get_blob_range_with_record(&record, 4, Some(6)).await,
        Err(StoreError::Corruption(_))
    ));
    assert!(matches!(
        Arc::clone(&store)
            .get_blob_range_stream(record, 4, Some(6))
            .await,
        Err(StoreError::Corruption(_))
    ));
    state.corrupt_206_start.store(false, Ordering::Relaxed);
}

#[tokio::test]
async fn s3_put_blob_from_file_streams_with_sigv4_content_length_and_idempotency() {
    let creds = S3Credentials {
        access_key: "stream-ak".to_string(),
        secret_key: "stream-sk-secret".to_string(),
        session_token: Some("stream-session-tok".to_string()),
    };
    let state = MockS3State::new(Some(creds.clone()), "us-west-2");
    let endpoint = spawn_mock_s3(state.clone()).await;
    let store = S3BlobStore::connect(&S3ConnectConfig {
        bucket: "stream-bucket".to_string(),
        endpoint: Some(endpoint),
        region: "us-west-2".to_string(),
        access_key: Some(creds.access_key.clone()),
        secret_key: Some(creds.secret_key.clone()),
        session_token: creds.session_token.clone(),
        ..Default::default()
    })
    .await
    .unwrap();

    let dir = tempfile::tempdir().unwrap();

    // Multi-chunk (>64 KiB) staged file streams without buffering full blob in RAM.
    let payload: Vec<u8> = (0..(160 * 1024 + 37)).map(|i| (i % 251) as u8).collect();
    let file_path = dir.path().join("staged.bin");
    std::fs::write(&file_path, &payload).unwrap();
    let blob_id = [0x5a, 0x6b, 0x7c, 0x8d];

    store
        .put_blob_from_file(&blob_id, &file_path, payload.len() as u64)
        .await
        .unwrap();
    {
        let reqs = state.requests.lock().unwrap();
        let put_req = reqs.last().unwrap();
        assert_eq!(put_req.method, "PUT");
        assert_eq!(put_req.path, "/stream-bucket/blobs/5a/6b/5a6b7c8d");
        assert_eq!(
            put_req.headers.get("content-length").map(String::as_str),
            Some(payload.len().to_string().as_str())
        );
        assert_eq!(
            put_req
                .headers
                .get("x-amz-content-sha256")
                .map(String::as_str),
            Some(sha256_hex(&payload).as_str())
        );
        assert_eq!(
            put_req.headers.get("if-none-match").map(String::as_str),
            Some("*")
        );
        assert_eq!(
            put_req.headers.get("cache-control").map(String::as_str),
            Some(IMMUTABLE_CACHE_CONTROL)
        );
        assert_eq!(put_req.body, payload);
    }
    assert_eq!(
        store.get_blob(&blob_id).await.unwrap(),
        Some(Bytes::from(payload.clone()))
    );

    // Duplicate `put_blob_from_file` hits 412 Precondition Failed and succeeds idempotently.
    store
        .put_blob_from_file(&blob_id, &file_path, payload.len() as u64)
        .await
        .unwrap();

    // 0-byte staged file roundtrips.
    let empty_path = dir.path().join("empty.bin");
    std::fs::write(&empty_path, b"").unwrap();
    let empty_id = [0x01, 0x02, 0x03, 0x04];
    store
        .put_blob_from_file(&empty_id, &empty_path, 0)
        .await
        .unwrap();
    assert_eq!(store.get_blob(&empty_id).await.unwrap(), Some(Bytes::new()));

    // Size mismatch fails before issuing any PUT request.
    let puts_before = state.put_count.load(Ordering::Relaxed);
    let err_short = store
        .put_blob_from_file(&[0xfe, 0xed], &file_path, payload.len() as u64 + 1)
        .await
        .unwrap_err();
    assert!(matches!(err_short, StoreError::Backend(_)));
    let err_long = store
        .put_blob_from_file(&[0xfe, 0xed], &file_path, payload.len() as u64 - 1)
        .await
        .unwrap_err();
    assert!(matches!(err_long, StoreError::Backend(_)));
    assert_eq!(state.put_count.load(Ordering::Relaxed), puts_before);
}

#[tokio::test]
async fn s3_get_blob_range_stream_streams_single_get_rejects_oversized_chunked_and_uses_read_timeout()
 {
    // Single-GET multi-chunk streaming (full, +1 EOF probe, bounded range, ignore_range).
    let state = MockS3State::new(None, DEFAULT_S3_REGION);
    state.insert_bucket("stream-read-bucket");
    let endpoint = spawn_mock_s3(state.clone()).await;
    let store = Arc::new(
        S3BlobStore::connect(&S3ConnectConfig {
            bucket: "stream-read-bucket".to_string(),
            endpoint: Some(endpoint),
            auto_create_bucket: Some(false),
            ..Default::default()
        })
        .await
        .unwrap(),
    );

    let payload: Vec<u8> = (0..(200 * 1024 + 19)).map(|i| (i % 251) as u8).collect();
    let blob_id = vec![0x12, 0x34, 0x56, 0x78];
    store
        .put_blob(&blob_id, Bytes::from(payload.clone()))
        .await
        .unwrap();
    let record = BlobRecord {
        blob_id: blob_id.clone(),
        content_size: payload.len() as u64,
        inline_content: None,
        primary_location: Some("s3://stream-read-bucket".to_string()),
        replicated_locations: vec!["s3://stream-read-bucket".to_string()],
    };

    // Full read streams in <=64 KiB frames with a single GET and no Range header.
    let reqs_before = state.requests.lock().unwrap().len();
    let rx = Arc::clone(&store)
        .get_blob_range_stream(record.clone(), 0, None)
        .await
        .unwrap()
        .expect("existing blob should stream");
    let (got_full, frames) = collect_stream(rx).await.unwrap();
    assert_eq!(got_full, payload);
    assert!(frames >= 4, "expected multi-frame stream, got {frames}");
    {
        let reqs = state.requests.lock().unwrap();
        assert_eq!(reqs.len() - reqs_before, 1);
        assert_eq!(reqs.last().unwrap().method, "GET");
        assert!(!reqs.last().unwrap().headers.contains_key("range"));
    }

    // Partial-to-EOF read sends a single GET with a +1 EOF range probe.
    let reqs_before = state.requests.lock().unwrap().len();
    let offset = 70_000u64;
    let rx = Arc::clone(&store)
        .get_blob_range_stream(record.clone(), offset, None)
        .await
        .unwrap()
        .unwrap();
    let (got_suffix, _) = collect_stream(rx).await.unwrap();
    assert_eq!(got_suffix, payload[offset as usize..]);
    {
        let reqs = state.requests.lock().unwrap();
        assert_eq!(reqs.len() - reqs_before, 1);
        assert_eq!(
            reqs.last()
                .unwrap()
                .headers
                .get("range")
                .map(String::as_str),
            Some(format!("bytes={offset}-{}", payload.len()).as_str())
        );
    }

    // Bounded mid-range read sends exact inclusive end.
    let rx = Arc::clone(&store)
        .get_blob_range_stream(record.clone(), 10_000, Some(80_000))
        .await
        .unwrap()
        .unwrap();
    let (got_mid, _) = collect_stream(rx).await.unwrap();
    assert_eq!(got_mid, payload[10_000..90_000]);
    assert_eq!(
        state
            .requests
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .headers
            .get("range")
            .map(String::as_str),
        Some("bytes=10000-89999")
    );

    // Server ignoring Range (returning 200 OK) streams only the requested slice.
    state.ignore_range.store(true, Ordering::Relaxed);
    let rx = Arc::clone(&store)
        .get_blob_range_stream(record.clone(), 70_000, Some(75_000))
        .await
        .unwrap()
        .unwrap();
    let (got_sliced, _) = collect_stream(rx).await.unwrap();
    assert_eq!(got_sliced, payload[70_000..145_000]);
    state.ignore_range.store(false, Ordering::Relaxed);

    // Chunked responses without Content-Length: incremental oversize rejection aborts
    // immediately without buffering an unbounded body, and truncated streams fail at EOF.
    #[derive(Clone, Copy)]
    enum ChunkedMode {
        Oversized200,
        Oversized206,
        Truncated200,
        SlowValid200,
        Stalled200,
        CancelOnDrop200,
        CancelOnDrop206,
    }

    let mode = Arc::new(Mutex::new(ChunkedMode::Oversized200));
    let server_chunks_written = Arc::new(AtomicUsize::new(0));
    let resume_oversized = Arc::new(tokio::sync::Notify::new());
    let oversized_done = Arc::new(tokio::sync::Notify::new());
    let cancel_done = Arc::new(tokio::sync::Notify::new());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mode_c = Arc::clone(&mode);
    let written_c = Arc::clone(&server_chunks_written);
    let resume_c = Arc::clone(&resume_oversized);
    let done_c = Arc::clone(&oversized_done);
    let cancel_c = Arc::clone(&cancel_done);
    let server_task = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let current_mode = *mode_c.lock().unwrap();
            let written = Arc::clone(&written_c);
            let resume = Arc::clone(&resume_c);
            let done = Arc::clone(&done_c);
            let cancel = Arc::clone(&cancel_c);
            tokio::spawn(async move {
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                loop {
                    line.clear();
                    if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                        return;
                    }
                    if line.trim().is_empty() {
                        break;
                    }
                }
                let stream = reader.get_mut();
                async fn send_chunk(
                    s: &mut tokio::net::TcpStream,
                    data: &[u8],
                ) -> std::io::Result<()> {
                    let header = format!("{:x}\r\n", data.len());
                    s.write_all(header.as_bytes()).await?;
                    s.write_all(data).await?;
                    s.write_all(b"\r\n").await?;
                    s.flush().await
                }
                match current_mode {
                    ChunkedMode::Oversized200 | ChunkedMode::Oversized206 => {
                        let status = if matches!(current_mode, ChunkedMode::Oversized206) {
                            "HTTP/1.1 206 Partial Content\r\nTransfer-Encoding: chunked\r\n\r\n"
                        } else {
                            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
                        };
                        if stream.write_all(status.as_bytes()).await.is_err() {
                            done.notify_one();
                            return;
                        }
                        // Send a small first chunk and wait for the client to observe it
                        // before flooding oversized chunks, proving true incremental streaming.
                        if send_chunk(stream, b"01234567").await.is_err() {
                            done.notify_one();
                            return;
                        }
                        resume.notified().await;
                        let block = vec![b'X'; 64 * 1024];
                        // Attempt to stream 64 MiB; client must abort as soon as expected_len
                        // is exceeded rather than draining the full stream.
                        for _ in 0..1024 {
                            if send_chunk(stream, &block).await.is_err() {
                                break;
                            }
                            written.fetch_add(1, Ordering::Relaxed);
                        }
                        let _ = stream.write_all(b"0\r\n\r\n").await;
                        done.notify_one();
                    }
                    ChunkedMode::Truncated200 => {
                        let _ = stream
                            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                            .await;
                        let _ = send_chunk(stream, b"short").await;
                        let _ = stream.write_all(b"0\r\n\r\n").await;
                    }
                    ChunkedMode::SlowValid200 => {
                        let _ = stream
                            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                            .await;
                        for part in [b"aaaa", b"bbbb", b"cccc", b"dddd"] {
                            tokio::time::sleep(Duration::from_millis(90)).await;
                            if send_chunk(stream, part).await.is_err() {
                                return;
                            }
                        }
                        let _ = stream.write_all(b"0\r\n\r\n").await;
                    }
                    ChunkedMode::Stalled200 => {
                        let _ = stream
                            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                            .await;
                        let _ = send_chunk(stream, b"aaaa").await;
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                    ChunkedMode::CancelOnDrop200 => {
                        let _ = stream
                            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                            .await;
                        let _ = send_chunk(stream, b"aaaa").await;
                        let mut eof = [0u8; 1];
                        let _ = stream.read(&mut eof).await;
                        cancel.notify_one();
                    }
                    ChunkedMode::CancelOnDrop206 => {
                        let _ = stream
                            .write_all(
                                b"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 4-11/16\r\nTransfer-Encoding: chunked\r\n\r\n",
                            )
                            .await;
                        let _ = send_chunk(stream, b"4567").await;
                        let mut eof = [0u8; 1];
                        let _ = stream.read(&mut eof).await;
                        cancel.notify_one();
                    }
                }
            });
        }
    });

    let chunked_cfg = S3ConnectConfig {
        bucket: "chunked-bucket".to_string(),
        endpoint: Some(format!("http://{addr}")),
        auto_create_bucket: Some(false),
        ..Default::default()
    };
    let fast_timeout_store = Arc::new(
        S3BlobStore::connect_with_timeouts(
            &chunked_cfg,
            Duration::from_secs(2),
            Duration::from_millis(250),
        )
        .await
        .unwrap(),
    );
    let small_record = BlobRecord {
        blob_id: vec![0xaa, 0xbb, 0xcc, 0xdd],
        content_size: 16,
        inline_content: None,
        primary_location: Some("s3://chunked-bucket".to_string()),
        replicated_locations: vec!["s3://chunked-bucket".to_string()],
    };

    // Oversized chunked 200 OK streams the first chunk before EOF, then aborts immediately
    // with Corruption without draining 64 MiB.
    *mode.lock().unwrap() = ChunkedMode::Oversized200;
    server_chunks_written.store(0, Ordering::Relaxed);
    let mut rx = Arc::clone(&fast_timeout_store)
        .get_blob_range_stream(small_record.clone(), 0, None)
        .await
        .unwrap()
        .unwrap();
    let first = rx.recv().await.unwrap().unwrap();
    assert_eq!(first, Bytes::from_static(b"01234567"));
    resume_oversized.notify_one();
    let err = collect_stream(rx).await.unwrap_err();
    assert!(
        matches!(err, StoreError::Corruption(_)),
        "expected Corruption for oversized chunked 200, got {err:?}"
    );
    tokio::time::timeout(Duration::from_secs(5), oversized_done.notified())
        .await
        .expect("server task should observe early client disconnect");
    assert!(
        server_chunks_written.load(Ordering::Relaxed) < 512,
        "client should abort connection early instead of draining huge oversized stream (wrote {} chunks)",
        server_chunks_written.load(Ordering::Relaxed)
    );

    // Oversized chunked 206 Partial Content streams the first chunk, then rejects
    // incrementally with Corruption.
    *mode.lock().unwrap() = ChunkedMode::Oversized206;
    server_chunks_written.store(0, Ordering::Relaxed);
    let mut rx = Arc::clone(&fast_timeout_store)
        .get_blob_range_stream(small_record.clone(), 4, None)
        .await
        .unwrap()
        .unwrap();
    let first = rx.recv().await.unwrap().unwrap();
    assert_eq!(first, Bytes::from_static(b"01234567"));
    resume_oversized.notify_one();
    let err = collect_stream(rx).await.unwrap_err();
    assert!(
        matches!(err, StoreError::Corruption(_)),
        "expected Corruption for oversized chunked 206, got {err:?}"
    );
    tokio::time::timeout(Duration::from_secs(5), oversized_done.notified())
        .await
        .expect("server task should observe early client disconnect on 206");
    assert!(
        server_chunks_written.load(Ordering::Relaxed) < 512,
        "client should abort 206 connection early instead of draining huge oversized stream (wrote {} chunks)",
        server_chunks_written.load(Ordering::Relaxed)
    );

    // Truncated chunked 200 OK fails at EOF with Corruption.
    *mode.lock().unwrap() = ChunkedMode::Truncated200;
    let rx = Arc::clone(&fast_timeout_store)
        .get_blob_range_stream(small_record.clone(), 0, None)
        .await
        .unwrap()
        .unwrap();
    let err = collect_stream(rx).await.unwrap_err();
    assert!(
        matches!(err, StoreError::Corruption(_)),
        "expected Corruption for truncated chunked 200, got {err:?}"
    );

    // Slow steady stream exceeding total read_timeout (4 * 90ms = 360ms > 250ms) succeeds
    // because `.read_timeout(...)` resets on every arriving chunk.
    *mode.lock().unwrap() = ChunkedMode::SlowValid200;
    let rx = Arc::clone(&fast_timeout_store)
        .get_blob_range_stream(small_record.clone(), 0, None)
        .await
        .unwrap()
        .unwrap();
    let (got_slow, _) = collect_stream(rx).await.unwrap();
    assert_eq!(got_slow, b"aaaabbbbccccdddd");

    // Stalled stream that stops sending bytes past read_timeout surfaces Unavailable.
    *mode.lock().unwrap() = ChunkedMode::Stalled200;
    let rx = Arc::clone(&fast_timeout_store)
        .get_blob_range_stream(small_record.clone(), 0, None)
        .await
        .unwrap()
        .unwrap();
    let err = collect_stream(rx).await.unwrap_err();
    assert!(
        matches!(err, StoreError::Unavailable(_)),
        "expected Unavailable for stalled read timeout, got {err:?}"
    );

    // Dropping the receiver cancels stalled upstream reads immediately (well before the 10s
    // read_timeout), including during 200 OK prefix-skipping and mid-stream 206 reads.
    let long_timeout_store = Arc::new(
        S3BlobStore::connect_with_timeouts(
            &chunked_cfg,
            Duration::from_secs(2),
            Duration::from_secs(10),
        )
        .await
        .unwrap(),
    );

    *mode.lock().unwrap() = ChunkedMode::CancelOnDrop200;
    let rx = Arc::clone(&long_timeout_store)
        .get_blob_range_stream(small_record.clone(), 8, None)
        .await
        .unwrap()
        .unwrap();
    // Let the spawned task consume and skip the prefix chunk so it is blocked inside
    // `resp.chunk()` before the receiver drops.
    tokio::time::sleep(Duration::from_millis(20)).await;
    drop(rx);
    tokio::time::timeout(Duration::from_secs(2), cancel_done.notified())
        .await
        .expect("dropping 200 OK stream receiver must cancel upstream read before read_timeout");

    *mode.lock().unwrap() = ChunkedMode::CancelOnDrop206;
    let mut rx = Arc::clone(&long_timeout_store)
        .get_blob_range_stream(small_record, 4, Some(8))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        rx.recv().await.unwrap().unwrap(),
        Bytes::from_static(b"4567")
    );
    drop(rx);
    tokio::time::timeout(Duration::from_secs(2), cancel_done.notified())
        .await
        .expect("dropping 206 stream receiver must cancel upstream read before read_timeout");

    server_task.abort();
}

#[tokio::test]
async fn s3_get_blob_range_guards_offsets_past_i64_max() {
    let state = MockS3State::new(None, DEFAULT_S3_REGION);
    state.insert_bucket("range-guard-bucket");
    let endpoint = spawn_mock_s3(state.clone()).await;
    let store = S3BlobStore::connect(&S3ConnectConfig {
        bucket: "range-guard-bucket".to_string(),
        endpoint: Some(endpoint),
        auto_create_bucket: Some(false),
        ..Default::default()
    })
    .await
    .unwrap();

    let existing_id = [0x12, 0x34, 0x56];
    let missing_id = [0x78, 0x9a, 0xbc];
    store
        .put_blob(&existing_id, Bytes::from_static(b"payload"))
        .await
        .unwrap();

    let huge_offset = (i64::MAX as u64) + 1;

    let reqs_before = state.requests.lock().unwrap().len();
    let err = store
        .get_blob_range(&existing_id, huge_offset, Some(16))
        .await
        .unwrap_err();
    assert!(
        matches!(err, StoreError::OutOfRange(_)),
        "expected OutOfRange for existing blob at offset > i64::MAX, got {err:?}"
    );
    {
        let reqs = state.requests.lock().unwrap();
        assert_eq!(reqs.len() - reqs_before, 1);
        assert_eq!(reqs.last().unwrap().method, "HEAD");
    }

    let reqs_before = state.requests.lock().unwrap().len();
    assert_eq!(
        store
            .get_blob_range(&missing_id, huge_offset, Some(16))
            .await
            .unwrap(),
        None
    );
    {
        let reqs = state.requests.lock().unwrap();
        assert_eq!(reqs.len() - reqs_before, 1);
        assert_eq!(reqs.last().unwrap().method, "HEAD");
    }
}

#[tokio::test]
async fn s3_blob_store_retries_transient_errors_and_never_retries_permanent_4xx() {
    let creds = S3Credentials {
        access_key: "retry-ak".to_string(),
        secret_key: "retry-sk".to_string(),
        session_token: Some("retry-tok".to_string()),
    };
    let state = MockS3State::new(Some(creds.clone()), "us-west-2");
    let endpoint = spawn_mock_s3(state.clone()).await;

    // ensure_bucket_exists retries transient HEAD (503) and PUT (500) during connect.
    state.push_status(Some("HEAD"), 503, "<Error><Code>SlowDown</Code></Error>");
    state.push_status(
        Some("PUT"),
        500,
        "<Error><Code>InternalError</Code></Error>",
    );
    let store = Arc::new(
        S3BlobStore::connect(&S3ConnectConfig {
            bucket: "retry-bucket".to_string(),
            endpoint: Some(endpoint),
            region: "us-west-2".to_string(),
            access_key: Some(creds.access_key),
            secret_key: Some(creds.secret_key),
            session_token: creds.session_token,
            ..Default::default()
        })
        .await
        .unwrap(),
    );
    {
        let reqs = state.requests.lock().unwrap();
        // HEAD (503) -> HEAD (404) -> PUT (500) -> PUT (200)
        assert_eq!(reqs.len(), 4);
    }

    // put_blob retries 503, 429, 400 RequestTimeout, and 409 ConditionalRequestConflict
    // (when object is not yet visible on HEAD) before succeeding.
    let blob_a = [0x01, 0x02, 0x03, 0x04];
    state.push_status(Some("PUT"), 503, "<Error><Code>SlowDown</Code></Error>");
    state.push_status(
        Some("PUT"),
        429,
        "<Error><Code>TooManyRequests</Code></Error>",
    );
    store
        .put_blob(&blob_a, Bytes::from_static(b"hello-retries"))
        .await
        .unwrap();

    let blob_b = [0x05, 0x06, 0x07, 0x08];
    state.push_status(
        Some("PUT"),
        400,
        "<Error><Code>RequestTimeout</Code></Error>",
    );
    state.push_status(
        Some("PUT"),
        409,
        "<Error><Code>ConditionalRequestConflict</Code></Error>",
    );
    store
        .put_blob(&blob_b, Bytes::from_static(b"conflict-then-ok"))
        .await
        .unwrap();
    assert_eq!(
        store.get_blob(&blob_b).await.unwrap(),
        Some(Bytes::from_static(b"conflict-then-ok"))
    );

    // put_blob_from_file reopens a multi-chunk staged file after a 503 and a
    // dropped TCP connection, completing with valid SigV4 and full bytes.
    let dir = tempfile::tempdir().unwrap();
    let payload: Vec<u8> = (0..(140 * 1024 + 13)).map(|i| (i % 251) as u8).collect();
    let staged_path = dir.path().join("retry-staged.bin");
    std::fs::write(&staged_path, &payload).unwrap();
    let blob_staged = [0xaa, 0xbb, 0xcc, 0xdd];
    state.push_status(Some("PUT"), 503, "<Error><Code>SlowDown</Code></Error>");
    state.push_drop(Some("PUT"));
    store
        .put_blob_from_file(&blob_staged, &staged_path, payload.len() as u64)
        .await
        .unwrap();
    assert_eq!(
        store.get_blob(&blob_staged).await.unwrap(),
        Some(Bytes::from(payload.clone()))
    );

    // get_blob, get_blob_range, get_blob_range_stream, and has_blob recover from transient
    // HTTP errors and stale keep-alive connection drops.
    state.push_drop(Some("GET"));
    assert_eq!(
        store.get_blob(&blob_a).await.unwrap(),
        Some(Bytes::from_static(b"hello-retries"))
    );

    state.push_status(Some("GET"), 502, "Bad Gateway");
    assert_eq!(
        store.get_blob_range(&blob_a, 6, Some(7)).await.unwrap(),
        Some(Bytes::from_static(b"retries"))
    );

    state.push_status(Some("GET"), 504, "Gateway Timeout");
    let record = BlobRecord {
        blob_id: blob_a.to_vec(),
        content_size: b"hello-retries".len() as u64,
        inline_content: None,
        primary_location: Some("s3://retry-bucket".to_string()),
        replicated_locations: vec!["s3://retry-bucket".to_string()],
    };
    let rx = Arc::clone(&store)
        .get_blob_range_stream(record, 0, None)
        .await
        .unwrap()
        .unwrap();
    let (streamed, _) = collect_stream(rx).await.unwrap();
    assert_eq!(streamed, b"hello-retries");

    state.push_status(Some("HEAD"), 408, "Request Timeout");
    assert!(store.has_blob(&blob_a).await.unwrap());

    // Exhausting MAX_RETRY_ATTEMPTS returns Unavailable after exactly 3 attempts.
    let reqs_before = state.requests.lock().unwrap().len();
    for _ in 0..MAX_RETRY_ATTEMPTS {
        state.push_status(Some("GET"), 503, "<Error><Code>SlowDown</Code></Error>");
    }
    let err = store.get_blob(&blob_a).await.unwrap_err();
    assert!(matches!(err, StoreError::Unavailable(_)));
    assert_eq!(
        state.requests.lock().unwrap().len() - reqs_before,
        MAX_RETRY_ATTEMPTS
    );

    // Non-retryable 4xx (400 non-timeout, 403, 404) never retry (exactly 1 request each).
    for (code, body, expect_err) in [
        (400, "<Error><Code>InvalidArgument</Code></Error>", true),
        (403, "<Error><Code>AccessDenied</Code></Error>", true),
        (404, "<Error><Code>NoSuchKey</Code></Error>", false),
    ] {
        let reqs_before = state.requests.lock().unwrap().len();
        state.push_status(Some("GET"), code, body);
        let res = store.get_blob(&blob_a).await;
        if expect_err {
            assert!(
                res.is_err(),
                "expected error for HTTP {code} {body}, got {res:?}"
            );
        } else {
            assert_eq!(res.unwrap(), None);
        }
        assert_eq!(
            state.requests.lock().unwrap().len() - reqs_before,
            1,
            "HTTP {code} must not be retried"
        );
    }
}
