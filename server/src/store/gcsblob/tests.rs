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

use std::sync::Arc;

use super::*;
use bytes::Bytes;
use google_cloud_gax::error::rpc::Status as RpcStatus;
use google_cloud_gax::error::{CredentialsError, Error as GaxError};
use google_cloud_storage::error::ChecksumMismatch;

#[test]
fn loopback_endpoint_host_detection_rejects_remote_plaintext_endpoints() {
    for ok in [
        "127.0.0.1:4443",
        "127.0.0.1",
        "localhost:4443",
        "LOCALHOST",
        "[::1]:4443",
        "::1",
    ] {
        assert!(is_loopback_endpoint_host(ok), "{ok}");
    }
    for remote in [
        "storage.googleapis.com",
        "storage.googleapis.com:443",
        "gcs-emulator:4443",
        "10.0.0.5:4443",
        "[::1]evil.com",
        "localhost@evil.com",
        "127.0.0.1:0",
        "127.0.0.1:99999",
    ] {
        assert!(!is_loopback_endpoint_host(remote), "{remote}");
    }
}

#[test]
fn normalize_bucket_identifiers_enforces_gcs_naming_rules() {
    assert_eq!(
        normalize_bucket_identifiers("my-bucket_1.example").unwrap(),
        (
            "projects/_/buckets/my-bucket_1.example".to_string(),
            "gs://my-bucket_1.example".to_string()
        )
    );
    assert_eq!(
        normalize_bucket_identifiers("gs://abc/").unwrap(),
        ("projects/_/buckets/abc".to_string(), "gs://abc".to_string())
    );
    assert_eq!(
        normalize_bucket_identifiers("projects/_/buckets/jj-blobs-uc1").unwrap(),
        (
            "projects/_/buckets/jj-blobs-uc1".to_string(),
            "gs://jj-blobs-uc1".to_string()
        )
    );
    let max_len = "a".repeat(222);
    assert!(normalize_bucket_identifiers(&max_len).is_ok());

    let too_long = "a".repeat(223);
    for invalid in [
        "",
        "ab",
        &too_long,
        ".",
        "..",
        "...",
        "a..b",
        ".abc",
        "abc.",
        "-abc",
        "abc-",
        "_abc",
        "abc_",
        "UPPER",
        "projects/my-proj/buckets/jj-blobs",
        "foo/bar/buckets/jj-blobs",
        "gs://my-bucket/extra",
        "gs://my-bucket//",
        "projects/_/buckets/",
        "\r\n",
        "my-bucket\n",
        "gs://my-bucket\r\n",
        "\tmy-bucket",
        "my-bucket\t",
        "my-\0bucket",
    ] {
        let err = normalize_bucket_identifiers(invalid).unwrap_err();
        assert!(
            matches!(err, StoreError::InvalidArgument(_)),
            "expected InvalidArgument for {invalid:?}, got {err:?}"
        );
    }
}

#[test]
fn object_key_enforces_1_to_64_byte_blob_ids() {
    assert!(matches!(
        GcsBlobStore::<DefaultStorage>::object_key(&[]),
        Err(StoreError::InvalidArgument(_))
    ));
    assert!(GcsBlobStore::<DefaultStorage>::object_key(&[0xab; 64]).is_ok());
    assert!(matches!(
        GcsBlobStore::<DefaultStorage>::object_key(&[0xab; 65]),
        Err(StoreError::InvalidArgument(_))
    ));
}

#[test]
fn strict_endpoint_validation_covers_http_https_and_portless_loopback() {
    let ep = parse_gcs_endpoint("HTTP://localhost", false).unwrap();
    assert!(ep.is_http);
    assert_eq!(ep.url, "http://localhost");
    assert_eq!(ep.host_header, "localhost");
    assert_eq!(ep.connect_addr, "localhost:80");

    let ep = parse_gcs_endpoint("http://127.0.0.1", false).unwrap();
    assert!(ep.is_http);
    assert_eq!(ep.connect_addr, "127.0.0.1:80");

    let ep = parse_gcs_endpoint("http://[::1]", false).unwrap();
    assert!(ep.is_http);
    assert_eq!(ep.url, "http://[::1]");
    assert_eq!(ep.host_header, "[::1]");
    assert_eq!(ep.connect_addr, "[::1]:80");

    let ep = parse_gcs_endpoint("::1", false).unwrap();
    assert!(ep.is_http);
    assert_eq!(ep.url, "http://[::1]");
    assert_eq!(ep.connect_addr, "[::1]:80");

    let ep = parse_gcs_endpoint("HTTPS://storage.googleapis.com:443/storage", false).unwrap();
    assert!(!ep.is_http);
    assert_eq!(ep.url, "https://storage.googleapis.com:443/storage");
    assert_eq!(ep.connect_addr, "storage.googleapis.com:443");

    let ep = parse_gcs_endpoint("gcs-emulator", true).unwrap();
    assert!(ep.is_http);
    assert_eq!(ep.url, "http://gcs-emulator");
    assert_eq!(ep.connect_addr, "gcs-emulator:80");

    let ep = parse_gcs_endpoint("http://127.0.0.1:4443/", false).unwrap();
    assert!(ep.is_http);
    assert_eq!(ep.url, "http://127.0.0.1:4443");

    let remote_http_err = parse_gcs_endpoint("http://storage.googleapis.com:4443", false)
        .unwrap_err()
        .to_string();
    assert!(
        remote_http_err.contains("unless host is loopback or matches STORAGE_EMULATOR_HOST"),
        "unexpected plaintext error wording: {remote_http_err}"
    );

    assert!(matches!(
        parse_gcs_endpoint("gcs-emulator/prefix", true),
        Err(StoreError::InvalidArgument(_))
    ));

    for invalid in [
        "",
        "   ",
        "https://",
        "http://",
        "https:///path",
        "https://:443",
        "https://user:pass@storage.googleapis.com",
        "http://127.0.0.1@evil.example:4443",
        "https://storage.googleapis.com?foo=bar",
        "https://storage.googleapis.com#frag",
        "http://127.0.0.1:4443\r\nX-Injected: 1",
        "https://storage.googleapis.com\n",
        "http://127.0.0.1:4443/bad path",
        "http://127.0.0.1:4443/prefix",
        "http://localhost:4443/storage/v1",
        "http://127.0.0.1:4443//",
        "http://[::1]evil.example:4443",
        "https://[::1]trailing",
        "https://[not-ipv6]:443",
        "https://[::1",
        "https://::1",
        "https://storage.googleapis.com:",
        "https://storage.googleapis.com:0",
        "https://storage.googleapis.com:65536",
        "https://storage.googleapis.com:abc",
        "https://storage.googleapis.com:+443",
        "HTTP://storage.googleapis.com",
        "http://storage.googleapis.com:4443",
        "storage.googleapis.com",
        "ftp://127.0.0.1:4443",
    ] {
        let err = parse_gcs_endpoint(invalid, false).unwrap_err();
        assert!(
            matches!(err, StoreError::InvalidArgument(_)),
            "expected InvalidArgument for {invalid:?}, got {err:?}"
        );
    }
}

#[test]
fn map_gcs_err_maps_transport_auth_rpc_and_http_status_codes() {
    let auth_err = GaxError::authentication(CredentialsError::from_source(
        false,
        std::io::Error::other("missing credentials"),
    ));
    assert!(matches!(
        map_gcs_err(auth_err),
        StoreError::Unauthenticated(_)
    ));

    for transport_err in [
        GaxError::connect("dns failed"),
        GaxError::timeout("deadline exceeded"),
        GaxError::io("connection reset"),
        GaxError::exhausted("retry budget exhausted"),
    ] {
        let mapped = map_gcs_err(transport_err);
        assert!(
            matches!(mapped, StoreError::Unavailable(_)),
            "expected Unavailable, got {mapped:?}"
        );
    }

    for (code, check) in [
        (400u16, "invalid"),
        (401, "unauth"),
        (403, "perm"),
        (408, "unavail"),
        (416, "range"),
        (429, "exhausted"),
        (500, "unavail"),
        (502, "unavail"),
        (503, "unavail"),
        (504, "unavail"),
        (418, "backend"),
    ] {
        let err = GaxError::http(code, Default::default(), Bytes::from_static(b"err"));
        let mapped = map_gcs_err(err);
        let ok = match check {
            "invalid" => matches!(mapped, StoreError::InvalidArgument(_)),
            "unauth" => matches!(mapped, StoreError::Unauthenticated(_)),
            "perm" => matches!(mapped, StoreError::PermissionDenied(_)),
            "unavail" => matches!(mapped, StoreError::Unavailable(_)),
            "range" => matches!(mapped, StoreError::OutOfRange(_)),
            "exhausted" => matches!(mapped, StoreError::ResourceExhausted(_)),
            "backend" => matches!(mapped, StoreError::Backend(_)),
            _ => unreachable!(),
        };
        assert!(ok, "HTTP {code} mapped to unexpected variant {mapped:?}");
    }

    let rpc_err = GaxError::service(RpcStatus::default().set_code(RpcCode::PermissionDenied));
    assert!(matches!(
        map_gcs_err(rpc_err),
        StoreError::PermissionDenied(_)
    ));

    let read_crc_err = GaxError::deser(ReadError::ChecksumMismatch(ChecksumMismatch::Crc32c {
        got: 1,
        want: 2,
    }));
    assert!(matches!(
        map_gcs_err(read_crc_err),
        StoreError::Corruption(_)
    ));

    let write_crc_err = GaxError::ser(WriteError::ChecksumMismatch {
        mismatch: ChecksumMismatch::Crc32c { got: 3, want: 4 },
        object: Box::default(),
    });
    assert!(matches!(
        map_gcs_err(write_crc_err),
        StoreError::Corruption(_)
    ));
}

#[test]
fn parse_content_range_total_extracts_size_from_416_headers() {
    let mut headers = tonic::codegen::http::HeaderMap::new();
    headers.insert(
        "content-range",
        tonic::codegen::http::HeaderValue::from_static("bytes */42"),
    );
    let err = GaxError::http(416, headers, Bytes::new());
    assert_eq!(parse_content_range_total(&err), Some(42));
}

#[tokio::test]
async fn ensure_emulator_bucket_http10_sends_host_header_and_handles_200_and_409() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    for status_line in ["HTTP/1.0 200 OK", "HTTP/1.0 409 Conflict"] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let status_owned = status_line.to_string();
        let server_task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let n = stream.read(&mut buf).await.unwrap();
            let req = String::from_utf8_lossy(&buf[..n]).into_owned();
            let resp = format!("{status_owned}\r\nContent-Length: 2\r\n\r\n{{}}");
            stream.write_all(resp.as_bytes()).await.unwrap();
            req
        });

        ensure_emulator_bucket_http10(&addr.to_string(), "localhost", "test-bucket")
            .await
            .unwrap();
        let received = server_task.await.unwrap();
        assert!(received.starts_with("POST /storage/v1/b?project=local-project HTTP/1.0\r\n"));
        assert!(received.contains("Host: localhost\r\n"));
        assert!(received.contains("Connection: close\r\n"));
        assert!(received.contains("\"test-bucket\""));
    }

    // Oversized emulator responses are capped at MAX_EMULATOR_RESPONSE_BYTES without failing.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = stream.read(&mut buf).await;
        let header = b"HTTP/1.0 200 OK\r\nContent-Type: application/json\r\n\r\n";
        let _ = stream.write_all(header).await;
        let chunk = vec![b' '; 16 * 1024];
        for _ in 0..8 {
            if stream.write_all(&chunk).await.is_err() {
                break;
            }
        }
    });
    ensure_emulator_bucket_http10(&addr.to_string(), "localhost", "big-resp-bucket")
        .await
        .unwrap();
    server_task.await.unwrap();
}

#[tokio::test]
async fn verify_exact_eof_zero_size_uses_bounded_one_byte_probe() {
    use std::sync::Mutex;

    use google_cloud_storage::model::ReadObjectRequest;
    use google_cloud_storage::model_ext::ObjectHighlights;
    use google_cloud_storage::read_object::ReadObjectResponse;
    use google_cloud_storage::request_options::RequestOptions;

    #[derive(Clone, Debug)]
    enum Mode {
        Missing,
        EmptyOutOfRange,
        EmptyBody,
        Oversized,
    }

    #[derive(Debug)]
    struct ProbeStub {
        mode: Mutex<Mode>,
        ranges: Mutex<Vec<(i64, i64)>>,
    }

    impl google_cloud_storage::stub::Storage for ProbeStub {
        async fn read_object(
            &self,
            req: ReadObjectRequest,
            _options: RequestOptions,
        ) -> google_cloud_storage::Result<ReadObjectResponse> {
            self.ranges
                .lock()
                .unwrap()
                .push((req.read_offset, req.read_limit));
            let mode = self.mode.lock().unwrap().clone();
            match mode {
                Mode::Missing => Err(GaxError::service(
                    RpcStatus::default().set_code(RpcCode::NotFound),
                )),
                Mode::EmptyOutOfRange => Err(GaxError::service(
                    RpcStatus::default().set_code(RpcCode::OutOfRange),
                )),
                Mode::EmptyBody => Ok(ReadObjectResponse::from_source(
                    ObjectHighlights::default(),
                    Bytes::new(),
                )),
                Mode::Oversized => Ok(ReadObjectResponse::from_source(
                    ObjectHighlights::default(),
                    Bytes::from_static(b"x"),
                )),
            }
        }
    }

    let stub = Arc::new(ProbeStub {
        mode: Mutex::new(Mode::Missing),
        ranges: Mutex::new(Vec::new()),
    });
    let store = GcsBlobStore::from_storage(
        "gs://zero-probe-bucket",
        Storage::<ProbeStub>::from_stub(stub.clone()),
    )
    .unwrap();
    let blob_id = [0xab; 20];
    let key = GcsBlobStore::<ProbeStub>::object_key(&blob_id).unwrap();

    assert_eq!(
        store.verify_exact_eof(&blob_id, &key, 0).await.unwrap(),
        None
    );

    *stub.mode.lock().unwrap() = Mode::EmptyOutOfRange;
    assert_eq!(
        store.verify_exact_eof(&blob_id, &key, 0).await.unwrap(),
        Some(())
    );

    *stub.mode.lock().unwrap() = Mode::EmptyBody;
    assert_eq!(
        store.verify_exact_eof(&blob_id, &key, 0).await.unwrap(),
        Some(())
    );

    *stub.mode.lock().unwrap() = Mode::Oversized;
    assert!(matches!(
        store.verify_exact_eof(&blob_id, &key, 0).await,
        Err(StoreError::Corruption(_))
    ));

    let ranges = stub.ranges.lock().unwrap().clone();
    assert_eq!(ranges, vec![(0, 1), (0, 1), (0, 1), (0, 1)]);
}

mod streaming_validation_tests {
    use std::collections::{HashMap, VecDeque};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use bytes::{Bytes, BytesMut};
    use google_cloud_gax::error::Error as GaxError;
    use google_cloud_gax::error::rpc::{Code as RpcCode, Status as RpcStatus};
    use google_cloud_storage::client::Storage;
    use google_cloud_storage::model::ReadObjectRequest;
    use google_cloud_storage::model_ext::ObjectHighlights;
    use google_cloud_storage::read_object::ReadObjectResponse;
    use google_cloud_storage::request_options::RequestOptions;
    use google_cloud_storage::streaming_source::{SizeHint, StreamingSource};

    use super::*;
    use crate::store::replicatedblob::SMALL_BLOB_MAX_BYTES;
    use crate::store::{
        BlobRecord, BlobStore, CompositeStore, MemoryMetadataStore, MetadataStore,
        ReplicatedBlobStore, ReplicatedBlobStoreConfig,
    };

    #[derive(Clone, Debug)]
    enum StubObject {
        Bytes {
            data: Bytes,
            /// `None` reports `data.len() as i64`, mirroring real Cloud Storage.
            /// `Some(size)` overrides `ObjectHighlights.size` (e.g. `0` when omitted or `-1`).
            reported_size: Option<i64>,
        },
        Chunks {
            reported_size: i64,
            chunks: Vec<Result<Bytes, String>>,
        },
    }

    struct ChunkedSource {
        chunks: VecDeque<Result<Bytes, String>>,
    }

    impl StreamingSource for ChunkedSource {
        type Error = std::io::Error;

        async fn next(&mut self) -> Option<Result<Bytes, Self::Error>> {
            self.chunks
                .pop_front()
                .map(|item| item.map_err(std::io::Error::other))
        }

        async fn size_hint(&self) -> Result<SizeHint, Self::Error> {
            Ok(SizeHint::default())
        }
    }

    #[derive(Debug, Default)]
    struct TestGcsStub {
        objects: Mutex<HashMap<(String, String), StubObject>>,
        write_calls: AtomicUsize,
    }

    impl TestGcsStub {
        fn put_raw(
            &self,
            bucket_id: &str,
            blob_id: &[u8],
            data: Bytes,
            reported_size: Option<i64>,
        ) {
            let key = GcsBlobStore::<TestGcsStub>::object_key(blob_id).unwrap();
            let bucket_resource = format!("projects/_/buckets/{bucket_id}");
            self.objects.lock().unwrap().insert(
                (bucket_resource, key),
                StubObject::Bytes {
                    data,
                    reported_size,
                },
            );
        }

        fn put_chunks(
            &self,
            bucket_id: &str,
            blob_id: &[u8],
            reported_size: i64,
            chunks: Vec<Result<Bytes, String>>,
        ) {
            let key = GcsBlobStore::<TestGcsStub>::object_key(blob_id).unwrap();
            let bucket_resource = format!("projects/_/buckets/{bucket_id}");
            self.objects.lock().unwrap().insert(
                (bucket_resource, key),
                StubObject::Chunks {
                    reported_size,
                    chunks,
                },
            );
        }
    }

    impl google_cloud_storage::stub::Storage for TestGcsStub {
        async fn read_object(
            &self,
            req: ReadObjectRequest,
            _options: RequestOptions,
        ) -> google_cloud_storage::Result<ReadObjectResponse> {
            let entry = self
                .objects
                .lock()
                .unwrap()
                .get(&(req.bucket.clone(), req.object.clone()))
                .cloned();
            let Some(entry) = entry else {
                return Err(GaxError::service(
                    RpcStatus::default().set_code(RpcCode::NotFound),
                ));
            };
            match entry {
                StubObject::Bytes {
                    data,
                    reported_size,
                } => {
                    let total = data.len() as i64;
                    let start = req.read_offset.max(0) as usize;
                    if (total == 0 && (start > 0 || req.read_limit > 0))
                        || (total > 0 && start >= data.len())
                    {
                        return Err(GaxError::service(
                            RpcStatus::default().set_code(RpcCode::OutOfRange),
                        ));
                    }
                    let end = if req.read_limit > 0 {
                        start
                            .saturating_add(req.read_limit as usize)
                            .min(data.len())
                    } else {
                        data.len()
                    };
                    let mut highlights = ObjectHighlights::default();
                    highlights.size = reported_size.unwrap_or(total);
                    Ok(ReadObjectResponse::from_source(
                        highlights,
                        data.slice(start..end),
                    ))
                }
                StubObject::Chunks {
                    reported_size,
                    chunks,
                } => {
                    let mut highlights = ObjectHighlights::default();
                    highlights.size = reported_size;
                    let source = ChunkedSource {
                        chunks: VecDeque::from(chunks),
                    };
                    Ok(ReadObjectResponse::from_source(highlights, source))
                }
            }
        }

        async fn write_object_buffered<P>(
            &self,
            _payload: P,
            _req: google_cloud_storage::model_ext::WriteObjectRequest,
            _options: RequestOptions,
        ) -> google_cloud_storage::Result<google_cloud_storage::model::Object>
        where
            P: StreamingSource + Send + Sync + 'static,
        {
            self.write_calls.fetch_add(1, Ordering::SeqCst);
            Ok(google_cloud_storage::model::Object::default())
        }
    }

    async fn drain_rx(
        mut rx: tokio::sync::mpsc::Receiver<StoreResult<Bytes>>,
    ) -> StoreResult<Bytes> {
        let mut out = BytesMut::new();
        while let Some(item) = rx.recv().await {
            out.extend_from_slice(&item?);
        }
        Ok(out.freeze())
    }

    #[tokio::test]
    async fn interior_range_reads_reject_mismatched_or_negative_reported_size() {
        let stub = Arc::new(TestGcsStub::default());
        let store = Arc::new(
            GcsBlobStore::from_storage(
                "gs://interior-range-bucket",
                Storage::<TestGcsStub>::from_stub(stub.clone()),
            )
            .unwrap(),
        );
        let blob_id = vec![0x31; 20];
        let record = BlobRecord {
            blob_id: blob_id.clone(),
            content_size: 4096,
            inline_content: None,
            primary_location: Some("gs://interior-range-bucket".to_string()),
            replicated_locations: vec!["gs://interior-range-bucket".to_string()],
        };

        // Truncated object (2048 bytes) still covers interior range [64, 192), yet reported size
        // disagrees with `record.content_size` (4096).
        stub.put_raw(
            "interior-range-bucket",
            &blob_id,
            Bytes::from(vec![0xaa; 2048]),
            None,
        );
        assert!(matches!(
            store
                .get_blob_range_with_record(&record, 64, Some(128))
                .await,
            Err(StoreError::Corruption(_))
        ));
        assert!(matches!(
            store
                .clone()
                .get_blob_range_stream(record.clone(), 64, Some(128))
                .await,
            Err(StoreError::Corruption(_))
        ));

        // Oversized object (5000 bytes) on an interior range.
        stub.put_raw(
            "interior-range-bucket",
            &blob_id,
            Bytes::from(vec![0xbb; 5000]),
            None,
        );
        assert!(matches!(
            store
                .get_blob_range_with_record(&record, 64, Some(128))
                .await,
            Err(StoreError::Corruption(_))
        ));
        assert!(matches!(
            store
                .clone()
                .get_blob_range_stream(record.clone(), 64, Some(128))
                .await,
            Err(StoreError::Corruption(_))
        ));

        // Negative reported size fails closed synchronously.
        stub.put_raw(
            "interior-range-bucket",
            &blob_id,
            Bytes::from(vec![0xcc; 4096]),
            Some(-1),
        );
        assert!(matches!(
            store
                .get_blob_range_with_record(&record, 64, Some(128))
                .await,
            Err(StoreError::Corruption(_))
        ));
        assert!(matches!(
            store
                .clone()
                .get_blob_range_stream(record.clone(), 64, Some(128))
                .await,
            Err(StoreError::Corruption(_))
        ));

        // Omitted `x-goog-stored-content-length` (`reported_size == 0`) remains supported for
        // healthy objects on both ranged and streaming reads.
        stub.put_raw(
            "interior-range-bucket",
            &blob_id,
            Bytes::from(vec![0xdd; 4096]),
            Some(0),
        );
        assert_eq!(
            store
                .get_blob_range_with_record(&record, 64, Some(128))
                .await
                .unwrap(),
            Some(Bytes::from(vec![0xdd; 128]))
        );
        let rx = store
            .clone()
            .get_blob_range_stream(record, 64, Some(128))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(drain_rx(rx).await.unwrap(), Bytes::from(vec![0xdd; 128]));
    }

    #[tokio::test]
    async fn stream_validates_first_chunk_synchronously_and_drains_remaining_chunks() {
        let stub = Arc::new(TestGcsStub::default());
        let store = Arc::new(
            GcsBlobStore::from_storage(
                "gs://chunk-stream-bucket",
                Storage::<TestGcsStub>::from_stub(stub.clone()),
            )
            .unwrap(),
        );
        let blob_id = vec![0x32; 20];
        let record = BlobRecord {
            blob_id: blob_id.clone(),
            content_size: 10,
            inline_content: None,
            primary_location: Some("gs://chunk-stream-bucket".to_string()),
            replicated_locations: vec!["gs://chunk-stream-bucket".to_string()],
        };

        // 0-byte truncated body with `reported_size == 0` fails synchronously before spawning.
        stub.put_chunks("chunk-stream-bucket", &blob_id, 0, vec![]);
        assert!(matches!(
            store
                .clone()
                .get_blob_range_stream(record.clone(), 0, None)
                .await,
            Err(StoreError::Corruption(_))
        ));

        // Oversized first chunk with `reported_size == 0` fails synchronously before spawning.
        stub.put_chunks(
            "chunk-stream-bucket",
            &blob_id,
            0,
            vec![Ok(Bytes::from(vec![0x11; 11]))],
        );
        assert!(matches!(
            store
                .clone()
                .get_blob_range_stream(record.clone(), 0, None)
                .await,
            Err(StoreError::Corruption(_))
        ));

        // Immediate I/O error on first chunk maps to `Unavailable` synchronously.
        stub.put_chunks(
            "chunk-stream-bucket",
            &blob_id,
            0,
            vec![Err("connection reset".to_string())],
        );
        assert!(matches!(
            store
                .clone()
                .get_blob_range_stream(record.clone(), 0, None)
                .await,
            Err(StoreError::Unavailable(_))
        ));

        // Leading empty chunks are skipped; remaining chunks stream and validate total.
        stub.put_chunks(
            "chunk-stream-bucket",
            &blob_id,
            0,
            vec![
                Ok(Bytes::new()),
                Ok(Bytes::from_static(b"hello")),
                Ok(Bytes::new()),
                Ok(Bytes::from_static(b"world")),
            ],
        );
        let rx = store
            .clone()
            .get_blob_range_stream(record.clone(), 0, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            drain_rx(rx).await.unwrap(),
            Bytes::from_static(b"helloworld")
        );

        // Mid-stream trailing bytes after `expected_len` are still caught when draining to None.
        stub.put_chunks(
            "chunk-stream-bucket",
            &blob_id,
            0,
            vec![
                Ok(Bytes::from_static(b"helloworld")),
                Ok(Bytes::from_static(b"!")),
            ],
        );
        let rx = store
            .clone()
            .get_blob_range_stream(record.clone(), 0, None)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(drain_rx(rx).await, Err(StoreError::Corruption(_))));

        // Mid-stream truncation with `reported_size == 0` surfaces as Corruption.
        stub.put_chunks(
            "chunk-stream-bucket",
            &blob_id,
            0,
            vec![Ok(Bytes::from_static(b"short"))],
        );
        let rx = store
            .clone()
            .get_blob_range_stream(record, 0, None)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(drain_rx(rx).await, Err(StoreError::Corruption(_))));
    }

    #[tokio::test]
    async fn replicated_large_gcs_stream_and_interior_range_fail_over_on_local_corruption() {
        let local_stub = Arc::new(TestGcsStub::default());
        let peer_stub = Arc::new(TestGcsStub::default());
        let local_gcs: Arc<dyn BlobStore> = Arc::new(
            GcsBlobStore::from_storage(
                "gs://repl-corrupt-local",
                Storage::<TestGcsStub>::from_stub(local_stub.clone()),
            )
            .unwrap(),
        );
        let peer_gcs: Arc<dyn BlobStore> = Arc::new(
            GcsBlobStore::from_storage(
                "gs://repl-corrupt-peer",
                Storage::<TestGcsStub>::from_stub(peer_stub.clone()),
            )
            .unwrap(),
        );
        let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
        metadata.register_repo("failover-repo", None).await.unwrap();

        let repl = Arc::new(ReplicatedBlobStore::new(
            local_gcs,
            vec![peer_gcs],
            metadata.clone(),
            ReplicatedBlobStoreConfig::default(),
        ));
        let composite = CompositeStore::new(metadata.clone(), repl.clone());

        let blob_id = vec![0x44; 20];
        let large_len = (SMALL_BLOB_MAX_BYTES as usize) + 64 * 1024;
        let healthy_bytes = Bytes::from(
            (0..large_len)
                .map(|i| ((i * 31) & 0xff) as u8)
                .collect::<Vec<_>>(),
        );
        peer_stub.put_raw("repl-corrupt-peer", &blob_id, healthy_bytes.clone(), None);

        metadata
            .put_blob_record(
                "failover-repo",
                &BlobRecord {
                    blob_id: blob_id.clone(),
                    content_size: large_len as u64,
                    inline_content: None,
                    primary_location: Some("gs://repl-corrupt-peer".to_string()),
                    replicated_locations: vec![
                        "gs://repl-corrupt-local".to_string(),
                        "gs://repl-corrupt-peer".to_string(),
                    ],
                },
            )
            .await
            .unwrap();

        let mut expected_fallbacks = 0u64;

        // Truncated non-empty, 0-byte, and oversized local objects all fail over to the
        // healthy peer on full streaming reads.
        for (label, corrupt_bytes, reported_override) in [
            (
                "truncated non-empty",
                Bytes::from(vec![0x99; 256 * 1024]),
                None,
            ),
            ("zero-byte truncated", Bytes::new(), Some(0)),
            (
                "oversized reported",
                Bytes::from(vec![0x99; large_len + 1]),
                None,
            ),
            (
                "oversized omitted header",
                Bytes::from(vec![0x99; large_len + 1]),
                Some(0),
            ),
            ("negative reported size", healthy_bytes.clone(), Some(-5)),
        ] {
            local_stub.put_raw(
                "repl-corrupt-local",
                &blob_id,
                corrupt_bytes,
                reported_override,
            );
            let rx = composite
                .get_file_range_stream("failover-repo", &blob_id, 0, None)
                .await
                .unwrap_or_else(|e| panic!("{label}: expected peer failover, got error: {e}"));
            let got = drain_rx(rx).await.unwrap();
            assert_eq!(got, healthy_bytes, "{label}: streamed bytes mismatch");
            expected_fallbacks += 1;
            assert_eq!(
                repl.stats().local_hits.load(Ordering::Relaxed),
                0,
                "{label}: corrupt local must not count as local_hit"
            );
            assert_eq!(
                repl.stats().origin_fallbacks.load(Ordering::Relaxed),
                expected_fallbacks,
                "{label}: expected origin fallback increment"
            );
        }

        // Interior ranged stream and `get_file_range` on truncated (512 KiB) and oversized
        // local objects both detect `reported_size` mismatch and fail over to the peer.
        let interior_offset = 1024u64;
        let interior_len = 4096u64;
        let expected_interior = healthy_bytes
            .slice(interior_offset as usize..(interior_offset + interior_len) as usize);
        for (label, corrupt_bytes) in [
            ("interior truncated", Bytes::from(vec![0x88; 512 * 1024])),
            ("interior oversized", Bytes::from(vec![0x88; large_len + 1])),
        ] {
            local_stub.put_raw("repl-corrupt-local", &blob_id, corrupt_bytes, None);

            let rx = composite
                .get_file_range_stream(
                    "failover-repo",
                    &blob_id,
                    interior_offset,
                    Some(interior_len),
                )
                .await
                .unwrap_or_else(|e| panic!("{label} stream: expected peer failover, got {e}"));
            assert_eq!(drain_rx(rx).await.unwrap(), expected_interior);
            expected_fallbacks += 1;

            let got_range = composite
                .get_file_range(
                    "failover-repo",
                    &blob_id,
                    interior_offset,
                    Some(interior_len),
                )
                .await
                .unwrap_or_else(|e| panic!("{label} range: expected peer failover, got {e}"));
            assert_eq!(got_range, expected_interior);
            expected_fallbacks += 1;

            assert_eq!(repl.stats().local_hits.load(Ordering::Relaxed), 0);
            assert_eq!(
                repl.stats().origin_fallbacks.load(Ordering::Relaxed),
                expected_fallbacks
            );
        }

        // First-chunk transient stream I/O error fails over synchronously to healthy peer.
        local_stub.put_chunks(
            "repl-corrupt-local",
            &blob_id,
            large_len as i64,
            vec![Err("transient socket reset".to_string())],
        );
        let rx = composite
            .get_file_range_stream("failover-repo", &blob_id, 0, None)
            .await
            .unwrap();
        assert_eq!(drain_rx(rx).await.unwrap(), healthy_bytes);
        expected_fallbacks += 1;
        assert_eq!(repl.stats().local_hits.load(Ordering::Relaxed), 0);
        assert_eq!(
            repl.stats().origin_fallbacks.load(Ordering::Relaxed),
            expected_fallbacks
        );

        // Corrupt local reads must never trigger background read-repair overwrites.
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(local_stub.write_calls.load(Ordering::SeqCst), 0);
    }
}
