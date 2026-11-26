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

use std::collections::HashMap;
use std::io::Write as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use google_cloud_gax::error::Error as GaxError;
use google_cloud_gax::error::rpc::{Code as RpcCode, Status as RpcStatus};
use google_cloud_storage::client::Storage;
use google_cloud_storage::model::{Object, ReadObjectRequest};
use google_cloud_storage::model_ext::{ObjectHighlights, WriteObjectRequest};
use google_cloud_storage::read_object::ReadObjectResponse;
use google_cloud_storage::request_options::RequestOptions;
use google_cloud_storage::streaming_source::{Seek, StreamingSource};
use jj_commit_cloud_server::store::{
    BlobRecord, BlobStore, CompositeStore, GcsBlobStore, IMMUTABLE_CACHE_CONTROL,
    MemoryMetadataStore, MetadataStore, ReplicatedBlobStore, ReplicatedBlobStoreConfig,
    SpannerConnectConfig, SpannerMetadataStore, StoreError, StoreResult,
};

fn unique_db_id(prefix: &str) -> String {
    let max_prefix = 30 - 9; // '_' + 8 random hex chars within Spanner's 30-char DB ID limit
    let trimmed = &prefix[..prefix.len().min(max_prefix)];
    let db_id = format!("{trimmed}_{:08x}", rand::random::<u32>());
    debug_assert!(db_id.len() <= 30);
    db_id
}

#[derive(Clone, Debug)]
struct StoredObject {
    data: Bytes,
    visible_at: Instant,
    cache_control: String,
    generation: i64,
}

#[derive(Debug)]
struct TestLatch {
    tx: tokio::sync::watch::Sender<bool>,
}

impl TestLatch {
    fn new() -> Arc<Self> {
        let (tx, _rx) = tokio::sync::watch::channel(false);
        Arc::new(Self { tx })
    }

    fn trigger(&self) {
        self.tx.send_replace(true);
    }

    async fn wait(&self) {
        let mut rx = self.tx.subscribe();
        tokio::time::timeout(Duration::from_secs(5), rx.wait_for(|v| *v))
            .await
            .expect("timed out waiting for TestLatch")
            .expect("TestLatch sender unexpectedly dropped");
    }
}

/// Deterministic in-memory `google_cloud_storage::stub::Storage` implementation enforcing
/// `if_generation_match` preconditions, byte-range slicing, and configurable visibility lag.
#[derive(Debug, Default)]
struct FakeGcsStub {
    objects: Mutex<HashMap<(String, String), StoredObject>>,
    visibility_delay: Duration,
    read_delay: Duration,
    write_delay: Duration,
    read_gate: Mutex<Option<(Arc<TestLatch>, Arc<TestLatch>)>>,
    write_gate: Mutex<Option<(Arc<TestLatch>, Arc<TestLatch>)>>,
    injected_read_error: Mutex<Option<RpcCode>>,
    read_calls: AtomicUsize,
    recorded_read_ranges: Mutex<Vec<(i64, i64)>>,
    bytes_read: AtomicUsize,
    write_calls: AtomicUsize,
    buffered_write_calls: AtomicUsize,
    unbuffered_write_calls: AtomicUsize,
    precondition_412_hits: AtomicUsize,
}

impl FakeGcsStub {
    fn with_delays(visibility_delay: Duration, read_delay: Duration) -> Self {
        Self {
            visibility_delay,
            read_delay,
            ..Default::default()
        }
    }

    fn overwrite_raw_blob(&self, bucket_id: &str, blob_id: &[u8], data: Bytes) {
        let key = GcsBlobStore::<FakeGcsStub>::object_key(blob_id).unwrap();
        let bucket_resource = format!("projects/_/buckets/{bucket_id}");
        let mut guard = self.objects.lock().unwrap();
        let next_gen = guard
            .get(&(bucket_resource.clone(), key.clone()))
            .map_or(1, |prev| prev.generation + 1);
        guard.insert(
            (bucket_resource, key),
            StoredObject {
                data,
                visible_at: Instant::now(),
                cache_control: IMMUTABLE_CACHE_CONTROL.to_string(),
                generation: next_gen,
            },
        );
    }

    async fn write_object_inner<P>(
        &self,
        mut payload: P,
        req: WriteObjectRequest,
    ) -> google_cloud_storage::Result<Object>
    where
        P: StreamingSource + Send + Sync + 'static,
    {
        self.write_calls.fetch_add(1, Ordering::SeqCst);
        let write_gate = self.write_gate.lock().unwrap().clone();
        if let Some((entered, release)) = write_gate {
            entered.trigger();
            release.wait().await;
        }
        if !self.write_delay.is_zero() {
            tokio::time::sleep(self.write_delay).await;
        }
        let mut buf = BytesMut::new();
        while let Some(chunk) = payload.next().await {
            let chunk = chunk.map_err(|e| {
                GaxError::service(
                    RpcStatus::default()
                        .set_code(RpcCode::Internal)
                        .set_message(format!("payload stream error: {e}")),
                )
            })?;
            buf.extend_from_slice(&chunk);
        }
        let data = buf.freeze();

        let resource = req.spec.resource.unwrap_or_default();
        let bucket = resource.bucket.clone();
        let name = resource.name.clone();
        let cache_control = resource.cache_control.clone();

        let mut guard = self.objects.lock().unwrap();
        if req.spec.if_generation_match == Some(0)
            && guard.contains_key(&(bucket.clone(), name.clone()))
        {
            self.precondition_412_hits.fetch_add(1, Ordering::SeqCst);
            return Err(GaxError::service(
                RpcStatus::default()
                    .set_code(RpcCode::FailedPrecondition)
                    .set_message("412 Precondition Failed: if_generation_match=0"),
            ));
        }

        let next_gen = guard
            .get(&(bucket.clone(), name.clone()))
            .map_or(1, |prev| prev.generation + 1);
        guard.insert(
            (bucket, name),
            StoredObject {
                data,
                visible_at: Instant::now() + self.visibility_delay,
                cache_control,
                generation: next_gen,
            },
        );
        Ok(resource)
    }
}

impl google_cloud_storage::stub::Storage for FakeGcsStub {
    async fn read_object(
        &self,
        req: ReadObjectRequest,
        _options: RequestOptions,
    ) -> google_cloud_storage::Result<ReadObjectResponse> {
        self.read_calls.fetch_add(1, Ordering::SeqCst);
        self.recorded_read_ranges
            .lock()
            .unwrap()
            .push((req.read_offset, req.read_limit));
        let read_gate = self.read_gate.lock().unwrap().clone();
        if let Some((entered, release)) = read_gate {
            entered.trigger();
            release.wait().await;
        }
        if !self.read_delay.is_zero() {
            tokio::time::sleep(self.read_delay).await;
        }
        if let Some(code) = *self.injected_read_error.lock().unwrap() {
            return Err(GaxError::service(
                RpcStatus::default()
                    .set_code(code)
                    .set_message("injected read failure"),
            ));
        }

        // Mirror google-cloud-storage's HTTP `bytes=offset-(offset+limit-1)` i64 arithmetic.
        if req.read_offset >= 0 && req.read_limit > 0 {
            assert!(
                req.read_offset.checked_add(req.read_limit - 1).is_some(),
                "unclamped ReadRange overflowed i64: offset={}, limit={}",
                req.read_offset,
                req.read_limit
            );
        }

        let entry = {
            let guard = self.objects.lock().unwrap();
            guard
                .get(&(req.bucket.clone(), req.object.clone()))
                .cloned()
        };

        let Some(stored) = entry else {
            return Err(GaxError::service(
                RpcStatus::default()
                    .set_code(RpcCode::NotFound)
                    .set_message("object not found"),
            ));
        };

        if Instant::now() < stored.visible_at {
            return Err(GaxError::service(
                RpcStatus::default()
                    .set_code(RpcCode::NotFound)
                    .set_message("object replication still in flight"),
            ));
        }

        let total = stored.data.len() as i64;
        let start = if req.read_offset < 0 {
            (total + req.read_offset).max(0) as usize
        } else {
            req.read_offset as usize
        };
        if (total == 0 && (start > 0 || req.read_limit > 0))
            || (total > 0 && start >= stored.data.len())
        {
            return Err(GaxError::service(
                RpcStatus::default()
                    .set_code(RpcCode::OutOfRange)
                    .set_message("read offset out of range"),
            ));
        }

        let end = if req.read_limit > 0 {
            start
                .saturating_add(req.read_limit as usize)
                .min(stored.data.len())
        } else {
            stored.data.len()
        };

        let slice = stored.data.slice(start..end);
        self.bytes_read.fetch_add(slice.len(), Ordering::SeqCst);
        let mut highlights = ObjectHighlights::default();
        highlights.generation = stored.generation;
        highlights.size = total;
        Ok(ReadObjectResponse::from_source(highlights, slice))
    }

    async fn write_object_buffered<P>(
        &self,
        payload: P,
        req: WriteObjectRequest,
        _options: RequestOptions,
    ) -> google_cloud_storage::Result<Object>
    where
        P: StreamingSource + Send + Sync + 'static,
    {
        self.buffered_write_calls.fetch_add(1, Ordering::SeqCst);
        self.write_object_inner(payload, req).await
    }

    async fn write_object_unbuffered<P>(
        &self,
        payload: P,
        req: WriteObjectRequest,
        _options: RequestOptions,
    ) -> google_cloud_storage::Result<Object>
    where
        P: StreamingSource + Seek + Send + Sync + 'static,
    {
        self.unbuffered_write_calls.fetch_add(1, Ordering::SeqCst);
        self.write_object_inner(payload, req).await
    }
}

#[tokio::test]
async fn test_gcs_blob_store_conditional_writes_412_idempotency_and_range_reads() {
    let stub = Arc::new(FakeGcsStub::default());
    let gcs_client = Storage::<FakeGcsStub>::from_stub(stub.clone());
    let store = GcsBlobStore::from_storage("gs://jj-blobs-uc1", gcs_client).unwrap();
    assert_eq!(store.location_id(), "gs://jj-blobs-uc1");

    let blob_id = hex::decode("ab12cd3400000000000000000000000000000099").unwrap();
    let payload = Bytes::from_static(b"immutable content-addressed blob payload");

    // First upload writes object with if_generation_match=0 and immutable Cache-Control.
    store.put_blob(&blob_id, payload.clone()).await.unwrap();
    assert_eq!(stub.write_calls.load(Ordering::SeqCst), 1);
    assert_eq!(stub.precondition_412_hits.load(Ordering::SeqCst), 0);

    {
        let guard = stub.objects.lock().unwrap();
        let stored = guard
            .get(&(
                "projects/_/buckets/jj-blobs-uc1".to_string(),
                "blobs/ab/12/ab12cd3400000000000000000000000000000099".to_string(),
            ))
            .expect("sharded object key should exist in bucket");
        assert_eq!(stored.cache_control, IMMUTABLE_CACHE_CONTROL);
    }

    // Duplicate upload hits HTTP 412 / FailedPrecondition and succeeds idempotently.
    store.put_blob(&blob_id, payload.clone()).await.unwrap();
    assert_eq!(stub.precondition_412_hits.load(Ordering::SeqCst), 1);

    // Full read and ranged read.
    assert_eq!(store.get_blob(&blob_id).await.unwrap(), Some(payload));
    assert_eq!(
        store
            .get_blob_range(&blob_id, 10, Some(17))
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"content-addressed"
    );

    // Missing object returns Ok(None).
    assert_eq!(store.get_blob(&[0xff; 20]).await.unwrap(), None);
}

#[tokio::test]
async fn test_replicated_blob_store_cross_region_origin_fallback_and_read_repair() {
    // Use real Cloud Spanner Emulator when available, falling back to MemoryMetadataStore.
    let (_emulator_guard, metadata): (
        Option<testutils::SpannerEmulatorGuard>,
        Arc<dyn MetadataStore>,
    ) = if let Some(emu) = testutils::SpannerEmulatorGuard::start_or_skip(
        "test_replicated_blob_store_cross_region_origin_fallback_and_read_repair",
        false,
    )
    .await
    {
        let cfg = SpannerConnectConfig {
            database: emu.database_uri(&unique_db_id("gcs_repl_test")),
            endpoint: Some(emu.grpc_endpoint()),
            admin_endpoint: Some(emu.rest_endpoint().to_string()),
            auto_migrate: true,
            emulator: true,
            ..Default::default()
        };
        let span = Arc::new(SpannerMetadataStore::connect(&cfg).await.unwrap());
        (Some(emu), span)
    } else {
        (None, Arc::new(MemoryMetadataStore::new()))
    };

    let stub_uc1 = Arc::new(FakeGcsStub::default());
    let stub_ew1 = Arc::new(FakeGcsStub::default());

    let gcs_uc1: Arc<dyn BlobStore> = Arc::new(
        GcsBlobStore::from_storage(
            "gs://jj-blobs-uc1",
            Storage::<FakeGcsStub>::from_stub(stub_uc1.clone()),
        )
        .unwrap(),
    );
    let gcs_ew1: Arc<dyn BlobStore> = Arc::new(
        GcsBlobStore::from_storage(
            "gs://jj-blobs-ew1",
            Storage::<FakeGcsStub>::from_stub(stub_ew1.clone()),
        )
        .unwrap(),
    );

    // Region US-Central1 server (writes locally without fanout so EW1 starts lagging).
    let store_us = CompositeStore::new(
        metadata.clone(),
        Arc::new(ReplicatedBlobStore::new(
            gcs_uc1.clone(),
            vec![gcs_ew1.clone()],
            metadata.clone(),
            ReplicatedBlobStoreConfig::default(),
        )),
    );

    // Region Europe-West1 server (knows UC1 as peer bucket for origin fallback).
    let repl_eu = Arc::new(ReplicatedBlobStore::new(
        gcs_ew1.clone(),
        vec![gcs_uc1.clone()],
        metadata.clone(),
        ReplicatedBlobStoreConfig::default(),
    ));
    let store_eu = CompositeStore::new(metadata.clone(), repl_eu.clone());

    store_us.register_repo("shared-repo", None).await.unwrap();
    let blob_id = vec![0x3a; 20];
    let body = Bytes::from_static(b"written in us-central1 before eu replication");
    store_us
        .put_file("shared-repo", &blob_id, body.clone())
        .await
        .unwrap();

    // Confirm EW1 bucket does NOT yet have the object (simulating cross-region GCS lag).
    assert_eq!(gcs_ew1.get_blob(&blob_id).await.unwrap(), None);

    // Reader in Europe-West1 reads immediately: Spanner metadata points to gs://jj-blobs-uc1,
    // EW1 misses locally, falls back to UC1, returns exact bytes, and schedules read-repair.
    let hydrated = store_eu
        .get_file_range("shared-repo", &blob_id, 0, None)
        .await
        .unwrap();
    assert_eq!(hydrated, body);
    assert_eq!(repl_eu.stats().origin_fallbacks.load(Ordering::Relaxed), 1);

    // Wait briefly for background read-repair into gs://jj-blobs-ew1 and Spanner location update.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if repl_eu
            .stats()
            .read_repairs_completed
            .load(Ordering::Relaxed)
            >= 1
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for background read-repair completion"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(gcs_ew1.get_blob(&blob_id).await.unwrap(), Some(body));
    let updated_record = metadata
        .get_global_blob_record(&blob_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        updated_record
            .replicated_locations
            .contains(&"gs://jj-blobs-ew1".to_string())
    );
}

#[tokio::test]
async fn test_replicated_blob_store_single_endpoint_visibility_lag_retry_and_missing_fast_fail() {
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    // Dual-region single-endpoint bucket with 120ms replication visibility lag.
    let lagging_stub = Arc::new(FakeGcsStub::with_delays(
        Duration::from_millis(120),
        Duration::ZERO,
    ));
    let gcs_dual: Arc<dyn BlobStore> = Arc::new(
        GcsBlobStore::from_storage(
            "gs://jj-blobs-nam4",
            Storage::<FakeGcsStub>::from_stub(lagging_stub.clone()),
        )
        .unwrap(),
    );

    // Writer uploads to nam4 and records metadata; Reader has a separate cold cache instance.
    let writer = CompositeStore::new(metadata.clone(), gcs_dual.clone());
    let reader_repl = Arc::new(ReplicatedBlobStore::new(
        gcs_dual,
        vec![],
        metadata.clone(),
        ReplicatedBlobStoreConfig {
            cache_capacity_bytes: 1024 * 1024,
            replication_retry_budget: Duration::from_millis(1_500),
            initial_retry_backoff: Duration::from_millis(20),
            async_write_fanout: false,
        },
    ));
    let reader = CompositeStore::new(metadata.clone(), reader_repl.clone());

    writer.register_repo("dual-repo", None).await.unwrap();
    let blob_id = vec![0x7c; 20];
    let payload = Bytes::from_static(b"eventually visible dual-region object");
    writer
        .put_file("dual-repo", &blob_id, payload.clone())
        .await
        .unwrap();

    // Immediate read hits initial 404 during 120ms lag, sees Spanner/Metadata confirmation,
    // backs off with jitter, and succeeds once object becomes visible.
    let fetched = reader
        .get_file_range("dual-repo", &blob_id, 0, None)
        .await
        .unwrap();
    assert_eq!(fetched, payload);
    assert!(
        reader_repl
            .stats()
            .visibility_lag_retries
            .load(Ordering::Relaxed)
            >= 1
    );
    assert_eq!(reader_repl.stats().local_hits.load(Ordering::Relaxed), 1);

    // Conversely, probing an unconfirmed missing blob_id via ReplicatedBlobStore::get_blob
    // fast-fails in zero retries.
    let retries_before = reader_repl
        .stats()
        .visibility_lag_retries
        .load(Ordering::Relaxed);
    let reads_before = lagging_stub.read_calls.load(Ordering::SeqCst);
    assert_eq!(reader_repl.get_blob(&[0xee; 20]).await.unwrap(), None);
    assert_eq!(
        reader_repl
            .stats()
            .visibility_lag_retries
            .load(Ordering::Relaxed),
        retries_before
    );
    assert_eq!(
        lagging_stub.read_calls.load(Ordering::SeqCst),
        reads_before + 1
    );
    assert!(matches!(
        reader
            .get_file_range("dual-repo", &[0xee; 20], 0, None)
            .await,
        Err(StoreError::ObjectNotFound { .. })
    ));
}

#[tokio::test]
async fn test_replicated_blob_store_singleflight_coalesces_concurrent_misses() {
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    let origin_stub = Arc::new(FakeGcsStub::with_delays(
        Duration::ZERO,
        Duration::from_millis(60),
    ));
    let local_stub = Arc::new(FakeGcsStub::default());

    let origin_gcs: Arc<dyn BlobStore> = Arc::new(
        GcsBlobStore::from_storage(
            "gs://jj-blobs-origin",
            Storage::<FakeGcsStub>::from_stub(origin_stub.clone()),
        )
        .unwrap(),
    );
    let local_gcs: Arc<dyn BlobStore> = Arc::new(
        GcsBlobStore::from_storage(
            "gs://jj-blobs-edge",
            Storage::<FakeGcsStub>::from_stub(local_stub),
        )
        .unwrap(),
    );

    let writer = CompositeStore::new(metadata.clone(), origin_gcs.clone());
    writer.register_repo("coalesce-repo", None).await.unwrap();
    let blob_id = vec![0x99; 20];
    let payload = Bytes::from(vec![0x5a; 128 * 1024]);
    writer
        .put_file("coalesce-repo", &blob_id, payload.clone())
        .await
        .unwrap();

    let edge_repl = Arc::new(ReplicatedBlobStore::new(
        local_gcs,
        vec![origin_gcs],
        metadata.clone(),
        ReplicatedBlobStoreConfig::default(),
    ));
    let edge_store = Arc::new(CompositeStore::new(metadata, edge_repl.clone()));

    let concurrency = 24usize;
    let barrier = Arc::new(tokio::sync::Barrier::new(concurrency));
    let mut tasks = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let s = edge_store.clone();
        let b = barrier.clone();
        let id = blob_id.clone();
        let expected = payload.clone();
        tasks.push(tokio::spawn(async move {
            b.wait().await;
            let got = s
                .get_file_range("coalesce-repo", &id, 0, None)
                .await
                .unwrap();
            assert_eq!(got, expected);
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    // All 24 concurrent callers coalesced onto a single origin GCS read.
    assert_eq!(origin_stub.read_calls.load(Ordering::SeqCst), 1);
    assert!(
        edge_repl
            .stats()
            .singleflight_coalesced
            .load(Ordering::Relaxed)
            >= 1
    );
}

#[tokio::test]
async fn test_replicated_blob_store_ranged_reads_small_only_cache_and_range_clamp() {
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    let stub_uc1 = Arc::new(FakeGcsStub::default());
    let stub_ew1 = Arc::new(FakeGcsStub::default());

    let gcs_uc1: Arc<dyn BlobStore> = Arc::new(
        GcsBlobStore::from_storage(
            "gs://jj-blobs-uc1",
            Storage::<FakeGcsStub>::from_stub(stub_uc1.clone()),
        )
        .unwrap(),
    );
    let gcs_ew1: Arc<dyn BlobStore> = Arc::new(
        GcsBlobStore::from_storage(
            "gs://jj-blobs-ew1",
            Storage::<FakeGcsStub>::from_stub(stub_ew1.clone()),
        )
        .unwrap(),
    );

    let store_us = CompositeStore::new(metadata.clone(), gcs_uc1.clone());
    // Cache holds 8 MiB, which is larger than the 2 MiB asset below; ranged reads must still
    // stream only the requested slice rather than downloading or caching the 2 MiB object.
    let repl_eu = Arc::new(ReplicatedBlobStore::new(
        gcs_ew1.clone(),
        vec![gcs_uc1],
        metadata.clone(),
        ReplicatedBlobStoreConfig {
            cache_capacity_bytes: 8 * 1024 * 1024,
            ..ReplicatedBlobStoreConfig::default()
        },
    ));
    let store_eu = CompositeStore::new(metadata, repl_eu.clone());

    store_us.register_repo("range-repo", None).await.unwrap();

    // Small blob (<= 1 MiB) populates the read-through LRU cache on first read.
    let small_id = vec![0x11; 20];
    let small_body = Bytes::from(vec![0x41; 32 * 1024]);
    store_us
        .put_file("range-repo", &small_id, small_body.clone())
        .await
        .unwrap();
    assert_eq!(
        store_eu
            .get_file_range("range-repo", &small_id, 0, Some(16))
            .await
            .unwrap()
            .len(),
        16
    );
    assert_eq!(repl_eu.stats().cache_hits.load(Ordering::Relaxed), 0);
    assert_eq!(
        store_eu
            .get_file_range("range-repo", &small_id, 8, Some(16))
            .await
            .unwrap()
            .len(),
        16
    );
    assert_eq!(repl_eu.stats().cache_hits.load(Ordering::Relaxed), 1);

    // Large blob (2 MiB > SMALL_BLOB_MAX_BYTES): peer fallback ranged read transfers ONLY
    // the 4 KiB slice, not 2 MiB, and does not spawn whole-object read-repair.
    let large_id = vec![0x22; 20];
    let large_len = 2 * 1024 * 1024;
    let large_body: Bytes = (0..large_len)
        .map(|i| ((i * 7 + 13) & 0xff) as u8)
        .collect();
    store_us
        .put_file("range-repo", &large_id, large_body.clone())
        .await
        .unwrap();

    stub_uc1.bytes_read.store(0, Ordering::SeqCst);
    let slice = store_eu
        .get_file_range("range-repo", &large_id, 500_000, Some(4_096))
        .await
        .unwrap();
    assert_eq!(slice.as_ref(), &large_body[500_000..504_096]);
    assert_eq!(stub_uc1.bytes_read.load(Ordering::SeqCst), 4_096);
    assert_eq!(gcs_ew1.get_blob(&large_id).await.unwrap(), None);

    // Unbounded `max_bytes = Some(u64::MAX)` at `offset >= 2` is clamped to `content_size - offset`
    // without overflowing `i64` Range header arithmetic (verified both through ReplicatedBlobStore
    // and directly on GcsBlobStore).
    let tail_offset = (large_len - 128) as u64;
    let tail = store_eu
        .get_file_range("range-repo", &large_id, tail_offset, Some(u64::MAX))
        .await
        .unwrap();
    assert_eq!(tail.as_ref(), &large_body[(large_len - 128)..]);
    gcs_ew1
        .put_blob(&large_id, large_body.clone())
        .await
        .unwrap();
    let direct_tail = gcs_ew1
        .get_blob_range(&large_id, tail_offset, Some(u64::MAX))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(direct_tail.as_ref(), &large_body[(large_len - 128)..]);

    // Full reads of >1 MiB blobs and `put_blob` write bursts do not evict cached small blobs.
    let tiny_cache_repl = Arc::new(ReplicatedBlobStore::new(
        gcs_ew1,
        vec![],
        Arc::new(MemoryMetadataStore::new()),
        ReplicatedBlobStoreConfig {
            cache_capacity_bytes: 48 * 1024,
            ..ReplicatedBlobStoreConfig::default()
        },
    ));
    assert_eq!(
        tiny_cache_repl
            .get_blob(&large_id)
            .await
            .unwrap()
            .unwrap()
            .len(),
        large_len
    );
    let cached_small_id = vec![0x33; 20];
    tiny_cache_repl
        .put_blob(&cached_small_id, Bytes::from(vec![0x52; 32 * 1024]))
        .await
        .unwrap();
    // First get_blob populates cache; subsequent large get_blob and put_blob writes must not evict it.
    let _ = tiny_cache_repl.get_blob(&cached_small_id).await.unwrap();
    assert_eq!(
        tiny_cache_repl.stats().cache_hits.load(Ordering::Relaxed),
        0
    );
    let _ = tiny_cache_repl.get_blob(&large_id).await.unwrap();
    for idx in 0..4u8 {
        tiny_cache_repl
            .put_blob(&[idx; 20], Bytes::from(vec![idx; 40 * 1024]))
            .await
            .unwrap();
    }
    let _ = tiny_cache_repl.get_blob(&cached_small_id).await.unwrap();
    assert_eq!(
        tiny_cache_repl.stats().cache_hits.load(Ordering::Relaxed),
        1
    );
}

#[tokio::test]
async fn test_replicated_blob_store_detached_leader_transient_fallback_and_deduped_repair() {
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    let origin_stub = Arc::new(FakeGcsStub::default());
    let local_stub = Arc::new(FakeGcsStub::default());

    let origin_gcs: Arc<dyn BlobStore> = Arc::new(
        GcsBlobStore::from_storage(
            "gs://jj-blobs-origin",
            Storage::<FakeGcsStub>::from_stub(origin_stub.clone()),
        )
        .unwrap(),
    );
    let local_gcs: Arc<dyn BlobStore> = Arc::new(
        GcsBlobStore::from_storage(
            "gs://jj-blobs-edge",
            Storage::<FakeGcsStub>::from_stub(local_stub.clone()),
        )
        .unwrap(),
    );

    let writer = CompositeStore::new(metadata.clone(), origin_gcs.clone());
    writer.register_repo("resilience-repo", None).await.unwrap();

    let edge_repl = Arc::new(ReplicatedBlobStore::new(
        local_gcs.clone(),
        vec![origin_gcs],
        metadata.clone(),
        ReplicatedBlobStoreConfig::default(),
    ));
    let edge_store = Arc::new(CompositeStore::new(metadata.clone(), edge_repl.clone()));

    // Detached singleflight leader: cancelling the first caller's future must not fail
    // coalesced followers or trigger duplicate origin reads.
    let cancel_id = vec![0xa1; 20];
    let cancel_payload = Bytes::from(vec![0x7b; 64 * 1024]);
    writer
        .put_file("resilience-repo", &cancel_id, cancel_payload.clone())
        .await
        .unwrap();
    origin_stub.read_calls.store(0, Ordering::SeqCst);

    let read_entered = TestLatch::new();
    let read_release = TestLatch::new();
    *origin_stub.read_gate.lock().unwrap() = Some((read_entered.clone(), read_release.clone()));

    let s_first = edge_store.clone();
    let id_first = cancel_id.clone();
    let first_caller = tokio::spawn(async move {
        s_first
            .get_file_range("resilience-repo", &id_first, 0, None)
            .await
    });

    // Wait deterministically until the detached leader is blocked inside the origin read.
    read_entered.wait().await;

    let mut followers = Vec::new();
    for _ in 0..6 {
        let s = edge_store.clone();
        let id = cancel_id.clone();
        followers.push(tokio::spawn(async move {
            s.get_file_range("resilience-repo", &id, 0, None).await
        }));
    }

    let coalesce_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while edge_repl
        .stats()
        .singleflight_coalesced
        .load(Ordering::Relaxed)
        < 6
    {
        assert!(
            tokio::time::Instant::now() < coalesce_deadline,
            "timed out waiting for followers to coalesce"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Abort the leader caller while followers are coalesced and the origin read is blocked.
    first_caller.abort();
    assert!(
        first_caller.await.unwrap_err().is_cancelled(),
        "first caller should be cancelled"
    );

    *origin_stub.read_gate.lock().unwrap() = None;
    read_release.trigger();

    for f in followers {
        assert_eq!(f.await.unwrap().unwrap(), cancel_payload);
    }
    assert_eq!(origin_stub.read_calls.load(Ordering::SeqCst), 1);

    // Wait for background read-repair of `cancel_id` to finish before snapshotting
    // `local_stub.write_calls`.
    let repair_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while edge_repl
        .stats()
        .read_repairs_completed
        .load(Ordering::Relaxed)
        < 1
    {
        assert!(
            tokio::time::Instant::now() < repair_deadline,
            "timed out waiting for Step 1 background read-repair to finish"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Transient local Unavailable error falls back to peer replica without amplifying writes
    // onto the degraded local bucket via read-repair.
    let transient_id = vec![0xb2; 20];
    let transient_payload = Bytes::from_static(b"served from origin while local returns 503");
    writer
        .put_file("resilience-repo", &transient_id, transient_payload.clone())
        .await
        .unwrap();
    let local_writes_before = local_stub.write_calls.load(Ordering::SeqCst);
    *local_stub.injected_read_error.lock().unwrap() = Some(RpcCode::Unavailable);

    let recovered = edge_store
        .get_file_range("resilience-repo", &transient_id, 0, None)
        .await
        .unwrap();
    assert_eq!(recovered, transient_payload);
    *local_stub.injected_read_error.lock().unwrap() = None;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        local_stub.write_calls.load(Ordering::SeqCst),
        local_writes_before,
        "transient local Unavailable fallback must not spawn read-repair"
    );
    assert_eq!(local_gcs.get_blob(&transient_id).await.unwrap(), None);

    // Non-transient local error (InvalidArgument) preserves its StoreError variant across singleflight.
    *local_stub.injected_read_error.lock().unwrap() = Some(RpcCode::InvalidArgument);
    let err = edge_repl.get_blob(&[0xc3; 20]).await.unwrap_err();
    assert!(
        matches!(err, StoreError::InvalidArgument(_)),
        "unexpected error variant: {err:?}"
    );
    *local_stub.injected_read_error.lock().unwrap() = None;

    // Deduped read-repair: repeated misses on an uncached >1 MiB blob while local repair
    // upload is deterministically held in flight only spawn one repair write.
    let big_payload = Bytes::from(vec![0x3c; 1536 * 1024]);

    let fast_origin_stub = Arc::new(FakeGcsStub::default());
    let fast_origin_gcs: Arc<dyn BlobStore> = Arc::new(
        GcsBlobStore::from_storage(
            "gs://jj-blobs-origin-fast",
            Storage::<FakeGcsStub>::from_stub(fast_origin_stub),
        )
        .unwrap(),
    );
    let fast_writer = CompositeStore::new(metadata.clone(), fast_origin_gcs.clone());
    let dedupe_id = vec![0xd5; 20];
    fast_writer
        .put_file("resilience-repo", &dedupe_id, big_payload.clone())
        .await
        .unwrap();

    let slow_local_stub = Arc::new(FakeGcsStub::default());
    let write_entered = TestLatch::new();
    let write_release = TestLatch::new();
    *slow_local_stub.write_gate.lock().unwrap() =
        Some((write_entered.clone(), write_release.clone()));
    let slow_local_gcs: Arc<dyn BlobStore> = Arc::new(
        GcsBlobStore::from_storage(
            "gs://jj-blobs-edge-slow",
            Storage::<FakeGcsStub>::from_stub(slow_local_stub.clone()),
        )
        .unwrap(),
    );
    let dedupe_repl = Arc::new(ReplicatedBlobStore::new(
        slow_local_gcs,
        vec![fast_origin_gcs],
        metadata.clone(),
        ReplicatedBlobStoreConfig::default(),
    ));
    let dedupe_store = CompositeStore::new(metadata, dedupe_repl.clone());

    assert_eq!(
        dedupe_store
            .get_file_range("resilience-repo", &dedupe_id, 0, None)
            .await
            .unwrap(),
        big_payload
    );
    // Hold the first background repair write deterministically before issuing the second read.
    write_entered.wait().await;

    assert_eq!(
        dedupe_store
            .get_file_range("resilience-repo", &dedupe_id, 0, None)
            .await
            .unwrap(),
        big_payload
    );
    assert!(
        dedupe_repl
            .stats()
            .read_repairs_deduplicated
            .load(Ordering::Relaxed)
            >= 1
    );

    *slow_local_stub.write_gate.lock().unwrap() = None;
    write_release.trigger();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while dedupe_repl
        .stats()
        .read_repairs_completed
        .load(Ordering::Relaxed)
        < 1
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for deduped repair"
        );
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    assert_eq!(slow_local_stub.write_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_gcs_blob_store_has_blob_and_boundary_range_reads() {
    let stub = Arc::new(FakeGcsStub::default());
    let gcs_client = Storage::<FakeGcsStub>::from_stub(stub);
    let gcs: Arc<dyn BlobStore> =
        Arc::new(GcsBlobStore::from_storage("gs://jj-blobs-bounds", gcs_client).unwrap());

    let missing_id = vec![0x01; 20];
    assert!(!gcs.has_blob(&missing_id).await.unwrap());
    assert!(matches!(
        gcs.has_blob(&[]).await,
        Err(StoreError::InvalidArgument(_))
    ));
    assert!(matches!(
        gcs.has_blob(&[0xaa; 65]).await,
        Err(StoreError::InvalidArgument(_))
    ));

    // 0-byte blob: has_blob is true, offset == 0 returns empty, offset > 0 returns OutOfRange.
    let empty_id = vec![0x02; 20];
    gcs.put_blob(&empty_id, Bytes::new()).await.unwrap();
    assert!(gcs.has_blob(&empty_id).await.unwrap());
    assert_eq!(gcs.get_blob(&empty_id).await.unwrap(), Some(Bytes::new()));
    assert_eq!(
        gcs.get_blob_range(&empty_id, 0, None).await.unwrap(),
        Some(Bytes::new())
    );
    assert_eq!(
        gcs.get_blob_range(&empty_id, 0, Some(0)).await.unwrap(),
        Some(Bytes::new())
    );
    assert_eq!(
        gcs.get_blob_range(&empty_id, 0, Some(10)).await.unwrap(),
        Some(Bytes::new())
    );
    assert!(matches!(
        gcs.get_blob_range(&empty_id, 1, None).await,
        Err(StoreError::OutOfRange(_))
    ));
    assert!(matches!(
        gcs.get_blob_range(&empty_id, 1, Some(10)).await,
        Err(StoreError::OutOfRange(_))
    ));

    // Non-empty blob: has_blob is true, offset == total_len returns empty, offset > total_len returns OutOfRange.
    let data_id = vec![0x03; 20];
    let payload = Bytes::from_static(b"0123456789abcdef");
    let total_len = payload.len() as u64;
    gcs.put_blob(&data_id, payload.clone()).await.unwrap();
    assert!(gcs.has_blob(&data_id).await.unwrap());
    assert_eq!(
        gcs.get_blob_range(&data_id, total_len, None).await.unwrap(),
        Some(Bytes::new())
    );
    assert_eq!(
        gcs.get_blob_range(&data_id, total_len, Some(8))
            .await
            .unwrap(),
        Some(Bytes::new())
    );
    assert!(matches!(
        gcs.get_blob_range(&data_id, total_len + 1, None).await,
        Err(StoreError::OutOfRange(_))
    ));
    // Offsets past i64::MAX fail explicitly with OutOfRange on existing objects and Ok(None) on missing ones.
    assert_eq!(
        gcs.get_blob_range(&missing_id, u64::MAX, None)
            .await
            .unwrap(),
        None
    );
    assert!(matches!(
        gcs.get_blob_range(&data_id, u64::MAX, None).await,
        Err(StoreError::OutOfRange(_))
    ));

    // Verify offset == total_len parity through CompositeStore (both direct GcsBlobStore and ReplicatedBlobStore).
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    let direct_store = CompositeStore::new(metadata.clone(), gcs.clone());
    direct_store
        .register_repo("bounds-repo", None)
        .await
        .unwrap();
    let stored_id = vec![0x04; 20];
    let stored_body = Bytes::from_static(b"hello boundary range");
    let stored_len = stored_body.len() as u64;
    direct_store
        .put_file("bounds-repo", &stored_id, stored_body)
        .await
        .unwrap();

    assert_eq!(
        direct_store
            .get_file_range("bounds-repo", &stored_id, stored_len, None)
            .await
            .unwrap(),
        Bytes::new()
    );
    assert_eq!(
        direct_store
            .get_file_range("bounds-repo", &stored_id, stored_len, Some(16))
            .await
            .unwrap(),
        Bytes::new()
    );
    assert!(matches!(
        direct_store
            .get_file_range("bounds-repo", &stored_id, stored_len + 1, None)
            .await,
        Err(StoreError::OutOfRange(_))
    ));

    let repl_store = CompositeStore::new(
        metadata.clone(),
        Arc::new(ReplicatedBlobStore::new(
            gcs,
            vec![],
            metadata,
            ReplicatedBlobStoreConfig::default(),
        )),
    );
    assert_eq!(
        repl_store
            .get_file_range("bounds-repo", &stored_id, stored_len, None)
            .await
            .unwrap(),
        Bytes::new()
    );
    assert_eq!(
        repl_store
            .get_file_range("bounds-repo", &stored_id, stored_len, Some(16))
            .await
            .unwrap(),
        Bytes::new()
    );
    assert!(matches!(
        repl_store
            .get_file_range("bounds-repo", &stored_id, stored_len + 1, None)
            .await,
        Err(StoreError::OutOfRange(_))
    ));
}

#[tokio::test]
async fn test_replicated_blob_store_async_write_fanout_replicates_to_peers() {
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    let stub_primary = Arc::new(FakeGcsStub::default());
    let stub_peer_a = Arc::new(FakeGcsStub::default());
    let stub_peer_b = Arc::new(FakeGcsStub::default());

    let primary_gcs: Arc<dyn BlobStore> = Arc::new(
        GcsBlobStore::from_storage(
            "gs://jj-blobs-primary",
            Storage::<FakeGcsStub>::from_stub(stub_primary),
        )
        .unwrap(),
    );
    let peer_a_gcs: Arc<dyn BlobStore> = Arc::new(
        GcsBlobStore::from_storage(
            "gs://jj-blobs-peer-a",
            Storage::<FakeGcsStub>::from_stub(stub_peer_a),
        )
        .unwrap(),
    );
    let peer_b_gcs: Arc<dyn BlobStore> = Arc::new(
        GcsBlobStore::from_storage(
            "gs://jj-blobs-peer-b",
            Storage::<FakeGcsStub>::from_stub(stub_peer_b),
        )
        .unwrap(),
    );

    let repl = Arc::new(ReplicatedBlobStore::new(
        primary_gcs,
        vec![peer_a_gcs.clone(), peer_b_gcs.clone()],
        metadata.clone(),
        ReplicatedBlobStoreConfig {
            async_write_fanout: true,
            ..ReplicatedBlobStoreConfig::default()
        },
    ));
    let store = CompositeStore::new(metadata.clone(), repl);
    store.register_repo("fanout-repo", None).await.unwrap();

    let blob_id = vec![0x51; 20];
    let payload = Bytes::from_static(b"replicated eagerly in background");
    store
        .put_file("fanout-repo", &blob_id, payload.clone())
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let rec = metadata
            .get_global_blob_record(&blob_id)
            .await
            .unwrap()
            .unwrap();
        if rec
            .replicated_locations
            .contains(&"gs://jj-blobs-peer-a".to_string())
            && rec
                .replicated_locations
                .contains(&"gs://jj-blobs-peer-b".to_string())
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for async_write_fanout to record peer locations: {:?}",
            rec.replicated_locations
        );
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    assert_eq!(
        peer_a_gcs.get_blob(&blob_id).await.unwrap(),
        Some(payload.clone())
    );
    assert_eq!(peer_b_gcs.get_blob(&blob_id).await.unwrap(), Some(payload));
}

#[tokio::test]
async fn test_replicated_blob_store_peer_error_preservation_and_non_transient_skip_retry() {
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    let local_stub = Arc::new(FakeGcsStub::default());
    let peer1_stub = Arc::new(FakeGcsStub::default());
    let peer2_stub = Arc::new(FakeGcsStub::default());

    let local_gcs: Arc<dyn BlobStore> = Arc::new(
        GcsBlobStore::from_storage(
            "gs://jj-blobs-local",
            Storage::<FakeGcsStub>::from_stub(local_stub),
        )
        .unwrap(),
    );
    let peer1_gcs: Arc<dyn BlobStore> = Arc::new(
        GcsBlobStore::from_storage(
            "gs://jj-blobs-peer-1",
            Storage::<FakeGcsStub>::from_stub(peer1_stub.clone()),
        )
        .unwrap(),
    );
    let peer2_gcs: Arc<dyn BlobStore> = Arc::new(
        GcsBlobStore::from_storage(
            "gs://jj-blobs-peer-2",
            Storage::<FakeGcsStub>::from_stub(peer2_stub.clone()),
        )
        .unwrap(),
    );

    let writer1 = CompositeStore::new(metadata.clone(), peer1_gcs.clone());
    writer1.register_repo("peer-err-repo", None).await.unwrap();

    // Peer 1 fails with PermissionDenied while Peer 2 holds a healthy replica:
    // try_peer_fallback continues past Peer 1 and serves the blob from Peer 2.
    let multi_peer_id = vec![0x61; 20];
    let multi_payload = Bytes::from_static(b"served from healthy second peer");
    writer1
        .put_file("peer-err-repo", &multi_peer_id, multi_payload.clone())
        .await
        .unwrap();
    peer2_gcs
        .put_blob(&multi_peer_id, multi_payload.clone())
        .await
        .unwrap();
    metadata
        .add_blob_replica_location(&multi_peer_id, "gs://jj-blobs-peer-2")
        .await
        .unwrap();
    *peer1_stub.injected_read_error.lock().unwrap() = Some(RpcCode::PermissionDenied);

    let repl = Arc::new(ReplicatedBlobStore::new(
        local_gcs,
        vec![peer1_gcs, peer2_gcs],
        metadata.clone(),
        ReplicatedBlobStoreConfig {
            cache_capacity_bytes: 0,
            replication_retry_budget: Duration::from_millis(500),
            initial_retry_backoff: Duration::from_millis(10),
            async_write_fanout: false,
        },
    ));
    let reader = CompositeStore::new(metadata.clone(), repl.clone());

    assert_eq!(
        reader
            .get_file_range("peer-err-repo", &multi_peer_id, 0, None)
            .await
            .unwrap(),
        multi_payload
    );

    // When all configured peers fail with a non-transient error (PermissionDenied),
    // ReplicatedBlobStore preserves the original StoreError variant and skips retrying.
    let perm_only_id = vec![0x62; 20];
    writer1
        .put_file(
            "peer-err-repo",
            &perm_only_id,
            Bytes::from_static(b"denied"),
        )
        .await
        .unwrap();
    let retries_before = repl.stats().visibility_lag_retries.load(Ordering::Relaxed);
    let err = reader
        .get_file_range("peer-err-repo", &perm_only_id, 0, None)
        .await
        .unwrap_err();
    assert!(
        matches!(err, StoreError::PermissionDenied(_)),
        "expected preserved PermissionDenied, got {err:?}"
    );
    assert_eq!(
        repl.stats().visibility_lag_retries.load(Ordering::Relaxed),
        retries_before,
        "non-transient peer error should skip visibility-lag retries"
    );

    // When a peer fails transiently (Unavailable), retries DO run within the budget.
    *peer1_stub.injected_read_error.lock().unwrap() = Some(RpcCode::Unavailable);
    let retries_before_transient = repl.stats().visibility_lag_retries.load(Ordering::Relaxed);
    let transient_err = reader
        .get_file_range("peer-err-repo", &perm_only_id, 0, None)
        .await
        .unwrap_err();
    assert!(
        matches!(transient_err, StoreError::Unavailable(_)),
        "expected Unavailable, got {transient_err:?}"
    );
    assert!(
        repl.stats().visibility_lag_retries.load(Ordering::Relaxed) > retries_before_transient,
        "transient peer error should retry within replication_retry_budget"
    );
}

#[tokio::test]
async fn test_composite_store_full_read_uses_unranged_gcs_request_for_crc32c() {
    let stub = Arc::new(FakeGcsStub::default());
    let gcs_client = Storage::<FakeGcsStub>::from_stub(stub.clone());
    let gcs: Arc<dyn BlobStore> =
        Arc::new(GcsBlobStore::from_storage("gs://jj-blobs-crc", gcs_client).unwrap());
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    let store = CompositeStore::new(metadata, gcs);
    store.register_repo("crc-repo", None).await.unwrap();

    let blob_id = vec![0x44; 20];
    let payload = Bytes::from_static(b"full read should trigger HTTP 200 CRC32C check");
    let len = payload.len() as u64;
    store
        .put_file("crc-repo", &blob_id, payload.clone())
        .await
        .unwrap();

    // Full reads (None, Some(0), exact len, or len+extra) must send read_offset == 0 && read_limit == 0.
    for max_bytes in [None, Some(0), Some(len), Some(len + 64)] {
        stub.recorded_read_ranges.lock().unwrap().clear();
        let got = store
            .get_file_range("crc-repo", &blob_id, 0, max_bytes)
            .await
            .unwrap();
        assert_eq!(got, payload);
        assert_eq!(
            *stub.recorded_read_ranges.lock().unwrap(),
            vec![(0, 0)],
            "full read with max_bytes={max_bytes:?} should issue an unranged GCS read"
        );
    }

    // Partial reads still issue byte-range requests.
    stub.recorded_read_ranges.lock().unwrap().clear();
    let prefix = store
        .get_file_range("crc-repo", &blob_id, 0, Some(9))
        .await
        .unwrap();
    assert_eq!(prefix.as_ref(), b"full read");
    assert_eq!(*stub.recorded_read_ranges.lock().unwrap(), vec![(0, 9)]);

    stub.recorded_read_ranges.lock().unwrap().clear();
    let tail = store
        .get_file_range("crc-repo", &blob_id, 5, Some(4))
        .await
        .unwrap();
    assert_eq!(tail.as_ref(), b"read");
    assert_eq!(*stub.recorded_read_ranges.lock().unwrap(), vec![(5, 4)]);
}

#[tokio::test]
async fn test_replicated_blob_store_empty_non_inline_and_eof_existence_checks() {
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    let stub_local = Arc::new(FakeGcsStub::default());
    let stub_peer = Arc::new(FakeGcsStub::default());

    let local_gcs: Arc<dyn BlobStore> = Arc::new(
        GcsBlobStore::from_storage(
            "gs://jj-blobs-eof-local",
            Storage::<FakeGcsStub>::from_stub(stub_local.clone()),
        )
        .unwrap(),
    );
    let peer_gcs: Arc<dyn BlobStore> = Arc::new(
        GcsBlobStore::from_storage(
            "gs://jj-blobs-eof-peer",
            Storage::<FakeGcsStub>::from_stub(stub_peer.clone()),
        )
        .unwrap(),
    );

    let repl = Arc::new(ReplicatedBlobStore::new(
        local_gcs.clone(),
        vec![peer_gcs.clone()],
        metadata.clone(),
        ReplicatedBlobStoreConfig::default(),
    ));
    let store = CompositeStore::new(metadata.clone(), repl.clone());
    store.register_repo("eof-repo", None).await.unwrap();

    // Missing non-inline 0-byte blob must fail with Corruption (underlying Ok(None)), not
    // silently return empty bytes.
    let missing_empty_id = vec![0xe1; 20];
    metadata
        .put_blob_record(
            "eof-repo",
            &BlobRecord {
                blob_id: missing_empty_id.clone(),
                content_size: 0,
                inline_content: None,
                primary_location: Some("gs://jj-blobs-eof-peer".to_string()),
                replicated_locations: vec!["gs://jj-blobs-eof-peer".to_string()],
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        store
            .get_file_range("eof-repo", &missing_empty_id, 0, None)
            .await,
        Err(StoreError::Corruption(_))
    ));

    // Peer-only non-inline 0-byte blob falls back to peer GCS and repairs into local GCS.
    peer_gcs
        .put_blob(&missing_empty_id, Bytes::new())
        .await
        .unwrap();
    assert_eq!(
        store
            .get_file_range("eof-repo", &missing_empty_id, 0, None)
            .await
            .unwrap(),
        Bytes::new()
    );
    assert_eq!(repl.stats().origin_fallbacks.load(Ordering::Relaxed), 1);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while repl.stats().read_repairs_completed.load(Ordering::Relaxed) < 1 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for 0-byte GCS read-repair"
        );
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    assert_eq!(
        local_gcs.get_blob(&missing_empty_id).await.unwrap(),
        Some(Bytes::new())
    );

    // EOF read (`offset == content_size > 0`) verifies object existence across GCS buckets
    // and propagates GCS permission / availability failures.
    let missing_eof_id = vec![0xe2; 20];
    let payload = Bytes::from_static(b"non-empty eof verification");
    let len = payload.len() as u64;
    metadata
        .put_blob_record(
            "eof-repo",
            &BlobRecord {
                blob_id: missing_eof_id.clone(),
                content_size: len,
                inline_content: None,
                primary_location: Some("gs://jj-blobs-eof-peer".to_string()),
                replicated_locations: vec!["gs://jj-blobs-eof-peer".to_string()],
            },
        )
        .await
        .unwrap();

    assert!(matches!(
        store
            .get_file_range("eof-repo", &missing_eof_id, len, None)
            .await,
        Err(StoreError::Corruption(_))
    ));

    *stub_local.injected_read_error.lock().unwrap() = Some(RpcCode::PermissionDenied);
    assert!(matches!(
        store
            .get_file_range("eof-repo", &missing_eof_id, len, None)
            .await,
        Err(StoreError::PermissionDenied(_))
    ));
    *stub_local.injected_read_error.lock().unwrap() = Some(RpcCode::Unavailable);
    assert!(matches!(
        store
            .get_file_range("eof-repo", &missing_eof_id, len, None)
            .await,
        Err(StoreError::Unavailable(_))
    ));
    *stub_local.injected_read_error.lock().unwrap() = None;

    peer_gcs.put_blob(&missing_eof_id, payload).await.unwrap();
    assert_eq!(
        store
            .get_file_range("eof-repo", &missing_eof_id, len, None)
            .await
            .unwrap(),
        Bytes::new()
    );
    assert_eq!(
        store
            .get_file_range("eof-repo", &missing_eof_id, len, Some(16))
            .await
            .unwrap(),
        Bytes::new()
    );
}

async fn collect_stream(
    mut rx: tokio::sync::mpsc::Receiver<StoreResult<Bytes>>,
) -> StoreResult<Vec<u8>> {
    let mut out = Vec::new();
    while let Some(item) = rx.recv().await {
        let chunk = item?;
        assert!(
            chunk.len() <= cc_common::FILE_STREAM_CHUNK_SIZE,
            "stream chunk {} exceeded FILE_STREAM_CHUNK_SIZE",
            chunk.len()
        );
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

async fn collect_stream_result(
    store: &CompositeStore,
    repo: &str,
    blob_id: &[u8],
    offset: u64,
    max_bytes: Option<u64>,
) -> StoreResult<Vec<u8>> {
    let rx = store
        .get_file_range_stream(repo, blob_id, offset, max_bytes)
        .await?;
    collect_stream(rx).await
}

#[tokio::test]
async fn test_put_file_from_temp_file_streams_unbuffered_through_gcs_and_replicated_store() {
    let large_len = 5 * 1024 * 1024 + 37;
    let payload: Vec<u8> = (0..large_len)
        .map(|i| ((i * 31 + 7) & 0xff) as u8)
        .collect();
    let mut staged = tempfile::NamedTempFile::new().unwrap();
    staged.write_all(&payload).unwrap();
    staged.flush().unwrap();

    // Direct GcsBlobStore streams from disk via write_object_unbuffered without RAM buffering.
    let direct_stub = Arc::new(FakeGcsStub::default());
    let direct_gcs: Arc<dyn BlobStore> = Arc::new(
        GcsBlobStore::from_storage(
            "gs://jj-blobs-unbuf-direct",
            Storage::<FakeGcsStub>::from_stub(direct_stub.clone()),
        )
        .unwrap(),
    );
    let direct_meta: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    let direct_store = CompositeStore::new(direct_meta, direct_gcs.clone());
    direct_store
        .register_repo("unbuf-repo", None)
        .await
        .unwrap();

    let blob_id = vec![0x4a; 20];
    direct_store
        .put_file_from_temp_file("unbuf-repo", &blob_id, staged.path(), payload.len() as u64)
        .await
        .unwrap();
    assert_eq!(direct_stub.unbuffered_write_calls.load(Ordering::SeqCst), 1);
    assert_eq!(direct_stub.buffered_write_calls.load(Ordering::SeqCst), 0);

    // Duplicate temp-file upload hits 412 FailedPrecondition and succeeds idempotently.
    direct_gcs
        .put_blob_from_file(&blob_id, staged.path(), payload.len() as u64)
        .await
        .unwrap();
    assert_eq!(direct_stub.precondition_412_hits.load(Ordering::SeqCst), 1);

    // Size mismatch on staged temp file is rejected before upload.
    assert!(matches!(
        direct_gcs
            .put_blob_from_file(&[0x4b; 20], staged.path(), payload.len() as u64 + 1)
            .await,
        Err(StoreError::Backend(_))
    ));

    let fetched = collect_stream_result(&direct_store, "unbuf-repo", &blob_id, 0, None)
        .await
        .unwrap();
    assert_eq!(fetched, payload);

    // ReplicatedBlobStore forwards put_blob_from_file directly to local GcsBlobStore.
    let repl_stub = Arc::new(FakeGcsStub::default());
    let repl_gcs: Arc<dyn BlobStore> = Arc::new(
        GcsBlobStore::from_storage(
            "gs://jj-blobs-unbuf-repl",
            Storage::<FakeGcsStub>::from_stub(repl_stub.clone()),
        )
        .unwrap(),
    );
    let repl_meta: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    let repl = Arc::new(ReplicatedBlobStore::new(
        repl_gcs,
        vec![],
        repl_meta.clone(),
        ReplicatedBlobStoreConfig::default(),
    ));
    let repl_store = CompositeStore::new(repl_meta, repl);
    repl_store
        .register_repo("unbuf-repl-repo", None)
        .await
        .unwrap();

    let repl_blob_id = vec![0x4c; 20];
    repl_store
        .put_file_from_temp_file(
            "unbuf-repl-repo",
            &repl_blob_id,
            staged.path(),
            payload.len() as u64,
        )
        .await
        .unwrap();
    assert_eq!(repl_stub.unbuffered_write_calls.load(Ordering::SeqCst), 1);
    assert_eq!(repl_stub.buffered_write_calls.load(Ordering::SeqCst), 0);

    let fetched_repl =
        collect_stream_result(&repl_store, "unbuf-repl-repo", &repl_blob_id, 0, None)
            .await
            .unwrap();
    assert_eq!(fetched_repl, payload);
}

#[tokio::test]
async fn test_replicated_blob_store_async_write_fanout_for_temp_file_uploads() {
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    let stub_primary = Arc::new(FakeGcsStub::default());
    let stub_peer_a = Arc::new(FakeGcsStub::default());
    let stub_peer_b = Arc::new(FakeGcsStub::default());

    let primary_gcs: Arc<dyn BlobStore> = Arc::new(
        GcsBlobStore::from_storage(
            "gs://jj-blobs-file-fanout-primary",
            Storage::<FakeGcsStub>::from_stub(stub_primary.clone()),
        )
        .unwrap(),
    );
    let peer_a_gcs: Arc<dyn BlobStore> = Arc::new(
        GcsBlobStore::from_storage(
            "gs://jj-blobs-file-fanout-peer-a",
            Storage::<FakeGcsStub>::from_stub(stub_peer_a.clone()),
        )
        .unwrap(),
    );
    let peer_b_gcs: Arc<dyn BlobStore> = Arc::new(
        GcsBlobStore::from_storage(
            "gs://jj-blobs-file-fanout-peer-b",
            Storage::<FakeGcsStub>::from_stub(stub_peer_b.clone()),
        )
        .unwrap(),
    );

    let repl = Arc::new(ReplicatedBlobStore::new(
        primary_gcs,
        vec![peer_a_gcs.clone(), peer_b_gcs.clone()],
        metadata.clone(),
        ReplicatedBlobStoreConfig {
            async_write_fanout: true,
            ..ReplicatedBlobStoreConfig::default()
        },
    ));
    let store = CompositeStore::new(metadata.clone(), repl);
    store.register_repo("file-fanout-repo", None).await.unwrap();

    // Large (> 4 MiB) spilled upload fans out via streaming temp file + put_blob_from_file.
    let large_id = vec![0x52; 20];
    let large_payload: Vec<u8> = (0..(5 * 1024 * 1024 + 13))
        .map(|i| ((i * 19 + 3) & 0xff) as u8)
        .collect();
    {
        let mut staged = tempfile::NamedTempFile::new().unwrap();
        staged.write_all(&large_payload).unwrap();
        staged.flush().unwrap();
        store
            .put_file_from_temp_file(
                "file-fanout-repo",
                &large_id,
                staged.path(),
                large_payload.len() as u64,
            )
            .await
            .unwrap();
        // Drop caller's temp file immediately to prove background fan-out reads from local store.
    }

    // Small (<= SMALL_BLOB_MAX_BYTES) temp-file upload also fans out.
    let small_id = vec![0x53; 20];
    let small_payload: Vec<u8> = (0..(32 * 1024)).map(|i| (i & 0xff) as u8).collect();
    {
        let mut staged = tempfile::NamedTempFile::new().unwrap();
        staged.write_all(&small_payload).unwrap();
        staged.flush().unwrap();
        store
            .put_file_from_temp_file(
                "file-fanout-repo",
                &small_id,
                staged.path(),
                small_payload.len() as u64,
            )
            .await
            .unwrap();
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    for blob_id in [&large_id, &small_id] {
        loop {
            let rec = metadata
                .get_global_blob_record(blob_id)
                .await
                .unwrap()
                .unwrap();
            if rec
                .replicated_locations
                .contains(&"gs://jj-blobs-file-fanout-peer-a".to_string())
                && rec
                    .replicated_locations
                    .contains(&"gs://jj-blobs-file-fanout-peer-b".to_string())
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for temp-file fan-out metadata for {}: {:?}",
                hex::encode(blob_id),
                rec.replicated_locations
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    assert_eq!(
        stub_primary.unbuffered_write_calls.load(Ordering::SeqCst),
        2
    );
    // Staging reads each uploaded blob from local once across both peers (1 large + 1 small).
    assert_eq!(stub_primary.read_calls.load(Ordering::SeqCst), 2);
    assert_eq!(stub_peer_a.unbuffered_write_calls.load(Ordering::SeqCst), 1);
    assert_eq!(stub_peer_b.unbuffered_write_calls.load(Ordering::SeqCst), 1);
    assert_eq!(stub_peer_a.buffered_write_calls.load(Ordering::SeqCst), 1);
    assert_eq!(stub_peer_b.buffered_write_calls.load(Ordering::SeqCst), 1);

    assert_eq!(
        peer_a_gcs
            .get_blob(&large_id)
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        large_payload.as_slice()
    );
    assert_eq!(
        peer_b_gcs
            .get_blob(&large_id)
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        large_payload.as_slice()
    );
    assert_eq!(
        peer_a_gcs
            .get_blob(&small_id)
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        small_payload.as_slice()
    );
    assert_eq!(
        peer_b_gcs
            .get_blob(&small_id)
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        small_payload.as_slice()
    );
}

#[tokio::test]
async fn test_gcs_blob_store_streaming_full_read_over_4mib_and_corruption_detection() {
    let stub = Arc::new(FakeGcsStub::default());
    let gcs_client = Storage::<FakeGcsStub>::from_stub(stub.clone());
    let gcs: Arc<dyn BlobStore> =
        Arc::new(GcsBlobStore::from_storage("gs://jj-blobs-stream-crc", gcs_client).unwrap());
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    let store = CompositeStore::new(metadata.clone(), gcs);
    store.register_repo("stream-repo", None).await.unwrap();

    let large_len = 5 * 1024 * 1024 + 19;
    let payload: Vec<u8> = (0..large_len)
        .map(|i| ((i * 13 + 5) & 0xff) as u8)
        .collect();
    let blob_id = vec![0x71; 20];
    let mut staged = tempfile::NamedTempFile::new().unwrap();
    staged.write_all(&payload).unwrap();
    staged.flush().unwrap();
    store
        .put_file_from_temp_file("stream-repo", &blob_id, staged.path(), large_len as u64)
        .await
        .unwrap();

    // Full >4 MiB stream issues a single unranged GCS read `(0, 0)` (enabling HTTP 200 CRC32C).
    for max_bytes in [
        None,
        Some(0),
        Some(large_len as u64),
        Some(large_len as u64 + 64),
    ] {
        stub.recorded_read_ranges.lock().unwrap().clear();
        let got = collect_stream_result(&store, "stream-repo", &blob_id, 0, max_bytes)
            .await
            .unwrap();
        assert_eq!(got, payload);
        assert_eq!(
            *stub.recorded_read_ranges.lock().unwrap(),
            vec![(0, 0)],
            "full >4 MiB stream (max_bytes={max_bytes:?}) must use a single unranged request"
        );
    }

    // Interior ranged stream uses a single bounded segment; tail stream includes +1 EOF probe.
    stub.recorded_read_ranges.lock().unwrap().clear();
    let mid = collect_stream_result(&store, "stream-repo", &blob_id, 1_000_000, Some(200_000))
        .await
        .unwrap();
    assert_eq!(mid, &payload[1_000_000..1_200_000]);
    assert_eq!(
        *stub.recorded_read_ranges.lock().unwrap(),
        vec![(1_000_000, 200_000)]
    );

    let tail_offset = (5 * 1024 * 1024) as u64;
    stub.recorded_read_ranges.lock().unwrap().clear();
    let tail = collect_stream_result(&store, "stream-repo", &blob_id, tail_offset, None)
        .await
        .unwrap();
    assert_eq!(tail, &payload[tail_offset as usize..]);
    assert_eq!(
        *stub.recorded_read_ranges.lock().unwrap(),
        vec![(tail_offset as i64, 20)]
    );

    // Exact EOF succeeds with empty stream; past EOF returns OutOfRange.
    assert!(
        collect_stream_result(&store, "stream-repo", &blob_id, large_len as u64, None)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        store
            .get_file_range_stream("stream-repo", &blob_id, large_len as u64 + 1, None)
            .await,
        Err(StoreError::OutOfRange(_))
    ));

    // Truncated GCS object surfaces Corruption on full, ranged, and EOF reads (range + stream).
    stub.overwrite_raw_blob(
        "jj-blobs-stream-crc",
        &blob_id,
        Bytes::copy_from_slice(&payload[..2 * 1024 * 1024]),
    );
    for (offset, max_bytes) in [
        (0, None),
        (1_000_000, Some(2 * 1024 * 1024)),
        (4 * 1024 * 1024, None),
        (large_len as u64, None),
    ] {
        assert!(
            matches!(
                store
                    .get_file_range("stream-repo", &blob_id, offset, max_bytes)
                    .await,
                Err(StoreError::Corruption(_))
            ),
            "truncated get_file_range({offset}, {max_bytes:?}) must fail with Corruption"
        );
        assert!(
            matches!(
                collect_stream_result(&store, "stream-repo", &blob_id, offset, max_bytes).await,
                Err(StoreError::Corruption(_))
            ),
            "truncated get_file_range_stream({offset}, {max_bytes:?}) must fail with Corruption"
        );
    }

    // Oversized GCS object (+1 extra byte) surfaces Corruption on full, tail, and EOF reads.
    let mut oversized = payload.clone();
    oversized.push(0x99);
    stub.overwrite_raw_blob("jj-blobs-stream-crc", &blob_id, Bytes::from(oversized));
    for (offset, max_bytes) in [
        (0, None),
        (tail_offset, None),
        (tail_offset, Some(u64::MAX)),
        (large_len as u64, None),
    ] {
        assert!(
            matches!(
                store
                    .get_file_range("stream-repo", &blob_id, offset, max_bytes)
                    .await,
                Err(StoreError::Corruption(_))
            ),
            "oversized get_file_range({offset}, {max_bytes:?}) must fail with Corruption"
        );
        assert!(
            matches!(
                collect_stream_result(&store, "stream-repo", &blob_id, offset, max_bytes).await,
                Err(StoreError::Corruption(_))
            ),
            "oversized get_file_range_stream({offset}, {max_bytes:?}) must fail with Corruption"
        );
    }

    // Non-inline 0-byte metadata pointing to non-empty GCS object surfaces Corruption.
    let empty_corrupt_id = vec![0x72; 20];
    stub.overwrite_raw_blob(
        "jj-blobs-stream-crc",
        &empty_corrupt_id,
        Bytes::from_static(b"unexpected bytes"),
    );
    metadata
        .put_blob_record(
            "stream-repo",
            &BlobRecord {
                blob_id: empty_corrupt_id.clone(),
                content_size: 0,
                inline_content: None,
                primary_location: Some("gs://jj-blobs-stream-crc".to_string()),
                replicated_locations: vec!["gs://jj-blobs-stream-crc".to_string()],
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        store
            .get_file_range("stream-repo", &empty_corrupt_id, 0, None)
            .await,
        Err(StoreError::Corruption(_))
    ));
    assert!(matches!(
        collect_stream_result(&store, "stream-repo", &empty_corrupt_id, 0, None).await,
        Err(StoreError::Corruption(_))
    ));
}

#[tokio::test]
async fn test_replicated_blob_store_rejects_truncated_and_oversized_blobs_in_range_and_stream() {
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    let local_stub = Arc::new(FakeGcsStub::default());
    let peer_stub = Arc::new(FakeGcsStub::default());

    let local_gcs: Arc<dyn BlobStore> = Arc::new(
        GcsBlobStore::from_storage(
            "gs://jj-blobs-corrupt-local",
            Storage::<FakeGcsStub>::from_stub(local_stub.clone()),
        )
        .unwrap(),
    );
    let peer_gcs: Arc<dyn BlobStore> = Arc::new(
        GcsBlobStore::from_storage(
            "gs://jj-blobs-corrupt-peer",
            Storage::<FakeGcsStub>::from_stub(peer_stub.clone()),
        )
        .unwrap(),
    );

    let repl = Arc::new(ReplicatedBlobStore::new(
        local_gcs,
        vec![peer_gcs],
        metadata.clone(),
        ReplicatedBlobStoreConfig::default(),
    ));
    let store = CompositeStore::new(metadata.clone(), repl.clone());
    store
        .register_repo("corrupt-repl-repo", None)
        .await
        .unwrap();

    // Small blob on peer only: oversized and truncated objects must fail with Corruption
    // without populating the LRU cache or read-repairing into the local bucket.
    let small_id = vec![0x81; 20];
    let small_len = 32 * 1024u64;
    metadata
        .put_blob_record(
            "corrupt-repl-repo",
            &BlobRecord {
                blob_id: small_id.clone(),
                content_size: small_len,
                inline_content: None,
                primary_location: Some("gs://jj-blobs-corrupt-peer".to_string()),
                replicated_locations: vec!["gs://jj-blobs-corrupt-peer".to_string()],
            },
        )
        .await
        .unwrap();

    for bad_data in [
        Bytes::from(vec![0x11; 16 * 1024]),
        Bytes::from(vec![0x11; 32 * 1024 + 5]),
    ] {
        peer_stub.overwrite_raw_blob("jj-blobs-corrupt-peer", &small_id, bad_data);
        for (offset, max_bytes) in [(0, None), (0, Some(1024)), (small_len, None)] {
            assert!(matches!(
                store
                    .get_file_range("corrupt-repl-repo", &small_id, offset, max_bytes)
                    .await,
                Err(StoreError::Corruption(_))
            ));
            assert!(matches!(
                collect_stream_result(&store, "corrupt-repl-repo", &small_id, offset, max_bytes)
                    .await,
                Err(StoreError::Corruption(_))
            ));
        }
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(repl.stats().cache_hits.load(Ordering::Relaxed), 0);
    assert_eq!(local_stub.write_calls.load(Ordering::SeqCst), 0);

    // Large (> 4 MiB) blob on local and peer: truncated and oversized (+1 byte) objects must
    // fail with Corruption on both `get_file_range` and `get_file_range_stream` (verifying
    // `+1` EOF probe preservation on `ReadSpan::Range` across streaming windows).
    let large_id = vec![0x82; 20];
    let large_len = 5 * 1024 * 1024u64;
    metadata
        .put_blob_record(
            "corrupt-repl-repo",
            &BlobRecord {
                blob_id: large_id.clone(),
                content_size: large_len,
                inline_content: None,
                primary_location: Some("gs://jj-blobs-corrupt-local".to_string()),
                replicated_locations: vec!["gs://jj-blobs-corrupt-local".to_string()],
            },
        )
        .await
        .unwrap();

    for bad_data in [
        Bytes::from(vec![0x22; (4 * 1024 * 1024) + 512]),
        Bytes::from(vec![0x22; (5 * 1024 * 1024) + 1]),
    ] {
        local_stub.overwrite_raw_blob("jj-blobs-corrupt-local", &large_id, bad_data);
        for (offset, max_bytes) in [
            (0, None),
            (4 * 1024 * 1024, None),
            (4 * 1024 * 1024, Some(u64::MAX)),
            (large_len, None),
        ] {
            assert!(
                matches!(
                    store
                        .get_file_range("corrupt-repl-repo", &large_id, offset, max_bytes)
                        .await,
                    Err(StoreError::Corruption(_))
                ),
                "large get_file_range({offset}, {max_bytes:?}) must fail with Corruption"
            );
            assert!(
                matches!(
                    collect_stream_result(
                        &store,
                        "corrupt-repl-repo",
                        &large_id,
                        offset,
                        max_bytes
                    )
                    .await,
                    Err(StoreError::Corruption(_))
                ),
                "large get_file_range_stream({offset}, {max_bytes:?}) must fail with Corruption"
            );
        }
    }
}

#[tokio::test]
async fn test_replicated_blob_store_streaming_over_4mib_uses_unranged_gcs_and_falls_back_to_peer() {
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    let local_stub = Arc::new(FakeGcsStub::default());
    let peer_stub = Arc::new(FakeGcsStub::default());

    let local_gcs: Arc<dyn BlobStore> = Arc::new(
        GcsBlobStore::from_storage(
            "gs://jj-blobs-repl-stream-local",
            Storage::<FakeGcsStub>::from_stub(local_stub.clone()),
        )
        .unwrap(),
    );
    let peer_gcs: Arc<dyn BlobStore> = Arc::new(
        GcsBlobStore::from_storage(
            "gs://jj-blobs-repl-stream-peer",
            Storage::<FakeGcsStub>::from_stub(peer_stub.clone()),
        )
        .unwrap(),
    );

    let repl = Arc::new(ReplicatedBlobStore::new(
        local_gcs,
        vec![peer_gcs.clone()],
        metadata.clone(),
        ReplicatedBlobStoreConfig::default(),
    ));
    let store = CompositeStore::new(metadata.clone(), repl.clone());
    store.register_repo("repl-stream-repo", None).await.unwrap();

    let large_len = 5 * 1024 * 1024 + 29;
    let payload: Vec<u8> = (0..large_len)
        .map(|i| ((i * 23 + 11) & 0xff) as u8)
        .collect();
    let blob_id = vec![0x91; 20];
    let mut staged = tempfile::NamedTempFile::new().unwrap();
    staged.write_all(&payload).unwrap();
    staged.flush().unwrap();
    store
        .put_file_from_temp_file(
            "repl-stream-repo",
            &blob_id,
            staged.path(),
            large_len as u64,
        )
        .await
        .unwrap();

    // Full >4 MiB stream through ReplicatedBlobStore issues a single unranged `(0, 0)` GCS
    // GET rather than 4 MiB HTTP 206 windows, keeping end-of-stream CRC32C validation active.
    local_stub.recorded_read_ranges.lock().unwrap().clear();
    let got = collect_stream_result(&store, "repl-stream-repo", &blob_id, 0, None)
        .await
        .unwrap();
    assert_eq!(got, payload);
    assert_eq!(
        *local_stub.recorded_read_ranges.lock().unwrap(),
        vec![(0, 0)]
    );
    assert_eq!(repl.stats().local_hits.load(Ordering::Relaxed), 1);

    // Peer-only >4 MiB stream falls back to peer with a single unranged `(0, 0)` GCS GET.
    let peer_only_id = vec![0x92; 20];
    peer_gcs
        .put_blob_from_file(&peer_only_id, staged.path(), large_len as u64)
        .await
        .unwrap();
    metadata
        .put_blob_record(
            "repl-stream-repo",
            &BlobRecord {
                blob_id: peer_only_id.clone(),
                content_size: large_len as u64,
                inline_content: None,
                primary_location: Some("gs://jj-blobs-repl-stream-peer".to_string()),
                replicated_locations: vec!["gs://jj-blobs-repl-stream-peer".to_string()],
            },
        )
        .await
        .unwrap();

    peer_stub.recorded_read_ranges.lock().unwrap().clear();
    let got_peer = collect_stream_result(&store, "repl-stream-repo", &peer_only_id, 0, None)
        .await
        .unwrap();
    assert_eq!(got_peer, payload);
    assert_eq!(
        *peer_stub.recorded_read_ranges.lock().unwrap(),
        vec![(0, 0)]
    );
    assert_eq!(repl.stats().origin_fallbacks.load(Ordering::Relaxed), 1);

    // Transient local Unavailable error falls back to peer stream.
    *local_stub.injected_read_error.lock().unwrap() = Some(RpcCode::Unavailable);
    peer_stub.recorded_read_ranges.lock().unwrap().clear();
    let got_after_unavail =
        collect_stream_result(&store, "repl-stream-repo", &peer_only_id, 0, None)
            .await
            .unwrap();
    assert_eq!(got_after_unavail, payload);
    assert_eq!(
        *peer_stub.recorded_read_ranges.lock().unwrap(),
        vec![(0, 0)]
    );
    *local_stub.injected_read_error.lock().unwrap() = None;

    // Truncated and oversized >4 MiB peer streams still surface Corruption.
    peer_stub.overwrite_raw_blob(
        "jj-blobs-repl-stream-peer",
        &peer_only_id,
        Bytes::copy_from_slice(&payload[..3 * 1024 * 1024]),
    );
    assert!(matches!(
        collect_stream_result(&store, "repl-stream-repo", &peer_only_id, 0, None).await,
        Err(StoreError::Corruption(_))
    ));

    let mut oversized = payload;
    oversized.push(0xfe);
    peer_stub.overwrite_raw_blob(
        "jj-blobs-repl-stream-peer",
        &peer_only_id,
        Bytes::from(oversized),
    );
    assert!(matches!(
        collect_stream_result(&store, "repl-stream-repo", &peer_only_id, 0, None).await,
        Err(StoreError::Corruption(_))
    ));
}
