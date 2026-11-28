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

use async_trait::async_trait;

use super::repair::{FanoutGuard, RepairGuard};
use super::*;
use crate::store::{MemoryBlobStore, MemoryMetadataStore};

#[test]
fn lru_cache_bounds_tiny_blobs_and_compacts_order_on_repeated_inserts() {
    let key_len = 20usize;
    let single_empty_cost = entry_cost_bytes(&[0u8; 20], &Bytes::new());
    let capacity = single_empty_cost * 4;
    let mut cache = ByteBoundedLruCache::new(capacity);

    // Inserting hundreds of 0-byte blobs must stay bounded by capacity / entry_cost.
    for i in 0..200u16 {
        let mut key = vec![0u8; key_len];
        key[0] = (i >> 8) as u8;
        key[1] = (i & 0xff) as u8;
        cache.insert(&key, Bytes::new());
        assert!(cache.used_bytes <= capacity);
        assert!(cache.entries.len() <= 4);
    }
    assert_eq!(cache.entries.len(), 4);
    assert_eq!(cache.used_bytes, capacity);

    // Repeatedly re-inserting the same key without calling get() compacts order.
    let hot_key = vec![0xaa; key_len];
    for _ in 0..250 {
        cache.insert(&hot_key, Bytes::from_static(b"x"));
    }
    assert!(
        cache.order.len() <= cache.entries.len().saturating_mul(4).max(64),
        "order deque grew unbounded on insert: {}",
        cache.order.len()
    );
    let expected_used: usize = cache
        .entries
        .iter()
        .map(|(k, (v, _))| entry_cost_bytes(k, v))
        .sum();
    assert_eq!(cache.used_bytes, expected_used);
}

#[tokio::test]
async fn spawn_read_repair_skips_when_background_semaphore_is_exhausted() {
    let local: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::with_location("gs://local"));
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    let repl = ReplicatedBlobStore::new(
        local.clone(),
        vec![],
        metadata,
        ReplicatedBlobStoreConfig::default(),
    );

    let held_permits = repl
        .inner
        .bg_tasks
        .clone()
        .acquire_many_owned(DEFAULT_MAX_BACKGROUND_TASKS as u32)
        .await
        .unwrap();

    let blob_id = vec![0x42; 20];
    repl.inner
        .spawn_read_repair(blob_id.clone(), Bytes::from_static(b"repair payload"));

    // Neither repairs_in_flight nor the local store should be touched while saturated.
    assert!(repl.inner.repairs_in_flight.lock().unwrap().is_empty());
    assert_eq!(local.get_blob(&blob_id).await.unwrap(), None);

    drop(held_permits);
}

#[test]
fn lru_contains_key_does_not_mutate_recency() {
    let key_a = vec![0x01; 20];
    let key_b = vec![0x02; 20];
    let key_c = vec![0x03; 20];
    let val = Bytes::from_static(b"payload!");
    let cost = entry_cost_bytes(&key_a, &val);
    let mut cache = ByteBoundedLruCache::new(cost * 2);

    cache.insert(&key_a, val.clone());
    cache.insert(&key_b, val.clone());
    let order_len_before = cache.order.len();
    let tick_before = cache.tick;

    // Probing `key_a` via `contains_key` must not refresh its recency or mutate order/tick.
    assert!(cache.contains_key(&key_a));
    assert!(!cache.contains_key(&key_c));
    assert_eq!(cache.order.len(), order_len_before);
    assert_eq!(cache.tick, tick_before);

    // Inserting `key_c` must evict `key_a` (still the oldest), keeping `key_b`.
    cache.insert(&key_c, val);
    assert!(!cache.contains_key(&key_a));
    assert!(cache.contains_key(&key_b));
    assert!(cache.contains_key(&key_c));
}

struct ScriptedBlobStore {
    location: String,
    inner: MemoryBlobStore,
    delay_put: Duration,
    put_calls: AtomicU64,
    full_read_calls: AtomicU64,
    range_read_calls: AtomicU64,
    fail_reads_after: Mutex<Option<StoreError>>,
    miss_first_n_reads: AtomicU64,
}

impl ScriptedBlobStore {
    fn new(location: &str) -> Self {
        Self {
            location: location.to_string(),
            inner: MemoryBlobStore::with_location(location),
            delay_put: Duration::ZERO,
            put_calls: AtomicU64::new(0),
            full_read_calls: AtomicU64::new(0),
            range_read_calls: AtomicU64::new(0),
            fail_reads_after: Mutex::new(None),
            miss_first_n_reads: AtomicU64::new(0),
        }
    }

    fn consume_miss(&self) -> bool {
        let remaining = self.miss_first_n_reads.load(Ordering::SeqCst);
        remaining > 0
            && self
                .miss_first_n_reads
                .compare_exchange(remaining, remaining - 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
    }
}

#[async_trait]
impl BlobStore for ScriptedBlobStore {
    fn location_id(&self) -> &str {
        &self.location
    }

    async fn put_blob(&self, blob_id: &[u8], data: Bytes) -> StoreResult<()> {
        self.put_calls.fetch_add(1, Ordering::SeqCst);
        if !self.delay_put.is_zero() {
            tokio::time::sleep(self.delay_put).await;
        }
        self.inner.put_blob(blob_id, data).await
    }

    async fn get_blob(&self, blob_id: &[u8]) -> StoreResult<Option<Bytes>> {
        self.full_read_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(err) = self.fail_reads_after.lock().unwrap().clone() {
            return Err(err);
        }
        if self.consume_miss() {
            return Ok(None);
        }
        self.inner.get_blob(blob_id).await
    }

    async fn get_blob_range(
        &self,
        blob_id: &[u8],
        offset: u64,
        length: Option<u64>,
    ) -> StoreResult<Option<Bytes>> {
        self.range_read_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(err) = self.fail_reads_after.lock().unwrap().clone() {
            return Err(err);
        }
        if self.consume_miss() {
            return Ok(None);
        }
        self.inner.get_blob_range(blob_id, offset, length).await
    }

    async fn has_blob(&self, blob_id: &[u8]) -> StoreResult<bool> {
        if let Some(err) = self.fail_reads_after.lock().unwrap().clone() {
            return Err(err);
        }
        if self.consume_miss() {
            return Ok(false);
        }
        self.inner.has_blob(blob_id).await
    }
}

#[tokio::test]
async fn retry_loop_handles_extreme_budget_and_backoff_without_overflow() {
    let local = Arc::new(ScriptedBlobStore::new("gs://local"));
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    metadata.register_project("proj", None).await.unwrap();

    let blob_id = vec![0x91; 20];
    let payload = Bytes::from_static(b"becomes visible after first retry");
    local.put_blob(&blob_id, payload.clone()).await.unwrap();
    local.miss_first_n_reads.store(1, Ordering::SeqCst);
    metadata
        .put_blob_record(
            "proj",
            &BlobRecord {
                blob_id: blob_id.clone(),
                content_size: payload.len() as u64,
                inline_content: None,
                primary_location: Some("gs://local".to_string()),
                replicated_locations: vec!["gs://local".to_string()],
            },
        )
        .await
        .unwrap();

    let repl = ReplicatedBlobStore::new(
        local,
        vec![],
        metadata,
        ReplicatedBlobStoreConfig {
            cache_capacity_bytes: 0,
            replication_retry_budget: Duration::from_millis(u64::MAX),
            initial_retry_backoff: Duration::MAX,
            async_write_fanout: false,
        },
    );

    let got = repl.get_blob(&blob_id).await.unwrap();
    assert_eq!(got, Some(payload));
    assert_eq!(
        repl.stats().visibility_lag_retries.load(Ordering::Relaxed),
        1
    );
}

#[tokio::test]
async fn has_blob_checks_cache_and_falls_back_to_peers() {
    let local = Arc::new(ScriptedBlobStore::new("gs://local"));
    let peer = Arc::new(ScriptedBlobStore::new("gs://peer"));
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    metadata.register_project("proj", None).await.unwrap();

    let peer_only_id = vec![0x55; 20];
    let payload = Bytes::from_static(b"peer-only blob");
    peer.put_blob(&peer_only_id, payload.clone()).await.unwrap();
    metadata
        .put_blob_record(
            "proj",
            &BlobRecord {
                blob_id: peer_only_id.clone(),
                content_size: payload.len() as u64,
                inline_content: None,
                primary_location: Some("gs://peer".to_string()),
                replicated_locations: vec!["gs://peer".to_string()],
            },
        )
        .await
        .unwrap();

    let repl = ReplicatedBlobStore::new(
        local.clone(),
        vec![peer],
        metadata,
        ReplicatedBlobStoreConfig::default(),
    );

    // Missing locally, present in configured peer listed in metadata.
    assert!(repl.has_blob(&peer_only_id).await.unwrap());
    assert!(!repl.has_blob(&[0x66; 20]).await.unwrap());

    // Populate cache via local read, then break local reads: cached has_blob still succeeds.
    let cached_id = vec![0x77; 20];
    local
        .put_blob(&cached_id, Bytes::from_static(b"cached bytes"))
        .await
        .unwrap();
    assert!(repl.get_blob(&cached_id).await.unwrap().is_some());
    *local.fail_reads_after.lock().unwrap() = Some(StoreError::Backend(
        "local must not be hit on cache hit".into(),
    ));
    assert!(repl.has_blob(&cached_id).await.unwrap());
}

#[tokio::test]
async fn get_blob_range_serves_warm_cache_without_querying_metadata() {
    let local = Arc::new(ScriptedBlobStore::new("gs://local"));
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());

    let blob_id = vec![0x88; 20];
    let payload = Bytes::from_static(b"0123456789abcdef");
    local.put_blob(&blob_id, payload).await.unwrap();

    let repl = ReplicatedBlobStore::new(
        local.clone(),
        vec![],
        metadata,
        ReplicatedBlobStoreConfig::default(),
    );
    // Warm the cache (note: no BlobRecord exists in metadata).
    assert!(repl.get_blob(&blob_id).await.unwrap().is_some());
    // Break local reads so any fallback past the cache would fail.
    *local.fail_reads_after.lock().unwrap() = Some(StoreError::Backend(
        "local must not be hit on cache hit".into(),
    ));

    let slice = repl
        .get_blob_range(&blob_id, 4, Some(6))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(slice.as_ref(), b"456789");
    assert_eq!(repl.stats().cache_hits.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn spawn_write_fanout_deduplicates_concurrent_blob_location_uploads() {
    let local: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::with_location("gs://local"));
    let mut slow_peer = ScriptedBlobStore::new("gs://peer-slow");
    slow_peer.delay_put = Duration::from_millis(80);
    let slow_peer = Arc::new(slow_peer);
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    metadata.register_project("proj", None).await.unwrap();

    let blob_id = vec![0x99; 20];
    let payload = Bytes::from_static(b"fanout dedupe payload");
    metadata
        .put_blob_record(
            "proj",
            &BlobRecord {
                blob_id: blob_id.clone(),
                content_size: payload.len() as u64,
                inline_content: None,
                primary_location: Some("gs://local".to_string()),
                replicated_locations: vec!["gs://local".to_string()],
            },
        )
        .await
        .unwrap();

    let repl = ReplicatedBlobStore::new(
        local,
        vec![slow_peer.clone()],
        metadata,
        ReplicatedBlobStoreConfig {
            async_write_fanout: true,
            ..ReplicatedBlobStoreConfig::default()
        },
    );

    for _ in 0..5 {
        repl.notify_blob_published(&blob_id, payload.clone()).await;
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    while !repl.inner.fanouts_in_flight.lock().unwrap().is_empty() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for fan-out task to finish"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(slow_peer.put_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn get_blob_range_with_record_inline_short_circuits_and_validates_size() {
    let local = Arc::new(ScriptedBlobStore::new("gs://local"));
    *local.fail_reads_after.lock().unwrap() = Some(StoreError::Backend(
        "inline records must not touch object store".into(),
    ));
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    let repl = ReplicatedBlobStore::new(
        local,
        vec![],
        metadata,
        ReplicatedBlobStoreConfig::default(),
    );

    let empty_inline = BlobRecord {
        blob_id: vec![0xa1; 20],
        content_size: 0,
        inline_content: Some(Bytes::new()),
        primary_location: None,
        replicated_locations: vec![],
    };
    assert_eq!(
        repl.get_blob_range_with_record(&empty_inline, 0, None)
            .await
            .unwrap(),
        Some(Bytes::new())
    );
    assert!(matches!(
        repl.get_blob_range_with_record(&empty_inline, 1, None)
            .await,
        Err(StoreError::OutOfRange(_))
    ));

    let payload = Bytes::from_static(b"inline bytes");
    let inline_rec = BlobRecord {
        blob_id: vec![0xa2; 20],
        content_size: payload.len() as u64,
        inline_content: Some(payload.clone()),
        primary_location: None,
        replicated_locations: vec![],
    };
    assert_eq!(
        repl.get_blob_range_with_record(&inline_rec, 0, Some(6))
            .await
            .unwrap(),
        Some(Bytes::from_static(b"inline"))
    );
    assert_eq!(
        repl.get_blob_range_with_record(&inline_rec, payload.len() as u64, None)
            .await
            .unwrap(),
        Some(Bytes::new())
    );
    assert!(matches!(
        repl.get_blob_range_with_record(&inline_rec, payload.len() as u64 + 1, None)
            .await,
        Err(StoreError::OutOfRange(_))
    ));

    let corrupt_inline = BlobRecord {
        blob_id: vec![0xa3; 20],
        content_size: 99,
        inline_content: Some(Bytes::from_static(b"short")),
        primary_location: None,
        replicated_locations: vec![],
    };
    assert!(matches!(
        repl.get_blob_range_with_record(&corrupt_inline, 0, None)
            .await,
        Err(StoreError::Corruption(_))
    ));
}

#[tokio::test]
async fn get_blob_range_with_record_non_inline_empty_blob_checks_storage_and_repairs() {
    let local = Arc::new(ScriptedBlobStore::new("gs://local"));
    let peer = Arc::new(ScriptedBlobStore::new("gs://peer"));
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    metadata.register_project("proj", None).await.unwrap();

    let blob_id = vec![0xb1; 20];
    let record = BlobRecord {
        blob_id: blob_id.clone(),
        content_size: 0,
        inline_content: None,
        primary_location: Some("gs://peer".to_string()),
        replicated_locations: vec!["gs://peer".to_string()],
    };
    metadata.put_blob_record("proj", &record).await.unwrap();

    let repl = ReplicatedBlobStore::new(
        local.clone(),
        vec![peer.clone()],
        metadata.clone(),
        ReplicatedBlobStoreConfig::default(),
    );

    // Missing across local and peer must return Ok(None), never Ok(Some(empty)).
    assert_eq!(
        repl.get_blob_range_with_record(&record, 0, None)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        repl.get_blob_range_with_record(&record, 0, Some(16))
            .await
            .unwrap(),
        None
    );

    // Backend errors on local or peer propagate rather than succeeding with empty bytes.
    *local.fail_reads_after.lock().unwrap() =
        Some(StoreError::PermissionDenied("local denied".into()));
    assert!(matches!(
        repl.get_blob_range_with_record(&record, 0, None).await,
        Err(StoreError::PermissionDenied(_))
    ));
    *local.fail_reads_after.lock().unwrap() = Some(StoreError::Unavailable("local down".into()));
    assert!(matches!(
        repl.get_blob_range_with_record(&record, 0, None).await,
        Err(StoreError::Unavailable(_))
    ));
    *local.fail_reads_after.lock().unwrap() = None;

    *peer.fail_reads_after.lock().unwrap() =
        Some(StoreError::PermissionDenied("peer denied".into()));
    assert!(matches!(
        repl.get_blob_range_with_record(&record, 0, None).await,
        Err(StoreError::PermissionDenied(_))
    ));
    *peer.fail_reads_after.lock().unwrap() = None;

    // Present only on peer: falls back to peer, caches, and triggers read-repair into local.
    peer.put_blob(&blob_id, Bytes::new()).await.unwrap();
    assert_eq!(
        repl.get_blob_range_with_record(&record, 0, None)
            .await
            .unwrap(),
        Some(Bytes::new())
    );
    assert_eq!(repl.stats().origin_fallbacks.load(Ordering::Relaxed), 1);

    let deadline = Instant::now() + Duration::from_secs(5);
    while repl.stats().read_repairs_completed.load(Ordering::Relaxed) < 1 {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for 0-byte blob read-repair"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(local.get_blob(&blob_id).await.unwrap(), Some(Bytes::new()));
    let updated = metadata
        .get_global_blob_record(&blob_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        updated
            .replicated_locations
            .contains(&"gs://local".to_string())
    );

    // Cached 0-byte blob is served from cache even if backends fail.
    *local.fail_reads_after.lock().unwrap() = Some(StoreError::Backend("should hit cache".into()));
    *peer.fail_reads_after.lock().unwrap() = Some(StoreError::Backend("should hit cache".into()));
    assert_eq!(
        repl.get_blob_range_with_record(&record, 0, Some(8))
            .await
            .unwrap(),
        Some(Bytes::new())
    );
    assert_eq!(repl.stats().cache_hits.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn get_blob_range_with_record_eof_verifies_existence_and_propagates_errors() {
    let local = Arc::new(ScriptedBlobStore::new("gs://local"));
    let peer = Arc::new(ScriptedBlobStore::new("gs://peer"));
    // Leave MetadataStore empty so peer checks prove `verify_eof_with_record` uses `record`
    // directly without re-querying MetadataStore.
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());

    let blob_id = vec![0xc1; 20];
    let payload = Bytes::from_static(b"hello eof existence check");
    let len = payload.len() as u64;
    let record = BlobRecord {
        blob_id: blob_id.clone(),
        content_size: len,
        inline_content: None,
        primary_location: Some("gs://peer".to_string()),
        replicated_locations: vec!["gs://peer".to_string()],
    };

    let repl = ReplicatedBlobStore::new(
        local.clone(),
        vec![peer.clone()],
        metadata,
        ReplicatedBlobStoreConfig::default(),
    );

    // Missing across local and peer returns Ok(None).
    assert_eq!(
        repl.get_blob_range_with_record(&record, len, None)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        repl.get_blob_range_with_record(&record, len, Some(8))
            .await
            .unwrap(),
        None
    );

    // Backend errors propagate from local and peers.
    *local.fail_reads_after.lock().unwrap() =
        Some(StoreError::PermissionDenied("local denied".into()));
    assert!(matches!(
        repl.get_blob_range_with_record(&record, len, None).await,
        Err(StoreError::PermissionDenied(_))
    ));
    *local.fail_reads_after.lock().unwrap() = Some(StoreError::Unavailable("local down".into()));
    assert!(matches!(
        repl.get_blob_range_with_record(&record, len, None).await,
        Err(StoreError::Unavailable(_))
    ));
    *local.fail_reads_after.lock().unwrap() = None;

    *peer.fail_reads_after.lock().unwrap() =
        Some(StoreError::PermissionDenied("peer denied".into()));
    assert!(matches!(
        repl.get_blob_range_with_record(&record, len, None).await,
        Err(StoreError::PermissionDenied(_))
    ));
    *peer.fail_reads_after.lock().unwrap() = None;

    // Present only on peer: returns Ok(Some(empty)) using `record` peers without metadata,
    // including when local returns a transient Unavailable error.
    peer.put_blob(&blob_id, payload.clone()).await.unwrap();
    assert_eq!(
        repl.get_blob_range_with_record(&record, len, None)
            .await
            .unwrap(),
        Some(Bytes::new())
    );
    *local.fail_reads_after.lock().unwrap() =
        Some(StoreError::Unavailable("transient local error".into()));
    assert_eq!(
        repl.get_blob_range_with_record(&record, len, Some(4))
            .await
            .unwrap(),
        Some(Bytes::new())
    );
    *local.fail_reads_after.lock().unwrap() = None;

    // Present locally: succeeds even if peer fails.
    local.put_blob(&blob_id, payload.clone()).await.unwrap();
    *peer.fail_reads_after.lock().unwrap() =
        Some(StoreError::PermissionDenied("peer unreachable".into()));
    assert_eq!(
        repl.get_blob_range_with_record(&record, len, None)
            .await
            .unwrap(),
        Some(Bytes::new())
    );

    // Present in cache: succeeds even if both local and peer fail.
    assert_eq!(repl.get_blob(&blob_id).await.unwrap(), Some(payload));
    *local.fail_reads_after.lock().unwrap() = Some(StoreError::Backend(
        "local must not be hit on cache hit".into(),
    ));
    assert_eq!(
        repl.get_blob_range_with_record(&record, len, None)
            .await
            .unwrap(),
        Some(Bytes::new())
    );
}

#[tokio::test]
async fn corrupt_local_and_peer_blobs_are_rejected_without_caching_or_read_repair() {
    let local = Arc::new(ScriptedBlobStore::new("gs://local"));
    let peer = Arc::new(ScriptedBlobStore::new("gs://peer"));
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    metadata.register_project("proj", None).await.unwrap();

    let repl = ReplicatedBlobStore::new(
        local.clone(),
        vec![peer.clone()],
        metadata.clone(),
        ReplicatedBlobStoreConfig::default(),
    );

    // Peer holds a truncated small blob: must return Corruption, must not cache or repair.
    let peer_trunc_id = vec![0xd1; 20];
    peer.put_blob(&peer_trunc_id, Bytes::from_static(b"short"))
        .await
        .unwrap();
    let peer_trunc_rec = BlobRecord {
        blob_id: peer_trunc_id.clone(),
        content_size: 32,
        inline_content: None,
        primary_location: Some("gs://peer".to_string()),
        replicated_locations: vec!["gs://peer".to_string()],
    };
    metadata
        .put_blob_record("proj", &peer_trunc_rec)
        .await
        .unwrap();

    assert!(matches!(
        repl.get_blob_range_with_record(&peer_trunc_rec, 0, None)
            .await,
        Err(StoreError::Corruption(_))
    ));
    assert!(
        !repl
            .inner
            .cache
            .lock()
            .unwrap()
            .contains_key(&peer_trunc_id),
        "corrupt peer blob must not populate cache"
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        local.put_calls.load(Ordering::SeqCst),
        0,
        "corrupt peer blob must not trigger read-repair"
    );

    // Local holds an oversized small blob: must return Corruption without caching.
    let local_over_id = vec![0xd2; 20];
    local
        .put_blob(
            &local_over_id,
            Bytes::from_static(b"way too many bytes here"),
        )
        .await
        .unwrap();
    let local_over_rec = BlobRecord {
        blob_id: local_over_id.clone(),
        content_size: 4,
        inline_content: None,
        primary_location: Some("gs://local".to_string()),
        replicated_locations: vec!["gs://local".to_string()],
    };
    assert!(matches!(
        repl.get_blob_range_with_record(&local_over_rec, 0, Some(4))
            .await,
        Err(StoreError::Corruption(_))
    ));
    assert!(
        !repl
            .inner
            .cache
            .lock()
            .unwrap()
            .contains_key(&local_over_id),
        "corrupt local blob must not populate cache"
    );

    // If cache was previously populated via unvalidated `get_blob`, `get_blob_range_with_record`
    // still detects the size mismatch on both range reads and EOF checks.
    assert!(repl.get_blob(&local_over_id).await.unwrap().is_some());
    assert!(
        repl.inner
            .cache
            .lock()
            .unwrap()
            .contains_key(&local_over_id)
    );
    assert!(matches!(
        repl.get_blob_range_with_record(&local_over_rec, 0, Some(2))
            .await,
        Err(StoreError::Corruption(_))
    ));
    assert!(matches!(
        repl.get_blob_range_with_record(&local_over_rec, 4, None)
            .await,
        Err(StoreError::Corruption(_))
    ));
}

#[tokio::test]
async fn corrupt_peer_falls_back_to_healthy_peer_and_returns_corruption_when_all_corrupt() {
    let local = Arc::new(ScriptedBlobStore::new("gs://local"));
    let peer1 = Arc::new(ScriptedBlobStore::new("gs://peer-1"));
    let peer2 = Arc::new(ScriptedBlobStore::new("gs://peer-2"));
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    metadata.register_project("proj", None).await.unwrap();

    let repl = ReplicatedBlobStore::new(
        local.clone(),
        vec![peer1.clone(), peer2.clone()],
        metadata.clone(),
        ReplicatedBlobStoreConfig {
            cache_capacity_bytes: 64 * 1024,
            replication_retry_budget: Duration::from_millis(200),
            initial_retry_backoff: Duration::from_millis(10),
            async_write_fanout: false,
        },
    );

    // Small blob: peer-1 is truncated, peer-2 is healthy -> served from peer-2, cached, repaired.
    let small_id = vec![0xe1; 20];
    let small_payload = Bytes::from_static(b"healthy small replica payload");
    peer1
        .put_blob(&small_id, Bytes::from_static(b"short"))
        .await
        .unwrap();
    peer2
        .put_blob(&small_id, small_payload.clone())
        .await
        .unwrap();
    let small_rec = BlobRecord {
        blob_id: small_id.clone(),
        content_size: small_payload.len() as u64,
        inline_content: None,
        primary_location: Some("gs://peer-1".to_string()),
        replicated_locations: vec!["gs://peer-1".to_string(), "gs://peer-2".to_string()],
    };
    metadata.put_blob_record("proj", &small_rec).await.unwrap();

    assert_eq!(
        repl.get_blob_range_with_record(&small_rec, 0, None)
            .await
            .unwrap(),
        Some(small_payload.clone())
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    while repl.stats().read_repairs_completed.load(Ordering::Relaxed) < 1 {
        assert!(Instant::now() < deadline, "timed out waiting for repair");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        local.get_blob(&small_id).await.unwrap(),
        Some(small_payload)
    );

    // Large blob (> SMALL_BLOB_MAX_BYTES): peer-1 is oversized, peer-2 is healthy ->
    // tail read and EOF verification both fall back past peer-1 to peer-2.
    let large_id = vec![0xe2; 20];
    let large_len = SMALL_BLOB_MAX_BYTES as usize + 64;
    let large_payload = Bytes::from(vec![0x5a; large_len]);
    let mut oversized_payload = vec![0x5a; large_len + 1];
    oversized_payload[large_len] = 0xff;
    peer1
        .put_blob(&large_id, Bytes::from(oversized_payload))
        .await
        .unwrap();
    peer2
        .put_blob(&large_id, large_payload.clone())
        .await
        .unwrap();
    let large_rec = BlobRecord {
        blob_id: large_id.clone(),
        content_size: large_len as u64,
        inline_content: None,
        primary_location: Some("gs://peer-1".to_string()),
        replicated_locations: vec!["gs://peer-1".to_string(), "gs://peer-2".to_string()],
    };
    assert_eq!(
        repl.get_blob_range_with_record(&large_rec, large_len as u64 - 16, None)
            .await
            .unwrap(),
        Some(Bytes::from(vec![0x5a; 16]))
    );
    assert_eq!(
        repl.get_blob_range_with_record(&large_rec, large_len as u64, None)
            .await
            .unwrap(),
        Some(Bytes::new())
    );

    // All peers corrupt: returns Corruption with zero visibility-lag retries.
    let all_bad_id = vec![0xe3; 20];
    peer1
        .put_blob(&all_bad_id, Bytes::from_static(b"short"))
        .await
        .unwrap();
    peer2
        .put_blob(&all_bad_id, Bytes::from_static(b"also wrong size!!"))
        .await
        .unwrap();
    let all_bad_rec = BlobRecord {
        blob_id: all_bad_id.clone(),
        content_size: 10,
        inline_content: None,
        primary_location: Some("gs://peer-1".to_string()),
        replicated_locations: vec!["gs://peer-1".to_string(), "gs://peer-2".to_string()],
    };
    let retries_before = repl.stats().visibility_lag_retries.load(Ordering::Relaxed);
    assert!(matches!(
        repl.get_blob_range_with_record(&all_bad_rec, 0, None).await,
        Err(StoreError::Corruption(_))
    ));
    assert_eq!(
        repl.stats().visibility_lag_retries.load(Ordering::Relaxed),
        retries_before
    );
}

#[test]
fn drop_guards_recover_from_poisoned_mutexes() {
    let local: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::with_location("gs://local"));
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    let repl = ReplicatedBlobStore::new(
        local,
        vec![],
        metadata,
        ReplicatedBlobStoreConfig::default(),
    );
    let blob_id = vec![0xee; 20];
    let fanout_key = (blob_id.clone(), "gs://peer".to_string());

    let (tx, _rx) = watch::channel(None);
    repl.inner
        .inflight
        .lock()
        .unwrap()
        .insert(blob_id.clone(), tx.subscribe());
    repl.inner
        .repairs_in_flight
        .lock()
        .unwrap()
        .insert(blob_id.clone());
    repl.inner
        .fanouts_in_flight
        .lock()
        .unwrap()
        .insert(fanout_key.clone());

    // Poison all three tracking mutexes.
    let inner = repl.inner.clone();
    let _ = std::thread::spawn(move || {
        let _g1 = inner.inflight.lock().unwrap();
        let _g2 = inner.repairs_in_flight.lock().unwrap();
        let _g3 = inner.fanouts_in_flight.lock().unwrap();
        panic!("intentional poison");
    })
    .join();

    assert!(repl.inner.inflight.is_poisoned());
    assert!(repl.inner.repairs_in_flight.is_poisoned());
    assert!(repl.inner.fanouts_in_flight.is_poisoned());

    // Dropping the guards must recover without panicking and remove their entries.
    drop(InflightGuard {
        inner: repl.inner.clone(),
        blob_id: blob_id.clone(),
        tx: Some(tx),
    });
    drop(RepairGuard {
        inner: repl.inner.clone(),
        blob_id: blob_id.clone(),
    });
    drop(FanoutGuard {
        inner: repl.inner.clone(),
        key: fanout_key,
    });

    assert!(
        repl.inner
            .inflight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
    );
    assert!(
        repl.inner
            .repairs_in_flight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
    );
    assert!(
        repl.inner
            .fanouts_in_flight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
    );
}

#[tokio::test]
async fn transient_local_unavailable_fallback_does_not_spawn_read_repair() {
    let local = Arc::new(ScriptedBlobStore::new("gs://local"));
    let peer = Arc::new(ScriptedBlobStore::new("gs://peer"));
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    metadata.register_project("proj", None).await.unwrap();

    let blob_id = vec![0xf1; 20];
    let payload = Bytes::from_static(b"served from peer during local outage");
    peer.put_blob(&blob_id, payload.clone()).await.unwrap();
    let record = BlobRecord {
        blob_id,
        content_size: payload.len() as u64,
        inline_content: None,
        primary_location: Some("gs://peer".to_string()),
        replicated_locations: vec!["gs://local".to_string(), "gs://peer".to_string()],
    };
    metadata.put_blob_record("proj", &record).await.unwrap();

    *local.fail_reads_after.lock().unwrap() =
        Some(StoreError::Unavailable("local 503 outage".into()));
    let repl = ReplicatedBlobStore::new(
        local.clone(),
        vec![peer],
        metadata,
        ReplicatedBlobStoreConfig::default(),
    );

    assert_eq!(
        repl.get_blob_range_with_record(&record, 0, None)
            .await
            .unwrap(),
        Some(payload)
    );
    assert_eq!(repl.stats().origin_fallbacks.load(Ordering::Relaxed), 1);
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(local.put_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        repl.stats().read_repairs_completed.load(Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn corrupt_local_small_large_and_eof_fail_over_to_peer_without_repair_or_local_hits() {
    let local = Arc::new(ScriptedBlobStore::new("gs://local"));
    let peer = Arc::new(ScriptedBlobStore::new("gs://peer"));
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    metadata.register_project("proj", None).await.unwrap();

    let repl = ReplicatedBlobStore::new(
        local.clone(),
        vec![peer.clone()],
        metadata.clone(),
        ReplicatedBlobStoreConfig {
            cache_capacity_bytes: 64 * 1024,
            replication_retry_budget: Duration::from_millis(150),
            initial_retry_backoff: Duration::from_millis(10),
            async_write_fanout: false,
        },
    );

    // Small blob: local is truncated, peer is healthy -> fails over without incrementing
    // local_hits or spawning read-repair (which would mask corruption via 412).
    let small_id = vec![0xf2; 20];
    let small_payload = Bytes::from_static(b"healthy small blob from peer");
    local
        .put_blob(&small_id, Bytes::from_static(b"bad"))
        .await
        .unwrap();
    peer.put_blob(&small_id, small_payload.clone())
        .await
        .unwrap();
    let small_rec = BlobRecord {
        blob_id: small_id,
        content_size: small_payload.len() as u64,
        inline_content: None,
        primary_location: Some("gs://peer".to_string()),
        replicated_locations: vec!["gs://local".to_string(), "gs://peer".to_string()],
    };
    let local_puts_before = local.put_calls.load(Ordering::SeqCst);
    assert_eq!(
        repl.get_blob_range_with_record(&small_rec, 0, None)
            .await
            .unwrap(),
        Some(small_payload)
    );
    assert_eq!(repl.stats().local_hits.load(Ordering::Relaxed), 0);
    assert_eq!(repl.stats().origin_fallbacks.load(Ordering::Relaxed), 1);
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(local.put_calls.load(Ordering::SeqCst), local_puts_before);
    assert_eq!(
        repl.stats().read_repairs_completed.load(Ordering::Relaxed),
        0
    );

    // Large blob (> SMALL_BLOB_MAX_BYTES): local is oversized (tail) and truncated (past
    // offset), peer is healthy -> range read and EOF check both fail over without inflating
    // local_hits.
    let large_id = vec![0xf3; 20];
    let large_len = SMALL_BLOB_MAX_BYTES as usize + 128;
    let large_payload = Bytes::from(vec![0x6b; large_len]);
    peer.put_blob(&large_id, large_payload).await.unwrap();
    let large_rec = BlobRecord {
        blob_id: large_id.clone(),
        content_size: large_len as u64,
        inline_content: None,
        primary_location: Some("gs://peer".to_string()),
        replicated_locations: vec!["gs://local".to_string(), "gs://peer".to_string()],
    };

    // Oversized local object.
    local
        .put_blob(&large_id, Bytes::from(vec![0x6b; large_len + 1]))
        .await
        .unwrap();
    assert_eq!(
        repl.get_blob_range_with_record(&large_rec, large_len as u64 - 32, None)
            .await
            .unwrap(),
        Some(Bytes::from(vec![0x6b; 32]))
    );
    assert_eq!(
        repl.get_blob_range_with_record(&large_rec, large_len as u64, None)
            .await
            .unwrap(),
        Some(Bytes::new())
    );
    assert_eq!(repl.stats().local_hits.load(Ordering::Relaxed), 0);

    // Local object truncated before the requested offset (local OutOfRange).
    let trunc_id = vec![0xf4; 20];
    let trunc_payload = Bytes::from(vec![0x7c; large_len]);
    local
        .put_blob(&trunc_id, Bytes::from(vec![0x7c; 64]))
        .await
        .unwrap();
    peer.put_blob(&trunc_id, trunc_payload).await.unwrap();
    let trunc_rec = BlobRecord {
        blob_id: trunc_id,
        content_size: large_len as u64,
        inline_content: None,
        primary_location: Some("gs://peer".to_string()),
        replicated_locations: vec!["gs://local".to_string(), "gs://peer".to_string()],
    };
    assert_eq!(
        repl.get_blob_range_with_record(&trunc_rec, large_len as u64 - 32, None)
            .await
            .unwrap(),
        Some(Bytes::from(vec![0x7c; 32]))
    );
    assert_eq!(
        repl.get_blob_range_with_record(&trunc_rec, large_len as u64, None)
            .await
            .unwrap(),
        Some(Bytes::new())
    );
    assert_eq!(repl.stats().local_hits.load(Ordering::Relaxed), 0);

    // No healthy peer (single local store with retry budget): returns Corruption immediately
    // with zero retries, zero local_hits, and zero cache insertions.
    let solo_local = Arc::new(ScriptedBlobStore::new("gs://solo-local"));
    let solo_repl = ReplicatedBlobStore::new(
        solo_local.clone(),
        vec![],
        metadata,
        ReplicatedBlobStoreConfig {
            cache_capacity_bytes: 64 * 1024,
            replication_retry_budget: Duration::from_millis(150),
            initial_retry_backoff: Duration::from_millis(10),
            async_write_fanout: false,
        },
    );
    let solo_bad_id = vec![0xf5; 20];
    solo_local
        .put_blob(&solo_bad_id, Bytes::from(vec![0x11; large_len + 1]))
        .await
        .unwrap();
    let solo_bad_rec = BlobRecord {
        blob_id: solo_bad_id.clone(),
        content_size: large_len as u64,
        inline_content: None,
        primary_location: Some("gs://solo-local".to_string()),
        replicated_locations: vec!["gs://solo-local".to_string()],
    };
    assert!(matches!(
        solo_repl
            .get_blob_range_with_record(&solo_bad_rec, large_len as u64 - 16, None)
            .await,
        Err(StoreError::Corruption(_))
    ));
    assert!(matches!(
        solo_repl
            .get_blob_range_with_record(&solo_bad_rec, large_len as u64, None)
            .await,
        Err(StoreError::Corruption(_))
    ));
    assert_eq!(solo_repl.stats().local_hits.load(Ordering::Relaxed), 0);
    assert_eq!(
        solo_repl
            .stats()
            .visibility_lag_retries
            .load(Ordering::Relaxed),
        0
    );
    assert!(
        !solo_repl
            .inner
            .cache
            .lock()
            .unwrap()
            .contains_key(&solo_bad_id)
    );
}

#[tokio::test]
async fn has_blob_handles_inline_without_peers_and_visibility_lag_retries() {
    let local = Arc::new(ScriptedBlobStore::new("gs://local"));
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    metadata.register_project("proj", None).await.unwrap();

    let inline_id = vec![0xf6; 20];
    let inline_payload = Bytes::from_static(b"inline with zero peers");
    metadata
        .put_blob_record(
            "proj",
            &BlobRecord {
                blob_id: inline_id.clone(),
                content_size: inline_payload.len() as u64,
                inline_content: Some(inline_payload.clone()),
                primary_location: None,
                replicated_locations: vec![],
            },
        )
        .await
        .unwrap();

    let corrupt_inline_id = vec![0xf7; 20];
    let corrupt_inline_rec = BlobRecord {
        blob_id: corrupt_inline_id.clone(),
        content_size: 99,
        inline_content: Some(Bytes::from_static(b"short")),
        primary_location: None,
        replicated_locations: vec![],
    };
    metadata
        .put_blob_record("proj", &corrupt_inline_rec)
        .await
        .unwrap();

    let lagging_id = vec![0xf8; 20];
    let lagging_payload = Bytes::from_static(b"visible after retry");
    let lagging_len = lagging_payload.len() as u64;
    local.put_blob(&lagging_id, lagging_payload).await.unwrap();
    metadata
        .put_blob_record(
            "proj",
            &BlobRecord {
                blob_id: lagging_id.clone(),
                content_size: lagging_len,
                inline_content: None,
                primary_location: Some("gs://local".to_string()),
                replicated_locations: vec!["gs://local".to_string()],
            },
        )
        .await
        .unwrap();

    let repl = ReplicatedBlobStore::new(
        local.clone(),
        vec![],
        metadata,
        ReplicatedBlobStoreConfig {
            cache_capacity_bytes: 0,
            replication_retry_budget: Duration::from_millis(500),
            initial_retry_backoff: Duration::from_millis(10),
            async_write_fanout: false,
        },
    );

    // Inline blob returns true even when no peers are configured.
    assert!(repl.has_blob(&inline_id).await.unwrap());
    assert_eq!(
        repl.get_blob(&inline_id).await.unwrap(),
        Some(inline_payload)
    );

    // Corrupt inline blob is rejected by both `has_blob` and `verify_eof_with_record`.
    assert!(matches!(
        repl.has_blob(&corrupt_inline_id).await,
        Err(StoreError::Corruption(_))
    ));
    assert!(matches!(
        repl.verify_eof_with_record(&corrupt_inline_rec).await,
        Err(StoreError::Corruption(_))
    ));

    // Metadata-confirmed lagging blob succeeds via VisibilityRetry.
    local.miss_first_n_reads.store(1, Ordering::SeqCst);
    assert!(repl.has_blob(&lagging_id).await.unwrap());
    assert_eq!(
        repl.stats().visibility_lag_retries.load(Ordering::Relaxed),
        1
    );

    // Unconfirmed missing blob fast-fails with zero retries.
    let retries_before = repl.stats().visibility_lag_retries.load(Ordering::Relaxed);
    assert!(!repl.has_blob(&[0xf9; 20]).await.unwrap());
    assert_eq!(
        repl.stats().visibility_lag_retries.load(Ordering::Relaxed),
        retries_before
    );
}

#[tokio::test]
async fn spawn_read_repair_deduplicates_before_checking_semaphore_saturation() {
    let local: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::with_location("gs://local"));
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    let repl = ReplicatedBlobStore::new(
        local,
        vec![],
        metadata,
        ReplicatedBlobStoreConfig::default(),
    );

    let blob_id = vec![0xfa; 20];
    repl.inner
        .repairs_in_flight
        .lock()
        .unwrap()
        .insert(blob_id.clone());
    let held_permits = repl
        .inner
        .bg_tasks
        .clone()
        .acquire_many_owned(DEFAULT_MAX_BACKGROUND_TASKS as u32)
        .await
        .unwrap();

    repl.inner
        .spawn_read_repair(blob_id.clone(), Bytes::from_static(b"duplicate repair"));

    assert_eq!(
        repl.stats()
            .read_repairs_deduplicated
            .load(Ordering::Relaxed),
        1
    );
    assert!(
        repl.inner
            .repairs_in_flight
            .lock()
            .unwrap()
            .contains(&blob_id)
    );
    drop(held_permits);
}

#[tokio::test]
async fn partial_read_of_uncacheable_small_blob_uses_ranged_read_not_full_fetch() {
    let local = Arc::new(ScriptedBlobStore::new("gs://local"));
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    let blob_id = vec![0xfb; 20];
    let payload = Bytes::from(vec![0x42; 64 * 1024]);
    let payload_len = payload.len();
    local.put_blob(&blob_id, payload).await.unwrap();
    let record = BlobRecord {
        blob_id,
        content_size: payload_len as u64,
        inline_content: None,
        primary_location: Some("gs://local".to_string()),
        replicated_locations: vec!["gs://local".to_string()],
    };

    for capacity in [0usize, 1024] {
        local.full_read_calls.store(0, Ordering::SeqCst);
        local.range_read_calls.store(0, Ordering::SeqCst);
        let repl = ReplicatedBlobStore::new(
            local.clone(),
            vec![],
            metadata.clone(),
            ReplicatedBlobStoreConfig {
                cache_capacity_bytes: capacity,
                ..ReplicatedBlobStoreConfig::default()
            },
        );

        let slice = repl
            .get_blob_range_with_record(&record, 128, Some(32))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(slice.len(), 32);
        assert_eq!(
            local.full_read_calls.load(Ordering::SeqCst),
            0,
            "partial read with cache_capacity_bytes={capacity} must not fetch full blob"
        );
        assert_eq!(local.range_read_calls.load(Ordering::SeqCst), 1);

        // Full reads still use singleflight (`get_blob`).
        let full = repl
            .get_blob_range_with_record(&record, 0, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(full.len(), payload_len);
        assert_eq!(local.full_read_calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn large_blob_ranged_reads_and_zero_capacity_cache_do_not_inflate_cache_misses() {
    let local = Arc::new(ScriptedBlobStore::new("gs://local"));
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    metadata.register_project("proj", None).await.unwrap();

    let large_id = vec![0xf1; 20];
    let large_len = SMALL_BLOB_MAX_BYTES as usize + 128 * 1024;
    let large_payload = Bytes::from(vec![0x42; large_len]);
    local.put_blob(&large_id, large_payload).await.unwrap();
    let large_rec = BlobRecord {
        blob_id: large_id,
        content_size: large_len as u64,
        inline_content: None,
        primary_location: Some("gs://local".to_string()),
        replicated_locations: vec!["gs://local".to_string()],
    };
    metadata.put_blob_record("proj", &large_rec).await.unwrap();

    // Enabled cache: ranged and full reads of a >SMALL_BLOB_MAX_BYTES blob must not
    // increment `cache_misses` because large blobs are never cached in RAM.
    let repl = ReplicatedBlobStore::new(
        local.clone(),
        vec![],
        metadata.clone(),
        ReplicatedBlobStoreConfig {
            cache_capacity_bytes: 2 * 1024 * 1024,
            ..Default::default()
        },
    );
    for chunk_idx in 0..4u64 {
        let offset = chunk_idx * 256 * 1024;
        let got = repl
            .get_blob_range_with_record(&large_rec, offset, Some(64 * 1024))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.len(), 64 * 1024);
    }
    let full = repl
        .get_blob_range_with_record(&large_rec, 0, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(full.len(), large_len);
    assert_eq!(repl.stats().cache_hits.load(Ordering::Relaxed), 0);
    assert_eq!(repl.stats().cache_misses.load(Ordering::Relaxed), 0);

    // Disabled cache (`cache_capacity_bytes == 0`): even small blobs must not record
    // cache misses.
    let small_id = vec![0xf2; 20];
    let small_payload = Bytes::from_static(b"small uncached blob");
    local
        .put_blob(&small_id, small_payload.clone())
        .await
        .unwrap();
    let small_rec = BlobRecord {
        blob_id: small_id,
        content_size: small_payload.len() as u64,
        inline_content: None,
        primary_location: Some("gs://local".to_string()),
        replicated_locations: vec!["gs://local".to_string()],
    };
    metadata.put_blob_record("proj", &small_rec).await.unwrap();

    let no_cache_repl = ReplicatedBlobStore::new(
        local,
        vec![],
        metadata,
        ReplicatedBlobStoreConfig {
            cache_capacity_bytes: 0,
            ..Default::default()
        },
    );
    assert_eq!(
        no_cache_repl
            .get_blob_range_with_record(&small_rec, 0, None)
            .await
            .unwrap(),
        Some(small_payload)
    );
    assert_eq!(no_cache_repl.stats().cache_hits.load(Ordering::Relaxed), 0);
    assert_eq!(
        no_cache_repl.stats().cache_misses.load(Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn small_blob_exceeding_cache_capacity_does_not_inflate_cache_misses() {
    let local = Arc::new(ScriptedBlobStore::new("gs://local"));
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
    metadata.register_project("proj", None).await.unwrap();

    let blob_id = vec![0xf3; 20];
    let payload = Bytes::from(vec![0x5a; 256]);
    local.put_blob(&blob_id, payload.clone()).await.unwrap();
    let record = BlobRecord {
        blob_id: blob_id.clone(),
        content_size: payload.len() as u64,
        inline_content: None,
        primary_location: Some("gs://local".to_string()),
        replicated_locations: vec!["gs://local".to_string()],
    };
    metadata.put_blob_record("proj", &record).await.unwrap();

    // Test both a capacity smaller than payload bytes and a capacity equal to payload bytes
    // (where key + LRU entry overhead still makes the blob uncacheable).
    let exact_cost = entry_cost_bytes(&blob_id, &payload);
    for capacity in [64usize, payload.len(), exact_cost.saturating_sub(1)] {
        let repl = ReplicatedBlobStore::new(
            local.clone(),
            vec![],
            metadata.clone(),
            ReplicatedBlobStoreConfig {
                cache_capacity_bytes: capacity,
                ..Default::default()
            },
        );
        for _ in 0..2 {
            assert_eq!(
                repl.get_blob_range_with_record(&record, 0, None)
                    .await
                    .unwrap(),
                Some(payload.clone())
            );
        }
        assert_eq!(
            repl.stats().cache_hits.load(Ordering::Relaxed),
            0,
            "capacity={capacity}"
        );
        assert_eq!(
            repl.stats().cache_misses.load(Ordering::Relaxed),
            0,
            "uncacheable blob with capacity={capacity} must not increment cache_misses"
        );
    }

    // At exact entry cost, the first read records 1 miss and the second records 1 hit.
    let fitting = ReplicatedBlobStore::new(
        local,
        vec![],
        metadata,
        ReplicatedBlobStoreConfig {
            cache_capacity_bytes: exact_cost,
            ..Default::default()
        },
    );
    assert_eq!(
        fitting
            .get_blob_range_with_record(&record, 0, None)
            .await
            .unwrap(),
        Some(payload.clone())
    );
    assert_eq!(
        fitting
            .get_blob_range_with_record(&record, 0, None)
            .await
            .unwrap(),
        Some(payload)
    );
    assert_eq!(fitting.stats().cache_misses.load(Ordering::Relaxed), 1);
    assert_eq!(fitting.stats().cache_hits.load(Ordering::Relaxed), 1);
}
