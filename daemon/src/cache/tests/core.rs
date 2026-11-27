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

use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use cc_common::backend::{Commit, ReadTreeResponse, TreeEntry};
use cc_common::op_store::{Operation, View};
use prost::Message;
use tokio::sync::watch;

use super::super::l1::{
    ByteBoundedLru, Flight, FlightOutcome, L1_ENTRY_OVERHEAD_BYTES, L1Key, L1Partition,
    l1_entry_weight,
};
use super::super::l2::l2_payload_is_valid;
use super::super::{
    CACHE_STREAM_CHUNK_BYTES, CacheRangeError, CacheScope, MAX_L1_OBJECT_BYTES,
    MAX_OBJECT_ID_BYTES, ObjectKind, TwoTierCache,
};
use super::{collect_range_chunks, git_blob_id, tree_payload, view_payload};

#[tokio::test]
async fn two_tier_cache_promotes_l2_to_l1_coalesces_singleflight_and_isolates_scopes() {
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    let digest = vec![0xab; 20];
    let scope_a = CacheScope::new("http://127.0.0.1:8080", "repo-alpha");
    let scope_b = CacheScope::new("http://127.0.0.1:8080", "repo-beta");

    let mut set = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let c = Arc::clone(&cache);
        let cnt = Arc::clone(&calls);
        let id = digest.clone();
        let sc = scope_a.clone();
        set.spawn(async move {
            c.get_or_fetch(&sc, ObjectKind::Tree, &id, || {
                let id_for_payload = id.clone();
                async move {
                    cnt.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    Ok(tree_payload(&id_for_payload, "tree-payload"))
                }
            })
            .await
            .unwrap()
        });
    }
    while let Some(res) = set.join_next().await {
        assert_eq!(res.unwrap(), tree_payload(&digest, "tree-payload"));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let st = cache.stats();
    assert_eq!(st.misses, 1);
    assert!(st.singleflight_coalesced >= 1);

    // Unauthorized repository `scope_b` must NOT hit `scope_a`'s cached payload without
    // upstream verification.
    assert!(
        cache
            .lookup_cached(&scope_b, ObjectKind::Tree, &digest)
            .is_none()
    );

    // Reopen over the same L2 directory with a fresh L1 to verify `scope_a` hits L2.
    let cold_l1_cache = TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap();
    assert!(
        cold_l1_cache
            .lookup_cached(&scope_b, ObjectKind::Tree, &digest)
            .is_none()
    );
    let hit = cold_l1_cache
        .get_or_fetch(&scope_a, ObjectKind::Tree, &digest, || async {
            panic!("should hit L2 disk CAS without calling upstream");
        })
        .await
        .unwrap();
    assert_eq!(hit, tree_payload(&digest, "tree-payload"));
    assert_eq!(cold_l1_cache.stats().l2_hits, 1);
}

#[tokio::test]
async fn cached_file_range_serves_partial_slices_across_l1_and_large_l2_blobs() {
    let dir = tempfile::tempdir().unwrap();
    let cache = TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap();
    let scope_a = CacheScope::new("http://127.0.0.1:8080", "repo-alpha");
    let scope_b = CacheScope::new("http://127.0.0.1:8080", "repo-beta");
    let payload: Vec<u8> = (0..(MAX_L1_OBJECT_BYTES + 8192))
        .map(|i| (i % 251) as u8)
        .collect();
    let large_id = git_blob_id(&payload);
    cache.put_write_through(
        &scope_a,
        ObjectKind::File,
        &large_id,
        Bytes::from(payload.clone()),
    );
    assert_eq!(cache.stats().l1_entries, 0, ">4 MiB blob stays in L2 only");
    assert_eq!(
        cache.blob_size(&scope_a, &large_id),
        Some(payload.len() as u64)
    );
    assert_eq!(cache.blob_size(&scope_b, &large_id), None);

    let rx = cache
        .read_cached_file_range(&scope_a, &large_id, 4 * 1024 * 1024, 4096)
        .await
        .unwrap()
        .expect("present in L2");
    let slice_chunks = collect_range_chunks(rx).await.unwrap();
    assert!(
        slice_chunks
            .iter()
            .all(|c| !c.is_empty() && c.len() <= CACHE_STREAM_CHUNK_BYTES)
    );
    let joined: Vec<u8> = slice_chunks.into_iter().flatten().collect();
    assert_eq!(joined, &payload[4 * 1024 * 1024..4 * 1024 * 1024 + 4096]);

    // Also verify L1 streaming yields bounded frames matching a sub-slice.
    let small: Vec<u8> = (0..(CACHE_STREAM_CHUNK_BYTES * 2 + 17))
        .map(|i| (i % 251) as u8)
        .collect();
    let small_id = git_blob_id(&small);
    cache.put_write_through(
        &scope_a,
        ObjectKind::File,
        &small_id,
        Bytes::from(small.clone()),
    );
    let l1_rx = cache
        .read_cached_file_range(
            &scope_a,
            &small_id,
            100,
            (CACHE_STREAM_CHUNK_BYTES + 500) as u64,
        )
        .await
        .unwrap()
        .expect("present in L1");
    let l1_chunks = collect_range_chunks(l1_rx).await.unwrap();
    assert_eq!(l1_chunks.len(), 2);
    assert_eq!(l1_chunks[0].len(), CACHE_STREAM_CHUNK_BYTES);
    assert_eq!(l1_chunks[1].len(), 500);
    let l1_joined: Vec<u8> = l1_chunks.into_iter().flatten().collect();
    assert_eq!(l1_joined, &small[100..100 + CACHE_STREAM_CHUNK_BYTES + 500]);
}

#[tokio::test]
async fn singleflight_waiter_sees_result_published_before_it_starts_waiting() {
    let dir = tempfile::tempdir().unwrap();
    let cache = TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap();
    let scope = CacheScope::new("http://127.0.0.1:8080", "repo-alpha");
    let id = vec![0x11; 20];
    let key = (
        cache.purge_epoch(),
        scope.scope_hex.clone(),
        ObjectKind::Tree,
        id.clone(),
    );

    let Flight::Leader(leader) = cache.join_flight(&key) else {
        panic!("first caller leads")
    };
    let Flight::Waiter(mut waiter) = cache.join_flight(&key) else {
        panic!("second waits")
    };
    // The leader finishes after the waiter joined but before it starts waiting. A waiter
    // parked on an edge-triggered `Notify` lost its only wakeup in this window.
    leader.publish(&tree_payload(&id, "published"));
    drop(leader);
    let seen = tokio::time::timeout(Duration::from_secs(5), waiter.wait_for(Option::is_some))
        .await
        .expect("waiter must not hang")
        .map(|v| (*v).clone())
        .unwrap();
    assert!(matches!(
        seen,
        Some(FlightOutcome::Ok(ref bytes)) if bytes == &tree_payload(&id, "published")
    ));
    assert!(cache.inner.inflight.lock().unwrap().is_empty());

    // A leader that fails publishes its error to coalesced waiters; a leader that is dropped
    // before publishing closes the channel so surviving waiters retry.
    let Flight::Leader(err_leader) = cache.join_flight(&key) else {
        panic!("error fetch leads")
    };
    let Flight::Waiter(mut err_waiter) = cache.join_flight(&key) else {
        panic!("second waits")
    };
    err_leader.publish_err(&tonic::Status::not_found("missing"));
    drop(err_leader);
    let err_seen =
        tokio::time::timeout(Duration::from_secs(5), err_waiter.wait_for(Option::is_some))
            .await
            .expect("error waiter must not hang")
            .map(|v| (*v).clone())
            .unwrap();
    assert!(matches!(
        err_seen,
        Some(FlightOutcome::Err(ref st))
            if st.code() == tonic::Code::NotFound && st.message() == "missing"
    ));

    let Flight::Leader(cancelled) = cache.join_flight(&key) else {
        panic!("new fetch leads")
    };
    let Flight::Waiter(mut orphan) = cache.join_flight(&key) else {
        panic!("second waits")
    };
    drop(cancelled);
    assert!(orphan.wait_for(Option::is_some).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn singleflight_never_strands_waiters_on_multi_thread_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap());
    let scope = CacheScope::new("http://127.0.0.1:8080", "repo-alpha");
    let calls = Arc::new(AtomicUsize::new(0));
    let rounds = async {
        for round in 0u32..500 {
            let id = round.to_be_bytes().repeat(5);
            let expected = tree_payload(&id, "payload");
            let mut set = tokio::task::JoinSet::new();
            for _ in 0..16 {
                let (cache, scope, id) = (Arc::clone(&cache), scope.clone(), id.clone());
                let calls = Arc::clone(&calls);
                set.spawn(async move {
                    let id_for_payload = id.clone();
                    cache
                        .get_or_fetch(&scope, ObjectKind::Tree, &id, || async move {
                            calls.fetch_add(1, Ordering::SeqCst);
                            tokio::task::yield_now().await;
                            Ok(tree_payload(&id_for_payload, "payload"))
                        })
                        .await
                });
            }
            while let Some(res) = set.join_next().await {
                assert_eq!(res.unwrap().unwrap(), expected);
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(120), rounds)
        .await
        .expect("a singleflight waiter never completed");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        500,
        "one upstream fetch per object"
    );
}

#[tokio::test]
async fn singleflight_waiters_retry_with_their_own_fetch_after_leader_failure() {
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap());
    let scope = CacheScope::new("http://127.0.0.1:8080", "repo-alpha");
    let id = vec![0x22; 20];
    let key = (
        cache.purge_epoch(),
        scope.scope_hex.clone(),
        ObjectKind::Tree,
        id.clone(),
    );

    let Flight::Leader(leader) = cache.join_flight(&key) else {
        panic!("test leads")
    };
    let waiter = tokio::spawn({
        let (cache, scope, id) = (Arc::clone(&cache), scope.clone(), id.clone());
        async move {
            let id_for_payload = id.clone();
            cache
                .get_or_fetch(&scope, ObjectKind::Tree, &id, || async move {
                    Ok(tree_payload(&id_for_payload, "retried"))
                })
                .await
        }
    });
    while cache.stats().singleflight_coalesced == 0 {
        tokio::task::yield_now().await;
    }
    drop(leader);
    assert_eq!(waiter.await.unwrap().unwrap(), tree_payload(&id, "retried"));
    assert_eq!(cache.stats().misses, 1);
}

#[tokio::test]
async fn shared_cas_entries_are_partitioned_per_upstream() {
    let dir = tempfile::tempdir().unwrap();
    let cache = TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap();
    let scope_a = CacheScope::new("http://server-a:8080", "repo");
    let scope_b = CacheScope::new("http://server-b:8080/", "repo");
    let tree_id = vec![0x42; 20];

    cache.put_write_through(
        &scope_a,
        ObjectKind::Tree,
        &tree_id,
        tree_payload(&tree_id, "from-a"),
    );
    // Server B proves reachability by serving the id itself and must keep its own bytes.
    let from_b = cache
        .get_or_fetch(&scope_b, ObjectKind::Tree, &tree_id, || async {
            Ok(tree_payload(&tree_id, "from-b"))
        })
        .await
        .unwrap();
    assert_eq!(from_b, tree_payload(&tree_id, "from-b"));
    assert_eq!(
        cache
            .lookup_cached(&scope_a, ObjectKind::Tree, &tree_id)
            .unwrap(),
        tree_payload(&tree_id, "from-a")
    );

    let restarted = TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap();
    assert_eq!(
        restarted.lookup_cached(&scope_a, ObjectKind::Tree, &tree_id),
        Some(tree_payload(&tree_id, "from-a"))
    );
    assert_eq!(
        restarted.lookup_cached(&scope_b, ObjectKind::Tree, &tree_id),
        Some(tree_payload(&tree_id, "from-b"))
    );
}

#[tokio::test]
async fn empty_or_undecodable_l2_entries_are_discarded_and_refetched() {
    let dir = tempfile::tempdir().unwrap();
    let scope = CacheScope::new("http://127.0.0.1:8080", "repo-alpha");
    let tree_id = vec![0x5a; 20];
    let cache = TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap();
    cache.put_write_through(
        &scope,
        ObjectKind::Tree,
        &tree_id,
        tree_payload(&tree_id, "good"),
    );
    let path = cache.object_path(&scope, ObjectKind::Tree, &tree_id);

    for torn in [&b""[..], &[0u8; 64][..]] {
        fs::write(&path, torn).unwrap();
        let restarted = TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap();
        assert_eq!(
            restarted.lookup_cached(&scope, ObjectKind::Tree, &tree_id),
            None
        );
        assert!(!path.exists(), "invalid entry should be deleted");
        let refetched = restarted
            .get_or_fetch(&scope, ObjectKind::Tree, &tree_id, || async {
                Ok(tree_payload(&tree_id, "good"))
            })
            .await
            .unwrap();
        assert_eq!(refetched, tree_payload(&tree_id, "good"));
        assert!(path.exists(), "refetched entry should be rewritten");
    }
}

#[tokio::test]
async fn file_payloads_are_cached_only_under_their_git_blob_id() {
    let dir = tempfile::tempdir().unwrap();
    let cache = TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap();
    let scope = CacheScope::new("http://127.0.0.1:8080", "repo-alpha");
    let content = Bytes::from_static(b"hello\n");
    let id = git_blob_id(&content);
    assert_eq!(hex::encode(&id), "ce013625030ba8dba906f756967f9e9ca394464a");
    let forged = Bytes::from_static(b"forged\n");

    cache.put_write_through(&scope, ObjectKind::File, &id, forged.clone());
    assert_eq!(cache.blob_size(&scope, &id), None);
    let fetched = cache
        .get_or_fetch(&scope, ObjectKind::File, &id, || async {
            Ok(forged.clone())
        })
        .await
        .unwrap();
    assert_eq!(
        fetched, forged,
        "the caller still receives the upstream bytes"
    );
    assert!(!cache.object_path(&scope, ObjectKind::File, &id).exists());

    cache.put_write_through(&scope, ObjectKind::File, &id, content.clone());
    assert_eq!(cache.blob_size(&scope, &id), Some(content.len() as u64));

    // An empty L2 entry is valid only for the empty blob.
    let empty_id = git_blob_id(b"");
    cache.put_write_through(&scope, ObjectKind::File, &empty_id, Bytes::new());
    let restarted = TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap();
    assert_eq!(restarted.blob_size(&scope, &empty_id), Some(0));
    let empty_rx = restarted
        .read_cached_file_range(&scope, &empty_id, 0, 0)
        .await
        .unwrap()
        .expect("empty blob cached");
    assert!(collect_range_chunks(empty_rx).await.unwrap().is_empty());
    fs::write(restarted.object_path(&scope, ObjectKind::File, &id), b"").unwrap();
    assert_eq!(restarted.blob_size(&scope, &id), None);
    assert!(
        restarted
            .read_cached_file_range(&scope, &id, 0, 0)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn failed_l2_writes_are_counted_and_do_not_authorize_uncached_blobs() {
    let dir = tempfile::tempdir().unwrap();
    // L1 is too small for the payload, so after the fetch it is in neither tier.
    let cache = TwoTierCache::new(dir.path().to_path_buf(), 4).unwrap();
    let scope = CacheScope::new("http://127.0.0.1:8080", "repo-alpha");
    // A regular file where the upstream partition belongs makes every L2 write fail.
    fs::write(dir.path().join("objects").join(&scope.upstream_hex), b"").unwrap();
    let content = Bytes::from_static(b"file contents larger than L1\n");
    let id = git_blob_id(&content);

    let fetched = cache
        .get_or_fetch(&scope, ObjectKind::File, &id, || async {
            Ok(content.clone())
        })
        .await
        .unwrap();
    assert_eq!(fetched, content);
    assert_eq!(cache.stats().l2_write_errors, 1);
    assert!(
        !cache.is_authorized(&scope, ObjectKind::File, &id),
        "scope must not be authorized when neither L2 nor L1 retained the object"
    );
    assert!(
        cache
            .read_cached_file_range(&scope, &id, 0, 0)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn caller_auth_fingerprint_isolates_scopes_views_and_singleflight() {
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap());

    let alice = CacheScope::new_with_auth(
        "https://CommitCloud.Example.COM:443/",
        "proj-1",
        Some("Bearer alice-secret"),
    );
    let alice_same_scheme_case = CacheScope::new_with_auth(
        "https://commitcloud.example.com",
        "proj-1",
        Some("  bearer   alice-secret "),
    );
    let bob = CacheScope::new_with_auth(
        "https://commitcloud.example.com",
        "proj-1",
        Some("Bearer bob-secret"),
    );
    let unauth = CacheScope::new("https://commitcloud.example.com", "proj-1");

    assert_eq!(alice, alice_same_scheme_case);
    assert_ne!(alice.scope_hex(), bob.scope_hex());
    assert_ne!(alice.scope_hex(), unauth.scope_hex());
    assert_ne!(bob.scope_hex(), unauth.scope_hex());
    assert_eq!(
        alice.upstream_hex, bob.upstream_hex,
        "shared CAS partition still deduplicates bytes per normalized upstream"
    );
    assert!(
        !format!("{alice:?}").contains("alice-secret"),
        "Debug output must never expose caller credentials"
    );

    // Concurrent fetches with different caller tokens must not coalesce via singleflight.
    let tree_id = vec![0x77; 20];
    let payload = tree_payload(&tree_id, "shared");
    let calls = Arc::new(AtomicUsize::new(0));
    let (tx_a_started, mut rx_a_started) = watch::channel(false);
    let (tx_a_release, mut rx_a_release) = watch::channel(false);

    let task_a = tokio::spawn({
        let cache = Arc::clone(&cache);
        let alice = alice.clone();
        let tree_id = tree_id.clone();
        let payload = payload.clone();
        let calls = Arc::clone(&calls);
        async move {
            cache
                .get_or_fetch(&alice, ObjectKind::Tree, &tree_id, || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let _ = tx_a_started.send(true);
                    let _ = rx_a_release.wait_for(|v| *v).await;
                    Ok(payload)
                })
                .await
        }
    });

    rx_a_started.wait_for(|v| *v).await.unwrap();
    let err_bob = cache
        .get_or_fetch(&bob, ObjectKind::Tree, &tree_id, || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Err(tonic::Status::permission_denied("revoked"))
        })
        .await
        .unwrap_err();
    assert_eq!(err_bob.code(), tonic::Code::PermissionDenied);

    let _ = tx_a_release.send(true);
    assert_eq!(task_a.await.unwrap().unwrap(), payload);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(cache.stats().singleflight_coalesced, 0);

    // Bob and unauthenticated callers still cannot read Alice's cached tree or View.
    assert_eq!(
        cache.lookup_cached(&alice_same_scheme_case, ObjectKind::Tree, &tree_id),
        Some(payload)
    );
    assert_eq!(cache.lookup_cached(&bob, ObjectKind::Tree, &tree_id), None);
    assert_eq!(
        cache.lookup_cached(&unauth, ObjectKind::Tree, &tree_id),
        None
    );

    let view_id = vec![0x88; 16];
    cache.put_write_through(&alice, ObjectKind::View, &view_id, view_payload(1));
    assert_eq!(
        cache.lookup_cached(&alice, ObjectKind::View, &view_id),
        Some(view_payload(1))
    );
    assert_eq!(cache.lookup_cached(&bob, ObjectKind::View, &view_id), None);
    assert_eq!(
        cache.lookup_cached(&unauth, ObjectKind::View, &view_id),
        None
    );
}

#[tokio::test]
async fn empty_oversized_and_directory_paths_are_rejected_across_public_methods() {
    let dir = tempfile::tempdir().unwrap();
    let cache = TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap();
    let scope = CacheScope::new("http://127.0.0.1:8080", "repo-alpha");
    let oversized = vec![0xaa; MAX_OBJECT_ID_BYTES + 1];

    // Pre-create `00/00` shard directories so empty-id paths resolve to existing directories.
    let short_id = vec![0x00];
    cache.put_write_through(
        &scope,
        ObjectKind::File,
        &short_id,
        Bytes::from_static(b"x"),
    );

    for bad_id in [&b""[..], oversized.as_slice()] {
        cache.authorize_scope(&scope, ObjectKind::File, bad_id);
        assert!(!cache.is_authorized(&scope, ObjectKind::File, bad_id));
        assert!(!cache.is_authorized(&scope, ObjectKind::View, bad_id));
        assert_eq!(cache.blob_size(&scope, bad_id), None);
        let before_wt = cache.stats().write_through_objects;
        cache.put_write_through(&scope, ObjectKind::File, bad_id, Bytes::from_static(b"a"));
        assert_eq!(cache.stats().write_through_objects, before_wt);
        assert_eq!(cache.lookup_cached(&scope, ObjectKind::File, bad_id), None);
        assert!(
            cache
                .read_cached_file_range(&scope, bad_id, 0, 0)
                .await
                .unwrap()
                .is_none()
        );
        let err = cache
            .get_or_fetch(&scope, ObjectKind::File, bad_id, || async {
                panic!("fetch_fn must not run for invalid object id");
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    // Directories planted at valid marker or blob paths must never pass `.is_file()` checks.
    let valid_id = vec![0x33; 20];
    let marker_path = cache.inner.scoped_path(&scope, ObjectKind::File, &valid_id);
    fs::create_dir_all(&marker_path).unwrap();
    assert!(!cache.is_authorized(&scope, ObjectKind::File, &valid_id));

    let dir_blob_id = vec![0x44; 20];
    cache.authorize_scope(&scope, ObjectKind::File, &dir_blob_id);
    let blob_path = cache.object_path(&scope, ObjectKind::File, &dir_blob_id);
    fs::create_dir_all(&blob_path).unwrap();
    assert_eq!(cache.blob_size(&scope, &dir_blob_id), None);
    assert!(
        cache
            .read_cached_file_range(&scope, &dir_blob_id, 0, 0)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn validation_rejects_nul_symlinks_partial_trees_and_mismatched_ids() {
    let dir = tempfile::tempdir().unwrap();
    let cache = TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap();
    let scope = CacheScope::new("http://127.0.0.1:8080", "repo-alpha");

    // Symlink with an embedded NUL byte must be rejected even with a matching Git blob SHA-1.
    let nul_target = Bytes::from_static(b"dir/target\0evil");
    let nul_id = git_blob_id(&nul_target);
    cache.put_write_through(&scope, ObjectKind::Symlink, &nul_id, nul_target);
    assert!(!cache.is_authorized(&scope, ObjectKind::Symlink, &nul_id));
    assert!(
        !cache
            .object_path(&scope, ObjectKind::Symlink, &nul_id)
            .exists()
    );

    // Paginated partial tree (`next_page_token` non-empty) must never be cached.
    let tree_id = vec![0x66; 20];
    let partial_tree = Bytes::from(
        ReadTreeResponse {
            tree_id: tree_id.clone(),
            entries: Vec::new(),
            next_page_token: "cursor-2".to_string(),
        }
        .encode_to_vec(),
    );
    cache.put_write_through(&scope, ObjectKind::Tree, &tree_id, partial_tree);
    assert!(!cache.is_authorized(&scope, ObjectKind::Tree, &tree_id));
    assert!(
        !cache
            .object_path(&scope, ObjectKind::Tree, &tree_id)
            .exists()
    );

    // Mismatched or empty `tree_id` must be rejected.
    let wrong_tree = Bytes::from(
        ReadTreeResponse {
            tree_id: vec![0x99; 20],
            entries: Vec::new(),
            next_page_token: String::new(),
        }
        .encode_to_vec(),
    );
    cache.put_write_through(&scope, ObjectKind::Tree, &tree_id, wrong_tree);
    assert!(!cache.is_authorized(&scope, ObjectKind::Tree, &tree_id));

    let empty_id_tree = Bytes::from(
        ReadTreeResponse {
            tree_id: Vec::new(),
            entries: vec![TreeEntry {
                name: "orphan".to_string(),
                value: None,
            }],
            next_page_token: String::new(),
        }
        .encode_to_vec(),
    );
    assert!(!l2_payload_is_valid(
        ObjectKind::Tree,
        &tree_id,
        &empty_id_tree
    ));
    cache.put_write_through(&scope, ObjectKind::Tree, &tree_id, empty_id_tree);
    assert!(!cache.is_authorized(&scope, ObjectKind::Tree, &tree_id));

    // Mismatched `commit_id`, missing `change_id`, or empty/even/empty-term `root_tree_id`
    // must be rejected, while matching or empty `commit_id` with valid `change_id` and
    // odd-length `root_tree_id` is accepted.
    let commit_id = vec![0x55; 20];
    let invalid_commits = [
        // Mismatched `commit_id`
        Commit {
            commit_id: vec![0x99; 20],
            change_id: vec![0x12; 16],
            root_tree_id: vec![vec![0x34; 20]],
            description: "wrong id".to_string(),
            ..Default::default()
        },
        // Missing `change_id`
        Commit {
            commit_id: commit_id.clone(),
            change_id: Vec::new(),
            root_tree_id: vec![vec![0x34; 20]],
            description: "missing change_id".to_string(),
            ..Default::default()
        },
        // Empty `root_tree_id`
        Commit {
            commit_id: commit_id.clone(),
            change_id: vec![0x12; 16],
            root_tree_id: Vec::new(),
            description: "empty root_tree_id".to_string(),
            ..Default::default()
        },
        // Even-length `root_tree_id`
        Commit {
            commit_id: commit_id.clone(),
            change_id: vec![0x12; 16],
            root_tree_id: vec![vec![0x34; 20], vec![0x35; 20]],
            description: "even root_tree_id".to_string(),
            ..Default::default()
        },
        // Odd-length `root_tree_id` containing an empty term
        Commit {
            commit_id: commit_id.clone(),
            change_id: vec![0x12; 16],
            root_tree_id: vec![Vec::new()],
            description: "empty tree term".to_string(),
            ..Default::default()
        },
    ];
    for bad in invalid_commits {
        let encoded = Bytes::from(bad.encode_to_vec());
        assert!(!l2_payload_is_valid(
            ObjectKind::Commit,
            &commit_id,
            &encoded
        ));
        cache.put_write_through(&scope, ObjectKind::Commit, &commit_id, encoded);
        assert_eq!(
            cache.lookup_cached(&scope, ObjectKind::Commit, &commit_id),
            None
        );
        assert!(
            !cache
                .object_path(&scope, ObjectKind::Commit, &commit_id)
                .exists()
        );
    }

    let valid_commit = Bytes::from(
        Commit {
            commit_id: commit_id.clone(),
            change_id: vec![0x12; 16],
            root_tree_id: vec![vec![0x34; 20]],
            description: "ok".to_string(),
            ..Default::default()
        }
        .encode_to_vec(),
    );
    cache.put_write_through(&scope, ObjectKind::Commit, &commit_id, valid_commit.clone());
    assert_eq!(
        cache.lookup_cached(&scope, ObjectKind::Commit, &commit_id),
        Some(valid_commit)
    );

    let conflict_commit_id = vec![0x56; 20];
    let valid_conflict_commit = Bytes::from(
        Commit {
            commit_id: Vec::new(),
            change_id: vec![0x13; 16],
            root_tree_id: vec![vec![0x34; 20], vec![0x35; 20], vec![0x36; 20]],
            description: "3-term conflict".to_string(),
            ..Default::default()
        }
        .encode_to_vec(),
    );
    cache.put_write_through(
        &scope,
        ObjectKind::Commit,
        &conflict_commit_id,
        valid_conflict_commit.clone(),
    );
    assert_eq!(
        cache.lookup_cached(&scope, ObjectKind::Commit, &conflict_commit_id),
        Some(valid_conflict_commit)
    );

    // Views without `head_ids` and Operations without `view_id` must be rejected.
    let view_id = vec![0x71; 16];
    let empty_heads_view = Bytes::from(
        View {
            head_ids: Vec::new(),
            wc_commit_ids: [("default".to_string(), vec![0x01; 20])]
                .into_iter()
                .collect(),
            ..Default::default()
        }
        .encode_to_vec(),
    );
    assert!(!l2_payload_is_valid(
        ObjectKind::View,
        &view_id,
        &empty_heads_view
    ));
    cache.put_write_through(&scope, ObjectKind::View, &view_id, empty_heads_view);
    assert_eq!(
        cache.lookup_cached(&scope, ObjectKind::View, &view_id),
        None
    );
    assert!(
        !cache
            .object_path(&scope, ObjectKind::View, &view_id)
            .exists()
    );

    let op_id = vec![0x72; 16];
    let empty_view_op = Bytes::from(
        Operation {
            view_id: Vec::new(),
            parents: vec![vec![0x00; 16]],
            ..Default::default()
        }
        .encode_to_vec(),
    );
    assert!(!l2_payload_is_valid(
        ObjectKind::Operation,
        &op_id,
        &empty_view_op
    ));
    cache.put_write_through(&scope, ObjectKind::Operation, &op_id, empty_view_op);
    assert_eq!(
        cache.lookup_cached(&scope, ObjectKind::Operation, &op_id),
        None
    );
    assert!(
        !cache
            .object_path(&scope, ObjectKind::Operation, &op_id)
            .exists()
    );

    let valid_op = Bytes::from(
        Operation {
            view_id: view_id.clone(),
            parents: vec![vec![0x00; 16]],
            ..Default::default()
        }
        .encode_to_vec(),
    );
    cache.put_write_through(&scope, ObjectKind::Operation, &op_id, valid_op.clone());
    assert_eq!(
        cache.lookup_cached(&scope, ObjectKind::Operation, &op_id),
        Some(valid_op)
    );
}

#[test]
fn byte_bounded_lru_enforces_zero_budget_and_key_overhead() {
    let mut zero_lru = ByteBoundedLru::new(0);
    let key: L1Key = (
        L1Partition::Shared {
            upstream_hex: "u".repeat(64),
        },
        ObjectKind::File,
        vec![0x01; 20],
    );
    assert!(!zero_lru.put(key.clone(), Bytes::new()));
    assert_eq!(zero_lru.map.len(), 0);
    assert_eq!(zero_lru.current_bytes, 0);

    let entry_weight = l1_entry_weight(&key, &Bytes::new());
    assert_eq!(entry_weight, 64 + 20 + L1_ENTRY_OVERHEAD_BYTES);
    // Budget fits exactly one empty-payload entry, so inserting a second must evict the first.
    let mut tight_lru = ByteBoundedLru::new(entry_weight);
    let key2: L1Key = (
        L1Partition::Shared {
            upstream_hex: "u".repeat(64),
        },
        ObjectKind::File,
        vec![0x02; 20],
    );
    assert!(tight_lru.put(key.clone(), Bytes::new()));
    assert_eq!(tight_lru.map.len(), 1);
    assert_eq!(tight_lru.current_bytes, entry_weight);

    assert!(tight_lru.put(key2.clone(), Bytes::new()));
    assert_eq!(tight_lru.map.len(), 1);
    assert_eq!(tight_lru.current_bytes, entry_weight);
    assert!(tight_lru.get(&key).is_none());
    assert!(tight_lru.get(&key2).is_some());
}

#[test]
fn overwriting_l1_entry_with_oversized_payload_evicts_stale_l1_bytes() {
    // Overwriting an L1-cached Commit with a payload exceeding `max_bytes` evicts the stale L1
    // entry so subsequent lookups return the updated L2 bytes.
    let dir = tempfile::tempdir().unwrap();
    let cache = TwoTierCache::new(dir.path().to_path_buf(), 512).unwrap();
    let scope = CacheScope::new("http://127.0.0.1:8080", "repo-alpha");
    let commit_id = vec![0xd1; 20];

    let small_commit = Bytes::from(
        Commit {
            commit_id: commit_id.clone(),
            change_id: vec![0x10; 16],
            root_tree_id: vec![vec![0x20; 20]],
            description: "small".to_string(),
            ..Default::default()
        }
        .encode_to_vec(),
    );
    let oversized_commit = Bytes::from(
        Commit {
            commit_id: commit_id.clone(),
            change_id: vec![0x10; 16],
            root_tree_id: vec![vec![0x20; 20]],
            description: "x".repeat(1024),
            ..Default::default()
        }
        .encode_to_vec(),
    );

    cache.put_write_through(&scope, ObjectKind::Commit, &commit_id, small_commit.clone());
    assert_eq!(cache.stats().l1_entries, 1);
    assert!(cache.stats().l1_bytes > 0);
    assert_eq!(
        cache.lookup_cached(&scope, ObjectKind::Commit, &commit_id),
        Some(small_commit)
    );

    cache.put_write_through(
        &scope,
        ObjectKind::Commit,
        &commit_id,
        oversized_commit.clone(),
    );
    assert_eq!(
        cache.stats().l1_entries,
        0,
        "oversized overwrite must pop stale L1 entry"
    );
    assert_eq!(
        cache.stats().l1_bytes,
        0,
        "oversized overwrite must reclaim stale L1 byte accounting"
    );
    assert_eq!(
        cache.lookup_cached(&scope, ObjectKind::Commit, &commit_id),
        Some(oversized_commit)
    );

    // Overwriting an L1-cached Tree with a payload exceeding `MAX_L1_OBJECT_BYTES` (even when
    // `max_bytes` is larger) also evicts the stale L1 entry.
    let large_dir = tempfile::tempdir().unwrap();
    let large_budget_cache =
        TwoTierCache::new(large_dir.path().to_path_buf(), MAX_L1_OBJECT_BYTES * 2).unwrap();
    let tree_id = vec![0xd2; 20];
    let small_tree = tree_payload(&tree_id, "small-entry");
    let huge_tree = tree_payload(&tree_id, &"y".repeat(MAX_L1_OBJECT_BYTES + 128));
    assert!(huge_tree.len() > MAX_L1_OBJECT_BYTES);

    large_budget_cache.put_write_through(&scope, ObjectKind::Tree, &tree_id, small_tree);
    assert_eq!(large_budget_cache.stats().l1_entries, 1);

    large_budget_cache.put_write_through(&scope, ObjectKind::Tree, &tree_id, huge_tree.clone());
    assert_eq!(large_budget_cache.stats().l1_entries, 0);
    assert_eq!(large_budget_cache.stats().l1_bytes, 0);
    assert_eq!(
        large_budget_cache.lookup_cached(&scope, ObjectKind::Tree, &tree_id),
        Some(huge_tree)
    );
}

#[tokio::test]
async fn read_cached_file_range_returns_typed_offset_out_of_range_error() {
    let dir = tempfile::tempdir().unwrap();
    let cache = TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap();
    let scope = CacheScope::new("http://127.0.0.1:8080", "repo-alpha");

    // L1 hit out-of-range
    let small = Bytes::from_static(b"abcdef");
    let small_id = git_blob_id(&small);
    cache.put_write_through(&scope, ObjectKind::File, &small_id, small.clone());
    let eof_rx = cache
        .read_cached_file_range(&scope, &small_id, small.len() as u64, 16)
        .await
        .unwrap()
        .expect("empty stream at EOF");
    assert!(collect_range_chunks(eof_rx).await.unwrap().is_empty());
    let err = cache
        .read_cached_file_range(&scope, &small_id, 100, 16)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        CacheRangeError::OffsetOutOfRange {
            offset: 100,
            total_len: 6
        }
    ));
    assert!(err.to_string().contains("100"));

    // Cold L1 (small L2 hit) out-of-range
    let restarted = TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap();
    let err = restarted
        .read_cached_file_range(&scope, &small_id, 7, 1)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        CacheRangeError::OffsetOutOfRange {
            offset: 7,
            total_len: 6
        }
    ));

    // Large (>4 MiB) L2-only hit out-of-range
    let large: Vec<u8> = (0..(MAX_L1_OBJECT_BYTES + 128))
        .map(|i| (i % 251) as u8)
        .collect();
    let large_id = git_blob_id(&large);
    cache.put_write_through(
        &scope,
        ObjectKind::File,
        &large_id,
        Bytes::from(large.clone()),
    );
    let err = cache
        .read_cached_file_range(&scope, &large_id, (large.len() as u64) + 1, 16)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        CacheRangeError::OffsetOutOfRange { offset, total_len }
            if offset == (large.len() as u64) + 1 && total_len == large.len() as u64
    ));
    assert_eq!(
        cache.stats().l2_hits,
        1,
        "large L2 out-of-range check should count as an L2 hit matching small L2 and L1 hits"
    );
}

#[test]
fn auth_fingerprint_splits_strictly_on_space_and_normalizes_bearer_only() {
    let bearer_standard = CacheScope::new_with_auth(
        "https://commitcloud.example.com",
        "proj-1",
        Some("Bearer tok-123"),
    );
    let bearer_case_and_spaces = CacheScope::new_with_auth(
        "https://commitcloud.example.com",
        "proj-1",
        Some("  bearer   tok-123 "),
    );
    let bearer_tab_sep = CacheScope::new_with_auth(
        "https://commitcloud.example.com",
        "proj-1",
        Some("Bearer\ttok-123"),
    );
    let bearer_space_tab = CacheScope::new_with_auth(
        "https://commitcloud.example.com",
        "proj-1",
        Some("Bearer \ttok-123"),
    );
    let basic_upper = CacheScope::new_with_auth(
        "https://commitcloud.example.com",
        "proj-1",
        Some("Basic tok-123"),
    );
    let basic_lower = CacheScope::new_with_auth(
        "https://commitcloud.example.com",
        "proj-1",
        Some("basic tok-123"),
    );

    assert_eq!(bearer_standard, bearer_case_and_spaces);
    assert_ne!(
        bearer_standard.scope_hex(),
        bearer_tab_sep.scope_hex(),
        "tab-separated Bearer header must not collide with valid space-separated Bearer header"
    );
    assert_ne!(
        bearer_standard.scope_hex(),
        bearer_space_tab.scope_hex(),
        "leading tab before token must not collide with space-separated Bearer header"
    );
    assert_ne!(
        basic_upper.scope_hex(),
        basic_lower.scope_hex(),
        "non-Bearer schemes must not be case-normalized"
    );
}

#[tokio::test]
async fn commits_are_scope_partitioned_across_projects_and_callers() {
    let dir = tempfile::tempdir().unwrap();
    let cache = TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap();
    let scope_a = CacheScope::new_with_auth(
        "https://commitcloud.example.com",
        "proj-a",
        Some("Bearer a"),
    );
    let scope_b = CacheScope::new_with_auth(
        "https://commitcloud.example.com",
        "proj-b",
        Some("Bearer a"),
    );
    let scope_a_other_user = CacheScope::new_with_auth(
        "https://commitcloud.example.com",
        "proj-a",
        Some("Bearer b"),
    );

    let commit_id = vec![0xc1; 20];
    let commit_a = Bytes::from(
        Commit {
            commit_id: commit_id.clone(),
            change_id: vec![0x01; 16],
            root_tree_id: vec![vec![0x02; 20]],
            description: "same git commit".to_string(),
            predecessors: vec![vec![0x11; 20]],
            external_ids: [("git-sha1".to_string(), vec![0xaa; 20])]
                .into_iter()
                .collect(),
            ..Default::default()
        }
        .encode_to_vec(),
    );
    let commit_b = Bytes::from(
        Commit {
            commit_id: commit_id.clone(),
            change_id: vec![0x01; 16],
            root_tree_id: vec![vec![0x02; 20]],
            description: "same git commit".to_string(),
            predecessors: vec![vec![0x22; 20]],
            external_ids: [("perforce-cl".to_string(), vec![0xbb; 8])]
                .into_iter()
                .collect(),
            ..Default::default()
        }
        .encode_to_vec(),
    );

    assert!(ObjectKind::Commit.is_scoped());
    assert_ne!(
        cache.object_path(&scope_a, ObjectKind::Commit, &commit_id),
        cache.object_path(&scope_b, ObjectKind::Commit, &commit_id)
    );

    let fetched_a = cache
        .get_or_fetch(&scope_a, ObjectKind::Commit, &commit_id, || async {
            Ok(commit_a.clone())
        })
        .await
        .unwrap();
    assert_eq!(fetched_a, commit_a);

    // A different scope with the same `commit_id` stays isolated and stores its own metadata.
    assert_eq!(
        cache.lookup_cached(&scope_b, ObjectKind::Commit, &commit_id),
        None
    );
    assert_eq!(
        cache.lookup_cached(&scope_a_other_user, ObjectKind::Commit, &commit_id),
        None
    );
    let fetched_b = cache
        .get_or_fetch(&scope_b, ObjectKind::Commit, &commit_id, || async {
            Ok(commit_b.clone())
        })
        .await
        .unwrap();
    assert_eq!(fetched_b, commit_b);

    assert_eq!(
        cache.lookup_cached(&scope_a, ObjectKind::Commit, &commit_id),
        Some(commit_a.clone())
    );
    assert_eq!(
        cache.lookup_cached(&scope_b, ObjectKind::Commit, &commit_id),
        Some(commit_b.clone())
    );

    let restarted = TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap();
    assert_eq!(
        restarted.lookup_cached(&scope_a, ObjectKind::Commit, &commit_id),
        Some(commit_a)
    );
    assert_eq!(
        restarted.lookup_cached(&scope_b, ObjectKind::Commit, &commit_id),
        Some(commit_b)
    );
    assert_eq!(
        restarted.lookup_cached(&scope_a_other_user, ObjectKind::Commit, &commit_id),
        None
    );
}

#[tokio::test]
async fn singleflight_propagates_leader_errors_to_coalesced_waiters_without_retry_storm() {
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap());
    let scope = CacheScope::new("http://127.0.0.1:8080", "repo-alpha");
    let id = vec![0x9c; 20];
    let calls = Arc::new(AtomicUsize::new(0));
    let (entered_tx, mut entered_rx) = watch::channel(false);
    let (release_tx, release_rx) = watch::channel(false);

    let leader = tokio::spawn({
        let cache = Arc::clone(&cache);
        let scope = scope.clone();
        let id = id.clone();
        let calls = Arc::clone(&calls);
        let mut release_rx = release_rx.clone();
        async move {
            cache
                .get_or_fetch(&scope, ObjectKind::Tree, &id, || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let _ = entered_tx.send(true);
                    let _ = release_rx.wait_for(|&v| v).await;
                    Err(tonic::Status::with_details(
                        tonic::Code::PermissionDenied,
                        "upstream denied",
                        Bytes::from_static(b"err-details"),
                    ))
                })
                .await
        }
    });

    entered_rx.wait_for(|&v| v).await.unwrap();

    let mut waiters = tokio::task::JoinSet::new();
    for _ in 0..7 {
        let cache = Arc::clone(&cache);
        let scope = scope.clone();
        let id = id.clone();
        let calls = Arc::clone(&calls);
        waiters.spawn(async move {
            cache
                .get_or_fetch(&scope, ObjectKind::Tree, &id, || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    panic!("coalesced waiter must not issue a cascading retry fetch");
                })
                .await
        });
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while cache.stats().singleflight_coalesced < 7 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for all 7 waiters to coalesce"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let _ = release_tx.send(true);
    let leader_err = leader.await.unwrap().unwrap_err();
    assert_eq!(leader_err.code(), tonic::Code::PermissionDenied);
    assert_eq!(leader_err.message(), "upstream denied");
    assert_eq!(leader_err.details(), b"err-details");

    while let Some(res) = waiters.join_next().await {
        let err = res.unwrap().unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert_eq!(err.message(), "upstream denied");
        assert_eq!(err.details(), b"err-details");
    }

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let st = cache.stats();
    assert_eq!(st.misses, 1);
    assert_eq!(st.singleflight_coalesced, 7);
}
