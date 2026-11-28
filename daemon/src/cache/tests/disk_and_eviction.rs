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
use std::time::{Duration, SystemTime};

use super::super::eviction::EvictionStats;
#[cfg(unix)]
use super::super::eviction::is_pid_dead;
use super::super::l2::{create_dir_all_private, create_private_file_with_initial_mode};
use super::super::{
    CACHE_STREAM_CHUNK_BYTES, CacheScope, CacheStatsSnapshot, DEFAULT_MAX_L2_OBJECT_BYTES,
    MAX_L1_OBJECT_BYTES, ObjectKind, TwoTierCache, effective_max_l2_object_bytes,
};
use super::{collect_range_chunks, git_blob_id};
use bytes::Bytes;
use cc_common::backend::Commit;
use cc_common::op_store::{Operation, View};
use prost::Message;
use sha1::{Digest, Sha1};
use tokio::sync::{Notify, watch};

#[cfg(unix)]
#[test]
fn cache_directories_and_files_enforce_private_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let cache_root = dir.path().join("cache");
    fs::create_dir_all(&cache_root).unwrap();
    fs::set_permissions(&cache_root, fs::Permissions::from_mode(0o755)).unwrap();

    let cache = TwoTierCache::new(cache_root.clone(), 1024 * 1024).unwrap();
    let scope = CacheScope::new("http://127.0.0.1:8080", "repo-alpha");
    let content = Bytes::from_static(b"secret payload");
    let id = git_blob_id(&content);
    cache.put_write_through(&scope, ObjectKind::File, &id, content);

    let object_path = cache.object_path(&scope, ObjectKind::File, &id);
    let marker_path = cache.inner.scoped_path(&scope, ObjectKind::File, &id);

    for dir_path in [
        cache_root.as_path(),
        cache_root.join("objects").as_path(),
        cache_root.join("scopes").as_path(),
        cache_root.join("staging").as_path(),
        cache_root
            .join("scopes")
            .join(scope.namespace_hex())
            .as_path(),
        object_path.parent().unwrap(),
        marker_path.parent().unwrap(),
    ] {
        let mode = fs::metadata(dir_path).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            mode,
            0o700,
            "expected 0700 on {}, got {mode:o}",
            dir_path.display()
        );
    }

    for file_path in [object_path.as_path(), marker_path.as_path()] {
        let mode = fs::metadata(file_path).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            mode,
            0o600,
            "expected 0600 on {}, got {mode:o}",
            file_path.display()
        );
    }
}

#[tokio::test]
async fn lookup_cached_skips_large_l2_file_blobs_without_loading_into_ram() {
    let dir = tempfile::tempdir().unwrap();
    let cache = TwoTierCache::new(dir.path().to_path_buf(), 8 * 1024 * 1024).unwrap();
    let scope = CacheScope::new("http://127.0.0.1:8080", "repo-alpha");

    // Valid >4 MiB File blob stored in L2 via BlobSpooler must not be loaded by `lookup_cached`,
    // must remain intact on disk, and must stream via `read_cached_file_range`.
    let payload: Vec<u8> = (0..(MAX_L1_OBJECT_BYTES + 16 * 1024))
        .map(|i| (i % 251) as u8)
        .collect();
    let large_id = git_blob_id(&payload);
    let mut spooler = cache.new_write_blob_spooler(Some(payload.len() as u64));
    for chunk in payload.chunks(CACHE_STREAM_CHUNK_BYTES) {
        spooler.write_chunk(chunk).await;
    }
    assert!(spooler.commit(scope.clone(), large_id.clone(), true).await);
    assert_eq!(cache.stats().l1_entries, 0);
    let l2_path = cache.object_path(&scope, ObjectKind::File, &large_id);
    assert!(l2_path.exists());

    assert_eq!(
        cache.lookup_cached(&scope, ObjectKind::File, &large_id),
        None,
        "lookup_cached must never load > MAX_L1_OBJECT_BYTES File blobs into RAM"
    );
    assert_eq!(cache.stats().l2_hits, 0);
    assert_eq!(cache.stats().l1_entries, 0);
    assert!(
        l2_path.exists(),
        "lookup_cached must not delete valid large L2 File blobs"
    );

    let rx = cache
        .read_cached_file_range(&scope, &large_id, MAX_L1_OBJECT_BYTES as u64, 4096)
        .await
        .unwrap()
        .expect("large blob must still stream from L2");
    let joined: Vec<u8> = collect_range_chunks(rx)
        .await
        .unwrap()
        .into_iter()
        .flatten()
        .collect();
    assert_eq!(
        joined,
        &payload[MAX_L1_OBJECT_BYTES..MAX_L1_OBJECT_BYTES + 4096]
    );
    assert_eq!(cache.stats().l2_hits, 1);

    // Even a huge sparse file (256 MiB) at an authorized L2 File path is skipped by metadata
    // length check before any read/allocation occurs.
    let huge_id = vec![0xef; 20];
    let huge_path = cache.object_path(&scope, ObjectKind::File, &huge_id);
    fs::create_dir_all(huge_path.parent().unwrap()).unwrap();
    let sparse = fs::File::create(&huge_path).unwrap();
    sparse.set_len(256 * 1024 * 1024).unwrap();
    drop(sparse);
    cache.authorize_scope(&scope, ObjectKind::File, &huge_id);
    assert_eq!(
        cache.lookup_cached(&scope, ObjectKind::File, &huge_id),
        None
    );
    assert!(huge_path.exists());
}

#[tokio::test]
async fn read_cached_file_range_detects_short_eof_on_large_l2_and_corrupt_small_l2() {
    let dir = tempfile::tempdir().unwrap();
    let cache = TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap();
    let scope = CacheScope::new("http://127.0.0.1:8080", "repo-alpha");

    // Large L2 blob truncated mid-stream while bounded channel is backpressured.
    let large: Vec<u8> = (0..(MAX_L1_OBJECT_BYTES + 2 * 1024 * 1024))
        .map(|i| (i % 251) as u8)
        .collect();
    let large_id = git_blob_id(&large);
    assert!(
        cache
            .put_write_through_async(
                scope.clone(),
                ObjectKind::File,
                large_id.clone(),
                Bytes::from(large.clone()),
            )
            .await
    );
    let large_path = cache.object_path(&scope, ObjectKind::File, &large_id);
    let rx = cache
        .read_cached_file_range(&scope, &large_id, 0, 0)
        .await
        .unwrap()
        .expect("large blob cached");
    // Truncate the underlying L2 file so reading beyond the first buffered chunks hits EOF.
    let truncated_len = (CACHE_STREAM_CHUNK_BYTES * 2) as u64;
    fs::OpenOptions::new()
        .write(true)
        .open(&large_path)
        .unwrap()
        .set_len(truncated_len)
        .unwrap();
    let err = collect_range_chunks(rx).await.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    assert!(
        !large_path.exists(),
        "truncated L2 blob must be discarded after UnexpectedEof"
    );

    // Tampered small L2 blob is rejected and discarded rather than promoted to L1.
    let small = Bytes::from_static(b"authentic small payload");
    let small_id = git_blob_id(&small);
    cache.put_write_through(&scope, ObjectKind::File, &small_id, small.clone());
    let small_path = cache.object_path(&scope, ObjectKind::File, &small_id);
    fs::write(&small_path, b"authentic small payload + extra bytes").unwrap();
    let restarted = TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap();
    assert!(
        restarted
            .read_cached_file_range(&scope, &small_id, 0, 0)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        !small_path.exists(),
        "tampered small L2 entry must be discarded"
    );
}

#[tokio::test]
async fn async_batched_helpers_and_hit_ratio_saturation() {
    let dir = tempfile::tempdir().unwrap();
    let cache = TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap();
    let scope_a = CacheScope::new("http://127.0.0.1:8080", "proj-a");
    let scope_b = CacheScope::new("http://127.0.0.1:8080", "proj-b");

    let b1 = Bytes::from_static(b"first blob");
    let id1 = git_blob_id(&b1);
    let b2 = Bytes::from_static(b"second blob longer");
    let id2 = git_blob_id(&b2);
    let missing_id = git_blob_id(b"not cached");

    assert!(
        cache
            .put_write_through_async(scope_a.clone(), ObjectKind::File, id1.clone(), b1.clone())
            .await
    );
    assert!(
        cache
            .put_write_through_async(scope_a.clone(), ObjectKind::File, id2.clone(), b2.clone())
            .await
    );

    assert_eq!(
        cache.blob_sizes_async(scope_a.clone(), Vec::new()).await,
        vec![]
    );
    assert_eq!(
        cache
            .blob_sizes_async(
                scope_a.clone(),
                vec![id1.clone(), missing_id.clone(), id2.clone()]
            )
            .await,
        vec![Some(b1.len() as u64), None, Some(b2.len() as u64)]
    );
    assert_eq!(
        cache
            .blob_sizes_async(scope_b.clone(), vec![id1.clone(), id2.clone()])
            .await,
        vec![None, None]
    );

    cache.authorize_scope(&scope_b, ObjectKind::File, &id1);
    cache.authorize_scope(&scope_b, ObjectKind::File, &id2);
    assert_eq!(
        cache
            .blob_sizes_async(scope_b.clone(), vec![id1.clone(), id2.clone()])
            .await,
        vec![Some(b1.len() as u64), Some(b2.len() as u64)]
    );

    let saturated = CacheStatsSnapshot {
        l1_hits: u64::MAX,
        l2_hits: u64::MAX,
        misses: u64::MAX,
        ..Default::default()
    };
    let ratio = saturated.hit_ratio();
    assert!(ratio.is_finite() && (0.0..=1.0).contains(&ratio));
}

#[cfg(unix)]
#[test]
fn startup_sweeps_dead_pid_staging_files_and_preserves_live_ones() {
    use std::process::{Command, Stdio};

    let dir = tempfile::tempdir().unwrap();
    let staging = dir.path().join("staging");
    create_dir_all_private(&staging).unwrap();

    let mut child = Command::new(std::env::current_exe().unwrap())
        .arg("--help")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn short-lived helper process");
    let dead_pid = child.id();
    let _ = child.wait().expect("reap helper process");
    assert!(is_pid_dead(dead_pid), "reaped child PID must report dead");

    let current_pid = std::process::id();
    let dead_file = staging.join(format!(".tmp.{dead_pid}.1"));
    let live_file = staging.join(format!(".tmp.{current_pid}.2"));
    let unrelated_file = staging.join("keep-me.tmp");
    let bad_name = staging.join(".tmp.notapid.3");
    for path in [&dead_file, &live_file, &unrelated_file, &bad_name] {
        fs::write(path, b"partial").unwrap();
    }

    let _cache = TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap();
    assert!(!dead_file.exists(), "dead-PID staging file must be swept");
    assert!(live_file.exists(), "live-PID staging file must be kept");
    assert!(unrelated_file.exists(), "unrelated file must be kept");
    assert!(bad_name.exists(), "malformed temp filename must be kept");
}

#[test]
fn insert_verified_respects_max_l2_object_bytes() {
    let scope = CacheScope::new("http://127.0.0.1:8080", "repo-alpha");

    // L1 disabled, L2 capped at 16 bytes.
    let dir1 = tempfile::tempdir().unwrap();
    let capped = TwoTierCache::new_with_l2_object_cap(dir1.path().to_path_buf(), 0, 16).unwrap();
    let too_large = Bytes::from(vec![b'x'; 32]);
    let too_large_id = git_blob_id(&too_large);
    capped.put_write_through(&scope, ObjectKind::File, &too_large_id, too_large);
    assert!(
        !capped
            .object_path(&scope, ObjectKind::File, &too_large_id)
            .exists()
    );
    assert!(!capped.is_authorized(&scope, ObjectKind::File, &too_large_id));
    assert_eq!(capped.stats().write_through_objects, 0);

    let fits = Bytes::from_static(b"small-blob");
    let fits_id = git_blob_id(&fits);
    capped.put_write_through(&scope, ObjectKind::File, &fits_id, fits);
    assert!(
        capped
            .object_path(&scope, ObjectKind::File, &fits_id)
            .exists()
    );
    assert_eq!(capped.stats().write_through_objects, 1);

    // L2 disabled (`max_l2_object_bytes == 0`), L1 enabled: caches in L1 without touching L2.
    let dir2 = tempfile::tempdir().unwrap();
    let l1_only =
        TwoTierCache::new_with_l2_object_cap(dir2.path().to_path_buf(), 1024 * 1024, 0).unwrap();
    let small = Bytes::from_static(b"l1-only");
    let small_id = git_blob_id(&small);
    l1_only.put_write_through(&scope, ObjectKind::File, &small_id, small.clone());
    assert!(
        !l1_only
            .object_path(&scope, ObjectKind::File, &small_id)
            .exists()
    );
    assert_eq!(
        l1_only.lookup_cached(&scope, ObjectKind::File, &small_id),
        Some(small)
    );
}
#[cfg(unix)]
#[test]
fn l2_dir_helpers_reject_symlinks_without_modifying_target_permissions() {
    use std::os::unix::fs::{PermissionsExt, symlink};

    let dir = tempfile::tempdir().unwrap();
    let victim = dir.path().join("victim");
    fs::create_dir(&victim).unwrap();
    fs::set_permissions(&victim, fs::Permissions::from_mode(0o755)).unwrap();

    // Symlink at `cache_dir` itself must be rejected without modifying the target's mode.
    let cache_link = dir.path().join("cache_link");
    symlink(&victim, &cache_link).unwrap();
    let err = TwoTierCache::new(cache_link, 1024 * 1024)
        .err()
        .expect("symlinked cache_dir must fail");
    assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
    assert_eq!(
        fs::metadata(&victim).unwrap().permissions().mode() & 0o777,
        0o755,
        "symlinked cache_dir must not chmod target directory"
    );

    // Symlink planted inside `<cache_dir>/objects` must be rejected before creating subdirs
    // or changing the target's permissions.
    let cache_root = dir.path().join("real_cache");
    let cache = TwoTierCache::new(cache_root.clone(), 0).unwrap();
    fs::remove_dir(cache_root.join("objects")).unwrap();
    symlink(&victim, cache_root.join("objects")).unwrap();

    let scope = CacheScope::new("http://127.0.0.1:8080", "repo-alpha");
    let payload = Bytes::from_static(b"symlink-safety-payload");
    let file_id = git_blob_id(&payload);
    cache.put_write_through(&scope, ObjectKind::File, &file_id, payload);

    assert_eq!(cache.stats().l2_write_errors, 1);
    assert_eq!(
        fs::metadata(&victim).unwrap().permissions().mode() & 0o777,
        0o755,
        "symlink inside L2 cache dir must not chmod target directory"
    );
    assert_eq!(
        fs::read_dir(&victim).unwrap().count(),
        0,
        "symlink inside L2 cache dir must not create subdirectories in target"
    );
}

#[tokio::test]
async fn purge_namespace_evicts_all_auth_variants_and_preserves_shared_cas() {
    let dir = tempfile::tempdir().unwrap();
    let cache = TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap();
    let url = "https://commitcloud.example.com";
    let repo_a_tok1 = CacheScope::new_with_auth(url, "repo-a", Some("Bearer tok-1"));
    let repo_a_tok2 = CacheScope::new_with_auth(url, "repo-a", Some("Bearer tok-2"));
    let repo_b = CacheScope::new_with_auth(url, "repo-b", Some("Bearer tok-1"));

    let file_bytes = Bytes::from_static(b"shared blob content");
    let file_id = git_blob_id(&file_bytes);
    let view_id = vec![0x11; 64];
    let view_bytes = Bytes::from(
        View {
            head_ids: vec![vec![0xaa; 20]],
            ..Default::default()
        }
        .encode_to_vec(),
    );
    let op_id = vec![0x22; 64];
    let op_bytes = Bytes::from(
        Operation {
            view_id: view_id.clone(),
            parents: vec![vec![0x00; 16]],
            ..Default::default()
        }
        .encode_to_vec(),
    );
    let commit_id = vec![0x33; 20];
    let commit_bytes = Bytes::from(
        Commit {
            commit_id: commit_id.clone(),
            change_id: vec![0x44; 16],
            root_tree_id: vec![vec![0x55; 20]],
            description: "scoped commit".to_string(),
            ..Default::default()
        }
        .encode_to_vec(),
    );

    for scope in [&repo_a_tok1, &repo_a_tok2, &repo_b] {
        cache.put_write_through(scope, ObjectKind::File, &file_id, file_bytes.clone());
        cache.put_write_through(scope, ObjectKind::View, &view_id, view_bytes.clone());
        cache.put_write_through(scope, ObjectKind::Operation, &op_id, op_bytes.clone());
        cache.put_write_through(scope, ObjectKind::Commit, &commit_id, commit_bytes.clone());
    }
    let entries_before = cache.stats().l1_entries;
    let bytes_before = cache.stats().l1_bytes;
    assert_eq!(entries_before, 1 + 9); // 1 shared File + 3 scoped objects x 3 scopes
    assert!(bytes_before > 0);

    cache.purge_namespace(url, "repo-a").await;

    // Both credential variants of `repo-a` are purged from L1, authorized, and L2 disk.
    for scope in [&repo_a_tok1, &repo_a_tok2] {
        assert!(!cache.is_authorized(scope, ObjectKind::File, &file_id));
        assert_eq!(cache.lookup_cached(scope, ObjectKind::File, &file_id), None);
        assert_eq!(cache.lookup_cached(scope, ObjectKind::View, &view_id), None);
        assert_eq!(
            cache.lookup_cached(scope, ObjectKind::Operation, &op_id),
            None
        );
        assert_eq!(
            cache.lookup_cached(scope, ObjectKind::Commit, &commit_id),
            None
        );
    }
    assert!(
        !dir.path()
            .join("scopes")
            .join(repo_a_tok1.namespace_hex())
            .exists()
    );

    // `l1_entries` and `l1_bytes` drop accurately; shared CAS and `repo-b` stay intact.
    assert_eq!(cache.stats().l1_entries, 1 + 3);
    assert!(cache.stats().l1_bytes < bytes_before);
    assert!(cache.is_authorized(&repo_b, ObjectKind::File, &file_id));
    assert_eq!(
        cache.lookup_cached(&repo_b, ObjectKind::File, &file_id),
        Some(file_bytes.clone())
    );
    assert_eq!(
        cache.lookup_cached(&repo_b, ObjectKind::View, &view_id),
        Some(view_bytes.clone())
    );
    assert_eq!(
        cache.lookup_cached(&repo_b, ObjectKind::Operation, &op_id),
        Some(op_bytes.clone())
    );
    assert_eq!(
        cache.lookup_cached(&repo_b, ObjectKind::Commit, &commit_id),
        Some(commit_bytes.clone())
    );

    // Restarting from disk sees the purge too.
    let restarted = TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap();
    assert_eq!(
        restarted.lookup_cached(&repo_a_tok1, ObjectKind::View, &view_id),
        None
    );
    assert_eq!(
        restarted.lookup_cached(&repo_a_tok2, ObjectKind::File, &file_id),
        None
    );
    assert_eq!(
        restarted.lookup_cached(&repo_b, ObjectKind::File, &file_id),
        Some(file_bytes)
    );
    assert_eq!(
        restarted.lookup_cached(&repo_b, ObjectKind::View, &view_id),
        Some(view_bytes)
    );
}

#[tokio::test]
async fn purge_namespace_blocks_inflight_fetch_resurrection_and_cross_delete_coalescing() {
    let dir = tempfile::tempdir().unwrap();
    let cache = TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap();
    let url = "http://127.0.0.1:8080";
    let scope = CacheScope::new_with_auth(url, "repo-race", Some("Bearer tok"));

    let view_id = vec![0x11; 16];
    let view_bytes = Bytes::from(
        View {
            head_ids: vec![vec![0xaa; 20]],
            ..Default::default()
        }
        .encode_to_vec(),
    );
    let file_bytes = Bytes::from_static(b"file-content");
    let file_id = {
        let mut hasher = Sha1::new();
        hasher.update(format!("blob {}\0", file_bytes.len()).as_bytes());
        hasher.update(&file_bytes);
        hasher.finalize().to_vec()
    };

    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let entered_c = Arc::clone(&entered);
    let release_c = Arc::clone(&release);
    let cache_c = cache.clone();
    let scope_c = scope.clone();
    let view_id_c = view_id.clone();
    let view_bytes_c = view_bytes.clone();

    let pre_delete_epoch = cache.purge_epoch();
    let inflight = tokio::spawn(async move {
        cache_c
            .get_or_fetch(&scope_c, ObjectKind::View, &view_id_c, || async move {
                entered_c.notify_one();
                release_c.notified().await;
                Ok(view_bytes_c)
            })
            .await
    });

    entered.notified().await;

    // Purge while the pre-delete fetch is still in flight.
    cache.purge_namespace(url, "repo-race").await;

    // A post-delete reader must NOT coalesce onto the pre-delete singleflight leader.
    let post_delete_res = cache
        .get_or_fetch(&scope, ObjectKind::View, &view_id, || async {
            Err(tonic::Status::not_found("repo deleted"))
        })
        .await;
    assert_eq!(
        post_delete_res.unwrap_err().code(),
        tonic::Code::NotFound,
        "post-delete fetch must not coalesce onto pre-delete singleflight leader"
    );

    // Now let the pre-delete fetch resolve; its cache write must be suppressed.
    release.notify_one();
    assert_eq!(inflight.await.unwrap().unwrap(), view_bytes);

    // Pre-purge write-through must also be suppressed.
    assert!(
        !cache
            .put_write_through_if_epoch_async(
                scope.clone(),
                ObjectKind::File,
                file_id.clone(),
                file_bytes,
                Some(pre_delete_epoch),
            )
            .await
    );

    assert_eq!(
        cache.lookup_cached(&scope, ObjectKind::View, &view_id),
        None
    );
    assert!(!cache.is_authorized(&scope, ObjectKind::File, &file_id));
    assert!(
        !dir.path()
            .join("scopes")
            .join(scope.namespace_hex())
            .exists()
    );
}

#[test]
fn init_prunes_legacy_flat_scope_dirs_and_preserves_v2_layout() {
    let dir = tempfile::tempdir().unwrap();
    let scopes_dir = dir.path().join("scopes");

    // Seed legacy 1-level `scopes/<scope_hex>/<kind>/...` directories.
    let legacy_view_dir = scopes_dir
        .join("deadbeef00000000000000000000000000000000000000000000000000000001")
        .join("views")
        .join("11")
        .join("22");
    let legacy_blob_dir = scopes_dir
        .join("deadbeef00000000000000000000000000000000000000000000000000000002")
        .join("blobs")
        .join("aa")
        .join("bb");
    fs::create_dir_all(&legacy_view_dir).unwrap();
    fs::create_dir_all(&legacy_blob_dir).unwrap();
    fs::write(legacy_view_dir.join("1122"), b"legacy view").unwrap();
    fs::write(legacy_blob_dir.join("aabb"), b"").unwrap();

    let cache = TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap();
    assert!(
        !scopes_dir
            .join("deadbeef00000000000000000000000000000000000000000000000000000001")
            .exists(),
        "legacy 1-level views scope directory should be pruned on init"
    );
    assert!(
        !scopes_dir
            .join("deadbeef00000000000000000000000000000000000000000000000000000002")
            .exists(),
        "legacy 1-level blobs scope directory should be pruned on init"
    );

    let scope = CacheScope::new("https://commitcloud.example.com", "repo-v2");
    let file_bytes = Bytes::from_static(b"v2 preserved blob");
    let file_id = git_blob_id(&file_bytes);
    let view_id = vec![0x71; 16];
    let view_bytes = Bytes::from(
        View {
            head_ids: vec![vec![0xaa; 20]],
            ..Default::default()
        }
        .encode_to_vec(),
    );
    cache.put_write_through(&scope, ObjectKind::File, &file_id, file_bytes.clone());
    cache.put_write_through(&scope, ObjectKind::View, &view_id, view_bytes.clone());

    // Re-opening preserves all v2 2-level scope entries and shared CAS blobs.
    let reopened = TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap();
    assert_eq!(
        reopened.lookup_cached(&scope, ObjectKind::File, &file_id),
        Some(file_bytes)
    );
    assert_eq!(
        reopened.lookup_cached(&scope, ObjectKind::View, &view_id),
        Some(view_bytes)
    );
}

#[tokio::test]
async fn pending_purge_suppresses_l2_reads_authorizations_and_write_backs_until_disk_removed() {
    let dir = tempfile::tempdir().unwrap();
    let cache = TwoTierCache::new(dir.path().to_path_buf(), 1024 * 1024).unwrap();
    let url = "http://127.0.0.1:8080";
    let scope_a = CacheScope::new_with_auth(url, "repo-pending", Some("Bearer tok-a"));
    let scope_b = CacheScope::new_with_auth(url, "repo-other", Some("Bearer tok-a"));

    let view_id = vec![0x21; 16];
    let view_bytes = Bytes::from(
        View {
            head_ids: vec![vec![0xbb; 20]],
            ..Default::default()
        }
        .encode_to_vec(),
    );
    let file_bytes = Bytes::from_static(b"pending purge shared cas blob");
    let file_id = git_blob_id(&file_bytes);

    for scope in [&scope_a, &scope_b] {
        cache.put_write_through(scope, ObjectKind::View, &view_id, view_bytes.clone());
        cache.put_write_through(scope, ObjectKind::File, &file_id, file_bytes.clone());
    }
    let ns_dir = dir.path().join("scopes").join(scope_a.namespace_hex());
    assert!(ns_dir.exists());

    // Begin pending purge WITHOUT yet running disk removal: `ns_dir` still exists on disk,
    // yet every read, auth check, blob_size, ranged file read, write-through, and spool commit
    // for `repo-pending` must treat the namespace as uncached while `repo-other` stays warm.
    let guard = cache.begin_pending_purge(url, "repo-pending");
    assert!(ns_dir.exists());

    assert!(!cache.is_authorized(&scope_a, ObjectKind::File, &file_id));
    assert!(!cache.is_authorized(&scope_a, ObjectKind::View, &view_id));
    assert_eq!(
        cache.lookup_cached(&scope_a, ObjectKind::View, &view_id),
        None
    );
    assert_eq!(
        cache.lookup_cached(&scope_a, ObjectKind::File, &file_id),
        None
    );
    assert_eq!(cache.blob_size(&scope_a, &file_id), None);
    assert!(
        cache
            .read_cached_file_range(&scope_a, &file_id, 0, 0)
            .await
            .unwrap()
            .is_none()
    );

    // Attempting to authorize or write back into `repo-pending` while purge is pending must
    // be suppressed (including BlobSpooler commits).
    cache.authorize_scope(&scope_a, ObjectKind::File, &file_id);
    assert!(!cache.is_authorized(&scope_a, ObjectKind::File, &file_id));
    assert!(
        !cache
            .put_write_through_async(
                scope_a.clone(),
                ObjectKind::View,
                view_id.clone(),
                view_bytes.clone(),
            )
            .await
    );
    let mut spooler = cache.new_write_blob_spooler(Some(file_bytes.len() as u64));
    spooler.write_chunk(&file_bytes).await;
    assert!(!spooler.commit(scope_a.clone(), file_id.clone(), true).await);

    // Unrelated namespace remains unaffected.
    assert_eq!(
        cache.lookup_cached(&scope_b, ObjectKind::View, &view_id),
        Some(view_bytes.clone())
    );
    assert_eq!(
        cache.lookup_cached(&scope_b, ObjectKind::File, &file_id),
        Some(file_bytes.clone())
    );

    // Completing the blocking purge removes `ns_dir`, releases the guard, and allows fresh
    // writes afterward.
    cache.purge_namespace_blocking_with_guard(url, "repo-pending", guard);
    assert!(!ns_dir.exists());
    assert!(!cache.is_pending_purge_hex(scope_a.namespace_hex()));
    assert!(
        cache
            .put_write_through_async(
                scope_a.clone(),
                ObjectKind::View,
                view_id.clone(),
                view_bytes.clone(),
            )
            .await
    );
    assert_eq!(
        cache.lookup_cached(&scope_a, ObjectKind::View, &view_id),
        Some(view_bytes)
    );
}

#[cfg(unix)]
#[tokio::test]
async fn l2_files_and_markers_enforce_strict_0600_via_fchmod_independent_of_initial_mode() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();

    // Directly verify that `create_private_file_with_initial_mode` overrides both overly
    // restrictive (simulating umask 0o277/0o777) and overly permissive initial modes via
    // post-create `fchmod(0o600)` without mutating process-wide umask.
    for (name, initial_mode) in [
        ("restrictive_0400", 0o400u32),
        ("restrictive_0000", 0o000u32),
        ("permissive_0644", 0o644u32),
        ("permissive_0666", 0o666u32),
    ] {
        let p = dir.path().join(name);
        let f = create_private_file_with_initial_mode(&p, true, initial_mode).unwrap();
        let fd_mode = f.metadata().unwrap().permissions().mode() & 0o777;
        assert_eq!(
            fd_mode, 0o600,
            "post-create fchmod must restore 0600 for initial mode {initial_mode:o}"
        );
        drop(f);
        let path_mode = fs::metadata(&p).unwrap().permissions().mode() & 0o777;
        assert_eq!(path_mode, 0o600);
    }

    // Verify published L2 objects, scope auth markers, and spooled blobs all land at 0600.
    let cache_dir = dir.path().join("cache");
    let cache = TwoTierCache::new(cache_dir.clone(), 1024 * 1024).unwrap();
    let scope = CacheScope::new("http://127.0.0.1:8080", "proj-perms");

    let small_bytes = Bytes::from_static(b"small-blob-perms-check");
    let small_id = git_blob_id(&small_bytes);
    cache.put_write_through(&scope, ObjectKind::File, &small_id, small_bytes);
    let small_path = cache.object_path(&scope, ObjectKind::File, &small_id);
    assert_eq!(
        fs::metadata(&small_path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let marker_path = cache.inner.scoped_path(&scope, ObjectKind::File, &small_id);
    assert_eq!(
        fs::metadata(&marker_path).unwrap().permissions().mode() & 0o777,
        0o600
    );

    let spooled_bytes = vec![0x42u8; 64 * 1024];
    let spooled_id = git_blob_id(&spooled_bytes);
    let mut spooler = cache.new_write_blob_spooler(Some(spooled_bytes.len() as u64));
    spooler.write_chunk(&spooled_bytes).await;
    assert!(
        spooler
            .commit(scope.clone(), spooled_id.clone(), true)
            .await
    );
    let spooled_path = cache.object_path(&scope, ObjectKind::File, &spooled_id);
    assert_eq!(
        fs::metadata(&spooled_path).unwrap().permissions().mode() & 0o777,
        0o600
    );
}

#[test]
fn evict_l2_to_budget_evicts_oldest_first_honors_l2_hit_recency_and_stops_at_low_watermark() {
    let dir = tempfile::tempdir().unwrap();
    // Zero L1 budget so lookups always go through L2 and trigger mtime refresh.
    let cache = TwoTierCache::new(dir.path().to_path_buf(), 0).unwrap();
    let scope = CacheScope::new("http://127.0.0.1:8080", "proj-evict");

    let mut blobs = Vec::new();
    let now = SystemTime::now();
    for i in 0u8..5 {
        let payload = Bytes::from(vec![i + 1; 1000]);
        let id = git_blob_id(&payload);
        cache.put_write_through(&scope, ObjectKind::File, &id, payload.clone());
        let path = cache.object_path(&scope, ObjectKind::File, &id);
        // Set distinct historical mtimes: blob 0 oldest (10 min ago) .. blob 4 newest (6 min ago).
        let mtime = now - Duration::from_secs(600 - u64::from(i) * 60);
        fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(mtime)
            .unwrap();
        blobs.push((id, payload, path));
    }

    // Perform an L2 hit on blob 0 (originally oldest) so its mtime refreshes to ~now.
    assert_eq!(
        cache.lookup_cached(&scope, ObjectKind::File, &blobs[0].0),
        Some(blobs[0].1.clone())
    );

    // Budget = 3500 bytes -> low watermark = 85% = 2975 bytes.
    // Total = 5000 > 3500 -> must evict oldest blobs (blob 1, blob 2, blob 3 -> 3000 bytes)
    // leaving 2000 bytes (<= 2975): blob 0 (refreshed on L2 hit) and blob 4 (newest).
    let stats = cache.evict_l2_to_budget(3500).unwrap();
    assert_eq!(stats.bytes_before, 5000);
    assert_eq!(stats.bytes_after, 2000);
    assert_eq!(stats.evicted_objects, 3);
    assert_eq!(stats.evicted_bytes, 3000);
    assert_eq!(stats.orphaned_markers_removed, 3);

    assert!(
        blobs[0].2.exists(),
        "hot blob refreshed on L2 hit must survive"
    );
    assert!(!blobs[1].2.exists(), "oldest un-hit blob 1 must be evicted");
    assert!(!blobs[2].2.exists(), "blob 2 must be evicted");
    assert!(
        !blobs[3].2.exists(),
        "blob 3 must be evicted to reach 85% low watermark"
    );
    assert!(
        blobs[4].2.exists(),
        "newest blob 4 must survive (low watermark stop)"
    );

    let snap = cache.stats();
    assert_eq!(snap.l2_evicted_objects, 3);
    assert_eq!(snap.l2_evicted_bytes, 3000);

    // Re-running under budget is a no-op (low watermark prevented thrashing).
    let second = cache.evict_l2_to_budget(3500).unwrap();
    assert_eq!(second.evicted_objects, 0);
    assert_eq!(second.evicted_bytes, 0);
    assert_eq!(second.bytes_after, 2000);

    // Passing budget == 0 disables eviction.
    let disabled = cache.evict_l2_to_budget(0).unwrap();
    assert_eq!(disabled, EvictionStats::default());
    assert!(blobs[0].2.exists() && blobs[4].2.exists());
}

#[test]
fn evict_l2_preserves_active_tmp_staging_and_run_files_and_cleans_orphaned_markers() {
    let dir = tempfile::tempdir().unwrap();
    let cache = TwoTierCache::new(dir.path().to_path_buf(), 0).unwrap();
    let scope = CacheScope::new("http://127.0.0.1:8080", "proj-orphan");

    let keep_blob = Bytes::from(vec![0xaa; 500]);
    let keep_id = git_blob_id(&keep_blob);
    let evict_blob = Bytes::from(vec![0xbb; 1500]);
    let evict_id = git_blob_id(&evict_blob);
    cache.put_write_through(&scope, ObjectKind::File, &evict_id, evict_blob);
    let evict_path = cache.object_path(&scope, ObjectKind::File, &evict_id);
    let old = SystemTime::now() - Duration::from_secs(300);
    fs::File::options()
        .write(true)
        .open(&evict_path)
        .unwrap()
        .set_modified(old)
        .unwrap();
    cache.put_write_through(&scope, ObjectKind::File, &keep_id, keep_blob);

    // Plant an active `.tmp` file in the object shard directory, a staging file in `staging/`,
    // and daemon state files under `run/`: none may be touched or counted by eviction.
    let shard_tmp = evict_path.parent().unwrap().join(".tmp.99999.1.active");
    fs::write(&shard_tmp, vec![0xcc; 4000]).unwrap();
    let staging_tmp = dir.path().join("staging").join(".tmp.99999.2");
    fs::write(&staging_tmp, vec![0xdd; 4000]).unwrap();
    let run_dir = dir.path().join("run");
    fs::create_dir_all(&run_dir).unwrap();
    let daemon_json = run_dir.join("daemon.json");
    fs::write(&daemon_json, b"{}").unwrap();

    // Plant a pre-existing orphaned scope auth marker whose shared object does not exist.
    let orphan_id = vec![0xef; 20];
    cache.authorize_scope(&scope, ObjectKind::Tree, &orphan_id);
    let orphan_marker = cache
        .inner
        .scoped_path(&scope, ObjectKind::Tree, &orphan_id);
    assert!(orphan_marker.exists());
    assert!(cache.is_authorized(&scope, ObjectKind::Tree, &orphan_id));

    // Plant a stray non-ASCII UTF-8 filename inside `scopes/` to verify `evict_l2_to_budget`
    // handles unexpected filenames without panicking on UTF-8 char boundaries.
    let stray_non_ascii = orphan_marker.parent().unwrap().join("éééé");
    fs::write(&stray_non_ascii, b"").unwrap();

    let evict_marker = cache.inner.scoped_path(&scope, ObjectKind::File, &evict_id);
    let keep_marker = cache.inner.scoped_path(&scope, ObjectKind::File, &keep_id);
    assert!(evict_marker.exists() && keep_marker.exists());

    // Budget = 1000 bytes -> evicts `evict_blob` (1500) and cleans its marker, the
    // pre-existing orphaned tree marker, and the stray non-ASCII marker while leaving
    // `.tmp*`, `staging/`, `run/`, and `keep_blob` + its marker intact.
    let stats = cache.evict_l2_to_budget(1000).unwrap();
    assert_eq!(stats.bytes_before, 2000);
    assert_eq!(stats.bytes_after, 500);
    assert_eq!(stats.evicted_objects, 1);
    assert_eq!(stats.orphaned_markers_removed, 3);
    assert!(!stray_non_ascii.exists());

    assert!(
        shard_tmp.exists(),
        "active shard .tmp file must be preserved"
    );
    assert!(staging_tmp.exists(), "staging file must be preserved");
    assert!(daemon_json.exists(), "run/daemon.json must be preserved");
    assert!(
        cache
            .object_path(&scope, ObjectKind::File, &keep_id)
            .exists()
            && keep_marker.exists(),
        "surviving blob and its auth marker must stay intact"
    );
    assert!(
        !evict_marker.exists(),
        "evicted blob auth marker must be removed"
    );
    assert!(!orphan_marker.exists(), "orphaned marker must be removed");
    assert!(
        !cache.is_authorized(&scope, ObjectKind::File, &evict_id),
        "in-memory authorized cache must prune removed orphaned marker"
    );
    assert!(
        !cache.is_authorized(&scope, ObjectKind::Tree, &orphan_id),
        "in-memory authorized cache must prune pre-existing orphaned marker"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn evict_l2_unlinking_blob_does_not_corrupt_in_progress_range_read() {
    let dir = tempfile::tempdir().unwrap();
    let cache = TwoTierCache::new(dir.path().to_path_buf(), 0).unwrap();
    let scope = CacheScope::new("http://127.0.0.1:8080", "proj-live-reader");

    // Create a large (> MAX_L1_OBJECT_BYTES = 4 MiB) blob so `read_cached_file_range` streams
    // directly from an open L2 file descriptor across multiple 64 KiB frames.
    let size = MAX_L1_OBJECT_BYTES + 256 * 1024;
    let content: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
    let file_id = git_blob_id(&content);
    let mut spooler = cache.new_write_blob_spooler(Some(size as u64));
    for chunk in content.chunks(CACHE_STREAM_CHUNK_BYTES) {
        spooler.write_chunk(chunk).await;
    }
    assert!(spooler.commit(scope.clone(), file_id.clone(), true).await);

    let blob_path = cache.object_path(&scope, ObjectKind::File, &file_id);
    assert!(blob_path.exists());

    // Start streaming the large blob from L2 and read the first chunk to ensure the reader
    // task holds the open file descriptor.
    let mut rx = cache
        .read_cached_file_range(&scope, &file_id, 0, 0)
        .await
        .unwrap()
        .expect("large blob should hit L2");
    let first = rx.recv().await.unwrap().unwrap();
    assert!(!first.is_empty());

    // Evict the blob from L2 while the range reader is mid-stream.
    let evict_stats = cache.evict_l2_to_budget(1024).unwrap();
    assert_eq!(evict_stats.evicted_objects, 1);
    assert!(!blob_path.exists(), "eviction must unlink the blob path");

    // The in-progress reader must finish streaming every remaining byte without error.
    let mut assembled = first;
    while let Some(chunk) = rx.recv().await {
        assembled.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(assembled, content);
}

#[tokio::test]
async fn spawn_l2_eviction_task_triggers_on_write_volume_and_stops_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let cache = TwoTierCache::new(dir.path().to_path_buf(), 0).unwrap();
    let scope = CacheScope::new("http://127.0.0.1:8080", "proj-bg-evict");

    // `max_disk_bytes == 0` disables background eviction and returns `None`.
    let (_disabled_tx, disabled_rx) = watch::channel(false);
    assert!(
        cache
            .spawn_l2_eviction_task(0, disabled_rx, || {})
            .is_none()
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let evicted_signal = Arc::new(Notify::new());
    let evicted_signal_cb = Arc::clone(&evicted_signal);
    let handle = cache
        .spawn_l2_eviction_task(2000, shutdown_rx, move || {
            evicted_signal_cb.notify_one();
        })
        .expect("non-zero budget should spawn background eviction task");

    // Write three 1000-byte blobs (total 3000 > 2000 budget, trigger = 200 bytes) so the
    // background task wakes early on write volume without waiting for the 60s periodic tick.
    let now = SystemTime::now();
    for i in 0u8..3 {
        let payload = Bytes::from(vec![i + 10; 1000]);
        let id = git_blob_id(&payload);
        cache.put_write_through(&scope, ObjectKind::File, &id, payload);
        if i == 0 {
            let p = cache.object_path(&scope, ObjectKind::File, &id);
            let _ = fs::File::options()
                .write(true)
                .open(&p)
                .and_then(|f| f.set_modified(now - Duration::from_secs(120)));
        }
    }

    tokio::time::timeout(Duration::from_secs(3), evicted_signal.notified())
        .await
        .expect("write-volume notification should wake background eviction well before 60s");
    assert!(
        cache.stats().l2_evicted_objects >= 1,
        "background sweep should record evicted L2 objects"
    );

    let _ = shutdown_tx.send(true);
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("eviction task must exit promptly on shutdown signal")
        .expect("eviction task must not panic");
}

#[test]
fn evict_l2_preserves_l1_resident_shared_objects_and_l1_only_entries() {
    let dir = tempfile::tempdir().unwrap();
    // Each 1000-byte shared File has L1 weight 1000 + 64 (upstream_hex) + 20 (id) + 64 = 1148 bytes,
    // so a 2500-byte L1 budget holds at most two 1000-byte blobs at once.
    let cache = TwoTierCache::new(dir.path().to_path_buf(), 2500).unwrap();
    let scope = CacheScope::new("http://127.0.0.1:8080", "proj-l1-hot");

    let cold1 = Bytes::from(vec![1u8; 1000]);
    let cold2 = Bytes::from(vec![2u8; 1000]);
    let hot = Bytes::from(vec![3u8; 1000]);
    let warm = Bytes::from(vec![4u8; 1000]);
    let cold1_id = git_blob_id(&cold1);
    let cold2_id = git_blob_id(&cold2);
    let hot_id = git_blob_id(&hot);
    let warm_id = git_blob_id(&warm);

    for (id, payload) in [
        (&cold1_id, &cold1),
        (&cold2_id, &cold2),
        (&hot_id, &hot),
        (&warm_id, &warm),
    ] {
        cache.put_write_through(&scope, ObjectKind::File, id, payload.clone());
    }
    assert_eq!(cache.stats().l1_entries, 2);

    // Give `hot` (resident in L1) the OLDEST L2 mtime on disk, simulating a hot L1 object whose
    // on-disk mtime is never refreshed by L1 hits.
    let now = SystemTime::now();
    for (id, age_secs) in [
        (&hot_id, 600u64),
        (&cold1_id, 300),
        (&cold2_id, 240),
        (&warm_id, 60),
    ] {
        let p = cache.object_path(&scope, ObjectKind::File, id);
        fs::File::options()
            .write(true)
            .open(&p)
            .unwrap()
            .set_modified(now - Duration::from_secs(age_secs))
            .unwrap();
    }

    // Over-budget sweep evicts cold non-L1 entries (`cold1`, `cold2`) before L1-resident `hot`.
    let stats = cache.evict_l2_to_budget(2500).unwrap();
    assert_eq!(stats.evicted_objects, 2);
    assert_eq!(stats.bytes_after, 2000);
    assert!(
        !cache
            .object_path(&scope, ObjectKind::File, &cold1_id)
            .exists()
    );
    assert!(
        !cache
            .object_path(&scope, ObjectKind::File, &cold2_id)
            .exists()
    );
    assert!(
        cache
            .object_path(&scope, ObjectKind::File, &hot_id)
            .exists()
    );
    assert!(
        cache
            .object_path(&scope, ObjectKind::File, &warm_id)
            .exists()
    );

    let l1_hits_before = cache.stats().l1_hits;
    assert_eq!(
        cache.lookup_cached(&scope, ObjectKind::File, &hot_id),
        Some(hot.clone())
    );
    assert_eq!(cache.stats().l1_hits, l1_hits_before + 1);

    // When disk budget is smaller than L1, evicting L1-resident files from L2 disk still
    // preserves their authorization so L1 RAM hits continue working.
    let tight = cache.evict_l2_to_budget(1100).unwrap();
    assert_eq!(tight.evicted_objects, 2);
    assert_eq!(tight.bytes_after, 0);
    assert_eq!(tight.orphaned_markers_removed, 0);
    assert!(
        !cache
            .object_path(&scope, ObjectKind::File, &hot_id)
            .exists()
    );
    assert!(
        !cache
            .object_path(&scope, ObjectKind::File, &warm_id)
            .exists()
    );

    let l1_hits_before_tight = cache.stats().l1_hits;
    let l2_hits_before_tight = cache.stats().l2_hits;
    assert_eq!(
        cache.lookup_cached(&scope, ObjectKind::File, &hot_id),
        Some(hot)
    );
    assert_eq!(
        cache.lookup_cached(&scope, ObjectKind::File, &warm_id),
        Some(warm)
    );
    assert_eq!(cache.stats().l1_hits, l1_hits_before_tight + 2);
    assert_eq!(cache.stats().l2_hits, l2_hits_before_tight);

    // Once `hot` and `warm` are displaced from L1, a subsequent sweep cleans up their now
    // orphaned markers and in-memory authorization entries.
    let newer1 = Bytes::from(vec![5u8; 1000]);
    let newer2 = Bytes::from(vec![6u8; 1000]);
    cache.put_write_through(&scope, ObjectKind::File, &git_blob_id(&newer1), newer1);
    cache.put_write_through(&scope, ObjectKind::File, &git_blob_id(&newer2), newer2);
    let cleanup = cache.evict_l2_to_budget(10_000).unwrap();
    assert_eq!(cleanup.evicted_objects, 0);
    assert_eq!(cleanup.orphaned_markers_removed, 2);
    assert!(!cache.is_authorized(&scope, ObjectKind::File, &hot_id));
    assert!(!cache.is_authorized(&scope, ObjectKind::File, &warm_id));

    // L1-only cached shared objects (`max_l2_object_bytes == 0` or failed L2 write) are never
    // invalidated by under-budget eviction sweeps.
    let dir_l1_only = tempfile::tempdir().unwrap();
    let l1_only =
        TwoTierCache::new_with_l2_object_cap(dir_l1_only.path().to_path_buf(), 1024 * 1024, 0)
            .unwrap();
    let payload = Bytes::from_static(b"l1-only-shared-blob");
    let id = git_blob_id(&payload);
    l1_only.put_write_through(&scope, ObjectKind::File, &id, payload.clone());
    assert!(!l1_only.object_path(&scope, ObjectKind::File, &id).exists());
    let under_budget = l1_only.evict_l2_to_budget(1024 * 1024).unwrap();
    assert_eq!(under_budget.orphaned_markers_removed, 0);
    assert!(l1_only.is_authorized(&scope, ObjectKind::File, &id));
    assert_eq!(
        l1_only.lookup_cached(&scope, ObjectKind::File, &id),
        Some(payload)
    );
    assert_eq!(l1_only.stats().l1_hits, 1);
}

#[test]
fn evict_l2_prioritizes_oversized_files_without_wiping_warm_entries_and_caps_object_size() {
    assert_eq!(
        effective_max_l2_object_bytes(0),
        DEFAULT_MAX_L2_OBJECT_BYTES
    );
    assert_eq!(
        effective_max_l2_object_bytes(10 * 1024 * 1024 * 1024),
        DEFAULT_MAX_L2_OBJECT_BYTES
    );
    let cap_128_mb = effective_max_l2_object_bytes(128 * 1024 * 1024);
    assert_eq!(cap_128_mb, (128 * 1024 * 1024 * 85) / 100);

    let dir = tempfile::tempdir().unwrap();
    // Seed 3 older warm blobs (2000 bytes each = 6000 bytes) and 1 newer oversized blob (9000
    // bytes, larger than the 85% low watermark of 8500 bytes for a 10_000-byte disk budget).
    let cache = TwoTierCache::new(dir.path().to_path_buf(), 0).unwrap();
    let scope = CacheScope::new("http://127.0.0.1:8080", "proj-oversized");

    let now = SystemTime::now();
    let mut warm_paths = Vec::new();
    for i in 0u8..3 {
        let payload = Bytes::from(vec![i + 1; 2000]);
        let id = git_blob_id(&payload);
        cache.put_write_through(&scope, ObjectKind::File, &id, payload);
        let p = cache.object_path(&scope, ObjectKind::File, &id);
        fs::File::options()
            .write(true)
            .open(&p)
            .unwrap()
            .set_modified(now - Duration::from_secs(600 - u64::from(i) * 60))
            .unwrap();
        warm_paths.push(p);
    }

    let oversized = Bytes::from(vec![0xee; 9000]);
    let oversized_id = git_blob_id(&oversized);
    cache.put_write_through(&scope, ObjectKind::File, &oversized_id, oversized);
    let oversized_path = cache.object_path(&scope, ObjectKind::File, &oversized_id);
    assert!(oversized_path.exists());

    // Budget = 10_000 (low watermark = 8_500): evicting the oversized blob first drops usage from
    // 15_000 to 6_000 <= 8_500 without wiping any of the older warm entries.
    let stats = cache.evict_l2_to_budget(10_000).unwrap();
    assert_eq!(stats.bytes_before, 15_000);
    assert_eq!(stats.bytes_after, 6_000);
    assert_eq!(stats.evicted_objects, 1);
    assert_eq!(stats.evicted_bytes, 9_000);
    assert!(
        !oversized_path.exists(),
        "oversized blob must be evicted first"
    );
    for (idx, p) in warm_paths.iter().enumerate() {
        assert!(
            p.exists(),
            "warm blob {idx} must survive oversized eviction"
        );
    }

    // With `effective_max_l2_object_bytes(10_000)` applied, a 9000-byte blob is never written to L2.
    let dir_capped = tempfile::tempdir().unwrap();
    let capped = TwoTierCache::new_with_l2_object_cap(
        dir_capped.path().to_path_buf(),
        0,
        effective_max_l2_object_bytes(10_000),
    )
    .unwrap();
    let big = Bytes::from(vec![0xdd; 9000]);
    let big_id = git_blob_id(&big);
    capped.put_write_through(&scope, ObjectKind::File, &big_id, big);
    assert!(
        !capped
            .object_path(&scope, ObjectKind::File, &big_id)
            .exists(),
        "blob exceeding low-watermark cap must skip L2 caching"
    );
}

#[test]
fn evict_l2_to_budget_cancellable_stops_promptly_and_resets_write_counter_before_walk() {
    let dir = tempfile::tempdir().unwrap();
    let cache = TwoTierCache::new(dir.path().to_path_buf(), 0).unwrap();
    let scope = CacheScope::new("http://127.0.0.1:8080", "proj-cancel");

    let mut paths = Vec::new();
    for i in 0u8..5 {
        let payload = Bytes::from(vec![i + 20; 1000]);
        let id = git_blob_id(&payload);
        cache.put_write_through(&scope, ObjectKind::File, &id, payload);
        paths.push(cache.object_path(&scope, ObjectKind::File, &id));
    }

    // Pre-cancelled sweep exits immediately without modifying files.
    let cancelled = cache.evict_l2_to_budget_cancellable(1000, || true).unwrap();
    assert_eq!(cancelled, EvictionStats::default());
    assert!(paths.iter().all(|p| p.exists()));

    // `l2_bytes_written_since_sweep` is cleared BEFORE walking so concurrent writes recorded
    // while the sweep runs (observed on poll >= 1 during the directory walk) are preserved
    // for the next trigger instead of being zeroed out at the end of the sweep.
    cache
        .inner
        .l2_bytes_written_since_sweep
        .store(9999, Ordering::Relaxed);
    let walk_polls = AtomicUsize::new(0);
    let stats = cache
        .evict_l2_to_budget_cancellable(10_000, || {
            if walk_polls.fetch_add(1, Ordering::Relaxed) == 1 {
                cache.inner.record_l2_bytes_written(777);
            }
            false
        })
        .unwrap();
    assert_eq!(stats.evicted_objects, 0);
    assert!(
        walk_polls.load(Ordering::Relaxed) > 1,
        "sweep should poll cancellation during walk"
    );
    assert_eq!(
        cache
            .inner
            .l2_bytes_written_since_sweep
            .load(Ordering::Relaxed),
        777,
        "bytes written concurrently during sweep must not be discarded when sweep finishes"
    );

    // Cancellation triggered mid-walk stops early without evicting any objects.
    let mid_walk_polls = AtomicUsize::new(0);
    let partial_walk = cache
        .evict_l2_to_budget_cancellable(1000, || {
            mid_walk_polls.fetch_add(1, Ordering::Relaxed) >= 3
        })
        .unwrap();
    assert_eq!(partial_walk.evicted_objects, 0);
    assert!(paths.iter().all(|p| p.exists()));

    // Cancellation triggered mid-eviction stops after the already-evicted object and records
    // partial eviction metrics accurately without deleting remaining files.
    let partial_evict = cache
        .evict_l2_to_budget_cancellable(1000, || paths.iter().any(|p| !p.exists()))
        .unwrap();
    assert_eq!(partial_evict.evicted_objects, 1);
    assert_eq!(partial_evict.evicted_bytes, 1000);
    assert_eq!(paths.iter().filter(|p| p.exists()).count(), 4);
    assert_eq!(cache.stats().l2_evicted_objects, 1);
    assert_eq!(cache.stats().l2_evicted_bytes, 1000);
}
