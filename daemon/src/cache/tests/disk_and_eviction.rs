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

#[cfg(unix)]
use super::super::eviction::is_pid_dead;
use super::super::l2::create_dir_all_private;
use super::super::{
    CACHE_STREAM_CHUNK_BYTES, CacheScope, CacheStatsSnapshot, MAX_L1_OBJECT_BYTES, ObjectKind,
    TwoTierCache,
};
use super::{collect_range_chunks, git_blob_id};
use bytes::Bytes;

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
    let marker_path = cache
        .inner
        .scoped_path(scope.scope_hex(), ObjectKind::File, &id);

    for dir_path in [
        cache_root.as_path(),
        cache_root.join("objects").as_path(),
        cache_root.join("scopes").as_path(),
        cache_root.join("staging").as_path(),
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
