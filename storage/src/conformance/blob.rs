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

//! Conformance suite for [`BlobStore`].

use super::unique;
use crate::digest::compute;
use crate::{BlobKey, BlobStore, ByteRange, ErrorKind, HashAlgorithm, ProjectId, PutOutcome};
use bytes::Bytes;
use std::sync::Arc;

type Store = Arc<dyn BlobStore>;

/// Optional behaviour the suite should expect.
#[derive(Debug, Clone, Copy)]
pub struct Options {
    /// The store implements `delete` (otherwise it must return `Unsupported`).
    pub supports_delete: bool,
}

/// Run every case against `store`.
pub async fn run_all(store: Store, options: Options) {
    put_then_put_again_is_already_present(store.clone()).await;
    wrong_digest_is_rejected_and_not_stored(store.clone()).await;
    sha1_and_sha256_coexist(store.clone()).await;
    read_range_follows_the_contract(store.clone()).await;
    missing_blob_read_is_not_found(store.clone()).await;
    stat_and_find_missing_keep_input_order(store.clone()).await;
    projects_are_separate(store.clone()).await;
    limits_are_enforced(store.clone()).await;
    concurrent_puts_of_one_key_both_succeed(store.clone()).await;
    delete_behaves_as_declared(store.clone(), options).await;
    digest_of_the_wrong_length_is_invalid(store.clone()).await;
}

/// Number of cases `run_all` runs.
pub const CASE_COUNT: usize = 11;

/// Trait: `InvalidArgument` for a digest of the wrong length.
pub async fn digest_of_the_wrong_length_is_invalid(store: Store) {
    let key = BlobKey {
        project: ProjectId(unique("proj")),
        digest: crate::Digest {
            algorithm: HashAlgorithm::Sha256,
            bytes: vec![],
        },
    };
    let e = store
        .put(&key, Bytes::new())
        .await
        .expect_err("empty digest on put");
    assert_eq!(e.kind, ErrorKind::InvalidArgument, "put");
    let e = store
        .stat(std::slice::from_ref(&key))
        .await
        .expect_err("empty digest on stat");
    assert_eq!(e.kind, ErrorKind::InvalidArgument, "stat");
}

fn blob(content: &[u8]) -> (BlobKey, Bytes) {
    blob_in(ProjectId(unique("proj")), content, HashAlgorithm::Sha256)
}

fn blob_in(project: ProjectId, content: &[u8], alg: HashAlgorithm) -> (BlobKey, Bytes) {
    let data = Bytes::copy_from_slice(content);
    (
        BlobKey {
            project,
            digest: compute(alg, &data),
        },
        data,
    )
}

/// Trait: "returns AlreadyPresent without rewriting it".
pub async fn put_then_put_again_is_already_present(store: Store) {
    let (key, data) = blob(unique("hello").as_bytes());
    assert_eq!(
        store.put(&key, data.clone()).await.unwrap(),
        PutOutcome::Created
    );
    assert_eq!(
        store.put(&key, data.clone()).await.unwrap(),
        PutOutcome::AlreadyPresent
    );
    let read = store.read_range(&key, ByteRange::ALL).await.unwrap();
    assert_eq!(read.data, data);
}

/// Trait: "returns DigestMismatch and stores nothing".
pub async fn wrong_digest_is_rejected_and_not_stored(store: Store) {
    let (key, _) = blob(unique("real").as_bytes());
    let e = store
        .put(&key, Bytes::from_static(b"forged"))
        .await
        .expect_err("mismatch");
    assert_eq!(e.kind, ErrorKind::DigestMismatch);
    assert_eq!(
        store
            .find_missing(std::slice::from_ref(&key))
            .await
            .unwrap(),
        vec![key]
    );
}

/// Digests name their algorithm, so both can be stored side by side.
pub async fn sha1_and_sha256_coexist(store: Store) {
    let project = ProjectId(unique("proj"));
    let content = unique("same bytes");
    let (k1, d1) = blob_in(project.clone(), content.as_bytes(), HashAlgorithm::Sha1);
    let (k256, d256) = blob_in(project, content.as_bytes(), HashAlgorithm::Sha256);
    store.put(&k1, d1).await.unwrap();
    store.put(&k256, d256).await.unwrap();
    assert!(store.find_missing(&[k1, k256]).await.unwrap().is_empty());
}

/// Trait: ranges, truncation at the end, empty past the end, total_len.
pub async fn read_range_follows_the_contract(store: Store) {
    let content: Vec<u8> = (0..=255u8).cycle().take(10_000).collect();
    let (key, data) = blob(&content);
    store.put(&key, data.clone()).await.unwrap();
    let cases = [
        (
            ByteRange {
                offset: 0,
                len: Some(10),
            },
            &content[0..10],
        ),
        (
            ByteRange {
                offset: 9_990,
                len: Some(100),
            },
            &content[9_990..],
        ),
        (
            ByteRange {
                offset: 500,
                len: None,
            },
            &content[500..],
        ),
        (
            ByteRange {
                offset: 10_000,
                len: Some(5),
            },
            &content[0..0],
        ),
        (
            ByteRange {
                offset: 20_000,
                len: None,
            },
            &content[0..0],
        ),
    ];
    for (range, expected) in cases {
        let read = store.read_range(&key, range).await.unwrap();
        assert_eq!(&read.data[..], expected, "range {range:?}");
        assert_eq!(read.total_len, 10_000, "total_len for {range:?}");
    }
}

/// Trait: `NotFound`.
pub async fn missing_blob_read_is_not_found(store: Store) {
    let (key, _) = blob(unique("never stored").as_bytes());
    let e = store
        .read_range(&key, ByteRange::ALL)
        .await
        .expect_err("missing");
    assert_eq!(e.kind, ErrorKind::NotFound);
}

/// Trait: "one entry per input in input order".
pub async fn stat_and_find_missing_keep_input_order(store: Store) {
    let (a, da) = blob(unique("a").as_bytes());
    let (b, _) = blob(unique("b").as_bytes());
    let (c, dc) = blob(unique("ccc").as_bytes());
    store.put(&a, da.clone()).await.unwrap();
    store.put(&c, dc.clone()).await.unwrap();
    let keys = [c.clone(), b.clone(), a.clone()];
    let infos = store.stat(&keys).await.unwrap();
    let lens: Vec<Option<u64>> = infos.iter().map(|i| i.map(|i| i.len)).collect();
    assert_eq!(
        lens,
        vec![Some(dc.len() as u64), None, Some(da.len() as u64)]
    );
    assert_eq!(store.find_missing(&keys).await.unwrap(), vec![b]);
}

/// BlobKey doc: "two projects storing the same bytes hold two separate blobs".
pub async fn projects_are_separate(store: Store) {
    let content = unique("shared");
    let (k1, d1) = blob_in(
        ProjectId(unique("p1")),
        content.as_bytes(),
        HashAlgorithm::Sha256,
    );
    let (k2, _) = blob_in(
        ProjectId(unique("p2")),
        content.as_bytes(),
        HashAlgorithm::Sha256,
    );
    store.put(&k1, d1).await.unwrap();
    assert_eq!(
        store.find_missing(&[k1, k2.clone()]).await.unwrap(),
        vec![k2]
    );
}

/// Trait: `LimitExceeded` for blob size, read size and batch size.
pub async fn limits_are_enforced(store: Store) {
    let limits = store.limits();
    if limits.max_blob_bytes <= 64 * 1024 * 1024 {
        let (key, data) = blob(&vec![7u8; limits.max_blob_bytes as usize + 1]);
        let e = store.put(&key, data).await.expect_err("oversized blob");
        assert_eq!(e.kind, ErrorKind::LimitExceeded, "max_blob_bytes");
    }
    if limits.max_read_bytes < limits.max_blob_bytes && limits.max_read_bytes <= 16 * 1024 * 1024 {
        let (key, data) = blob(&vec![1u8; limits.max_read_bytes as usize + 1]);
        store.put(&key, data).await.unwrap();
        let e = store
            .read_range(&key, ByteRange::ALL)
            .await
            .expect_err("oversized read");
        assert_eq!(e.kind, ErrorKind::LimitExceeded, "max_read_bytes");
        let ok = store
            .read_range(
                &key,
                ByteRange {
                    offset: 1,
                    len: None,
                },
            )
            .await
            .expect("read of exactly max_read_bytes");
        assert_eq!(ok.data.len() as u64, limits.max_read_bytes);
    }
    let keys: Vec<BlobKey> = (0..=limits.max_batch).map(|i| blob(&[i as u8]).0).collect();
    let e = store.stat(&keys).await.expect_err("oversized batch");
    assert_eq!(e.kind, ErrorKind::LimitExceeded, "max_batch");
}

/// Trait: "Two concurrent puts of the same key both succeed".
pub async fn concurrent_puts_of_one_key_both_succeed(store: Store) {
    let size = (256 * 1024).min(store.limits().max_read_bytes as usize);
    let (key, data) = blob(&vec![42u8; size]);
    let tasks: Vec<_> = (0..8)
        .map(|_| {
            let (store, key, data) = (store.clone(), key.clone(), data.clone());
            tokio::spawn(async move { store.put(&key, data).await })
        })
        .collect();
    let mut created = 0;
    for t in tasks {
        if t.await.expect("task").expect("put") == PutOutcome::Created {
            created += 1;
        }
    }
    assert!(created >= 1, "at least one put reports Created");
    let read = store.read_range(&key, ByteRange::ALL).await.unwrap();
    assert_eq!(read.data, data, "complete, correct bytes");
}

/// Trait: delete is idempotent, or `Unsupported` where not implemented.
pub async fn delete_behaves_as_declared(store: Store, options: Options) {
    let (key, data) = blob(unique("doomed").as_bytes());
    store.put(&key, data).await.unwrap();
    if options.supports_delete {
        store.delete(&key).await.expect("delete");
        store
            .delete(&key)
            .await
            .expect("delete again is idempotent");
        assert_eq!(
            store
                .find_missing(std::slice::from_ref(&key))
                .await
                .unwrap(),
            vec![key]
        );
    } else {
        let e = store.delete(&key).await.expect_err("delete unsupported");
        assert_eq!(e.kind, ErrorKind::Unsupported);
    }
}
