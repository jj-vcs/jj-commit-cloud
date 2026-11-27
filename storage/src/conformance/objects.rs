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

//! Conformance suite for [`ObjectStore`]: the object half of the storage
//! contract, which a cache implements on its own. The [`super::metadata`]
//! suite runs these cases too, so every `MetadataStore` passes them as well.
//!
//! The cases write only objects with fresh ids, so they can share one pair
//! of scopes and run against a shared database.

use super::unique;
use crate::digest::compute;
use crate::{ErrorKind, HashAlgorithm, ObjectKind, ObjectRecord, ObjectRef, ObjectStore, Scope};
use bytes::Bytes;
use std::sync::Arc;

type Store = Arc<dyn ObjectStore>;

/// Where the cases write. The store must accept objects in both: a store
/// that tracks projects and workspaces needs them to exist first, while a
/// cache accepts any.
#[derive(Debug, Clone)]
pub struct Scopes {
    /// For commits, trees, file manifests and symlinks.
    pub project: Scope,
    /// For operations and views.
    pub workspace: Scope,
}

/// Run every case against `store`, writing into `scopes`.
pub async fn run_all(store: Store, scopes: Scopes) {
    put_then_get_returns_objects_in_input_order(&store, &scopes).await;
    put_is_idempotent(&store, &scopes).await;
    put_with_different_bytes_fails_and_keeps_stored_bytes(&store, &scopes).await;
    kind_must_match_scope(&store, &scopes).await;
    find_missing_returns_exactly_the_absent(&store, &scopes).await;
    limits_are_enforced(&store, &scopes).await;
    two_versions_of_one_id_in_a_batch_fail(&store, &scopes).await;
    same_bytes_with_different_refs_fail(&store, &scopes).await;
    batch_limits_apply_to_put_and_get(&store, &scopes).await;
    virtual_roots_are_never_missing(&store, &scopes).await;
    blob_refs_are_kept(&store, &scopes).await;
    same_bytes_with_different_blob_refs_fail(&store, &scopes).await;
    digests_of_the_wrong_length_are_refused(&store, &scopes).await;
}

/// Number of cases `run_all` runs.
pub const CASE_COUNT: usize = 13;

fn record(kind: ObjectKind, data: &str, refs: Vec<ObjectRef>) -> ObjectRecord {
    let data = Bytes::from(data.to_owned());
    ObjectRecord {
        kind,
        id: compute(HashAlgorithm::Sha256, &data),
        data,
        refs,
        blob_refs: vec![],
    }
}

fn r(rec: &ObjectRecord) -> ObjectRef {
    ObjectRef {
        kind: rec.kind,
        id: rec.id.clone(),
    }
}

/// Trait: "one entry per input, in input order; None where not stored".
pub async fn put_then_get_returns_objects_in_input_order(store: &Store, ws: &Scopes) {
    let a = record(ObjectKind::Commit, &unique("a"), vec![]);
    let b = record(ObjectKind::Tree, &unique("b"), vec![]);
    let absent = record(ObjectKind::Commit, &unique("absent"), vec![]);
    store
        .put_objects(&ws.project, vec![a.clone(), b.clone()])
        .await
        .unwrap();
    let got = store
        .get_objects(&ws.project, &[r(&b), r(&absent), r(&a)])
        .await
        .unwrap();
    assert_eq!(
        got,
        vec![Some(b), None, Some(a)],
        "get_objects: order and None for absent"
    );
}

/// Trait: "an object already stored with identical data is a no-op".
pub async fn put_is_idempotent(store: &Store, ws: &Scopes) {
    let a = record(ObjectKind::Commit, &unique("a"), vec![]);
    store
        .put_objects(&ws.project, vec![a.clone()])
        .await
        .unwrap();
    store
        .put_objects(&ws.project, vec![a.clone()])
        .await
        .expect("second identical put");
    let got = store.get_objects(&ws.project, &[r(&a)]).await.unwrap();
    assert_eq!(got, vec![Some(a)]);
}

/// DEV-89: "the write fails and the stored bytes are unchanged".
pub async fn put_with_different_bytes_fails_and_keeps_stored_bytes(store: &Store, ws: &Scopes) {
    let a = record(ObjectKind::Commit, &unique("a"), vec![]);
    store
        .put_objects(&ws.project, vec![a.clone()])
        .await
        .unwrap();
    let mut forged = a.clone();
    forged.data = Bytes::from_static(b"different bytes");
    let e = store
        .put_objects(&ws.project, vec![forged])
        .await
        .expect_err("mismatch must fail");
    assert_eq!(e.kind, ErrorKind::ObjectMismatch, "error kind");
    let got = store.get_objects(&ws.project, &[r(&a)]).await.unwrap();
    assert_eq!(got, vec![Some(a)], "stored bytes unchanged");
}

/// Trait: "InvalidArgument if a reference's kind does not match scope".
pub async fn kind_must_match_scope(store: &Store, ws: &Scopes) {
    let op = record(ObjectKind::Operation, &unique("op"), vec![]);
    let e = store
        .put_objects(&ws.project, vec![op.clone()])
        .await
        .expect_err("op in project");
    assert_eq!(e.kind, ErrorKind::InvalidArgument);
    let commit = record(ObjectKind::Commit, &unique("c"), vec![]);
    let e = store
        .get_objects(&ws.workspace, &[r(&commit)])
        .await
        .expect_err("commit in ws");
    assert_eq!(e.kind, ErrorKind::InvalidArgument);
}

/// DEV-89: "1,000 ids of which 10 are absent ... returns exactly the 10".
pub async fn find_missing_returns_exactly_the_absent(store: &Store, ws: &Scopes) {
    let n = store.limits().max_batch.min(1_000);
    let all: Vec<ObjectRecord> = (0..n)
        .map(|i| record(ObjectKind::Tree, &format!("{} {i}", unique("t")), vec![]))
        .collect();
    let absent_every = (n / 10).max(1);
    let (absent, present): (Vec<_>, Vec<_>) = all
        .iter()
        .enumerate()
        .partition(|(i, _)| i % absent_every == 0);
    let present: Vec<ObjectRecord> = present.into_iter().map(|(_, o)| o.clone()).collect();
    for chunk in present.chunks(store.limits().max_batch) {
        store
            .put_objects(&ws.project, chunk.to_vec())
            .await
            .unwrap();
    }
    let refs: Vec<ObjectRef> = all.iter().map(r).collect();
    let missing = store.find_missing(&ws.project, &refs).await.unwrap();
    let expected: Vec<ObjectRef> = absent.into_iter().map(|(_, o)| r(o)).collect();
    assert_eq!(missing, expected, "exactly the absent ids, in input order");
}

/// Trait: `LimitExceeded` for batch size and object size.
pub async fn limits_are_enforced(store: &Store, ws: &Scopes) {
    let limits = store.limits();
    let big = ObjectRecord {
        kind: ObjectKind::Tree,
        id: compute(HashAlgorithm::Sha256, unique("big").as_bytes()),
        data: Bytes::from(vec![0u8; limits.max_object_bytes + 1]),
        refs: vec![],
        blob_refs: vec![],
    };
    let e = store
        .put_objects(&ws.project, vec![big])
        .await
        .expect_err("oversized object");
    assert_eq!(e.kind, ErrorKind::LimitExceeded, "tree size guard (DEV-89)");
    let refs: Vec<ObjectRef> = (0..=limits.max_batch)
        .map(|i| ObjectRef {
            kind: ObjectKind::Commit,
            id: compute(HashAlgorithm::Sha256, format!("x{i}").as_bytes()),
        })
        .collect();
    let e = store
        .find_missing(&ws.project, &refs)
        .await
        .expect_err("oversized batch");
    assert_eq!(e.kind, ErrorKind::LimitExceeded);
}

/// Trait: "two different versions of one id inside the batch" fail.
pub async fn two_versions_of_one_id_in_a_batch_fail(store: &Store, ws: &Scopes) {
    let a = record(ObjectKind::Commit, &unique("a"), vec![]);
    let mut b = a.clone();
    b.data = Bytes::from_static(b"other bytes, same id");
    let e = store
        .put_objects(&ws.project, vec![a, b])
        .await
        .expect_err("two versions");
    assert_eq!(e.kind, ErrorKind::ObjectMismatch);
}

/// Trait: "different data or refs" fails; refs feed the closure check.
pub async fn same_bytes_with_different_refs_fail(store: &Store, ws: &Scopes) {
    let tree = record(ObjectKind::Tree, &unique("t"), vec![]);
    let a = record(ObjectKind::Commit, &unique("c"), vec![]);
    store
        .put_objects(&ws.project, vec![a.clone()])
        .await
        .unwrap();
    let mut b = a.clone();
    b.refs = vec![r(&tree)];
    let e = store
        .put_objects(&ws.project, vec![b])
        .await
        .expect_err("refs differ");
    assert_eq!(e.kind, ErrorKind::ObjectMismatch);
}

/// Trait: `LimitExceeded` for batch count on put and get, and batch bytes.
pub async fn batch_limits_apply_to_put_and_get(store: &Store, ws: &Scopes) {
    let limits = store.limits();
    let many: Vec<ObjectRecord> = (0..=limits.max_batch)
        .map(|i| record(ObjectKind::Tree, &format!("{} {i}", unique("m")), vec![]))
        .collect();
    let e = store
        .put_objects(&ws.project, many.clone())
        .await
        .expect_err("put count");
    assert_eq!(e.kind, ErrorKind::LimitExceeded, "put_objects max_batch");
    let refs: Vec<ObjectRef> = many.iter().map(r).collect();
    let e = store
        .get_objects(&ws.project, &refs)
        .await
        .expect_err("get count");
    assert_eq!(e.kind, ErrorKind::LimitExceeded, "get_objects max_batch");
    let per = limits.max_object_bytes;
    let n = limits.max_batch_bytes / per + 1;
    if n <= limits.max_batch && n * per <= 64 * 1024 * 1024 {
        let heavy: Vec<ObjectRecord> = (0..n)
            .map(|i| ObjectRecord {
                kind: ObjectKind::Tree,
                id: compute(
                    HashAlgorithm::Sha256,
                    format!("{} {i}", unique("h")).as_bytes(),
                ),
                data: Bytes::from(vec![i as u8; per]),
                refs: vec![],
                blob_refs: vec![],
            })
            .collect();
        let e = store
            .put_objects(&ws.project, heavy)
            .await
            .expect_err("batch bytes");
        assert_eq!(e.kind, ErrorKind::LimitExceeded, "max_batch_bytes");
    }
}

/// Trait: "Virtual roots are never missing", in either scope.
pub async fn virtual_roots_are_never_missing(store: &Store, ws: &Scopes) {
    for root in store.virtual_roots() {
        let scope = if root.kind.is_project_scoped() {
            &ws.project
        } else {
            &ws.workspace
        };
        let missing = store
            .find_missing(scope, std::slice::from_ref(&root))
            .await
            .unwrap();
        assert!(missing.is_empty(), "virtual root {root:?} reported missing");
    }
}

fn blob(name: &str) -> crate::Digest {
    compute(HashAlgorithm::Sha256, unique(name).as_bytes())
}

/// Trait: the store keeps `blob_refs` and returns them unchanged.
pub async fn blob_refs_are_kept(store: &Store, ws: &Scopes) {
    let mut manifest = record(ObjectKind::FileManifest, &unique("file"), vec![]);
    manifest.blob_refs = vec![blob("contents"), blob("more contents")];
    store
        .put_objects(&ws.project, vec![manifest.clone()])
        .await
        .unwrap();
    let got = store
        .get_objects(&ws.project, &[r(&manifest)])
        .await
        .unwrap();
    assert_eq!(got, vec![Some(manifest)], "blob_refs round trip");
}

/// Trait: "different data, refs or blob_refs" fails.
pub async fn same_bytes_with_different_blob_refs_fail(store: &Store, ws: &Scopes) {
    let mut manifest = record(ObjectKind::FileManifest, &unique("file"), vec![]);
    manifest.blob_refs = vec![blob("contents")];
    store
        .put_objects(&ws.project, vec![manifest.clone()])
        .await
        .unwrap();
    manifest.blob_refs = vec![blob("other contents")];
    let e = store
        .put_objects(&ws.project, vec![manifest])
        .await
        .expect_err("blob_refs differ");
    assert_eq!(e.kind, ErrorKind::ObjectMismatch);
}

/// Every digest in a record, its id, refs and blob refs, must be as long as
/// its algorithm's output. A store that kept a malformed one could not read
/// it back.
pub async fn digests_of_the_wrong_length_are_refused(store: &Store, ws: &Scopes) {
    let good = record(ObjectKind::Commit, &unique("target"), vec![]);
    let mut long = good.id.clone();
    long.bytes.resize(300, 0);

    let mut bad_id = record(ObjectKind::Commit, &unique("bad id"), vec![]);
    bad_id.id = long.clone();
    let bad_ref = record(
        ObjectKind::Commit,
        &unique("bad ref"),
        vec![ObjectRef {
            kind: ObjectKind::Tree,
            id: long.clone(),
        }],
    );
    let mut bad_blob_ref = record(ObjectKind::FileManifest, &unique("bad blob"), vec![]);
    bad_blob_ref.blob_refs = vec![long];

    for (what, rec) in [("id", bad_id), ("ref", bad_ref), ("blob ref", bad_blob_ref)] {
        let e = store
            .put_objects(&ws.project, vec![rec.clone()])
            .await
            .expect_err(what);
        assert_eq!(e.kind, ErrorKind::InvalidArgument, "{what}");
        // A malformed id cannot be read back either; a store may refuse it.
        if rec.id.check_len().is_ok() {
            let got = store.get_objects(&ws.project, &[r(&rec)]).await.unwrap();
            assert_eq!(got, vec![None], "{what}: nothing stored");
        }
    }
}
