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

//! DEV-194 acceptance criteria that are specific to the local-disk store.

use bytes::Bytes;
use cc_storage::digest::compute;
use cc_storage::local_disk::LocalDiskBlobStore;
use cc_storage::{BlobKey, BlobStore, ByteRange, HashAlgorithm, ProjectId, PutOutcome};
use std::fs;

fn key(content: &[u8]) -> BlobKey {
    BlobKey {
        project: ProjectId("demo/project".into()),
        digest: compute(HashAlgorithm::Sha256, content),
    }
}

/// "Given a crash in the middle of a write, When the store reopens, Then no
/// half-written blob is visible." A crash before the rename leaves only a
/// temporary file; simulate that and reopen.
#[tokio::test]
async fn interrupted_write_is_invisible_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let content = b"the full content of a blob";
    let k = key(content);
    {
        let _store = LocalDiskBlobStore::open(dir.path()).unwrap();
        // What a crash between write and rename leaves behind:
        fs::write(dir.path().join("tmp").join("1234-0-0.part"), &content[..10]).unwrap();
    }
    let store = LocalDiskBlobStore::open(dir.path()).unwrap();
    assert_eq!(
        store.find_missing(std::slice::from_ref(&k)).await.unwrap(),
        vec![k.clone()]
    );
    assert_eq!(
        fs::read_dir(dir.path().join("tmp")).unwrap().count(),
        0,
        "leftovers removed"
    );
    // And the blob can still be written normally afterwards.
    assert_eq!(
        store.put(&k, Bytes::from_static(content)).await.unwrap(),
        PutOutcome::Created
    );
}

/// Blobs survive closing and reopening the store.
#[tokio::test]
async fn blobs_persist_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let content = b"persistent";
    let k = key(content);
    LocalDiskBlobStore::open(dir.path())
        .unwrap()
        .put(&k, Bytes::from_static(content))
        .await
        .unwrap();
    let store = LocalDiskBlobStore::open(dir.path()).unwrap();
    let read = store.read_range(&k, ByteRange::ALL).await.unwrap();
    assert_eq!(&read.data[..], content);
}

/// The project id may contain any characters; it never escapes the root.
#[tokio::test]
async fn hostile_project_ids_stay_inside_the_root() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalDiskBlobStore::open(dir.path()).unwrap();
    let content = b"x";
    let k = BlobKey {
        project: ProjectId("../../etc".into()),
        digest: compute(HashAlgorithm::Sha256, content),
    };
    store.put(&k, Bytes::from_static(content)).await.unwrap();
    assert!(store.path_of(&k).starts_with(dir.path().join("blobs")));
}

/// One process per root: a second open fails instead of deleting the first
/// process's in-flight writes.
#[tokio::test]
async fn second_open_of_the_same_root_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let _first = LocalDiskBlobStore::open(dir.path()).unwrap();
    let second = LocalDiskBlobStore::open(dir.path());
    assert_eq!(
        second.err().map(|e| e.kind),
        Some(cc_storage::ErrorKind::Unavailable)
    );
}

/// Project ids longer than a file name can hold still work.
#[tokio::test]
async fn very_long_project_ids_work() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalDiskBlobStore::open(dir.path()).unwrap();
    let content = b"long";
    let k = BlobKey {
        project: ProjectId("p".repeat(500)),
        digest: compute(HashAlgorithm::Sha256, content),
    };
    assert_eq!(
        store.put(&k, Bytes::from_static(content)).await.unwrap(),
        PutOutcome::Created
    );
    assert_eq!(
        &store.read_range(&k, ByteRange::ALL).await.unwrap().data[..],
        content
    );
}
