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

//! Crash-test helper (DEV-194): writes 1 MiB blobs to a local-disk store in a
//! loop until killed. Usage: `cargo run --example write_loop -- <root> [MiB per blob]`.

use bytes::Bytes;
use cc_storage::digest::compute;
use cc_storage::local_disk::LocalDiskBlobStore;
use cc_storage::{BlobKey, BlobStore, HashAlgorithm, ProjectId};

#[tokio::main]
async fn main() {
    let root = std::env::args().nth(1).expect("usage: write_loop <root>");
    let mib: usize = std::env::args()
        .nth(2)
        .map(|s| s.parse().unwrap())
        .unwrap_or(1);
    let store = LocalDiskBlobStore::open(&root).expect("open");
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    // Runs until it is killed: that is the crash test.
    let mut i: u64 = 0;
    loop {
        i = i.wrapping_add(1);
        let mut data = vec![0u8; mib * 1024 * 1024];
        for (j, b) in data.iter_mut().enumerate() {
            *b = (seed as u64 ^ i.wrapping_mul(31) ^ j as u64) as u8;
        }
        let data = Bytes::from(data);
        let key = BlobKey {
            project: ProjectId("crash-test".into()),
            digest: compute(HashAlgorithm::Sha256, &data),
        };
        store.put(&key, data).await.expect("put");
    }
}
