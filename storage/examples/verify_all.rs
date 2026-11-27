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

//! Crash-test helper (DEV-194): checks that every visible blob under a
//! local-disk store hashes to its own name, after reopening the store.
//! Usage: `cargo run --example verify_all -- <root>`. Exits non-zero on any
//! partial or corrupt blob.

use cc_storage::HashAlgorithm;
use cc_storage::digest::compute;
use cc_storage::local_disk::LocalDiskBlobStore;
use std::path::Path;

fn walk(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            walk(&path, out)
        } else {
            out.push(path)
        }
    }
}

fn main() {
    let root = std::env::args().nth(1).expect("usage: verify_all <root>");
    LocalDiskBlobStore::open(&root).expect("reopen"); // clears tmp/
    let leftovers = std::fs::read_dir(Path::new(&root).join("tmp"))
        .unwrap()
        .count();
    let mut files = Vec::new();
    walk(&Path::new(&root).join("blobs"), &mut files);
    let mut bad = 0;
    for f in &files {
        let alg = match f
            .parent()
            .and_then(|p| p.parent())
            .and_then(|p| p.file_name())
            .and_then(|n| n.to_str())
        {
            Some("sha256") => HashAlgorithm::Sha256,
            Some("sha1") => HashAlgorithm::Sha1,
            other => panic!("unexpected layout at {f:?}: {other:?}"),
        };
        let data = std::fs::read(f).unwrap();
        if compute(alg, &data).to_hex() != f.file_name().unwrap().to_str().unwrap() {
            eprintln!("BAD: {f:?} ({} bytes)", data.len());
            bad += 1;
        }
    }
    println!(
        "visible blobs: {}, bad: {bad}, tmp leftovers after reopen: {leftovers}",
        files.len()
    );
    if bad > 0 || leftovers > 0 {
        std::process::exit(1)
    }
}
