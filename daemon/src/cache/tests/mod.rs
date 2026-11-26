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

use bytes::Bytes;
use cc_common::backend::{ReadTreeResponse, TreeEntry};
use cc_common::op_store::View;
use prost::Message;
use sha1::{Digest, Sha1};
use tokio::sync::mpsc;

mod core;
mod disk_and_eviction;

fn tree_payload(id: &[u8], label: &str) -> Bytes {
    let tree = ReadTreeResponse {
        tree_id: id.to_vec(),
        entries: vec![TreeEntry {
            name: label.to_string(),
            value: None,
        }],
        next_page_token: String::new(),
    };
    Bytes::from(tree.encode_to_vec())
}

fn view_payload(head_byte: u8) -> Bytes {
    let view = View {
        head_ids: vec![vec![head_byte; 20]],
        ..Default::default()
    };
    Bytes::from(view.encode_to_vec())
}

fn git_blob_id(content: &[u8]) -> Vec<u8> {
    let mut hasher = Sha1::new();
    hasher.update(format!("blob {}\0", content.len()).as_bytes());
    hasher.update(content);
    hasher.finalize().to_vec()
}

async fn collect_range_chunks(
    mut rx: mpsc::Receiver<Result<Vec<u8>, std::io::Error>>,
) -> Result<Vec<Vec<u8>>, std::io::Error> {
    let mut chunks = Vec::new();
    while let Some(chunk) = rx.recv().await {
        chunks.push(chunk?);
    }
    Ok(chunks)
}
