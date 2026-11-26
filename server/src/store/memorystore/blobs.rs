// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Ephemeral in-memory `BlobStore` implementation for tests and local development.
//!
//! # Authority and Crash-Safety Ordering
//! Content-addressed blob writes are insert-only (`or_insert`) in an in-memory map and
//! never overwrite existing digests.
//!
//! # Concurrency and Merge Invariants
//! A single `Mutex` guards the blob map; payloads are reference-counted `Bytes` buffers
//! so reads return cheap zero-copy clones.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;

use super::FileId;
use crate::store::{BlobStore, StoreResult};

#[derive(Debug)]
pub struct MemoryBlobStore {
    location: String,
    blobs: Mutex<HashMap<FileId, Bytes>>,
}

impl Default for MemoryBlobStore {
    fn default() -> Self {
        Self::with_location("memory://default")
    }
}

impl MemoryBlobStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_location(location: impl Into<String>) -> Self {
        Self {
            location: location.into(),
            blobs: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl BlobStore for MemoryBlobStore {
    fn location_id(&self) -> &str {
        &self.location
    }

    async fn put_blob(&self, blob_id: &[u8], data: Bytes) -> StoreResult<()> {
        self.blobs
            .lock()
            .unwrap()
            .entry(blob_id.to_vec())
            .or_insert(data);
        Ok(())
    }

    async fn get_blob(&self, blob_id: &[u8]) -> StoreResult<Option<Bytes>> {
        Ok(self.blobs.lock().unwrap().get(blob_id).cloned())
    }

    async fn has_blob(&self, blob_id: &[u8]) -> StoreResult<bool> {
        Ok(self.blobs.lock().unwrap().contains_key(blob_id))
    }
}
