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

use crate::cc_backend::CommitCloudBackend;
use crate::cc_index_store::CommitCloudIndexStore;
use crate::cc_op_heads_store::CommitCloudOpHeadsStore;
use crate::cc_op_store::CommitCloudOpStore;
use jj_lib::backend::{Backend, BackendLoadError};
use jj_lib::index_store::IndexStore;
use jj_lib::op_heads_store::OpHeadsStore;
use jj_lib::op_store::OpStore;
use jj_lib::repo::StoreFactories;

pub trait StoreFactoriesExt {
    fn add_commit_cloud(&mut self);
}

impl StoreFactoriesExt for StoreFactories {
    fn add_commit_cloud(&mut self) {
        self.add_backend(
            "commit_cloud",
            Box::new(|_settings, store_path| {
                let backend =
                    CommitCloudBackend::load(store_path).map_err(|e| BackendLoadError(e.into()))?;
                Ok(Box::new(backend) as Box<dyn Backend>)
            }),
        );

        self.add_op_store(
            "commit_cloud",
            Box::new(|_settings, store_path, _root_op_data| {
                let op_store =
                    CommitCloudOpStore::load(store_path).map_err(|e| BackendLoadError(e.into()))?;
                Ok(Box::new(op_store) as Box<dyn OpStore>)
            }),
        );

        self.add_op_heads_store(
            "commit_cloud",
            Box::new(|_settings, store_path| {
                let op_heads_store = CommitCloudOpHeadsStore::load(store_path)
                    .map_err(|e| BackendLoadError(e.into()))?;
                Ok(Box::new(op_heads_store) as Box<dyn OpHeadsStore>)
            }),
        );

        self.add_index_store(
            "commit_cloud",
            Box::new(|_settings, store_path| {
                let index_store = CommitCloudIndexStore::load(store_path)
                    .map_err(|e| BackendLoadError(e.into()))?;
                Ok(Box::new(index_store) as Box<dyn IndexStore>)
            }),
        );
    }
}
