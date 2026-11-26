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

#![allow(
    clippy::redundant_closure,
    clippy::result_large_err,
    clippy::unnecessary_lazy_evaluations,
    clippy::useless_conversion
)]

pub mod cc_backend;
pub mod cc_index_store;
pub mod cc_op_heads_store;
pub mod cc_op_store;
pub mod cc_working_copy;
pub mod client;
pub mod git_importer;
pub mod repo;
pub mod util;

pub use repo::StoreFactoriesExt;
