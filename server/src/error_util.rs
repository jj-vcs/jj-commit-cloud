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

use crate::store::{Store, StoreError};

// Map a storage failure onto a gRPC status so it is surfaced to the client
// instead of being silently swallowed. Read/write/task failures are server-side
// a decode failure means the stored bytes are corrupt (`data_loss`).
impl From<StoreError> for tonic::Status {
    fn from(err: StoreError) -> Self {
        let msg = err.to_string();
        match err {
            StoreError::Decode(_) => tonic::Status::data_loss(msg),
            StoreError::CasConflict(_) => tonic::Status::aborted(msg),
            StoreError::AlreadyExists(_) => tonic::Status::already_exists(msg),
            StoreError::Read(_)
            | StoreError::Write(_)
            | StoreError::Encode(_)
            | StoreError::Task(_) => tonic::Status::internal(msg),
        }
    }
}

pub async fn ensure_project_registered_error(
    store: &dyn Store,
    project_id: &str,
    action: &str,
) -> Result<(), tonic::Status> {
    if !store.is_project_registered(project_id).await? {
        return Err(tonic::Status::not_found(format!(
            "project should have been registered before {action}: {project_id}"
        )));
    }
    Ok(())
}

pub async fn ensure_repo_registered_error(
    store: &dyn Store,
    repo_id: &str,
    action: &str,
) -> Result<(), tonic::Status> {
    if !store.is_repo_registered(repo_id).await? {
        return Err(tonic::Status::not_found(format!(
            "repository should have been registered before {action}: {repo_id}"
        )));
    }
    Ok(())
}
