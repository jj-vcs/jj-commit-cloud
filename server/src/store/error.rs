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

//! Typed persistence errors (`StoreError`) and deterministic `tonic::Status` conversions.
//!
//! # Cross-Process and RPC Boundaries
//! Storage implementations return [`StoreError`] variants rather than raw `tonic::Status`
//! values so error mapping stays consistent across `MetadataStore`, `BlobStore`, and
//! `CompositeStore` callers (`NOT_FOUND`, `ALREADY_EXISTS`, `ABORTED`, `FAILED_PRECONDITION`,
//! `INVALID_ARGUMENT`, `OUT_OF_RANGE`, `RESOURCE_EXHAUSTED`, `UNAVAILABLE`).

use thiserror::Error;

pub type StoreResult<T> = Result<T, StoreError>;

#[derive(Clone, Debug, Error)]
pub enum StoreError {
    #[error("repository should have been registered before {action}: {repo_id}")]
    RepoNotFound { repo_id: String, action: String },

    #[error("repository already exists: {0}")]
    RepoAlreadyExists(String),

    #[error("project already exists: {0}")]
    ProjectAlreadyExists(String),

    #[error("operation heads compare-and-swap conflict: {0}")]
    CasConflict(String),

    #[error("{message}")]
    ObjectNotFound { message: String },

    #[error("{parent_kind} references missing {missing_kind} {id_hex}")]
    MissingDependency {
        parent_kind: &'static str,
        missing_kind: &'static str,
        id_hex: String,
    },

    #[error("{0}")]
    FailedPrecondition(String),

    #[error("{0}")]
    InvalidArgument(String),

    #[error("{0}")]
    OutOfRange(String),

    #[error("corrupt persisted metadata: {0}")]
    Corruption(String),

    #[error("storage backend unavailable: {0}")]
    Unavailable(String),

    #[error("storage backend resource exhausted: {0}")]
    ResourceExhausted(String),

    #[error("storage backend permission denied: {0}")]
    PermissionDenied(String),

    #[error("storage backend authentication failed: {0}")]
    Unauthenticated(String),

    #[error("storage backend error: {0}")]
    Backend(String),
}

impl From<StoreError> for tonic::Status {
    fn from(err: StoreError) -> Self {
        match err {
            StoreError::RepoNotFound { .. } | StoreError::ObjectNotFound { .. } => {
                tonic::Status::not_found(err.to_string())
            }
            StoreError::RepoAlreadyExists(_) | StoreError::ProjectAlreadyExists(_) => {
                tonic::Status::already_exists(err.to_string())
            }
            StoreError::CasConflict(_) => tonic::Status::aborted(err.to_string()),
            StoreError::MissingDependency { .. } | StoreError::FailedPrecondition(_) => {
                tonic::Status::failed_precondition(err.to_string())
            }
            StoreError::InvalidArgument(_) => tonic::Status::invalid_argument(err.to_string()),
            StoreError::OutOfRange(_) => tonic::Status::out_of_range(err.to_string()),
            StoreError::Corruption(_) => tonic::Status::data_loss(err.to_string()),
            StoreError::Unavailable(_) => tonic::Status::unavailable(err.to_string()),
            StoreError::ResourceExhausted(_) => tonic::Status::resource_exhausted(err.to_string()),
            StoreError::PermissionDenied(_) => tonic::Status::permission_denied(err.to_string()),
            StoreError::Unauthenticated(_) => tonic::Status::unauthenticated(err.to_string()),
            StoreError::Backend(_) => tonic::Status::internal(err.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    #[test]
    fn backend_failure_classes_keep_distinct_status_codes() {
        let cases = [
            (StoreError::Unavailable("x".into()), Code::Unavailable),
            (
                StoreError::ResourceExhausted("x".into()),
                Code::ResourceExhausted,
            ),
            (
                StoreError::PermissionDenied("x".into()),
                Code::PermissionDenied,
            ),
            (
                StoreError::Unauthenticated("x".into()),
                Code::Unauthenticated,
            ),
            (StoreError::CasConflict("x".into()), Code::Aborted),
            (
                StoreError::FailedPrecondition("x".into()),
                Code::FailedPrecondition,
            ),
            (StoreError::Backend("x".into()), Code::Internal),
        ];
        for (err, code) in cases {
            assert_eq!(tonic::Status::from(err.clone()).code(), code, "{err:?}");
        }
    }
}
