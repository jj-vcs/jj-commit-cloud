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
//! `INVALID_ARGUMENT`, `OUT_OF_RANGE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`,
//! `UNAVAILABLE`), with outgoing error messages sanitized and length-bounded for HTTP/2.

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

    #[error("request deadline exceeded: {0}")]
    DeadlineExceeded(String),

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
            // Client-actionable errors preserve sanitized, wire-length-bounded messages.
            err @ (StoreError::RepoNotFound { .. } | StoreError::ObjectNotFound { .. }) => {
                tonic::Status::not_found(crate::resilience::sanitize_grpc_error_message(
                    &err.to_string(),
                ))
            }
            err @ (StoreError::RepoAlreadyExists(_) | StoreError::ProjectAlreadyExists(_)) => {
                tonic::Status::already_exists(crate::resilience::sanitize_grpc_error_message(
                    &err.to_string(),
                ))
            }
            err @ StoreError::CasConflict(_) => tonic::Status::aborted(
                crate::resilience::sanitize_grpc_error_message(&err.to_string()),
            ),
            err @ (StoreError::MissingDependency { .. } | StoreError::FailedPrecondition(_)) => {
                tonic::Status::failed_precondition(crate::resilience::sanitize_grpc_error_message(
                    &err.to_string(),
                ))
            }
            err @ StoreError::InvalidArgument(_) => tonic::Status::invalid_argument(
                crate::resilience::sanitize_grpc_error_message(&err.to_string()),
            ),
            err @ StoreError::OutOfRange(_) => tonic::Status::out_of_range(
                crate::resilience::sanitize_grpc_error_message(&err.to_string()),
            ),
            // Infrastructure/backend errors log full details server-side and return generic
            // messages so SQLSTATEs, schema details, filesystem paths, and URLs never leak.
            StoreError::Corruption(detail) => {
                tracing::error!(error = %detail, "Storage corruption detected");
                tonic::Status::data_loss("corrupt persisted metadata")
            }
            StoreError::DeadlineExceeded(detail) => {
                tracing::warn!(error = %detail, "Storage operation exceeded deadline");
                tonic::Status::deadline_exceeded("request deadline exceeded")
            }
            StoreError::Unavailable(detail) => {
                tracing::warn!(error = %detail, "Storage backend unavailable");
                tonic::Status::unavailable("storage backend unavailable")
            }
            StoreError::ResourceExhausted(detail) => {
                tracing::warn!(error = %detail, "Storage backend resource exhausted");
                tonic::Status::resource_exhausted("storage backend resource exhausted")
            }
            StoreError::PermissionDenied(detail) => {
                tracing::error!(error = %detail, "Storage backend permission denied");
                tonic::Status::permission_denied("storage backend permission denied")
            }
            StoreError::Unauthenticated(detail) => {
                // Server-side storage credential failures must not emit client-facing
                // `UNAUTHENTICATED` (`grpc-status: 16`), which misleads callers about their
                // own token and suppresses `MetricsLayer` latency recording.
                tracing::error!(error = %detail, "Storage backend authentication failed");
                tonic::Status::internal("storage backend error")
            }
            StoreError::Backend(detail) => {
                tracing::error!(error = %detail, "Storage backend error");
                tonic::Status::internal("storage backend error")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resilience::MAX_GRPC_ERROR_MESSAGE_BYTES;
    use tonic::Code;

    #[test]
    fn backend_failure_classes_keep_distinct_status_codes() {
        let cases = [
            (
                StoreError::DeadlineExceeded("x".into()),
                Code::DeadlineExceeded,
            ),
            (StoreError::Unavailable("x".into()), Code::Unavailable),
            (
                StoreError::ResourceExhausted("x".into()),
                Code::ResourceExhausted,
            ),
            (
                StoreError::PermissionDenied("x".into()),
                Code::PermissionDenied,
            ),
            // Backend auth errors map to INTERNAL so clients do not misread them as caller
            // token rejections and `MetricsLayer` still records RPC latency.
            (StoreError::Unauthenticated("x".into()), Code::Internal),
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

    #[test]
    fn status_conversion_sanitizes_client_errors_and_redacts_internal_storage_errors() {
        let raw = format!("bad input\r\nwith\0controls {}", "x".repeat(4000));
        let client_status = tonic::Status::from(StoreError::InvalidArgument(raw));
        assert_eq!(client_status.code(), Code::InvalidArgument);
        assert!(client_status.message().len() <= MAX_GRPC_ERROR_MESSAGE_BYTES);
        assert!(
            !client_status
                .message()
                .bytes()
                .any(|b| b.is_ascii_control())
        );
        assert!(client_status.message().ends_with("..."));

        let secret_detail = "SQLSTATE 42P01 relation \"secret_tbl\" at /var/lib/pg/data https://internal.bucket/xml";
        for (err, expected_code) in [
            (StoreError::Backend(secret_detail.into()), Code::Internal),
            (
                StoreError::Unavailable(secret_detail.into()),
                Code::Unavailable,
            ),
            (StoreError::Corruption(secret_detail.into()), Code::DataLoss),
            (
                StoreError::ResourceExhausted(secret_detail.into()),
                Code::ResourceExhausted,
            ),
            (
                StoreError::PermissionDenied(secret_detail.into()),
                Code::PermissionDenied,
            ),
            (
                StoreError::Unauthenticated(secret_detail.into()),
                Code::Internal,
            ),
            (
                StoreError::DeadlineExceeded(secret_detail.into()),
                Code::DeadlineExceeded,
            ),
        ] {
            let status = tonic::Status::from(err);
            assert_eq!(status.code(), expected_code);
            assert!(
                !status.message().contains("secret_tbl")
                    && !status.message().contains("/var/lib/pg")
                    && !status.message().contains("internal.bucket")
                    && !status.message().contains("SQLSTATE"),
                "leaked internal storage detail in {status:?}"
            );
        }
    }
}
