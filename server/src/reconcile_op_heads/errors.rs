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

//! Error classification and transient-retry detection for op-head reconciliation.
//!
//! # Cross-Process and RPC Boundaries
//! Unwraps nested `jj_lib` backend, op-store, index, and repo-loader error chains to
//! preserve typed [`StoreError`] gRPC status codes while distinguishing retryable
//! transient failures from deterministic corruption, precondition, or deadline errors.

use std::error::Error as StdError;

use jj_lib::backend::BackendError;
use jj_lib::index::IndexError;
use jj_lib::index_store::IndexStoreError;
use jj_lib::op_heads_store::OpHeadsStoreError;
use jj_lib::op_store::OpStoreError;
use jj_lib::repo::RepoLoaderError;
use jj_lib::transaction::TransactionCommitError;

use crate::jj_lib_adapters::IndexWalkBudgetExceeded;
use crate::store::StoreError;

#[derive(Debug)]
pub(super) struct ReconcileError {
    pub(super) status: tonic::Status,
    pub(super) transient: bool,
}

impl ReconcileError {
    pub(super) fn from_store(err: StoreError) -> Self {
        let transient = is_transient_store_error(&err);
        Self {
            status: tonic::Status::from(err),
            transient,
        }
    }

    pub(super) fn from_op_store(err: OpStoreError) -> Self {
        if let Some(store_err) = find_typed_error_in_op_store::<StoreError>(&err) {
            return Self::from_store(store_err.clone());
        }
        match err {
            OpStoreError::ObjectNotFound { .. } => Self {
                status: tonic::Status::not_found(err.to_string()),
                transient: false,
            },
            other => {
                let msg = other.to_string();
                let transient = is_transient_reconcile_error(&msg);
                Self {
                    status: tonic::Status::internal(msg),
                    transient,
                }
            }
        }
    }

    pub(super) fn from_index(err: IndexError) -> Self {
        if let Some(store_err) = find_typed_error_in_index::<StoreError>(&err) {
            return Self::from_store(store_err.clone());
        }
        if is_walk_budget_exceeded_in_index(&err) {
            return Self {
                status: tonic::Status::resource_exhausted(err.to_string()),
                transient: false,
            };
        }
        let msg = err.to_string();
        let transient = is_transient_reconcile_error(&msg);
        Self {
            status: tonic::Status::internal(msg),
            transient,
        }
    }

    pub(super) fn from_repo_loader(err: RepoLoaderError) -> Self {
        if let Some(store_err) = find_typed_error_in_repo_loader::<StoreError>(&err) {
            return Self::from_store(store_err.clone());
        }
        if is_walk_budget_exceeded_in_repo_loader(&err) {
            return Self {
                status: tonic::Status::resource_exhausted(err.to_string()),
                transient: false,
            };
        }
        match &err {
            RepoLoaderError::Backend(BackendError::ObjectNotFound { .. })
            | RepoLoaderError::OpStore(OpStoreError::ObjectNotFound { .. })
            | RepoLoaderError::TransactionCommit(TransactionCommitError::OpStore(
                OpStoreError::ObjectNotFound { .. },
            )) => Self {
                status: tonic::Status::not_found(err.to_string()),
                transient: false,
            },
            RepoLoaderError::Backend(BackendError::Unsupported(_)) => Self {
                status: tonic::Status::unimplemented(err.to_string()),
                transient: false,
            },
            _ => {
                let msg = err.to_string();
                let transient = is_transient_reconcile_error(&msg);
                Self {
                    status: tonic::Status::internal(msg),
                    transient,
                }
            }
        }
    }
}

#[cfg(test)]
pub(super) fn find_store_error<'a>(err: &'a (dyn StdError + 'static)) -> Option<&'a StoreError> {
    find_typed_error(err)
}

pub(super) fn is_walk_budget_exceeded_in_index(err: &IndexError) -> bool {
    find_typed_error_in_index::<IndexWalkBudgetExceeded>(err).is_some()
}

pub(super) fn is_walk_budget_exceeded_in_repo_loader(err: &RepoLoaderError) -> bool {
    find_typed_error_in_repo_loader::<IndexWalkBudgetExceeded>(err).is_some()
}

fn find_typed_error<'a, T: StdError + 'static>(err: &'a (dyn StdError + 'static)) -> Option<&'a T> {
    let mut current: Option<&'a (dyn StdError + 'static)> = Some(err);
    while let Some(e) = current {
        if let Some(typed) = e.downcast_ref::<T>() {
            return Some(typed);
        }
        if let Some(index_err) = e.downcast_ref::<IndexError>() {
            if let Some(found) = find_typed_error_in_index(index_err) {
                return Some(found);
            }
        } else if let Some(backend_err) = e.downcast_ref::<BackendError>() {
            if let Some(found) = find_typed_error_in_backend(backend_err) {
                return Some(found);
            }
        } else if let Some(op_store_err) = e.downcast_ref::<OpStoreError>() {
            if let Some(found) = find_typed_error_in_op_store(op_store_err) {
                return Some(found);
            }
        } else if let Some(index_store_err) = e.downcast_ref::<IndexStoreError>() {
            if let Some(found) = find_typed_error_in_index_store(index_store_err) {
                return Some(found);
            }
        } else if let Some(op_heads_err) = e.downcast_ref::<OpHeadsStoreError>() {
            if let Some(found) = find_typed_error_in_op_heads(op_heads_err) {
                return Some(found);
            }
        } else if let Some(loader_err) = e.downcast_ref::<RepoLoaderError>()
            && let Some(found) = find_typed_error_in_repo_loader(loader_err)
        {
            return Some(found);
        }
        current = e.source();
    }
    None
}

// `jj_lib` uses `#[error(transparent)]` on `Other(Box<dyn Error + Send + Sync>)` and
// `RepoLoaderError` / `TransactionCommitError`, which skips the inner boxed error in
// `Error::source()`. Match those variants explicitly so typed errors are preserved.
fn find_typed_error_in_op_store<T: StdError + 'static>(err: &OpStoreError) -> Option<&T> {
    match err {
        OpStoreError::Other(inner)
        | OpStoreError::ReadObject { source: inner, .. }
        | OpStoreError::WriteObject { source: inner, .. } => find_typed_error(inner.as_ref()),
        OpStoreError::ObjectNotFound {
            source: Some(inner),
            ..
        } => find_typed_error(inner.as_ref()),
        OpStoreError::ObjectNotFound { source: None, .. } => None,
    }
}

fn find_typed_error_in_backend<T: StdError + 'static>(err: &BackendError) -> Option<&T> {
    match err {
        BackendError::Other(inner)
        | BackendError::ReadObject { source: inner, .. }
        | BackendError::ReadFile { source: inner, .. }
        | BackendError::WriteObject { source: inner, .. } => find_typed_error(inner.as_ref()),
        BackendError::ObjectNotFound {
            source: Some(inner),
            ..
        } => find_typed_error(inner.as_ref()),
        BackendError::ObjectNotFound { source: None, .. } => None,
        _ => err.source().and_then(find_typed_error),
    }
}

fn find_typed_error_in_index<T: StdError + 'static>(err: &IndexError) -> Option<&T> {
    match err {
        IndexError::Other(inner) => find_typed_error(inner.as_ref()),
        _ => err.source().and_then(find_typed_error),
    }
}

fn find_typed_error_in_index_store<T: StdError + 'static>(err: &IndexStoreError) -> Option<&T> {
    match err {
        IndexStoreError::Read(inner) | IndexStoreError::Write(inner) => {
            find_typed_error(inner.as_ref())
        }
    }
}

fn find_typed_error_in_op_heads<T: StdError + 'static>(err: &OpHeadsStoreError) -> Option<&T> {
    match err {
        OpHeadsStoreError::Read(inner)
        | OpHeadsStoreError::Write { source: inner, .. }
        | OpHeadsStoreError::Lock(inner) => find_typed_error(inner.as_ref()),
    }
}

fn find_typed_error_in_repo_loader<T: StdError + 'static>(err: &RepoLoaderError) -> Option<&T> {
    match err {
        RepoLoaderError::Backend(e) => find_typed_error_in_backend(e),
        RepoLoaderError::Index(e) => find_typed_error_in_index(e),
        RepoLoaderError::IndexStore(e) => find_typed_error_in_index_store(e),
        RepoLoaderError::OpHeadsStoreError(e) => find_typed_error_in_op_heads(e),
        RepoLoaderError::OpStore(e) => find_typed_error_in_op_store(e),
        RepoLoaderError::TransactionCommit(e) => match e {
            TransactionCommitError::Index(e) => find_typed_error_in_index(e),
            TransactionCommitError::IndexStore(e) => find_typed_error_in_index_store(e),
            TransactionCommitError::OpHeadsStore(e) => find_typed_error_in_op_heads(e),
            TransactionCommitError::OpStore(e) => find_typed_error_in_op_store(e),
        },
    }
}

pub(super) fn is_transient_store_error(err: &StoreError) -> bool {
    match err {
        StoreError::CasConflict(_) | StoreError::Unavailable(_) => true,
        StoreError::Backend(msg) => is_transient_reconcile_error(msg),
        StoreError::RepoNotFound { .. }
        | StoreError::RepoAlreadyExists(_)
        | StoreError::ProjectAlreadyExists(_)
        | StoreError::ObjectNotFound { .. }
        | StoreError::MissingDependency { .. }
        | StoreError::FailedPrecondition(_)
        | StoreError::InvalidArgument(_)
        | StoreError::OutOfRange(_)
        | StoreError::Corruption(_)
        | StoreError::ResourceExhausted(_)
        | StoreError::PermissionDenied(_)
        | StoreError::Unauthenticated(_) => false,
    }
}

pub(super) fn is_transient_reconcile_error(err: &str) -> bool {
    let lower = err.to_ascii_lowercase();
    if lower.contains("compare-and-swap conflict") || lower.contains("cas conflict") {
        return true;
    }
    !(lower.contains("not found")
        || lower.contains("objectnotfound")
        || lower.contains("invalid argument")
        || lower.contains("corrupt")
        || lower.contains("unsupported")
        || lower.contains("missing")
        || lower.contains("permission denied")
        || lower.contains("authentication failed")
        || lower.contains("unauthenticated"))
}
