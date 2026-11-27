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

//! Errors shared by both stores.
//!
//! Callers branch on [`ErrorKind`], never on the message or the source. The
//! service layer maps kinds to gRPC codes and reasons (for example
//! `ObjectMismatch` to `ALREADY_EXISTS`, `LimitExceeded` to
//! `RESOURCE_EXHAUSTED`); that table lives with the wire contract (DEV-41).

use crate::ids::ObjectRef;
use crate::metadata::HeadState;
use std::fmt;

/// What went wrong, independent of the backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The named project, workspace or blob does not exist, where existence is
    /// required (a plain read that finds nothing returns `Ok(None)` instead).
    NotFound,
    /// A project or workspace with this name already exists.
    AlreadyExists,
    /// An object or blob with this name is already stored with different
    /// bytes. The stored bytes are unchanged.
    ObjectMismatch,
    /// Bytes given for a blob do not hash to the digest they were named by.
    DigestMismatch,
    /// A request exceeds a limit reported by `limits()`.
    LimitExceeded,
    /// The request is malformed (for example a kind in the wrong scope, a
    /// digest of the wrong length, or a reused request id).
    InvalidArgument,
    /// The implementation does not support this call or mode.
    Unsupported,
    /// The backend is temporarily unavailable or timed out. Safe to retry,
    /// because every write in this contract is idempotent.
    Unavailable,
    /// Stored data could not be decoded or failed verification on read.
    Corrupt,
    /// Anything else. Not assumed safe to retry.
    Internal,
}

/// Error returned by store calls.
#[derive(Debug)]
pub struct StoreError {
    /// What went wrong, for callers to match on.
    pub kind: ErrorKind,
    /// Human-readable detail, for logs. Never matched on.
    pub message: String,
    /// The backend's own error, for logs only. Never matched on.
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl StoreError {
    /// An error of `kind` with no backend source.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            source: None,
        }
    }

    /// True if retrying the same call unchanged may succeed.
    pub fn is_retryable(&self) -> bool {
        matches!(self.kind, ErrorKind::Unavailable)
    }
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|e| e as &(dyn std::error::Error + 'static))
    }
}

/// Result of a store call.
pub type StoreResult<T> = Result<T, StoreError>;

/// Outcome of a failed [`crate::MetadataStore::advance_head`].
///
/// The first two variants are expected outcomes of a correct client, not
/// faults; they carry what the client needs to recover.
#[derive(Debug)]
#[non_exhaustive]
pub enum AdvanceHeadError {
    /// The workspace head is not the expected head. Nothing was written.
    /// Maps to `ABORTED` / `HEAD_MOVED` on the wire.
    HeadMoved {
        /// The workspace's head now.
        current: HeadState,
    },
    /// The new head reaches objects that are not stored. Nothing was written.
    /// Maps to `FAILED_PRECONDITION` / `MISSING_OBJECTS`. The list may be
    /// truncated to the limit in [`crate::MetadataLimits::max_missing_reported`].
    MissingObjects {
        /// Objects the new head reaches that are not stored.
        missing: Vec<ObjectRef>,
    },
    /// Any other failure, including `NotFound` for an unknown workspace and
    /// `Unsupported` for a reserved mode.
    Store(StoreError),
}

impl From<StoreError> for AdvanceHeadError {
    fn from(e: StoreError) -> Self {
        AdvanceHeadError::Store(e)
    }
}

impl fmt::Display for AdvanceHeadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AdvanceHeadError::HeadMoved { current } => {
                write!(f, "head moved; current sequence {}", current.sequence)
            }
            AdvanceHeadError::MissingObjects { missing } => {
                write!(f, "{} objects missing from closure", missing.len())
            }
            AdvanceHeadError::Store(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for AdvanceHeadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            AdvanceHeadError::Store(e) => Some(e),
            _ => None,
        }
    }
}
