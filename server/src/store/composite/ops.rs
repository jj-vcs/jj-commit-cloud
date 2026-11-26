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

//! Operation log, view, and operation-head methods on `CompositeStore`.
//!
//! # Authority and Crash-Safety Ordering
//! `put_view`, `put_operation`, and `publish_operation` verify that referenced commits,
//! views, and parent operations already exist before updating active operation heads.
//!
//! # Concurrency and Merge Invariants
//! Operation-head updates remove superseded ancestors while retaining concurrent sibling
//! heads without cross-client locks, using helper comparisons shared across store backends.

use std::collections::HashSet;

use cc_common::op_store::*;

use super::CompositeStore;
use crate::store::{IdPrefixResolution, OpId, StoreError, StoreResult, hex_prefix_byte_bounds};

impl CompositeStore {
    async fn validate_view_and_op_commits(
        &self,
        project_id: &str,
        view: Option<&View>,
        op: Option<&Operation>,
    ) -> StoreResult<()> {
        let mut seen: HashSet<&[u8]> = HashSet::new();
        let mut view_commits: Vec<Vec<u8>> = Vec::new();
        let mut op_commits: Vec<Vec<u8>> = Vec::new();

        fn push_commit<'a>(seen: &mut HashSet<&'a [u8]>, dest: &mut Vec<Vec<u8>>, id: &'a [u8]) {
            if id != cc_common::ROOT_COMMIT_ID_BYTES && seen.insert(id) {
                dest.push(id.to_vec());
            }
        }

        if let Some(view) = view {
            for cid in &view.head_ids {
                push_commit(&mut seen, &mut view_commits, cid);
            }
            for cid in view.wc_commit_ids.values() {
                push_commit(&mut seen, &mut view_commits, cid);
            }
            for target in view.local_bookmarks.values() {
                for term in target.adds.iter().chain(target.removes.iter()) {
                    push_commit(&mut seen, &mut view_commits, &term.commit_id);
                }
            }
            for remote_ref in view.remote_bookmarks.values() {
                if let Some(target) = &remote_ref.target {
                    for term in target.adds.iter().chain(target.removes.iter()) {
                        push_commit(&mut seen, &mut view_commits, &term.commit_id);
                    }
                }
            }
        }

        if let Some(op) = op {
            for entry in &op.commit_predecessors {
                push_commit(&mut seen, &mut op_commits, &entry.commit_id);
                for pred in &entry.predecessor_ids {
                    push_commit(&mut seen, &mut op_commits, pred);
                }
            }
        }

        if view_commits.is_empty() && op_commits.is_empty() {
            return Ok(());
        }

        let mut all_ids = Vec::with_capacity(view_commits.len() + op_commits.len());
        all_ids.extend(view_commits.iter().cloned());
        all_ids.extend(op_commits.iter().cloned());

        let found = self
            .metadata
            .get_commit_generations(project_id, &all_ids)
            .await?;

        for cid in &view_commits {
            if !found.contains_key(cid.as_slice()) {
                return Err(StoreError::MissingDependency {
                    parent_kind: "view",
                    missing_kind: "commit",
                    id_hex: hex::encode(cid),
                });
            }
        }
        for cid in &op_commits {
            if !found.contains_key(cid.as_slice()) {
                return Err(StoreError::MissingDependency {
                    parent_kind: "operation",
                    missing_kind: "commit",
                    id_hex: hex::encode(cid),
                });
            }
        }
        Ok(())
    }

    async fn validate_operation_parents(&self, repo_id: &str, op: &Operation) -> StoreResult<()> {
        let mut seen: HashSet<&[u8]> = HashSet::with_capacity(op.parents.len());
        for parent_id in &op.parents {
            if parent_id.as_slice() != cc_common::ROOT_OPERATION_ID_BYTES
                && seen.insert(parent_id.as_slice())
                && self
                    .metadata
                    .get_operation(repo_id, parent_id)
                    .await?
                    .is_none()
            {
                return Err(StoreError::MissingDependency {
                    parent_kind: "operation",
                    missing_kind: "parent operation",
                    id_hex: hex::encode(parent_id),
                });
            }
        }
        Ok(())
    }

    pub async fn put_view(&self, repo_id: &str, view_id: &[u8], view: &View) -> StoreResult<()> {
        let project_id = self.resolve_repo_project(repo_id, "writing views").await?;
        if self.config.validate_dependencies {
            self.validate_view_and_op_commits(&project_id, Some(view), None)
                .await?;
        }
        self.metadata.put_view(repo_id, view_id, view).await
    }

    pub async fn get_view(&self, repo_id: &str, view_id: &[u8]) -> StoreResult<Option<View>> {
        self.ensure_repo_registered(repo_id, "reading views")
            .await?;
        self.metadata.get_view(repo_id, view_id).await
    }

    pub async fn has_view(&self, repo_id: &str, view_id: &[u8]) -> StoreResult<bool> {
        self.ensure_repo_registered(repo_id, "checking views")
            .await?;
        self.metadata.has_view(repo_id, view_id).await
    }

    pub async fn put_operation(
        &self,
        repo_id: &str,
        op_id: &[u8],
        op: &Operation,
    ) -> StoreResult<()> {
        let project_id = self
            .resolve_repo_project(repo_id, "writing operations")
            .await?;
        if self.config.validate_dependencies {
            if op.view_id.as_slice() != cc_common::ROOT_VIEW_ID_BYTES
                && self
                    .metadata
                    .get_view(repo_id, &op.view_id)
                    .await?
                    .is_none()
            {
                return Err(StoreError::MissingDependency {
                    parent_kind: "operation",
                    missing_kind: "view",
                    id_hex: hex::encode(&op.view_id),
                });
            }
            self.validate_operation_parents(repo_id, op).await?;
            self.validate_view_and_op_commits(&project_id, None, Some(op))
                .await?;
        }
        self.metadata.put_operation(repo_id, op_id, op).await
    }

    pub async fn get_operation(
        &self,
        repo_id: &str,
        op_id: &[u8],
    ) -> StoreResult<Option<Operation>> {
        self.ensure_repo_registered(repo_id, "reading operations")
            .await?;
        self.metadata.get_operation(repo_id, op_id).await
    }

    pub async fn resolve_operation_prefix(
        &self,
        repo_id: &str,
        hex_prefix: &str,
    ) -> StoreResult<IdPrefixResolution> {
        self.ensure_repo_registered(repo_id, "resolving operation prefixes")
            .await?;

        let normalized = hex_prefix.to_ascii_lowercase();
        let Some((min_id, max_id)) =
            hex_prefix_byte_bounds(&normalized, cc_common::OPERATION_ID_LENGTH)?
        else {
            return Ok(IdPrefixResolution::NoMatch);
        };

        let mut matches = self
            .metadata
            .resolve_operation_id_prefix(repo_id, &min_id, &max_id)
            .await?;

        let root_id = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
        if root_id.as_slice() >= min_id.as_slice()
            && root_id.as_slice() <= max_id.as_slice()
            && !matches.contains(&root_id)
        {
            matches.push(root_id);
        }

        match matches.len() {
            0 => Ok(IdPrefixResolution::NoMatch),
            1 => Ok(IdPrefixResolution::SingleMatch(matches.swap_remove(0))),
            _ => Ok(IdPrefixResolution::AmbiguousMatch),
        }
    }

    pub async fn get_op_heads(&self, repo_id: &str) -> StoreResult<Vec<OpId>> {
        self.ensure_repo_registered(repo_id, "requesting op heads")
            .await?;
        Ok(self
            .metadata
            .get_op_heads(repo_id)
            .await?
            .unwrap_or_else(|| vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()]))
    }

    pub async fn update_op_heads(
        &self,
        repo_id: &str,
        old_ids: &[Vec<u8>],
        new_id: &[u8],
    ) -> StoreResult<Vec<OpId>> {
        if new_id == cc_common::ROOT_OPERATION_ID_BYTES {
            return Err(StoreError::InvalidArgument(
                "new_op_head_id must not be ROOT_OPERATION_ID".to_string(),
            ));
        }
        self.ensure_repo_registered(repo_id, "updating op heads")
            .await?;
        if self.config.validate_dependencies
            && self
                .metadata
                .get_operation(repo_id, new_id)
                .await?
                .is_none()
        {
            return Err(StoreError::MissingDependency {
                parent_kind: "op_heads",
                missing_kind: "operation",
                id_hex: hex::encode(new_id),
            });
        }
        self.metadata
            .update_op_heads(repo_id, old_ids, new_id)
            .await
    }

    pub async fn publish_operation(
        &self,
        repo_id: &str,
        view_id: &[u8],
        view: &View,
        op_id: &[u8],
        op: &Operation,
        old_ids: &[Vec<u8>],
    ) -> StoreResult<Vec<OpId>> {
        if op_id == cc_common::ROOT_OPERATION_ID_BYTES {
            return Err(StoreError::InvalidArgument(
                "op_id must not be ROOT_OPERATION_ID".to_string(),
            ));
        }
        let project_id = self
            .resolve_repo_project(repo_id, "publishing operation")
            .await?;
        if self.config.validate_dependencies {
            if op.view_id.as_slice() != view_id
                && op.view_id.as_slice() != cc_common::ROOT_VIEW_ID_BYTES
                && self
                    .metadata
                    .get_view(repo_id, &op.view_id)
                    .await?
                    .is_none()
            {
                return Err(StoreError::MissingDependency {
                    parent_kind: "operation",
                    missing_kind: "view",
                    id_hex: hex::encode(&op.view_id),
                });
            }
            self.validate_operation_parents(repo_id, op).await?;
            self.validate_view_and_op_commits(&project_id, Some(view), Some(op))
                .await?;
        }
        self.metadata
            .publish_operation_atomic(repo_id, view_id, view, op_id, op, old_ids)
            .await
    }

    pub async fn compare_and_swap_op_heads(
        &self,
        repo_id: &str,
        expected_old_ids: &[Vec<u8>],
        new_id: &[u8],
    ) -> StoreResult<()> {
        if new_id == cc_common::ROOT_OPERATION_ID_BYTES {
            return Err(StoreError::InvalidArgument(
                "new_op_head_id must not be ROOT_OPERATION_ID".to_string(),
            ));
        }
        self.ensure_repo_registered(repo_id, "comparing and swapping op heads")
            .await?;
        if self.config.validate_dependencies
            && self
                .metadata
                .get_operation(repo_id, new_id)
                .await?
                .is_none()
        {
            return Err(StoreError::MissingDependency {
                parent_kind: "op_heads",
                missing_kind: "operation",
                id_hex: hex::encode(new_id),
            });
        }
        self.metadata
            .compare_and_swap_op_heads(repo_id, expected_old_ids, new_id)
            .await
    }
}

pub fn op_heads_sets_equal(a: &[Vec<u8>], b: &[Vec<u8>]) -> bool {
    let set_a: std::collections::HashSet<&[u8]> = a.iter().map(Vec::as_slice).collect();
    let set_b: std::collections::HashSet<&[u8]> = b.iter().map(Vec::as_slice).collect();
    set_a == set_b
}

/// Applies the canonical jj Commit Cloud operation-head transition rules to `current_heads`:
/// - Removes all entries listed in `old_ids`.
/// - Preserves any concurrent sibling heads not listed in `old_ids`.
/// - Appends `new_id` so concurrent writers diverge instead of overwriting each other.
pub fn apply_op_heads_mutation(current_heads: &mut Vec<OpId>, old_ids: &[Vec<u8>], new_id: &[u8]) {
    current_heads.retain(|head| !old_ids.iter().any(|old| old.as_slice() == head.as_slice()));
    if !current_heads.iter().any(|head| head.as_slice() == new_id) {
        current_heads.push(new_id.to_vec());
    }
}
