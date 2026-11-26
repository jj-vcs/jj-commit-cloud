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

//! Commit Cloud operation-head reconciliation, bookmark merging, and atomic
//! `View` and `Operation` publication for Git imports.
//!
//! # Authority and Crash-Safety Ordering
//! `publish_imported_refs` resolves active operation heads, loads the current
//! `Operation` and `View` (aborting if either read fails), merges imported
//! bookmarks on top of the existing `View`, and commits the new `View`,
//! `Operation`, and `OpHeads` in one atomic `PublishOperation` RPC.
//!
//! # Concurrency and Merge Invariants
//! Local bookmark updates use ancestry checks backed by `CommitParentCache` to
//! fast-forward, preserve already-ahead local bookmarks, or produce algebraic
//! conflicts without rewinding history or dropping conflict arity.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use cc_common::ROOT_COMMIT_ID_BYTES;
use cc_common::backend::ReadCommitRequest;
use cc_common::op_store::{
    GetOpHeadsRequest, Operation, OperationMetadata, PublishOperationRequest, ReadOperationRequest,
    ReadViewRequest, ReconcileOpHeadsRequest, RefTarget, RefTargetTerm, RemoteRef,
    UpdateOpHeadsRequest, WriteOperationRequest, WriteViewRequest,
};

use crate::util::AuthBackendClient;

pub(super) fn validate_operation_id(
    bytes: &[u8],
    field: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if bytes.len() != cc_common::OPERATION_ID_LENGTH {
        return Err(format!(
            "{field} must be {} bytes (got {} bytes)",
            cc_common::OPERATION_ID_LENGTH,
            bytes.len()
        )
        .into());
    }
    Ok(())
}

pub(super) fn validate_view_id(
    bytes: &[u8],
    field: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if bytes.len() != cc_common::VIEW_ID_LENGTH {
        return Err(format!(
            "{field} must be {} bytes (got {} bytes)",
            cc_common::VIEW_ID_LENGTH,
            bytes.len()
        )
        .into());
    }
    Ok(())
}

pub(super) async fn read_operation_and_view(
    op_client: &mut crate::util::AuthOpStoreClient,
    repo_id: &str,
    operation_id: &[u8],
    context: &str,
) -> Result<(Operation, cc_common::op_store::View), Box<dyn std::error::Error + Send + Sync>> {
    let op = op_client
        .read_operation(ReadOperationRequest {
            repo_id: repo_id.to_string(),
            operation_id: operation_id.to_vec(),
        })
        .await?
        .into_inner()
        .operation
        .ok_or_else(|| format!("server returned empty operation during {context}"))?;
    validate_view_id(&op.view_id, "ReadOperationResponse.operation.view_id")?;

    let view_resp = op_client
        .read_view(ReadViewRequest {
            repo_id: repo_id.to_string(),
            view_id: op.view_id.clone(),
        })
        .await?
        .into_inner();
    if !view_resp.view_id.is_empty() && view_resp.view_id != op.view_id {
        return Err(format!(
            "ReadView response view_id {} did not match requested {}",
            hex::encode(&view_resp.view_id),
            hex::encode(&op.view_id),
        )
        .into());
    }
    let view = view_resp
        .view
        .ok_or_else(|| format!("server returned empty view during {context}"))?;
    Ok((op, view))
}

pub(super) async fn resolve_single_op_head(
    op_client: &mut crate::util::AuthOpStoreClient,
    repo_id: &str,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    match op_client
        .reconcile_op_heads(ReconcileOpHeadsRequest {
            repo_id: repo_id.to_string(),
        })
        .await
    {
        Ok(resp) => {
            let inner = resp.into_inner();
            if !inner.client_resolution_required && !inner.op_head.is_empty() {
                validate_operation_id(&inner.op_head, "ReconcileOpHeadsResponse.op_head")?;
                return Ok(inner.op_head);
            }
            if inner.client_resolution_required
                && let [single] = inner.current_op_head_ids.as_slice()
            {
                validate_operation_id(single, "ReconcileOpHeadsResponse.current_op_head_ids")?;
                return Ok(single.clone());
            }
        }
        Err(status) if status.code() == tonic::Code::Unimplemented => {}
        Err(status) => return Err(status.into()),
    }

    let heads = op_client
        .get_op_heads(GetOpHeadsRequest {
            repo_id: repo_id.to_string(),
        })
        .await?
        .into_inner()
        .op_head_ids;
    match heads.as_slice() {
        [] => Ok(cc_common::ROOT_OPERATION_ID_BYTES.to_vec()),
        [single] => {
            validate_operation_id(single, "GetOpHeadsResponse.op_head_ids")?;
            Ok(single.clone())
        }
        many => Err(format!(
            "repository '{repo_id}' has {} divergent operation heads that require client-side \
             reconciliation; run `jj status` in a workspace attached to this repository first",
            many.len()
        )
        .into()),
    }
}

pub(super) struct CommitParentCache {
    parents: HashMap<Vec<u8>, Vec<Vec<u8>>>,
}

impl CommitParentCache {
    pub(super) fn new(initial_parents: &HashMap<Vec<u8>, Vec<Vec<u8>>>) -> Self {
        Self {
            parents: initial_parents.clone(),
        }
    }

    pub(super) async fn parents_of(
        &mut self,
        commit_id: &[u8],
        backend_client: &mut Option<(&mut AuthBackendClient, &str)>,
    ) -> Result<Vec<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
        if commit_id == ROOT_COMMIT_ID_BYTES {
            return Ok(Vec::new());
        }
        if let Some(cached) = self.parents.get(commit_id) {
            return Ok(cached.clone());
        }
        let Some((client, project_id)) = backend_client.as_mut() else {
            return Err(format!(
                "missing backend client while resolving commit {} during import-git",
                hex::encode(commit_id)
            )
            .into());
        };
        let commit = client
            .read_commit(ReadCommitRequest {
                project_id: (*project_id).to_string(),
                commit_id: commit_id.to_vec(),
            })
            .await?
            .into_inner()
            .commit
            .ok_or_else(|| {
                format!(
                    "missing commit payload for {} during import-git",
                    hex::encode(commit_id)
                )
            })?;
        self.parents
            .insert(commit_id.to_vec(), commit.parent_commit_ids.clone());
        Ok(commit.parent_commit_ids)
    }

    pub(super) async fn is_ancestor(
        &mut self,
        ancestor: &[u8],
        descendant: &[u8],
        backend_client: &mut Option<(&mut AuthBackendClient, &str)>,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if ancestor == descendant || ancestor == ROOT_COMMIT_ID_BYTES {
            return Ok(true);
        }
        if descendant == ROOT_COMMIT_ID_BYTES {
            return Ok(false);
        }
        let mut visited: HashSet<Vec<u8>> = HashSet::new();
        let mut queue: VecDeque<Vec<u8>> = VecDeque::new();
        visited.insert(descendant.to_vec());
        queue.push_back(descendant.to_vec());
        while let Some(cid) = queue.pop_front() {
            for parent in self.parents_of(&cid, backend_client).await? {
                if parent.as_slice() == ancestor {
                    return Ok(true);
                }
                if parent.as_slice() != ROOT_COMMIT_ID_BYTES && visited.insert(parent.clone()) {
                    queue.push_back(parent);
                }
            }
        }
        Ok(false)
    }
}

/// Merges an imported Git branch tip into an existing Commit Cloud local bookmark without
/// rewinding bookmarks that advanced in Commit Cloud or erasing existing conflicts.
///
/// Mirrors `jj_lib::refs::merge_ref_targets` with an absent base target for resolved bookmarks
/// (`removes: [], adds: [existing, imported]` on divergence), and preserves already-conflicted
/// `RefTarget`s.
pub(super) async fn merge_imported_local_bookmark(
    existing: Option<&RefTarget>,
    imported_id: &[u8],
    parent_cache: &mut CommitParentCache,
    backend_client: &mut Option<(&mut AuthBackendClient, &str)>,
) -> Result<RefTarget, Box<dyn std::error::Error + Send + Sync>> {
    let imported_resolved = || RefTarget {
        removes: Vec::new(),
        adds: vec![RefTargetTerm {
            commit_id: imported_id.to_vec(),
        }],
    };
    let Some(existing) = existing else {
        return Ok(imported_resolved());
    };
    if existing.adds.is_empty() && existing.removes.is_empty() {
        return Ok(imported_resolved());
    }

    if existing.removes.is_empty() && existing.adds.len() == 1 {
        let existing_id = existing.adds[0].commit_id.as_slice();
        if existing_id == imported_id {
            return Ok(existing.clone());
        }
        if parent_cache
            .is_ancestor(imported_id, existing_id, backend_client)
            .await?
        {
            return Ok(existing.clone());
        }
        if parent_cache
            .is_ancestor(existing_id, imported_id, backend_client)
            .await?
        {
            return Ok(imported_resolved());
        }
        return Ok(RefTarget {
            removes: Vec::new(),
            adds: vec![
                RefTargetTerm {
                    commit_id: existing_id.to_vec(),
                },
                RefTargetTerm {
                    commit_id: imported_id.to_vec(),
                },
            ],
        });
    }

    // Existing bookmark is already conflicted (`adds.len() > 1 || !removes.is_empty()`).
    for term in &existing.adds {
        if term.commit_id.as_slice() == imported_id
            || parent_cache
                .is_ancestor(imported_id, &term.commit_id, backend_client)
                .await?
        {
            return Ok(existing.clone());
        }
    }
    let mut superseded_indices = Vec::new();
    for (idx, term) in existing.adds.iter().enumerate() {
        if parent_cache
            .is_ancestor(&term.commit_id, imported_id, backend_client)
            .await?
        {
            superseded_indices.push(idx);
        }
    }
    let mut updated_adds = existing.adds.clone();
    if let [only_idx] = superseded_indices.as_slice() {
        // Fast-forward the sole superseded side in place so `adds.len()` and `removes.len()`
        // keep their exact arity (preserving any pre-existing modify/delete conflict too).
        updated_adds[*only_idx] = RefTargetTerm {
            commit_id: imported_id.to_vec(),
        };
    } else {
        // Keep all existing conflicted sides alongside `imported_id`: dropping 2+ sides while
        // keeping `existing.removes` unchanged would shrink `adds.len()` relative to
        // `removes.len()`, silently erasing a conflict or fabricating a phantom absent (`None`)
        // add term when decoded via `jj_lib::op_store::RefTarget::from_legacy_form`.
        updated_adds.push(RefTargetTerm {
            commit_id: imported_id.to_vec(),
        });
    }
    Ok(RefTarget {
        removes: existing.removes.clone(),
        adds: updated_adds,
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn publish_imported_refs(
    op_client: &mut crate::util::AuthOpStoreClient,
    mut backend_client: Option<(&mut AuthBackendClient, &str)>,
    repo_id: &str,
    head_commit_ids: &[Vec<u8>],
    imported_ancestors: &HashSet<Vec<u8>>,
    imported_parents: &HashMap<Vec<u8>, Vec<Vec<u8>>>,
    local_branches: &BTreeMap<String, Vec<u8>>,
    remote_branches: &BTreeMap<String, Vec<u8>>,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let reconciled_head = resolve_single_op_head(op_client, repo_id).await?;
    let (_op, mut view) =
        read_operation_and_view(op_client, repo_id, &reconciled_head, "import-git").await?;

    let mut parent_cache = CommitParentCache::new(imported_parents);

    if !head_commit_ids.is_empty() {
        let imported_head_set: HashSet<&[u8]> = head_commit_ids.iter().map(Vec::as_slice).collect();
        // Drop the virtual root commit and any previously recorded heads that are strict
        // ancestors of the imported Git heads.
        view.head_ids
            .retain(|h| h.as_slice() != ROOT_COMMIT_ID_BYTES && !imported_ancestors.contains(h));

        // If unrelated or descendant heads already existed in the repository, walk their
        // parents up to the imported graph boundary so imported heads that are ancestors of
        // existing heads are not re-added.
        let mut ancestors_of_existing: HashSet<Vec<u8>> = HashSet::new();
        let existing_non_imported: Vec<Vec<u8>> = view
            .head_ids
            .iter()
            .filter(|h| !imported_head_set.contains(h.as_slice()))
            .cloned()
            .collect();
        if !existing_non_imported.is_empty() && backend_client.is_some() {
            let mut visited: HashSet<Vec<u8>> = existing_non_imported.iter().cloned().collect();
            let mut stack = existing_non_imported;
            while let Some(cid) = stack.pop() {
                if cid.as_slice() == ROOT_COMMIT_ID_BYTES {
                    continue;
                }
                for parent in parent_cache.parents_of(&cid, &mut backend_client).await? {
                    if parent.as_slice() == ROOT_COMMIT_ID_BYTES {
                        continue;
                    }
                    ancestors_of_existing.insert(parent.clone());
                    // Stop walking once we enter the imported Git graph: `head_commit_ids` are
                    // already maximal inside that graph.
                    if imported_head_set.contains(parent.as_slice())
                        || imported_ancestors.contains(&parent)
                    {
                        continue;
                    }
                    if visited.insert(parent.clone()) {
                        stack.push(parent);
                    }
                }
            }
            view.head_ids.retain(|h| !ancestors_of_existing.contains(h));
        }

        for head in head_commit_ids {
            if !ancestors_of_existing.contains(head) && !view.head_ids.contains(head) {
                view.head_ids.push(head.clone());
            }
        }
    }

    let mut bookmark_count = 0usize;
    for (name, cid) in local_branches {
        let merged = merge_imported_local_bookmark(
            view.local_bookmarks.get(name),
            cid,
            &mut parent_cache,
            &mut backend_client,
        )
        .await?;
        view.local_bookmarks.insert(name.clone(), merged);
        bookmark_count += 1;
    }
    for (remote_key, cid) in remote_branches {
        // Like `jj git import`, new remote bookmarks start untracked and existing ones keep their
        // tracking state.
        let is_tracked = view
            .remote_bookmarks
            .get(remote_key)
            .is_some_and(|r| r.is_tracked);
        view.remote_bookmarks.insert(
            remote_key.clone(),
            RemoteRef {
                target: Some(RefTarget {
                    removes: Vec::new(),
                    adds: vec![RefTargetTerm {
                        commit_id: cid.clone(),
                    }],
                }),
                is_tracked,
            },
        );
    }

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);

    let operation = Operation {
        view_id: Vec::new(),
        parents: vec![reconciled_head.clone()],
        metadata: Some(OperationMetadata {
            start_time_millis: now_ms,
            end_time_millis: now_ms,
            description: "import git refs and commits".to_string(),
            is_snapshot: false,
            workspace_name: None,
            hostname: "localhost".to_string(),
            username: "git-bridge".to_string(),
            attributes: HashMap::new(),
        }),
        commit_predecessors: Vec::new(),
        commit_predecessors_set: true,
    };

    match op_client
        .publish_operation(PublishOperationRequest {
            repo_id: repo_id.to_string(),
            view: Some(view.clone()),
            operation: Some(operation.clone()),
            old_op_head_ids: vec![reconciled_head.clone()],
        })
        .await
    {
        Ok(_) => {}
        Err(status) if status.code() == tonic::Code::Unimplemented => {
            let view_id = op_client
                .write_view(WriteViewRequest {
                    repo_id: repo_id.to_string(),
                    view: Some(view),
                })
                .await?
                .into_inner()
                .view_id;
            validate_view_id(&view_id, "WriteViewResponse.view_id")?;
            let mut op_with_view = operation;
            op_with_view.view_id = view_id;
            let op_id = op_client
                .write_operation(WriteOperationRequest {
                    repo_id: repo_id.to_string(),
                    operation: Some(op_with_view),
                })
                .await?
                .into_inner()
                .operation_id;
            validate_operation_id(&op_id, "WriteOperationResponse.operation_id")?;
            op_client
                .update_op_heads(UpdateOpHeadsRequest {
                    repo_id: repo_id.to_string(),
                    old_op_head_ids: vec![reconciled_head],
                    new_op_head_id: op_id,
                })
                .await?;
        }
        Err(status) => return Err(status.into()),
    }

    Ok(bookmark_count)
}
