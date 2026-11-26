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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use cc_common::op_store::{
    Operation as ProtoOperation, RefTarget, RefTargetTerm, View as ProtoView,
};
use cc_lib::cc_op_store::{operation_from_proto, view_from_proto};
use jj_lib::backend::CommitId;
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_heads_store::OpHeadsStore;
use jj_lib::op_store::OperationId;

use super::super::errors::{ReconcileError, find_store_error};
use super::super::{reconcile_repo_op_heads_proto, run_reconcile_join};
use super::{make_test_proto_commit, make_test_proto_op, new_commit_predecessors, put_view_and_op};
use crate::jj_lib_adapters::ServerOpHeadsStore;
use crate::store::{
    CompositeStore, CompositeStoreConfig, MemoryBlobStore, MemoryMetadataStore, MetadataStore,
    StoreError,
};

#[tokio::test]
async fn test_reconcile_single_head_is_noop() {
    let store = Arc::new(CompositeStore::in_memory());
    store.register_repo("single-head-repo", None).await.unwrap();
    let resp = reconcile_repo_op_heads_proto(store, "single-head-repo")
        .await
        .unwrap();
    assert!(!resp.client_resolution_required);
    assert_eq!(resp.op_head, cc_common::ROOT_OPERATION_ID_BYTES.to_vec());
}

#[tokio::test]
async fn test_reconcile_merges_divergent_bookmarks_and_prunes_ancestor_heads() {
    let store = Arc::new(CompositeStore::in_memory());
    let repo_id = "reconcile-ancestry-repo";
    store.register_repo(repo_id, None).await.unwrap();

    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let c1_id = vec![0x01; 20];
    let c2_id = vec![0x02; 20];
    let c3_id = vec![0x03; 20];

    // Build commit chain: root -> c1 -> c2, and sibling root -> c3.
    for (cid, parent) in [
        (&c1_id, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
        (&c2_id, c1_id.clone()),
        (&c3_id, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
    ] {
        store
            .put_commit(repo_id, cid, &make_test_proto_commit(cid, cid[0], parent))
            .await
            .unwrap();
    }

    // Base operation Op0 has bookmark `main = c1` and `head_ids = [c1]`.
    let mut bm_base = HashMap::new();
    bm_base.insert(
        "main".to_string(),
        RefTarget {
            removes: vec![],
            adds: vec![RefTargetTerm {
                commit_id: c1_id.clone(),
            }],
        },
    );
    let view0 = ProtoView {
        head_ids: vec![c1_id.clone()],
        wc_commit_ids: HashMap::new(),
        local_bookmarks: bm_base.clone(),
        remote_bookmarks: HashMap::new(),
    };
    let view0_id = crate::hash_utils::hash_view(&view0).unwrap();
    store.put_view(repo_id, &view0_id, &view0).await.unwrap();
    let mut op0 = make_test_proto_op(view0_id, vec![root_op.clone()], "base");
    op0.commit_predecessors = new_commit_predecessors(c1_id.clone());
    let op0_id = crate::hash_utils::hash_operation(&op0).unwrap();
    store.put_operation(repo_id, &op0_id, &op0).await.unwrap();
    store
        .update_op_heads(repo_id, &[root_op], &op0_id)
        .await
        .unwrap();

    // Branch A fast-forwards `main` from c1 -> c2 (`head_ids = [c2]`).
    let mut bm_a = HashMap::new();
    bm_a.insert(
        "main".to_string(),
        RefTarget {
            removes: vec![],
            adds: vec![RefTargetTerm {
                commit_id: c2_id.clone(),
            }],
        },
    );
    let view_a = ProtoView {
        head_ids: vec![c2_id.clone()],
        wc_commit_ids: HashMap::new(),
        local_bookmarks: bm_a,
        remote_bookmarks: HashMap::new(),
    };
    let view_a_id = crate::hash_utils::hash_view(&view_a).unwrap();
    store.put_view(repo_id, &view_a_id, &view_a).await.unwrap();
    let mut op_a = make_test_proto_op(view_a_id, vec![op0_id.clone()], "advance main");
    op_a.commit_predecessors = new_commit_predecessors(c2_id.clone());
    let op_a_id = crate::hash_utils::hash_operation(&op_a).unwrap();
    store.put_operation(repo_id, &op_a_id, &op_a).await.unwrap();

    // Branch B keeps `main = c1` and adds bookmark `feature = c3` (`head_ids = [c1, c3]`).
    let mut bm_b = bm_base;
    bm_b.insert(
        "feature".to_string(),
        RefTarget {
            removes: vec![],
            adds: vec![RefTargetTerm {
                commit_id: c3_id.clone(),
            }],
        },
    );
    let view_b = ProtoView {
        head_ids: vec![c1_id.clone(), c3_id.clone()],
        wc_commit_ids: HashMap::new(),
        local_bookmarks: bm_b,
        remote_bookmarks: HashMap::new(),
    };
    let view_b_id = crate::hash_utils::hash_view(&view_b).unwrap();
    store.put_view(repo_id, &view_b_id, &view_b).await.unwrap();
    let mut op_b = make_test_proto_op(view_b_id, vec![op0_id.clone()], "add feature");
    op_b.commit_predecessors = new_commit_predecessors(c3_id.clone());
    let op_b_id = crate::hash_utils::hash_operation(&op_b).unwrap();
    store.put_operation(repo_id, &op_b_id, &op_b).await.unwrap();

    store
        .update_op_heads(repo_id, std::slice::from_ref(&op0_id), &op_a_id)
        .await
        .unwrap();
    store
        .update_op_heads(repo_id, std::slice::from_ref(&op0_id), &op_b_id)
        .await
        .unwrap();

    let resp = reconcile_repo_op_heads_proto(store.clone(), repo_id)
        .await
        .unwrap();
    assert!(!resp.client_resolution_required);
    let merged_op_id = resp.op_head;
    assert_eq!(
        store.get_op_heads(repo_id).await.unwrap(),
        vec![merged_op_id.clone()]
    );

    let merged_op = operation_from_proto(
        store
            .get_operation(repo_id, &merged_op_id)
            .await
            .unwrap()
            .unwrap(),
    );
    assert_eq!(merged_op.metadata.hostname, "jj-cc-server");
    assert_eq!(merged_op.metadata.username, "jj-cc-server");
    let parent_set: HashSet<_> = merged_op.parents.into_iter().collect();
    assert_eq!(
        parent_set,
        HashSet::from([
            OperationId::from_bytes(&op_a_id),
            OperationId::from_bytes(&op_b_id),
        ])
    );

    let merged_view = view_from_proto(
        store
            .get_view(repo_id, merged_op.view_id.as_bytes())
            .await
            .unwrap()
            .unwrap(),
    );
    // Because c1 is an ancestor of c2, merged `head_ids` contains only `{c2, c3}` (no dangling
    // c1), and `main` fast-forwarded to `c2` alongside `feature = c3`.
    assert_eq!(
        merged_view.head_ids,
        HashSet::from([CommitId::from_bytes(&c2_id), CommitId::from_bytes(&c3_id)])
    );
    assert_eq!(
        merged_view
            .local_bookmarks
            .get(jj_lib::ref_name::RefName::new("main"))
            .and_then(|t| t.as_normal()),
        Some(&CommitId::from_bytes(&c2_id))
    );
    assert_eq!(
        merged_view
            .local_bookmarks
            .get(jj_lib::ref_name::RefName::new("feature"))
            .and_then(|t| t.as_normal()),
        Some(&CommitId::from_bytes(&c3_id))
    );
}

#[tokio::test]
async fn test_reconcile_prunes_ancestor_op_head_without_cas_wedge() {
    let store = Arc::new(CompositeStore::in_memory());
    let repo_id = "ancestor-head-repo";
    store.register_repo(repo_id, None).await.unwrap();

    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let c1_id = vec![0x11; 20];
    store
        .put_commit(
            repo_id,
            &c1_id,
            &make_test_proto_commit(&c1_id, 0x11, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
        )
        .await
        .unwrap();

    let op0_id = put_view_and_op(
        &store,
        repo_id,
        vec![c1_id.clone()],
        vec![root_op.clone()],
        "op0",
        new_commit_predecessors(c1_id.clone()),
    )
    .await;
    let op1_id = put_view_and_op(
        &store,
        repo_id,
        vec![c1_id],
        vec![op0_id.clone()],
        "op1 child of op0",
        vec![],
    )
    .await;

    // Advance to `op1`, then add ancestor `op0` so `OpHeads = [op1, op0]`.
    store
        .update_op_heads(repo_id, std::slice::from_ref(&root_op), &op1_id)
        .await
        .unwrap();
    store.update_op_heads(repo_id, &[], &op0_id).await.unwrap();
    assert_eq!(store.get_op_heads(repo_id).await.unwrap().len(), 2);

    let resp = reconcile_repo_op_heads_proto(store.clone(), repo_id)
        .await
        .unwrap();
    assert!(!resp.client_resolution_required);
    let resolved = resp.op_head;
    assert_eq!(resolved, op1_id);
    assert_eq!(
        store.get_op_heads(repo_id).await.unwrap(),
        vec![op1_id.clone()]
    );

    // Pinned `ServerOpHeadsStore::with_expected_heads` must reject stale CAS with `CasConflict`.
    let stale_store =
        ServerOpHeadsStore::with_expected_heads(store.clone(), repo_id.to_string(), vec![op0_id]);
    let cas_err = stale_store
        .update_op_heads(
            &[OperationId::from_bytes(&op1_id)],
            &OperationId::from_bytes(&op1_id),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(find_store_error(&cas_err), Some(StoreError::CasConflict(_))),
        "expected CasConflict from pinned ServerOpHeadsStore, got {cas_err:?}"
    );
}

#[tokio::test]
async fn test_reconcile_defers_cross_branch_commit_rewrites_to_client() {
    let store = Arc::new(CompositeStore::in_memory());
    let repo_id = "rewrite-defer-repo";
    store.register_repo(repo_id, None).await.unwrap();

    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let x_id = vec![0x21; 20];
    let x_rewritten_id = vec![0x22; 20];
    let y_child_id = vec![0x23; 20];

    for (cid, change_byte, parent) in [
        (&x_id, 0x21u8, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
        (
            &x_rewritten_id,
            0x21u8,
            cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
        ),
        (&y_child_id, 0x23u8, x_id.clone()),
    ] {
        store
            .put_commit(
                repo_id,
                cid,
                &make_test_proto_commit(cid, change_byte, parent),
            )
            .await
            .unwrap();
    }

    let op0_id = put_view_and_op(
        &store,
        repo_id,
        vec![x_id.clone()],
        vec![root_op.clone()],
        "base",
        new_commit_predecessors(x_id.clone()),
    )
    .await;
    store
        .update_op_heads(repo_id, &[root_op], &op0_id)
        .await
        .unwrap();

    // Branch A rewrites X -> X'.
    let op_a_id = put_view_and_op(
        &store,
        repo_id,
        vec![x_rewritten_id.clone()],
        vec![op0_id.clone()],
        "rewrite X",
        vec![cc_common::op_store::CommitPredecessors {
            commit_id: x_rewritten_id,
            predecessor_ids: vec![x_id],
        }],
    )
    .await;

    // Branch B creates child Y on top of X (replacing X in head_ids).
    let op_b_id = put_view_and_op(
        &store,
        repo_id,
        vec![y_child_id.clone()],
        vec![op0_id.clone()],
        "child of X",
        new_commit_predecessors(y_child_id),
    )
    .await;

    store
        .update_op_heads(repo_id, std::slice::from_ref(&op0_id), &op_a_id)
        .await
        .unwrap();
    store
        .update_op_heads(repo_id, std::slice::from_ref(&op0_id), &op_b_id)
        .await
        .unwrap();

    let resp = reconcile_repo_op_heads_proto(store.clone(), repo_id)
        .await
        .unwrap();
    assert!(resp.client_resolution_required);
    assert!(
        resp.op_head.is_empty(),
        "cross-branch rewrite must return empty op_head for client rebase"
    );
    assert_eq!(store.get_op_heads(repo_id).await.unwrap().len(), 2);
}

#[tokio::test]
async fn test_reconcile_defers_three_head_intermediate_ancestor_rewrite_to_client() {
    let store = Arc::new(CompositeStore::in_memory());
    let repo_id = "three-head-rewrite-repo";
    store.register_repo(repo_id, None).await.unwrap();

    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let c_id = vec![0x31; 20];
    let c_rewritten_id = vec![0x32; 20];
    let d_child_id = vec![0x33; 20];
    let w_sibling_id = vec![0x34; 20];

    for (cid, change_byte, parent) in [
        (&c_id, 0x31u8, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
        (
            &c_rewritten_id,
            0x31u8,
            cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
        ),
        (&d_child_id, 0x33u8, c_id.clone()),
        (
            &w_sibling_id,
            0x34u8,
            cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
        ),
    ] {
        store
            .put_commit(
                repo_id,
                cid,
                &make_test_proto_commit(cid, change_byte, parent),
            )
            .await
            .unwrap();
    }

    // Op1 (child of root) creates commit C (`commit_predecessors[C] = []`).
    let op1_id = put_view_and_op(
        &store,
        repo_id,
        vec![c_id.clone()],
        vec![root_op.clone()],
        "create C",
        new_commit_predecessors(c_id.clone()),
    )
    .await;
    // OpA (child of Op1) rewrites C -> C'.
    let op_a_id = put_view_and_op(
        &store,
        repo_id,
        vec![c_rewritten_id.clone()],
        vec![op1_id.clone()],
        "rewrite C",
        vec![cc_common::op_store::CommitPredecessors {
            commit_id: c_rewritten_id,
            predecessor_ids: vec![c_id],
        }],
    )
    .await;
    // OpB (child of Op1) creates child D on top of C.
    let op_b_id = put_view_and_op(
        &store,
        repo_id,
        vec![d_child_id.clone()],
        vec![op1_id],
        "child D of C",
        new_commit_predecessors(d_child_id),
    )
    .await;
    // OpC (child of root_op) creates independent commit W, pushing global LCA back to root_op.
    let op_c_id = put_view_and_op(
        &store,
        repo_id,
        vec![w_sibling_id.clone()],
        vec![root_op.clone()],
        "independent W",
        new_commit_predecessors(w_sibling_id),
    )
    .await;

    store
        .update_op_heads(repo_id, &[root_op], &op_a_id)
        .await
        .unwrap();
    store.update_op_heads(repo_id, &[], &op_b_id).await.unwrap();
    store.update_op_heads(repo_id, &[], &op_c_id).await.unwrap();

    let resp = reconcile_repo_op_heads_proto(store.clone(), repo_id)
        .await
        .unwrap();
    assert!(resp.client_resolution_required);
    assert!(
        resp.op_head.is_empty(),
        "3-head merge with intermediate ancestor rewrite must defer to client"
    );
}

#[tokio::test]
async fn test_reconcile_defers_cross_branch_commit_abandon_to_client() {
    let store = Arc::new(CompositeStore::in_memory());
    let repo_id = "abandon-defer-repo";
    store.register_repo(repo_id, None).await.unwrap();

    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let x_id = vec![0x41; 20];
    let y_child_id = vec![0x42; 20];

    for (cid, change_byte, parent) in [
        (&x_id, 0x41u8, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
        (&y_child_id, 0x42u8, x_id.clone()),
    ] {
        store
            .put_commit(
                repo_id,
                cid,
                &make_test_proto_commit(cid, change_byte, parent),
            )
            .await
            .unwrap();
    }

    let op0_id = put_view_and_op(
        &store,
        repo_id,
        vec![x_id.clone()],
        vec![root_op.clone()],
        "base",
        new_commit_predecessors(x_id),
    )
    .await;
    store
        .update_op_heads(repo_id, &[root_op], &op0_id)
        .await
        .unwrap();

    // Branch A abandons X (`head_ids = [root]`, empty `commit_predecessors`).
    let op_a_id = put_view_and_op(
        &store,
        repo_id,
        vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
        vec![op0_id.clone()],
        "abandon X",
        vec![],
    )
    .await;
    // Branch B creates child Y on top of X.
    let op_b_id = put_view_and_op(
        &store,
        repo_id,
        vec![y_child_id.clone()],
        vec![op0_id.clone()],
        "child Y of X",
        new_commit_predecessors(y_child_id),
    )
    .await;

    store
        .update_op_heads(repo_id, std::slice::from_ref(&op0_id), &op_a_id)
        .await
        .unwrap();
    store
        .update_op_heads(repo_id, std::slice::from_ref(&op0_id), &op_b_id)
        .await
        .unwrap();

    let resp = reconcile_repo_op_heads_proto(store.clone(), repo_id)
        .await
        .unwrap();
    assert!(resp.client_resolution_required);
    assert!(
        resp.op_head.is_empty(),
        "cross-branch abandon with descendant on sibling branch must defer to client"
    );
}

#[tokio::test]
async fn test_run_reconcile_join_catches_worker_panic_as_err() {
    let handle = tokio::task::spawn_blocking(|| -> Result<Option<Vec<u8>>, ReconcileError> {
        panic!("simulated worker panic");
    });
    let err = run_reconcile_join(handle).await.unwrap_err();
    assert!(
        !err.transient,
        "worker panic must be treated as non-transient"
    );
    assert!(
        err.status
            .message()
            .contains("reconciliation worker failed")
    );

    let cancelled_handle = tokio::spawn(async {
        futures::future::pending::<Result<Option<Vec<u8>>, ReconcileError>>().await
    });
    cancelled_handle.abort();
    let cancel_err = run_reconcile_join(cancelled_handle).await.unwrap_err();
    assert!(
        cancel_err.transient,
        "cancelled worker join error should remain transient"
    );
}

#[tokio::test]
async fn test_reconcile_concurrent_new_commits_with_empty_predecessors_reconciles_server_side() {
    let store = Arc::new(CompositeStore::in_memory());
    let repo_id = "repo-concurrent-new-commits";
    store
        .register_repo_in_project(repo_id, Some("proj-cnc"), None)
        .await
        .unwrap();

    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let c0 = vec![0x80; 20];
    let c1 = vec![0x81; 20];
    let c2 = vec![0x82; 20];
    for (cid, parent) in [
        (&c0, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
        (&c1, c0.clone()),
        (&c2, c0.clone()),
    ] {
        store
            .put_commit(
                "proj-cnc",
                cid,
                &make_test_proto_commit(cid, cid[0], parent),
            )
            .await
            .unwrap();
    }

    let base_op_id = put_view_and_op(
        &store,
        repo_id,
        vec![c0.clone()],
        vec![root_op.clone()],
        "base c0",
        new_commit_predecessors(c0),
    )
    .await;
    // Branch A creates new commit c1 (empty predecessor_ids as jj_lib 0.47 records).
    let op_a_id = put_view_and_op(
        &store,
        repo_id,
        vec![c1.clone()],
        vec![base_op_id.clone()],
        "new commit c1",
        new_commit_predecessors(c1.clone()),
    )
    .await;
    // Branch B concurrently creates new commit c2 (also with empty predecessor_ids).
    let op_b_id = put_view_and_op(
        &store,
        repo_id,
        vec![c2.clone()],
        vec![base_op_id],
        "new commit c2",
        new_commit_predecessors(c2.clone()),
    )
    .await;

    store
        .update_op_heads(repo_id, &[root_op], &op_a_id)
        .await
        .unwrap();
    store.update_op_heads(repo_id, &[], &op_b_id).await.unwrap();

    let resp = reconcile_repo_op_heads_proto(store.clone(), repo_id)
        .await
        .unwrap();
    assert!(
        !resp.client_resolution_required,
        "concurrent new commits must reconcile server-side"
    );
    let merged_op = store
        .get_operation(repo_id, &resp.op_head)
        .await
        .unwrap()
        .unwrap();
    let merged_view = store
        .get_view(repo_id, &merged_op.view_id)
        .await
        .unwrap()
        .unwrap();
    let head_set: HashSet<Vec<u8>> = merged_view.head_ids.into_iter().collect();
    assert_eq!(head_set, HashSet::from([c1, c2]));
    assert_eq!(
        store.get_op_heads(repo_id).await.unwrap(),
        vec![resp.op_head]
    );
}

#[tokio::test]
async fn test_reconcile_combined_abandon_and_new_commit_requires_client_resolution() {
    let store = Arc::new(CompositeStore::in_memory());
    let repo_id = "repo-abandon-plus-new";
    store
        .register_repo_in_project(repo_id, Some("proj-apn"), None)
        .await
        .unwrap();

    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let c0 = vec![0x90; 20];
    let c1 = vec![0x91; 20];
    let c2 = vec![0x92; 20];
    for (cid, parent) in [
        (&c0, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
        (&c1, c0.clone()),
        // c2 is rooted directly on ROOT_COMMIT_ID (does not descend from abandoned c0).
        (&c2, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
    ] {
        store
            .put_commit(
                "proj-apn",
                cid,
                &make_test_proto_commit(cid, cid[0], parent),
            )
            .await
            .unwrap();
    }

    let base_op_id = put_view_and_op(
        &store,
        repo_id,
        vec![c0.clone()],
        vec![root_op.clone()],
        "base c0",
        new_commit_predecessors(c0),
    )
    .await;
    // Branch A adds child c1 on top of c0.
    let op_a_id = put_view_and_op(
        &store,
        repo_id,
        vec![c1.clone()],
        vec![base_op_id.clone()],
        "add c1",
        new_commit_predecessors(c1),
    )
    .await;
    // Branch B abandons c0 and creates a fresh commit c2 in the same operation (recording
    // c2 with empty predecessor_ids, as `jj abandon` does when creating a new working copy).
    let op_b_id = put_view_and_op(
        &store,
        repo_id,
        vec![c2.clone()],
        vec![base_op_id],
        "abandon c0 and new c2",
        new_commit_predecessors(c2),
    )
    .await;

    store
        .update_op_heads(repo_id, &[root_op], &op_a_id)
        .await
        .unwrap();
    store.update_op_heads(repo_id, &[], &op_b_id).await.unwrap();

    let resp = reconcile_repo_op_heads_proto(store.clone(), repo_id)
        .await
        .unwrap();
    assert!(resp.client_resolution_required);
    assert!(resp.op_head.is_empty());
    assert_eq!(
        HashSet::<Vec<u8>>::from_iter(resp.current_op_head_ids),
        HashSet::from([op_a_id.clone(), op_b_id.clone()])
    );
    assert_eq!(
        HashSet::<Vec<u8>>::from_iter(store.get_op_heads(repo_id).await.unwrap()),
        HashSet::from([op_a_id, op_b_id])
    );
}

#[tokio::test]
async fn test_reconcile_merge_op_diamond_does_not_leak_past_common_ancestor() {
    let store = Arc::new(CompositeStore::in_memory());
    let repo_id = "repo-merge-diamond";
    store
        .register_repo_in_project(repo_id, Some("proj-md"), None)
        .await
        .unwrap();

    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let c_old = vec![0x30; 20];
    let c_rewritten = vec![0x31; 20];
    let c_l = vec![0x32; 20];
    let c_m = vec![0x33; 20];
    let c_h1 = vec![0x34; 20];
    for (cid, parent) in [
        (&c_old, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
        (&c_rewritten, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
        (&c_l, c_rewritten.clone()),
        (&c_m, c_rewritten.clone()),
        (&c_h1, c_l.clone()),
    ] {
        store
            .put_commit("proj-md", cid, &make_test_proto_commit(cid, cid[0], parent))
            .await
            .unwrap();
    }

    // Historical rewrite op X sits beneath both L and M. Because both H0 = merge(L, M)
    // and H1 = child(L) descend from L -> X, X is a common ancestor and must NOT trigger
    // a false client_resolution_required fallback.
    let op_x_id = put_view_and_op(
        &store,
        repo_id,
        vec![c_rewritten.clone()],
        vec![root_op.clone()],
        "historical rewrite",
        vec![cc_common::op_store::CommitPredecessors {
            commit_id: c_rewritten,
            predecessor_ids: vec![c_old],
        }],
    )
    .await;
    let op_l_id = put_view_and_op(
        &store,
        repo_id,
        vec![c_l.clone()],
        vec![op_x_id.clone()],
        "op L",
        new_commit_predecessors(c_l.clone()),
    )
    .await;
    let op_m_id = put_view_and_op(
        &store,
        repo_id,
        vec![c_m.clone()],
        vec![op_x_id],
        "op M",
        new_commit_predecessors(c_m.clone()),
    )
    .await;
    let op_h0_id = put_view_and_op(
        &store,
        repo_id,
        vec![c_l, c_m],
        vec![op_l_id.clone(), op_m_id],
        "merge L and M",
        vec![],
    )
    .await;
    let op_h1_id = put_view_and_op(
        &store,
        repo_id,
        vec![c_h1.clone()],
        vec![op_l_id],
        "child of L",
        new_commit_predecessors(c_h1),
    )
    .await;

    store
        .update_op_heads(repo_id, &[root_op], &op_h0_id)
        .await
        .unwrap();
    store
        .update_op_heads(repo_id, &[], &op_h1_id)
        .await
        .unwrap();

    let resp = reconcile_repo_op_heads_proto(store.clone(), repo_id)
        .await
        .unwrap();
    assert!(
        !resp.client_resolution_required && !resp.op_head.is_empty(),
        "expected server reconciliation without leaking past common ancestor X, got {resp:?}"
    );
}

#[tokio::test]
async fn test_reconcile_three_head_linear_ancestor_chain_with_rewrite() {
    let store = Arc::new(CompositeStore::in_memory());
    let repo_id = "repo-three-head-chain";
    store
        .register_repo_in_project(repo_id, Some("proj-3h"), None)
        .await
        .unwrap();

    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let c0 = vec![0x40; 20];
    let c1 = vec![0x41; 20];
    let c2 = vec![0x42; 20];
    for (cid, parent) in [
        (&c0, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
        (&c1, c0.clone()),
        (&c2, c0.clone()),
    ] {
        store
            .put_commit("proj-3h", cid, &make_test_proto_commit(cid, cid[0], parent))
            .await
            .unwrap();
    }

    // Chain: op_h0 (rewrites c1 -> c2) -> op_h1 (adds c1) -> op_h2 (adds c0) -> root.
    let op_h2 = put_view_and_op(
        &store,
        repo_id,
        vec![c0.clone()],
        vec![root_op.clone()],
        "h2",
        new_commit_predecessors(c0),
    )
    .await;
    let op_h1 = put_view_and_op(
        &store,
        repo_id,
        vec![c1.clone()],
        vec![op_h2.clone()],
        "h1",
        new_commit_predecessors(c1.clone()),
    )
    .await;
    let op_h0 = put_view_and_op(
        &store,
        repo_id,
        vec![c2.clone()],
        vec![op_h1.clone()],
        "h0 rewrite",
        vec![cc_common::op_store::CommitPredecessors {
            commit_id: c2,
            predecessor_ids: vec![c1],
        }],
    )
    .await;

    store
        .update_op_heads(repo_id, &[root_op], &op_h2)
        .await
        .unwrap();
    store.update_op_heads(repo_id, &[], &op_h1).await.unwrap();
    store.update_op_heads(repo_id, &[], &op_h0).await.unwrap();

    let resp = reconcile_repo_op_heads_proto(store.clone(), repo_id)
        .await
        .unwrap();
    assert!(!resp.client_resolution_required);
    assert_eq!(
        resp.op_head, op_h0,
        "3-head linear ancestor chain should prune ancestors directly to H0 even when H0 is a rewrite"
    );
    assert_eq!(store.get_op_heads(repo_id).await.unwrap(), vec![op_h0]);
}

#[tokio::test]
async fn test_reconcile_asymmetric_merge_diamonds_do_not_leak_past_common_ancestor() {
    // Asymmetric diamond terminating at root:
    // root <- X (rewrite) <- D2 <- D1 <- C, B -> X, A -> C, H0 = merge(A, B), H1 -> C.
    {
        let store = Arc::new(CompositeStore::in_memory());
        let repo_id = "repo-asym-diamond-root";
        store
            .register_repo_in_project(repo_id, Some("proj-adr"), None)
            .await
            .unwrap();

        let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
        let c_old = vec![0x50; 20];
        let c_x = vec![0x51; 20];
        let c_d2 = vec![0x52; 20];
        let c_d1 = vec![0x53; 20];
        let c_c = vec![0x54; 20];
        let c_a = vec![0x55; 20];
        let c_b = vec![0x56; 20];
        let c_h1 = vec![0x57; 20];
        for (cid, parent) in [
            (&c_old, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
            (&c_x, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
            (&c_d2, c_x.clone()),
            (&c_d1, c_d2.clone()),
            (&c_c, c_d1.clone()),
            (&c_a, c_c.clone()),
            (&c_b, c_x.clone()),
            (&c_h1, c_c.clone()),
        ] {
            store
                .put_commit(
                    "proj-adr",
                    cid,
                    &make_test_proto_commit(cid, cid[0], parent),
                )
                .await
                .unwrap();
        }

        let op_x = put_view_and_op(
            &store,
            repo_id,
            vec![c_x.clone()],
            vec![root_op.clone()],
            "historical rewrite X",
            vec![cc_common::op_store::CommitPredecessors {
                commit_id: c_x.clone(),
                predecessor_ids: vec![c_old],
            }],
        )
        .await;
        let op_d2 = put_view_and_op(
            &store,
            repo_id,
            vec![c_d2.clone()],
            vec![op_x.clone()],
            "D2",
            new_commit_predecessors(c_d2),
        )
        .await;
        let op_d1 = put_view_and_op(
            &store,
            repo_id,
            vec![c_d1.clone()],
            vec![op_d2],
            "D1",
            new_commit_predecessors(c_d1),
        )
        .await;
        let op_c = put_view_and_op(
            &store,
            repo_id,
            vec![c_c.clone()],
            vec![op_d1],
            "C",
            new_commit_predecessors(c_c),
        )
        .await;
        let op_a = put_view_and_op(
            &store,
            repo_id,
            vec![c_a.clone()],
            vec![op_c.clone()],
            "A",
            new_commit_predecessors(c_a.clone()),
        )
        .await;
        let op_b = put_view_and_op(
            &store,
            repo_id,
            vec![c_b.clone()],
            vec![op_x],
            "B",
            new_commit_predecessors(c_b.clone()),
        )
        .await;
        let op_h0 = put_view_and_op(
            &store,
            repo_id,
            vec![c_a, c_b],
            vec![op_a, op_b],
            "H0 merge(A, B)",
            vec![],
        )
        .await;
        let op_h1 = put_view_and_op(
            &store,
            repo_id,
            vec![c_h1.clone()],
            vec![op_c],
            "H1 -> C",
            new_commit_predecessors(c_h1),
        )
        .await;

        store
            .update_op_heads(repo_id, &[root_op], &op_h0)
            .await
            .unwrap();
        store.update_op_heads(repo_id, &[], &op_h1).await.unwrap();

        let resp = reconcile_repo_op_heads_proto(store.clone(), repo_id)
            .await
            .unwrap();
        assert!(
            !resp.client_resolution_required && !resp.op_head.is_empty(),
            "asymmetric diamond terminating at root must reconcile cleanly, got {resp:?}"
        );
    }

    // Mid-history asymmetric diamond terminating at a deeper common ancestor C2:
    // root <- C2 <- X (rewrite) <- D2 <- D1 <- C, B -> X, E -> C2, F -> C2,
    // A = merge(C, E), H1 = merge(C, F), H0 = merge(A, B).
    {
        let store = Arc::new(CompositeStore::in_memory());
        let repo_id = "repo-asym-diamond-mid";
        store
            .register_repo_in_project(repo_id, Some("proj-adm"), None)
            .await
            .unwrap();

        let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
        let c_c2 = vec![0x60; 20];
        let c_old = vec![0x61; 20];
        let c_x = vec![0x62; 20];
        let c_d2 = vec![0x63; 20];
        let c_d1 = vec![0x64; 20];
        let c_c = vec![0x65; 20];
        let c_e = vec![0x66; 20];
        let c_f = vec![0x67; 20];
        let c_b = vec![0x68; 20];
        for (cid, parent) in [
            (&c_c2, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
            (&c_old, c_c2.clone()),
            (&c_x, c_c2.clone()),
            (&c_d2, c_x.clone()),
            (&c_d1, c_d2.clone()),
            (&c_c, c_d1.clone()),
            (&c_e, c_c2.clone()),
            (&c_f, c_c2.clone()),
            (&c_b, c_x.clone()),
        ] {
            store
                .put_commit(
                    "proj-adm",
                    cid,
                    &make_test_proto_commit(cid, cid[0], parent),
                )
                .await
                .unwrap();
        }

        let op_c2 = put_view_and_op(
            &store,
            repo_id,
            vec![c_c2.clone()],
            vec![root_op.clone()],
            "C2",
            new_commit_predecessors(c_c2),
        )
        .await;
        let op_x = put_view_and_op(
            &store,
            repo_id,
            vec![c_x.clone()],
            vec![op_c2.clone()],
            "mid-history rewrite X",
            vec![cc_common::op_store::CommitPredecessors {
                commit_id: c_x.clone(),
                predecessor_ids: vec![c_old],
            }],
        )
        .await;
        let op_d2 = put_view_and_op(
            &store,
            repo_id,
            vec![c_d2.clone()],
            vec![op_x.clone()],
            "D2",
            new_commit_predecessors(c_d2),
        )
        .await;
        let op_d1 = put_view_and_op(
            &store,
            repo_id,
            vec![c_d1.clone()],
            vec![op_d2],
            "D1",
            new_commit_predecessors(c_d1),
        )
        .await;
        let op_c = put_view_and_op(
            &store,
            repo_id,
            vec![c_c.clone()],
            vec![op_d1],
            "C",
            new_commit_predecessors(c_c.clone()),
        )
        .await;
        let op_e = put_view_and_op(
            &store,
            repo_id,
            vec![c_e.clone()],
            vec![op_c2.clone()],
            "E",
            new_commit_predecessors(c_e.clone()),
        )
        .await;
        let op_f = put_view_and_op(
            &store,
            repo_id,
            vec![c_f.clone()],
            vec![op_c2],
            "F",
            new_commit_predecessors(c_f.clone()),
        )
        .await;
        let op_b = put_view_and_op(
            &store,
            repo_id,
            vec![c_b.clone()],
            vec![op_x],
            "B",
            new_commit_predecessors(c_b.clone()),
        )
        .await;
        let op_a = put_view_and_op(
            &store,
            repo_id,
            vec![c_c.clone(), c_e.clone()],
            vec![op_c.clone(), op_e],
            "A merge(C, E)",
            vec![],
        )
        .await;
        let op_h1 = put_view_and_op(
            &store,
            repo_id,
            vec![c_c.clone(), c_f],
            vec![op_c, op_f],
            "H1 merge(C, F)",
            vec![],
        )
        .await;
        let op_h0 = put_view_and_op(
            &store,
            repo_id,
            vec![c_c, c_e, c_b],
            vec![op_a, op_b],
            "H0 merge(A, B)",
            vec![],
        )
        .await;

        store
            .update_op_heads(repo_id, &[root_op], &op_h0)
            .await
            .unwrap();
        store.update_op_heads(repo_id, &[], &op_h1).await.unwrap();

        let resp = reconcile_repo_op_heads_proto(store.clone(), repo_id)
            .await
            .unwrap();
        assert!(
            !resp.client_resolution_required && !resp.op_head.is_empty(),
            "mid-history asymmetric diamond must reconcile cleanly, got {resp:?}"
        );
    }

    // Lagging common merge op with two distinct historical rewrite parents (X1, X2).
    for swap_order in [false, true] {
        let store = Arc::new(CompositeStore::in_memory());
        let repo_id = if swap_order {
            "repo-asym-two-rewrites-swapped"
        } else {
            "repo-asym-two-rewrites"
        };
        store
            .register_repo_in_project(repo_id, Some("proj-a2r"), None)
            .await
            .unwrap();

        let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
        let c_old1 = vec![0x70; 20];
        let c_x1 = vec![0x71; 20];
        let c_old2 = vec![0x72; 20];
        let c_x2 = vec![0x73; 20];
        let c_e2 = vec![0x74; 20];
        let c_e1 = vec![0x75; 20];
        let c_a = vec![0x76; 20];
        let c_h1 = vec![0x77; 20];
        for (cid, parent) in [
            (&c_old1, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
            (&c_x1, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
            (&c_old2, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
            (&c_x2, cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
            (&c_e2, c_x1.clone()),
            (&c_e1, c_e2.clone()),
            (&c_a, c_e1.clone()),
            (&c_h1, c_e1.clone()),
        ] {
            store
                .put_commit(
                    "proj-a2r",
                    cid,
                    &make_test_proto_commit(cid, cid[0], parent),
                )
                .await
                .unwrap();
        }

        let op_x1 = put_view_and_op(
            &store,
            repo_id,
            vec![c_x1.clone()],
            vec![root_op.clone()],
            "rewrite X1",
            vec![cc_common::op_store::CommitPredecessors {
                commit_id: c_x1.clone(),
                predecessor_ids: vec![c_old1],
            }],
        )
        .await;
        let op_x2 = put_view_and_op(
            &store,
            repo_id,
            vec![c_x2.clone()],
            vec![root_op.clone()],
            "rewrite X2",
            vec![cc_common::op_store::CommitPredecessors {
                commit_id: c_x2.clone(),
                predecessor_ids: vec![c_old2],
            }],
        )
        .await;
        let c_parents = if swap_order {
            vec![op_x2.clone(), op_x1.clone()]
        } else {
            vec![op_x1.clone(), op_x2.clone()]
        };
        let op_c = put_view_and_op(
            &store,
            repo_id,
            vec![c_x1.clone(), c_x2.clone()],
            c_parents.clone(),
            "C merge(X1, X2)",
            vec![],
        )
        .await;
        let op_e2 = put_view_and_op(
            &store,
            repo_id,
            vec![c_e2.clone(), c_x2.clone()],
            vec![op_c],
            "E2",
            new_commit_predecessors(c_e2),
        )
        .await;
        let op_e1 = put_view_and_op(
            &store,
            repo_id,
            vec![c_e1.clone(), c_x2.clone()],
            vec![op_e2],
            "E1",
            new_commit_predecessors(c_e1),
        )
        .await;
        let op_a = put_view_and_op(
            &store,
            repo_id,
            vec![c_a.clone(), c_x2.clone()],
            vec![op_e1.clone()],
            "A",
            new_commit_predecessors(c_a.clone()),
        )
        .await;
        let op_h1 = put_view_and_op(
            &store,
            repo_id,
            vec![c_h1.clone(), c_x2.clone()],
            vec![op_e1],
            "H1",
            new_commit_predecessors(c_h1),
        )
        .await;
        let op_b = put_view_and_op(
            &store,
            repo_id,
            vec![c_x1, c_x2.clone()],
            c_parents,
            "B merge(X1, X2)",
            vec![],
        )
        .await;
        let op_h0 = put_view_and_op(
            &store,
            repo_id,
            vec![c_a, c_x2],
            vec![op_a, op_b],
            "H0 merge(A, B)",
            vec![],
        )
        .await;

        store
            .update_op_heads(repo_id, &[root_op], &op_h0)
            .await
            .unwrap();
        store.update_op_heads(repo_id, &[], &op_h1).await.unwrap();

        let resp = reconcile_repo_op_heads_proto(store.clone(), repo_id)
            .await
            .unwrap();
        assert!(
            !resp.client_resolution_required && !resp.op_head.is_empty(),
            "lagging common merge with two rewrite parents must reconcile cleanly (swap={swap_order}), got {resp:?}"
        );
    }
}

#[tokio::test]
async fn test_reconcile_fails_closed_when_commit_predecessors_unset() {
    let store = Arc::new(CompositeStore::in_memory());
    let repo_id = "repo-unset-predecessors";
    store.register_repo(repo_id, None).await.unwrap();

    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let c1 = vec![0xa1; 20];
    let c2 = vec![0xa2; 20];
    for cid in [&c1, &c2] {
        store
            .put_commit(
                repo_id,
                cid,
                &make_test_proto_commit(cid, cid[0], cc_common::ROOT_COMMIT_ID_BYTES.to_vec()),
            )
            .await
            .unwrap();
    }

    let op_a_id = put_view_and_op(
        &store,
        repo_id,
        vec![c1.clone()],
        vec![root_op.clone()],
        "op with preds",
        new_commit_predecessors(c1),
    )
    .await;

    let view_b = ProtoView {
        head_ids: vec![c2],
        wc_commit_ids: HashMap::new(),
        local_bookmarks: HashMap::new(),
        remote_bookmarks: HashMap::new(),
    };
    let view_b_id = crate::hash_utils::hash_view(&view_b).unwrap();
    store.put_view(repo_id, &view_b_id, &view_b).await.unwrap();
    let mut op_b = make_test_proto_op(view_b_id, vec![root_op.clone()], "legacy op without preds");
    op_b.commit_predecessors_set = false;
    let op_b_id = crate::hash_utils::hash_operation(&op_b).unwrap();
    store.put_operation(repo_id, &op_b_id, &op_b).await.unwrap();

    store
        .update_op_heads(repo_id, &[root_op], &op_a_id)
        .await
        .unwrap();
    store.update_op_heads(repo_id, &[], &op_b_id).await.unwrap();

    let resp = reconcile_repo_op_heads_proto(store.clone(), repo_id)
        .await
        .unwrap();
    assert!(
        resp.client_resolution_required,
        "divergent op with commit_predecessors_set=false must fail closed"
    );
    assert!(resp.op_head.is_empty());
}

#[tokio::test]
async fn test_reconcile_preserves_typed_non_transient_status_codes() {
    // Corrupt persisted operation row surfaces DATA_LOSS (not flattened to INTERNAL).
    let meta = Arc::new(MemoryMetadataStore::new());
    let store = Arc::new(CompositeStore::with_config(
        meta.clone(),
        Arc::new(MemoryBlobStore::new()),
        CompositeStoreConfig {
            inline_blob_threshold: 0,
            validate_dependencies: false,
        },
    ));
    let repo_id = "repo-corrupt-status";
    store.register_repo(repo_id, None).await.unwrap();
    let root_op = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
    let corrupt_op_id = vec![0xf1; cc_common::OPERATION_ID_LENGTH];
    let valid_op_id = put_view_and_op(
        &store,
        repo_id,
        vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
        vec![root_op.clone()],
        "valid op",
        vec![],
    )
    .await;
    meta.put_operation(
        repo_id,
        &corrupt_op_id,
        &ProtoOperation {
            view_id: vec![0x01; 3],
            parents: vec![root_op.clone()],
            metadata: None,
            commit_predecessors: Vec::new(),
            commit_predecessors_set: true,
        },
    )
    .await
    .unwrap();
    store
        .update_op_heads(repo_id, std::slice::from_ref(&root_op), &valid_op_id)
        .await
        .unwrap();
    store
        .update_op_heads(repo_id, &[], &corrupt_op_id)
        .await
        .unwrap();

    let err = reconcile_repo_op_heads_proto(store.clone(), repo_id)
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::DataLoss,
        "corrupt stored operation must preserve Code::DataLoss, got {err:?}"
    );

    // Missing operation head surfaces NOT_FOUND (not flattened to INTERNAL).
    let repo_missing = "repo-missing-op";
    store.register_repo(repo_missing, None).await.unwrap();
    let missing_a = vec![0xf2; cc_common::OPERATION_ID_LENGTH];
    let missing_b = vec![0xf3; cc_common::OPERATION_ID_LENGTH];
    meta.update_op_heads(repo_missing, &[root_op], &missing_a)
        .await
        .unwrap();
    meta.update_op_heads(repo_missing, &[], &missing_b)
        .await
        .unwrap();
    let missing_err = reconcile_repo_op_heads_proto(store.clone(), repo_missing)
        .await
        .unwrap_err();
    assert_eq!(
        missing_err.code(),
        tonic::Code::NotFound,
        "missing op head must preserve Code::NotFound, got {missing_err:?}"
    );

    // Corrupt single-head op_head length surfaces DATA_LOSS on the single-head fast path.
    let repo_bad_single = "repo-bad-single-head";
    store.register_repo(repo_bad_single, None).await.unwrap();
    meta.update_op_heads(
        repo_bad_single,
        &[cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
        &[0xaa; 3],
    )
    .await
    .unwrap();
    let bad_single_err = reconcile_repo_op_heads_proto(store, repo_bad_single)
        .await
        .unwrap_err();
    assert_eq!(
        bad_single_err.code(),
        tonic::Code::DataLoss,
        "corrupt single op_head length must preserve Code::DataLoss, got {bad_single_err:?}"
    );
}
