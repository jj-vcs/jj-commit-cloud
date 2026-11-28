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

mod budgets_and_lifecycle;
mod kill_switch;
mod merge_and_rewrites;

use std::collections::HashMap;

use cc_common::backend::Commit as ProtoCommit;
use cc_common::op_store::{
    Operation as ProtoOperation, OperationMetadata as ProtoOperationMetadata, View as ProtoView,
};

use crate::store::CompositeStore;

fn make_test_proto_op(view_id: Vec<u8>, parents: Vec<Vec<u8>>, desc: &str) -> ProtoOperation {
    ProtoOperation {
        view_id,
        parents,
        metadata: Some(ProtoOperationMetadata {
            start_time_millis: 1000,
            end_time_millis: 1000,
            description: desc.to_string(),
            is_snapshot: false,
            workspace_name: None,
            hostname: "test-host".to_string(),
            username: "test-user".to_string(),
            attributes: HashMap::new(),
        }),
        commit_predecessors: vec![],
        commit_predecessors_set: true,
    }
}

fn make_test_proto_commit(cid: &[u8], change_byte: u8, parent: Vec<u8>) -> ProtoCommit {
    ProtoCommit {
        commit_id: cid.to_vec(),
        change_id: vec![change_byte; cc_common::CHANGE_ID_LENGTH],
        parent_commit_ids: vec![parent],
        root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
        description: "test\n".to_string(),
        author: None,
        committer: None,
        predecessors: vec![],
        conflict_labels: vec![],
        secure_sig: None,
        raw_git_commit: vec![],
        external_ids: Default::default(),
    }
}

fn new_commit_predecessors(commit_id: Vec<u8>) -> Vec<cc_common::op_store::CommitPredecessors> {
    vec![cc_common::op_store::CommitPredecessors {
        commit_id,
        predecessor_ids: vec![],
    }]
}

async fn put_view_and_op(
    store: &CompositeStore,
    repo_id: &str,
    head_ids: Vec<Vec<u8>>,
    parents: Vec<Vec<u8>>,
    desc: &str,
    preds: Vec<cc_common::op_store::CommitPredecessors>,
) -> Vec<u8> {
    let view = ProtoView {
        head_ids,
        wc_commit_ids: HashMap::new(),
        local_bookmarks: HashMap::new(),
        remote_bookmarks: HashMap::new(),
    };
    let vid = crate::hash_utils::hash_view(&view).unwrap();
    store.put_view(repo_id, &vid, &view).await.unwrap();
    let mut op = make_test_proto_op(vid, parents, desc);
    op.commit_predecessors = preds;
    let oid = crate::hash_utils::hash_operation(&op).unwrap();
    store.put_operation(repo_id, &oid, &op).await.unwrap();
    oid
}
