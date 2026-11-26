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

use std::sync::Arc;

use cc_common::ROOT_OPERATION_ID_BYTES;
use cc_common::op_store::op_store_service_server::OpStoreService;
use cc_common::op_store::*;

use super::super::DaemonProxy;
use super::{MockUpstream, req_with_upstream, spawn_mock_upstream};
use crate::cache::TwoTierCache;
use crate::upstream::UpstreamPool;

#[tokio::test]
async fn publish_operation_caches_view_and_operation_for_subsequent_reads() {
    let mock = MockUpstream::default();
    let url = spawn_mock_upstream(mock.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 8 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new(cache, Arc::new(UpstreamPool::new(None)), None);

    let view = View {
        head_ids: vec![vec![0xab; 20]],
        ..Default::default()
    };
    let op = Operation {
        view_id: Vec::new(),
        parents: vec![ROOT_OPERATION_ID_BYTES.to_vec()],
        metadata: Some(OperationMetadata {
            description: "published op".to_string(),
            ..Default::default()
        }),
        commit_predecessors: Vec::new(),
        commit_predecessors_set: true,
    };
    let published = OpStoreService::publish_operation(
        &proxy,
        req_with_upstream(
            PublishOperationRequest {
                repo_id: "repo-1".to_string(),
                view: Some(view.clone()),
                operation: Some(op),
                old_op_head_ids: vec![ROOT_OPERATION_ID_BYTES.to_vec()],
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner();

    let read_v = OpStoreService::read_view(
        &proxy,
        req_with_upstream(
            ReadViewRequest {
                repo_id: "repo-1".to_string(),
                view_id: published.view_id.clone(),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(read_v.view.unwrap().head_ids, view.head_ids);

    let read_op = OpStoreService::read_operation(
        &proxy,
        req_with_upstream(
            ReadOperationRequest {
                repo_id: "repo-1".to_string(),
                operation_id: published.operation_id,
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .operation
    .unwrap();
    assert_eq!(read_op.view_id, published.view_id);
    assert_eq!(read_op.metadata.unwrap().description, "published op");
    assert_eq!(mock.state.lock().unwrap().read_view_calls, 0);
    assert_eq!(mock.state.lock().unwrap().read_op_calls, 0);
}

#[tokio::test]
async fn read_view_rejects_mismatched_view_id_without_caching() {
    let mock = MockUpstream::default();
    let requested_view_id = vec![0x5a; 16];
    let wrong_view_id = vec![0x9b; 16];
    {
        let mut st = mock.state.lock().unwrap();
        st.views.insert(
            requested_view_id.clone(),
            View {
                head_ids: vec![vec![0xcd; 20]],
                ..Default::default()
            },
        );
        st.view_id_overrides
            .insert(requested_view_id.clone(), wrong_view_id);
    }
    let url = spawn_mock_upstream(mock.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 8 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new(Arc::clone(&cache), Arc::new(UpstreamPool::new(None)), None);

    let err = OpStoreService::read_view(
        &proxy,
        req_with_upstream(
            ReadViewRequest {
                repo_id: "repo-1".to_string(),
                view_id: requested_view_id.clone(),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Internal);
    assert_eq!(mock.state.lock().unwrap().read_view_calls, 1);

    // Must not have cached the mismatched response; once upstream returns the matching id,
    // a retry reaches upstream and succeeds.
    mock.state
        .lock()
        .unwrap()
        .view_id_overrides
        .remove(&requested_view_id);
    let ok = OpStoreService::read_view(
        &proxy,
        req_with_upstream(
            ReadViewRequest {
                repo_id: "repo-1".to_string(),
                view_id: requested_view_id.clone(),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(ok.view_id, requested_view_id);
    assert_eq!(ok.view.unwrap().head_ids, vec![vec![0xcd; 20]]);
    assert_eq!(mock.state.lock().unwrap().read_view_calls, 2);
}
