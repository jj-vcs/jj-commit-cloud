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

//! The SQLite store keeps everything across a restart (what DEV-202 needs).

use bytes::Bytes;
use cc_storage::digest::compute;
use cc_storage::memory::MemoryMetadataStore;
use cc_storage::sqlite::SqliteMetadataStore;
use cc_storage::*;

#[tokio::test]
async fn state_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("meta.db");
    let root = ObjectRef {
        kind: ObjectKind::Operation,
        id: compute(HashAlgorithm::Sha256, b"root"),
    };
    let open = || {
        SqliteMetadataStore::open(
            &path,
            MemoryMetadataStore::default_limits(),
            vec![root.clone()],
        )
        .unwrap()
    };

    let project = ProjectId("demo".into());
    let (ws, new_head) = {
        let store = open();
        store.create_project(&project).await.unwrap();
        let ws = store
            .create_workspace(
                &project,
                &WorkspaceName("u/w".into()),
                OperationId(root.id.clone()),
            )
            .await
            .unwrap();
        let data = Bytes::from_static(b"op 1");
        let op = ObjectRecord {
            kind: ObjectKind::Operation,
            id: compute(HashAlgorithm::Sha256, &data),
            data,
            refs: vec![root.clone()],
            blob_refs: vec![],
        };
        store
            .put_objects(&Scope::Workspace(ws.id.clone()), vec![op.clone()])
            .await
            .unwrap();
        let req = AdvanceHead {
            workspace: ws.id.clone(),
            expected: OperationId(root.id.clone()),
            new_head: OperationId(op.id.clone()),
            mode: AdvanceMode::Reject,
            request_id: RequestId(b"r1".to_vec()),
        };
        store.advance_head(req).await.unwrap();
        (ws, OperationId(op.id))
    }; // store dropped: simulated restart

    let store = open();
    let row = store
        .get_workspace(&ws.id)
        .await
        .unwrap()
        .expect("workspace survives");
    assert_eq!((row.head.head, row.head.sequence), (new_head, 1));
    assert_eq!(store.read_events(&ws.id, 0, 10).await.unwrap().len(), 1);
    let by_name = store
        .get_workspace_by_name(&WorkspaceName("u/w".into()))
        .await
        .unwrap();
    assert_eq!(by_name.map(|w| w.id), Some(ws.id));
}
