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

//! Conformance suite for [`MetadataStore`]: the [`objects`] suite, then the
//! workspace and head cases. Every case maps to a sentence of the trait's doc
//! comments or to an acceptance criterion of DEV-86, DEV-89 or DEV-92 (noted
//! per case).

use super::objects::{self, Scopes};
use super::unique;
use crate::digest::compute;
use crate::{
    AdvanceHead, AdvanceHeadError, AdvanceMode, ErrorKind, HashAlgorithm, MetadataStore,
    ObjectKind, ObjectRecord, ObjectRef, OperationId, ProjectId, RequestId, Scope, WorkspaceName,
    WorkspaceRecord,
};
use bytes::Bytes;
use std::sync::Arc;

type Store = Arc<dyn MetadataStore>;

/// Run every case against `store`: the [`objects`] cases in a fresh project
/// and workspace, then the workspace and head cases.
pub async fn run_all(store: Store) {
    let ws = workspace(&store).await;
    let scopes = Scopes {
        project: ws.project,
        workspace: ws.workspace,
    };
    objects::run_all(store.clone(), scopes).await;
    create_project_twice_is_already_exists(store.clone()).await;
    create_workspace_starts_at_sequence_zero(store.clone()).await;
    create_workspace_needs_a_project(store.clone()).await;
    advance_head_moves_head_and_writes_event(store.clone()).await;
    stale_head_is_rejected_and_writes_nothing(store.clone()).await;
    missing_objects_are_reported_and_nothing_moves(store.clone()).await;
    retry_of_the_last_request_succeeds(store.clone()).await;
    merge_mode_is_unsupported(store.clone()).await;
    racing_writers_have_exactly_one_winner(store.clone()).await;
    read_events_pages_in_sequence_order(store.clone()).await;
    unknown_workspace_is_not_found(store.clone()).await;
    put_into_unknown_project_is_not_found(store.clone()).await;
    missing_list_is_truncated_at_the_limit(store.clone()).await;
    closure_does_not_cross_projects(store.clone()).await;
    reused_request_id_for_another_head_is_rejected(store.clone()).await;
    retry_after_another_writer_finds_its_event(store.clone()).await;
    read_events_with_zero_limit_is_invalid(store.clone()).await;
    virtual_roots_count_as_present(store.clone()).await;
    failed_check_does_not_hide_missing_objects(store.clone()).await;
    heads_of_the_wrong_length_are_refused(store.clone()).await;
    read_events_takes_any_cursor_and_limit(store.clone()).await;
}

/// Number of cases `run_all` runs, the object cases included.
pub const CASE_COUNT: usize = objects::CASE_COUNT + 21;

// ---- fixtures ---------------------------------------------------------------

fn record(kind: ObjectKind, data: &str, refs: Vec<ObjectRef>) -> ObjectRecord {
    let data = Bytes::from(data.to_owned());
    ObjectRecord {
        kind,
        id: compute(HashAlgorithm::Sha256, &data),
        data,
        refs,
        blob_refs: vec![],
    }
}

fn r(rec: &ObjectRecord) -> ObjectRef {
    ObjectRef {
        kind: rec.kind,
        id: rec.id.clone(),
    }
}

fn op_id(rec: &ObjectRecord) -> OperationId {
    OperationId(rec.id.clone())
}

fn req_id() -> RequestId {
    RequestId(unique("req").into_bytes())
}

struct Ws {
    record: WorkspaceRecord,
    project: Scope,
    workspace: Scope,
}

/// A project, and a workspace whose head is a stored root operation.
async fn workspace(store: &Store) -> Ws {
    let project = ProjectId(unique("proj"));
    store
        .create_project(&project)
        .await
        .expect("create_project");
    let root_op = record(ObjectKind::Operation, &unique("root-op"), vec![]);
    let name = WorkspaceName(format!("tester/{}", unique("ws")));
    let rec = store
        .create_workspace(&project, &name, op_id(&root_op))
        .await
        .expect("create_workspace");
    let workspace = Scope::Workspace(rec.id.clone());
    store
        .put_objects(&workspace, vec![root_op])
        .await
        .expect("put root op");
    Ws {
        record: rec,
        project: Scope::Project(project),
        workspace,
    }
}

/// Store a complete new operation (op -> view -> commit -> tree -> file,
/// symlink) whose parent is `parent`, and return its id.
async fn new_operation(store: &Store, ws: &Ws, parent: &OperationId, tag: &str) -> OperationId {
    let file = record(ObjectKind::FileManifest, &format!("file {tag}"), vec![]);
    let link = record(ObjectKind::Symlink, &format!("link {tag}"), vec![]);
    let tree = record(
        ObjectKind::Tree,
        &format!("tree {tag}"),
        vec![r(&file), r(&link)],
    );
    let commit = record(ObjectKind::Commit, &format!("commit {tag}"), vec![r(&tree)]);
    let view = record(ObjectKind::View, &format!("view {tag}"), vec![r(&commit)]);
    let parent_ref = ObjectRef {
        kind: ObjectKind::Operation,
        id: parent.0.clone(),
    };
    let op = record(
        ObjectKind::Operation,
        &format!("op {tag}"),
        vec![r(&view), parent_ref],
    );
    store
        .put_objects(&ws.project, vec![file, link, tree, commit])
        .await
        .expect("put content");
    store
        .put_objects(&ws.workspace, vec![view, op.clone()])
        .await
        .expect("put op log");
    op_id(&op)
}

fn advance(ws: &Ws, expected: &OperationId, new_head: &OperationId) -> AdvanceHead {
    AdvanceHead {
        workspace: ws.record.id.clone(),
        expected: expected.clone(),
        new_head: new_head.clone(),
        mode: AdvanceMode::Reject,
        request_id: req_id(),
    }
}

// ---- workspaces -------------------------------------------------------------

/// Trait: creating an existing project fails with `AlreadyExists` (AIP-133)
/// and changes nothing.
pub async fn create_project_twice_is_already_exists(store: Store) {
    let project = ProjectId(unique("proj"));
    store.create_project(&project).await.expect("first create");
    let e = store
        .create_project(&project)
        .await
        .expect_err("second create");
    assert_eq!(e.kind, ErrorKind::AlreadyExists);
}

/// DEV-92: "exists with sequence 0; a second call with the same name returns
/// ALREADY_EXISTS".
pub async fn create_workspace_starts_at_sequence_zero(store: Store) {
    let ws = workspace(&store).await;
    assert_eq!(ws.record.head.sequence, 0);
    let by_name = store.get_workspace_by_name(&ws.record.name).await.unwrap();
    assert_eq!(by_name.as_ref(), Some(&ws.record), "lookup by name");
    let by_id = store.get_workspace(&ws.record.id).await.unwrap();
    assert_eq!(by_id.as_ref(), Some(&ws.record), "lookup by id");
    let e = store
        .create_workspace(
            &ws.record.project,
            &ws.record.name,
            ws.record.head.head.clone(),
        )
        .await
        .expect_err("duplicate name");
    assert_eq!(e.kind, ErrorKind::AlreadyExists);
}

/// Trait: `NotFound` (unknown project).
pub async fn create_workspace_needs_a_project(store: Store) {
    let head = OperationId(compute(HashAlgorithm::Sha256, b"x"));
    let e = store
        .create_workspace(
            &ProjectId(unique("nope")),
            &WorkspaceName(unique("u/ws")),
            head,
        )
        .await
        .expect_err("unknown project");
    assert_eq!(e.kind, ErrorKind::NotFound);
}

/// DEV-86: "the row holds the new head and sequence n+1, and a RepoEvents
/// row with sequence n+1 exists".
pub async fn advance_head_moves_head_and_writes_event(store: Store) {
    let ws = workspace(&store).await;
    let base = ws.record.head.head.clone();
    let next = new_operation(&store, &ws, &base, &unique("n")).await;
    let req = advance(&ws, &base, &next);
    let state = store.advance_head(req.clone()).await.expect("advance");
    assert_eq!(
        (state.head.clone(), state.sequence, state.merged),
        (next.clone(), 1, false)
    );
    let row = store.get_workspace(&ws.record.id).await.unwrap().unwrap();
    assert_eq!(row.head, state, "workspace row holds the new head");
    let events = store.read_events(&ws.record.id, 0, 10).await.unwrap();
    assert_eq!(events.len(), 1, "one event");
    assert_eq!(events[0].state, state);
    assert_eq!(events[0].request_id, req.request_id);
}

/// DEV-86: "returns the current head and writes nothing".
pub async fn stale_head_is_rejected_and_writes_nothing(store: Store) {
    let ws = workspace(&store).await;
    let base = ws.record.head.head.clone();
    let first = new_operation(&store, &ws, &base, &unique("a")).await;
    store
        .advance_head(advance(&ws, &base, &first))
        .await
        .unwrap();
    let second = new_operation(&store, &ws, &base, &unique("b")).await;
    match store.advance_head(advance(&ws, &base, &second)).await {
        Err(AdvanceHeadError::HeadMoved { current }) => {
            assert_eq!((current.head, current.sequence), (first.clone(), 1));
        }
        other => panic!("expected HeadMoved, got {other:?}"),
    }
    let events = store.read_events(&ws.record.id, 0, 10).await.unwrap();
    assert_eq!(events.len(), 1, "no event for the rejected advance");
}

/// DEV-89: "fails with the missing object's id and moves nothing".
pub async fn missing_objects_are_reported_and_nothing_moves(store: Store) {
    let ws = workspace(&store).await;
    let base = ws.record.head.head.clone();
    let missing_commit = record(ObjectKind::Commit, &unique("never stored"), vec![]);
    let view = record(ObjectKind::View, &unique("v"), vec![r(&missing_commit)]);
    let op = record(ObjectKind::Operation, &unique("op"), vec![r(&view)]);
    store
        .put_objects(&ws.workspace, vec![view, op.clone()])
        .await
        .unwrap();
    match store.advance_head(advance(&ws, &base, &op_id(&op))).await {
        Err(AdvanceHeadError::MissingObjects { missing }) => {
            assert_eq!(
                missing,
                vec![r(&missing_commit)],
                "names the missing commit"
            );
        }
        other => panic!("expected MissingObjects, got {other:?}"),
    }
    let row = store.get_workspace(&ws.record.id).await.unwrap().unwrap();
    assert_eq!(row.head.sequence, 0, "head did not move");
}

/// Trait: "a retry after a lost reply does not report HeadMoved against itself".
pub async fn retry_of_the_last_request_succeeds(store: Store) {
    let ws = workspace(&store).await;
    let base = ws.record.head.head.clone();
    let next = new_operation(&store, &ws, &base, &unique("n")).await;
    let req = advance(&ws, &base, &next);
    let first = store.advance_head(req.clone()).await.unwrap();
    let retry = store
        .advance_head(req)
        .await
        .expect("retry with same request id");
    assert_eq!(retry, first);
    assert_eq!(
        store.read_events(&ws.record.id, 0, 10).await.unwrap().len(),
        1,
        "applied once"
    );
}

/// Trait: "`Merge` returns `Unsupported`" (v0).
pub async fn merge_mode_is_unsupported(store: Store) {
    let ws = workspace(&store).await;
    let base = ws.record.head.head.clone();
    let next = new_operation(&store, &ws, &base, &unique("n")).await;
    let mut req = advance(&ws, &base, &next);
    req.mode = AdvanceMode::Merge;
    match store.advance_head(req).await {
        Err(AdvanceHeadError::Store(e)) if e.kind == ErrorKind::Unsupported => {}
        other => panic!("expected Unsupported, got {other:?}"),
    }
}

/// DEV-86: "two writers that expect the same head ... each round has one
/// success and one head-moved error" (here: 8 writers, 20 rounds).
pub async fn racing_writers_have_exactly_one_winner(store: Store) {
    let ws = Arc::new(workspace(&store).await);
    let mut head = ws.record.head.head.clone();
    for round in 0..20 {
        let mut tasks = Vec::new();
        for writer in 0..8 {
            let (store, ws, base) = (store.clone(), ws.clone(), head.clone());
            tasks.push(tokio::spawn(async move {
                let next = new_operation(&store, &ws, &base, &format!("r{round} w{writer}")).await;
                store.advance_head(advance(&ws, &base, &next)).await
            }));
        }
        let mut winners = Vec::new();
        for t in tasks {
            match t.await.expect("task") {
                Ok(state) => winners.push(state),
                Err(AdvanceHeadError::HeadMoved { .. }) => {}
                Err(e) => panic!("round {round}: unexpected error {e}"),
            }
        }
        assert_eq!(winners.len(), 1, "round {round}: exactly one winner");
        assert_eq!(winners[0].sequence, round + 1);
        head = winners[0].head.clone();
    }
    let events = store.read_events(&ws.record.id, 0, 100).await.unwrap();
    assert_eq!(events.len(), 20, "losers wrote no events");
}

/// Trait: "events with sequence greater than after_sequence, in sequence
/// order, at most limit".
pub async fn read_events_pages_in_sequence_order(store: Store) {
    let ws = workspace(&store).await;
    let mut head = ws.record.head.head.clone();
    for i in 0..5 {
        let next = new_operation(&store, &ws, &head, &format!("{} {i}", unique("e"))).await;
        head = store
            .advance_head(advance(&ws, &head, &next))
            .await
            .unwrap()
            .head;
    }
    let page = store.read_events(&ws.record.id, 1, 2).await.unwrap();
    let seqs: Vec<u64> = page.iter().map(|e| e.state.sequence).collect();
    assert_eq!(seqs, vec![2, 3]);
    assert!(
        store
            .read_events(&ws.record.id, 5, 10)
            .await
            .unwrap()
            .is_empty(),
        "up to date"
    );
}

/// Trait: `NotFound` for an unknown workspace.
pub async fn unknown_workspace_is_not_found(store: Store) {
    let ws = workspace(&store).await;
    let mut req = advance(&ws, &ws.record.head.head, &ws.record.head.head);
    req.workspace = crate::WorkspaceId(unique("ghost").into_bytes());
    match store.advance_head(req.clone()).await {
        Err(AdvanceHeadError::Store(e)) if e.kind == ErrorKind::NotFound => {}
        other => panic!("expected NotFound, got {other:?}"),
    }
    let e = store
        .read_events(&req.workspace, 0, 1)
        .await
        .expect_err("read_events");
    assert_eq!(e.kind, ErrorKind::NotFound);
    assert_eq!(store.get_workspace(&req.workspace).await.unwrap(), None);
}

// ---- added after review -----------------------------------------------------

/// Trait: `NotFound` (unknown project or workspace) on put.
pub async fn put_into_unknown_project_is_not_found(store: Store) {
    let scope = Scope::Project(ProjectId(unique("nope")));
    let a = record(ObjectKind::Commit, &unique("a"), vec![]);
    let e = store
        .put_objects(&scope, vec![a])
        .await
        .expect_err("unknown project");
    assert_eq!(e.kind, ErrorKind::NotFound);
}

/// Limits: "Most missing objects listed in one MissingObjects error".
pub async fn missing_list_is_truncated_at_the_limit(store: Store) {
    let ws = workspace(&store).await;
    let limit = store.limits().max_missing_reported;
    let absent: Vec<ObjectRef> = (0..=limit)
        .map(|i| {
            r(&record(
                ObjectKind::Commit,
                &format!("{} {i}", unique("gone")),
                vec![],
            ))
        })
        .collect();
    let view = record(ObjectKind::View, &unique("v"), absent);
    let op = record(ObjectKind::Operation, &unique("op"), vec![r(&view)]);
    store
        .put_objects(&ws.workspace, vec![view, op.clone()])
        .await
        .unwrap();
    match store
        .advance_head(advance(&ws, &ws.record.head.head, &op_id(&op)))
        .await
    {
        Err(AdvanceHeadError::MissingObjects { missing }) => {
            assert_eq!(missing.len(), limit, "truncated to max_missing_reported");
        }
        other => panic!("expected MissingObjects, got {other:?}"),
    }
}

/// Trait: "Objects of another project never satisfy the check".
pub async fn closure_does_not_cross_projects(store: Store) {
    let ws = workspace(&store).await;
    let other = workspace(&store).await;
    let commit = record(ObjectKind::Commit, &unique("c"), vec![]);
    store
        .put_objects(&other.project, vec![commit.clone()])
        .await
        .unwrap();
    let view = record(ObjectKind::View, &unique("v"), vec![r(&commit)]);
    let op = record(ObjectKind::Operation, &unique("op"), vec![r(&view)]);
    store
        .put_objects(&ws.workspace, vec![view, op.clone()])
        .await
        .unwrap();
    match store
        .advance_head(advance(&ws, &ws.record.head.head, &op_id(&op)))
        .await
    {
        Err(AdvanceHeadError::MissingObjects { missing }) => assert_eq!(missing, vec![r(&commit)]),
        other => panic!("expected MissingObjects, got {other:?}"),
    }
}

/// Trait: "Reusing that request_id with a different new_head is InvalidArgument".
pub async fn reused_request_id_for_another_head_is_rejected(store: Store) {
    let ws = workspace(&store).await;
    let base = ws.record.head.head.clone();
    let first = new_operation(&store, &ws, &base, &unique("a")).await;
    let req = advance(&ws, &base, &first);
    store.advance_head(req.clone()).await.unwrap();
    let second = new_operation(&store, &ws, &first, &unique("b")).await;
    let mut reused = advance(&ws, &first, &second);
    reused.request_id = req.request_id;
    match store.advance_head(reused).await {
        Err(AdvanceHeadError::Store(e)) if e.kind == ErrorKind::InvalidArgument => {}
        other => panic!("expected InvalidArgument, got {other:?}"),
    }
}

/// Trait: after another writer advanced, the retry gets HeadMoved and finds
/// its own write in read_events.
pub async fn retry_after_another_writer_finds_its_event(store: Store) {
    let ws = workspace(&store).await;
    let base = ws.record.head.head.clone();
    let mine = new_operation(&store, &ws, &base, &unique("mine")).await;
    let req = advance(&ws, &base, &mine);
    store.advance_head(req.clone()).await.unwrap();
    let theirs = new_operation(&store, &ws, &mine, &unique("theirs")).await;
    store
        .advance_head(advance(&ws, &mine, &theirs))
        .await
        .unwrap();
    match store.advance_head(req.clone()).await {
        Err(AdvanceHeadError::HeadMoved { current }) => assert_eq!(current.sequence, 2),
        other => panic!("expected HeadMoved, got {other:?}"),
    }
    let events = store.read_events(&ws.record.id, 0, 10).await.unwrap();
    assert!(
        events
            .iter()
            .any(|e| e.request_id == req.request_id && e.state.head == mine),
        "the earlier write is visible in the change feed"
    );
}

/// Trait: `InvalidArgument` (`limit` is 0).
pub async fn read_events_with_zero_limit_is_invalid(store: Store) {
    let ws = workspace(&store).await;
    let e = store
        .read_events(&ws.record.id, 0, 0)
        .await
        .expect_err("limit 0");
    assert_eq!(e.kind, ErrorKind::InvalidArgument);
}

/// Trait: virtual roots are present for the closure check and find_missing.
/// Skipped (passes trivially) if the store has no virtual roots configured.
pub async fn virtual_roots_count_as_present(store: Store) {
    let roots = store.virtual_roots();
    let Some(root_op) = roots
        .iter()
        .find(|r| r.kind == ObjectKind::Operation)
        .cloned()
    else {
        return;
    };
    let project = ProjectId(unique("proj"));
    store.create_project(&project).await.unwrap();
    let name = WorkspaceName(format!("tester/{}", unique("ws")));
    let rec = store
        .create_workspace(&project, &name, OperationId(root_op.id.clone()))
        .await
        .unwrap();
    let ws = Ws {
        project: Scope::Project(project),
        workspace: Scope::Workspace(rec.id.clone()),
        record: rec,
    };
    let project_roots: Vec<ObjectRef> = roots
        .iter()
        .filter(|r| r.kind.is_project_scoped())
        .cloned()
        .collect();
    let commit = record(ObjectKind::Commit, &unique("c"), project_roots.clone());
    let view = record(ObjectKind::View, &unique("v"), vec![r(&commit)]);
    let op = record(
        ObjectKind::Operation,
        &unique("op"),
        vec![r(&view), root_op.clone()],
    );
    store.put_objects(&ws.project, vec![commit]).await.unwrap();
    store
        .put_objects(&ws.workspace, vec![view, op.clone()])
        .await
        .unwrap();
    let state = store
        .advance_head(advance(&ws, &OperationId(root_op.id.clone()), &op_id(&op)))
        .await
        .expect("history ending at virtual roots is complete");
    assert_eq!(state.sequence, 1);
    assert!(
        store
            .find_missing(&ws.project, &project_roots)
            .await
            .unwrap()
            .is_empty()
    );
}

/// Trait: closure marking is sound. An object whose check failed must not be
/// treated as verified by a later advance that reaches it again.
pub async fn failed_check_does_not_hide_missing_objects(store: Store) {
    let ws = workspace(&store).await;
    let base = ws.record.head.head.clone();
    let absent = record(ObjectKind::Commit, &unique("absent"), vec![]);
    let view = record(ObjectKind::View, &unique("v"), vec![r(&absent)]);
    let op1 = record(ObjectKind::Operation, &unique("op1"), vec![r(&view)]);
    let op2 = record(ObjectKind::Operation, &unique("op2"), vec![r(&view)]);
    store
        .put_objects(&ws.workspace, vec![view, op1.clone(), op2.clone()])
        .await
        .unwrap();
    for op in [&op1, &op2] {
        match store.advance_head(advance(&ws, &base, &op_id(op))).await {
            Err(AdvanceHeadError::MissingObjects { missing }) => {
                assert_eq!(missing, vec![r(&absent)])
            }
            other => panic!("expected MissingObjects, got {other:?}"),
        }
    }
}

/// A head is a digest too: `create_workspace` and `advance_head` refuse one
/// that is not as long as its algorithm's output, and nothing moves.
pub async fn heads_of_the_wrong_length_are_refused(store: Store) {
    let ws = workspace(&store).await;
    let mut long = ws.record.head.head.clone();
    long.0.bytes.resize(300, 0);

    let project = ProjectId(unique("proj"));
    store.create_project(&project).await.unwrap();
    let name = WorkspaceName(format!("tester/{}", unique("ws")));
    let e = store
        .create_workspace(&project, &name, long.clone())
        .await
        .expect_err("create_workspace");
    assert_eq!(e.kind, ErrorKind::InvalidArgument);
    assert_eq!(store.get_workspace_by_name(&name).await.unwrap(), None);

    let e = store
        .advance_head(AdvanceHead {
            workspace: ws.record.id.clone(),
            expected: ws.record.head.head.clone(),
            new_head: long,
            mode: AdvanceMode::Reject,
            request_id: req_id(),
        })
        .await
        .expect_err("advance_head");
    assert!(
        matches!(&e, AdvanceHeadError::Store(s) if s.kind == ErrorKind::InvalidArgument),
        "{e:?}"
    );
    let now = store.get_workspace(&ws.record.id).await.unwrap().unwrap();
    assert_eq!(now.head, ws.record.head, "head unchanged");
}

/// `read_events` takes any `u64` cursor and `usize` limit: a cursor past the
/// last event returns nothing, and a huge limit returns every event.
pub async fn read_events_takes_any_cursor_and_limit(store: Store) {
    let ws = workspace(&store).await;
    let op = record(ObjectKind::Operation, &unique("op"), vec![]);
    store
        .put_objects(&ws.workspace, vec![op.clone()])
        .await
        .unwrap();
    store
        .advance_head(AdvanceHead {
            workspace: ws.record.id.clone(),
            expected: ws.record.head.head.clone(),
            new_head: op_id(&op),
            mode: AdvanceMode::Reject,
            request_id: req_id(),
        })
        .await
        .unwrap();
    let id = &ws.record.id;
    let past = store.read_events(id, u64::MAX, 10).await.unwrap();
    assert!(past.is_empty(), "cursor u64::MAX: {past:?}");
    let all = store.read_events(id, 0, usize::MAX).await.unwrap();
    assert_eq!(all.len(), 1, "limit usize::MAX");
}
