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

use tonic::Request;

use super::*;
use crate::hash_utils::serialize_canonical_git_commit;

fn make_service() -> CommitCloudBackendService {
    CommitCloudBackendService::new(Arc::new(CompositeStore::in_memory()))
}

fn base_commit() -> Commit {
    Commit {
        commit_id: vec![],
        change_id: vec![0x42; 16],
        parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
        description: "honest commit\n".to_string(),
        author: Some(Signature {
            name: "Alice".to_string(),
            email: "alice@example.com".to_string(),
            timestamp: Some(Timestamp {
                millis_since_epoch: 1_730_000_000_000,
                tz_offset: 0,
            }),
        }),
        committer: Some(Signature {
            name: "Alice".to_string(),
            email: "alice@example.com".to_string(),
            timestamp: Some(Timestamp {
                millis_since_epoch: 1_730_000_000_000,
                tz_offset: 0,
            }),
        }),
        predecessors: vec![],
        conflict_labels: vec![],
        secure_sig: None,
        raw_git_commit: vec![],
        external_ids: Default::default(),
    }
}

#[tokio::test]
async fn test_write_tree_and_write_commit_reject_malformed_inputs_with_invalid_argument() {
    let svc = make_service();
    svc.register_repository(Request::new(RegisterRepositoryRequest {
        project_id: "repo-1".to_string(),
        repo_id: Some("repo-1".to_string()),
        name: None,
        hash_algorithm: HashAlgorithm::Sha1 as i32,
    }))
    .await
    .unwrap();

    let file_id = svc
        .write_file(Request::new(WriteFileRequest {
            project_id: "repo-1".to_string(),
            content: b"hello\n".to_vec(),
        }))
        .await
        .unwrap()
        .into_inner()
        .file_id;

    let valid_entry = TreeEntry {
        name: "hello.txt".to_string(),
        value: Some(TreeValue {
            value: Some(tree_value::Value::File(File {
                id: file_id,
                executable: false,
                copy_id: vec![],
                size_bytes: None,
            })),
        }),
    };
    let tree_id = svc
        .write_tree(Request::new(WriteTreeRequest {
            project_id: "repo-1".to_string(),
            path: String::new(),
            entries: vec![valid_entry.clone()],
        }))
        .await
        .unwrap()
        .into_inner()
        .tree_id;

    // Trying to inject a zero-byte submodule entry alongside `valid_entry` is rejected
    // with INVALID_ARGUMENT rather than dropped from the hash and persisted under `tree_id`.
    let err = svc
        .write_tree(Request::new(WriteTreeRequest {
            project_id: "repo-1".to_string(),
            path: String::new(),
            entries: vec![
                valid_entry,
                TreeEntry {
                    name: "phantom".to_string(),
                    value: Some(TreeValue {
                        value: Some(tree_value::Value::GitSubmoduleId(vec![])),
                    }),
                },
            ],
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    let stored_entries = svc
        .read_tree(Request::new(ReadTreeRequest {
            project_id: "repo-1".to_string(),
            path: String::new(),
            tree_id,
            page_size: 0,
            page_token: String::new(),
        }))
        .await
        .unwrap()
        .into_inner()
        .entries;
    assert_eq!(stored_entries.len(), 1);
    assert_eq!(stored_entries[0].name, "hello.txt");

    // Malformed commits (empty root_tree_id, NUL/angle-bracket in author) return
    // INVALID_ARGUMENT instead of panicking or poisoning repository readers.
    let mut empty_trees = base_commit();
    empty_trees.root_tree_id.clear();
    let err = svc
        .write_commit(Request::new(WriteCommitRequest {
            project_id: "repo-1".to_string(),
            commit: Some(empty_trees),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    let mut bad_author = base_commit();
    bad_author.author.as_mut().unwrap().name = "Bad <Actor>".to_string();
    let err = svc
        .write_commit(Request::new(WriteCommitRequest {
            project_id: "repo-1".to_string(),
            commit: Some(bad_author),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_write_commit_blocks_signature_spoofing_and_raw_git_commit_forging() {
    let svc = make_service();
    svc.register_repository(Request::new(RegisterRepositoryRequest {
        project_id: "repo-sig".to_string(),
        repo_id: Some("repo-sig".to_string()),
        name: None,
        hash_algorithm: HashAlgorithm::Sha1 as i32,
    }))
    .await
    .unwrap();

    let mut commit_a = base_commit();
    let data_a = serialize_canonical_git_commit(&commit_a, false).unwrap();
    let sig_a = b"-----BEGIN SSH SIGNATURE-----\nsigA\n-----END SSH SIGNATURE-----\n".to_vec();
    commit_a.secure_sig = Some(SecureSig {
        data: data_a.clone(),
        sig: sig_a.clone(),
    });

    let id_a = svc
        .write_commit(Request::new(WriteCommitRequest {
            project_id: "repo-sig".to_string(),
            commit: Some(commit_a),
        }))
        .await
        .unwrap()
        .into_inner()
        .commit_id;
    let read_a = svc
        .read_commit(Request::new(ReadCommitRequest {
            project_id: "repo-sig".to_string(),
            commit_id: id_a,
        }))
        .await
        .unwrap()
        .into_inner()
        .commit
        .unwrap();

    // Replaying commit A's `(data_A, sig_A)` onto commit M is rejected.
    let mut commit_m = base_commit();
    commit_m.description = "tampered commit\n".to_string();
    commit_m.secure_sig = read_a.secure_sig.clone();
    let err = svc
        .write_commit(Request::new(WriteCommitRequest {
            project_id: "repo-sig".to_string(),
            commit: Some(commit_m.clone()),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    // Sending `raw_git_commit` bytes from commit A with commit M's description is rejected.
    let raw_a = serialize_canonical_git_commit(&read_a, true).unwrap();
    commit_m.raw_git_commit = raw_a;
    commit_m.commit_id.clear();
    let err = svc
        .write_commit(Request::new(WriteCommitRequest {
            project_id: "repo-sig".to_string(),
            commit: Some(commit_m),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_shared_project_object_rpcs_accept_both_project_id_and_repo_id() {
    let svc = make_service();
    svc.register_project(Request::new(RegisterProjectRequest {
        project_id: Some("shared-proj".to_string()),
        name: Some("Shared Project".to_string()),
    }))
    .await
    .unwrap();
    svc.register_repository(Request::new(RegisterRepositoryRequest {
        project_id: "shared-proj".to_string(),
        repo_id: Some("actor-1".to_string()),
        name: None,
        hash_algorithm: HashAlgorithm::Sha1 as i32,
    }))
    .await
    .unwrap();

    // Write commit addressing `shared-proj` directly (where no repo named `shared-proj` exists).
    let cid = svc
        .write_commit(Request::new(WriteCommitRequest {
            project_id: "shared-proj".to_string(),
            commit: Some(base_commit()),
        }))
        .await
        .unwrap()
        .into_inner()
        .commit_id;

    let by_proj = svc
        .read_commit(Request::new(ReadCommitRequest {
            project_id: "shared-proj".to_string(),
            commit_id: cid.clone(),
        }))
        .await
        .unwrap()
        .into_inner()
        .commit
        .unwrap();
    let by_repo = svc
        .read_commit(Request::new(ReadCommitRequest {
            project_id: "actor-1".to_string(),
            commit_id: cid.clone(),
        }))
        .await
        .unwrap()
        .into_inner()
        .commit
        .unwrap();
    assert_eq!(by_proj.commit_id, by_repo.commit_id);

    let listed_proj = svc
        .list_commits(Request::new(ListCommitsRequest {
            project_id: "shared-proj".to_string(),
            page_token: Vec::new(),
            page_size: 100,
            since_token: Vec::new(),
        }))
        .await
        .unwrap()
        .into_inner()
        .commit_ids;
    let listed_repo = svc
        .list_commits(Request::new(ListCommitsRequest {
            project_id: "actor-1".to_string(),
            page_token: Vec::new(),
            page_size: 100,
            since_token: Vec::new(),
        }))
        .await
        .unwrap()
        .into_inner()
        .commit_ids;
    assert_eq!(listed_proj, vec![cid.clone()]);
    assert_eq!(listed_repo, vec![cid]);
}

#[tokio::test]
async fn test_backend_validates_identifiers_digests_copy_id_and_symlink_nul() {
    let svc = make_service();
    svc.register_project(Request::new(RegisterProjectRequest {
        project_id: Some("valid-proj".to_string()),
        name: None,
    }))
    .await
    .unwrap();

    // Whitespace-padded and malformed identifiers are rejected rather than trimmed.
    for bad in ["", " valid-proj ", "../escape", "bad/id"] {
        let err = svc
            .register_project(Request::new(RegisterProjectRequest {
                project_id: Some(bad.to_string()),
                name: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "bad={bad:?}");
    }

    // Digest length validation returns INVALID_ARGUMENT before storage lookup.
    let bad_len = svc
        .read_commit(Request::new(ReadCommitRequest {
            project_id: "valid-proj".to_string(),
            commit_id: vec![0x01; 5],
        }))
        .await
        .unwrap_err();
    assert_eq!(bad_len.code(), tonic::Code::InvalidArgument);

    // Symlink NUL bytes are rejected on write and treated as DATA_LOSS on read.
    let nul_write = svc
        .write_symlink(Request::new(WriteSymlinkRequest {
            project_id: "valid-proj".to_string(),
            target: "foo\0bar".to_string(),
        }))
        .await
        .unwrap_err();
    assert_eq!(nul_write.code(), tonic::Code::InvalidArgument);

    let corrupted_id = vec![0x99; 20];
    svc.store
        .metadata()
        .put_symlink("valid-proj", &corrupted_id, "bad\0target")
        .await
        .unwrap();
    let nul_read = svc
        .read_symlink(Request::new(ReadSymlinkRequest {
            project_id: "valid-proj".to_string(),
            symlink_id: corrupted_id,
        }))
        .await
        .unwrap_err();
    assert_eq!(nul_read.code(), tonic::Code::DataLoss);

    // Non-empty File.copy_id is rejected on WriteTree.
    let blob_id = svc
        .write_file(Request::new(WriteFileRequest {
            project_id: "valid-proj".to_string(),
            content: b"hello".to_vec(),
        }))
        .await
        .unwrap()
        .into_inner()
        .file_id;
    let copy_err = svc
        .write_tree(Request::new(WriteTreeRequest {
            project_id: "valid-proj".to_string(),
            path: String::new(),
            entries: vec![TreeEntry {
                name: "a.txt".to_string(),
                value: Some(TreeValue {
                    value: Some(tree_value::Value::File(File {
                        id: blob_id,
                        executable: false,
                        copy_id: vec![0x01],
                        size_bytes: None,
                    })),
                }),
            }],
        }))
        .await
        .unwrap_err();
    assert_eq!(copy_err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_write_and_read_symlink_roundtrip_nul_rejection_and_not_found() {
    let store = Arc::new(CompositeStore::in_memory());
    let svc = CommitCloudBackendService::new(store.clone());
    svc.register_project(tonic::Request::new(RegisterProjectRequest {
        project_id: Some("proj-sym".to_string()),
        name: None,
    }))
    .await
    .unwrap();

    let target = "../shared/target.txt";
    let symlink_id = svc
        .write_symlink(tonic::Request::new(WriteSymlinkRequest {
            project_id: "proj-sym".to_string(),
            target: target.to_string(),
        }))
        .await
        .unwrap()
        .into_inner()
        .symlink_id;
    assert_eq!(
        symlink_id,
        try_compute_git_blob_hash(target.as_bytes()).unwrap()
    );

    let read_resp = svc
        .read_symlink(tonic::Request::new(ReadSymlinkRequest {
            project_id: "proj-sym".to_string(),
            symlink_id,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(read_resp.target, target);

    // NUL bytes in target rejected by WriteSymlink.
    let nul_err = svc
        .write_symlink(tonic::Request::new(WriteSymlinkRequest {
            project_id: "proj-sym".to_string(),
            target: "bad\0target".to_string(),
        }))
        .await
        .unwrap_err();
    assert_eq!(nul_err.code(), tonic::Code::InvalidArgument);

    // Absent symlink_id returns NOT_FOUND.
    let missing_err = svc
        .read_symlink(tonic::Request::new(ReadSymlinkRequest {
            project_id: "proj-sym".to_string(),
            symlink_id: vec![0x99; cc_common::SYMLINK_ID_LENGTH],
        }))
        .await
        .unwrap_err();
    assert_eq!(missing_err.code(), tonic::Code::NotFound);

    // NUL-bearing persisted symlink target returns DATA_LOSS.
    let bad_target_id = vec![0x9a; cc_common::SYMLINK_ID_LENGTH];
    store
        .metadata()
        .put_symlink("proj-sym", &bad_target_id, "corrupt\0link")
        .await
        .unwrap();
    let data_loss_err = svc
        .read_symlink(tonic::Request::new(ReadSymlinkRequest {
            project_id: "proj-sym".to_string(),
            symlink_id: bad_target_id,
        }))
        .await
        .unwrap_err();
    assert_eq!(data_loss_err.code(), tonic::Code::DataLoss);
}

#[tokio::test]
async fn test_read_commit_synthesizes_root_commit_on_registered_project_only() {
    let svc = make_service();
    let missing = svc
        .read_commit(Request::new(ReadCommitRequest {
            project_id: "unregistered-proj".to_string(),
            commit_id: cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
        }))
        .await
        .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::NotFound);

    svc.register_project(Request::new(RegisterProjectRequest {
        project_id: Some("root-proj".to_string()),
        name: None,
    }))
    .await
    .unwrap();

    let root = svc
        .read_commit(Request::new(ReadCommitRequest {
            project_id: "root-proj".to_string(),
            commit_id: cc_common::ROOT_COMMIT_ID_BYTES.to_vec(),
        }))
        .await
        .unwrap()
        .into_inner()
        .commit
        .expect("synthesized root commit must be present");
    assert_eq!(root.commit_id, cc_common::ROOT_COMMIT_ID_BYTES);
    assert_eq!(root.change_id, cc_common::ROOT_CHANGE_ID_BYTES);
    assert!(root.parent_commit_ids.is_empty());
    assert_eq!(
        root.root_tree_id,
        vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()]
    );
}

#[tokio::test]
async fn test_register_repository_conflicting_rebinding_returns_already_exists_without_leaking_project()
 {
    let mem_store = Arc::new(CompositeStore::in_memory());
    let dir = tempfile::tempdir().unwrap();
    let sqlite_store = Arc::new(CompositeStore::new(
        Arc::new(crate::store::SqliteMetadataStore::open(&dir.path().join("reg.db")).unwrap()),
        Arc::new(crate::store::MemoryBlobStore::new()),
    ));

    for store in [mem_store, sqlite_store] {
        let svc = CommitCloudBackendService::new(store.clone());
        svc.register_repository(Request::new(RegisterRepositoryRequest {
            project_id: "proj-a".to_string(),
            repo_id: Some("shared-repo".to_string()),
            name: None,
            hash_algorithm: HashAlgorithm::Sha1 as i32,
        }))
        .await
        .unwrap();

        // Idempotent re-registration in the same project succeeds.
        svc.register_repository(Request::new(RegisterRepositoryRequest {
            project_id: "proj-a".to_string(),
            repo_id: Some("shared-repo".to_string()),
            name: None,
            hash_algorithm: HashAlgorithm::Sha1 as i32,
        }))
        .await
        .unwrap();

        // Re-binding to a different project fails with ALREADY_EXISTS and never leaks `proj-b`.
        let err = svc
            .register_repository(Request::new(RegisterRepositoryRequest {
                project_id: "proj-b".to_string(),
                repo_id: Some("shared-repo".to_string()),
                name: None,
                hash_algorithm: HashAlgorithm::Sha1 as i32,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::AlreadyExists);
        assert!(
            !store
                .metadata()
                .is_project_registered("proj-b")
                .await
                .unwrap(),
            "rejected RegisterRepository must not leak phantom project 'proj-b'"
        );
    }
}

#[tokio::test]
async fn test_read_file_empty_and_end_offset_returns_zero_frames_and_past_eof_returns_out_of_range()
{
    let svc = make_service();
    svc.register_project(Request::new(RegisterProjectRequest {
        project_id: Some("proj-range".to_string()),
        name: None,
    }))
    .await
    .unwrap();

    // Empty file returns a 0-frame stream at offset 0 and OUT_OF_RANGE at offset 1.
    let empty_id = svc
        .write_file(Request::new(WriteFileRequest {
            project_id: "proj-range".to_string(),
            content: Vec::new(),
        }))
        .await
        .unwrap()
        .into_inner()
        .file_id;
    let mut empty_stream = svc
        .read_file(Request::new(ReadFileRequest {
            project_id: "proj-range".to_string(),
            file_id: empty_id.clone(),
            offset: 0,
            max_bytes: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(
        empty_stream.next().await.is_none(),
        "empty file read must emit 0 frames"
    );
    let past_empty = svc
        .read_file(Request::new(ReadFileRequest {
            project_id: "proj-range".to_string(),
            file_id: empty_id,
            offset: 1,
            max_bytes: 0,
        }))
        .await
        .unwrap_err();
    assert_eq!(past_empty.code(), tonic::Code::OutOfRange);

    // Non-empty file at offset == len returns 0 frames; offset == len + 1 returns OUT_OF_RANGE.
    let file_id = svc
        .write_file(Request::new(WriteFileRequest {
            project_id: "proj-range".to_string(),
            content: b"abcdef".to_vec(),
        }))
        .await
        .unwrap()
        .into_inner()
        .file_id;
    let mut eof_stream = svc
        .read_file(Request::new(ReadFileRequest {
            project_id: "proj-range".to_string(),
            file_id: file_id.clone(),
            offset: 6,
            max_bytes: 10,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(
        eof_stream.next().await.is_none(),
        "ReadFile at offset == total_len must emit 0 frames"
    );
    let past_eof = svc
        .read_file(Request::new(ReadFileRequest {
            project_id: "proj-range".to_string(),
            file_id,
            offset: 7,
            max_bytes: 0,
        }))
        .await
        .unwrap_err();
    assert_eq!(past_eof.code(), tonic::Code::OutOfRange);
}

#[tokio::test]
async fn test_resolve_change_prefix_honors_max_results_and_truncated_flag() {
    let svc = make_service();
    svc.register_project(Request::new(RegisterProjectRequest {
        project_id: Some("proj-change".to_string()),
        name: None,
    }))
    .await
    .unwrap();

    let shared_change = vec![0xab; 16];
    for i in 0..3u8 {
        let mut c = base_commit();
        c.change_id = shared_change.clone();
        c.description = format!("variant {i}\n");
        svc.write_commit(Request::new(WriteCommitRequest {
            project_id: "proj-change".to_string(),
            commit: Some(c),
        }))
        .await
        .unwrap();
    }

    let capped = svc
        .resolve_change_prefix(Request::new(ResolveChangePrefixRequest {
            project_id: "proj-change".to_string(),
            prefix: "abab".to_string(),
            repo_id: None,
            visible_head_ids: Vec::new(),
            max_results: 2,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        capped.status,
        ObjectPrefixStatus::ObjectPrefixSingleMatch as i32
    );
    assert_eq!(capped.matched_change_id, shared_change);
    assert_eq!(capped.commit_ids.len(), 2);
    assert!(capped.truncated);

    let full = svc
        .resolve_change_prefix(Request::new(ResolveChangePrefixRequest {
            project_id: "proj-change".to_string(),
            prefix: "abab".to_string(),
            repo_id: None,
            visible_head_ids: Vec::new(),
            max_results: 3,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(full.commit_ids.len(), 3);
    assert!(!full.truncated);
}

#[tokio::test]
async fn test_register_project_and_repository_validate_optional_name_identically() {
    let svc = make_service();
    let max_name = "é".repeat(MAX_DISPLAY_NAME_LEN);
    let too_long_name = "é".repeat(MAX_DISPLAY_NAME_LEN + 1);

    // Blank, trimmed, and exact max-length Unicode names succeed on both RPCs.
    for (idx, ok_name) in [Some("   "), Some("  Demo Name  "), Some(max_name.as_str())]
        .into_iter()
        .enumerate()
    {
        let pid = format!("ok-proj-{idx}");
        let rid = format!("ok-repo-{idx}");
        svc.register_project(Request::new(RegisterProjectRequest {
            project_id: Some(pid.clone()),
            name: ok_name.map(ToOwned::to_owned),
        }))
        .await
        .unwrap();
        svc.register_repository(Request::new(RegisterRepositoryRequest {
            project_id: pid,
            repo_id: Some(rid),
            name: ok_name.map(ToOwned::to_owned),
            hash_algorithm: HashAlgorithm::Sha1 as i32,
        }))
        .await
        .unwrap();
    }

    // Control characters and names exceeding 256 Unicode chars fail with INVALID_ARGUMENT.
    for bad_name in [
        "line1\nline2",
        "nul\0byte",
        "bell\x07",
        too_long_name.as_str(),
    ] {
        let proj_err = svc
            .register_project(Request::new(RegisterProjectRequest {
                project_id: Some("bad-name-proj".to_string()),
                name: Some(bad_name.to_string()),
            }))
            .await
            .unwrap_err();
        assert_eq!(
            proj_err.code(),
            tonic::Code::InvalidArgument,
            "register_project bad_name={bad_name:?}"
        );

        let repo_err = svc
            .register_repository(Request::new(RegisterRepositoryRequest {
                project_id: "ok-proj-0".to_string(),
                repo_id: Some("bad-name-repo".to_string()),
                name: Some(bad_name.to_string()),
                hash_algorithm: HashAlgorithm::Sha1 as i32,
            }))
            .await
            .unwrap_err();
        assert_eq!(
            repo_err.code(),
            tonic::Code::InvalidArgument,
            "register_repository bad_name={bad_name:?}"
        );
    }
}

async fn spawn_backend_client(
    svc: CommitCloudBackendService,
) -> (
    cc_common::backend::backend_service_client::BackendServiceClient<tonic::transport::Channel>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(cc_common::backend::backend_service_server::BackendServiceServer::new(svc))
            .serve_with_incoming(incoming)
            .await;
    });
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    (
        cc_common::backend::backend_service_client::BackendServiceClient::new(channel),
        handle,
    )
}

#[tokio::test]
async fn test_write_file_stream_uploads_validation_and_empty_chunk_guards() {
    let svc = make_service();
    svc.register_project(Request::new(RegisterProjectRequest {
        project_id: Some("proj-stream".to_string()),
        name: None,
    }))
    .await
    .unwrap();
    let (mut client, server_handle) = spawn_backend_client(svc.clone()).await;

    let full_payload = b"hello streaming world!";
    let expected_hash = try_compute_git_blob_hash(full_payload).unwrap();

    // Multi-chunk upload with matching total_size and expected_file_id succeeds.
    let ok_resp = client
        .write_file_stream(tokio_stream::iter(vec![
            WriteFileChunk {
                project_id: "proj-stream".to_string(),
                chunk: b"hello ".to_vec(),
                total_size: Some(full_payload.len() as u64),
                expected_file_id: Some(expected_hash.clone()),
            },
            WriteFileChunk {
                project_id: String::new(),
                chunk: b"streaming ".to_vec(),
                total_size: None,
                expected_file_id: None,
            },
            WriteFileChunk {
                project_id: "proj-stream".to_string(),
                chunk: b"world!".to_vec(),
                total_size: None,
                expected_file_id: None,
            },
        ]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ok_resp.file_id, expected_hash);
    let stored = svc
        .store
        .get_file_range("proj-stream", &expected_hash, 0, None)
        .await
        .unwrap();
    assert_eq!(stored.as_ref(), full_payload);

    // Multi-chunk upload without total_size also succeeds.
    let no_size_payload = b"chunked-without-size";
    let no_size_hash = try_compute_git_blob_hash(no_size_payload).unwrap();
    let no_size_resp = client
        .write_file_stream(tokio_stream::iter(vec![
            WriteFileChunk {
                project_id: "proj-stream".to_string(),
                chunk: b"chunked-".to_vec(),
                total_size: None,
                expected_file_id: None,
            },
            WriteFileChunk {
                project_id: String::new(),
                chunk: b"without-size".to_vec(),
                total_size: None,
                expected_file_id: None,
            },
        ]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(no_size_resp.file_id, no_size_hash);

    // Empty-file 1-frame stream succeeds; 0-frame stream returns INVALID_ARGUMENT.
    let empty_hash = try_compute_git_blob_hash(b"").unwrap();
    let empty_resp = client
        .write_file_stream(tokio_stream::iter(vec![WriteFileChunk {
            project_id: "proj-stream".to_string(),
            chunk: Vec::new(),
            total_size: Some(0),
            expected_file_id: Some(empty_hash.clone()),
        }]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(empty_resp.file_id, empty_hash);

    let zero_frame_err = client
        .write_file_stream(tokio_stream::iter(Vec::<WriteFileChunk>::new()))
        .await
        .unwrap_err();
    assert_eq!(zero_frame_err.code(), tonic::Code::InvalidArgument);

    // total_size truncation and overflow are rejected.
    let truncated_err = client
        .write_file_stream(tokio_stream::iter(vec![WriteFileChunk {
            project_id: "proj-stream".to_string(),
            chunk: b"short".to_vec(),
            total_size: Some(10),
            expected_file_id: None,
        }]))
        .await
        .unwrap_err();
    assert_eq!(truncated_err.code(), tonic::Code::InvalidArgument);

    let overflow_err = client
        .write_file_stream(tokio_stream::iter(vec![
            WriteFileChunk {
                project_id: "proj-stream".to_string(),
                chunk: b"12345".to_vec(),
                total_size: Some(6),
                expected_file_id: None,
            },
            WriteFileChunk {
                project_id: String::new(),
                chunk: b"67".to_vec(),
                total_size: None,
                expected_file_id: None,
            },
        ]))
        .await
        .unwrap_err();
    assert_eq!(overflow_err.code(), tonic::Code::InvalidArgument);

    // expected_file_id mismatch and wrong length are rejected.
    let wrong_digest_err = client
        .write_file_stream(tokio_stream::iter(vec![WriteFileChunk {
            project_id: "proj-stream".to_string(),
            chunk: b"actual".to_vec(),
            total_size: Some(6),
            expected_file_id: Some(vec![0xaa; cc_common::FILE_ID_LENGTH]),
        }]))
        .await
        .unwrap_err();
    assert_eq!(wrong_digest_err.code(), tonic::Code::InvalidArgument);

    let wrong_len_err = client
        .write_file_stream(tokio_stream::iter(vec![WriteFileChunk {
            project_id: "proj-stream".to_string(),
            chunk: b"actual".to_vec(),
            total_size: None,
            expected_file_id: Some(vec![0xaa; 4]),
        }]))
        .await
        .unwrap_err();
    assert_eq!(wrong_len_err.code(), tonic::Code::InvalidArgument);

    // Mid-stream metadata changes and trailing empty chunks are rejected.
    let bad_mid_stream_cases = [
        vec![
            WriteFileChunk {
                project_id: "proj-stream".to_string(),
                chunk: b"part1".to_vec(),
                total_size: None,
                expected_file_id: None,
            },
            WriteFileChunk {
                project_id: "other-proj".to_string(),
                chunk: b"part2".to_vec(),
                total_size: None,
                expected_file_id: None,
            },
        ],
        vec![
            WriteFileChunk {
                project_id: "proj-stream".to_string(),
                chunk: b"part1".to_vec(),
                total_size: None,
                expected_file_id: None,
            },
            WriteFileChunk {
                project_id: String::new(),
                chunk: b"part2".to_vec(),
                total_size: Some(10),
                expected_file_id: None,
            },
        ],
        vec![
            WriteFileChunk {
                project_id: "proj-stream".to_string(),
                chunk: b"part1".to_vec(),
                total_size: None,
                expected_file_id: None,
            },
            WriteFileChunk {
                project_id: String::new(),
                chunk: b"part2".to_vec(),
                total_size: None,
                expected_file_id: Some(vec![0x11; cc_common::FILE_ID_LENGTH]),
            },
        ],
        vec![
            WriteFileChunk {
                project_id: "proj-stream".to_string(),
                chunk: b"part1".to_vec(),
                total_size: None,
                expected_file_id: None,
            },
            WriteFileChunk {
                project_id: String::new(),
                chunk: Vec::new(),
                total_size: None,
                expected_file_id: None,
            },
        ],
    ];
    for (idx, chunks) in bad_mid_stream_cases.into_iter().enumerate() {
        let err = client
            .write_file_stream(tokio_stream::iter(chunks))
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            tonic::Code::InvalidArgument,
            "mid-stream case {idx} expected INVALID_ARGUMENT, got {err:?}"
        );
    }

    server_handle.abort();
}

#[tokio::test]
async fn test_resolve_commit_prefix_rpc_outcomes_and_validation() {
    let svc = make_service();
    svc.register_project(Request::new(RegisterProjectRequest {
        project_id: Some("proj-cid-prefix".to_string()),
        name: None,
    }))
    .await
    .unwrap();

    let mut c1 = vec![0x00; cc_common::COMMIT_ID_LENGTH];
    c1[0] = 0xab;
    c1[1] = 0x10;
    let mut c2 = vec![0x00; cc_common::COMMIT_ID_LENGTH];
    c2[0] = 0xab;
    c2[1] = 0x20;
    for cid in [&c1, &c2] {
        let mut commit = base_commit();
        commit.commit_id = cid.clone();
        svc.store
            .put_commit("proj-cid-prefix", cid, &commit)
            .await
            .unwrap();
    }

    // NO_MATCH
    let no_match = svc
        .resolve_commit_prefix(Request::new(ResolveCommitPrefixRequest {
            project_id: "proj-cid-prefix".to_string(),
            hex_prefix: "ff00".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        no_match.status,
        ObjectPrefixStatus::ObjectPrefixNoMatch as i32
    );
    assert!(no_match.matched_commit_id.is_empty());

    // SINGLE_MATCH (including odd-length uppercase prefix)
    let single = svc
        .resolve_commit_prefix(Request::new(ResolveCommitPrefixRequest {
            project_id: "proj-cid-prefix".to_string(),
            hex_prefix: "AB1".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        single.status,
        ObjectPrefixStatus::ObjectPrefixSingleMatch as i32
    );
    assert_eq!(single.matched_commit_id, c1);

    // AMBIGUOUS_MATCH
    let ambiguous = svc
        .resolve_commit_prefix(Request::new(ResolveCommitPrefixRequest {
            project_id: "proj-cid-prefix".to_string(),
            hex_prefix: "ab".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        ambiguous.status,
        ObjectPrefixStatus::ObjectPrefixAmbiguousMatch as i32
    );
    assert!(ambiguous.matched_commit_id.is_empty());

    // Virtual root commit match without a persisted root row
    let root_match = svc
        .resolve_commit_prefix(Request::new(ResolveCommitPrefixRequest {
            project_id: "proj-cid-prefix".to_string(),
            hex_prefix: "0000".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        root_match.status,
        ObjectPrefixStatus::ObjectPrefixSingleMatch as i32
    );
    assert_eq!(
        root_match.matched_commit_id,
        cc_common::ROOT_COMMIT_ID_BYTES
    );

    // Malformed hex -> INVALID_ARGUMENT
    let bad_hex = svc
        .resolve_commit_prefix(Request::new(ResolveCommitPrefixRequest {
            project_id: "proj-cid-prefix".to_string(),
            hex_prefix: "not-hex!".to_string(),
        }))
        .await
        .unwrap_err();
    assert_eq!(bad_hex.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_write_file_stream_spill_to_disk_and_read_file_streaming_and_truncation() {
    let dir = tempfile::tempdir().unwrap();
    let meta =
        Arc::new(crate::store::SqliteMetadataStore::open(&dir.path().join("stream.db")).unwrap());
    let blobs = Arc::new(crate::store::LocalFsBlobStore::open(dir.path().join("blobs")).unwrap());
    let store = Arc::new(CompositeStore::new(meta, blobs).with_stream_memory_budget(64 * 1024));
    let svc = CommitCloudBackendService::new(store.clone());
    svc.register_project(Request::new(RegisterProjectRequest {
        project_id: Some("proj-big".to_string()),
        name: None,
    }))
    .await
    .unwrap();
    let (mut client, server_handle) = spawn_backend_client(svc.clone()).await;

    // >4 MiB blob uploaded with `total_size: Some(...)` spills to disk under the default
    // 4 MiB threshold, persists to LocalFsBlobStore, and streams back via ReadFile.
    let big_len = DEFAULT_WRITE_STREAM_SPILL_THRESHOLD_BYTES + 128 * 1024;
    let big_payload: Vec<u8> = (0..big_len).map(|i| ((i * 31) % 251) as u8).collect();
    let expected_big_id = try_compute_git_blob_hash(&big_payload).unwrap();
    let mut big_chunks = Vec::new();
    for (idx, slice) in big_payload
        .chunks(cc_common::FILE_STREAM_CHUNK_SIZE)
        .enumerate()
    {
        big_chunks.push(WriteFileChunk {
            project_id: if idx == 0 {
                "proj-big".to_string()
            } else {
                String::new()
            },
            chunk: slice.to_vec(),
            total_size: (idx == 0).then_some(big_len as u64),
            expected_file_id: (idx == 0).then(|| expected_big_id.clone()),
        });
    }
    let big_resp = client
        .write_file_stream(tokio_stream::iter(big_chunks))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(big_resp.file_id, expected_big_id);
    assert_eq!(store.stream_memory_budget().available_permits(), 64 * 1024);

    let mut read_stream = client
        .read_file(ReadFileRequest {
            project_id: "proj-big".to_string(),
            file_id: expected_big_id.clone(),
            offset: 0,
            max_bytes: 0,
        })
        .await
        .unwrap()
        .into_inner();
    let mut read_back = Vec::with_capacity(big_len);
    let mut frames = 0usize;
    while let Some(resp) = read_stream.next().await {
        let frame = resp.unwrap();
        assert!(frame.chunk.len() <= cc_common::FILE_STREAM_CHUNK_SIZE);
        read_back.extend_from_slice(&frame.chunk);
        frames += 1;
    }
    assert!(frames >= 66);
    assert_eq!(read_back, big_payload);

    // Spilled upload without `total_size` (spills both from the 64 KiB budget and
    // threshold) hashes from the temp file and round-trips via ReadFile range read.
    let unsized_payload: Vec<u8> = (0..(192 * 1024 + 33)).map(|i| (i % 199) as u8).collect();
    let expected_unsized_id = try_compute_git_blob_hash(&unsized_payload).unwrap();
    let unsized_chunks: Vec<WriteFileChunk> = unsized_payload
        .chunks(32 * 1024)
        .enumerate()
        .map(|(idx, slice)| WriteFileChunk {
            project_id: if idx == 0 {
                "proj-big".to_string()
            } else {
                String::new()
            },
            chunk: slice.to_vec(),
            total_size: None,
            expected_file_id: (idx == 0).then(|| expected_unsized_id.clone()),
        })
        .collect();
    let unsized_resp = client
        .write_file_stream(tokio_stream::iter(unsized_chunks))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(unsized_resp.file_id, expected_unsized_id);
    assert_eq!(store.stream_memory_budget().available_permits(), 64 * 1024);

    let mut range_stream = client
        .read_file(ReadFileRequest {
            project_id: "proj-big".to_string(),
            file_id: expected_unsized_id.clone(),
            offset: 1000,
            max_bytes: 90_000,
        })
        .await
        .unwrap()
        .into_inner();
    let mut ranged = Vec::new();
    while let Some(resp) = range_stream.next().await {
        ranged.extend_from_slice(&resp.unwrap().chunk);
    }
    assert_eq!(ranged, unsized_payload[1000..91_000]);

    // Truncating the on-disk blob causes ReadFile to fail with DATA_LOSS.
    let hex_id = hex::encode(&expected_unsized_id);
    let blob_path = dir
        .path()
        .join("blobs")
        .join("blobs")
        .join(&hex_id[0..2])
        .join(&hex_id[2..4])
        .join(&hex_id);
    std::fs::write(&blob_path, &unsized_payload[..500]).unwrap();
    let trunc_err = client
        .read_file(ReadFileRequest {
            project_id: "proj-big".to_string(),
            file_id: expected_unsized_id,
            offset: 0,
            max_bytes: 0,
        })
        .await
        .unwrap_err();
    assert_eq!(trunc_err.code(), tonic::Code::DataLoss);

    server_handle.abort();
}

#[tokio::test]
async fn test_register_repository_empty_project_defaults_to_repo_id() {
    let svc = make_service();
    let resp = svc
        .register_repository(Request::new(RegisterRepositoryRequest {
            project_id: String::new(),
            repo_id: Some("solo-repo".to_string()),
            name: None,
            hash_algorithm: HashAlgorithm::Sha1 as i32,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.repo_id, "solo-repo");
    assert_eq!(resp.project_id, "solo-repo");

    // Auto-generated repo_id also becomes project_id when project_id is empty.
    let resp2 = svc
        .register_repository(Request::new(RegisterRepositoryRequest {
            project_id: String::new(),
            repo_id: None,
            name: None,
            hash_algorithm: HashAlgorithm::Unspecified as i32,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!resp2.repo_id.is_empty());
    assert_eq!(resp2.project_id, resp2.repo_id);
}

#[tokio::test]
async fn test_write_tree_utf8_order_size_bytes_and_duplicate_rejection() {
    let svc = make_service();
    svc.register_project(Request::new(RegisterProjectRequest {
        project_id: Some("proj-tree".to_string()),
        name: None,
    }))
    .await
    .unwrap();

    let file_a = svc
        .write_file(Request::new(WriteFileRequest {
            project_id: "proj-tree".to_string(),
            content: b"hello".to_vec(),
        }))
        .await
        .unwrap()
        .into_inner()
        .file_id;
    let file_b = svc
        .write_file(Request::new(WriteFileRequest {
            project_id: "proj-tree".to_string(),
            content: b"world!".to_vec(),
        }))
        .await
        .unwrap()
        .into_inner()
        .file_id;

    // Duplicate entry names across a file and a directory must be rejected.
    let dup_err = svc
        .write_tree(Request::new(WriteTreeRequest {
            project_id: "proj-tree".to_string(),
            path: String::new(),
            entries: vec![
                TreeEntry {
                    name: "foo".to_string(),
                    value: Some(TreeValue {
                        value: Some(tree_value::Value::File(File {
                            id: file_a,
                            executable: false,
                            copy_id: Vec::new(),
                            size_bytes: None,
                        })),
                    }),
                },
                TreeEntry {
                    name: "foo-bar".to_string(),
                    value: Some(TreeValue {
                        value: Some(tree_value::Value::File(File {
                            id: file_b.clone(),
                            executable: false,
                            copy_id: Vec::new(),
                            size_bytes: None,
                        })),
                    }),
                },
                TreeEntry {
                    name: "foo".to_string(),
                    value: Some(TreeValue {
                        value: Some(tree_value::Value::TreeId(
                            cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                        )),
                    }),
                },
            ],
        }))
        .await
        .unwrap_err();
    assert_eq!(dup_err.code(), tonic::Code::InvalidArgument);

    // Out-of-order entries are persisted and returned in UTF-8 RepoPathComponent order
    // (`"foo" < "foo.rs"`) with server-populated `File.size_bytes`.
    let tree_id = svc
        .write_tree(Request::new(WriteTreeRequest {
            project_id: "proj-tree".to_string(),
            path: String::new(),
            entries: vec![
                TreeEntry {
                    name: "foo.rs".to_string(),
                    value: Some(TreeValue {
                        value: Some(tree_value::Value::File(File {
                            id: file_b,
                            executable: false,
                            copy_id: Vec::new(),
                            size_bytes: None,
                        })),
                    }),
                },
                TreeEntry {
                    name: "foo".to_string(),
                    value: Some(TreeValue {
                        value: Some(tree_value::Value::TreeId(
                            cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
                        )),
                    }),
                },
                TreeEntry {
                    name: "sub".to_string(),
                    value: Some(TreeValue {
                        value: Some(tree_value::Value::GitSubmoduleId(vec![0xaa; 20])),
                    }),
                },
            ],
        }))
        .await
        .unwrap()
        .into_inner()
        .tree_id;

    let read_resp = svc
        .read_tree(Request::new(ReadTreeRequest {
            project_id: "proj-tree".to_string(),
            path: String::new(),
            tree_id,
            page_size: 0,
            page_token: String::new(),
        }))
        .await
        .unwrap()
        .into_inner();
    let names: Vec<&str> = read_resp.entries.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(names, ["foo", "foo.rs", "sub"]);
    match read_resp.entries[1]
        .value
        .as_ref()
        .and_then(|v| v.value.as_ref())
    {
        Some(tree_value::Value::File(f)) => assert_eq!(f.size_bytes, Some(6)),
        other => panic!("expected file entry with size_bytes=Some(6), got {other:?}"),
    }
}

#[tokio::test]
async fn test_write_and_read_commit_external_ids_roundtrip_and_validation() {
    let svc = make_service();
    svc.register_project(Request::new(RegisterProjectRequest {
        project_id: Some("proj-ext".to_string()),
        name: None,
    }))
    .await
    .unwrap();

    let mut commit = base_commit();
    commit
        .external_ids
        .insert("git-sha1".to_string(), vec![0x42; 20]);
    commit
        .external_ids
        .insert("custom.vcs_id".to_string(), b"rev-12345".to_vec());
    let expected_ext = commit.external_ids.clone();

    let commit_id = svc
        .write_commit(Request::new(WriteCommitRequest {
            project_id: "proj-ext".to_string(),
            commit: Some(commit),
        }))
        .await
        .unwrap()
        .into_inner()
        .commit_id;

    let loaded = svc
        .read_commit(Request::new(ReadCommitRequest {
            project_id: "proj-ext".to_string(),
            commit_id,
        }))
        .await
        .unwrap()
        .into_inner()
        .commit
        .expect("written commit must be readable");
    assert_eq!(loaded.external_ids, expected_ext);

    // Invalid external_ids keys and wrong digest lengths are rejected with INVALID_ARGUMENT.
    for (bad_key, bad_val) in [
        ("UPPER", vec![0x01; 4]),
        ("..", vec![0x01; 4]),
        ("git-sha1", vec![0x01; 19]),
        ("git-sha256", vec![0x01; 20]),
        ("custom", Vec::new()),
    ] {
        let mut bad_commit = base_commit();
        bad_commit.description = format!("bad ext {bad_key}\n");
        bad_commit.external_ids.insert(bad_key.to_string(), bad_val);
        let err = svc
            .write_commit(Request::new(WriteCommitRequest {
                project_id: "proj-ext".to_string(),
                commit: Some(bad_commit),
            }))
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            tonic::Code::InvalidArgument,
            "expected InvalidArgument for external_ids[{bad_key:?}]"
        );
    }
}
