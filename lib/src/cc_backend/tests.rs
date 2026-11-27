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
use std::sync::atomic::AtomicUsize;

use cc_common::backend::backend_service_server::{BackendService, BackendServiceServer};
use cc_common::backend::*;
use cc_common::op_store::op_store_service_server::{OpStoreService, OpStoreServiceServer};
use cc_common::op_store::{
    GetOpHeadsRequest, GetOpHeadsResponse, PublishOperationRequest, PublishOperationResponse,
    ReadOperationRequest, ReadOperationResponse, ReadViewRequest, ReadViewResponse,
    ReconcileOpHeadsRequest, ReconcileOpHeadsResponse, ResolveOperationPrefixRequest,
    ResolveOperationPrefixResponse, UpdateOpHeadsRequest, UpdateOpHeadsResponse,
    WriteOperationRequest, WriteOperationResponse, WriteViewRequest, WriteViewResponse,
};
use jj_lib::backend::{Commit, Signature, Timestamp};
use sha1::{Digest as _, Sha1};
use tonic::{Request, Response, Status, Streaming};

use std::sync::atomic::Ordering;

use futures::AsyncReadExt as _;

use super::codec::{canonical_git_commit_bytes, verified_commit_from_proto};
use super::streaming::tree_from_proto_entries;
use super::*;

struct UnaryOnlyBackendServer {
    stream_attempts: Arc<AtomicUsize>,
    unary_writes: Arc<AtomicUsize>,
    expected_project_id: String,
}

fn unimpl<T>() -> Result<Response<T>, Status> {
    Err(Status::unimplemented("unimplemented in mock"))
}

#[tonic::async_trait]
impl BackendService for UnaryOnlyBackendServer {
    async fn register_project(
        &self,
        _: Request<RegisterProjectRequest>,
    ) -> Result<Response<RegisterProjectResponse>, Status> {
        unimpl()
    }
    async fn register_repository(
        &self,
        req: Request<RegisterRepositoryRequest>,
    ) -> Result<Response<RegisterRepositoryResponse>, Status> {
        let r = req.into_inner();
        if r.project_id.trim().is_empty() {
            return Err(Status::invalid_argument("project_id required"));
        }
        if r.project_id != self.expected_project_id {
            return Err(Status::failed_precondition(format!(
                "repo belongs to project {}, not {}",
                self.expected_project_id, r.project_id
            )));
        }
        let repo_id = r.repo_id.unwrap_or_else(|| "alloc-repo".to_string());
        Ok(Response::new(RegisterRepositoryResponse {
            repo_id,
            project_id: r.project_id,
        }))
    }
    async fn delete_repository(
        &self,
        _: Request<DeleteRepositoryRequest>,
    ) -> Result<Response<DeleteRepositoryResponse>, Status> {
        unimpl()
    }
    async fn read_commit(
        &self,
        _: Request<ReadCommitRequest>,
    ) -> Result<Response<ReadCommitResponse>, Status> {
        unimpl()
    }
    async fn write_commit(
        &self,
        _: Request<WriteCommitRequest>,
    ) -> Result<Response<WriteCommitResponse>, Status> {
        unimpl()
    }
    async fn list_commits(
        &self,
        _: Request<ListCommitsRequest>,
    ) -> Result<Response<ListCommitsResponse>, Status> {
        unimpl()
    }
    async fn resolve_commit_prefix(
        &self,
        _: Request<ResolveCommitPrefixRequest>,
    ) -> Result<Response<ResolveCommitPrefixResponse>, Status> {
        unimpl()
    }
    async fn resolve_change_prefix(
        &self,
        _: Request<ResolveChangePrefixRequest>,
    ) -> Result<Response<ResolveChangePrefixResponse>, Status> {
        unimpl()
    }
    async fn read_tree(
        &self,
        _: Request<ReadTreeRequest>,
    ) -> Result<Response<ReadTreeResponse>, Status> {
        unimpl()
    }
    async fn write_tree(
        &self,
        _: Request<WriteTreeRequest>,
    ) -> Result<Response<WriteTreeResponse>, Status> {
        unimpl()
    }
    type ReadFileStream =
        Pin<Box<dyn futures::Stream<Item = Result<ReadFileResponse, Status>> + Send + 'static>>;
    async fn read_file(
        &self,
        _: Request<ReadFileRequest>,
    ) -> Result<Response<Self::ReadFileStream>, Status> {
        unimpl()
    }
    async fn write_file(
        &self,
        req: Request<WriteFileRequest>,
    ) -> Result<Response<WriteFileResponse>, Status> {
        let r = req.into_inner();
        assert_eq!(r.project_id, self.expected_project_id);
        self.unary_writes.fetch_add(1, Ordering::SeqCst);
        let mut hasher = Sha1::new();
        hasher.update(format!("blob {}\0", r.content.len()).as_bytes());
        hasher.update(&r.content);
        Ok(Response::new(WriteFileResponse {
            file_id: hasher.finalize().to_vec(),
        }))
    }
    async fn write_file_stream(
        &self,
        _: Request<Streaming<WriteFileChunk>>,
    ) -> Result<Response<WriteFileResponse>, Status> {
        self.stream_attempts.fetch_add(1, Ordering::SeqCst);
        Err(Status::unimplemented("WriteFileStream not supported"))
    }
    async fn read_symlink(
        &self,
        _: Request<ReadSymlinkRequest>,
    ) -> Result<Response<ReadSymlinkResponse>, Status> {
        unimpl()
    }
    async fn write_symlink(
        &self,
        _: Request<WriteSymlinkRequest>,
    ) -> Result<Response<WriteSymlinkResponse>, Status> {
        unimpl()
    }
}

struct ExistingRepoOpStore {
    existing_repo_id: String,
}

#[tonic::async_trait]
impl OpStoreService for ExistingRepoOpStore {
    async fn read_operation(
        &self,
        _: Request<ReadOperationRequest>,
    ) -> Result<Response<ReadOperationResponse>, Status> {
        unimpl()
    }
    async fn write_operation(
        &self,
        _: Request<WriteOperationRequest>,
    ) -> Result<Response<WriteOperationResponse>, Status> {
        unimpl()
    }
    async fn read_view(
        &self,
        _: Request<ReadViewRequest>,
    ) -> Result<Response<ReadViewResponse>, Status> {
        unimpl()
    }
    async fn write_view(
        &self,
        _: Request<WriteViewRequest>,
    ) -> Result<Response<WriteViewResponse>, Status> {
        unimpl()
    }
    async fn get_op_heads(
        &self,
        req: Request<GetOpHeadsRequest>,
    ) -> Result<Response<GetOpHeadsResponse>, Status> {
        if req.into_inner().repo_id == self.existing_repo_id {
            Ok(Response::new(GetOpHeadsResponse {
                op_head_ids: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
            }))
        } else {
            Err(Status::not_found("repo not found"))
        }
    }
    async fn update_op_heads(
        &self,
        _: Request<UpdateOpHeadsRequest>,
    ) -> Result<Response<UpdateOpHeadsResponse>, Status> {
        unimpl()
    }
    async fn reconcile_op_heads(
        &self,
        _: Request<ReconcileOpHeadsRequest>,
    ) -> Result<Response<ReconcileOpHeadsResponse>, Status> {
        unimpl()
    }
    async fn resolve_operation_prefix(
        &self,
        _: Request<ResolveOperationPrefixRequest>,
    ) -> Result<Response<ResolveOperationPrefixResponse>, Status> {
        unimpl()
    }
    async fn publish_operation(
        &self,
        _: Request<PublishOperationRequest>,
    ) -> Result<Response<PublishOperationResponse>, Status> {
        unimpl()
    }
}

fn make_base_commit() -> Commit {
    Commit {
        parents: vec![CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES)],
        predecessors: Vec::new(),
        root_tree: jj_lib::merge::Merge::resolved(TreeId::from_bytes(
            &cc_common::EMPTY_TREE_ID_BYTES,
        )),
        conflict_labels: jj_lib::merge::Merge::resolved(String::new()),
        change_id: ChangeId::from_bytes(&[0x42; cc_common::CHANGE_ID_LENGTH]),
        description: "signed test commit".to_string(),
        author: Signature {
            name: "Alice".to_string(),
            email: "alice@example.com".to_string(),
            timestamp: Timestamp {
                timestamp: MillisSinceEpoch(1_700_000_000_000),
                tz_offset: 0,
            },
        },
        committer: Signature {
            name: "Alice".to_string(),
            email: "alice@example.com".to_string(),
            timestamp: Timestamp {
                timestamp: MillisSinceEpoch(1_700_000_000_000),
                tz_offset: 0,
            },
        },
        secure_sig: None,
    }
}

#[test]
fn test_write_file_falls_back_to_unary_write_file_and_caches_preference() {
    let stream_attempts = Arc::new(AtomicUsize::new(0));
    let unary_writes = Arc::new(AtomicUsize::new(0));
    let sa = Arc::clone(&stream_attempts);
    let uw = Arc::clone(&unary_writes);

    let url = run_async(move || async move {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let incoming = tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
            .map_err(|e| format!("incoming error: {e}"))?;
        let svc = UnaryOnlyBackendServer {
            stream_attempts: sa,
            unary_writes: uw,
            expected_project_id: "shared-proj".to_string(),
        };
        let op_svc = ExistingRepoOpStore {
            existing_repo_id: "actor-1".to_string(),
        };
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(BackendServiceServer::new(svc))
                .add_service(OpStoreServiceServer::new(op_svc))
                .serve_with_incoming(incoming),
        );
        Ok(format!("http://{addr}"))
    })
    .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let backend = CommitCloudBackend::init_in_project(
        dir.path(),
        &url,
        None,
        Some("shared-proj"),
        Some("actor-1"),
        true,
    )
    .unwrap();
    assert_eq!(backend.project_id(), "shared-proj");
    assert_eq!(backend.repo_id(), "actor-1");

    futures::executor::block_on(async {
        let mut r1 = futures::io::Cursor::new(b"first payload".to_vec());
        let _ = backend.write_file(RepoPath::root(), &mut r1).await.unwrap();
        let mut r2 = futures::io::Cursor::new(b"second payload".to_vec());
        let _ = backend.write_file(RepoPath::root(), &mut r2).await.unwrap();
    });

    assert_eq!(
        stream_attempts.load(Ordering::SeqCst),
        1,
        "second write_file must skip WriteFileStream once UNIMPLEMENTED was observed"
    );
    assert_eq!(unary_writes.load(Ordering::SeqCst), 2);

    // `init_in_project(create = false)` must verify project binding and fail closed on mismatch.
    let attach_bad = tempfile::tempdir().unwrap();
    assert!(
        CommitCloudBackend::init_in_project(
            attach_bad.path(),
            &url,
            None,
            Some("wrong-proj"),
            Some("actor-1"),
            false,
        )
        .is_err(),
        "mismatched project_id on attach must fail closed"
    );
    assert!(
        !attach_bad.path().join("config.toml").exists(),
        "failed attach must not write config.toml"
    );

    let attach_ok = tempfile::tempdir().unwrap();
    let attached = CommitCloudBackend::init_in_project(
        attach_ok.path(),
        &url,
        None,
        Some("shared-proj"),
        Some("actor-1"),
        false,
    )
    .unwrap();
    assert_eq!(attached.project_id(), "shared-proj");
    assert_eq!(attached.repo_id(), "actor-1");
}

#[test]
fn init_in_project_rejects_missing_or_blank_identifiers() {
    let dir = tempfile::tempdir().unwrap();
    assert!(
        CommitCloudBackend::init_in_project(
            dir.path(),
            "http://127.0.0.1:1",
            None,
            None,
            None,
            false,
        )
        .is_err(),
        "create = false without repo_id must fail"
    );
    assert!(
        CommitCloudBackend::init_in_project(
            dir.path(),
            "http://127.0.0.1:1",
            None,
            None,
            Some("   "),
            false,
        )
        .is_err(),
        "whitespace-only repo_id must fail"
    );
    assert!(
        CommitCloudBackend::init_in_project(
            dir.path(),
            "http://127.0.0.1:1",
            None,
            Some("  "),
            Some("repo-1"),
            true,
        )
        .is_err(),
        "whitespace-only project_id must fail"
    );
}

#[test]
fn apply_commit_signing_and_verified_commit_from_proto_roundtrip() {
    let mut commit = make_base_commit();
    let mut sign_fn = |data: &[u8]| {
        assert!(!data.is_empty());
        Ok(b"-----BEGIN SSH SIGNATURE-----\nfake\n-----END SSH SIGNATURE-----\n".to_vec())
    };
    apply_commit_signing(&mut commit, Some(&mut sign_fn)).unwrap();
    let sig = commit.secure_sig.clone().expect("signature populated");

    let proto = commit_to_proto(&commit);
    let signed_bytes = canonical_git_commit_bytes(&proto, Some(&sig.sig)).unwrap();
    let expected_id = CommitId::from_bytes(
        gix::objs::compute_hash(
            gix::hash::Kind::Sha1,
            gix::objs::Kind::Commit,
            &signed_bytes,
        )
        .unwrap()
        .as_bytes(),
    );

    let decoded = verified_commit_from_proto(&expected_id, proto.clone()).unwrap();
    assert_eq!(decoded.secure_sig, Some(sig.clone()));

    // Tampering with description on canonical commit fails digest check.
    let mut tampered = proto.clone();
    tampered.description = "tampered!".to_string();
    assert!(verified_commit_from_proto(&expected_id, tampered).is_err());

    // Empty sig.sig rejected.
    let mut empty_sig = proto.clone();
    empty_sig.secure_sig = Some(cc_common::backend::SecureSig {
        data: sig.data.clone(),
        sig: Vec::new(),
    });
    assert!(verified_commit_from_proto(&expected_id, empty_sig).is_err());

    // Even non-zero conflict_labels rejected without panicking.
    let mut bad_labels = proto.clone();
    bad_labels.conflict_labels = vec!["a".to_string(), "b".to_string()];
    assert!(verified_commit_from_proto(&expected_id, bad_labels).is_err());

    // Smuggling signed raw_git_commit onto mismatched structured fields is rejected.
    let mut smuggled = proto.clone();
    smuggled.raw_git_commit = signed_bytes.clone();
    smuggled.description = "evil description".to_string();
    assert!(verified_commit_from_proto(&expected_id, smuggled).is_err());

    // Stripping secure_sig on a signed raw_git_commit must be rejected.
    let mut stripped_sig = proto.clone();
    stripped_sig.raw_git_commit = signed_bytes.clone();
    stripped_sig.secure_sig = None;
    assert!(verified_commit_from_proto(&expected_id, stripped_sig).is_err());

    // Signed raw_git_commit with matching secure_sig succeeds.
    let mut raw_signed = proto;
    raw_signed.raw_git_commit = signed_bytes;
    let decoded_raw = verified_commit_from_proto(&expected_id, raw_signed).unwrap();
    assert_eq!(decoded_raw.secure_sig, Some(sig));

    // Raw hand-formatted Git commit with multi-line `gpgsig` header also round-trips.
    let hand_raw = format!(
        "tree {}\nauthor Signer <signer@example.com> 1700000000 +0000\ncommitter Signer <signer@example.com> 1700000000 +0000\ngpgsig -----BEGIN PGP SIGNATURE-----\n \n wsBcBAABCAAQ\n -----END PGP SIGNATURE-----\n\nsigned imported commit\n",
        cc_common::EMPTY_TREE_ID_HEX
    );
    let (parsed_sig, parsed_data) =
        gix::objs::CommitRefIter::signature(hand_raw.as_bytes(), gix::hash::Kind::Sha1)
            .unwrap()
            .expect("raw gpgsig header should parse");
    let hand_id = CommitId::from_bytes(
        gix::objs::compute_hash(
            gix::hash::Kind::Sha1,
            gix::objs::Kind::Commit,
            hand_raw.as_bytes(),
        )
        .unwrap()
        .as_bytes(),
    );
    let hand_proto = cc_common::backend::Commit {
        commit_id: hand_id.as_bytes().to_vec(),
        change_id: jj_lib::git_backend::synthetic_change_id_from_git_commit_id(&hand_id)
            .as_bytes()
            .to_vec(),
        parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
        description: "signed imported commit\n".to_string(),
        author: Some(cc_common::backend::Signature {
            name: "Signer".to_string(),
            email: "signer@example.com".to_string(),
            timestamp: Some(cc_common::backend::Timestamp {
                millis_since_epoch: 1_700_000_000_000,
                tz_offset: 0,
            }),
        }),
        committer: Some(cc_common::backend::Signature {
            name: "Signer".to_string(),
            email: "signer@example.com".to_string(),
            timestamp: Some(cc_common::backend::Timestamp {
                millis_since_epoch: 1_700_000_000_000,
                tz_offset: 0,
            }),
        }),
        predecessors: Vec::new(),
        conflict_labels: Vec::new(),
        secure_sig: Some(cc_common::backend::SecureSig {
            data: Vec::from(parsed_data.to_bstring()),
            sig: parsed_sig.as_ref().to_vec(),
        }),
        raw_git_commit: hand_raw.into_bytes(),
        external_ids: Default::default(),
    };
    let hand_decoded = verified_commit_from_proto(&hand_id, hand_proto).unwrap();
    assert!(hand_decoded.secure_sig.is_some());
}

#[test]
fn apply_commit_signing_rejects_empty_signature() {
    let mut commit = make_base_commit();
    let mut empty_sign_fn = |_data: &[u8]| Ok(Vec::new());
    assert!(apply_commit_signing(&mut commit, Some(&mut empty_sign_fn)).is_err());
    assert!(commit.secure_sig.is_none());
}

#[test]
fn apply_commit_signing_rejects_already_signed_commit() {
    let mut commit = make_base_commit();
    let existing_sig = jj_lib::backend::SecureSig {
        data: b"original signed bytes".to_vec(),
        sig: b"existing-sig".to_vec(),
    };
    commit.secure_sig = Some(existing_sig.clone());

    let called = Arc::new(AtomicUsize::new(0));
    let called_ref = Arc::clone(&called);
    let mut sign_fn = move |_data: &[u8]| {
        called_ref.fetch_add(1, Ordering::SeqCst);
        Ok(b"replacement-sig".to_vec())
    };
    let err = apply_commit_signing(&mut commit, Some(&mut sign_fn))
        .expect_err("sign_with on a commit that already carries secure_sig must fail");
    assert!(
        matches!(err, BackendError::Other(_)) && err.to_string().contains("already has secure_sig"),
        "unexpected error: {err:?}"
    );
    assert_eq!(called.load(Ordering::SeqCst), 0);
    assert_eq!(commit.secure_sig, Some(existing_sig));

    // Passing `sign_with = None` leaves an existing signature intact.
    apply_commit_signing(&mut commit, None).unwrap();
}

#[test]
fn verified_commit_from_proto_rejects_malformed_wire_payloads_without_panicking() {
    let base = make_base_commit();
    let proto = commit_to_proto(&base);
    let unsigned = canonical_git_commit_bytes(&proto, None).unwrap();
    let expected_id = CommitId::from_bytes(
        gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Commit, &unsigned)
            .unwrap()
            .as_bytes(),
    );

    let mut bad_change = proto.clone();
    bad_change.change_id = vec![1; 8];
    assert!(verified_commit_from_proto(&expected_id, bad_change).is_err());

    let mut empty_trees = proto.clone();
    empty_trees.root_tree_id.clear();
    assert!(verified_commit_from_proto(&expected_id, empty_trees).is_err());

    let mut even_trees = proto.clone();
    even_trees
        .root_tree_id
        .push(cc_common::EMPTY_TREE_ID_BYTES.to_vec());
    assert!(verified_commit_from_proto(&expected_id, even_trees).is_err());

    let mut short_tree = proto.clone();
    short_tree.root_tree_id = vec![
        cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
        vec![0xaa; 5],
        cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
    ];
    assert!(verified_commit_from_proto(&expected_id, short_tree.clone()).is_err());
    assert!(canonical_git_commit_bytes(&short_tree, None).is_err());

    let mut single_tree_label = proto.clone();
    single_tree_label.conflict_labels = vec!["side-a".to_string()];
    assert!(verified_commit_from_proto(&expected_id, single_tree_label).is_err());

    let mut newline_label = proto.clone();
    newline_label.root_tree_id = vec![
        cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
        cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
        cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
    ];
    newline_label.conflict_labels = vec!["a\nb".into(), "base".into(), "c".into()];
    assert!(verified_commit_from_proto(&expected_id, newline_label).is_err());

    let mut no_parents = proto.clone();
    no_parents.parent_commit_ids.clear();
    assert!(verified_commit_from_proto(&expected_id, no_parents).is_err());

    let mut merge_with_root = proto.clone();
    merge_with_root.parent_commit_ids.push(vec![0x11; 20]);
    assert!(verified_commit_from_proto(&expected_id, merge_with_root).is_err());

    let mut short_parent = proto.clone();
    short_parent.parent_commit_ids = vec![vec![0x11; 19]];
    assert!(verified_commit_from_proto(&expected_id, short_parent).is_err());

    let mut short_pred = proto;
    short_pred.predecessors = vec![vec![0x22; 12]];
    assert!(verified_commit_from_proto(&expected_id, short_pred).is_err());
}

#[test]
fn tree_deserialization_rejects_invalid_names_lengths_and_duplicates() {
    let valid_file = |name: &str| cc_common::backend::TreeEntry {
        name: name.to_string(),
        value: Some(cc_common::backend::TreeValue {
            value: Some(cc_common::backend::tree_value::Value::File(
                cc_common::backend::File {
                    id: vec![0xab; cc_common::FILE_ID_LENGTH],
                    executable: false,
                    copy_id: Vec::new(),
                    size_bytes: None,
                },
            )),
        }),
    };

    for bad_name in ["", ".", "..", "a/b", "a\0b"] {
        assert!(
            tree_entry_from_proto(valid_file(bad_name)).is_err(),
            "expected invalid name {bad_name:?} to be rejected"
        );
    }

    let short_file = cc_common::backend::TreeEntry {
        name: "f.txt".to_string(),
        value: Some(cc_common::backend::TreeValue {
            value: Some(cc_common::backend::tree_value::Value::File(
                cc_common::backend::File {
                    id: vec![0xab; 19],
                    executable: false,
                    copy_id: Vec::new(),
                    size_bytes: None,
                },
            )),
        }),
    };
    assert!(tree_entry_from_proto(short_file).is_err());

    let short_tree = cc_common::backend::TreeEntry {
        name: "dir".to_string(),
        value: Some(cc_common::backend::TreeValue {
            value: Some(cc_common::backend::tree_value::Value::TreeId(vec![
                0xab;
                10
            ])),
        }),
    };
    assert!(tree_entry_from_proto(short_tree).is_err());

    let short_symlink = cc_common::backend::TreeEntry {
        name: "sym".to_string(),
        value: Some(cc_common::backend::TreeValue {
            value: Some(cc_common::backend::tree_value::Value::SymlinkId(vec![
                0xab;
                19
            ])),
        }),
    };
    assert!(tree_entry_from_proto(short_symlink).is_err());

    let short_submodule = cc_common::backend::TreeEntry {
        name: "sub".to_string(),
        value: Some(cc_common::backend::TreeValue {
            value: Some(cc_common::backend::tree_value::Value::GitSubmoduleId(vec![
                0xab;
                19
            ])),
        }),
    };
    assert!(tree_entry_from_proto(short_submodule).is_err());

    let conflict_entry = cc_common::backend::TreeEntry {
        name: "conflicted".to_string(),
        value: Some(cc_common::backend::TreeValue {
            value: Some(cc_common::backend::tree_value::Value::ConflictId(vec![
                0xab;
                20
            ])),
        }),
    };
    let conflict_err =
        tree_entry_from_proto(conflict_entry).expect_err("ConflictId tree entry must be rejected");
    assert!(
        conflict_err.to_string().contains("ConflictId"),
        "unexpected ConflictId error: {conflict_err}"
    );

    let non_empty_copy_id = cc_common::backend::TreeEntry {
        name: "copied.txt".to_string(),
        value: Some(cc_common::backend::TreeValue {
            value: Some(cc_common::backend::tree_value::Value::File(
                cc_common::backend::File {
                    id: vec![0xab; cc_common::FILE_ID_LENGTH],
                    executable: false,
                    copy_id: vec![0xcd; 4],
                    size_bytes: None,
                },
            )),
        }),
    };
    assert!(tree_entry_from_proto(non_empty_copy_id).is_err());

    let dup = tree_from_proto_entries(vec![valid_file("dup.txt"), valid_file("dup.txt")]);
    assert!(
        dup.is_err(),
        "duplicate tree entry names must return BackendError instead of panicking"
    );
}

#[test]
fn tree_entry_proto_roundtrips_all_tree_value_variants() {
    use jj_lib::backend::TreeValue;
    use jj_lib::repo_path::RepoPathComponentBuf;

    let expected_entries = vec![
        (
            RepoPathComponentBuf::new("dir").unwrap(),
            TreeValue::Tree(TreeId::from_bytes(&[0x11; cc_common::TREE_ID_LENGTH])),
        ),
        (
            RepoPathComponentBuf::new("exec.sh").unwrap(),
            TreeValue::File {
                id: FileId::from_bytes(&[0x22; cc_common::FILE_ID_LENGTH]),
                executable: true,
                copy_id: CopyId::placeholder(),
            },
        ),
        (
            RepoPathComponentBuf::new("file.txt").unwrap(),
            TreeValue::File {
                id: FileId::from_bytes(&[0x33; cc_common::FILE_ID_LENGTH]),
                executable: false,
                copy_id: CopyId::placeholder(),
            },
        ),
        (
            RepoPathComponentBuf::new("link").unwrap(),
            TreeValue::Symlink(SymlinkId::from_bytes(&[0x44; cc_common::SYMLINK_ID_LENGTH])),
        ),
        (
            RepoPathComponentBuf::new("submod").unwrap(),
            TreeValue::GitSubmodule(CommitId::from_bytes(&[0x55; cc_common::COMMIT_ID_LENGTH])),
        ),
    ];
    let tree = Tree::from_sorted_entries(expected_entries.clone());
    let proto_entries: Vec<_> = tree
        .entries()
        .map(|entry| tree_entry_to_proto(&entry).unwrap())
        .collect();
    let decoded = tree_from_proto_entries(proto_entries).unwrap();
    let decoded_entries: Vec<_> = decoded
        .entries()
        .map(|entry| (entry.name().to_owned(), entry.value().clone()))
        .collect();
    assert_eq!(decoded_entries, expected_entries);
}

#[test]
fn verified_commit_from_proto_binds_synthetic_change_id_and_validates_external_ids() {
    use gix::objs::WriteTo as _;

    let raw_gix = gix::objs::Commit {
        tree: gix::hash::ObjectId::empty_tree(gix::hash::Kind::Sha1),
        parents: Default::default(),
        author: gix::actor::Signature {
            name: "Alice".into(),
            email: "alice@example.com".into(),
            time: gix::date::Time::new(1_700_000_000, 0),
        },
        committer: gix::actor::Signature {
            name: "Alice".into(),
            email: "alice@example.com".into(),
            time: gix::date::Time::new(1_700_000_000, 0),
        },
        encoding: None,
        extra_headers: Vec::new(),
        message: "unsigned headerless git commit".into(),
    };
    let mut raw_bytes = Vec::new();
    raw_gix.write_to(&mut raw_bytes).unwrap();
    let expected_id = CommitId::from_bytes(
        gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Commit, &raw_bytes)
            .unwrap()
            .as_bytes(),
    );
    let expected_change_id =
        jj_lib::git_backend::synthetic_change_id_from_git_commit_id(&expected_id);

    let mut base = make_base_commit();
    base.description = "unsigned headerless git commit".to_string();
    base.change_id = expected_change_id.clone();
    let mut proto = commit_to_proto(&base);
    proto.raw_git_commit = raw_bytes;

    let decoded = verified_commit_from_proto(&expected_id, proto.clone()).unwrap();
    assert_eq!(decoded.change_id, expected_change_id);

    // Mutating change_id away from the synthetic Git change ID is rejected.
    let mut wrong_change_id = proto.clone();
    wrong_change_id.change_id = vec![0x99; cc_common::CHANGE_ID_LENGTH];
    assert!(verified_commit_from_proto(&expected_id, wrong_change_id).is_err());

    // External IDs validation: valid entries pass, malformed entries fail.
    let mut valid_ext = proto.clone();
    valid_ext
        .external_ids
        .insert("git-sha1".to_string(), expected_id.as_bytes().to_vec());
    valid_ext
        .external_ids
        .insert("git-sha256".to_string(), vec![0xab; 32]);
    valid_ext
        .external_ids
        .insert("perforce-cl".to_string(), b"123456".to_vec());
    assert!(verified_commit_from_proto(&expected_id, valid_ext).is_ok());

    for (bad_key, bad_val) in [
        ("", vec![1]),
        ("bad/key", vec![1]),
        ("git-sha1", vec![1; 19]),
        ("git-sha256", vec![1; 20]),
        ("perforce-cl", vec![]),
        ("perforce-cl", vec![1; 65]),
    ] {
        let mut bad_ext = proto.clone();
        bad_ext.external_ids.insert(bad_key.to_string(), bad_val);
        assert!(
            verified_commit_from_proto(&expected_id, bad_ext).is_err(),
            "expected invalid external_ids key={bad_key:?} to be rejected"
        );
    }
}

#[test]
fn verified_commit_from_proto_accepts_legacy_all_empty_conflict_labels_and_rejects_invalid() {
    let mut commit = make_base_commit();
    commit.root_tree = jj_lib::merge::Merge::from_vec(vec![
        TreeId::from_bytes(&[0x11; cc_common::TREE_ID_LENGTH]),
        TreeId::from_bytes(&[0x22; cc_common::TREE_ID_LENGTH]),
        TreeId::from_bytes(&[0x33; cc_common::TREE_ID_LENGTH]),
    ]);
    let mut proto = commit_to_proto(&commit);
    let canonical_bytes = canonical_git_commit_bytes(&proto, None).unwrap();
    let expected_id = CommitId::from_bytes(
        gix::objs::compute_hash(
            gix::hash::Kind::Sha1,
            gix::objs::Kind::Commit,
            &canonical_bytes,
        )
        .unwrap()
        .as_bytes(),
    );

    // Legacy all-empty per-tree conflict_labels (`["", "", ""]`) on a 3-tree conflict
    // must be accepted and canonicalized to unlabeled (`Merge::resolved("")`).
    proto.conflict_labels = vec![String::new(), String::new(), String::new()];
    let decoded = verified_commit_from_proto(&expected_id, proto.clone()).unwrap();
    assert_eq!(
        decoded.conflict_labels,
        jj_lib::merge::Merge::resolved(String::new())
    );

    // Wrong-length all-empty and mixed labels on a 3-tree conflict must still be rejected.
    for bad_labels in [
        vec![String::new(), String::new()],
        vec!["left".to_string(), String::new()],
        vec!["a".to_string(), "b".to_string()],
        vec![String::new(); 4],
    ] {
        let mut invalid = proto.clone();
        invalid.conflict_labels = bad_labels.clone();
        assert!(
            verified_commit_from_proto(&expected_id, invalid).is_err(),
            "expected invalid conflict_labels={bad_labels:?} to be rejected"
        );
    }

    // Valid partial labels matching tree count succeed when digest includes them.
    let mut labeled_proto = proto;
    labeled_proto.conflict_labels = vec!["left".to_string(), String::new(), "right".to_string()];
    let labeled_bytes = canonical_git_commit_bytes(&labeled_proto, None).unwrap();
    let labeled_id = CommitId::from_bytes(
        gix::objs::compute_hash(
            gix::hash::Kind::Sha1,
            gix::objs::Kind::Commit,
            &labeled_bytes,
        )
        .unwrap()
        .as_bytes(),
    );
    let decoded_labeled = verified_commit_from_proto(&labeled_id, labeled_proto).unwrap();
    assert_eq!(
        decoded_labeled.conflict_labels,
        jj_lib::merge::Merge::from_vec(vec![
            "left".to_string(),
            String::new(),
            "right".to_string(),
        ])
    );
}

#[test]
fn verified_commit_from_proto_handles_all_empty_raw_conflict_labels_header() {
    use gix::objs::WriteTo as _;

    let trees = [
        vec![0x11; cc_common::TREE_ID_LENGTH],
        vec![0x22; cc_common::TREE_ID_LENGTH],
        vec![0x33; cc_common::TREE_ID_LENGTH],
    ];
    let trees_header = trees
        .iter()
        .map(|id| jj_lib::hex_util::encode_hex(id))
        .collect::<Vec<_>>()
        .join(" ");

    let make_raw_with_headers = |extra_headers: Vec<(&str, &[u8])>| -> (CommitId, Vec<u8>) {
        let raw_gix = gix::objs::Commit {
            tree: gix::hash::ObjectId::try_from(trees[0].as_slice()).unwrap(),
            parents: Default::default(),
            author: gix::actor::Signature {
                name: "Alice".into(),
                email: "alice@example.com".into(),
                time: gix::date::Time::new(1_700_000_000, 0),
            },
            committer: gix::actor::Signature {
                name: "Alice".into(),
                email: "alice@example.com".into(),
                time: gix::date::Time::new(1_700_000_000, 0),
            },
            encoding: None,
            extra_headers: extra_headers
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
            message: "signed test commit".into(),
        };
        let mut raw_bytes = Vec::new();
        raw_gix.write_to(&mut raw_bytes).unwrap();
        let id = CommitId::from_bytes(
            gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Commit, &raw_bytes)
                .unwrap()
                .as_bytes(),
        );
        (id, raw_bytes)
    };

    // Valid all-empty header lengths (0, 1 legacy, or matching 3 trees) are accepted on read.
    for header_val in [&b""[..], &b"\n"[..], &b"\n\n\n"[..]] {
        let (id, raw_bytes) = make_raw_with_headers(vec![
            ("jj:conflict-labels", header_val),
            ("jj:trees", trees_header.as_bytes()),
        ]);
        let mut commit = make_base_commit();
        commit.root_tree = jj_lib::merge::Merge::from_vec(
            trees
                .iter()
                .map(|t| TreeId::from_bytes(t))
                .collect::<Vec<_>>(),
        );
        commit.change_id = jj_lib::git_backend::synthetic_change_id_from_git_commit_id(&id);
        let mut proto = commit_to_proto(&commit);
        proto.raw_git_commit = raw_bytes;
        proto.conflict_labels = vec![String::new(), String::new(), String::new()];

        let decoded = verified_commit_from_proto(&id, proto).unwrap();
        assert_eq!(
            decoded.conflict_labels,
            jj_lib::merge::Merge::resolved(String::new())
        );
    }

    // Wrong-length all-empty header (`"\n\n"` for 3 trees) must be rejected.
    let (bad_id, bad_raw) = make_raw_with_headers(vec![
        ("jj:conflict-labels", b"\n\n"),
        ("jj:trees", trees_header.as_bytes()),
    ]);
    let mut bad_commit = make_base_commit();
    bad_commit.root_tree = jj_lib::merge::Merge::from_vec(
        trees
            .iter()
            .map(|t| TreeId::from_bytes(t))
            .collect::<Vec<_>>(),
    );
    bad_commit.change_id = jj_lib::git_backend::synthetic_change_id_from_git_commit_id(&bad_id);
    let mut bad_proto = commit_to_proto(&bad_commit);
    bad_proto.raw_git_commit = bad_raw;
    assert!(verified_commit_from_proto(&bad_id, bad_proto).is_err());

    // 3 empty lines on a single-tree commit must also be rejected.
    let (single_id, single_raw) = make_raw_with_headers(vec![("jj:conflict-labels", b"\n\n\n")]);
    let mut single_commit = make_base_commit();
    single_commit.root_tree = jj_lib::merge::Merge::resolved(TreeId::from_bytes(&trees[0]));
    single_commit.change_id =
        jj_lib::git_backend::synthetic_change_id_from_git_commit_id(&single_id);
    let mut single_proto = commit_to_proto(&single_commit);
    single_proto.raw_git_commit = single_raw;
    assert!(verified_commit_from_proto(&single_id, single_proto).is_err());
}

enum ReadTreeMockMode {
    MismatchedTreeId,
    EmptyPageWithNextToken,
    ExceedEntryCap {
        page_entries: Vec<cc_common::backend::TreeEntry>,
    },
}

struct MockTreeAndStreamBackend {
    mode: ReadTreeMockMode,
    page_counter: AtomicUsize,
    recorded_stream_frames: std::sync::Mutex<Vec<(String, Option<u64>, usize)>>,
}

#[tonic::async_trait]
impl BackendService for MockTreeAndStreamBackend {
    async fn register_project(
        &self,
        _: Request<RegisterProjectRequest>,
    ) -> Result<Response<RegisterProjectResponse>, Status> {
        unimpl()
    }
    async fn register_repository(
        &self,
        req: Request<RegisterRepositoryRequest>,
    ) -> Result<Response<RegisterRepositoryResponse>, Status> {
        let r = req.into_inner();
        Ok(Response::new(RegisterRepositoryResponse {
            repo_id: r.repo_id.unwrap_or_else(|| "repo-1".to_string()),
            project_id: r.project_id,
        }))
    }
    async fn delete_repository(
        &self,
        _: Request<DeleteRepositoryRequest>,
    ) -> Result<Response<DeleteRepositoryResponse>, Status> {
        unimpl()
    }
    async fn read_commit(
        &self,
        _: Request<ReadCommitRequest>,
    ) -> Result<Response<ReadCommitResponse>, Status> {
        unimpl()
    }
    async fn write_commit(
        &self,
        _: Request<WriteCommitRequest>,
    ) -> Result<Response<WriteCommitResponse>, Status> {
        unimpl()
    }
    async fn list_commits(
        &self,
        _: Request<ListCommitsRequest>,
    ) -> Result<Response<ListCommitsResponse>, Status> {
        unimpl()
    }
    async fn resolve_commit_prefix(
        &self,
        _: Request<ResolveCommitPrefixRequest>,
    ) -> Result<Response<ResolveCommitPrefixResponse>, Status> {
        unimpl()
    }
    async fn resolve_change_prefix(
        &self,
        _: Request<ResolveChangePrefixRequest>,
    ) -> Result<Response<ResolveChangePrefixResponse>, Status> {
        unimpl()
    }
    async fn read_tree(
        &self,
        req: Request<ReadTreeRequest>,
    ) -> Result<Response<ReadTreeResponse>, Status> {
        let requested = req.into_inner();
        match &self.mode {
            ReadTreeMockMode::MismatchedTreeId => Ok(Response::new(ReadTreeResponse {
                tree_id: vec![0x99; cc_common::TREE_ID_LENGTH],
                entries: Vec::new(),
                next_page_token: String::new(),
            })),
            ReadTreeMockMode::EmptyPageWithNextToken => Ok(Response::new(ReadTreeResponse {
                tree_id: requested.tree_id,
                entries: Vec::new(),
                next_page_token: "next-cursor".to_string(),
            })),
            ReadTreeMockMode::ExceedEntryCap { page_entries } => {
                let page = self.page_counter.fetch_add(1, Ordering::SeqCst) + 1;
                Ok(Response::new(ReadTreeResponse {
                    tree_id: requested.tree_id,
                    entries: page_entries.clone(),
                    next_page_token: format!("p{page:06}"),
                }))
            }
        }
    }
    async fn write_tree(
        &self,
        _: Request<WriteTreeRequest>,
    ) -> Result<Response<WriteTreeResponse>, Status> {
        unimpl()
    }
    type ReadFileStream =
        Pin<Box<dyn futures::Stream<Item = Result<ReadFileResponse, Status>> + Send + 'static>>;
    async fn read_file(
        &self,
        _: Request<ReadFileRequest>,
    ) -> Result<Response<Self::ReadFileStream>, Status> {
        unimpl()
    }
    async fn write_file(
        &self,
        _: Request<WriteFileRequest>,
    ) -> Result<Response<WriteFileResponse>, Status> {
        unimpl()
    }
    async fn write_file_stream(
        &self,
        req: Request<Streaming<WriteFileChunk>>,
    ) -> Result<Response<WriteFileResponse>, Status> {
        let mut stream = req.into_inner();
        let mut body = Vec::new();
        let mut frames = Vec::new();
        while let Some(frame) = stream.next().await {
            let chunk = frame?;
            frames.push((chunk.project_id, chunk.total_size, chunk.chunk.len()));
            body.extend_from_slice(&chunk.chunk);
        }
        *self.recorded_stream_frames.lock().unwrap() = frames;
        let mut hasher = Sha1::new();
        hasher.update(format!("blob {}\0", body.len()).as_bytes());
        hasher.update(&body);
        Ok(Response::new(WriteFileResponse {
            file_id: hasher.finalize().to_vec(),
        }))
    }
    async fn read_symlink(
        &self,
        _: Request<ReadSymlinkRequest>,
    ) -> Result<Response<ReadSymlinkResponse>, Status> {
        unimpl()
    }
    async fn write_symlink(
        &self,
        _: Request<WriteSymlinkRequest>,
    ) -> Result<Response<WriteSymlinkResponse>, Status> {
        unimpl()
    }
}

fn spawn_mock_tree_server(svc: Arc<MockTreeAndStreamBackend>) -> String {
    run_async(move || async move {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let incoming = tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
            .map_err(|e| format!("incoming error: {e}"))?;
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(BackendServiceServer::from_arc(svc))
                .serve_with_incoming(incoming),
        );
        Ok(format!("http://{addr}"))
    })
    .unwrap()
}

#[test]
fn test_read_tree_rejects_mismatched_tree_id_empty_page_and_entry_cap() {
    let target_tree_id = TreeId::from_bytes(&[0x42; cc_common::TREE_ID_LENGTH]);

    // Mismatched non-empty `resp.tree_id`.
    let mismatch_svc = Arc::new(MockTreeAndStreamBackend {
        mode: ReadTreeMockMode::MismatchedTreeId,
        page_counter: AtomicUsize::new(0),
        recorded_stream_frames: std::sync::Mutex::new(Vec::new()),
    });
    let mismatch_url = spawn_mock_tree_server(mismatch_svc);
    let dir1 = tempfile::tempdir().unwrap();
    let b1 = CommitCloudBackend::init_in_project(
        dir1.path(),
        &mismatch_url,
        None,
        Some("proj-1"),
        Some("repo-1"),
        true,
    )
    .unwrap();
    let err = futures::executor::block_on(b1.read_tree(RepoPath::root(), &target_tree_id))
        .expect_err("mismatched tree_id must be rejected");
    assert!(
        err.to_string().contains("did not match requested"),
        "unexpected error: {err}"
    );

    // Non-progressing empty page with non-empty `next_page_token`.
    let empty_page_svc = Arc::new(MockTreeAndStreamBackend {
        mode: ReadTreeMockMode::EmptyPageWithNextToken,
        page_counter: AtomicUsize::new(0),
        recorded_stream_frames: std::sync::Mutex::new(Vec::new()),
    });
    let empty_page_url = spawn_mock_tree_server(empty_page_svc);
    let dir2 = tempfile::tempdir().unwrap();
    let b2 = CommitCloudBackend::init_in_project(
        dir2.path(),
        &empty_page_url,
        None,
        Some("proj-1"),
        Some("repo-1"),
        true,
    )
    .unwrap();
    let err = futures::executor::block_on(b2.read_tree(RepoPath::root(), &target_tree_id))
        .expect_err("empty page with non-empty next_page_token must be rejected");
    assert!(
        err.to_string().contains("empty page"),
        "unexpected error: {err}"
    );

    // Accumulated entry count cap (`> MAX_TREE_ENTRIES`).
    let sample_entry = cc_common::backend::TreeEntry {
        name: "a".to_string(),
        value: Some(cc_common::backend::TreeValue {
            value: Some(cc_common::backend::tree_value::Value::File(
                cc_common::backend::File {
                    id: vec![0xab; cc_common::FILE_ID_LENGTH],
                    executable: false,
                    copy_id: Vec::new(),
                    size_bytes: None,
                },
            )),
        }),
    };
    let cap_svc = Arc::new(MockTreeAndStreamBackend {
        mode: ReadTreeMockMode::ExceedEntryCap {
            page_entries: vec![sample_entry; 50_000],
        },
        page_counter: AtomicUsize::new(0),
        recorded_stream_frames: std::sync::Mutex::new(Vec::new()),
    });
    let cap_url = spawn_mock_tree_server(Arc::clone(&cap_svc));
    let dir3 = tempfile::tempdir().unwrap();
    let b3 = CommitCloudBackend::init_in_project(
        dir3.path(),
        &cap_url,
        None,
        Some("proj-1"),
        Some("repo-1"),
        true,
    )
    .unwrap();
    let err = futures::executor::block_on(b3.read_tree(RepoPath::root(), &target_tree_id))
        .expect_err("tree exceeding MAX_TREE_ENTRIES must be rejected");
    assert!(
        err.to_string().contains("exceeded maximum"),
        "unexpected error: {err}"
    );
    assert_eq!(
        cap_svc.page_counter.load(Ordering::SeqCst),
        11,
        "should stop immediately on page 11 once count exceeds MAX_TREE_ENTRIES"
    );
}

#[test]
fn test_write_file_lazy_stream_handles_empty_exact_and_multi_chunk_payloads() {
    let svc = Arc::new(MockTreeAndStreamBackend {
        mode: ReadTreeMockMode::MismatchedTreeId,
        page_counter: AtomicUsize::new(0),
        recorded_stream_frames: std::sync::Mutex::new(Vec::new()),
    });
    let url = spawn_mock_tree_server(Arc::clone(&svc));
    let dir = tempfile::tempdir().unwrap();
    let backend = CommitCloudBackend::init_in_project(
        dir.path(),
        &url,
        None,
        Some("proj-stream"),
        Some("repo-stream"),
        true,
    )
    .unwrap();

    futures::executor::block_on(async {
        // Empty file sends a single frame with total_size = Some(0).
        let mut empty = futures::io::Cursor::new(Vec::new());
        backend
            .write_file(RepoPath::root(), &mut empty)
            .await
            .unwrap();
        assert_eq!(
            *svc.recorded_stream_frames.lock().unwrap(),
            vec![("proj-stream".to_string(), Some(0), 0)]
        );

        // Exact 64 KiB boundary sends 1 frame with no trailing empty chunk.
        let exact_len = cc_common::FILE_STREAM_CHUNK_SIZE;
        let mut exact = futures::io::Cursor::new(vec![0x5a; exact_len]);
        backend
            .write_file(RepoPath::root(), &mut exact)
            .await
            .unwrap();
        assert_eq!(
            *svc.recorded_stream_frames.lock().unwrap(),
            vec![("proj-stream".to_string(), Some(exact_len as u64), exact_len)]
        );

        // Multi-chunk file streams 64 KiB + 64 KiB + 7 bytes lazily.
        let multi_len = 2 * cc_common::FILE_STREAM_CHUNK_SIZE + 7;
        let mut multi = futures::io::Cursor::new(vec![0x33; multi_len]);
        backend
            .write_file(RepoPath::root(), &mut multi)
            .await
            .unwrap();
        assert_eq!(
            *svc.recorded_stream_frames.lock().unwrap(),
            vec![
                (
                    "proj-stream".to_string(),
                    Some(multi_len as u64),
                    cc_common::FILE_STREAM_CHUNK_SIZE
                ),
                (String::new(), None, cc_common::FILE_STREAM_CHUNK_SIZE),
                (String::new(), None, 7),
            ]
        );
    });
}

#[test]
fn commit_from_proto_strips_unverified_secure_sig() {
    let proto = cc_common::backend::Commit {
        commit_id: vec![1u8; cc_common::COMMIT_ID_LENGTH],
        change_id: vec![2u8; cc_common::CHANGE_ID_LENGTH],
        parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
        description: "signed".to_string(),
        author: None,
        committer: None,
        predecessors: Vec::new(),
        conflict_labels: Vec::new(),
        secure_sig: Some(cc_common::backend::SecureSig {
            data: b"forged unsigned payload".to_vec(),
            sig: b"-----BEGIN PGP SIGNATURE-----\nfake\n-----END PGP SIGNATURE-----\n".to_vec(),
        }),
        raw_git_commit: Vec::new(),
        external_ids: Default::default(),
    };
    let decoded = commit_from_proto(proto).expect("valid commit should decode");
    assert!(
        decoded.secure_sig.is_none(),
        "commit_from_proto must strip unverified SecureSig"
    );
}

#[test]
fn commit_from_proto_rejects_malformed_inputs_without_panicking() {
    let base = cc_common::backend::Commit {
        commit_id: vec![1u8; cc_common::COMMIT_ID_LENGTH],
        change_id: vec![2u8; cc_common::CHANGE_ID_LENGTH],
        parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
        description: "valid".to_string(),
        author: None,
        committer: None,
        predecessors: Vec::new(),
        conflict_labels: Vec::new(),
        secure_sig: None,
        raw_git_commit: Vec::new(),
        external_ids: Default::default(),
    };
    assert!(commit_from_proto(base.clone()).is_ok());

    let mut empty_trees = base.clone();
    empty_trees.root_tree_id.clear();
    assert!(commit_from_proto(empty_trees).is_err());

    let mut even_trees = base.clone();
    even_trees
        .root_tree_id
        .push(cc_common::EMPTY_TREE_ID_BYTES.to_vec());
    assert!(commit_from_proto(even_trees).is_err());

    let mut bad_tree_len = base.clone();
    bad_tree_len.root_tree_id = vec![vec![0u8; cc_common::TREE_ID_LENGTH - 1]];
    assert!(commit_from_proto(bad_tree_len).is_err());

    let mut bad_change_len = base.clone();
    bad_change_len.change_id = vec![0u8; cc_common::CHANGE_ID_LENGTH - 1];
    assert!(commit_from_proto(bad_change_len).is_err());

    let mut bad_parent_len = base.clone();
    bad_parent_len.parent_commit_ids = vec![vec![0u8; 3]];
    assert!(commit_from_proto(bad_parent_len).is_err());

    let mut bad_pred_len = base.clone();
    bad_pred_len.predecessors = vec![vec![0u8; 3]];
    assert!(commit_from_proto(bad_pred_len).is_err());

    let mut even_labels = base.clone();
    even_labels.root_tree_id = vec![
        cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
        cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
        cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
    ];
    even_labels.conflict_labels = vec!["a".to_string(), "b".to_string()];
    assert!(commit_from_proto(even_labels).is_err());

    let mut single_tree_label = base.clone();
    single_tree_label.conflict_labels = vec!["unexpected".to_string()];
    assert!(commit_from_proto(single_tree_label).is_err());

    let mut newline_label = base.clone();
    newline_label.root_tree_id = vec![
        cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
        cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
        cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
    ];
    newline_label.conflict_labels = vec!["a\nb".to_string(), String::new(), "c".to_string()];
    assert!(commit_from_proto(newline_label).is_err());

    let mut legacy_empty_labels = base;
    legacy_empty_labels.root_tree_id = vec![
        cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
        cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
        cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
    ];
    legacy_empty_labels.conflict_labels = vec![String::new(); 3];
    let decoded = commit_from_proto(legacy_empty_labels).expect("all-empty labels normalize");
    assert!(decoded.conflict_labels.is_resolved());
}

#[test]
fn tree_entry_from_proto_rejects_bad_lengths_and_non_empty_copy_id() {
    let bad_file = cc_common::backend::TreeEntry {
        name: "a.txt".to_string(),
        value: Some(cc_common::backend::TreeValue {
            value: Some(cc_common::backend::tree_value::Value::File(
                cc_common::backend::File {
                    id: vec![1u8; cc_common::FILE_ID_LENGTH - 1],
                    executable: false,
                    copy_id: Vec::new(),
                    size_bytes: None,
                },
            )),
        }),
    };
    assert!(tree_entry_from_proto(bad_file).is_err());

    let non_empty_copy = cc_common::backend::TreeEntry {
        name: "b.txt".to_string(),
        value: Some(cc_common::backend::TreeValue {
            value: Some(cc_common::backend::tree_value::Value::File(
                cc_common::backend::File {
                    id: vec![2u8; cc_common::FILE_ID_LENGTH],
                    executable: false,
                    copy_id: vec![9u8; 16],
                    size_bytes: None,
                },
            )),
        }),
    };
    let err = tree_entry_from_proto(non_empty_copy)
        .unwrap_err()
        .to_string();
    assert!(err.contains("copy_id"), "unexpected error: {err}");

    let valid_file = cc_common::backend::TreeEntry {
        name: "ok.txt".to_string(),
        value: Some(cc_common::backend::TreeValue {
            value: Some(cc_common::backend::tree_value::Value::File(
                cc_common::backend::File {
                    id: vec![3u8; cc_common::FILE_ID_LENGTH],
                    executable: true,
                    copy_id: Vec::new(),
                    size_bytes: Some(42),
                },
            )),
        }),
    };
    let (name, val) = tree_entry_from_proto(valid_file).unwrap();
    assert_eq!(name.as_internal_str(), "ok.txt");
    assert_eq!(
        val,
        jj_lib::backend::TreeValue::File {
            id: FileId::from_bytes(&[3u8; cc_common::FILE_ID_LENGTH]),
            executable: true,
            copy_id: CopyId::placeholder(),
        }
    );
}

#[test]
fn commit_to_proto_normalizes_unlabeled_conflict_labels_to_empty() {
    let mut commit = jj_lib::backend::Commit {
        parents: vec![CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES)],
        predecessors: Vec::new(),
        root_tree: jj_lib::merge::Merge::from_vec(vec![
            TreeId::from_bytes(&[1u8; cc_common::TREE_ID_LENGTH]),
            TreeId::from_bytes(&[2u8; cc_common::TREE_ID_LENGTH]),
            TreeId::from_bytes(&[3u8; cc_common::TREE_ID_LENGTH]),
        ]),
        conflict_labels: jj_lib::merge::Merge::from_vec(vec![
            String::new(),
            String::new(),
            String::new(),
        ]),
        change_id: ChangeId::from_bytes(&[4u8; cc_common::CHANGE_ID_LENGTH]),
        description: "unlabeled conflict".to_string(),
        author: jj_lib::backend::Signature {
            name: "Test".to_string(),
            email: "test@example.com".to_string(),
            timestamp: jj_lib::backend::Timestamp {
                timestamp: jj_lib::backend::MillisSinceEpoch(0),
                tz_offset: 0,
            },
        },
        committer: jj_lib::backend::Signature {
            name: "Test".to_string(),
            email: "test@example.com".to_string(),
            timestamp: jj_lib::backend::Timestamp {
                timestamp: jj_lib::backend::MillisSinceEpoch(0),
                tz_offset: 0,
            },
        },
        secure_sig: None,
    };
    assert!(commit_to_proto(&commit).conflict_labels.is_empty());

    commit.conflict_labels = jj_lib::merge::Merge::from_vec(vec![
        "left".to_string(),
        String::new(),
        "right".to_string(),
    ]);
    assert_eq!(
        commit_to_proto(&commit).conflict_labels,
        vec!["left".to_string(), String::new(), "right".to_string()]
    );
}

#[test]
fn test_read_file_mid_stream_not_found_maps_to_object_not_found() {
    struct MidStreamNotFoundBackend;

    #[tonic::async_trait]
    impl BackendService for MidStreamNotFoundBackend {
        async fn register_project(
            &self,
            _: Request<RegisterProjectRequest>,
        ) -> Result<Response<RegisterProjectResponse>, Status> {
            unimpl()
        }
        async fn register_repository(
            &self,
            req: Request<RegisterRepositoryRequest>,
        ) -> Result<Response<RegisterRepositoryResponse>, Status> {
            let r = req.into_inner();
            Ok(Response::new(RegisterRepositoryResponse {
                repo_id: r.repo_id.unwrap_or_else(|| "repo-mid-nf".to_string()),
                project_id: r.project_id,
            }))
        }
        async fn delete_repository(
            &self,
            _: Request<cc_common::backend::DeleteRepositoryRequest>,
        ) -> Result<Response<cc_common::backend::DeleteRepositoryResponse>, Status> {
            unimpl()
        }
        async fn read_commit(
            &self,
            _: Request<ReadCommitRequest>,
        ) -> Result<Response<ReadCommitResponse>, Status> {
            unimpl()
        }
        async fn write_commit(
            &self,
            _: Request<WriteCommitRequest>,
        ) -> Result<Response<WriteCommitResponse>, Status> {
            unimpl()
        }
        async fn list_commits(
            &self,
            _: Request<ListCommitsRequest>,
        ) -> Result<Response<ListCommitsResponse>, Status> {
            unimpl()
        }
        async fn resolve_commit_prefix(
            &self,
            _: Request<ResolveCommitPrefixRequest>,
        ) -> Result<Response<ResolveCommitPrefixResponse>, Status> {
            unimpl()
        }
        async fn resolve_change_prefix(
            &self,
            _: Request<ResolveChangePrefixRequest>,
        ) -> Result<Response<ResolveChangePrefixResponse>, Status> {
            unimpl()
        }
        async fn read_tree(
            &self,
            _: Request<ReadTreeRequest>,
        ) -> Result<Response<ReadTreeResponse>, Status> {
            unimpl()
        }
        async fn write_tree(
            &self,
            _: Request<WriteTreeRequest>,
        ) -> Result<Response<WriteTreeResponse>, Status> {
            unimpl()
        }
        type ReadFileStream =
            Pin<Box<dyn futures::Stream<Item = Result<ReadFileResponse, Status>> + Send + 'static>>;
        async fn read_file(
            &self,
            _: Request<ReadFileRequest>,
        ) -> Result<Response<Self::ReadFileStream>, Status> {
            let items: Vec<Result<ReadFileResponse, Status>> = vec![
                Ok(ReadFileResponse {
                    chunk: b"partial-prefix".to_vec(),
                }),
                Err(Status::not_found("blob missing mid-stream")),
            ];
            Ok(Response::new(Box::pin(futures::stream::iter(items))))
        }
        async fn write_file(
            &self,
            _: Request<WriteFileRequest>,
        ) -> Result<Response<WriteFileResponse>, Status> {
            unimpl()
        }
        async fn write_file_stream(
            &self,
            _: Request<Streaming<WriteFileChunk>>,
        ) -> Result<Response<WriteFileResponse>, Status> {
            unimpl()
        }
        async fn read_symlink(
            &self,
            _: Request<ReadSymlinkRequest>,
        ) -> Result<Response<ReadSymlinkResponse>, Status> {
            unimpl()
        }
        async fn write_symlink(
            &self,
            _: Request<WriteSymlinkRequest>,
        ) -> Result<Response<WriteSymlinkResponse>, Status> {
            unimpl()
        }
    }

    let url = run_async(|| async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let incoming = tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
            .map_err(|e| format!("incoming error: {e}"))?;
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(BackendServiceServer::new(MidStreamNotFoundBackend))
                .serve_with_incoming(incoming),
        );
        Ok(format!("http://{addr}"))
    })
    .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let backend = CommitCloudBackend::init_in_project(
        dir.path(),
        &url,
        None,
        Some("proj-mid-nf"),
        Some("repo-mid-nf"),
        true,
    )
    .unwrap();
    let missing_id = FileId::from_bytes(&[0xab; cc_common::FILE_ID_LENGTH]);
    let err = futures::executor::block_on(backend.read_file(RepoPath::root(), &missing_id))
        .err()
        .expect("mid-stream NOT_FOUND must fail");
    assert!(
        matches!(
            &err,
            BackendError::ObjectNotFound { object_type, hash, .. }
                if object_type == "file" && hash == &missing_id.hex()
        ),
        "expected BackendError::ObjectNotFound for file, got: {err:?}"
    );
}

struct IntegrityMockBackend {
    file_chunks: Vec<Vec<u8>>,
    file_chunks_yielded: Arc<AtomicUsize>,
    symlink_target: String,
    tree_entries: Vec<cc_common::backend::TreeEntry>,
}

#[tonic::async_trait]
impl BackendService for IntegrityMockBackend {
    async fn register_project(
        &self,
        _: Request<RegisterProjectRequest>,
    ) -> Result<Response<RegisterProjectResponse>, Status> {
        unimpl()
    }
    async fn register_repository(
        &self,
        req: Request<RegisterRepositoryRequest>,
    ) -> Result<Response<RegisterRepositoryResponse>, Status> {
        let r = req.into_inner();
        Ok(Response::new(RegisterRepositoryResponse {
            repo_id: r.repo_id.unwrap_or_else(|| "repo-integrity".to_string()),
            project_id: r.project_id,
        }))
    }
    async fn delete_repository(
        &self,
        _: Request<cc_common::backend::DeleteRepositoryRequest>,
    ) -> Result<Response<cc_common::backend::DeleteRepositoryResponse>, Status> {
        unimpl()
    }
    async fn read_commit(
        &self,
        _: Request<ReadCommitRequest>,
    ) -> Result<Response<ReadCommitResponse>, Status> {
        unimpl()
    }
    async fn write_commit(
        &self,
        _: Request<WriteCommitRequest>,
    ) -> Result<Response<WriteCommitResponse>, Status> {
        unimpl()
    }
    async fn list_commits(
        &self,
        _: Request<ListCommitsRequest>,
    ) -> Result<Response<ListCommitsResponse>, Status> {
        unimpl()
    }
    async fn resolve_commit_prefix(
        &self,
        _: Request<ResolveCommitPrefixRequest>,
    ) -> Result<Response<ResolveCommitPrefixResponse>, Status> {
        unimpl()
    }
    async fn resolve_change_prefix(
        &self,
        _: Request<ResolveChangePrefixRequest>,
    ) -> Result<Response<ResolveChangePrefixResponse>, Status> {
        unimpl()
    }
    async fn read_tree(
        &self,
        req: Request<ReadTreeRequest>,
    ) -> Result<Response<ReadTreeResponse>, Status> {
        Ok(Response::new(ReadTreeResponse {
            tree_id: req.into_inner().tree_id,
            entries: self.tree_entries.clone(),
            next_page_token: String::new(),
        }))
    }
    async fn write_tree(
        &self,
        _: Request<WriteTreeRequest>,
    ) -> Result<Response<WriteTreeResponse>, Status> {
        unimpl()
    }
    type ReadFileStream =
        Pin<Box<dyn futures::Stream<Item = Result<ReadFileResponse, Status>> + Send + 'static>>;
    async fn read_file(
        &self,
        _: Request<ReadFileRequest>,
    ) -> Result<Response<Self::ReadFileStream>, Status> {
        let chunks = self.file_chunks.clone();
        let yielded = Arc::clone(&self.file_chunks_yielded);
        let stream = stream::iter(chunks).map(move |chunk| {
            yielded.fetch_add(1, Ordering::SeqCst);
            Ok(ReadFileResponse { chunk })
        });
        Ok(Response::new(Box::pin(stream)))
    }
    async fn write_file(
        &self,
        _: Request<WriteFileRequest>,
    ) -> Result<Response<WriteFileResponse>, Status> {
        unimpl()
    }
    async fn write_file_stream(
        &self,
        _: Request<Streaming<WriteFileChunk>>,
    ) -> Result<Response<WriteFileResponse>, Status> {
        unimpl()
    }
    async fn read_symlink(
        &self,
        _: Request<ReadSymlinkRequest>,
    ) -> Result<Response<ReadSymlinkResponse>, Status> {
        Ok(Response::new(ReadSymlinkResponse {
            target: self.symlink_target.clone(),
        }))
    }
    async fn write_symlink(
        &self,
        _: Request<WriteSymlinkRequest>,
    ) -> Result<Response<WriteSymlinkResponse>, Status> {
        unimpl()
    }
}

fn spawn_integrity_mock_backend(svc: IntegrityMockBackend) -> String {
    run_async(move || async move {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let incoming = tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
            .map_err(|e| format!("incoming error: {e}"))?;
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(BackendServiceServer::new(svc))
                .serve_with_incoming(incoming),
        );
        Ok(format!("http://{addr}"))
    })
    .unwrap()
}

fn git_blob_sha1(content: &[u8]) -> Vec<u8> {
    let mut hasher = Sha1::new();
    hasher.update(format!("blob {}\0", content.len()).as_bytes());
    hasher.update(content);
    hasher.finalize().to_vec()
}

#[test]
fn test_read_file_rejects_oversized_stream_and_digest_mismatch() {
    let chunks_yielded = Arc::new(AtomicUsize::new(0));
    let url = spawn_integrity_mock_backend(IntegrityMockBackend {
        file_chunks: vec![b"0123456789".to_vec(), b"abcdefghij".to_vec()],
        file_chunks_yielded: Arc::clone(&chunks_yielded),
        symlink_target: String::new(),
        tree_entries: Vec::new(),
    });
    let backend = CommitCloudBackend::from_config(CommitCloudConfig {
        server_url: url,
        repo_id: "repo-file-check".to_string(),
        project_id: Some("proj-file-check".to_string()),
    });

    let payload = b"0123456789abcdefghij";
    let valid_id = FileId::from_bytes(&git_blob_sha1(payload));
    let wrong_id = FileId::from_bytes(&[0xde; cc_common::FILE_ID_LENGTH]);

    futures::executor::block_on(async {
        // Oversized stream rejected before buffering past limit.
        let oversized_err = backend
            .read_file_with_max_bytes(&valid_id, 15)
            .await
            .expect_err("file stream exceeding max_bytes must be rejected");
        assert!(
            matches!(oversized_err, BackendError::Other(_))
                && oversized_err
                    .to_string()
                    .contains("exceeded maximum allowed size"),
            "unexpected oversized error: {oversized_err:?}"
        );

        // Digest mismatch rejected with BackendError::Other.
        let mismatch_err = backend
            .read_file(RepoPath::root(), &wrong_id)
            .await
            .err()
            .expect("file digest mismatch must be rejected");
        assert!(
            matches!(mismatch_err, BackendError::Other(_))
                && mismatch_err.to_string().contains("digest mismatch"),
            "unexpected mismatch error: {mismatch_err:?}"
        );

        // Matching canonical Git blob SHA-1 succeeds.
        let mut reader = backend
            .read_file(RepoPath::root(), &valid_id)
            .await
            .unwrap();
        let mut read_back = Vec::new();
        reader.read_to_end(&mut read_back).await.unwrap();
        assert_eq!(read_back, payload);
    });
}

#[test]
fn test_write_file_rejects_oversized_input_before_rpc() {
    let stream_attempts = Arc::new(AtomicUsize::new(0));
    let unary_writes = Arc::new(AtomicUsize::new(0));
    let sa = Arc::clone(&stream_attempts);
    let uw = Arc::clone(&unary_writes);

    let url = run_async(move || async move {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let incoming = tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
            .map_err(|e| format!("incoming error: {e}"))?;
        let svc = UnaryOnlyBackendServer {
            stream_attempts: sa,
            unary_writes: uw,
            expected_project_id: "proj-write-cap".to_string(),
        };
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(BackendServiceServer::new(svc))
                .serve_with_incoming(incoming),
        );
        Ok(format!("http://{addr}"))
    })
    .unwrap();

    let backend = CommitCloudBackend::from_config(CommitCloudConfig {
        server_url: url,
        repo_id: "repo-write-cap".to_string(),
        project_id: Some("proj-write-cap".to_string()),
    });

    futures::executor::block_on(async {
        // Input exceeding max_bytes fails immediately without issuing any RPC.
        let mut oversized = futures::io::Cursor::new(vec![b'x'; 16]);
        let err = backend
            .write_file_streaming_with_max_bytes(RepoPath::root(), &mut oversized, 15)
            .await
            .expect_err("file upload exceeding max_bytes must be rejected before RPC");
        assert!(
            matches!(err, BackendError::Other(_))
                && err.to_string().contains("exceeded maximum allowed size"),
            "unexpected oversized write error: {err:?}"
        );
        assert_eq!(stream_attempts.load(Ordering::SeqCst), 0);
        assert_eq!(unary_writes.load(Ordering::SeqCst), 0);

        // Multi-chunk input exceeding max_bytes stops before buffering past the cap.
        let mut oversized_multi =
            futures::io::Cursor::new(vec![b'y'; cc_common::FILE_STREAM_CHUNK_SIZE + 1]);
        let multi_err = backend
            .write_file_streaming_with_max_bytes(
                RepoPath::root(),
                &mut oversized_multi,
                cc_common::FILE_STREAM_CHUNK_SIZE,
            )
            .await
            .expect_err("multi-chunk upload exceeding max_bytes must fail");
        assert!(
            matches!(multi_err, BackendError::Other(_))
                && multi_err
                    .to_string()
                    .contains("exceeded maximum allowed size"),
            "unexpected multi-chunk write error: {multi_err:?}"
        );
        assert_eq!(stream_attempts.load(Ordering::SeqCst), 0);
        assert_eq!(unary_writes.load(Ordering::SeqCst), 0);

        // Input exactly matching max_bytes succeeds.
        let exact_payload = b"0123456789abcde".to_vec();
        let mut exact = futures::io::Cursor::new(exact_payload.clone());
        let file_id = backend
            .write_file_streaming_with_max_bytes(RepoPath::root(), &mut exact, exact_payload.len())
            .await
            .unwrap();
        assert_eq!(file_id, FileId::from_bytes(&git_blob_sha1(&exact_payload)));
        assert_eq!(unary_writes.load(Ordering::SeqCst), 1);
    });
}

#[test]
fn test_read_symlink_rejects_digest_mismatch_and_embedded_nul() {
    // Valid symlink target passes when digest matches, fails on digest mismatch.
    let valid_target = "relative/target.txt";
    let valid_url = spawn_integrity_mock_backend(IntegrityMockBackend {
        file_chunks: Vec::new(),
        file_chunks_yielded: Arc::new(AtomicUsize::new(0)),
        symlink_target: valid_target.to_string(),
        tree_entries: Vec::new(),
    });
    let valid_backend = CommitCloudBackend::from_config(CommitCloudConfig {
        server_url: valid_url,
        repo_id: "repo-sym".to_string(),
        project_id: Some("proj-sym".to_string()),
    });
    let valid_id = SymlinkId::from_bytes(&git_blob_sha1(valid_target.as_bytes()));
    let wrong_id = SymlinkId::from_bytes(&[0xef; cc_common::SYMLINK_ID_LENGTH]);

    futures::executor::block_on(async {
        assert_eq!(
            valid_backend
                .read_symlink(RepoPath::root(), &valid_id)
                .await
                .unwrap(),
            valid_target
        );
        let mismatch_err = valid_backend
            .read_symlink(RepoPath::root(), &wrong_id)
            .await
            .expect_err("symlink digest mismatch must be rejected");
        assert!(
            matches!(mismatch_err, BackendError::Other(_))
                && mismatch_err.to_string().contains("digest mismatch"),
            "unexpected symlink mismatch error: {mismatch_err:?}"
        );
    });

    // Embedded NUL byte is rejected even when the Git blob SHA-1 matches.
    let nul_target = "dir/\0evil";
    let nul_id = SymlinkId::from_bytes(&git_blob_sha1(nul_target.as_bytes()));
    let nul_url = spawn_integrity_mock_backend(IntegrityMockBackend {
        file_chunks: Vec::new(),
        file_chunks_yielded: Arc::new(AtomicUsize::new(0)),
        symlink_target: nul_target.to_string(),
        tree_entries: Vec::new(),
    });
    let nul_backend = CommitCloudBackend::from_config(CommitCloudConfig {
        server_url: nul_url,
        repo_id: "repo-sym-nul".to_string(),
        project_id: Some("proj-sym-nul".to_string()),
    });
    futures::executor::block_on(async {
        let nul_err = nul_backend
            .read_symlink(RepoPath::root(), &nul_id)
            .await
            .expect_err("symlink target containing NUL must be rejected");
        assert!(
            matches!(nul_err, BackendError::Other(_)) && nul_err.to_string().contains("NUL"),
            "unexpected symlink NUL error: {nul_err:?}"
        );
    });
}

#[test]
fn test_read_tree_verifies_canonical_git_tree_digest_and_preserves_jj_order() {
    // Include directory `foo` alongside file `foo-bar`: in Git tree sort order, `foo-bar`
    // (`'-' = 0x2d`) sorts BEFORE `foo/` (`'/' = 0x2f`), whereas `RepoPathComponent` byte
    // order sorts `foo` BEFORE `foo-bar`.
    let dir_oid = [0x11u8; cc_common::TREE_ID_LENGTH];
    let file_oid = [0x22u8; cc_common::FILE_ID_LENGTH];
    let exec_oid = [0x33u8; cc_common::FILE_ID_LENGTH];
    let link_oid = [0x44u8; cc_common::SYMLINK_ID_LENGTH];

    let proto_entries = vec![
        cc_common::backend::TreeEntry {
            name: "foo".to_string(),
            value: Some(cc_common::backend::TreeValue {
                value: Some(cc_common::backend::tree_value::Value::TreeId(
                    dir_oid.to_vec(),
                )),
            }),
        },
        cc_common::backend::TreeEntry {
            name: "foo-bar".to_string(),
            value: Some(cc_common::backend::TreeValue {
                value: Some(cc_common::backend::tree_value::Value::File(
                    cc_common::backend::File {
                        id: file_oid.to_vec(),
                        executable: false,
                        copy_id: Vec::new(),
                        size_bytes: None,
                    },
                )),
            }),
        },
        cc_common::backend::TreeEntry {
            name: "run.sh".to_string(),
            value: Some(cc_common::backend::TreeValue {
                value: Some(cc_common::backend::tree_value::Value::File(
                    cc_common::backend::File {
                        id: exec_oid.to_vec(),
                        executable: true,
                        copy_id: Vec::new(),
                        size_bytes: None,
                    },
                )),
            }),
        },
        cc_common::backend::TreeEntry {
            name: "sym".to_string(),
            value: Some(cc_common::backend::TreeValue {
                value: Some(cc_common::backend::tree_value::Value::SymlinkId(
                    link_oid.to_vec(),
                )),
            }),
        },
    ];

    // Manually construct raw canonical Git tree payload in Git's trailing-`/` sort order:
    // `foo-bar` (100644), `foo` (40000), `run.sh` (100755), `sym` (120000).
    let mut raw_tree = Vec::new();
    raw_tree.extend_from_slice(b"100644 foo-bar\0");
    raw_tree.extend_from_slice(&file_oid);
    raw_tree.extend_from_slice(b"40000 foo\0");
    raw_tree.extend_from_slice(&dir_oid);
    raw_tree.extend_from_slice(b"100755 run.sh\0");
    raw_tree.extend_from_slice(&exec_oid);
    raw_tree.extend_from_slice(b"120000 sym\0");
    raw_tree.extend_from_slice(&link_oid);

    let mut hasher = Sha1::new();
    hasher.update(format!("tree {}\0", raw_tree.len()).as_bytes());
    hasher.update(&raw_tree);
    let expected_tree_id = TreeId::from_bytes(&hasher.finalize());
    let wrong_tree_id = TreeId::from_bytes(&[0xaa; cc_common::TREE_ID_LENGTH]);

    let url = spawn_integrity_mock_backend(IntegrityMockBackend {
        file_chunks: Vec::new(),
        file_chunks_yielded: Arc::new(AtomicUsize::new(0)),
        symlink_target: String::new(),
        tree_entries: proto_entries,
    });
    let backend = CommitCloudBackend::from_config(CommitCloudConfig {
        server_url: url,
        repo_id: "repo-tree-check".to_string(),
        project_id: Some("proj-tree-check".to_string()),
    });

    futures::executor::block_on(async {
        // Valid canonical Git tree SHA-1 passes and preserves `RepoPathComponent` ordering.
        let tree = backend
            .read_tree(RepoPath::root(), &expected_tree_id)
            .await
            .unwrap();
        let names: Vec<&str> = tree.entries().map(|e| e.name().as_internal_str()).collect();
        assert_eq!(names, vec!["foo", "foo-bar", "run.sh", "sym"]);

        // Mismatched TreeId is rejected with BackendError::Other.
        let mismatch_err = backend
            .read_tree(RepoPath::root(), &wrong_tree_id)
            .await
            .expect_err("tree digest mismatch must be rejected");
        assert!(
            matches!(mismatch_err, BackendError::Other(_))
                && mismatch_err.to_string().contains("digest mismatch"),
            "unexpected tree mismatch error: {mismatch_err:?}"
        );
    });
}
