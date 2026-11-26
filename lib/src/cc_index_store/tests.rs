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

use super::*;

#[test]
fn test_parents_first_order_handles_deep_chain_without_stack_overflow() {
    const DEPTH: usize = 20_000;
    let make_id = |idx: usize| {
        let mut b = [0u8; 20];
        b[..8].copy_from_slice(&(idx as u64 + 1).to_be_bytes());
        CommitId::from_bytes(&b)
    };

    let mut parents = HashMap::with_capacity(DEPTH);
    let root = CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES);
    for idx in 0..DEPTH {
        let cid = make_id(idx);
        let parent = if idx == 0 {
            root.clone()
        } else {
            make_id(idx - 1)
        };
        parents.insert(cid, vec![parent]);
    }

    let ordered = parents_first_order(&parents).unwrap();
    assert_eq!(ordered.len(), DEPTH);
    for (idx, cid) in ordered.iter().enumerate() {
        assert_eq!(cid, &make_id(idx));
    }
}

#[test]
fn test_extract_cli_commit_hints_parses_exact_full_commit_ids() {
    let cid1 = "0123456789abcdef0123456789abcdef01234567";
    let cid2 = "fedcba9876543210fedcba9876543210fedcba98";
    let too_short = "0123456789abcdef";
    let too_long = "0123456789abcdef0123456789abcdef0123456789";

    let hints = extract_cli_commit_hints([
        "jj",
        "new",
        cid1,
        &format!("present({cid2})|{cid1}"),
        too_short,
        too_long,
    ]);
    assert_eq!(
        hints,
        vec![CommitId::from_hex(cid1), CommitId::from_hex(cid2)]
    );
}

#[test]
fn test_is_sync_unavailable_recognizes_transient_and_unimplemented_statuses() {
    let unimpl = BackendError::Other(Box::new(tonic::Status::unimplemented("no ListCommits")));
    let unavail = BackendError::Other(Box::new(tonic::Status::unavailable("server restarting")));
    let deadline = tonic::Status::deadline_exceeded("timed out");
    let auth = BackendError::Other(Box::new(tonic::Status::unauthenticated("bad token")));

    assert!(is_sync_unavailable(&unimpl));
    assert!(is_sync_unavailable(&unavail));
    assert!(is_sync_unavailable(&deadline));
    assert!(!is_sync_unavailable(&auth));
}

#[test]
fn test_parse_env_bool() {
    assert_eq!(parse_env_bool("0"), Some(false));
    assert_eq!(parse_env_bool("false"), Some(false));
    assert_eq!(parse_env_bool("OFF"), Some(false));
    assert_eq!(parse_env_bool("1"), Some(true));
    assert_eq!(parse_env_bool("true"), Some(true));
    assert_eq!(parse_env_bool("invalid"), None);
}

struct MockListCommitsBackend {
    commit_ids: Vec<Vec<u8>>,
    next_page_token: Vec<u8>,
}

#[tonic::async_trait]
impl cc_common::backend::backend_service_server::BackendService for MockListCommitsBackend {
    async fn register_project(
        &self,
        _: tonic::Request<cc_common::backend::RegisterProjectRequest>,
    ) -> Result<tonic::Response<cc_common::backend::RegisterProjectResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }
    async fn register_repository(
        &self,
        _: tonic::Request<cc_common::backend::RegisterRepositoryRequest>,
    ) -> Result<tonic::Response<cc_common::backend::RegisterRepositoryResponse>, tonic::Status>
    {
        Err(tonic::Status::unimplemented("unused"))
    }
    async fn read_commit(
        &self,
        _: tonic::Request<cc_common::backend::ReadCommitRequest>,
    ) -> Result<tonic::Response<cc_common::backend::ReadCommitResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }
    async fn write_commit(
        &self,
        _: tonic::Request<cc_common::backend::WriteCommitRequest>,
    ) -> Result<tonic::Response<cc_common::backend::WriteCommitResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }
    async fn list_commits(
        &self,
        _: tonic::Request<cc_common::backend::ListCommitsRequest>,
    ) -> Result<tonic::Response<cc_common::backend::ListCommitsResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            cc_common::backend::ListCommitsResponse {
                commit_ids: self.commit_ids.clone(),
                next_page_token: self.next_page_token.clone(),
                sync_token: Vec::new(),
            },
        ))
    }
    async fn resolve_commit_prefix(
        &self,
        _: tonic::Request<cc_common::backend::ResolveCommitPrefixRequest>,
    ) -> Result<tonic::Response<cc_common::backend::ResolveCommitPrefixResponse>, tonic::Status>
    {
        Err(tonic::Status::unimplemented("unused"))
    }
    async fn resolve_change_prefix(
        &self,
        _: tonic::Request<cc_common::backend::ResolveChangePrefixRequest>,
    ) -> Result<tonic::Response<cc_common::backend::ResolveChangePrefixResponse>, tonic::Status>
    {
        Err(tonic::Status::unimplemented("unused"))
    }
    async fn read_tree(
        &self,
        _: tonic::Request<cc_common::backend::ReadTreeRequest>,
    ) -> Result<tonic::Response<cc_common::backend::ReadTreeResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }
    async fn write_tree(
        &self,
        _: tonic::Request<cc_common::backend::WriteTreeRequest>,
    ) -> Result<tonic::Response<cc_common::backend::WriteTreeResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }
    type ReadFileStream = std::pin::Pin<
        Box<
            dyn futures::Stream<Item = Result<cc_common::backend::ReadFileResponse, tonic::Status>>
                + Send
                + 'static,
        >,
    >;
    async fn read_file(
        &self,
        _: tonic::Request<cc_common::backend::ReadFileRequest>,
    ) -> Result<tonic::Response<Self::ReadFileStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }
    async fn write_file(
        &self,
        _: tonic::Request<cc_common::backend::WriteFileRequest>,
    ) -> Result<tonic::Response<cc_common::backend::WriteFileResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }
    async fn write_file_stream(
        &self,
        _: tonic::Request<tonic::Streaming<cc_common::backend::WriteFileChunk>>,
    ) -> Result<tonic::Response<cc_common::backend::WriteFileResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }
    async fn read_symlink(
        &self,
        _: tonic::Request<cc_common::backend::ReadSymlinkRequest>,
    ) -> Result<tonic::Response<cc_common::backend::ReadSymlinkResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }
    async fn write_symlink(
        &self,
        _: tonic::Request<cc_common::backend::WriteSymlinkRequest>,
    ) -> Result<tonic::Response<cc_common::backend::WriteSymlinkResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }
}

async fn spawn_mock_list_commits_server(svc: MockListCommitsBackend) -> String {
    crate::util::run_on_tokio(async move {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let incoming = tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
            .map_err(|e| format!("incoming error: {e}"))?;
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(
                    cc_common::backend::backend_service_server::BackendServiceServer::new(svc),
                )
                .serve_with_incoming(incoming),
        );
        Ok(format!("http://{addr}"))
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn test_list_project_commits_rejects_stuck_pagination_cursor() {
    let url = spawn_mock_list_commits_server(MockListCommitsBackend {
        commit_ids: vec![vec![0x11; cc_common::COMMIT_ID_LENGTH]],
        next_page_token: b"stuck-cursor".to_vec(),
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let err = list_project_commits(
        url,
        "proj-1".to_string(),
        Vec::new(),
        dir.path().join(SYNC_CURSOR_FILENAME),
        Arc::new(Mutex::new(())),
    )
    .await
    .expect_err("stuck pagination cursor must return error instead of looping forever");
    assert!(err.to_string().contains("failed to advance"));
}

#[tokio::test]
async fn test_list_project_commits_rejects_malformed_commit_id_length() {
    let url = spawn_mock_list_commits_server(MockListCommitsBackend {
        commit_ids: vec![vec![0xaa; 7]],
        next_page_token: Vec::new(),
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let err = list_project_commits(
        url,
        "proj-1".to_string(),
        Vec::new(),
        dir.path().join(SYNC_CURSOR_FILENAME),
        Arc::new(Mutex::new(())),
    )
    .await
    .expect_err("malformed commit_id length must be rejected");
    assert!(err.to_string().contains("invalid commit_id length"));
}

#[tokio::test]
async fn test_list_project_commits_rejects_empty_page_with_next_page_token() {
    let url = spawn_mock_list_commits_server(MockListCommitsBackend {
        commit_ids: Vec::new(),
        next_page_token: b"next-page".to_vec(),
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let err = list_project_commits(
        url,
        "proj-1".to_string(),
        Vec::new(),
        dir.path().join(SYNC_CURSOR_FILENAME),
        Arc::new(Mutex::new(())),
    )
    .await
    .expect_err("empty page with non-empty next_page_token must return error");
    assert!(
        err.to_string().contains("empty page"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_list_project_commits_enforces_max_page_cap() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct InfinitePagesBackend {
        pages_served: Arc<AtomicUsize>,
    }

    #[tonic::async_trait]
    impl cc_common::backend::backend_service_server::BackendService for InfinitePagesBackend {
        async fn register_project(
            &self,
            _: tonic::Request<cc_common::backend::RegisterProjectRequest>,
        ) -> Result<tonic::Response<cc_common::backend::RegisterProjectResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("unused"))
        }
        async fn register_repository(
            &self,
            _: tonic::Request<cc_common::backend::RegisterRepositoryRequest>,
        ) -> Result<tonic::Response<cc_common::backend::RegisterRepositoryResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("unused"))
        }
        async fn read_commit(
            &self,
            _: tonic::Request<cc_common::backend::ReadCommitRequest>,
        ) -> Result<tonic::Response<cc_common::backend::ReadCommitResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("unused"))
        }
        async fn write_commit(
            &self,
            _: tonic::Request<cc_common::backend::WriteCommitRequest>,
        ) -> Result<tonic::Response<cc_common::backend::WriteCommitResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("unused"))
        }
        async fn list_commits(
            &self,
            _: tonic::Request<cc_common::backend::ListCommitsRequest>,
        ) -> Result<tonic::Response<cc_common::backend::ListCommitsResponse>, tonic::Status>
        {
            let page = self.pages_served.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(tonic::Response::new(
                cc_common::backend::ListCommitsResponse {
                    commit_ids: vec![vec![0x11; cc_common::COMMIT_ID_LENGTH]],
                    next_page_token: format!("page-{page}").into_bytes(),
                    sync_token: Vec::new(),
                },
            ))
        }
        async fn resolve_commit_prefix(
            &self,
            _: tonic::Request<cc_common::backend::ResolveCommitPrefixRequest>,
        ) -> Result<tonic::Response<cc_common::backend::ResolveCommitPrefixResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("unused"))
        }
        async fn resolve_change_prefix(
            &self,
            _: tonic::Request<cc_common::backend::ResolveChangePrefixRequest>,
        ) -> Result<tonic::Response<cc_common::backend::ResolveChangePrefixResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("unused"))
        }
        async fn read_tree(
            &self,
            _: tonic::Request<cc_common::backend::ReadTreeRequest>,
        ) -> Result<tonic::Response<cc_common::backend::ReadTreeResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("unused"))
        }
        async fn write_tree(
            &self,
            _: tonic::Request<cc_common::backend::WriteTreeRequest>,
        ) -> Result<tonic::Response<cc_common::backend::WriteTreeResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("unused"))
        }
        type ReadFileStream = std::pin::Pin<
            Box<
                dyn futures::Stream<
                        Item = Result<cc_common::backend::ReadFileResponse, tonic::Status>,
                    > + Send
                    + 'static,
            >,
        >;
        async fn read_file(
            &self,
            _: tonic::Request<cc_common::backend::ReadFileRequest>,
        ) -> Result<tonic::Response<Self::ReadFileStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("unused"))
        }
        async fn write_file(
            &self,
            _: tonic::Request<cc_common::backend::WriteFileRequest>,
        ) -> Result<tonic::Response<cc_common::backend::WriteFileResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("unused"))
        }
        async fn write_file_stream(
            &self,
            _: tonic::Request<tonic::Streaming<cc_common::backend::WriteFileChunk>>,
        ) -> Result<tonic::Response<cc_common::backend::WriteFileResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("unused"))
        }
        async fn read_symlink(
            &self,
            _: tonic::Request<cc_common::backend::ReadSymlinkRequest>,
        ) -> Result<tonic::Response<cc_common::backend::ReadSymlinkResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("unused"))
        }
        async fn write_symlink(
            &self,
            _: tonic::Request<cc_common::backend::WriteSymlinkRequest>,
        ) -> Result<tonic::Response<cc_common::backend::WriteSymlinkResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("unused"))
        }
    }

    let pages_served = Arc::new(AtomicUsize::new(0));
    let pages_for_svc = Arc::clone(&pages_served);
    let url = crate::util::run_on_tokio(async move {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let incoming = tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
            .map_err(|e| format!("incoming error: {e}"))?;
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(
                    cc_common::backend::backend_service_server::BackendServiceServer::new(
                        InfinitePagesBackend {
                            pages_served: pages_for_svc,
                        },
                    ),
                )
                .serve_with_incoming(incoming),
        );
        Ok(format!("http://{addr}"))
    })
    .await
    .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let err = list_project_commits_with_max_pages(
        url,
        "proj-cap".to_string(),
        Vec::new(),
        dir.path().join(SYNC_CURSOR_FILENAME),
        Arc::new(Mutex::new(())),
        3,
    )
    .await
    .expect_err("unbounded distinct pagination cursors must hit max page cap");
    assert!(
        err.to_string().contains("exceeded maximum of 3 pages"),
        "unexpected error: {err}"
    );
    assert_eq!(pages_served.load(Ordering::SeqCst), 3);
}

struct DynamicListCommitsState {
    commits: HashMap<Vec<u8>, cc_common::backend::Commit>,
    log: Vec<(u64, Vec<u8>)>,
    reject_since_with_unimplemented: bool,
    reject_all_list_with_unimplemented: bool,
    recorded_since_tokens: Vec<Vec<u8>>,
}

struct DynamicListCommitsBackend {
    state: Arc<Mutex<DynamicListCommitsState>>,
    read_commit_calls: Arc<std::sync::atomic::AtomicUsize>,
}

#[tonic::async_trait]
impl cc_common::backend::backend_service_server::BackendService for DynamicListCommitsBackend {
    async fn register_project(
        &self,
        _: tonic::Request<cc_common::backend::RegisterProjectRequest>,
    ) -> Result<tonic::Response<cc_common::backend::RegisterProjectResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }
    async fn register_repository(
        &self,
        _: tonic::Request<cc_common::backend::RegisterRepositoryRequest>,
    ) -> Result<tonic::Response<cc_common::backend::RegisterRepositoryResponse>, tonic::Status>
    {
        Err(tonic::Status::unimplemented("unused"))
    }
    async fn read_commit(
        &self,
        req: tonic::Request<cc_common::backend::ReadCommitRequest>,
    ) -> Result<tonic::Response<cc_common::backend::ReadCommitResponse>, tonic::Status> {
        self.read_commit_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let cid = req.into_inner().commit_id;
        let guard = self.state.lock().unwrap();
        match guard.commits.get(&cid) {
            Some(commit) => Ok(tonic::Response::new(
                cc_common::backend::ReadCommitResponse {
                    commit: Some(commit.clone()),
                },
            )),
            None => Err(tonic::Status::not_found("commit not found")),
        }
    }
    async fn write_commit(
        &self,
        _: tonic::Request<cc_common::backend::WriteCommitRequest>,
    ) -> Result<tonic::Response<cc_common::backend::WriteCommitResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }
    async fn list_commits(
        &self,
        req: tonic::Request<cc_common::backend::ListCommitsRequest>,
    ) -> Result<tonic::Response<cc_common::backend::ListCommitsResponse>, tonic::Status> {
        let r = req.into_inner();
        let mut guard = self.state.lock().unwrap();
        guard.recorded_since_tokens.push(r.since_token.clone());
        if guard.reject_all_list_with_unimplemented {
            return Err(tonic::Status::unimplemented("ListCommits unsupported"));
        }
        if !r.since_token.is_empty() && guard.reject_since_with_unimplemented {
            return Err(tonic::Status::unimplemented("since_token unsupported"));
        }
        let since_seq = if r.since_token.is_empty() {
            0u64
        } else {
            let bytes: [u8; 8] = r
                .since_token
                .as_slice()
                .try_into()
                .map_err(|_| tonic::Status::invalid_argument("invalid since_token"))?;
            u64::from_be_bytes(bytes)
        };
        let commit_ids: Vec<Vec<u8>> = guard
            .log
            .iter()
            .filter(|(seq, _)| *seq > since_seq)
            .map(|(_, id)| id.clone())
            .collect();
        let sync_token = if guard.reject_since_with_unimplemented {
            Vec::new()
        } else {
            guard
                .log
                .last()
                .map(|(seq, _)| seq.to_be_bytes().to_vec())
                .unwrap_or_default()
        };
        Ok(tonic::Response::new(
            cc_common::backend::ListCommitsResponse {
                commit_ids,
                next_page_token: Vec::new(),
                sync_token,
            },
        ))
    }
    async fn resolve_commit_prefix(
        &self,
        _: tonic::Request<cc_common::backend::ResolveCommitPrefixRequest>,
    ) -> Result<tonic::Response<cc_common::backend::ResolveCommitPrefixResponse>, tonic::Status>
    {
        Err(tonic::Status::unimplemented("unused"))
    }
    async fn resolve_change_prefix(
        &self,
        _: tonic::Request<cc_common::backend::ResolveChangePrefixRequest>,
    ) -> Result<tonic::Response<cc_common::backend::ResolveChangePrefixResponse>, tonic::Status>
    {
        Err(tonic::Status::unimplemented("unused"))
    }
    async fn read_tree(
        &self,
        _: tonic::Request<cc_common::backend::ReadTreeRequest>,
    ) -> Result<tonic::Response<cc_common::backend::ReadTreeResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }
    async fn write_tree(
        &self,
        _: tonic::Request<cc_common::backend::WriteTreeRequest>,
    ) -> Result<tonic::Response<cc_common::backend::WriteTreeResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }
    type ReadFileStream = std::pin::Pin<
        Box<
            dyn futures::Stream<Item = Result<cc_common::backend::ReadFileResponse, tonic::Status>>
                + Send
                + 'static,
        >,
    >;
    async fn read_file(
        &self,
        _: tonic::Request<cc_common::backend::ReadFileRequest>,
    ) -> Result<tonic::Response<Self::ReadFileStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }
    async fn write_file(
        &self,
        _: tonic::Request<cc_common::backend::WriteFileRequest>,
    ) -> Result<tonic::Response<cc_common::backend::WriteFileResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }
    async fn write_file_stream(
        &self,
        _: tonic::Request<tonic::Streaming<cc_common::backend::WriteFileChunk>>,
    ) -> Result<tonic::Response<cc_common::backend::WriteFileResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }
    async fn read_symlink(
        &self,
        _: tonic::Request<cc_common::backend::ReadSymlinkRequest>,
    ) -> Result<tonic::Response<cc_common::backend::ReadSymlinkResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }
    async fn write_symlink(
        &self,
        _: tonic::Request<cc_common::backend::WriteSymlinkRequest>,
    ) -> Result<tonic::Response<cc_common::backend::WriteSymlinkResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }
}

async fn spawn_dynamic_list_commits_server(state: Arc<Mutex<DynamicListCommitsState>>) -> String {
    spawn_dynamic_list_commits_server_with_counter(state)
        .await
        .0
}

async fn spawn_dynamic_list_commits_server_with_counter(
    state: Arc<Mutex<DynamicListCommitsState>>,
) -> (String, Arc<std::sync::atomic::AtomicUsize>) {
    let read_commit_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter_bg = Arc::clone(&read_commit_calls);
    let url = crate::util::run_on_tokio(async move {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let incoming = tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
            .map_err(|e| format!("incoming error: {e}"))?;
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(
                    cc_common::backend::backend_service_server::BackendServiceServer::new(
                        DynamicListCommitsBackend {
                            state,
                            read_commit_calls: counter_bg,
                        },
                    ),
                )
                .serve_with_incoming(incoming),
        );
        Ok(format!("http://{addr}"))
    })
    .await
    .unwrap();
    (url, read_commit_calls)
}

#[tokio::test]
async fn test_list_project_commits_falls_back_to_full_scan_on_since_token_unimplemented() {
    let cid = vec![0x22; cc_common::COMMIT_ID_LENGTH];
    let state = Arc::new(Mutex::new(DynamicListCommitsState {
        commits: HashMap::new(),
        log: vec![(1, cid.clone())],
        reject_since_with_unimplemented: true,
        reject_all_list_with_unimplemented: false,
        recorded_since_tokens: Vec::new(),
    }));
    let url = spawn_dynamic_list_commits_server(Arc::clone(&state)).await;
    let dir = tempfile::tempdir().unwrap();
    let cursor_path = dir.path().join(SYNC_CURSOR_FILENAME);
    fs::write(&cursor_path, b"stale-cursor").unwrap();

    let cursor_lock = Arc::new(Mutex::new(()));
    let (ids, token, cursor_reset) = list_project_commits(
        url.clone(),
        "proj-1".to_string(),
        1u64.to_be_bytes().to_vec(),
        cursor_path.clone(),
        Arc::clone(&cursor_lock),
    )
    .await
    .expect("UNIMPLEMENTED on since_token must fall back to full scan");
    assert!(cursor_reset);
    assert!(token.is_empty());
    assert_eq!(ids, vec![CommitId::from_bytes(&cid)]);
    assert!(
        !cursor_path.exists(),
        "saved cursor file must be cleared when falling back from UNIMPLEMENTED"
    );

    // When even full-scan ListCommits is UNIMPLEMENTED, clear cursor and return UNIMPLEMENTED.
    state.lock().unwrap().reject_all_list_with_unimplemented = true;
    fs::write(&cursor_path, b"stale-cursor").unwrap();
    let err = list_project_commits(
        url,
        "proj-1".to_string(),
        1u64.to_be_bytes().to_vec(),
        cursor_path.clone(),
        cursor_lock,
    )
    .await
    .expect_err("full-scan UNIMPLEMENTED should surface as error for keep_local_index_or_fail");
    assert!(is_sync_unavailable(err.as_ref()));
    assert!(!cursor_path.exists());
}

fn make_verified_proto_commit(
    parent_ids: &[CommitId],
    change_id_byte: u8,
    description: &str,
) -> (CommitId, cc_common::backend::Commit) {
    use gix::objs::WriteTo as _;

    let change_id = [change_id_byte; cc_common::CHANGE_ID_LENGTH];
    let gix_parents: Vec<gix::hash::ObjectId> = parent_ids
        .iter()
        .filter(|id| id.as_bytes() != cc_common::ROOT_COMMIT_ID_BYTES)
        .map(|id| gix::hash::ObjectId::try_from(id.as_bytes()).unwrap())
        .collect();
    let raw_gix = gix::objs::Commit {
        tree: gix::hash::ObjectId::empty_tree(gix::hash::Kind::Sha1),
        parents: gix_parents.into(),
        author: gix::actor::Signature {
            name: "Test".into(),
            email: "test@example.com".into(),
            time: gix::date::Time::new(1_700_000_000, 0),
        },
        committer: gix::actor::Signature {
            name: "Test".into(),
            email: "test@example.com".into(),
            time: gix::date::Time::new(1_700_000_000, 0),
        },
        encoding: None,
        extra_headers: vec![(
            "change-id".into(),
            jj_lib::hex_util::encode_reverse_hex(&change_id).into(),
        )],
        message: description.as_bytes().into(),
    };
    let mut raw_bytes = Vec::new();
    raw_gix.write_to(&mut raw_bytes).unwrap();
    let hash = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Commit, &raw_bytes)
        .unwrap();
    let commit_id = CommitId::from_bytes(hash.as_bytes());
    let sig = cc_common::backend::Signature {
        name: "Test".to_string(),
        email: "test@example.com".to_string(),
        timestamp: Some(cc_common::backend::Timestamp {
            millis_since_epoch: 1_700_000_000_000,
            tz_offset: 0,
        }),
    };
    let proto = cc_common::backend::Commit {
        commit_id: commit_id.as_bytes().to_vec(),
        change_id: change_id.to_vec(),
        parent_commit_ids: parent_ids.iter().map(|p| p.as_bytes().to_vec()).collect(),
        root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
        description: description.to_string(),
        author: Some(sig.clone()),
        committer: Some(sig),
        predecessors: Vec::new(),
        conflict_labels: Vec::new(),
        secure_sig: None,
        raw_git_commit: raw_bytes,
        external_ids: Default::default(),
    };
    (commit_id, proto)
}

#[derive(Debug)]
struct InMemoryOpStore {
    root_op_id: jj_lib::op_store::OperationId,
    ops: HashMap<jj_lib::op_store::OperationId, jj_lib::op_store::Operation>,
    views: HashMap<jj_lib::op_store::ViewId, jj_lib::op_store::View>,
}

#[async_trait]
impl jj_lib::op_store::OpStore for InMemoryOpStore {
    fn name(&self) -> &str {
        "in_memory_test_op_store"
    }
    fn root_operation_id(&self) -> &jj_lib::op_store::OperationId {
        &self.root_op_id
    }
    async fn read_view(
        &self,
        id: &jj_lib::op_store::ViewId,
    ) -> jj_lib::op_store::OpStoreResult<jj_lib::op_store::View> {
        self.views
            .get(id)
            .cloned()
            .ok_or_else(|| jj_lib::op_store::OpStoreError::Other("missing view".into()))
    }
    async fn write_view(
        &self,
        _: &jj_lib::op_store::View,
    ) -> jj_lib::op_store::OpStoreResult<jj_lib::op_store::ViewId> {
        unreachable!("unused in test")
    }
    async fn read_operation(
        &self,
        id: &jj_lib::op_store::OperationId,
    ) -> jj_lib::op_store::OpStoreResult<jj_lib::op_store::Operation> {
        self.ops
            .get(id)
            .cloned()
            .ok_or_else(|| jj_lib::op_store::OpStoreError::Other("missing op".into()))
    }
    async fn write_operation(
        &self,
        _: &jj_lib::op_store::Operation,
    ) -> jj_lib::op_store::OpStoreResult<jj_lib::op_store::OperationId> {
        unreachable!("unused in test")
    }
    async fn resolve_operation_id_prefix(
        &self,
        _: &jj_lib::object_id::HexPrefix,
    ) -> jj_lib::op_store::OpStoreResult<
        jj_lib::object_id::PrefixResolution<jj_lib::op_store::OperationId>,
    > {
        unreachable!("unused in test")
    }
    async fn gc(
        &self,
        _: &[jj_lib::op_store::OperationId],
        _: std::time::SystemTime,
    ) -> jj_lib::op_store::OpStoreResult<()> {
        Ok(())
    }
}

#[tokio::test]
async fn test_incremental_no_missing_ids_sync_advances_anchor_for_earlier_op() {
    let root_commit_id = CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES);
    let (c1_id, c1_proto) =
        make_verified_proto_commit(std::slice::from_ref(&root_commit_id), 0x11, "commit C1");
    let (c2_id, c2_proto) =
        make_verified_proto_commit(std::slice::from_ref(&c1_id), 0x22, "commit C2");

    let state = Arc::new(Mutex::new(DynamicListCommitsState {
        commits: HashMap::from([(c1_id.as_bytes().to_vec(), c1_proto)]),
        log: vec![(1, c1_id.as_bytes().to_vec())],
        reject_since_with_unimplemented: false,
        reject_all_list_with_unimplemented: false,
        recorded_since_tokens: Vec::new(),
    }));
    let url = spawn_dynamic_list_commits_server(Arc::clone(&state)).await;

    let dir = tempfile::tempdir().unwrap();
    let store_dir = dir.path().join(".jj/repo/store");
    let index_dir = dir.path().join(".jj/repo/index");
    fs::create_dir_all(&index_dir).unwrap();
    CommitCloudConfig {
        server_url: url,
        repo_id: "repo-anchor".to_string(),
        project_id: Some("proj-anchor".to_string()),
    }
    .save_to_store(&store_dir)
    .unwrap();

    let settings = UserSettings::from_config_and_home_dir(
        jj_lib::config::StackedConfig::with_defaults(),
        None,
    )
    .unwrap();
    let index_store = CommitCloudIndexStore::init(&settings, &index_dir).unwrap();
    let backend = crate::cc_backend::CommitCloudBackend::load(&store_dir).unwrap();
    let signer = jj_lib::signing_factory::signer_from_settings(&settings).unwrap();
    let merge_options = settings.merge_options().unwrap();
    let store = Store::new(Box::new(backend), signer, merge_options);

    let root_op_id = jj_lib::op_store::OperationId::from_bytes(&cc_common::ROOT_OPERATION_ID_BYTES);
    let root_view_id = jj_lib::op_store::ViewId::from_bytes(&cc_common::ROOT_VIEW_ID_BYTES);
    let op1_id = jj_lib::op_store::OperationId::from_bytes(&[0x01; cc_common::OPERATION_ID_LENGTH]);
    let view1_id = jj_lib::op_store::ViewId::from_bytes(&[0x01; cc_common::VIEW_ID_LENGTH]);
    let op2_id = jj_lib::op_store::OperationId::from_bytes(&[0x02; cc_common::OPERATION_ID_LENGTH]);
    let view2_id = jj_lib::op_store::ViewId::from_bytes(&[0x02; cc_common::VIEW_ID_LENGTH]);

    let make_meta = |desc: &str| jj_lib::op_store::OperationMetadata {
        time: jj_lib::op_store::TimestampRange {
            start: jj_lib::backend::Timestamp {
                timestamp: jj_lib::backend::MillisSinceEpoch(0),
                tz_offset: 0,
            },
            end: jj_lib::backend::Timestamp {
                timestamp: jj_lib::backend::MillisSinceEpoch(0),
                tz_offset: 0,
            },
        },
        description: desc.to_string(),
        hostname: String::new(),
        username: String::new(),
        is_snapshot: false,
        workspace_name: None,
        attributes: Default::default(),
    };

    let root_view = jj_lib::op_store::View::make_root(root_commit_id.clone());
    let mut view1 = root_view.clone();
    view1.head_ids = HashSet::from([c1_id.clone()]);
    let mut view2 = root_view.clone();
    view2.head_ids = HashSet::from([c2_id.clone()]);

    let root_op_data = jj_lib::op_store::Operation {
        view_id: root_view_id.clone(),
        parents: Vec::new(),
        metadata: make_meta("root"),
        commit_predecessors: Some(Default::default()),
    };
    let op1_data = jj_lib::op_store::Operation {
        view_id: view1_id.clone(),
        parents: vec![root_op_id.clone()],
        metadata: make_meta("op1"),
        commit_predecessors: Some(Default::default()),
    };
    let op2_data = jj_lib::op_store::Operation {
        view_id: view2_id.clone(),
        parents: vec![op1_id.clone()],
        metadata: make_meta("op2"),
        commit_predecessors: Some(Default::default()),
    };

    let op_store: Arc<dyn jj_lib::op_store::OpStore> = Arc::new(InMemoryOpStore {
        root_op_id: root_op_id.clone(),
        ops: HashMap::from([
            (root_op_id, root_op_data),
            (op1_id.clone(), op1_data.clone()),
            (op2_id.clone(), op2_data.clone()),
        ]),
        views: HashMap::from([
            (root_view_id, root_view),
            (view1_id, view1),
            (view2_id, view2),
        ]),
    });
    let op1 = Operation::new(op_store.clone(), op1_id, op1_data);
    let op2 = Operation::new(op_store, op2_id, op2_data);

    // Sync at op1: indexes C1 and records cursor (sync_token=1, anchor=C1).
    let idx1 = index_store.get_index_at_op(&op1, &store).await.unwrap();
    assert!(idx1.as_index().has_id(&c1_id).await.unwrap());
    assert!(!idx1.as_index().has_id(&c2_id).await.unwrap());

    // A no-op repeat sync at op1 must leave cc_sync_cursor.json untouched (no rewrite/fsync).
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        let cursor_path = index_dir.join(SYNC_CURSOR_FILENAME);
        let ino_before = fs::metadata(&cursor_path).unwrap().ino();
        let _idx1_noop = index_store.get_index_at_op(&op1, &store).await.unwrap();
        let ino_after = fs::metadata(&cursor_path).unwrap().ino();
        assert_eq!(
            ino_before, ino_after,
            "no-op incremental sync must skip rewriting cc_sync_cursor.json"
        );
    }

    // Publish C2 on server and sync at op2: op2's DefaultIndexStore already contains C2,
    // so `missing_ids` is empty; the cursor must advance BOTH `sync_token` and `anchor` to C2.
    {
        let mut guard = state.lock().unwrap();
        guard.commits.insert(c2_id.as_bytes().to_vec(), c2_proto);
        guard.log.push((2, c2_id.as_bytes().to_vec()));
    }
    let idx2 = index_store.get_index_at_op(&op2, &store).await.unwrap();
    assert!(idx2.as_index().has_id(&c2_id).await.unwrap());

    // Loading the earlier operation index (`--at-op op1`) must use op1's watermark (1)
    // and discover + index C2.
    let idx1_reloaded = index_store.get_index_at_op(&op1, &store).await.unwrap();
    assert!(
        idx1_reloaded.as_index().has_id(&c2_id).await.unwrap(),
        "loading earlier operation index after incremental no-missing-ids sync must still discover C2"
    );
}

fn make_test_op_meta(desc: &str) -> jj_lib::op_store::OperationMetadata {
    jj_lib::op_store::OperationMetadata {
        time: jj_lib::op_store::TimestampRange {
            start: jj_lib::backend::Timestamp {
                timestamp: jj_lib::backend::MillisSinceEpoch(0),
                tz_offset: 0,
            },
            end: jj_lib::backend::Timestamp {
                timestamp: jj_lib::backend::MillisSinceEpoch(0),
                tz_offset: 0,
            },
        },
        description: desc.to_string(),
        hostname: String::new(),
        username: String::new(),
        is_snapshot: false,
        workspace_name: None,
        attributes: Default::default(),
    }
}

#[tokio::test]
async fn test_per_op_watermark_prevents_sibling_commit_loss_across_ops() {
    let root_commit_id = CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES);
    let (c1_id, c1_proto) =
        make_verified_proto_commit(std::slice::from_ref(&root_commit_id), 0x11, "commit C1");
    let (c2_id, c2_proto) =
        make_verified_proto_commit(std::slice::from_ref(&c1_id), 0x22, "sibling commit C2");
    let (c3_id, c3_proto) =
        make_verified_proto_commit(std::slice::from_ref(&c1_id), 0x33, "local commit C3");

    let state = Arc::new(Mutex::new(DynamicListCommitsState {
        commits: HashMap::from([(c1_id.as_bytes().to_vec(), c1_proto)]),
        log: vec![(1, c1_id.as_bytes().to_vec())],
        reject_since_with_unimplemented: false,
        reject_all_list_with_unimplemented: false,
        recorded_since_tokens: Vec::new(),
    }));
    let url = spawn_dynamic_list_commits_server(Arc::clone(&state)).await;

    let dir = tempfile::tempdir().unwrap();
    let store_dir = dir.path().join(".jj/repo/store");
    let index_dir = dir.path().join(".jj/repo/index");
    fs::create_dir_all(&index_dir).unwrap();
    CommitCloudConfig {
        server_url: url,
        repo_id: "repo-per-op".to_string(),
        project_id: Some("proj-per-op".to_string()),
    }
    .save_to_store(&store_dir)
    .unwrap();

    let settings = UserSettings::from_config_and_home_dir(
        jj_lib::config::StackedConfig::with_defaults(),
        None,
    )
    .unwrap();
    let index_store = CommitCloudIndexStore::init(&settings, &index_dir).unwrap();
    let backend = crate::cc_backend::CommitCloudBackend::load(&store_dir).unwrap();
    let signer = jj_lib::signing_factory::signer_from_settings(&settings).unwrap();
    let merge_options = settings.merge_options().unwrap();
    let store = Store::new(Box::new(backend), signer, merge_options);

    let root_op_id = jj_lib::op_store::OperationId::from_bytes(&cc_common::ROOT_OPERATION_ID_BYTES);
    let root_view_id = jj_lib::op_store::ViewId::from_bytes(&cc_common::ROOT_VIEW_ID_BYTES);
    let op1_id = jj_lib::op_store::OperationId::from_bytes(&[0x11; cc_common::OPERATION_ID_LENGTH]);
    let view1_id = jj_lib::op_store::ViewId::from_bytes(&[0x11; cc_common::VIEW_ID_LENGTH]);
    let op2_id = jj_lib::op_store::OperationId::from_bytes(&[0x22; cc_common::OPERATION_ID_LENGTH]);
    let view2_id = jj_lib::op_store::ViewId::from_bytes(&[0x22; cc_common::VIEW_ID_LENGTH]);

    let root_view = jj_lib::op_store::View::make_root(root_commit_id.clone());
    let mut view1 = root_view.clone();
    view1.head_ids = HashSet::from([c1_id.clone()]);
    let mut view2 = root_view.clone();
    view2.head_ids = HashSet::from([c3_id.clone()]);

    let root_op_data = jj_lib::op_store::Operation {
        view_id: root_view_id.clone(),
        parents: Vec::new(),
        metadata: make_test_op_meta("root"),
        commit_predecessors: Some(Default::default()),
    };
    let op1_data = jj_lib::op_store::Operation {
        view_id: view1_id.clone(),
        parents: vec![root_op_id.clone()],
        metadata: make_test_op_meta("op1"),
        commit_predecessors: Some(Default::default()),
    };
    let op2_data = jj_lib::op_store::Operation {
        view_id: view2_id.clone(),
        parents: vec![op1_id.clone()],
        metadata: make_test_op_meta("op2"),
        commit_predecessors: Some(Default::default()),
    };

    let op_store: Arc<dyn jj_lib::op_store::OpStore> = Arc::new(InMemoryOpStore {
        root_op_id: root_op_id.clone(),
        ops: HashMap::from([
            (root_op_id, root_op_data),
            (op1_id.clone(), op1_data.clone()),
            (op2_id.clone(), op2_data.clone()),
        ]),
        views: HashMap::from([
            (root_view_id, root_view),
            (view1_id, view1),
            (view2_id, view2),
        ]),
    });
    let op1 = Operation::new(op_store.clone(), op1_id, op1_data);
    let op2 = Operation::new(op_store, op2_id, op2_data);

    // op1 syncs at token 1 ({C1}).
    let idx1 = index_store.get_index_at_op(&op1, &store).await.unwrap();
    assert!(idx1.as_index().has_id(&c1_id).await.unwrap());

    // Sibling writes C2 (token 2).
    {
        let mut guard = state.lock().unwrap();
        guard.commits.insert(c2_id.as_bytes().to_vec(), c2_proto);
        guard.log.push((2, c2_id.as_bytes().to_vec()));
    }

    // op2 writes C3 (token 3) and persists local index {C1, C3} (missing C2), inheriting
    // op1's watermark (token 1).
    {
        let mut guard = state.lock().unwrap();
        guard.commits.insert(c3_id.as_bytes().to_vec(), c3_proto);
        guard.log.push((3, c3_id.as_bytes().to_vec()));
    }
    let c3_commit = store.get_commit_async(&c3_id).await.unwrap();
    let mut mut_idx2 = idx1.start_modification();
    mut_idx2.add_commit(&c3_commit).await.unwrap();
    let persisted_idx2 = index_store.write_index(mut_idx2, &op2).unwrap();
    assert!(persisted_idx2.as_index().has_id(&c1_id).await.unwrap());
    assert!(persisted_idx2.as_index().has_id(&c3_id).await.unwrap());
    assert!(!persisted_idx2.as_index().has_id(&c2_id).await.unwrap());

    // get_index_at_op(&op1) syncs [C2, C3], advancing op1's watermark to 3 while leaving
    // op2's watermark at 1.
    let idx1_synced = index_store.get_index_at_op(&op1, &store).await.unwrap();
    assert!(idx1_synced.as_index().has_id(&c2_id).await.unwrap());
    assert!(idx1_synced.as_index().has_id(&c3_id).await.unwrap());

    // get_index_at_op(&op2) MUST still query from watermark 1 and discover + index C2.
    state.lock().unwrap().recorded_since_tokens.clear();
    let idx2_synced = index_store.get_index_at_op(&op2, &store).await.unwrap();
    assert_eq!(
        state.lock().unwrap().recorded_since_tokens,
        vec![1u64.to_be_bytes().to_vec()],
        "op2 must query ListCommits from its own inherited watermark (1), not op1's (3)"
    );
    assert!(
        idx2_synced.as_index().has_id(&c2_id).await.unwrap(),
        "op2 must discover and index sibling commit C2"
    );
}

#[tokio::test]
async fn test_write_index_merge_op_propagates_only_identical_parent_watermarks() {
    let root_commit_id = CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES);
    let (c1_id, c1_proto) =
        make_verified_proto_commit(std::slice::from_ref(&root_commit_id), 0x11, "commit C1");
    let (c2_id, c2_proto) =
        make_verified_proto_commit(std::slice::from_ref(&c1_id), 0x22, "commit C2");

    let state = Arc::new(Mutex::new(DynamicListCommitsState {
        commits: HashMap::from([(c1_id.as_bytes().to_vec(), c1_proto)]),
        log: vec![(1, c1_id.as_bytes().to_vec())],
        reject_since_with_unimplemented: false,
        reject_all_list_with_unimplemented: false,
        recorded_since_tokens: Vec::new(),
    }));
    let url = spawn_dynamic_list_commits_server(Arc::clone(&state)).await;

    let dir = tempfile::tempdir().unwrap();
    let store_dir = dir.path().join(".jj/repo/store");
    let index_dir = dir.path().join(".jj/repo/index");
    fs::create_dir_all(&index_dir).unwrap();
    CommitCloudConfig {
        server_url: url,
        repo_id: "repo-merge".to_string(),
        project_id: Some("proj-merge".to_string()),
    }
    .save_to_store(&store_dir)
    .unwrap();

    let settings = UserSettings::from_config_and_home_dir(
        jj_lib::config::StackedConfig::with_defaults(),
        None,
    )
    .unwrap();
    let index_store = CommitCloudIndexStore::init(&settings, &index_dir).unwrap();
    let backend = crate::cc_backend::CommitCloudBackend::load(&store_dir).unwrap();
    let signer = jj_lib::signing_factory::signer_from_settings(&settings).unwrap();
    let merge_options = settings.merge_options().unwrap();
    let store = Store::new(Box::new(backend), signer, merge_options);

    let root_op_id = jj_lib::op_store::OperationId::from_bytes(&cc_common::ROOT_OPERATION_ID_BYTES);
    let root_view_id = jj_lib::op_store::ViewId::from_bytes(&cc_common::ROOT_VIEW_ID_BYTES);
    let op_a_id =
        jj_lib::op_store::OperationId::from_bytes(&[0xa1; cc_common::OPERATION_ID_LENGTH]);
    let op_b_id =
        jj_lib::op_store::OperationId::from_bytes(&[0xb1; cc_common::OPERATION_ID_LENGTH]);
    let op_unsynced_id =
        jj_lib::op_store::OperationId::from_bytes(&[0xc1; cc_common::OPERATION_ID_LENGTH]);
    let op_merge_same_id =
        jj_lib::op_store::OperationId::from_bytes(&[0xd1; cc_common::OPERATION_ID_LENGTH]);
    let op_merge_diff_id =
        jj_lib::op_store::OperationId::from_bytes(&[0xd2; cc_common::OPERATION_ID_LENGTH]);
    let op_merge_missing_id =
        jj_lib::op_store::OperationId::from_bytes(&[0xd3; cc_common::OPERATION_ID_LENGTH]);
    let view1_id = jj_lib::op_store::ViewId::from_bytes(&[0x51; cc_common::VIEW_ID_LENGTH]);

    let root_view = jj_lib::op_store::View::make_root(root_commit_id.clone());
    let mut view1 = root_view.clone();
    view1.head_ids = HashSet::from([c1_id.clone()]);

    let mk_op =
        |parents: Vec<jj_lib::op_store::OperationId>, desc: &str| jj_lib::op_store::Operation {
            view_id: view1_id.clone(),
            parents,
            metadata: make_test_op_meta(desc),
            commit_predecessors: Some(Default::default()),
        };
    let root_op_data = jj_lib::op_store::Operation {
        view_id: root_view_id.clone(),
        parents: Vec::new(),
        metadata: make_test_op_meta("root"),
        commit_predecessors: Some(Default::default()),
    };
    let op_a_data = mk_op(vec![root_op_id.clone()], "op_a");
    let op_b_data = mk_op(vec![root_op_id.clone()], "op_b");
    let op_unsynced_data = mk_op(vec![root_op_id.clone()], "op_unsynced");
    let op_merge_same_data = mk_op(vec![op_a_id.clone(), op_b_id.clone()], "merge_same");
    let op_merge_diff_data = mk_op(vec![op_a_id.clone(), op_b_id.clone()], "merge_diff");
    let op_merge_missing_data = mk_op(
        vec![op_a_id.clone(), op_unsynced_id.clone()],
        "merge_missing",
    );

    let op_store: Arc<dyn jj_lib::op_store::OpStore> = Arc::new(InMemoryOpStore {
        root_op_id: root_op_id.clone(),
        ops: HashMap::from([
            (root_op_id, root_op_data),
            (op_a_id.clone(), op_a_data.clone()),
            (op_b_id.clone(), op_b_data.clone()),
            (op_unsynced_id.clone(), op_unsynced_data.clone()),
            (op_merge_same_id.clone(), op_merge_same_data.clone()),
            (op_merge_diff_id.clone(), op_merge_diff_data.clone()),
            (op_merge_missing_id.clone(), op_merge_missing_data.clone()),
        ]),
        views: HashMap::from([(root_view_id, root_view), (view1_id, view1)]),
    });
    let op_a = Operation::new(op_store.clone(), op_a_id, op_a_data);
    let op_b = Operation::new(op_store.clone(), op_b_id, op_b_data);
    let op_merge_same = Operation::new(op_store.clone(), op_merge_same_id, op_merge_same_data);
    let op_merge_diff = Operation::new(op_store.clone(), op_merge_diff_id, op_merge_diff_data);
    let op_merge_missing = Operation::new(op_store, op_merge_missing_id, op_merge_missing_data);

    // Both op_a and op_b sync at token 1 -> merge_same inherits token 1.
    let idx_a = index_store.get_index_at_op(&op_a, &store).await.unwrap();
    let _idx_b = index_store.get_index_at_op(&op_b, &store).await.unwrap();
    index_store
        .write_index(idx_a.start_modification(), &op_merge_same)
        .unwrap();
    assert_eq!(
        index_store
            .load_op_watermark(op_merge_same.id())
            .map(|w| w.sync_token_hex),
        Some(hex::encode(1u64.to_be_bytes()))
    );

    // Missing parent watermark -> merge_missing stays unrecorded.
    index_store
        .write_index(idx_a.start_modification(), &op_merge_missing)
        .unwrap();
    assert!(
        index_store
            .load_op_watermark(op_merge_missing.id())
            .is_none()
    );

    // Advance op_b to token 2 while op_a remains at token 1 -> merge_diff stays unrecorded
    // and performs a safe full scan on load.
    {
        let mut guard = state.lock().unwrap();
        guard.commits.insert(c2_id.as_bytes().to_vec(), c2_proto);
        guard.log.push((2, c2_id.as_bytes().to_vec()));
    }
    let _idx_b2 = index_store.get_index_at_op(&op_b, &store).await.unwrap();
    index_store
        .write_index(idx_a.start_modification(), &op_merge_diff)
        .unwrap();
    assert!(index_store.load_op_watermark(op_merge_diff.id()).is_none());

    state.lock().unwrap().recorded_since_tokens.clear();
    let idx_diff = index_store
        .get_index_at_op(&op_merge_diff, &store)
        .await
        .unwrap();
    assert_eq!(
        state.lock().unwrap().recorded_since_tokens,
        vec![Vec::<u8>::new()],
        "merge op with differing parent watermarks must fall back to a full scan"
    );
    assert!(idx_diff.as_index().has_id(&c2_id).await.unwrap());
}

#[tokio::test]
async fn test_legacy_single_cursor_falls_back_to_full_scan_and_evicts_past_limit() {
    let root_commit_id = CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES);
    let (c1_id, c1_proto) =
        make_verified_proto_commit(std::slice::from_ref(&root_commit_id), 0x11, "commit C1");

    let state = Arc::new(Mutex::new(DynamicListCommitsState {
        commits: HashMap::from([(c1_id.as_bytes().to_vec(), c1_proto)]),
        log: vec![(1, c1_id.as_bytes().to_vec())],
        reject_since_with_unimplemented: false,
        reject_all_list_with_unimplemented: false,
        recorded_since_tokens: Vec::new(),
    }));
    let url = spawn_dynamic_list_commits_server(Arc::clone(&state)).await;

    let dir = tempfile::tempdir().unwrap();
    let store_dir = dir.path().join(".jj/repo/store");
    let index_dir = dir.path().join(".jj/repo/index");
    fs::create_dir_all(&index_dir).unwrap();
    CommitCloudConfig {
        server_url: url.clone(),
        repo_id: "repo-legacy".to_string(),
        project_id: Some("proj-legacy".to_string()),
    }
    .save_to_store(&store_dir)
    .unwrap();

    // Pre-seed legacy single-cursor v1 file (no `version` or `op_watermarks`).
    let legacy_json = format!(
        r#"{{"server_url":"{url}","project_id":"proj-legacy","sync_token_hex":"0000000000000001","anchor_commit_hex":"{}"}}"#,
        c1_id.hex()
    );
    fs::write(index_dir.join(SYNC_CURSOR_FILENAME), legacy_json).unwrap();

    let settings = UserSettings::from_config_and_home_dir(
        jj_lib::config::StackedConfig::with_defaults(),
        None,
    )
    .unwrap();
    let index_store = CommitCloudIndexStore::init(&settings, &index_dir).unwrap();
    let backend = crate::cc_backend::CommitCloudBackend::load(&store_dir).unwrap();
    let signer = jj_lib::signing_factory::signer_from_settings(&settings).unwrap();
    let merge_options = settings.merge_options().unwrap();
    let store = Store::new(Box::new(backend), signer, merge_options);

    let root_op_id = jj_lib::op_store::OperationId::from_bytes(&cc_common::ROOT_OPERATION_ID_BYTES);
    let root_view_id = jj_lib::op_store::ViewId::from_bytes(&cc_common::ROOT_VIEW_ID_BYTES);
    let op1_id = jj_lib::op_store::OperationId::from_bytes(&[0x01; cc_common::OPERATION_ID_LENGTH]);
    let root_view = jj_lib::op_store::View::make_root(root_commit_id);
    let root_op_data = jj_lib::op_store::Operation {
        view_id: root_view_id.clone(),
        parents: Vec::new(),
        metadata: make_test_op_meta("root"),
        commit_predecessors: Some(Default::default()),
    };
    let op1_data = jj_lib::op_store::Operation {
        view_id: root_view_id.clone(),
        parents: vec![root_op_id.clone()],
        metadata: make_test_op_meta("op1"),
        commit_predecessors: Some(Default::default()),
    };
    let op_store: Arc<dyn jj_lib::op_store::OpStore> = Arc::new(InMemoryOpStore {
        root_op_id: root_op_id.clone(),
        ops: HashMap::from([
            (root_op_id, root_op_data),
            (op1_id.clone(), op1_data.clone()),
        ]),
        views: HashMap::from([(root_view_id, root_view)]),
    });
    let op1 = Operation::new(op_store, op1_id, op1_data);

    let idx = index_store.get_index_at_op(&op1, &store).await.unwrap();
    assert!(idx.as_index().has_id(&c1_id).await.unwrap());
    assert_eq!(
        state.lock().unwrap().recorded_since_tokens,
        vec![Vec::<u8>::new()],
        "legacy v1 single-cursor file must trigger a full scan and upgrade to v2"
    );
    let upgraded = index_store
        .load_cursor_file()
        .expect("v2 cursor should be written after full scan");
    assert_eq!(upgraded.version, SYNC_CURSOR_SCHEMA_VERSION);
    assert_eq!(upgraded.op_watermarks.len(), 1);

    // Bounded eviction keeps only the MAX_OP_WATERMARKS most recently updated ops.
    let total = MAX_OP_WATERMARKS + 10;
    for i in 0..total {
        index_store.save_op_watermark(OpSyncWatermark {
            op_id_hex: format!("op-{i:04}"),
            sync_token_hex: hex::encode((i as u64 + 1).to_be_bytes()),
            anchor_commit_hex: None,
        });
    }
    let bounded = index_store.load_cursor_file().unwrap();
    assert_eq!(bounded.op_watermarks.len(), MAX_OP_WATERMARKS);
    assert_eq!(bounded.op_watermarks[0].op_id_hex, "op-0010");
    assert_eq!(
        bounded.op_watermarks.last().unwrap().op_id_hex,
        format!("op-{:04}", total - 1)
    );
}

#[tokio::test]
async fn test_anchor_prefers_missing_sibling_over_already_indexed_local_commit() {
    let root_commit_id = CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES);
    let (c1_id, c1_proto) =
        make_verified_proto_commit(std::slice::from_ref(&root_commit_id), 0x11, "commit C1");
    let (c_sibling_id, c_sibling_proto) =
        make_verified_proto_commit(std::slice::from_ref(&c1_id), 0x22, "sibling commit");
    let (c_local_id, c_local_proto) =
        make_verified_proto_commit(std::slice::from_ref(&c1_id), 0x33, "local commit");

    let state = Arc::new(Mutex::new(DynamicListCommitsState {
        commits: HashMap::from([(c1_id.as_bytes().to_vec(), c1_proto)]),
        log: vec![(1, c1_id.as_bytes().to_vec())],
        reject_since_with_unimplemented: false,
        reject_all_list_with_unimplemented: false,
        recorded_since_tokens: Vec::new(),
    }));
    let url = spawn_dynamic_list_commits_server(Arc::clone(&state)).await;

    let dir = tempfile::tempdir().unwrap();
    let store_dir = dir.path().join(".jj/repo/store");
    let index_dir = dir.path().join(".jj/repo/index");
    fs::create_dir_all(&index_dir).unwrap();
    CommitCloudConfig {
        server_url: url,
        repo_id: "repo-anchor-missing".to_string(),
        project_id: Some("proj-anchor-missing".to_string()),
    }
    .save_to_store(&store_dir)
    .unwrap();

    let settings = UserSettings::from_config_and_home_dir(
        jj_lib::config::StackedConfig::with_defaults(),
        None,
    )
    .unwrap();
    let index_store = CommitCloudIndexStore::init(&settings, &index_dir).unwrap();
    let backend = crate::cc_backend::CommitCloudBackend::load(&store_dir).unwrap();
    let signer = jj_lib::signing_factory::signer_from_settings(&settings).unwrap();
    let merge_options = settings.merge_options().unwrap();
    let store = Store::new(Box::new(backend), signer, merge_options);

    let root_op_id = jj_lib::op_store::OperationId::from_bytes(&cc_common::ROOT_OPERATION_ID_BYTES);
    let root_view_id = jj_lib::op_store::ViewId::from_bytes(&cc_common::ROOT_VIEW_ID_BYTES);
    let op1_id = jj_lib::op_store::OperationId::from_bytes(&[0x41; cc_common::OPERATION_ID_LENGTH]);
    let view1_id = jj_lib::op_store::ViewId::from_bytes(&[0x41; cc_common::VIEW_ID_LENGTH]);
    let op2_id = jj_lib::op_store::OperationId::from_bytes(&[0x42; cc_common::OPERATION_ID_LENGTH]);
    let view2_id = jj_lib::op_store::ViewId::from_bytes(&[0x42; cc_common::VIEW_ID_LENGTH]);

    let root_view = jj_lib::op_store::View::make_root(root_commit_id);
    let mut view1 = root_view.clone();
    view1.head_ids = HashSet::from([c1_id.clone()]);
    let mut view2 = root_view.clone();
    view2.head_ids = HashSet::from([c_local_id.clone()]);

    let root_op_data = jj_lib::op_store::Operation {
        view_id: root_view_id.clone(),
        parents: Vec::new(),
        metadata: make_test_op_meta("root"),
        commit_predecessors: Some(Default::default()),
    };
    let op1_data = jj_lib::op_store::Operation {
        view_id: view1_id.clone(),
        parents: vec![root_op_id.clone()],
        metadata: make_test_op_meta("op1"),
        commit_predecessors: Some(Default::default()),
    };
    let op2_data = jj_lib::op_store::Operation {
        view_id: view2_id.clone(),
        parents: vec![op1_id.clone()],
        metadata: make_test_op_meta("op2"),
        commit_predecessors: Some(Default::default()),
    };

    let op_store: Arc<dyn jj_lib::op_store::OpStore> = Arc::new(InMemoryOpStore {
        root_op_id: root_op_id.clone(),
        ops: HashMap::from([
            (root_op_id, root_op_data),
            (op1_id.clone(), op1_data.clone()),
            (op2_id.clone(), op2_data.clone()),
        ]),
        views: HashMap::from([
            (root_view_id, root_view),
            (view1_id, view1),
            (view2_id, view2),
        ]),
    });
    let op1 = Operation::new(op_store.clone(), op1_id, op1_data);
    let op2 = Operation::new(op_store, op2_id, op2_data);

    // Sync op1 at token 1 ({C1}).
    let idx1 = index_store.get_index_at_op(&op1, &store).await.unwrap();
    assert!(idx1.as_index().has_id(&c1_id).await.unwrap());

    // Server log advances with `[C_sibling (seq 2), C_local (seq 3)]`.
    {
        let mut guard = state.lock().unwrap();
        guard
            .commits
            .insert(c_sibling_id.as_bytes().to_vec(), c_sibling_proto);
        guard.log.push((2, c_sibling_id.as_bytes().to_vec()));
        guard
            .commits
            .insert(c_local_id.as_bytes().to_vec(), c_local_proto);
        guard.log.push((3, c_local_id.as_bytes().to_vec()));
    }

    // Persist op2's pre-sync local index containing `{C1, C_local}` (without `C_sibling`).
    let c_local_commit = store.get_commit_async(&c_local_id).await.unwrap();
    let mut mut_idx2 = idx1.start_modification();
    mut_idx2.add_commit(&c_local_commit).await.unwrap();
    let pre_sync_idx2 = index_store.write_index(mut_idx2, &op2).unwrap();
    assert!(pre_sync_idx2.as_index().has_id(&c_local_id).await.unwrap());
    assert!(
        !pre_sync_idx2
            .as_index()
            .has_id(&c_sibling_id)
            .await
            .unwrap()
    );

    // Sync op2: `ListCommits` returns `[C_sibling, C_local]`, `missing_ids` is `[C_sibling]`.
    let synced_idx2 = index_store.get_index_at_op(&op2, &store).await.unwrap();
    assert!(synced_idx2.as_index().has_id(&c_sibling_id).await.unwrap());
    assert!(synced_idx2.as_index().has_id(&c_local_id).await.unwrap());

    let wm = index_store
        .load_op_watermark(op2.id())
        .expect("op2 watermark should be saved");
    assert_eq!(wm.sync_token_hex, hex::encode(3u64.to_be_bytes()));
    assert_eq!(
        wm.anchor_commit_hex.as_deref(),
        Some(c_sibling_id.hex().as_str()),
        "anchor must pick missing C_sibling rather than already-indexed C_local"
    );
    assert!(
        validate_watermark(Some(wm.clone()), pre_sync_idx2.as_ref())
            .await
            .is_none(),
        "pre-sync index lacking C_sibling must fail watermark validation"
    );
    assert!(
        validate_watermark(Some(wm), synced_idx2.as_ref())
            .await
            .is_some(),
        "post-sync index containing C_sibling must pass watermark validation"
    );
}

#[tokio::test]
async fn test_no_missing_ids_incremental_sync_preserves_non_view_prev_anchor() {
    let root_commit_id = CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES);
    let (c1_id, c1_proto) =
        make_verified_proto_commit(std::slice::from_ref(&root_commit_id), 0x71, "commit C1");
    let (c_sibling_id, c_sibling_proto) =
        make_verified_proto_commit(std::slice::from_ref(&c1_id), 0x72, "sibling commit");
    let (c_local_id, c_local_proto) =
        make_verified_proto_commit(std::slice::from_ref(&c1_id), 0x73, "local commit");

    let state = Arc::new(Mutex::new(DynamicListCommitsState {
        commits: HashMap::from([
            (c1_id.as_bytes().to_vec(), c1_proto),
            (c_sibling_id.as_bytes().to_vec(), c_sibling_proto),
            (c_local_id.as_bytes().to_vec(), c_local_proto),
        ]),
        log: vec![
            (1, c1_id.as_bytes().to_vec()),
            (2, c_sibling_id.as_bytes().to_vec()),
        ],
        reject_since_with_unimplemented: false,
        reject_all_list_with_unimplemented: false,
        recorded_since_tokens: Vec::new(),
    }));
    let url = spawn_dynamic_list_commits_server(Arc::clone(&state)).await;

    let dir = tempfile::tempdir().unwrap();
    let store_dir = dir.path().join(".jj/repo/store");
    let index_dir = dir.path().join(".jj/repo/index");
    fs::create_dir_all(&index_dir).unwrap();
    CommitCloudConfig {
        server_url: url,
        repo_id: "repo-preserve-anchor".to_string(),
        project_id: Some("proj-preserve-anchor".to_string()),
    }
    .save_to_store(&store_dir)
    .unwrap();

    let settings = UserSettings::from_config_and_home_dir(
        jj_lib::config::StackedConfig::with_defaults(),
        None,
    )
    .unwrap();
    let index_store = CommitCloudIndexStore::init(&settings, &index_dir).unwrap();
    let backend = crate::cc_backend::CommitCloudBackend::load(&store_dir).unwrap();
    let signer = jj_lib::signing_factory::signer_from_settings(&settings).unwrap();
    let merge_options = settings.merge_options().unwrap();
    let store = Store::new(Box::new(backend), signer, merge_options);

    let root_op_id = jj_lib::op_store::OperationId::from_bytes(&cc_common::ROOT_OPERATION_ID_BYTES);
    let root_view_id = jj_lib::op_store::ViewId::from_bytes(&cc_common::ROOT_VIEW_ID_BYTES);
    let op1_id = jj_lib::op_store::OperationId::from_bytes(&[0x71; cc_common::OPERATION_ID_LENGTH]);
    let view1_id = jj_lib::op_store::ViewId::from_bytes(&[0x71; cc_common::VIEW_ID_LENGTH]);

    let root_view = jj_lib::op_store::View::make_root(root_commit_id);
    let mut view1 = root_view.clone();
    view1.head_ids = HashSet::from([c_local_id.clone()]);

    let root_op_data = jj_lib::op_store::Operation {
        view_id: root_view_id.clone(),
        parents: Vec::new(),
        metadata: make_test_op_meta("root"),
        commit_predecessors: Some(Default::default()),
    };
    let op1_data = jj_lib::op_store::Operation {
        view_id: view1_id.clone(),
        parents: vec![root_op_id.clone()],
        metadata: make_test_op_meta("op1"),
        commit_predecessors: Some(Default::default()),
    };

    let op_store: Arc<dyn jj_lib::op_store::OpStore> = Arc::new(InMemoryOpStore {
        root_op_id: root_op_id.clone(),
        ops: HashMap::from([
            (root_op_id, root_op_data),
            (op1_id.clone(), op1_data.clone()),
        ]),
        views: HashMap::from([(root_view_id, root_view), (view1_id, view1)]),
    });
    let op1 = Operation::new(op_store, op1_id, op1_data);

    // Build a view-only index for op1 (contains C1 and C_local, lacks C_sibling) to simulate
    // later segment rebuild from view1 alone.
    let view_only_dir = tempfile::tempdir().unwrap();
    let view_only_idx = DefaultIndexStore::init(view_only_dir.path())
        .unwrap()
        .get_index_at_op(&op1, &store)
        .await
        .unwrap();
    assert!(view_only_idx.as_index().has_id(&c_local_id).await.unwrap());
    assert!(
        !view_only_idx
            .as_index()
            .has_id(&c_sibling_id)
            .await
            .unwrap()
    );

    // Initial sync of op1 at seq 2 discovers non-view C_sibling and anchors on it.
    let idx1 = index_store.get_index_at_op(&op1, &store).await.unwrap();
    assert!(idx1.as_index().has_id(&c_sibling_id).await.unwrap());
    assert!(idx1.as_index().has_id(&c_local_id).await.unwrap());
    let wm1 = index_store
        .load_op_watermark(op1.id())
        .expect("op1 watermark should exist after initial sync");
    assert_eq!(wm1.sync_token_hex, hex::encode(2u64.to_be_bytes()));
    assert_eq!(
        wm1.anchor_commit_hex.as_deref(),
        Some(c_sibling_id.hex().as_str())
    );

    // Server log appends C_local (seq 3), which op1 already has indexed, so `missing_ids` is
    // empty. Advancing the watermark to seq 3 must preserve the non-view `prev_anchor`
    // (C_sibling) instead of overwriting it with C_local.
    state
        .lock()
        .unwrap()
        .log
        .push((3, c_local_id.as_bytes().to_vec()));
    let _idx1_again = index_store.get_index_at_op(&op1, &store).await.unwrap();
    let wm2 = index_store
        .load_op_watermark(op1.id())
        .expect("op1 watermark should advance to seq 3");
    assert_eq!(wm2.sync_token_hex, hex::encode(3u64.to_be_bytes()));
    assert_eq!(
        wm2.anchor_commit_hex.as_deref(),
        Some(c_sibling_id.hex().as_str()),
        "no-missing-ids incremental sync must preserve prior non-view anchor C_sibling"
    );
    assert!(
        validate_watermark(Some(wm2), view_only_idx.as_ref())
            .await
            .is_none(),
        "rebuilt view-only index lacking C_sibling must still fail watermark validation"
    );
}

#[test]
fn test_save_op_watermark_serializes_with_cross_process_file_lock() {
    let dir = tempfile::tempdir().unwrap();
    let store_dir = dir.path().join(".jj/repo/store");
    let index_dir = dir.path().join(".jj/repo/index");
    fs::create_dir_all(&index_dir).unwrap();
    CommitCloudConfig {
        server_url: "http://127.0.0.1:1".to_string(),
        repo_id: "repo-lock".to_string(),
        project_id: Some("proj-lock".to_string()),
    }
    .save_to_store(&store_dir)
    .unwrap();

    let settings = UserSettings::from_config_and_home_dir(
        jj_lib::config::StackedConfig::with_defaults(),
        None,
    )
    .unwrap();
    let _store_a = CommitCloudIndexStore::init(&settings, &index_dir).unwrap();
    let store_b = CommitCloudIndexStore::load(&settings, &index_dir).unwrap();

    // Hold `cc_sync_cursor.lock` as if another process is mid read-modify-write.
    let held_lock = FileLock::lock(index_dir.join(SYNC_CURSOR_LOCK_FILENAME)).unwrap();

    let wm_a = OpSyncWatermark {
        op_id_hex: "op-process-a".to_string(),
        sync_token_hex: hex::encode(1u64.to_be_bytes()),
        anchor_commit_hex: None,
    };
    let wm_b = OpSyncWatermark {
        op_id_hex: "op-process-b".to_string(),
        sync_token_hex: hex::encode(2u64.to_be_bytes()),
        anchor_commit_hex: None,
    };

    let wm_b_clone = wm_b.clone();
    let writer_b = std::thread::spawn(move || {
        store_b.save_op_watermark(wm_b_clone);
        store_b
    });

    // Confirm process B is blocked on the cross-process lock until we release it.
    std::thread::sleep(std::time::Duration::from_millis(50));
    assert!(
        !index_dir.join(SYNC_CURSOR_FILENAME).exists(),
        "save_op_watermark must block while cc_sync_cursor.lock is held"
    );

    // Process A finishes writing `wm_a` before releasing the lock.
    let cursor_a = IndexSyncCursor {
        version: SYNC_CURSOR_SCHEMA_VERSION,
        server_url: "http://127.0.0.1:1".to_string(),
        project_id: "proj-lock".to_string(),
        op_watermarks: vec![wm_a.clone()],
    };
    fs::write(
        index_dir.join(SYNC_CURSOR_FILENAME),
        serde_json::to_vec(&cursor_a).unwrap(),
    )
    .unwrap();
    drop(held_lock);

    let store_b = writer_b.join().unwrap();
    let final_cursor = store_b
        .load_cursor_file()
        .expect("merged cursor file should exist");
    assert!(
        final_cursor.op_watermarks.contains(&wm_a),
        "process A watermark must not be overwritten by process B"
    );
    assert!(
        final_cursor.op_watermarks.contains(&wm_b),
        "process B watermark must be persisted after acquiring lock"
    );
}

#[tokio::test]
async fn test_fetch_missing_commits_deduplicates_frontier() {
    let root_commit_id = CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES);
    let (c1_id, c1_proto) =
        make_verified_proto_commit(std::slice::from_ref(&root_commit_id), 0x55, "commit C1");

    let state = Arc::new(Mutex::new(DynamicListCommitsState {
        commits: HashMap::from([(c1_id.as_bytes().to_vec(), c1_proto)]),
        log: vec![(1, c1_id.as_bytes().to_vec())],
        reject_since_with_unimplemented: false,
        reject_all_list_with_unimplemented: false,
        recorded_since_tokens: Vec::new(),
    }));
    let (url, read_commit_calls) =
        spawn_dynamic_list_commits_server_with_counter(Arc::clone(&state)).await;

    let dir = tempfile::tempdir().unwrap();
    let store_dir = dir.path().join(".jj/repo/store");
    let index_dir = dir.path().join(".jj/repo/index");
    fs::create_dir_all(&index_dir).unwrap();
    CommitCloudConfig {
        server_url: url,
        repo_id: "repo-dedup".to_string(),
        project_id: Some("proj-dedup".to_string()),
    }
    .save_to_store(&store_dir)
    .unwrap();

    let settings = UserSettings::from_config_and_home_dir(
        jj_lib::config::StackedConfig::with_defaults(),
        None,
    )
    .unwrap();
    let default_index_store = DefaultIndexStore::init(&index_dir).unwrap();
    let backend = crate::cc_backend::CommitCloudBackend::load(&store_dir).unwrap();
    let signer = jj_lib::signing_factory::signer_from_settings(&settings).unwrap();
    let merge_options = settings.merge_options().unwrap();
    let store = Store::new(Box::new(backend), signer, merge_options);

    let root_op_id = jj_lib::op_store::OperationId::from_bytes(&cc_common::ROOT_OPERATION_ID_BYTES);
    let root_view_id = jj_lib::op_store::ViewId::from_bytes(&cc_common::ROOT_VIEW_ID_BYTES);
    let root_view = jj_lib::op_store::View::make_root(root_commit_id);
    let root_op_data = jj_lib::op_store::Operation {
        view_id: root_view_id.clone(),
        parents: Vec::new(),
        metadata: make_test_op_meta("root"),
        commit_predecessors: Some(Default::default()),
    };
    let op_store: Arc<dyn jj_lib::op_store::OpStore> = Arc::new(InMemoryOpStore {
        root_op_id: root_op_id.clone(),
        ops: HashMap::from([(root_op_id.clone(), root_op_data.clone())]),
        views: HashMap::from([(root_view_id, root_view)]),
    });
    let root_op = Operation::new(op_store, root_op_id, root_op_data);
    let empty_index = default_index_store
        .get_index_at_op(&root_op, &store)
        .await
        .unwrap();

    let fetched = fetch_missing_commits(
        &store,
        empty_index.as_index(),
        vec![c1_id.clone(), c1_id.clone(), c1_id.clone()],
    )
    .await
    .unwrap();
    assert_eq!(fetched.len(), 1);
    assert!(fetched.contains_key(&c1_id));
    assert_eq!(
        read_commit_calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "duplicate missing_ids must be deduplicated in frontier before fetching"
    );
}

#[tokio::test]
async fn test_index_hinted_commits_retains_probed_commits_and_skips_not_found() {
    let root_commit_id = CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES);
    let (c1_id, c1_proto) =
        make_verified_proto_commit(std::slice::from_ref(&root_commit_id), 0x61, "hinted base");
    let (c2_id, c2_proto) =
        make_verified_proto_commit(std::slice::from_ref(&c1_id), 0x62, "hinted tip");
    let missing_id = CommitId::from_bytes(&[0xee; cc_common::COMMIT_ID_LENGTH]);

    let state = Arc::new(Mutex::new(DynamicListCommitsState {
        commits: HashMap::from([
            (c1_id.as_bytes().to_vec(), c1_proto),
            (c2_id.as_bytes().to_vec(), c2_proto),
        ]),
        log: vec![
            (1, c1_id.as_bytes().to_vec()),
            (2, c2_id.as_bytes().to_vec()),
        ],
        reject_since_with_unimplemented: false,
        reject_all_list_with_unimplemented: false,
        recorded_since_tokens: Vec::new(),
    }));
    let (url, read_commit_calls) =
        spawn_dynamic_list_commits_server_with_counter(Arc::clone(&state)).await;

    let dir = tempfile::tempdir().unwrap();
    let store_dir = dir.path().join(".jj/repo/store");
    let index_dir = dir.path().join(".jj/repo/index");
    fs::create_dir_all(&index_dir).unwrap();
    CommitCloudConfig {
        server_url: url,
        repo_id: "repo-hints".to_string(),
        project_id: Some("proj-hints".to_string()),
    }
    .save_to_store(&store_dir)
    .unwrap();

    let settings = UserSettings::from_config_and_home_dir(
        jj_lib::config::StackedConfig::with_defaults(),
        None,
    )
    .unwrap();
    let index_store = CommitCloudIndexStore::init(&settings, &index_dir).unwrap();
    let backend = crate::cc_backend::CommitCloudBackend::load(&store_dir).unwrap();
    let signer = jj_lib::signing_factory::signer_from_settings(&settings).unwrap();
    let merge_options = settings.merge_options().unwrap();
    let store = Store::new(Box::new(backend), signer, merge_options);

    let root_op_id = jj_lib::op_store::OperationId::from_bytes(&cc_common::ROOT_OPERATION_ID_BYTES);
    let root_view_id = jj_lib::op_store::ViewId::from_bytes(&cc_common::ROOT_VIEW_ID_BYTES);
    let root_view = jj_lib::op_store::View::make_root(root_commit_id);
    let root_op_data = jj_lib::op_store::Operation {
        view_id: root_view_id.clone(),
        parents: Vec::new(),
        metadata: make_test_op_meta("root"),
        commit_predecessors: Some(Default::default()),
    };
    let op_store: Arc<dyn jj_lib::op_store::OpStore> = Arc::new(InMemoryOpStore {
        root_op_id: root_op_id.clone(),
        ops: HashMap::from([(root_op_id.clone(), root_op_data.clone())]),
        views: HashMap::from([(root_view_id, root_view)]),
    });
    let root_op = Operation::new(op_store, root_op_id, root_op_data);
    let empty_index = index_store
        .inner
        .get_index_at_op(&root_op, &store)
        .await
        .unwrap();

    let indexed = index_store
        .index_hinted_commits(
            empty_index,
            &root_op,
            &store,
            vec![c2_id.clone(), missing_id.clone(), c2_id.clone()],
        )
        .await
        .unwrap();

    assert!(indexed.as_index().has_id(&c1_id).await.unwrap());
    assert!(indexed.as_index().has_id(&c2_id).await.unwrap());
    assert!(!indexed.as_index().has_id(&missing_id).await.unwrap());
    // 1 probe for `c2_id` (retained, not re-fetched) + 1 probe for `missing_id` (NotFound) +
    // 1 ancestor fetch for `c1_id` = 3 ReadCommit calls total.
    assert_eq!(
        read_commit_calls.load(std::sync::atomic::Ordering::SeqCst),
        3,
        "probed hinted root must be retained instead of re-fetched"
    );
}
