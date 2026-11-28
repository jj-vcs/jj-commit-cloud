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

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use cc_common::backend::backend_service_server::{BackendService, BackendServiceServer};
use cc_common::backend::*;
use cc_common::{EMPTY_TREE_ID_BYTES, MAX_GRPC_MESSAGE_BYTES};
use futures::Stream;
use tonic::{Request, Response, Status, Streaming};

use super::super::backend_tree::{slice_tree_page, validate_sorted_tree_entries};
use super::super::{DaemonProxy, MAX_TREE_PAGE_SIZE};
use super::{MockUpstream, git_blob_id, req_with_upstream, spawn_mock_upstream, unused};
use crate::cache::{CacheScope, ObjectKind, TwoTierCache};
use crate::upstream::UpstreamPool;

#[tokio::test]
async fn write_tree_sorts_utf8_and_enriches_cached_file_sizes() {
    let mock = MockUpstream::default();
    let url = spawn_mock_upstream(mock.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 8 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new(cache, Arc::new(UpstreamPool::new(None)), None);

    let file_content = b"hello tree size enrichment".to_vec();
    let file_id = BackendService::write_file(
        &proxy,
        req_with_upstream(
            WriteFileRequest {
                project_id: "proj".to_string(),
                content: file_content.clone(),
                path: String::new(),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .file_id;

    // Pass entries in Git trailing-slash order (`"foo.rs"` before directory `"foo"`).
    let tree_id = BackendService::write_tree(
        &proxy,
        req_with_upstream(
            WriteTreeRequest {
                project_id: "proj".to_string(),
                path: String::new(),
                entries: vec![
                    TreeEntry {
                        name: "foo.rs".to_string(),
                        value: Some(TreeValue {
                            value: Some(tree_value::Value::File(File {
                                id: file_id.clone(),
                                executable: false,
                                copy_id: Vec::new(),
                                size_bytes: None,
                            })),
                        }),
                    },
                    TreeEntry {
                        name: "foo".to_string(),
                        value: Some(TreeValue {
                            value: Some(tree_value::Value::TreeId(EMPTY_TREE_ID_BYTES.to_vec())),
                        }),
                    },
                    TreeEntry {
                        name: "bar".to_string(),
                        value: Some(TreeValue {
                            value: Some(tree_value::Value::SymlinkId(vec![0xaa; 20])),
                        }),
                    },
                ],
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .tree_id;

    let cached_tree = BackendService::read_tree(
        &proxy,
        req_with_upstream(
            ReadTreeRequest {
                project_id: "proj".to_string(),
                path: String::new(),
                tree_id,
                page_size: 0,
                page_token: String::new(),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(mock.state.lock().unwrap().read_tree_calls, 0);
    let names: Vec<&str> = cached_tree
        .entries
        .iter()
        .map(|e| e.name.as_str())
        .collect();
    assert_eq!(names, vec!["bar", "foo", "foo.rs"]);
    let Some(tree_value::Value::File(f)) = cached_tree.entries[2]
        .value
        .as_ref()
        .and_then(|v| v.value.as_ref())
    else {
        panic!("expected file entry");
    };
    assert_eq!(f.size_bytes, Some(file_content.len() as u64));

    // When a file's size is not yet known locally, write_tree skips write-through so
    // read_tree fetches authoritative sizes from upstream.
    let unknown_bytes = Bytes::from_static(b"remote-only");
    let unknown_id = git_blob_id(&unknown_bytes);
    mock.state
        .lock()
        .unwrap()
        .files
        .insert(unknown_id.clone(), unknown_bytes.clone());
    let uncached_tree_id = BackendService::write_tree(
        &proxy,
        req_with_upstream(
            WriteTreeRequest {
                project_id: "proj".to_string(),
                path: String::new(),
                entries: vec![TreeEntry {
                    name: "remote.txt".to_string(),
                    value: Some(TreeValue {
                        value: Some(tree_value::Value::File(File {
                            id: unknown_id,
                            executable: false,
                            copy_id: Vec::new(),
                            size_bytes: None,
                        })),
                    }),
                }],
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .tree_id;
    let fetched = BackendService::read_tree(
        &proxy,
        req_with_upstream(
            ReadTreeRequest {
                project_id: "proj".to_string(),
                path: String::new(),
                tree_id: uncached_tree_id,
                page_size: 0,
                page_token: String::new(),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(mock.state.lock().unwrap().read_tree_calls, 1);
    let Some(tree_value::Value::File(remote_file)) = fetched.entries[0]
        .value
        .as_ref()
        .and_then(|v| v.value.as_ref())
    else {
        panic!("expected file entry");
    };
    assert_eq!(remote_file.size_bytes, Some(unknown_bytes.len() as u64));
}

#[tokio::test]
async fn read_tree_multipages_validates_entries_and_clamps_page_size() {
    let mock = MockUpstream::default();
    let entry = |name: &str| TreeEntry {
        name: name.to_string(),
        value: Some(TreeValue {
            value: Some(tree_value::Value::TreeId(EMPTY_TREE_ID_BYTES.to_vec())),
        }),
    };
    let file_entry = |name: &str, id: &[u8]| TreeEntry {
        name: name.to_string(),
        value: Some(TreeValue {
            value: Some(tree_value::Value::File(File {
                id: id.to_vec(),
                executable: false,
                copy_id: Vec::new(),
                size_bytes: Some(1),
            })),
        }),
    };

    let multi_id = vec![0x31; 20];
    let file_a = vec![0xa1; 20];
    let file_foo = vec![0xa2; 20];
    let file_foo_rs = vec![0xa3; 20];
    let file_z = vec![0xa4; 20];
    let empty_page_id = vec![0x32; 20];
    let mismatch_id = vec![0x33; 20];
    let dup_id = vec![0x34; 20];
    let bad_name_id = vec![0x35; 20];
    {
        let mut st = mock.state.lock().unwrap();
        st.trees.insert(
            multi_id.clone(),
            vec![
                ReadTreeResponse {
                    tree_id: multi_id.clone(),
                    entries: vec![file_entry("foo.rs", &file_foo_rs), file_entry("a", &file_a)],
                    next_page_token: "foo.rs".to_string(),
                },
                ReadTreeResponse {
                    tree_id: multi_id.clone(),
                    entries: vec![file_entry("z", &file_z), file_entry("foo", &file_foo)],
                    next_page_token: String::new(),
                },
            ],
        );
        st.trees.insert(
            empty_page_id.clone(),
            vec![ReadTreeResponse {
                tree_id: empty_page_id.clone(),
                entries: Vec::new(),
                next_page_token: "cursor".to_string(),
            }],
        );
        st.trees.insert(
            mismatch_id.clone(),
            vec![ReadTreeResponse {
                tree_id: vec![0x99; 20],
                entries: vec![entry("a")],
                next_page_token: String::new(),
            }],
        );
        st.trees.insert(
            dup_id.clone(),
            vec![
                ReadTreeResponse {
                    tree_id: dup_id.clone(),
                    entries: vec![entry("dup")],
                    next_page_token: "dup".to_string(),
                },
                ReadTreeResponse {
                    tree_id: dup_id.clone(),
                    entries: vec![entry("dup")],
                    next_page_token: String::new(),
                },
            ],
        );
        st.trees.insert(
            bad_name_id.clone(),
            vec![ReadTreeResponse {
                tree_id: bad_name_id.clone(),
                entries: vec![entry("../escape")],
                next_page_token: String::new(),
            }],
        );
    }

    let url = spawn_mock_upstream(mock.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 8 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new(Arc::clone(&cache), Arc::new(UpstreamPool::new(None)), None);
    let scope = CacheScope::new(&url, "proj");

    // Multi-page fetch combines and sorts into strict UTF-8 order, requests bounded pages,
    // accounts for every upstream page RPC, and authorizes only the tree itself (never
    // transitively authorizing child shared-CAS objects).
    let p1 = BackendService::read_tree(
        &proxy,
        req_with_upstream(
            ReadTreeRequest {
                project_id: "proj".to_string(),
                path: String::new(),
                tree_id: multi_id.clone(),
                page_size: 2,
                page_token: String::new(),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner();
    {
        let st = mock.state.lock().unwrap();
        assert_eq!(st.read_tree_calls, 2);
        assert_eq!(
            st.read_tree_page_sizes,
            vec![MAX_TREE_PAGE_SIZE as u32, MAX_TREE_PAGE_SIZE as u32]
        );
    }
    assert_eq!(cache.stats().upstream_rpcs, 2);
    assert_eq!(
        p1.entries
            .iter()
            .map(|e| e.name.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "foo"]
    );
    assert_eq!(p1.next_page_token, "foo");
    assert!(cache.is_authorized(&scope, ObjectKind::Tree, &multi_id));
    for child_file in [&file_a, &file_foo, &file_foo_rs, &file_z] {
        assert!(
            !cache.is_authorized(&scope, ObjectKind::File, child_file),
            "read_tree must never transitively authorize child shared-CAS objects"
        );
    }

    let p2 = BackendService::read_tree(
        &proxy,
        req_with_upstream(
            ReadTreeRequest {
                project_id: "proj".to_string(),
                path: String::new(),
                tree_id: multi_id,
                page_size: 2,
                page_token: p1.next_page_token,
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        mock.state.lock().unwrap().read_tree_calls,
        2,
        "second page must be served from cache"
    );
    assert_eq!(cache.stats().upstream_rpcs, 2);
    assert_eq!(
        p2.entries
            .iter()
            .map(|e| e.name.as_str())
            .collect::<Vec<_>>(),
        vec!["foo.rs", "z"]
    );
    assert!(p2.next_page_token.is_empty());
    for child_file in [&file_a, &file_foo, &file_foo_rs, &file_z] {
        assert!(
            !cache.is_authorized(&scope, ObjectKind::File, child_file),
            "cached read_tree page must not transitively authorize child files"
        );
    }

    for bad_id in [empty_page_id, mismatch_id, dup_id, bad_name_id] {
        let err = BackendService::read_tree(
            &proxy,
            req_with_upstream(
                ReadTreeRequest {
                    project_id: "proj".to_string(),
                    path: String::new(),
                    tree_id: bad_id,
                    page_size: 0,
                    page_token: String::new(),
                },
                &url,
                None,
            ),
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
    }

    for invalid_name in ["", ".", "..", "a/b", "a\0b"] {
        let err = validate_sorted_tree_entries(&[entry(invalid_name)]).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
    }

    // `slice_tree_page` clamps paginated requests to `MAX_TREE_PAGE_SIZE = 20_000`.
    let wide_entries: Vec<TreeEntry> = (0..(MAX_TREE_PAGE_SIZE + 5))
        .map(|i| entry(&format!("e{i:06}")))
        .collect();
    let wide = ReadTreeResponse {
        tree_id: vec![0x36; 20],
        entries: wide_entries,
        next_page_token: String::new(),
    };
    let unpaginated = slice_tree_page(wide.clone(), 0, "");
    assert_eq!(unpaginated.entries.len(), MAX_TREE_PAGE_SIZE + 5);
    assert!(unpaginated.next_page_token.is_empty());

    let clamped = slice_tree_page(wide.clone(), 50_000, "");
    assert_eq!(clamped.entries.len(), MAX_TREE_PAGE_SIZE);
    assert_eq!(
        clamped.next_page_token,
        format!("e{:06}", MAX_TREE_PAGE_SIZE - 1)
    );
    let tail = slice_tree_page(wide.clone(), 50_000, &clamped.next_page_token);
    assert_eq!(tail.entries.len(), 5);
    assert!(tail.next_page_token.is_empty());

    let token_zero_size = slice_tree_page(wide, 0, "e000000");
    assert_eq!(token_zero_size.entries.len(), MAX_TREE_PAGE_SIZE);
    assert!(!token_zero_size.next_page_token.is_empty());
}

#[tokio::test]
async fn read_tree_accepts_non_lexicographic_cursors_and_rejects_duplicate_cursor_cycles() {
    let mock = MockUpstream::default();
    let entry = |name: &str| TreeEntry {
        name: name.to_string(),
        value: Some(TreeValue {
            value: Some(tree_value::Value::TreeId(EMPTY_TREE_ID_BYTES.to_vec())),
        }),
    };

    // 3-page tree whose opaque/numeric cursors go `"9"` -> `"10"` -> `""` (where `"10" < "9"`
    // lexicographically) and whose entry names arrive in Git trailing-slash order.
    let numeric_cursor_tree = vec![0x61; 20];
    let cycle_tree = vec![0x62; 20];
    {
        let mut st = mock.state.lock().unwrap();
        st.trees.insert(
            numeric_cursor_tree.clone(),
            vec![
                ReadTreeResponse {
                    tree_id: numeric_cursor_tree.clone(),
                    entries: vec![entry("foo.rs")],
                    next_page_token: "9".to_string(),
                },
                ReadTreeResponse {
                    tree_id: numeric_cursor_tree.clone(),
                    entries: vec![entry("foo")],
                    next_page_token: "10".to_string(),
                },
                ReadTreeResponse {
                    tree_id: numeric_cursor_tree.clone(),
                    entries: vec![entry("z.rs")],
                    next_page_token: String::new(),
                },
            ],
        );
        st.trees.insert(
            cycle_tree.clone(),
            vec![
                ReadTreeResponse {
                    tree_id: cycle_tree.clone(),
                    entries: vec![entry("a")],
                    next_page_token: "tok-a".to_string(),
                },
                ReadTreeResponse {
                    tree_id: cycle_tree.clone(),
                    entries: vec![entry("b")],
                    next_page_token: "tok-a".to_string(),
                },
            ],
        );
    }

    let url = spawn_mock_upstream(mock.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 8 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new(Arc::clone(&cache), Arc::new(UpstreamPool::new(None)), None);

    let full = BackendService::read_tree(
        &proxy,
        req_with_upstream(
            ReadTreeRequest {
                project_id: "proj".to_string(),
                path: String::new(),
                tree_id: numeric_cursor_tree.clone(),
                page_size: 0,
                page_token: String::new(),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner();
    let names: Vec<&str> = full.entries.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(names, vec!["foo", "foo.rs", "z.rs"]);
    assert_eq!(mock.state.lock().unwrap().read_tree_calls, 3);

    // Cached hit on repeat ReadTree.
    let cached = BackendService::read_tree(
        &proxy,
        req_with_upstream(
            ReadTreeRequest {
                project_id: "proj".to_string(),
                path: String::new(),
                tree_id: numeric_cursor_tree,
                page_size: 0,
                page_token: String::new(),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(cached.entries.len(), 3);
    assert_eq!(mock.state.lock().unwrap().read_tree_calls, 3);

    // Repeated cursor cycle still fails with Internal.
    let err = BackendService::read_tree(
        &proxy,
        req_with_upstream(
            ReadTreeRequest {
                project_id: "proj".to_string(),
                path: String::new(),
                tree_id: cycle_tree,
                page_size: 0,
                page_token: String::new(),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Internal);
}

#[tokio::test]
async fn read_tree_paginated_giant_tree_falls_back_to_direct_upstream_passthrough() {
    let mock = MockUpstream::default();
    let entry = |name: &str| TreeEntry {
        name: name.to_string(),
        value: Some(TreeValue {
            value: Some(tree_value::Value::TreeId(EMPTY_TREE_ID_BYTES.to_vec())),
        }),
    };

    let giant_entries_tree = vec![0x71; 20];
    let giant_bytes_tree = vec![0x72; 20];
    {
        let mut st = mock.state.lock().unwrap();
        st.dynamic_trees.insert(
            giant_entries_tree.clone(),
            (0..6).map(|i| entry(&format!("entry-{i:02}"))).collect(),
        );
        st.dynamic_trees.insert(
            giant_bytes_tree.clone(),
            (0..3)
                .map(|i| entry(&format!("long-name-{i:02}-{}", "x".repeat(80))))
                .collect(),
        );
    }

    let url = spawn_mock_upstream(mock.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 8 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new_with_tree_limits(
        Arc::clone(&cache),
        Arc::new(UpstreamPool::new(None)),
        None,
        4,   // triggers entry-count cap on 6-entry tree
        200, // triggers serialized-byte cap on 3-entry long-name tree
    );
    let scope = CacheScope::new(&url, "proj");

    // Unpaginated ReadTree on a giant tree returns ResourceExhausted, and a repeat
    // unpaginated call fast-fails from `giant_trees` with a miss and zero extra upstream RPCs.
    let unpaginated_err = BackendService::read_tree(
        &proxy,
        req_with_upstream(
            ReadTreeRequest {
                project_id: "proj".to_string(),
                path: String::new(),
                tree_id: giant_entries_tree.clone(),
                page_size: 0,
                page_token: String::new(),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(unpaginated_err.code(), tonic::Code::ResourceExhausted);
    assert_eq!(cache.stats().misses, 1);
    assert_eq!(cache.stats().upstream_rpcs, 1);

    let repeat_unpaginated_err = BackendService::read_tree(
        &proxy,
        req_with_upstream(
            ReadTreeRequest {
                project_id: "proj".to_string(),
                path: String::new(),
                tree_id: giant_entries_tree.clone(),
                page_size: 0,
                page_token: String::new(),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(
        repeat_unpaginated_err.code(),
        tonic::Code::ResourceExhausted
    );
    assert_eq!(cache.stats().misses, 2);
    assert_eq!(
        cache.stats().upstream_rpcs,
        1,
        "repeat unpaginated call on a known giant tree must not increment upstream_rpcs"
    );

    // Paginated ReadTree on a giant tree (exceeding entry cap) falls back to forwarding
    // the caller's exact paginated request directly to upstream across all 3 pages.
    let mut collected = Vec::new();
    let mut token = String::new();
    loop {
        let page = BackendService::read_tree(
            &proxy,
            req_with_upstream(
                ReadTreeRequest {
                    project_id: "proj".to_string(),
                    path: String::new(),
                    tree_id: giant_entries_tree.clone(),
                    page_size: 2,
                    page_token: token,
                },
                &url,
                None,
            ),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(page.entries.len(), 2);
        collected.extend(page.entries.into_iter().map(|e| e.name));
        if page.next_page_token.is_empty() {
            break;
        }
        token = page.next_page_token;
    }
    assert_eq!(
        collected,
        vec![
            "entry-00", "entry-01", "entry-02", "entry-03", "entry-04", "entry-05"
        ]
    );
    assert!(
        !cache.is_authorized(&scope, ObjectKind::Tree, &giant_entries_tree),
        "giant tree must not be cached"
    );
    // 1 failed full-tree assembly attempt (from unpaginated call) + 3 direct paginated forwards.
    assert_eq!(
        mock.state.lock().unwrap().read_tree_requests,
        vec![
            (MAX_TREE_PAGE_SIZE as u32, String::new()),
            (2, String::new()),
            (2, "entry-01".to_string()),
            (2, "entry-03".to_string()),
        ]
    );
    // 2 unpaginated misses + 3 paginated fast-path misses = 5 misses; 1 initial full attempt + 3
    // paginated forwards = 4 upstream RPCs.
    assert_eq!(cache.stats().misses, 5);
    assert_eq!(cache.stats().upstream_rpcs, 4);

    // Paginated ReadTree exceeding serialized-byte cap also falls back directly to upstream.
    mock.state.lock().unwrap().read_tree_requests.clear();
    let first_byte_page = BackendService::read_tree(
        &proxy,
        req_with_upstream(
            ReadTreeRequest {
                project_id: "proj".to_string(),
                path: String::new(),
                tree_id: giant_bytes_tree.clone(),
                page_size: 1,
                page_token: String::new(),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(first_byte_page.entries.len(), 1);
    assert!(!first_byte_page.next_page_token.is_empty());
    assert!(!cache.is_authorized(&scope, ObjectKind::Tree, &giant_bytes_tree));
    assert_eq!(
        mock.state.lock().unwrap().read_tree_requests,
        vec![
            (MAX_TREE_PAGE_SIZE as u32, String::new()),
            (1, String::new()),
        ]
    );
}

#[tokio::test]
async fn read_tree_falls_back_to_unpaginated_when_upstream_rejects_page_size() {
    let mock = MockUpstream::default();
    let entry = |name: &str| TreeEntry {
        name: name.to_string(),
        value: Some(TreeValue {
            value: Some(tree_value::Value::TreeId(EMPTY_TREE_ID_BYTES.to_vec())),
        }),
    };

    let single_page_tree = vec![0x81; 20];
    let second_single_page_tree = vec![0x84; 20];
    let multi_page_tree = vec![0x82; 20];
    let paginated_only_tree = vec![0x83; 20];
    {
        let mut st = mock.state.lock().unwrap();
        st.reject_nonzero_tree_page_size = true;
        st.trees.insert(
            single_page_tree.clone(),
            vec![ReadTreeResponse {
                tree_id: single_page_tree.clone(),
                entries: vec![entry("foo.rs"), entry("foo")],
                next_page_token: String::new(),
            }],
        );
        st.trees.insert(
            second_single_page_tree.clone(),
            vec![ReadTreeResponse {
                tree_id: second_single_page_tree.clone(),
                entries: vec![entry("bar.txt")],
                next_page_token: String::new(),
            }],
        );
        st.trees.insert(
            multi_page_tree.clone(),
            vec![
                ReadTreeResponse {
                    tree_id: multi_page_tree.clone(),
                    entries: vec![entry("b")],
                    next_page_token: "p1".to_string(),
                },
                ReadTreeResponse {
                    tree_id: multi_page_tree.clone(),
                    entries: vec![entry("a")],
                    next_page_token: String::new(),
                },
            ],
        );
        st.trees.insert(
            paginated_only_tree.clone(),
            vec![ReadTreeResponse {
                tree_id: paginated_only_tree.clone(),
                entries: vec![entry("x")],
                next_page_token: String::new(),
            }],
        );
    }

    let url = spawn_mock_upstream(mock.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 8 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new(Arc::clone(&cache), Arc::new(UpstreamPool::new(None)), None);

    // Cold unpaginated ReadTree falls back to `page_size: 0` on `UNIMPLEMENTED`, sorts,
    // caches, and stops when `next_page_token` is empty.
    let cold = BackendService::read_tree(
        &proxy,
        req_with_upstream(
            ReadTreeRequest {
                project_id: "proj".to_string(),
                path: String::new(),
                tree_id: single_page_tree.clone(),
                page_size: 0,
                page_token: String::new(),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        cold.entries
            .iter()
            .map(|e| e.name.as_str())
            .collect::<Vec<_>>(),
        vec!["foo", "foo.rs"]
    );
    assert!(cold.next_page_token.is_empty());
    assert_eq!(
        mock.state.lock().unwrap().read_tree_requests,
        vec![
            (MAX_TREE_PAGE_SIZE as u32, String::new()),
            (0, String::new()),
        ]
    );
    assert_eq!(cache.stats().upstream_rpcs, 2);

    // Warm read (both unpaginated and paginated) hits cache with zero extra upstream RPCs.
    let warm = BackendService::read_tree(
        &proxy,
        req_with_upstream(
            ReadTreeRequest {
                project_id: "proj".to_string(),
                path: String::new(),
                tree_id: single_page_tree.clone(),
                page_size: 0,
                page_token: String::new(),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(warm.entries.len(), 2);
    let warm_page = BackendService::read_tree(
        &proxy,
        req_with_upstream(
            ReadTreeRequest {
                project_id: "proj".to_string(),
                path: String::new(),
                tree_id: single_page_tree,
                page_size: 1,
                page_token: String::new(),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        warm_page
            .entries
            .iter()
            .map(|e| e.name.as_str())
            .collect::<Vec<_>>(),
        vec!["foo"]
    );
    assert_eq!(warm_page.next_page_token, "foo");
    assert_eq!(mock.state.lock().unwrap().read_tree_calls, 2);
    assert_eq!(cache.stats().upstream_rpcs, 2);

    // Subsequent cold unpaginated ReadTree on the same origin remembers the negative
    // capability and starts directly at `page_size: 0` (1 upstream RPC instead of 2).
    mock.state.lock().unwrap().read_tree_requests.clear();
    let second_cold = BackendService::read_tree(
        &proxy,
        req_with_upstream(
            ReadTreeRequest {
                project_id: "proj".to_string(),
                path: String::new(),
                tree_id: second_single_page_tree,
                page_size: 0,
                page_token: String::new(),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(second_cold.entries.len(), 1);
    assert_eq!(
        mock.state.lock().unwrap().read_tree_requests,
        vec![(0, String::new())]
    );
    assert_eq!(cache.stats().upstream_rpcs, 3);

    // If the `page_size: 0` response returns a non-empty `next_page_token`, paging
    // continues until empty without re-probing `MAX_TREE_PAGE_SIZE`.
    mock.state.lock().unwrap().read_tree_requests.clear();
    let multi = BackendService::read_tree(
        &proxy,
        req_with_upstream(
            ReadTreeRequest {
                project_id: "proj".to_string(),
                path: String::new(),
                tree_id: multi_page_tree,
                page_size: 0,
                page_token: String::new(),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        multi
            .entries
            .iter()
            .map(|e| e.name.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b"]
    );
    assert_eq!(
        mock.state.lock().unwrap().read_tree_requests,
        vec![(0, String::new()), (0, "p1".to_string())]
    );

    // Cold paginated caller (`page_size > 0`) still probes upstream and propagates
    // `UNIMPLEMENTED` without retrying.
    mock.state.lock().unwrap().read_tree_requests.clear();
    let err = BackendService::read_tree(
        &proxy,
        req_with_upstream(
            ReadTreeRequest {
                project_id: "proj".to_string(),
                path: String::new(),
                tree_id: paginated_only_tree,
                page_size: 1,
                page_token: String::new(),
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unimplemented);
    assert_eq!(
        mock.state.lock().unwrap().read_tree_requests,
        vec![(MAX_TREE_PAGE_SIZE as u32, String::new())]
    );
}

#[tokio::test]
async fn read_tree_giant_tree_coalesced_waiters_fail_fast_without_redundant_full_fetches() {
    use tokio::sync::Notify;

    struct GatedGiantTreeUpstream {
        entries: Vec<TreeEntry>,
        first_full_entered: Arc<Notify>,
        release_first_full: Arc<Notify>,
        full_fetch_calls: Arc<AtomicUsize>,
        paginated_calls: Arc<AtomicUsize>,
    }

    #[tonic::async_trait]
    impl BackendService for GatedGiantTreeUpstream {
        async fn register_project(
            &self,
            _: Request<RegisterProjectRequest>,
        ) -> Result<Response<RegisterProjectResponse>, Status> {
            unused()
        }
        async fn register_repository(
            &self,
            _: Request<RegisterRepositoryRequest>,
        ) -> Result<Response<RegisterRepositoryResponse>, Status> {
            unused()
        }
        async fn delete_repository(
            &self,
            _: Request<DeleteRepositoryRequest>,
        ) -> Result<Response<DeleteRepositoryResponse>, Status> {
            unused()
        }
        async fn read_commit(
            &self,
            _: Request<ReadCommitRequest>,
        ) -> Result<Response<ReadCommitResponse>, Status> {
            unused()
        }
        async fn write_commit(
            &self,
            _: Request<WriteCommitRequest>,
        ) -> Result<Response<WriteCommitResponse>, Status> {
            unused()
        }
        async fn list_commits(
            &self,
            _: Request<ListCommitsRequest>,
        ) -> Result<Response<ListCommitsResponse>, Status> {
            unused()
        }
        async fn resolve_commit_prefix(
            &self,
            _: Request<ResolveCommitPrefixRequest>,
        ) -> Result<Response<ResolveCommitPrefixResponse>, Status> {
            unused()
        }
        async fn resolve_change_prefix(
            &self,
            _: Request<ResolveChangePrefixRequest>,
        ) -> Result<Response<ResolveChangePrefixResponse>, Status> {
            unused()
        }
        async fn read_tree(
            &self,
            request: Request<ReadTreeRequest>,
        ) -> Result<Response<ReadTreeResponse>, Status> {
            let req = request.into_inner();
            if req.page_size == MAX_TREE_PAGE_SIZE as u32 {
                let prev = self.full_fetch_calls.fetch_add(1, Ordering::SeqCst);
                if prev == 0 {
                    self.first_full_entered.notify_one();
                    self.release_first_full.notified().await;
                }
            } else {
                self.paginated_calls.fetch_add(1, Ordering::SeqCst);
            }
            let full = ReadTreeResponse {
                tree_id: req.tree_id,
                entries: self.entries.clone(),
                next_page_token: String::new(),
            };
            Ok(Response::new(slice_tree_page(
                full,
                req.page_size,
                &req.page_token,
            )))
        }
        async fn write_tree(
            &self,
            _: Request<WriteTreeRequest>,
        ) -> Result<Response<WriteTreeResponse>, Status> {
            unused()
        }
        type ReadFileStream =
            Pin<Box<dyn Stream<Item = Result<ReadFileResponse, Status>> + Send + 'static>>;
        async fn read_file(
            &self,
            _: Request<ReadFileRequest>,
        ) -> Result<Response<Self::ReadFileStream>, Status> {
            unused()
        }
        async fn write_file(
            &self,
            _: Request<WriteFileRequest>,
        ) -> Result<Response<WriteFileResponse>, Status> {
            unused()
        }
        async fn write_file_stream(
            &self,
            _: Request<Streaming<WriteFileChunk>>,
        ) -> Result<Response<WriteFileResponse>, Status> {
            unused()
        }
        async fn read_symlink(
            &self,
            _: Request<ReadSymlinkRequest>,
        ) -> Result<Response<ReadSymlinkResponse>, Status> {
            unused()
        }
        async fn write_symlink(
            &self,
            _: Request<WriteSymlinkRequest>,
        ) -> Result<Response<WriteSymlinkResponse>, Status> {
            unused()
        }
    }

    let entries: Vec<TreeEntry> = (0..6)
        .map(|i| TreeEntry {
            name: format!("e-{i:02}"),
            value: Some(TreeValue {
                value: Some(tree_value::Value::TreeId(EMPTY_TREE_ID_BYTES.to_vec())),
            }),
        })
        .collect();
    let first_full_entered = Arc::new(Notify::new());
    let release_first_full = Arc::new(Notify::new());
    let full_fetch_calls = Arc::new(AtomicUsize::new(0));
    let paginated_calls = Arc::new(AtomicUsize::new(0));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let incoming =
        tonic::transport::server::TcpIncoming::from_listener(listener, true, None).unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(BackendServiceServer::new(GatedGiantTreeUpstream {
                entries,
                first_full_entered: Arc::clone(&first_full_entered),
                release_first_full: Arc::clone(&release_first_full),
                full_fetch_calls: Arc::clone(&full_fetch_calls),
                paginated_calls: Arc::clone(&paginated_calls),
            }))
            .serve_with_incoming(incoming),
    );

    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 8 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new_with_tree_limits(
        Arc::clone(&cache),
        Arc::new(UpstreamPool::new(None)),
        None,
        4,
        MAX_GRPC_MESSAGE_BYTES,
    );
    let giant_id = vec![0x84; 20];

    let leader = tokio::spawn({
        let proxy = proxy.clone();
        let url = url.clone();
        let giant_id = giant_id.clone();
        async move {
            BackendService::read_tree(
                &proxy,
                req_with_upstream(
                    ReadTreeRequest {
                        project_id: "proj".to_string(),
                        path: String::new(),
                        tree_id: giant_id,
                        page_size: 2,
                        page_token: String::new(),
                    },
                    &url,
                    None,
                ),
            )
            .await
        }
    });

    first_full_entered.notified().await;

    let waiter = tokio::spawn({
        let proxy = proxy.clone();
        let url = url.clone();
        let giant_id = giant_id.clone();
        async move {
            BackendService::read_tree(
                &proxy,
                req_with_upstream(
                    ReadTreeRequest {
                        project_id: "proj".to_string(),
                        path: String::new(),
                        tree_id: giant_id,
                        page_size: 2,
                        page_token: "e-01".to_string(),
                    },
                    &url,
                    None,
                ),
            )
            .await
        }
    });

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    while cache.stats().singleflight_coalesced < 1 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for waiter to coalesce"
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    release_first_full.notify_one();

    let p1 = leader.await.unwrap().unwrap().into_inner();
    let p2 = waiter.await.unwrap().unwrap().into_inner();
    assert_eq!(
        p1.entries
            .iter()
            .map(|e| e.name.as_str())
            .collect::<Vec<_>>(),
        vec!["e-00", "e-01"]
    );
    assert_eq!(
        p2.entries
            .iter()
            .map(|e| e.name.as_str())
            .collect::<Vec<_>>(),
        vec!["e-02", "e-03"]
    );
    assert_eq!(
        full_fetch_calls.load(Ordering::SeqCst),
        1,
        "coalesced waiter must fail fast on giant_trees without issuing a second full fetch"
    );
    assert_eq!(paginated_calls.load(Ordering::SeqCst), 2);
    // 1 aborted full fetch + 2 paginated forwards = 3 upstream RPCs; 1 miss per caller = 2.
    assert_eq!(cache.stats().upstream_rpcs, 3);
    assert_eq!(cache.stats().misses, 2);
}

#[tokio::test]
async fn write_tree_skips_write_through_when_exceeding_tree_cache_limits() {
    let mock = MockUpstream::default();
    let url = spawn_mock_upstream(mock.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 8 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new_with_tree_limits(
        Arc::clone(&cache),
        Arc::new(UpstreamPool::new(None)),
        None,
        2,   // max_cached_tree_entries
        120, // max_cached_tree_bytes
    );
    let scope = CacheScope::new(&url, "proj");
    let dir_entry = |name: &str| TreeEntry {
        name: name.to_string(),
        value: Some(TreeValue {
            value: Some(tree_value::Value::TreeId(EMPTY_TREE_ID_BYTES.to_vec())),
        }),
    };

    // Tree exceeding `max_cached_tree_entries` succeeds on upstream without write-through.
    let too_many_id = BackendService::write_tree(
        &proxy,
        req_with_upstream(
            WriteTreeRequest {
                project_id: "proj".to_string(),
                path: String::new(),
                entries: vec![dir_entry("a"), dir_entry("b"), dir_entry("c")],
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .tree_id;
    assert_eq!(cache.stats().write_through_objects, 0);
    assert!(!cache.is_authorized(&scope, ObjectKind::Tree, &too_many_id));
    assert!(
        !cache
            .object_path(&scope, ObjectKind::Tree, &too_many_id)
            .exists()
    );

    // Tree within entry cap but exceeding `max_cached_tree_bytes` skips write-through.
    let too_large_bytes_id = BackendService::write_tree(
        &proxy,
        req_with_upstream(
            WriteTreeRequest {
                project_id: "proj".to_string(),
                path: String::new(),
                entries: vec![
                    dir_entry(&format!("long-a-{}", "x".repeat(60))),
                    dir_entry(&format!("long-b-{}", "y".repeat(60))),
                ],
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .tree_id;
    assert_eq!(cache.stats().write_through_objects, 0);
    assert!(!cache.is_authorized(&scope, ObjectKind::Tree, &too_large_bytes_id));
    assert!(
        !cache
            .object_path(&scope, ObjectKind::Tree, &too_large_bytes_id)
            .exists()
    );

    // Tree within both caps still writes through normally.
    let small_id = BackendService::write_tree(
        &proxy,
        req_with_upstream(
            WriteTreeRequest {
                project_id: "proj".to_string(),
                path: String::new(),
                entries: vec![dir_entry("ok")],
            },
            &url,
            None,
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .tree_id;
    assert_eq!(cache.stats().write_through_objects, 1);
    assert!(cache.is_authorized(&scope, ObjectKind::Tree, &small_id));
}

#[tokio::test]
async fn read_tree_unpaginated_waiter_coalesced_on_paginated_leader_retries_after_unimplemented() {
    use tokio::sync::Notify;

    struct GatedUnpaginatedUpstream {
        entries: Vec<TreeEntry>,
        first_paginated_entered: Arc<Notify>,
        release_first_paginated: Arc<Notify>,
        calls: Arc<AtomicUsize>,
    }

    #[tonic::async_trait]
    impl BackendService for GatedUnpaginatedUpstream {
        async fn register_project(
            &self,
            _: Request<RegisterProjectRequest>,
        ) -> Result<Response<RegisterProjectResponse>, Status> {
            unused()
        }
        async fn register_repository(
            &self,
            _: Request<RegisterRepositoryRequest>,
        ) -> Result<Response<RegisterRepositoryResponse>, Status> {
            unused()
        }
        async fn delete_repository(
            &self,
            _: Request<DeleteRepositoryRequest>,
        ) -> Result<Response<DeleteRepositoryResponse>, Status> {
            unused()
        }
        async fn read_commit(
            &self,
            _: Request<ReadCommitRequest>,
        ) -> Result<Response<ReadCommitResponse>, Status> {
            unused()
        }
        async fn write_commit(
            &self,
            _: Request<WriteCommitRequest>,
        ) -> Result<Response<WriteCommitResponse>, Status> {
            unused()
        }
        async fn list_commits(
            &self,
            _: Request<ListCommitsRequest>,
        ) -> Result<Response<ListCommitsResponse>, Status> {
            unused()
        }
        async fn resolve_commit_prefix(
            &self,
            _: Request<ResolveCommitPrefixRequest>,
        ) -> Result<Response<ResolveCommitPrefixResponse>, Status> {
            unused()
        }
        async fn resolve_change_prefix(
            &self,
            _: Request<ResolveChangePrefixRequest>,
        ) -> Result<Response<ResolveChangePrefixResponse>, Status> {
            unused()
        }
        async fn read_tree(
            &self,
            request: Request<ReadTreeRequest>,
        ) -> Result<Response<ReadTreeResponse>, Status> {
            let req = request.into_inner();
            let prev = self.calls.fetch_add(1, Ordering::SeqCst);
            if req.page_size > 0 {
                if prev == 0 {
                    self.first_paginated_entered.notify_one();
                    self.release_first_paginated.notified().await;
                }
                return Err(Status::unimplemented("paginated ReadTree unsupported"));
            }
            Ok(Response::new(ReadTreeResponse {
                tree_id: req.tree_id,
                entries: self.entries.clone(),
                next_page_token: String::new(),
            }))
        }
        async fn write_tree(
            &self,
            _: Request<WriteTreeRequest>,
        ) -> Result<Response<WriteTreeResponse>, Status> {
            unused()
        }
        type ReadFileStream =
            Pin<Box<dyn Stream<Item = Result<ReadFileResponse, Status>> + Send + 'static>>;
        async fn read_file(
            &self,
            _: Request<ReadFileRequest>,
        ) -> Result<Response<Self::ReadFileStream>, Status> {
            unused()
        }
        async fn write_file(
            &self,
            _: Request<WriteFileRequest>,
        ) -> Result<Response<WriteFileResponse>, Status> {
            unused()
        }
        async fn write_file_stream(
            &self,
            _: Request<Streaming<WriteFileChunk>>,
        ) -> Result<Response<WriteFileResponse>, Status> {
            unused()
        }
        async fn read_symlink(
            &self,
            _: Request<ReadSymlinkRequest>,
        ) -> Result<Response<ReadSymlinkResponse>, Status> {
            unused()
        }
        async fn write_symlink(
            &self,
            _: Request<WriteSymlinkRequest>,
        ) -> Result<Response<WriteSymlinkResponse>, Status> {
            unused()
        }
    }

    let first_paginated_entered = Arc::new(Notify::new());
    let release_first_paginated = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let incoming =
        tonic::transport::server::TcpIncoming::from_listener(listener, true, None).unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(BackendServiceServer::new(GatedUnpaginatedUpstream {
                entries: vec![TreeEntry {
                    name: "hello.txt".to_string(),
                    value: Some(TreeValue {
                        value: Some(tree_value::Value::TreeId(EMPTY_TREE_ID_BYTES.to_vec())),
                    }),
                }],
                first_paginated_entered: Arc::clone(&first_paginated_entered),
                release_first_paginated: Arc::clone(&release_first_paginated),
                calls: Arc::clone(&calls),
            }))
            .serve_with_incoming(incoming),
    );

    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 8 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new(Arc::clone(&cache), Arc::new(UpstreamPool::new(None)), None);
    let tree_id = vec![0x95; 20];

    // Cold paginated caller leads and hits `UNIMPLEMENTED`.
    let paginated_leader = tokio::spawn({
        let proxy = proxy.clone();
        let url = url.clone();
        let tree_id = tree_id.clone();
        async move {
            BackendService::read_tree(
                &proxy,
                req_with_upstream(
                    ReadTreeRequest {
                        project_id: "proj".to_string(),
                        path: String::new(),
                        tree_id,
                        page_size: 1,
                        page_token: String::new(),
                    },
                    &url,
                    None,
                ),
            )
            .await
        }
    });
    first_paginated_entered.notified().await;

    // Cold unpaginated caller coalesces onto the paginated leader.
    let unpaginated_waiter = tokio::spawn({
        let proxy = proxy.clone();
        let url = url.clone();
        let tree_id = tree_id.clone();
        async move {
            BackendService::read_tree(
                &proxy,
                req_with_upstream(
                    ReadTreeRequest {
                        project_id: "proj".to_string(),
                        path: String::new(),
                        tree_id,
                        page_size: 0,
                        page_token: String::new(),
                    },
                    &url,
                    None,
                ),
            )
            .await
        }
    });

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    while cache.stats().singleflight_coalesced < 1 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for unpaginated caller to coalesce"
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    release_first_paginated.notify_one();

    let leader_err = paginated_leader.await.unwrap().unwrap_err();
    assert_eq!(leader_err.code(), tonic::Code::Unimplemented);

    let waiter_resp = unpaginated_waiter.await.unwrap().unwrap().into_inner();
    assert_eq!(waiter_resp.entries.len(), 1);
    assert_eq!(waiter_resp.entries[0].name, "hello.txt");
    // 1 paginated probe (`UNIMPLEMENTED`) + 1 unpaginated retry (`page_size: 0`).
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn write_tree_skips_write_through_for_invalid_or_duplicate_entry_names() {
    let mock = MockUpstream::default();
    let url = spawn_mock_upstream(mock.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(TwoTierCache::new(dir.path().to_path_buf(), 8 * 1024 * 1024).unwrap());
    let proxy = DaemonProxy::new(Arc::clone(&cache), Arc::new(UpstreamPool::new(None)), None);
    let scope = CacheScope::new(&url, "proj");
    let dir_entry = |name: &str| TreeEntry {
        name: name.to_string(),
        value: Some(TreeValue {
            value: Some(tree_value::Value::TreeId(EMPTY_TREE_ID_BYTES.to_vec())),
        }),
    };

    for bad_entries in [
        vec![dir_entry("dup"), dir_entry("dup")],
        vec![dir_entry("../escape")],
        vec![dir_entry("a/b")],
    ] {
        let tree_id = BackendService::write_tree(
            &proxy,
            req_with_upstream(
                WriteTreeRequest {
                    project_id: "proj".to_string(),
                    path: String::new(),
                    entries: bad_entries,
                },
                &url,
                None,
            ),
        )
        .await
        .unwrap()
        .into_inner()
        .tree_id;
        assert!(!cache.is_authorized(&scope, ObjectKind::Tree, &tree_id));
        assert_eq!(cache.stats().write_through_objects, 0);
    }
}
