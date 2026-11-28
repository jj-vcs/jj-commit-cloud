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

use super::*;

#[test]
fn test_encode_reverse_hex_matches_jj_alphabet() {
    assert_eq!(
        encode_reverse_hex(b"\x01\x23\x45\x67\x89\xab\xcd\xef"),
        "zyxwvutsrqponmlk"
    );
}

#[test]
fn test_compute_git_commit_hash_distinguishes_conflict_trees_and_signatures() {
    let base_tree = cc_common::EMPTY_TREE_ID_BYTES.to_vec();
    let side_a = vec![0x11; 20];
    let side_b = vec![0x22; 20];
    let side_c = vec![0x33; 20];

    let mut commit_ab = cc_common::backend::Commit {
        commit_id: vec![],
        change_id: b"\x01\x23\x45\x67\x89\xab\xcd\xef\x01\x23\x45\x67\x89\xab\xcd\xef".to_vec(),
        parent_commit_ids: vec![],
        root_tree_id: vec![side_a.clone(), base_tree.clone(), side_b],
        description: "conflicted commit\n".to_string(),
        author: None,
        committer: None,
        predecessors: vec![],
        conflict_labels: vec!["side 1".into(), "base".into(), "side 2".into()],
        secure_sig: None,
        raw_git_commit: vec![],
        external_ids: Default::default(),
    };
    let mut commit_ac = commit_ab.clone();
    commit_ac.root_tree_id = vec![side_a, base_tree, side_c];

    let hash_ab = compute_git_commit_hash(&commit_ab).unwrap();
    let hash_ac = compute_git_commit_hash(&commit_ac).unwrap();
    assert_ne!(hash_ab, hash_ac);

    commit_ab.secure_sig = Some(cc_common::backend::SecureSig {
        data: b"payload".to_vec(),
        sig: b"-----BEGIN PGP SIGNATURE-----\ntest\n-----END PGP SIGNATURE-----\n".to_vec(),
    });
    let hash_signed = compute_git_commit_hash(&commit_ab).unwrap();
    assert_ne!(hash_ab, hash_signed);
}

#[test]
fn test_verify_or_compute_git_commit_hash_rejects_forged_digest() {
    let mut commit = cc_common::backend::Commit {
        commit_id: vec![0xde; 20],
        change_id: vec![0x01; 16],
        parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
        description: "honest description\n".to_string(),
        author: None,
        committer: None,
        predecessors: vec![],
        conflict_labels: vec![],
        secure_sig: None,
        raw_git_commit: vec![],
        external_ids: Default::default(),
    };
    assert!(verify_or_compute_git_commit_hash(&mut commit).is_err());

    commit.commit_id.clear();
    let canonical = verify_or_compute_git_commit_hash(&mut commit).unwrap();
    commit.commit_id = canonical.clone();
    assert_eq!(
        verify_or_compute_git_commit_hash(&mut commit).unwrap(),
        canonical
    );
}

use cc_common::backend::tree_value::Value;
use cc_common::backend::{Commit, File, SecureSig, Signature, Timestamp, TreeEntry, TreeValue};

const TREE: [u8; 20] = [0x11; 20];
const PARENT: [u8; 20] = [0x22; 20];

fn signature(name: &str, email: &str, millis_since_epoch: i64, tz_offset: i32) -> Signature {
    Signature {
        name: name.to_string(),
        email: email.to_string(),
        timestamp: Some(Timestamp {
            millis_since_epoch,
            tz_offset,
        }),
    }
}

fn test_commit() -> Commit {
    Commit {
        commit_id: vec![],
        change_id: vec![0x01; 16],
        parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
        root_tree_id: vec![TREE.to_vec()],
        description: "test commit\n".to_string(),
        author: Some(signature(
            "Author",
            "author@example.com",
            1_700_000_000_000,
            60,
        )),
        committer: Some(signature(
            "Committer",
            "committer@example.com",
            1_700_000_000_000,
            0,
        )),
        predecessors: vec![],
        conflict_labels: vec![String::new()],
        secure_sig: None,
        raw_git_commit: vec![],
        external_ids: Default::default(),
    }
}

fn entry(name: &str, value: Option<Value>) -> TreeEntry {
    TreeEntry {
        name: name.to_string(),
        value: Some(TreeValue { value }),
    }
}

fn file(name: &str) -> TreeEntry {
    let id = vec![0x33; 20];
    entry(
        name,
        Some(Value::File(File {
            id,
            executable: false,
            copy_id: vec![],
            size_bytes: None,
        })),
    )
}

#[test]
fn test_compute_git_tree_hash_rejects_entries_the_digest_cannot_cover() {
    assert_eq!(
        compute_git_tree_hash(&[]).unwrap(),
        cc_common::EMPTY_TREE_ID_BYTES
    );
    let tree_id = compute_git_tree_hash(&[file("a.txt")]).unwrap();
    let submodule = entry("sub", Some(Value::GitSubmoduleId(vec![0x44; 20])));
    assert_ne!(
        compute_git_tree_hash(&[file("a.txt"), submodule]).unwrap(),
        tree_id
    );

    let invalid_trees = [
        // Re-sending a tree's entries plus one the digest would skip.
        vec![
            file("a.txt"),
            entry("", Some(Value::GitSubmoduleId(vec![]))),
        ],
        vec![
            file("a.txt"),
            entry("x", Some(Value::GitSubmoduleId(vec![0x01]))),
        ],
        vec![
            file("a.txt"),
            entry("x", Some(Value::SymlinkId(vec![0x55; 19]))),
        ],
        vec![
            file("a.txt"),
            entry("x", Some(Value::TreeId(vec![0x66; 32]))),
        ],
        vec![
            file("a.txt"),
            entry("x", Some(Value::ConflictId(vec![0x77; 20]))),
        ],
        vec![file("a.txt"), entry("x", None)],
        vec![
            file("a.txt"),
            TreeEntry {
                name: "x".to_string(),
                value: None,
            },
        ],
        vec![file("a.txt"), file("a.txt")],
        vec![
            file("a.txt"),
            entry("a.txt", Some(Value::TreeId(TREE.to_vec()))),
        ],
        vec![file(".")],
        vec![file("..")],
        vec![file("dir/a.txt")],
        vec![file("a\0b")],
    ];
    for entries in &invalid_trees {
        assert!(
            compute_git_tree_hash(entries).is_err(),
            "accepted {entries:?}"
        );
    }
}

#[test]
fn test_serialize_canonical_git_commit_reports_unrepresentable_fields() {
    let base = test_commit();
    assert!(serialize_canonical_git_commit(&base, true).is_ok());

    let mut invalid = Vec::new();
    let mut commit = base.clone();
    commit.author = Some(signature("Mallory <m@example.com>", "a@example.com", 0, 0));
    invalid.push(commit);
    let mut commit = base.clone();
    commit.committer = Some(signature("Committer", "c@example.com\nparent 00", 0, 0));
    invalid.push(commit);
    let mut commit = base.clone();
    commit.author = Some(signature("Author", "a@example.com", 0, i32::MAX));
    invalid.push(commit);
    let mut commit = base.clone();
    commit.author = Some(signature("Author", "a@example.com", 0, 100 * 60));
    invalid.push(commit);
    let mut commit = base.clone();
    commit.root_tree_id = vec![vec![0x11; 19]];
    invalid.push(commit);
    let mut commit = base.clone();
    commit.parent_commit_ids = vec![vec![0x22; 5]];
    invalid.push(commit);
    for commit in &invalid {
        assert!(
            serialize_canonical_git_commit(commit, true).is_err(),
            "accepted {commit:?}"
        );
    }
}

#[test]
fn test_signature_timestamps_round_toward_negative_infinity() {
    let mut commit = test_commit();
    commit.author = Some(signature("A", "a@example.com", -1_500, 0));
    commit.committer = Some(signature("C", "c@example.com", 1_500, 0));
    let text = String::from_utf8(serialize_canonical_git_commit(&commit, false).unwrap()).unwrap();
    assert!(
        text.contains("\nauthor A <a@example.com> -2 +0000\n"),
        "{text}"
    );
    assert!(
        text.contains("\ncommitter C <c@example.com> 1 +0000\n"),
        "{text}"
    );
}

#[test]
fn test_verify_or_compute_git_commit_hash_rejects_malformed_commits() {
    let three_trees = vec![TREE.to_vec(), vec![0x12; 20], vec![0x13; 20]];
    let labels = |values: &[&str]| values.iter().map(|v| v.to_string()).collect::<Vec<_>>();
    let check = |mutate: &dyn Fn(&mut Commit)| {
        let mut commit = test_commit();
        mutate(&mut commit);
        verify_or_compute_git_commit_hash(&mut commit)
    };

    assert!(check(&|_| {}).is_ok());
    assert!(check(&|c| c.conflict_labels.clear()).is_ok());
    // jj sends a single empty label for an unlabeled conflict.
    assert!(check(&|c| c.root_tree_id = three_trees.clone()).is_ok());
    assert!(
        check(&|c| {
            c.root_tree_id = three_trees.clone();
            c.conflict_labels = labels(&["left", "base", "right"]);
        })
        .is_ok()
    );

    assert!(check(&|c| c.root_tree_id.clear()).is_err());
    assert!(check(&|c| c.root_tree_id.push(vec![0x12; 20])).is_err());
    assert!(check(&|c| c.root_tree_id = vec![vec![0x11; 19]]).is_err());
    assert!(check(&|c| c.conflict_labels = labels(&["label"])).is_err());
    assert!(
        check(&|c| {
            c.root_tree_id = three_trees.clone();
            c.conflict_labels = labels(&["left", "right"]);
        })
        .is_err()
    );
    assert!(
        check(&|c| {
            c.root_tree_id = three_trees.clone();
            c.conflict_labels = labels(&["left\nbase", "right", ""]);
        })
        .is_err()
    );
    assert!(check(&|c| c.parent_commit_ids.clear()).is_err());
    assert!(check(&|c| c.parent_commit_ids = vec![vec![0x22; 19]]).is_err());
    assert!(check(&|c| c.change_id.clear()).is_err());
    assert!(check(&|c| c.change_id = vec![0x01; 20]).is_err());
    assert!(check(&|c| c.predecessors = vec![vec![0x44; 3]]).is_err());

    // Legacy `[""]` and all-empty per-tree labels canonicalize to `[]` so stored
    // commits stay uniform regardless of client version.
    let mut legacy_resolved = test_commit();
    legacy_resolved.conflict_labels = labels(&[""]);
    assert!(verify_or_compute_git_commit_hash(&mut legacy_resolved).is_ok());
    assert!(legacy_resolved.conflict_labels.is_empty());

    let mut all_empty_conflict = test_commit();
    all_empty_conflict.root_tree_id = three_trees.clone();
    all_empty_conflict.conflict_labels = labels(&["", "", ""]);
    assert!(verify_or_compute_git_commit_hash(&mut all_empty_conflict).is_ok());
    assert!(all_empty_conflict.conflict_labels.is_empty());

    let mut partial_conflict = test_commit();
    partial_conflict.root_tree_id = three_trees;
    partial_conflict.conflict_labels = labels(&["left", "", "right"]);
    assert!(verify_or_compute_git_commit_hash(&mut partial_conflict).is_ok());
    assert_eq!(
        partial_conflict.conflict_labels,
        labels(&["left", "", "right"])
    );
}

#[test]
fn test_secure_sig_data_is_derived_on_the_server_and_cannot_be_replayed() {
    let sig_a = b"-----BEGIN SSH SIGNATURE-----\nAAAA\n-----END SSH SIGNATURE-----\n".to_vec();
    let mut commit_a = test_commit();
    let data_a = serialize_canonical_git_commit(&commit_a, false).unwrap();
    commit_a.secure_sig = Some(SecureSig {
        data: data_a.clone(),
        sig: sig_a.clone(),
    });
    verify_or_compute_git_commit_hash(&mut commit_a).unwrap();
    assert_eq!(commit_a.secure_sig.unwrap().data, data_a);

    // Replaying A's payload and signature onto another commit is rejected.
    let mut commit_m = test_commit();
    commit_m.description = "unreviewed change\n".to_string();
    commit_m.secure_sig = Some(SecureSig {
        data: data_a.clone(),
        sig: sig_a.clone(),
    });
    assert!(verify_or_compute_git_commit_hash(&mut commit_m).is_err());

    // Without client-supplied data, the server stores M's own payload, which A's signature
    // does not sign.
    commit_m.secure_sig = Some(SecureSig {
        data: vec![],
        sig: sig_a,
    });
    verify_or_compute_git_commit_hash(&mut commit_m).unwrap();
    let stored_data = commit_m.secure_sig.clone().unwrap().data;
    assert_eq!(
        stored_data,
        serialize_canonical_git_commit(&commit_m, false).unwrap()
    );
    assert_ne!(stored_data, data_a);

    // A `secure_sig` without a signature is dropped.
    let mut unsigned = test_commit();
    unsigned.secure_sig = Some(SecureSig {
        data: b"stale".to_vec(),
        sig: vec![],
    });
    verify_or_compute_git_commit_hash(&mut unsigned).unwrap();
    assert_eq!(unsigned.secure_sig, None);
}

/// Serializes a native Git commit on top of `PARENT` whose committer name is jj's
/// empty-string placeholder.
fn raw_git_commit_with_tree(tree: [u8; 20], extra_headers: &[(&str, &[u8])]) -> Vec<u8> {
    use gix::objs::WriteTo;

    let commit = gix::objs::Commit {
        tree: gix::hash::ObjectId::try_from(&tree[..]).unwrap(),
        parents: vec![gix::hash::ObjectId::try_from(&PARENT[..]).unwrap()].into(),
        author: gix::actor::Signature {
            name: "Git Author".into(),
            email: "author@example.com".into(),
            time: gix::date::Time::new(1_730_000_000, 3600),
        },
        committer: gix::actor::Signature {
            name: EMPTY_STRING_PLACEHOLDER.into(),
            email: "committer@example.com".into(),
            time: gix::date::Time::new(1_730_000_100, -7200),
        },
        encoding: None,
        extra_headers: extra_headers
            .iter()
            .map(|(k, v)| ((*k).into(), (*v).into()))
            .collect(),
        message: "imported commit\n".into(),
    };
    let mut buf = Vec::new();
    commit.write_to(&mut buf).unwrap();
    buf
}

fn raw_git_commit(extra_headers: &[(&str, &[u8])]) -> Vec<u8> {
    raw_git_commit_with_tree(TREE, extra_headers)
}

fn synthetic_change_id_for_raw(raw: &[u8]) -> Vec<u8> {
    use jj_lib::object_id::ObjectId as _;
    let hash =
        gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Commit, raw).unwrap();
    let jj_commit_id = jj_lib::backend::CommitId::from_bytes(hash.as_bytes());
    jj_lib::git_backend::synthetic_change_id_from_git_commit_id(&jj_commit_id)
        .as_bytes()
        .to_vec()
}

/// Builds the structured commit that `jj cc import-git` sends for `raw`.
fn imported_commit(raw: Vec<u8>, change_id: Vec<u8>) -> Commit {
    let secure_sig = gix::objs::CommitRef::from_bytes(&raw, gix::hash::Kind::Sha1)
        .unwrap()
        .extra_headers()
        .pgp_signature()
        .map(|sig| SecureSig {
            data: vec![],
            sig: sig.to_vec(),
        });
    Commit {
        commit_id: gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Commit, &raw)
            .unwrap()
            .as_bytes()
            .to_vec(),
        change_id,
        parent_commit_ids: vec![PARENT.to_vec()],
        root_tree_id: vec![TREE.to_vec()],
        description: "imported commit\n".to_string(),
        author: Some(signature(
            "Git Author",
            "author@example.com",
            1_730_000_000_000,
            60,
        )),
        committer: Some(signature(
            "",
            "committer@example.com",
            1_730_000_100_000,
            -120,
        )),
        predecessors: vec![],
        conflict_labels: vec![],
        secure_sig,
        raw_git_commit: raw,
        external_ids: Default::default(),
    }
}

#[test]
fn test_raw_git_commit_binds_the_fields_jj_reads() {
    let change_id = vec![0x5a; 16];
    let change_id_header = encode_reverse_hex(&change_id);
    let sig = b"-----BEGIN PGP SIGNATURE-----\n\nwsBcBAABCAAQ\n-----END PGP SIGNATURE-----\n";
    let unsigned_raw = raw_git_commit(&[("change-id", change_id_header.as_bytes())]);
    let raw = raw_git_commit(&[
        ("change-id", change_id_header.as_bytes()),
        ("gpgsig", &sig[..]),
    ]);
    let honest = imported_commit(raw.clone(), change_id.clone());

    let mut commit = honest.clone();
    let commit_id = verify_or_compute_git_commit_hash(&mut commit).unwrap();
    assert_eq!(commit_id, honest.commit_id);
    // The signed payload is the raw commit without its gpgsig header.
    assert_eq!(commit.secure_sig.unwrap().data, unsigned_raw);

    // The importer keeps jj's placeholder for an empty committer name.
    let mut commit = honest.clone();
    commit.committer.as_mut().unwrap().name = EMPTY_STRING_PLACEHOLDER.to_string();
    assert!(verify_or_compute_git_commit_hash(&mut commit).is_ok());

    let forgeries: &[fn(&mut Commit)] = &[
        |c| c.description = "forged description\n".to_string(),
        |c| c.author.as_mut().unwrap().name = "Someone Else".to_string(),
        |c| c.author.as_mut().unwrap().email = "else@example.com".to_string(),
        |c| {
            c.author
                .as_mut()
                .unwrap()
                .timestamp
                .as_mut()
                .unwrap()
                .millis_since_epoch += 1000
        },
        |c| {
            c.committer
                .as_mut()
                .unwrap()
                .timestamp
                .as_mut()
                .unwrap()
                .tz_offset = 0
        },
        |c| c.change_id = vec![0x5b; 16],
        |c| c.root_tree_id = vec![TREE.to_vec(), TREE.to_vec(), vec![0xee; 20]],
        |c| c.secure_sig = None,
        |c| c.secure_sig.as_mut().unwrap().sig = b"other signature".to_vec(),
        |c| c.secure_sig.as_mut().unwrap().data = b"forged payload".to_vec(),
    ];
    for forge in forgeries {
        let mut commit = honest.clone();
        forge(&mut commit);
        assert!(
            verify_or_compute_git_commit_hash(&mut commit).is_err(),
            "accepted {commit:?}"
        );
    }

    // A commit without a change-id header must match jj_lib's synthetic Git change ID;
    // an arbitrary change_id or a signature on an unsigned raw commit is rejected.
    let headerless_raw = raw_git_commit(&[]);
    let synthetic_change_id = synthetic_change_id_for_raw(&headerless_raw);
    let mut wrong_change_id_commit = imported_commit(headerless_raw.clone(), vec![0x77; 16]);
    assert!(verify_or_compute_git_commit_hash(&mut wrong_change_id_commit).is_err());

    let mut commit = imported_commit(headerless_raw, synthetic_change_id);
    assert!(verify_or_compute_git_commit_hash(&mut commit.clone()).is_ok());
    commit.secure_sig = Some(SecureSig {
        data: vec![],
        sig: sig.to_vec(),
    });
    assert!(verify_or_compute_git_commit_hash(&mut commit).is_err());
}

#[test]
fn test_raw_git_commit_conflict_headers_must_match_trees_and_labels() {
    let trees = [TREE.to_vec(), vec![0x12; 20], vec![0x13; 20]];
    let trees_header = trees.iter().map(hex::encode).collect::<Vec<_>>().join(" ");
    let raw = raw_git_commit(&[
        ("jj:conflict-labels", b"left\nbase\nright\n"),
        ("jj:trees", trees_header.as_bytes()),
    ]);
    let change_id = synthetic_change_id_for_raw(&raw);
    let mut honest = imported_commit(raw, change_id);
    honest.root_tree_id = trees.to_vec();
    honest.conflict_labels = vec!["left".into(), "base".into(), "right".into()];
    assert!(verify_or_compute_git_commit_hash(&mut honest.clone()).is_ok());

    let mut commit = honest.clone();
    commit.root_tree_id[2] = vec![0xee; 20];
    assert!(verify_or_compute_git_commit_hash(&mut commit).is_err());
    let mut commit = honest.clone();
    commit.conflict_labels[2] = "other".into();
    assert!(verify_or_compute_git_commit_hash(&mut commit).is_err());

    let raw = raw_git_commit(&[("jj:trees", b"not-hex")]);
    let change_id = synthetic_change_id_for_raw(&raw);
    let mut commit = imported_commit(raw, change_id);
    commit.root_tree_id = trees.to_vec();
    assert!(verify_or_compute_git_commit_hash(&mut commit).is_err());

    // `jj_lib::GitBackend` writes a synthetic `.jjconflict` tree in the Git `tree` header
    // while listing the real conflict sides in `jj:trees`; accept that layout when
    // `jj:trees` matches `commit.root_tree_id`, and reject a mismatched `tree` header when
    // `jj:trees` is absent.
    let synthetic_conflict_tree = [0x99; 20];
    let raw_jj_conflict = raw_git_commit_with_tree(
        synthetic_conflict_tree,
        &[
            ("jj:conflict-labels", b"left\nbase\nright\n"),
            ("jj:trees", trees_header.as_bytes()),
        ],
    );
    let change_id = synthetic_change_id_for_raw(&raw_jj_conflict);
    let mut jj_conflict = imported_commit(raw_jj_conflict, change_id);
    jj_conflict.root_tree_id = trees.to_vec();
    jj_conflict.conflict_labels = vec!["left".into(), "base".into(), "right".into()];
    assert!(verify_or_compute_git_commit_hash(&mut jj_conflict).is_ok());

    let raw_mismatched_tree = raw_git_commit_with_tree(synthetic_conflict_tree, &[]);
    let change_id = synthetic_change_id_for_raw(&raw_mismatched_tree);
    let mut mismatched_resolved = imported_commit(raw_mismatched_tree, change_id);
    assert!(verify_or_compute_git_commit_hash(&mut mismatched_resolved).is_err());

    // All-empty `jj:conflict-labels` headers (`""`, `"\n"`, `"\n\n\n"`) on a 3-tree commit
    // must be accepted and canonicalize `commit.conflict_labels` to `[]`.
    for header_val in [&b""[..], &b"\n"[..], &b"\n\n\n"[..]] {
        let raw_empty_labels = raw_git_commit(&[
            ("jj:conflict-labels", header_val),
            ("jj:trees", trees_header.as_bytes()),
        ]);
        let change_id = synthetic_change_id_for_raw(&raw_empty_labels);
        let mut commit = imported_commit(raw_empty_labels, change_id);
        commit.root_tree_id = trees.to_vec();
        commit.conflict_labels = vec![String::new(), String::new(), String::new()];
        assert!(
            verify_or_compute_git_commit_hash(&mut commit).is_ok(),
            "all-empty jj:conflict-labels {header_val:?} should be accepted"
        );
        assert!(commit.conflict_labels.is_empty());
    }

    // Wrong-length all-empty `jj:conflict-labels` headers must be rejected.
    let raw_wrong_len = raw_git_commit(&[
        ("jj:conflict-labels", b"\n\n"),
        ("jj:trees", trees_header.as_bytes()),
    ]);
    let change_id = synthetic_change_id_for_raw(&raw_wrong_len);
    let mut wrong_len = imported_commit(raw_wrong_len, change_id);
    wrong_len.root_tree_id = trees.to_vec();
    assert!(verify_or_compute_git_commit_hash(&mut wrong_len).is_err());

    let raw_single_tree_three_empty = raw_git_commit(&[("jj:conflict-labels", b"\n\n\n")]);
    let change_id = synthetic_change_id_for_raw(&raw_single_tree_three_empty);
    let mut single_tree_three_empty = imported_commit(raw_single_tree_three_empty, change_id);
    assert!(verify_or_compute_git_commit_hash(&mut single_tree_three_empty).is_err());

    // All-empty raw header must still reject non-empty structured `conflict_labels`.
    let raw_three_empty = raw_git_commit(&[
        ("jj:conflict-labels", b"\n\n\n"),
        ("jj:trees", trees_header.as_bytes()),
    ]);
    let change_id = synthetic_change_id_for_raw(&raw_three_empty);
    let mut non_empty_mismatch = imported_commit(raw_three_empty, change_id);
    non_empty_mismatch.root_tree_id = trees.to_vec();
    non_empty_mismatch.conflict_labels = vec!["left".into(), String::new(), "right".into()];
    assert!(verify_or_compute_git_commit_hash(&mut non_empty_mismatch).is_err());
}

#[test]
fn test_validate_commit_external_ids_format() {
    let mut commit = test_commit();
    commit
        .external_ids
        .insert("git-sha1".to_string(), vec![0xaa; 20]);
    commit
        .external_ids
        .insert("git-sha256".to_string(), vec![0xbb; 32]);
    commit
        .external_ids
        .insert("perforce-cl".to_string(), b"987654".to_vec());
    assert!(verify_or_compute_git_commit_hash(&mut commit).is_ok());

    let long_key = "a".repeat(65);
    for (bad_key, bad_val) in [
        ("", vec![1]),
        (".", vec![1]),
        ("..", vec![1]),
        ("bad/key", vec![1]),
        ("Git-Sha1", vec![0xaa; 20]),
        (long_key.as_str(), vec![1]),
        ("git-sha1", vec![0xaa; 19]),
        ("git-sha256", vec![0xbb; 20]),
        ("perforce-cl", vec![]),
        ("perforce-cl", vec![1; 65]),
    ] {
        let mut bad = test_commit();
        bad.external_ids.insert(bad_key.to_string(), bad_val);
        assert!(
            verify_or_compute_git_commit_hash(&mut bad).is_err(),
            "expected invalid external_ids key={bad_key:?} to be rejected"
        );
    }

    let mut too_many = test_commit();
    for idx in 0..17 {
        too_many.external_ids.insert(format!("ext-{idx}"), vec![1]);
    }
    assert!(verify_or_compute_git_commit_hash(&mut too_many).is_err());
}

#[test]
fn test_validate_commit_external_ids_actor_attribution_preserves_git_hash() {
    let mut baseline = test_commit();
    let baseline_hash = verify_or_compute_git_commit_hash(&mut baseline).unwrap();

    let long_sub = format!("system:actor:atespace-prod:{}", "a".repeat(120));
    assert!(long_sub.len() > 64 && long_sub.len() <= 256);
    let max_len_uid = "u".repeat(256);

    let mut attributed = test_commit();
    attributed
        .external_ids
        .insert("actor-sub".to_string(), long_sub.as_bytes().to_vec());
    attributed
        .external_ids
        .insert("actor-uid".to_string(), max_len_uid.as_bytes().to_vec());
    let attributed_hash = verify_or_compute_git_commit_hash(&mut attributed).unwrap();
    assert_eq!(
        attributed_hash, baseline_hash,
        "actor-sub and actor-uid must be excluded from canonical Git SHA-1 hashing"
    );

    let overlong = vec![b'x'; 257];
    for key in ["actor-sub", "actor-uid"] {
        for bad_val in [
            vec![],
            overlong.clone(),
            vec![0xff, 0xfe],
            b"line1\nline2".to_vec(),
            b"null\0byte".to_vec(),
            b"tab\there".to_vec(),
            b"del\x7fchar".to_vec(),
        ] {
            let mut bad = test_commit();
            bad.external_ids.insert(key.to_string(), bad_val);
            assert!(
                verify_or_compute_git_commit_hash(&mut bad).is_err(),
                "expected invalid {key} external_id to be rejected"
            );
        }
    }
}

#[test]
fn test_try_blob_view_and_operation_hashes_return_deterministic_digests() {
    let empty_blob = try_compute_git_blob_hash(b"").unwrap();
    assert_eq!(
        hex::encode(&empty_blob),
        "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"
    );

    let view_a = cc_common::op_store::View {
        head_ids: vec![vec![0x22; 20], vec![0x11; 20], vec![0x11; 20]],
        wc_commit_ids: Default::default(),
        local_bookmarks: Default::default(),
        remote_bookmarks: Default::default(),
    };
    let view_b = cc_common::op_store::View {
        head_ids: vec![vec![0x11; 20], vec![0x22; 20]],
        wc_commit_ids: Default::default(),
        local_bookmarks: Default::default(),
        remote_bookmarks: Default::default(),
    };
    let vid_a = hash_view(&view_a).unwrap();
    let vid_b = hash_view(&view_b).unwrap();
    assert_eq!(vid_a.len(), cc_common::VIEW_ID_LENGTH);
    assert_eq!(vid_a, vid_b);

    let op = cc_common::op_store::Operation {
        view_id: vid_a,
        parents: vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()],
        metadata: None,
        commit_predecessors: Vec::new(),
        commit_predecessors_set: true,
    };
    let op_id = hash_operation(&op).unwrap();
    assert_eq!(op_id.len(), cc_common::OPERATION_ID_LENGTH);
    assert_eq!(op_id, hash_operation(&op).unwrap());
}
