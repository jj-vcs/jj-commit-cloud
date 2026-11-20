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

//! Deterministic SHA-1 content-addressed hashing for commits, trees, blobs, views, and operations.
//!
//! # Authority and Crash-Safety Ordering
//! Every object digest is computed or verified server-side via `gitoxide` (`gix`) before
//! persistence so corrupted or forged client digests cannot poison content-addressed maps.
//!
//! # Concurrency and Merge Invariants
//! Directory entries are hashed in canonical Git tree sort order (`"foo.rs"` before `"foo/"`)
//! while rejecting duplicate or path-traversing entry names; `View` and `Operation` collections
//! are length-framed and map-sorted (`BTreeMap`) while preserving `RefTarget` `Merge` side
//! ordering so identical snapshots converge on collision-free 16-byte `ViewId` and `OperationId`
//! digests.

use std::collections::{BTreeMap, HashSet};

const EMPTY_STRING_PLACEHOLDER: &str = "JJ_EMPTY_STRING";
const REVERSE_HEX_CHARS: &[u8; 16] = b"zyxwvutsrqponmlk";
const FORWARD_HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

/// Appends a byte slice preceded by its 8-byte length prefix to prevent boundary hash collisions.
fn append_length_prefixed_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    buf.extend_from_slice(bytes);
}

/// Appends a UTF-8 string slice preceded by its length prefix.
fn append_length_prefixed_str(buf: &mut Vec<u8>, s: &str) {
    append_length_prefixed_bytes(buf, s.as_bytes());
}

fn signature_to_git(
    sig: Option<&cc_common::backend::Signature>,
) -> Result<gix::actor::Signature, String> {
    let (name, email) = match sig {
        Some(s) => (
            if s.name.is_empty() {
                EMPTY_STRING_PLACEHOLDER
            } else {
                &s.name
            },
            if s.email.is_empty() {
                EMPTY_STRING_PLACEHOLDER
            } else {
                &s.email
            },
        ),
        None => (EMPTY_STRING_PLACEHOLDER, EMPTY_STRING_PLACEHOLDER),
    };
    if name.contains(['<', '>', '\n', '\0']) || email.contains(['<', '>', '\n', '\0']) {
        return Err("signature name or email contains invalid Git header characters".to_string());
    }
    let (secs, offset_mins) = sig.and_then(|s| s.timestamp.as_ref()).map_or((0, 0), |t| {
        (t.millis_since_epoch.div_euclid(1000), t.tz_offset)
    });
    let offset_secs = offset_mins
        .checked_mul(60)
        .ok_or_else(|| format!("signature tz_offset {offset_mins} overflows seconds"))?;
    if offset_secs.unsigned_abs() / 3_600 > 99 {
        return Err(format!(
            "signature tz_offset {offset_mins} minutes exceeds Git +-9959 limit"
        ));
    }

    Ok(gix::actor::Signature {
        name: name.into(),
        email: email.into(),
        time: gix::date::Time::new(secs, offset_secs),
    })
}

/// Validates structured `Commit` fields and computes its canonical 20-byte Git commit SHA-1.
pub fn compute_git_commit_hash(commit: &cc_common::backend::Commit) -> Result<Vec<u8>, String> {
    use gix::objs::WriteTo;

    if commit.secure_sig.is_some() || !commit.raw_git_commit.is_empty() {
        return Err("signed commits and raw_git_commit payloads are not yet supported".to_string());
    }
    if commit.change_id.len() != cc_common::CHANGE_ID_LENGTH {
        return Err(format!(
            "change_id must be {} bytes (got {} bytes)",
            cc_common::CHANGE_ID_LENGTH,
            commit.change_id.len()
        ));
    }
    let num_trees = commit.root_tree_id.len();
    if num_trees == 0 || num_trees.is_multiple_of(2) {
        return Err(format!(
            "root_tree_id must have odd non-zero length (got {num_trees})"
        ));
    }
    for tree_bytes in &commit.root_tree_id {
        if tree_bytes.len() != cc_common::TREE_ID_LENGTH {
            return Err(format!(
                "root_tree_id entry must be {} bytes (got {} bytes)",
                cc_common::TREE_ID_LENGTH,
                tree_bytes.len()
            ));
        }
    }
    if commit.conflict_labels.iter().any(|l| l.contains('\n')) {
        return Err("conflict_labels entries must not contain newline characters".to_string());
    }
    let unlabeled = commit.conflict_labels.is_empty()
        || (commit.conflict_labels.len() == 1 && commit.conflict_labels[0].is_empty());
    let has_non_empty_label = commit.conflict_labels.iter().any(|l| !l.is_empty());
    if !unlabeled
        && (num_trees == 1 || commit.conflict_labels.len() != num_trees || !has_non_empty_label)
    {
        return Err(format!(
            "conflict_labels must be empty or hold {num_trees} labels with at least one non-empty label on conflicted commits (got {} labels)",
            commit.conflict_labels.len()
        ));
    }

    let tree_id = gix::hash::ObjectId::try_from(commit.root_tree_id[0].as_slice())
        .map_err(|e| format!("invalid root_tree_id: {e}"))?;

    if commit.parent_commit_ids.is_empty() {
        return Err("parent_commit_ids must not be empty".to_string());
    }
    for id in &commit.parent_commit_ids {
        if id.len() != cc_common::COMMIT_ID_LENGTH {
            return Err(format!(
                "parent_commit_id must be {} bytes (got {} bytes)",
                cc_common::COMMIT_ID_LENGTH,
                id.len()
            ));
        }
    }
    let has_root_parent = commit
        .parent_commit_ids
        .iter()
        .any(|id| id.as_slice() == cc_common::ROOT_COMMIT_ID_BYTES);
    if has_root_parent && commit.parent_commit_ids.len() != 1 {
        return Err(
            "ROOT_COMMIT_ID may only appear as the sole entry in parent_commit_ids".to_string(),
        );
    }

    let mut parents = Vec::with_capacity(commit.parent_commit_ids.len());
    for id in &commit.parent_commit_ids {
        if id.as_slice() == cc_common::ROOT_COMMIT_ID_BYTES {
            continue;
        }
        let oid = gix::hash::ObjectId::try_from(id.as_slice())
            .map_err(|e| format!("invalid parent_commit_id: {e}"))?;
        parents.push(oid);
    }
    for pred in &commit.predecessors {
        if pred.len() != cc_common::COMMIT_ID_LENGTH {
            return Err(format!(
                "predecessor commit_id must be {} bytes (got {} bytes)",
                cc_common::COMMIT_ID_LENGTH,
                pred.len()
            ));
        }
    }

    let mut extra_headers = Vec::new();
    if has_non_empty_label {
        let mut joined = commit.conflict_labels.join("\n");
        joined.push('\n');
        extra_headers.push(("jj:conflict-labels".into(), joined.into()));
    }
    if num_trees > 1 {
        let trees_hex: Vec<String> = commit
            .root_tree_id
            .iter()
            .map(|id| {
                let bytes: Vec<u8> = id
                    .iter()
                    .flat_map(|b| {
                        [
                            FORWARD_HEX_CHARS[usize::from(b >> 4)],
                            FORWARD_HEX_CHARS[usize::from(b & 0x0f)],
                        ]
                    })
                    .collect();
                String::from_utf8(bytes).expect("hex chars are valid UTF-8")
            })
            .collect();
        extra_headers.push(("jj:trees".into(), trees_hex.join(" ").into()));
    }
    let rev_bytes: Vec<u8> = commit
        .change_id
        .iter()
        .flat_map(|b| {
            [
                REVERSE_HEX_CHARS[usize::from(b >> 4)],
                REVERSE_HEX_CHARS[usize::from(b & 0x0f)],
            ]
        })
        .collect();
    extra_headers.push((
        "change-id".into(),
        String::from_utf8(rev_bytes)
            .expect("reverse-hex chars are valid UTF-8")
            .into(),
    ));

    let gix_commit = gix::objs::Commit {
        tree: tree_id,
        parents: parents.into(),
        author: signature_to_git(commit.author.as_ref())?,
        committer: signature_to_git(commit.committer.as_ref())?,
        encoding: None,
        extra_headers,
        message: commit.description.as_bytes().into(),
    };

    let mut buf = Vec::new();
    gix_commit
        .write_to(&mut buf)
        .map_err(|e| format!("failed to serialize Git commit: {e}"))?;
    let hash = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Commit, &buf)
        .map_err(|e| format!("SHA-1 digest computation error: {e}"))?;
    Ok(hash.as_bytes().to_vec())
}

/// Computes a 20-byte Git blob SHA-1 digest using `gix` (returning an error on SHA-1DC collision blocks).
pub fn compute_git_blob_hash(content: &[u8]) -> Result<Vec<u8>, String> {
    let hash = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Blob, content)
        .map_err(|e| format!("SHA-1 digest computation error: {e}"))?;
    Ok(hash.as_bytes().to_vec())
}

/// Validates directory entry names and digests, rejects duplicate names, and computes a
/// canonical 20-byte Git tree SHA-1 digest using `gix`.
pub fn compute_git_tree_hash(entries: &[cc_common::backend::TreeEntry]) -> Result<Vec<u8>, String> {
    use gix::objs::WriteTo;

    let mut seen_names = HashSet::with_capacity(entries.len());
    let mut gix_entries = Vec::with_capacity(entries.len());
    for entry in entries {
        let name = entry.name.as_str();
        if name.is_empty() || name == "." || name == ".." || name.contains(['/', '\0']) {
            return Err(format!("invalid tree entry name {name:?}"));
        }
        if !seen_names.insert(name) {
            return Err(format!("duplicate tree entry name {name:?}"));
        }
        let (kind, expected_len, id_bytes) =
            match entry.value.as_ref().and_then(|v| v.value.as_ref()) {
                Some(cc_common::backend::tree_value::Value::File(f)) => {
                    if !f.copy_id.is_empty() {
                        return Err(format!(
                            "tree entry {name:?} has non-empty copy_id, which is not supported"
                        ));
                    }
                    let k = if f.executable {
                        gix::objs::tree::EntryKind::BlobExecutable
                    } else {
                        gix::objs::tree::EntryKind::Blob
                    };
                    (k, cc_common::FILE_ID_LENGTH, &f.id[..])
                }
                Some(cc_common::backend::tree_value::Value::TreeId(id)) => (
                    gix::objs::tree::EntryKind::Tree,
                    cc_common::TREE_ID_LENGTH,
                    &id[..],
                ),
                Some(cc_common::backend::tree_value::Value::SymlinkId(id)) => (
                    gix::objs::tree::EntryKind::Link,
                    cc_common::SYMLINK_ID_LENGTH,
                    &id[..],
                ),
                Some(cc_common::backend::tree_value::Value::GitSubmoduleId(id)) => (
                    gix::objs::tree::EntryKind::Commit,
                    cc_common::COMMIT_ID_LENGTH,
                    &id[..],
                ),
                Some(cc_common::backend::tree_value::Value::ConflictId(_)) => {
                    return Err(format!(
                        "tree entry {name:?} has an unsupported ConflictId value"
                    ));
                }
                None => return Err(format!("tree entry {name:?} has no value")),
            };
        if id_bytes.len() != expected_len {
            return Err(format!(
                "tree entry {name:?} object ID must be {expected_len} bytes (got {} bytes)",
                id_bytes.len()
            ));
        }
        let oid = gix::hash::ObjectId::try_from(id_bytes)
            .map_err(|e| format!("invalid tree entry object ID for {name:?}: {e}"))?;
        gix_entries.push(gix::objs::tree::Entry {
            mode: kind.into(),
            filename: entry.name.as_bytes().into(),
            oid,
        });
    }

    // Sort entries according to Git canonical tree entry ordering rules (`gix::objs::tree::Entry`
    // implements `Ord` with trailing `'/'` semantics on directories).
    gix_entries.sort_unstable();

    let gix_tree = gix::objs::Tree {
        entries: gix_entries,
    };
    let mut buf = Vec::new();
    gix_tree
        .write_to(&mut buf)
        .map_err(|e| format!("failed to serialize Git tree: {e}"))?;

    let hash = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Tree, &buf)
        .map_err(|e| format!("SHA-1 digest computation error: {e}"))?;
    Ok(hash.as_bytes().to_vec())
}

/// Computes a deterministic 16-byte `OperationId` digest over length-framed, sorted fields.
pub fn hash_operation(op: &cc_common::op_store::Operation) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    append_length_prefixed_bytes(&mut buf, &op.view_id);
    buf.extend_from_slice(&(op.parents.len() as u64).to_le_bytes());
    for p in &op.parents {
        append_length_prefixed_bytes(&mut buf, p);
    }
    let default_meta = cc_common::op_store::OperationMetadata::default();
    let meta = op.metadata.as_ref().unwrap_or(&default_meta);
    buf.extend_from_slice(&meta.start_time_millis.to_le_bytes());
    buf.extend_from_slice(&meta.end_time_millis.to_le_bytes());
    append_length_prefixed_str(&mut buf, &meta.description);
    append_length_prefixed_str(&mut buf, &meta.hostname);
    append_length_prefixed_str(&mut buf, &meta.username);
    buf.push(meta.is_snapshot as u8);
    if let Some(ws) = &meta.workspace_name {
        buf.push(1);
        append_length_prefixed_str(&mut buf, ws);
    } else {
        buf.push(0);
    }
    let sorted_attrs: BTreeMap<_, _> = meta.attributes.iter().collect();
    buf.extend_from_slice(&(sorted_attrs.len() as u64).to_le_bytes());
    for (k, v) in sorted_attrs {
        append_length_prefixed_str(&mut buf, k);
        append_length_prefixed_str(&mut buf, v);
    }
    buf.push(op.commit_predecessors_set as u8);
    let mut sorted_preds: Vec<_> = op.commit_predecessors.iter().collect();
    sorted_preds.sort_by(|a, b| a.commit_id.cmp(&b.commit_id));
    buf.extend_from_slice(&(sorted_preds.len() as u64).to_le_bytes());
    for pred in sorted_preds {
        append_length_prefixed_bytes(&mut buf, &pred.commit_id);
        buf.extend_from_slice(&(pred.predecessor_ids.len() as u64).to_le_bytes());
        for p_id in &pred.predecessor_ids {
            append_length_prefixed_bytes(&mut buf, p_id);
        }
    }
    let hash = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Blob, &buf)
        .map_err(|e| format!("SHA-1 digest computation error: {e}"))?;
    Ok(hash.as_bytes()[..cc_common::OPERATION_ID_LENGTH].to_vec())
}

/// Computes a deterministic 16-byte `ViewId` digest over length-framed, sorted fields while
/// preserving `RefTarget` `Merge` term order.
pub fn hash_view(view: &cc_common::op_store::View) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    let mut head_ids = view.head_ids.clone();
    head_ids.sort();
    head_ids.dedup();
    buf.extend_from_slice(&(head_ids.len() as u64).to_le_bytes());
    for head in &head_ids {
        append_length_prefixed_bytes(&mut buf, head);
    }

    let sorted_wc: BTreeMap<_, _> = view.wc_commit_ids.iter().collect();
    buf.extend_from_slice(&(sorted_wc.len() as u64).to_le_bytes());
    for (k, v) in sorted_wc {
        append_length_prefixed_str(&mut buf, k);
        append_length_prefixed_bytes(&mut buf, v);
    }

    let append_ref_target = |buf: &mut Vec<u8>, target: &cc_common::op_store::RefTarget| {
        buf.extend_from_slice(&(target.removes.len() as u64).to_le_bytes());
        for term in &target.removes {
            append_length_prefixed_bytes(buf, &term.commit_id);
        }
        buf.extend_from_slice(&(target.adds.len() as u64).to_le_bytes());
        for term in &target.adds {
            append_length_prefixed_bytes(buf, &term.commit_id);
        }
    };

    let sorted_bookmarks: BTreeMap<_, _> = view.local_bookmarks.iter().collect();
    buf.extend_from_slice(&(sorted_bookmarks.len() as u64).to_le_bytes());
    for (name, target) in sorted_bookmarks {
        append_length_prefixed_str(&mut buf, name);
        append_ref_target(&mut buf, target);
    }

    let sorted_remotes: BTreeMap<_, _> = view.remote_bookmarks.iter().collect();
    buf.extend_from_slice(&(sorted_remotes.len() as u64).to_le_bytes());
    for (name, remote_ref) in sorted_remotes {
        append_length_prefixed_str(&mut buf, name);
        buf.push(remote_ref.is_tracked as u8);
        let empty_target = cc_common::op_store::RefTarget::default();
        append_ref_target(
            &mut buf,
            remote_ref.target.as_ref().unwrap_or(&empty_target),
        );
    }

    let hash = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Blob, &buf)
        .map_err(|e| format!("SHA-1 digest computation error: {e}"))?;
    Ok(hash.as_bytes()[..cc_common::VIEW_ID_LENGTH].to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_common::backend::{File, Signature, Timestamp, TreeEntry, TreeValue, tree_value::Value};
    use cc_common::op_store::{Operation, RefTarget, RefTargetTerm, View};

    fn file_entry(name: &str, id: [u8; 20]) -> TreeEntry {
        TreeEntry {
            name: name.to_string(),
            value: Some(TreeValue {
                value: Some(Value::File(File {
                    id: id.to_vec(),
                    executable: false,
                    copy_id: Vec::new(),
                    size_bytes: Some(42),
                })),
            }),
        }
    }

    #[test]
    fn compute_git_tree_hash_rejects_invalid_and_duplicate_names_and_bad_digests() {
        for bad_name in ["", ".", "..", "a/b", "a\0b"] {
            let err = compute_git_tree_hash(&[file_entry(bad_name, [0x11; 20])]).unwrap_err();
            assert!(err.contains("invalid tree entry name"), "{err}");
        }

        let non_adjacent_dup = vec![
            file_entry("foo", [0x11; 20]),
            file_entry("foo-bar", [0x22; 20]),
            TreeEntry {
                name: "foo".to_string(),
                value: Some(TreeValue {
                    value: Some(Value::TreeId(vec![0x33; 20])),
                }),
            },
        ];
        assert!(
            compute_git_tree_hash(&non_adjacent_dup)
                .unwrap_err()
                .contains("duplicate tree entry name")
        );

        let copy_id_err = compute_git_tree_hash(&[TreeEntry {
            name: "copied".to_string(),
            value: Some(TreeValue {
                value: Some(Value::File(File {
                    id: vec![0x11; 20],
                    executable: false,
                    copy_id: vec![0x22; 20],
                    size_bytes: None,
                })),
            }),
        }])
        .unwrap_err();
        assert!(copy_id_err.contains("copy_id"), "{copy_id_err}");

        assert!(
            compute_git_tree_hash(&[TreeEntry {
                name: "conflicted".to_string(),
                value: Some(TreeValue {
                    value: Some(Value::ConflictId(vec![0x44; 20])),
                }),
            }])
            .unwrap_err()
            .contains("ConflictId")
        );
        assert!(
            compute_git_tree_hash(&[TreeEntry {
                name: "missing".to_string(),
                value: None,
            }])
            .unwrap_err()
            .contains("no value")
        );
        assert!(
            compute_git_tree_hash(&[TreeEntry {
                name: "short".to_string(),
                value: Some(TreeValue {
                    value: Some(Value::SymlinkId(vec![0x55; 19])),
                }),
            }])
            .unwrap_err()
            .contains("must be 20 bytes")
        );
    }

    #[test]
    fn compute_git_tree_hash_matches_git_submodule_mode_160000() {
        use gix::objs::WriteTo;

        let sub_oid = [0xab; 20];
        let computed = compute_git_tree_hash(&[TreeEntry {
            name: "vendor-sub".to_string(),
            value: Some(TreeValue {
                value: Some(Value::GitSubmoduleId(sub_oid.to_vec())),
            }),
        }])
        .unwrap();

        let gix_tree = gix::objs::Tree {
            entries: vec![gix::objs::tree::Entry {
                mode: gix::objs::tree::EntryKind::Commit.into(),
                filename: "vendor-sub".into(),
                oid: gix::hash::ObjectId::from_bytes_or_panic(&sub_oid),
            }],
        };
        let mut raw = Vec::new();
        gix_tree.write_to(&mut raw).unwrap();
        let expected =
            gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Tree, &raw).unwrap();
        assert_eq!(computed, expected.as_bytes());
    }

    #[test]
    fn compute_git_commit_hash_rejects_malformed_fields() {
        let base = cc_common::backend::Commit {
            change_id: vec![0x11; 16],
            parent_commit_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
            root_tree_id: vec![cc_common::EMPTY_TREE_ID_BYTES.to_vec()],
            author: Some(Signature {
                name: "Dev".to_string(),
                email: "dev@example.com".to_string(),
                timestamp: Some(Timestamp {
                    millis_since_epoch: 1_700_000_000_000,
                    tz_offset: 0,
                }),
            }),
            committer: Some(Signature {
                name: "Dev".to_string(),
                email: "dev@example.com".to_string(),
                timestamp: Some(Timestamp {
                    millis_since_epoch: 1_700_000_000_000,
                    tz_offset: 0,
                }),
            }),
            ..Default::default()
        };
        let digest = compute_git_commit_hash(&base).unwrap();
        assert_eq!(digest.len(), 20);
        // Verify exact Git commit serialization with z-k reverse-hex change-id (`0x11` -> `"yy"`).
        let mut expected_git = gix::objs::Commit {
            tree: gix::hash::ObjectId::try_from(cc_common::EMPTY_TREE_ID_BYTES.as_slice()).unwrap(),
            parents: Default::default(),
            author: gix::actor::Signature {
                name: "Dev".into(),
                email: "dev@example.com".into(),
                time: gix::date::Time::new(1_700_000_000, 0),
            },
            committer: gix::actor::Signature {
                name: "Dev".into(),
                email: "dev@example.com".into(),
                time: gix::date::Time::new(1_700_000_000, 0),
            },
            encoding: None,
            message: "".into(),
            extra_headers: vec![(
                "change-id".into(),
                "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy".into(),
            )],
        };
        let mut raw_buf = Vec::new();
        gix::objs::WriteTo::write_to(&expected_git, &mut raw_buf).unwrap();
        let expected_sha =
            gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Commit, &raw_buf)
                .unwrap();
        assert_eq!(digest, expected_sha.as_bytes());

        // Conflicted commit encodes jj:conflict-labels before jj:trees before change-id.
        let mut conflicted = base.clone();
        conflicted.root_tree_id = vec![
            cc_common::EMPTY_TREE_ID_BYTES.to_vec(),
            vec![0x22; cc_common::TREE_ID_LENGTH],
            vec![0x33; cc_common::TREE_ID_LENGTH],
        ];
        conflicted.conflict_labels = vec!["left".into(), "base".into(), "right".into()];
        let conflict_digest = compute_git_commit_hash(&conflicted).unwrap();
        expected_git.extra_headers = vec![
            ("jj:conflict-labels".into(), "left\nbase\nright\n".into()),
            (
                "jj:trees".into(),
                format!(
                    "{} {} {}",
                    cc_common::EMPTY_TREE_ID_HEX,
                    "22".repeat(20),
                    "33".repeat(20)
                )
                .into(),
            ),
            (
                "change-id".into(),
                "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy".into(),
            ),
        ];
        raw_buf.clear();
        gix::objs::WriteTo::write_to(&expected_git, &mut raw_buf).unwrap();
        let expected_conflict_sha =
            gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Commit, &raw_buf)
                .unwrap();
        assert_eq!(conflict_digest, expected_conflict_sha.as_bytes());

        let mut empty_change = base.clone();
        empty_change.change_id.clear();
        assert!(compute_git_commit_hash(&empty_change).is_err());

        let mut empty_trees = base.clone();
        empty_trees.root_tree_id.clear();
        assert!(compute_git_commit_hash(&empty_trees).is_err());

        let mut empty_parents = base.clone();
        empty_parents.parent_commit_ids.clear();
        assert!(compute_git_commit_hash(&empty_parents).is_err());

        let mut mixed_root_parents = base.clone();
        mixed_root_parents.parent_commit_ids =
            vec![vec![0x99; 20], cc_common::ROOT_COMMIT_ID_BYTES.to_vec()];
        assert!(compute_git_commit_hash(&mixed_root_parents).is_err());

        let mut newline_labels = base.clone();
        newline_labels.root_tree_id = vec![vec![0x11; 20], vec![0x22; 20], vec![0x33; 20]];
        newline_labels.conflict_labels = vec!["a\nb".into(), "c".into(), "d".into()];
        assert!(compute_git_commit_hash(&newline_labels).is_err());

        let mut all_empty_labels = newline_labels;
        all_empty_labels.conflict_labels = vec![String::new(), String::new(), String::new()];
        assert!(compute_git_commit_hash(&all_empty_labels).is_err());

        let mut bad_tz = base;
        bad_tz
            .author
            .as_mut()
            .unwrap()
            .timestamp
            .as_mut()
            .unwrap()
            .tz_offset = 6_000;
        assert!(compute_git_commit_hash(&bad_tz).is_err());
    }

    #[test]
    fn hash_view_and_operation_frame_collections_and_preserve_merge_order() {
        let term = |b: u8| RefTargetTerm {
            commit_id: vec![b; 20],
        };
        let mut v1 = View::default();
        v1.local_bookmarks.insert(
            "main".to_string(),
            RefTarget {
                removes: vec![term(1)],
                adds: vec![term(2), term(3)],
            },
        );
        let mut v2 = View::default();
        v2.local_bookmarks.insert(
            "main".to_string(),
            RefTarget {
                removes: vec![],
                adds: vec![term(1), term(2), term(3)],
            },
        );
        assert_ne!(hash_view(&v1).unwrap(), hash_view(&v2).unwrap());

        let mut v3 = View::default();
        v3.local_bookmarks.insert(
            "main".to_string(),
            RefTarget {
                removes: vec![term(1)],
                adds: vec![term(3), term(2)],
            },
        );
        assert_ne!(hash_view(&v1).unwrap(), hash_view(&v3).unwrap());

        let op_unset = Operation {
            view_id: vec![0xaa; 16],
            commit_predecessors_set: false,
            ..Default::default()
        };
        let op_set = Operation {
            view_id: vec![0xaa; 16],
            commit_predecessors_set: true,
            ..Default::default()
        };
        assert_ne!(
            hash_operation(&op_unset).unwrap(),
            hash_operation(&op_set).unwrap()
        );
        assert_eq!(compute_git_blob_hash(b"hello").unwrap().len(), 20);
    }
}
